// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

using System.Diagnostics;
using System.Reflection;
using System.Text.Json;
using AirflowWorker.Contracts;
using Amazon.S3;
using Amazon.S3.Model;
using Amazon.SQS;
using Amazon.SQS.Model;
using Microsoft.Extensions.Logging;

namespace AirflowWorker;

/// <summary>
/// Long-running worker process that polls SQS and executes tasks in-process.
/// Shared state is initialized once and reused across all tasks.
///
/// Unlike the Python worker which uses fork() for task isolation, the .NET worker
/// executes tasks in-process because the CLR is not fork-safe. This means:
/// - Tasks share memory space with the worker
/// - Exception handling is critical to prevent worker crashes
/// - Memory monitoring helps detect leaks
/// </summary>
public class WorkerProcess
{
    private readonly string _workerId;
    private readonly string _taskQueueUrl;
    private readonly string _resultQueueUrl;
    private readonly TimeSpan _idleTimeout;
    private readonly TimeSpan _visibilityTimeout;
    private readonly int _maxTasks;
    private readonly string? _initAssembly;
    private readonly string _initType;

    private readonly IAmazonSQS _sqsClient;
    private readonly ILogger<WorkerProcess> _logger;
    private readonly string _batchJobId;

    private bool _running = true;
    private int _tasksExecuted = 0;
    private DateTime _lastTaskTime;
    private string? _currentReceiptHandle;

    // Shared state - initialized once, used by all tasks
    private ISharedState? _sharedState;

    // Memory threshold for worker restart (2GB default)
    private const long MaxMemoryBytes = 2L * 1024 * 1024 * 1024;

    public WorkerProcess(
        string workerId,
        string taskQueueUrl,
        string resultQueueUrl,
        TimeSpan idleTimeout,
        TimeSpan visibilityTimeout,
        int maxTasks,
        string? initAssembly,
        string initType)
    {
        _workerId = workerId;
        _taskQueueUrl = taskQueueUrl;
        _resultQueueUrl = resultQueueUrl;
        _idleTimeout = idleTimeout;
        _visibilityTimeout = visibilityTimeout;
        _maxTasks = maxTasks;
        _initAssembly = initAssembly;
        _initType = initType;

        _sqsClient = new AmazonSQSClient();
        _logger = LoggerFactory
            .Create(b => b.AddConsole().SetMinimumLevel(LogLevel.Information))
            .CreateLogger<WorkerProcess>();
        _batchJobId = Environment.GetEnvironmentVariable("AWS_BATCH_JOB_ID") ?? "unknown";
        _lastTaskTime = DateTime.UtcNow;

        // Register for graceful shutdown
        Console.CancelKeyPress += (_, e) =>
        {
            e.Cancel = true;
            HandleShutdown();
        };
        AppDomain.CurrentDomain.ProcessExit += (_, _) => HandleShutdown();
    }

    private void HandleShutdown()
    {
        _logger.LogInformation("Received shutdown signal, finishing current task...");
        _running = false;

        // Return current message to queue if processing
        if (_currentReceiptHandle != null)
        {
            try
            {
                _sqsClient.ChangeMessageVisibilityAsync(new ChangeMessageVisibilityRequest
                {
                    QueueUrl = _taskQueueUrl,
                    ReceiptHandle = _currentReceiptHandle,
                    VisibilityTimeout = 0 // Immediate return to queue
                }).GetAwaiter().GetResult();
                _logger.LogInformation("Returned in-flight message to queue");
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Failed to return message to queue");
            }
        }
    }

    public async Task RunAsync(CancellationToken cancellationToken)
    {
        _logger.LogInformation(
            "Worker {WorkerId} starting (Batch job: {JobId})",
            _workerId, _batchJobId);

        // Initialize shared state
        await InitializeSharedStateAsync();

        _logger.LogInformation("Worker {WorkerId} entering main loop", _workerId);

        while (_running && !cancellationToken.IsCancellationRequested)
        {
            // Check idle timeout
            if (DateTime.UtcNow - _lastTaskTime > _idleTimeout)
            {
                _logger.LogInformation(
                    "Worker {WorkerId} idle for {Timeout}, terminating",
                    _workerId, _idleTimeout);
                break;
            }

            // Check task limit
            if (_maxTasks > 0 && _tasksExecuted >= _maxTasks)
            {
                _logger.LogInformation(
                    "Worker {WorkerId} reached max tasks ({MaxTasks}), terminating",
                    _workerId, _maxTasks);
                break;
            }

            // Check memory usage
            var memoryUsage = GC.GetTotalMemory(forceFullCollection: false);
            if (memoryUsage > MaxMemoryBytes)
            {
                _logger.LogWarning(
                    "Worker {WorkerId} memory usage {Memory:N0} bytes exceeds threshold, terminating",
                    _workerId, memoryUsage);
                break;
            }

            // Poll for task
            var taskMessage = await PollForTaskAsync(cancellationToken);
            if (taskMessage == null)
                continue;

            // Execute task IN-PROCESS (no fork)
            await ExecuteTaskAsync(taskMessage);
        }

        // Cleanup shared state
        if (_sharedState != null)
        {
            try
            {
                await _sharedState.DisposeAsync();
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error disposing shared state");
            }
        }

        _logger.LogInformation(
            "Worker {WorkerId} shutting down (executed {Count} tasks)",
            _workerId, _tasksExecuted);
    }

    private async Task InitializeSharedStateAsync()
    {
        // Download shared state from S3 if configured
        await DownloadSharedStateFromS3Async();

        if (string.IsNullOrEmpty(_initAssembly))
        {
            _logger.LogInformation("No shared state initialization configured");
            return;
        }

        _logger.LogInformation(
            "Initializing shared state from {Assembly}.{Type}",
            _initAssembly, _initType);

        try
        {
            var loadContext = new PluginLoadContext(_initAssembly);
            var assembly = loadContext.LoadFromAssemblyPath(Path.GetFullPath(_initAssembly));
            var type = assembly.GetType(_initType)
                ?? throw new InvalidOperationException($"Type {_initType} not found in assembly {_initAssembly}");

            if (!typeof(ISharedState).IsAssignableFrom(type))
                throw new InvalidOperationException($"Type {_initType} must implement ISharedState");

            _sharedState = (ISharedState)Activator.CreateInstance(type)!;
            await _sharedState.InitializeAsync();

            _logger.LogInformation("Shared state initialized successfully");
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to initialize shared state");
            // Continue anyway - tasks can still run without shared state
        }
    }

    private async Task DownloadSharedStateFromS3Async()
    {
        var s3Uri = Environment.GetEnvironmentVariable("AIRFLOW_SHARED_STATE_S3_URI");
        if (string.IsNullOrEmpty(s3Uri))
        {
            _logger.LogInformation("No S3 shared state URI configured");
            return;
        }

        _logger.LogInformation("Downloading shared state from {S3Uri}", s3Uri);

        try
        {
            // Parse S3 URI: s3://bucket/key/path
            var uri = new Uri(s3Uri);
            var bucket = uri.Host;
            var keyPrefix = uri.AbsolutePath.TrimStart('/');

            using var s3Client = new AmazonS3Client();
            var localBasePath = Path.Combine(Path.GetTempPath(), "airflow-shared-state");
            Directory.CreateDirectory(localBasePath);

            // List and download all objects under the prefix
            var listRequest = new ListObjectsV2Request { BucketName = bucket, Prefix = keyPrefix };
            ListObjectsV2Response listResponse;
            var downloadedCount = 0;

            do
            {
                listResponse = await s3Client.ListObjectsV2Async(listRequest);

                foreach (var obj in listResponse.S3Objects)
                {
                    // Skip "directory" markers
                    if (obj.Key.EndsWith("/")) continue;

                    var relativePath = obj.Key.Substring(keyPrefix.Length).TrimStart('/');
                    var localPath = Path.Combine(localBasePath, relativePath);

                    // Create subdirectories if needed
                    var localDir = Path.GetDirectoryName(localPath);
                    if (!string.IsNullOrEmpty(localDir))
                    {
                        Directory.CreateDirectory(localDir);
                    }

                    _logger.LogDebug("Downloading {Key} to {LocalPath}", obj.Key, localPath);

                    var getResponse = await s3Client.GetObjectAsync(bucket, obj.Key);
                    await using var fileStream = File.Create(localPath);
                    await getResponse.ResponseStream.CopyToAsync(fileStream);
                    downloadedCount++;
                }

                listRequest.ContinuationToken = listResponse.NextContinuationToken;
            } while (listResponse.IsTruncated);

            // Set environment variable so ISharedState.InitializeAsync() can find the files
            Environment.SetEnvironmentVariable("AIRFLOW_SHARED_STATE_LOCAL_PATH", localBasePath);

            _logger.LogInformation(
                "Downloaded {Count} shared state files to {LocalPath}",
                downloadedCount, localBasePath);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to download shared state from S3");
            // Continue anyway - InitializeAsync may still work without pre-downloaded state
        }
    }

    private async Task<TaskQueueMessage?> PollForTaskAsync(CancellationToken cancellationToken)
    {
        try
        {
            var response = await _sqsClient.ReceiveMessageAsync(new ReceiveMessageRequest
            {
                QueueUrl = _taskQueueUrl,
                MaxNumberOfMessages = 1,
                WaitTimeSeconds = 20, // Long-polling
                VisibilityTimeout = (int)_visibilityTimeout.TotalSeconds,
                MessageAttributeNames = ["All"]
            }, cancellationToken);

            if (response.Messages.Count == 0)
                return null;

            var msg = response.Messages[0];
            _currentReceiptHandle = msg.ReceiptHandle;

            return JsonSerializer.Deserialize<TaskQueueMessage>(msg.Body);
        }
        catch (OperationCanceledException)
        {
            return null;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to poll task queue");
            await Task.Delay(5000, cancellationToken); // Back off on error
            return null;
        }
    }

    private async Task ExecuteTaskAsync(TaskQueueMessage taskMessage)
    {
        var stopwatch = Stopwatch.StartNew();
        var taskKey = taskMessage.TaskKey;

        _logger.LogInformation("Executing task {TaskKey}", taskKey);

        string state;
        string? errorMessage = null;

        // Execute with timeout
        using var cts = new CancellationTokenSource(_visibilityTimeout - TimeSpan.FromMinutes(1));

        try
        {
            // Create task context with shared state
            var context = new TaskExecutionContext
            {
                TaskKey = taskKey,
                Workload = taskMessage.Workload,
                ExecutorConfig = taskMessage.ExecutorConfig,
                SharedState = _sharedState
            };

            // Execute task IN-PROCESS
            await ExecuteTaskInProcessAsync(context, cts.Token);

            state = "SUCCESS";
        }
        catch (OperationCanceledException)
        {
            _logger.LogError("Task {TaskKey} timed out", taskKey);
            state = "FAILED";
            errorMessage = $"Task timed out after {_visibilityTimeout.TotalSeconds}s";
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Task {TaskKey} execution failed", taskKey);
            state = "FAILED";
            errorMessage = ex.ToString();
        }

        stopwatch.Stop();
        var executionTime = stopwatch.Elapsed.TotalSeconds;

        // Report result to SQS
        await ReportResultAsync(taskKey, state, executionTime, errorMessage);

        // Delete message from queue
        try
        {
            await _sqsClient.DeleteMessageAsync(_taskQueueUrl, _currentReceiptHandle);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to delete task message");
        }

        _currentReceiptHandle = null;
        _tasksExecuted++;
        _lastTaskTime = DateTime.UtcNow;

        // Force GC after task to help reclaim memory
        GC.Collect(generation: 2, mode: GCCollectionMode.Optimized);

        _logger.LogInformation(
            "Task {TaskKey} completed: {State} ({Time:F2}s)",
            taskKey, state, executionTime);
    }

    private async Task ExecuteTaskInProcessAsync(TaskExecutionContext context, CancellationToken cancellationToken)
    {
        // The workload contains the task definition as JSON
        // For .NET tasks, we deserialize and invoke the task handler

        // Option 1: Load task handler from assembly specified in executor_config
        if (context.ExecutorConfig.TryGetValue("dotnet_handler_assembly", out var assemblyPathObj) &&
            context.ExecutorConfig.TryGetValue("dotnet_handler_type", out var typeNameObj))
        {
            var assemblyPath = assemblyPathObj.ToString()!;
            var typeName = typeNameObj.ToString()!;

            var loadContext = new PluginLoadContext(assemblyPath);
            var assembly = loadContext.LoadFromAssemblyPath(Path.GetFullPath(assemblyPath));
            var handlerType = assembly.GetType(typeName)
                ?? throw new InvalidOperationException($"Handler type {typeName} not found in {assemblyPath}");

            if (!typeof(ITaskHandler).IsAssignableFrom(handlerType))
                throw new InvalidOperationException($"Type {typeName} must implement ITaskHandler");

            var handler = (ITaskHandler)Activator.CreateInstance(handlerType)!;
            await handler.ExecuteAsync(context, cancellationToken);
        }
        // Option 2: Execute a shell command (for hybrid Python/.NET scenarios)
        else if (context.ExecutorConfig.TryGetValue("command", out var commandObj))
        {
            var command = commandObj.ToString()!;

            using var process = new Process
            {
                StartInfo = new ProcessStartInfo
                {
                    FileName = "/bin/bash",
                    Arguments = $"-c \"{command}\"",
                    RedirectStandardOutput = true,
                    RedirectStandardError = true,
                    UseShellExecute = false,
                    CreateNoWindow = true
                }
            };

            process.Start();

            // Wait for process with cancellation support
            try
            {
                await process.WaitForExitAsync(cancellationToken);
            }
            catch (OperationCanceledException)
            {
                process.Kill(entireProcessTree: true);
                throw;
            }

            if (process.ExitCode != 0)
            {
                var stderr = await process.StandardError.ReadToEndAsync(cancellationToken);
                throw new InvalidOperationException(
                    $"Command failed with exit code {process.ExitCode}: {stderr}");
            }
        }
        else
        {
            throw new InvalidOperationException(
                "No task handler configured. Set 'dotnet_handler_assembly' and 'dotnet_handler_type' " +
                "or 'command' in executor_config.");
        }
    }

    private async Task ReportResultAsync(
        TaskInstanceKey taskKey,
        string state,
        double executionTime,
        string? errorMessage)
    {
        var result = new TaskResultMessage
        {
            MessageId = Guid.NewGuid().ToString(),
            TaskKey = taskKey,
            State = state,
            Info = new TaskResultInfo
            {
                WorkerId = _workerId,
                BatchJobId = _batchJobId,
                ExecutionTimeSeconds = executionTime,
                ErrorMessage = errorMessage
            },
            CompletedAt = DateTime.UtcNow
        };

        try
        {
            await _sqsClient.SendMessageAsync(new SendMessageRequest
            {
                QueueUrl = _resultQueueUrl,
                MessageBody = JsonSerializer.Serialize(result),
                MessageAttributes = new Dictionary<string, MessageAttributeValue>
                {
                    ["task_id"] = new() { StringValue = taskKey.ToString(), DataType = "String" },
                    ["state"] = new() { StringValue = state, DataType = "String" }
                }
            });
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to send result to queue");
        }
    }
}
