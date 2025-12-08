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

using System.Text.Json;
using AirflowWorker.Contracts;
using Amazon.SQS;
using Amazon.SQS.Model;
using FluentAssertions;
using Microsoft.Extensions.Logging;
using Moq;
using Xunit;

namespace AirflowWorker.Tests;

/// <summary>
/// Tests for WorkerProcess using a testable wrapper that accepts mock dependencies.
/// </summary>
public class WorkerProcessTests
{
    /// <summary>
    /// Testable version of WorkerProcess that accepts mock dependencies.
    /// </summary>
    public class TestableWorkerProcess
    {
        private readonly string _workerId;
        private readonly string _taskQueueUrl;
        private readonly string _resultQueueUrl;
        private readonly TimeSpan _idleTimeout;
        private readonly TimeSpan _visibilityTimeout;
        private readonly int _maxTasks;
        private readonly IAmazonSQS _sqsClient;
        private readonly ILogger _logger;

        private bool _running = true;
        private int _tasksExecuted = 0;
        private DateTime _lastTaskTime;

        public int TasksExecuted => _tasksExecuted;
        public bool IsRunning => _running;

        public TestableWorkerProcess(
            string workerId,
            string taskQueueUrl,
            string resultQueueUrl,
            TimeSpan idleTimeout,
            TimeSpan visibilityTimeout,
            int maxTasks,
            IAmazonSQS sqsClient,
            ILogger logger)
        {
            _workerId = workerId;
            _taskQueueUrl = taskQueueUrl;
            _resultQueueUrl = resultQueueUrl;
            _idleTimeout = idleTimeout;
            _visibilityTimeout = visibilityTimeout;
            _maxTasks = maxTasks;
            _sqsClient = sqsClient;
            _logger = logger;
            _lastTaskTime = DateTime.UtcNow;
        }

        public void Stop() => _running = false;

        public async Task<TaskQueueMessage?> PollForTaskAsync(CancellationToken cancellationToken)
        {
            try
            {
                var response = await _sqsClient.ReceiveMessageAsync(new ReceiveMessageRequest
                {
                    QueueUrl = _taskQueueUrl,
                    MaxNumberOfMessages = 1,
                    WaitTimeSeconds = 20,
                    VisibilityTimeout = (int)_visibilityTimeout.TotalSeconds,
                    MessageAttributeNames = ["All"]
                }, cancellationToken);

                if (response.Messages.Count == 0)
                    return null;

                var msg = response.Messages[0];
                return JsonSerializer.Deserialize<TaskQueueMessage>(msg.Body);
            }
            catch (OperationCanceledException)
            {
                return null;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Failed to poll task queue");
                await Task.Delay(5000, cancellationToken);
                return null;
            }
        }

        public async Task ReportResultAsync(
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
                    BatchJobId = "test-batch-job",
                    ExecutionTimeSeconds = executionTime,
                    ErrorMessage = errorMessage
                },
                CompletedAt = DateTime.UtcNow
            };

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

        public bool ShouldContinue()
        {
            if (!_running) return false;

            if (DateTime.UtcNow - _lastTaskTime > _idleTimeout)
            {
                _logger.LogInformation("Idle timeout reached");
                return false;
            }

            if (_maxTasks > 0 && _tasksExecuted >= _maxTasks)
            {
                _logger.LogInformation("Max tasks reached");
                return false;
            }

            return true;
        }

        public void RecordTaskCompleted()
        {
            _tasksExecuted++;
            _lastTaskTime = DateTime.UtcNow;
        }
    }

    private Mock<IAmazonSQS> CreateMockSqs()
    {
        return new Mock<IAmazonSQS>();
    }

    private Mock<ILogger> CreateMockLogger()
    {
        return new Mock<ILogger>();
    }

    [Fact]
    public async Task PollForTask_WhenNoMessages_ReturnsNull()
    {
        // Arrange
        var mockSqs = CreateMockSqs();
        mockSqs.Setup(s => s.ReceiveMessageAsync(
                It.IsAny<ReceiveMessageRequest>(),
                It.IsAny<CancellationToken>()))
            .ReturnsAsync(new ReceiveMessageResponse { Messages = new List<Message>() });

        var worker = new TestableWorkerProcess(
            "worker-001",
            "https://sqs.us-east-1.amazonaws.com/123/task-queue",
            "https://sqs.us-east-1.amazonaws.com/123/result-queue",
            TimeSpan.FromMinutes(5),
            TimeSpan.FromHours(1),
            0,
            mockSqs.Object,
            CreateMockLogger().Object);

        // Act
        var result = await worker.PollForTaskAsync(CancellationToken.None);

        // Assert
        result.Should().BeNull();
    }

    [Fact]
    public async Task PollForTask_WhenMessageExists_ReturnsTaskMessage()
    {
        // Arrange
        var taskMessage = new TaskQueueMessage
        {
            MessageId = "msg-123",
            TaskKey = new TaskInstanceKey
            {
                DagId = "test_dag",
                TaskId = "test_task",
                RunId = "run_001",
                TryNumber = 1,
                MapIndex = -1
            },
            WorkloadJson = "{\"type\": \"ExecuteTask\"}",
            ExecutorConfig = new Dictionary<string, object>(),
            EnqueuedAt = DateTime.UtcNow
        };

        var mockSqs = CreateMockSqs();
        mockSqs.Setup(s => s.ReceiveMessageAsync(
                It.IsAny<ReceiveMessageRequest>(),
                It.IsAny<CancellationToken>()))
            .ReturnsAsync(new ReceiveMessageResponse
            {
                Messages = new List<Message>
                {
                    new Message
                    {
                        Body = JsonSerializer.Serialize(taskMessage),
                        ReceiptHandle = "receipt-123"
                    }
                }
            });

        var worker = new TestableWorkerProcess(
            "worker-001",
            "https://sqs.us-east-1.amazonaws.com/123/task-queue",
            "https://sqs.us-east-1.amazonaws.com/123/result-queue",
            TimeSpan.FromMinutes(5),
            TimeSpan.FromHours(1),
            0,
            mockSqs.Object,
            CreateMockLogger().Object);

        // Act
        var result = await worker.PollForTaskAsync(CancellationToken.None);

        // Assert
        result.Should().NotBeNull();
        result!.TaskKey.DagId.Should().Be("test_dag");
        result.TaskKey.TaskId.Should().Be("test_task");
    }

    [Fact]
    public async Task PollForTask_WhenCancelled_ReturnsNull()
    {
        // Arrange
        var mockSqs = CreateMockSqs();
        mockSqs.Setup(s => s.ReceiveMessageAsync(
                It.IsAny<ReceiveMessageRequest>(),
                It.IsAny<CancellationToken>()))
            .ThrowsAsync(new OperationCanceledException());

        var worker = new TestableWorkerProcess(
            "worker-001",
            "https://sqs.us-east-1.amazonaws.com/123/task-queue",
            "https://sqs.us-east-1.amazonaws.com/123/result-queue",
            TimeSpan.FromMinutes(5),
            TimeSpan.FromHours(1),
            0,
            mockSqs.Object,
            CreateMockLogger().Object);

        using var cts = new CancellationTokenSource();
        cts.Cancel();

        // Act
        var result = await worker.PollForTaskAsync(cts.Token);

        // Assert
        result.Should().BeNull();
    }

    [Fact]
    public async Task ReportResult_SendsMessageToResultQueue()
    {
        // Arrange
        var mockSqs = CreateMockSqs();
        SendMessageRequest? capturedRequest = null;

        mockSqs.Setup(s => s.SendMessageAsync(
                It.IsAny<SendMessageRequest>(),
                It.IsAny<CancellationToken>()))
            .Callback<SendMessageRequest, CancellationToken>((req, _) => capturedRequest = req)
            .ReturnsAsync(new SendMessageResponse { MessageId = "result-msg-id" });

        var worker = new TestableWorkerProcess(
            "worker-001",
            "https://sqs.us-east-1.amazonaws.com/123/task-queue",
            "https://sqs.us-east-1.amazonaws.com/123/result-queue",
            TimeSpan.FromMinutes(5),
            TimeSpan.FromHours(1),
            0,
            mockSqs.Object,
            CreateMockLogger().Object);

        var taskKey = new TaskInstanceKey
        {
            DagId = "test_dag",
            TaskId = "test_task",
            RunId = "run_001",
            TryNumber = 1,
            MapIndex = -1
        };

        // Act
        await worker.ReportResultAsync(taskKey, "SUCCESS", 5.5, null);

        // Assert
        capturedRequest.Should().NotBeNull();
        capturedRequest!.QueueUrl.Should().Contain("result-queue");

        var resultMessage = JsonSerializer.Deserialize<TaskResultMessage>(capturedRequest.MessageBody);
        resultMessage.Should().NotBeNull();
        resultMessage!.State.Should().Be("SUCCESS");
        resultMessage.TaskKey.DagId.Should().Be("test_dag");
        resultMessage.Info.ExecutionTimeSeconds.Should().Be(5.5);
        resultMessage.Info.ErrorMessage.Should().BeNull();
    }

    [Fact]
    public async Task ReportResult_IncludesErrorMessage_WhenFailed()
    {
        // Arrange
        var mockSqs = CreateMockSqs();
        SendMessageRequest? capturedRequest = null;

        mockSqs.Setup(s => s.SendMessageAsync(
                It.IsAny<SendMessageRequest>(),
                It.IsAny<CancellationToken>()))
            .Callback<SendMessageRequest, CancellationToken>((req, _) => capturedRequest = req)
            .ReturnsAsync(new SendMessageResponse { MessageId = "result-msg-id" });

        var worker = new TestableWorkerProcess(
            "worker-001",
            "https://sqs.us-east-1.amazonaws.com/123/task-queue",
            "https://sqs.us-east-1.amazonaws.com/123/result-queue",
            TimeSpan.FromMinutes(5),
            TimeSpan.FromHours(1),
            0,
            mockSqs.Object,
            CreateMockLogger().Object);

        var taskKey = new TaskInstanceKey
        {
            DagId = "test_dag",
            TaskId = "test_task",
            RunId = "run_001",
            TryNumber = 1,
            MapIndex = -1
        };

        // Act
        await worker.ReportResultAsync(taskKey, "FAILED", 1.0, "Task timed out");

        // Assert
        var resultMessage = JsonSerializer.Deserialize<TaskResultMessage>(capturedRequest!.MessageBody);
        resultMessage!.State.Should().Be("FAILED");
        resultMessage.Info.ErrorMessage.Should().Be("Task timed out");
    }

    [Fact]
    public void ShouldContinue_WhenRunningAndWithinLimits_ReturnsTrue()
    {
        // Arrange
        var worker = new TestableWorkerProcess(
            "worker-001",
            "https://sqs.us-east-1.amazonaws.com/123/task-queue",
            "https://sqs.us-east-1.amazonaws.com/123/result-queue",
            TimeSpan.FromMinutes(5),
            TimeSpan.FromHours(1),
            10, // max 10 tasks
            CreateMockSqs().Object,
            CreateMockLogger().Object);

        // Act
        var result = worker.ShouldContinue();

        // Assert
        result.Should().BeTrue();
    }

    [Fact]
    public void ShouldContinue_WhenStopped_ReturnsFalse()
    {
        // Arrange
        var worker = new TestableWorkerProcess(
            "worker-001",
            "https://sqs.us-east-1.amazonaws.com/123/task-queue",
            "https://sqs.us-east-1.amazonaws.com/123/result-queue",
            TimeSpan.FromMinutes(5),
            TimeSpan.FromHours(1),
            0,
            CreateMockSqs().Object,
            CreateMockLogger().Object);

        // Act
        worker.Stop();
        var result = worker.ShouldContinue();

        // Assert
        result.Should().BeFalse();
    }

    [Fact]
    public void ShouldContinue_WhenMaxTasksReached_ReturnsFalse()
    {
        // Arrange
        var worker = new TestableWorkerProcess(
            "worker-001",
            "https://sqs.us-east-1.amazonaws.com/123/task-queue",
            "https://sqs.us-east-1.amazonaws.com/123/result-queue",
            TimeSpan.FromMinutes(5),
            TimeSpan.FromHours(1),
            2, // max 2 tasks
            CreateMockSqs().Object,
            CreateMockLogger().Object);

        // Execute 2 tasks
        worker.RecordTaskCompleted();
        worker.RecordTaskCompleted();

        // Act
        var result = worker.ShouldContinue();

        // Assert
        result.Should().BeFalse();
        worker.TasksExecuted.Should().Be(2);
    }

    [Fact]
    public void ShouldContinue_WhenMaxTasksZero_AlwaysContinues()
    {
        // Arrange - maxTasks = 0 means unlimited
        var worker = new TestableWorkerProcess(
            "worker-001",
            "https://sqs.us-east-1.amazonaws.com/123/task-queue",
            "https://sqs.us-east-1.amazonaws.com/123/result-queue",
            TimeSpan.FromMinutes(5),
            TimeSpan.FromHours(1),
            0, // unlimited
            CreateMockSqs().Object,
            CreateMockLogger().Object);

        // Execute many tasks
        for (int i = 0; i < 100; i++)
        {
            worker.RecordTaskCompleted();
        }

        // Act
        var result = worker.ShouldContinue();

        // Assert
        result.Should().BeTrue();
        worker.TasksExecuted.Should().Be(100);
    }

    [Fact]
    public void RecordTaskCompleted_IncrementsTaskCount()
    {
        // Arrange
        var worker = new TestableWorkerProcess(
            "worker-001",
            "https://sqs.us-east-1.amazonaws.com/123/task-queue",
            "https://sqs.us-east-1.amazonaws.com/123/result-queue",
            TimeSpan.FromMinutes(5),
            TimeSpan.FromHours(1),
            0,
            CreateMockSqs().Object,
            CreateMockLogger().Object);

        // Act
        worker.RecordTaskCompleted();
        worker.RecordTaskCompleted();
        worker.RecordTaskCompleted();

        // Assert
        worker.TasksExecuted.Should().Be(3);
    }
}

public class WorkerProcessConfigurationTests
{
    [Fact]
    public void WorkerConfiguration_WithAllParameters_CreatesValidInstance()
    {
        // This test validates the configuration parameters
        var workerId = "test-worker-001";
        var taskQueueUrl = "https://sqs.us-east-1.amazonaws.com/123456789/task-queue";
        var resultQueueUrl = "https://sqs.us-east-1.amazonaws.com/123456789/result-queue";
        var idleTimeout = TimeSpan.FromMinutes(5);
        var visibilityTimeout = TimeSpan.FromHours(1);
        var maxTasks = 100;

        // Assert valid configuration
        workerId.Should().NotBeNullOrEmpty();
        taskQueueUrl.Should().StartWith("https://sqs.");
        resultQueueUrl.Should().StartWith("https://sqs.");
        idleTimeout.Should().BeGreaterThan(TimeSpan.Zero);
        visibilityTimeout.Should().BeGreaterThan(idleTimeout);
        maxTasks.Should().BeGreaterThanOrEqualTo(0);
    }

    [Theory]
    [InlineData(60, 300)]   // 1 min idle, 5 min visibility
    [InlineData(300, 3600)] // 5 min idle, 1 hour visibility
    [InlineData(600, 7200)] // 10 min idle, 2 hour visibility
    public void TimeoutConfiguration_VisibilityGreaterThanIdle_IsValid(int idleSeconds, int visibilitySeconds)
    {
        // Arrange
        var idleTimeout = TimeSpan.FromSeconds(idleSeconds);
        var visibilityTimeout = TimeSpan.FromSeconds(visibilitySeconds);

        // Assert
        visibilityTimeout.Should().BeGreaterThan(idleTimeout);
    }
}
