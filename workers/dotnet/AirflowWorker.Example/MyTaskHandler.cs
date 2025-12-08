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

using System.Net.Http.Json;
using System.Text.Json;
using AirflowWorker.Contracts;

namespace AirflowWorker.Example;

/// <summary>
/// Example task handler that demonstrates how to use shared state
/// and process task parameters from the Airflow workload.
///
/// This handler:
/// 1. Accesses the pre-initialized shared state (HTTP client, reference data)
/// 2. Extracts parameters from the task workload
/// 3. Performs some processing
/// 4. Optionally reports results to an external API
/// </summary>
public class ExampleTaskHandler : ITaskHandler
{
    public async Task ExecuteAsync(TaskExecutionContext context, CancellationToken cancellationToken)
    {
        Console.WriteLine($"[ExampleTaskHandler] Starting task {context.TaskKey}");

        // Access shared state (no initialization delay!)
        var sharedState = context.SharedState as MySharedState;
        if (sharedState == null)
        {
            Console.WriteLine("[ExampleTaskHandler] Warning: Shared state not available, proceeding without it");
        }
        else
        {
            Console.WriteLine($"[ExampleTaskHandler] Using shared state initialized at {sharedState.InitializedAt}");
            Console.WriteLine($"[ExampleTaskHandler] Reference data: {JsonSerializer.Serialize(sharedState.ReferenceData)}");
        }

        // Extract parameters from executor_config
        var parameters = new Dictionary<string, string>();
        if (context.ExecutorConfig.TryGetValue("parameters", out var paramsObj))
        {
            if (paramsObj is JsonElement jsonElement)
            {
                foreach (var prop in jsonElement.EnumerateObject())
                {
                    parameters[prop.Name] = prop.Value.GetString() ?? "";
                }
            }
        }

        Console.WriteLine($"[ExampleTaskHandler] Task parameters: {JsonSerializer.Serialize(parameters)}");

        // Simulate task processing
        var inputData = parameters.GetValueOrDefault("input", "default-input");
        Console.WriteLine($"[ExampleTaskHandler] Processing input: {inputData}");

        // Simulate some work
        await Task.Delay(100, cancellationToken);

        var result = new
        {
            task_id = context.TaskKey.ToString(),
            input = inputData,
            processed = true,
            processed_at = DateTime.UtcNow,
            environment = sharedState?.ReferenceData.GetValueOrDefault("environment", "unknown")
        };

        Console.WriteLine($"[ExampleTaskHandler] Result: {JsonSerializer.Serialize(result)}");

        // Optionally post results to external API using pre-configured HTTP client
        if (sharedState?.HttpClient != null && parameters.ContainsKey("report_endpoint"))
        {
            try
            {
                var endpoint = parameters["report_endpoint"];
                Console.WriteLine($"[ExampleTaskHandler] Posting result to {endpoint}");

                var response = await sharedState.HttpClient.PostAsJsonAsync(
                    endpoint,
                    result,
                    cancellationToken);

                if (response.IsSuccessStatusCode)
                {
                    Console.WriteLine("[ExampleTaskHandler] Result posted successfully");
                }
                else
                {
                    Console.WriteLine($"[ExampleTaskHandler] Failed to post result: {response.StatusCode}");
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[ExampleTaskHandler] Error posting result: {ex.Message}");
                // Don't fail the task for reporting errors - the work is done
            }
        }

        Console.WriteLine($"[ExampleTaskHandler] Task {context.TaskKey} completed successfully");
    }
}

/// <summary>
/// Another example handler showing data transformation pattern.
/// </summary>
public class DataTransformHandler : ITaskHandler
{
    public async Task ExecuteAsync(TaskExecutionContext context, CancellationToken cancellationToken)
    {
        Console.WriteLine($"[DataTransformHandler] Starting transform for {context.TaskKey}");

        // Extract source and destination from executor_config
        var sourcePath = GetConfigValue(context, "source_path")
            ?? throw new ArgumentException("source_path is required");
        var destPath = GetConfigValue(context, "dest_path")
            ?? throw new ArgumentException("dest_path is required");
        var transformType = GetConfigValue(context, "transform_type") ?? "default";

        Console.WriteLine($"[DataTransformHandler] Transform: {sourcePath} -> {destPath} (type: {transformType})");

        // Simulate reading source data
        if (!File.Exists(sourcePath))
        {
            throw new FileNotFoundException($"Source file not found: {sourcePath}");
        }

        var sourceData = await File.ReadAllTextAsync(sourcePath, cancellationToken);
        Console.WriteLine($"[DataTransformHandler] Read {sourceData.Length} bytes from source");

        // Apply transformation based on type
        var transformedData = transformType switch
        {
            "uppercase" => sourceData.ToUpperInvariant(),
            "lowercase" => sourceData.ToLowerInvariant(),
            "reverse" => new string(sourceData.Reverse().ToArray()),
            _ => sourceData // No transform
        };

        // Write to destination
        var destDir = Path.GetDirectoryName(destPath);
        if (!string.IsNullOrEmpty(destDir))
        {
            Directory.CreateDirectory(destDir);
        }

        await File.WriteAllTextAsync(destPath, transformedData, cancellationToken);
        Console.WriteLine($"[DataTransformHandler] Wrote {transformedData.Length} bytes to destination");

        Console.WriteLine($"[DataTransformHandler] Transform completed for {context.TaskKey}");
    }

    private static string? GetConfigValue(TaskExecutionContext context, string key)
    {
        if (context.ExecutorConfig.TryGetValue(key, out var value))
        {
            return value?.ToString();
        }
        return null;
    }
}
