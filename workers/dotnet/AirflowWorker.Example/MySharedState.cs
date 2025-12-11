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

namespace AirflowWorker.Example;

/// <summary>
/// Example shared state implementation that demonstrates how to initialize
/// expensive resources once and share them across multiple task executions.
///
/// When shared_state_s3_uri is configured, the worker downloads files from S3
/// to a local temp directory before calling InitializeAsync(). The local path
/// is available via AIRFLOW_SHARED_STATE_LOCAL_PATH environment variable.
///
/// In a real scenario, you might:
/// - Load ML models (ML.NET, ONNX, TensorFlow) from S3-downloaded files
/// - Initialize database connection pools (cannot be serialized)
/// - Load reference data from S3-downloaded JSON files
/// - Create HTTP clients with connection pooling (cannot be serialized)
/// </summary>
public class MySharedState : ISharedState
{
    /// <summary>
    /// HTTP client configured with connection pooling for external API calls.
    /// Note: Cannot be serialized - must be created in InitializeAsync.
    /// </summary>
    public HttpClient HttpClient { get; private set; } = null!;

    /// <summary>
    /// Reference data that can be loaded from S3-downloaded JSON file.
    /// </summary>
    public Dictionary<string, string> ReferenceData { get; private set; } = null!;

    /// <summary>
    /// Timestamp when initialization completed.
    /// </summary>
    public DateTime InitializedAt { get; private set; }

    /// <summary>
    /// Path to S3-downloaded shared state files (set by worker before calling InitializeAsync).
    /// </summary>
    public string? SharedStatePath { get; private set; }

    public async Task InitializeAsync()
    {
        Console.WriteLine("[MySharedState] Initializing shared resources...");

        // Get path to S3-downloaded files (set by worker after downloading from S3)
        SharedStatePath = Environment.GetEnvironmentVariable("AIRFLOW_SHARED_STATE_LOCAL_PATH");

        if (!string.IsNullOrEmpty(SharedStatePath))
        {
            Console.WriteLine($"[MySharedState] Loading from S3-downloaded files at: {SharedStatePath}");
        }

        // Create reusable HTTP client with connection pooling
        // Note: HttpClient cannot be serialized - must be created here
        HttpClient = new HttpClient
        {
            BaseAddress = new Uri(
                Environment.GetEnvironmentVariable("API_BASE_URL")
                ?? "https://api.example.com"),
            Timeout = TimeSpan.FromSeconds(30)
        };

        // Load reference data from S3-downloaded file or create default
        ReferenceData = await LoadReferenceDataAsync();

        InitializedAt = DateTime.UtcNow;

        Console.WriteLine($"[MySharedState] Initialized successfully at {InitializedAt}");
        Console.WriteLine($"[MySharedState] Reference data keys: {string.Join(", ", ReferenceData.Keys)}");

        // Example: If using ML.NET, load model from S3-downloaded file:
        // var modelPath = !string.IsNullOrEmpty(SharedStatePath)
        //     ? Path.Combine(SharedStatePath, "model.zip")
        //     : Environment.GetEnvironmentVariable("MODEL_PATH") ?? "/models/model.zip";
        // var mlContext = new MLContext();
        // Model = mlContext.Model.Load(modelPath, out _);
    }

    private async Task<Dictionary<string, string>> LoadReferenceDataAsync()
    {
        // Try to load from S3-downloaded file first
        if (!string.IsNullOrEmpty(SharedStatePath))
        {
            var refDataPath = Path.Combine(SharedStatePath, "reference-data.json");
            if (File.Exists(refDataPath))
            {
                Console.WriteLine($"[MySharedState] Loading reference data from: {refDataPath}");
                var json = await File.ReadAllTextAsync(refDataPath);
                var data = JsonSerializer.Deserialize<Dictionary<string, string>>(json);
                if (data != null)
                {
                    Console.WriteLine($"[MySharedState] Loaded {data.Count} reference data entries from S3");
                    return data;
                }
            }
        }

        // Fallback to default reference data
        Console.WriteLine("[MySharedState] Using default reference data");
        return new Dictionary<string, string>
        {
            ["config_version"] = "1.0",
            ["model_name"] = "example-model",
            ["environment"] = Environment.GetEnvironmentVariable("ENVIRONMENT") ?? "development"
        };
    }

    public async Task DisposeAsync()
    {
        Console.WriteLine("[MySharedState] Disposing shared resources...");

        HttpClient?.Dispose();
        ReferenceData?.Clear();

        await Task.CompletedTask;

        Console.WriteLine("[MySharedState] Disposed successfully");
    }
}
