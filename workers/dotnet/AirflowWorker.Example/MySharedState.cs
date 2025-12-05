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

namespace AirflowWorker.Example;

/// <summary>
/// Example shared state implementation that demonstrates how to initialize
/// expensive resources once and share them across multiple task executions.
///
/// In a real scenario, you might:
/// - Load ML models (ML.NET, ONNX, TensorFlow)
/// - Initialize database connection pools
/// - Load reference data into memory
/// - Create HTTP clients with connection pooling
/// </summary>
public class MySharedState : ISharedState
{
    /// <summary>
    /// HTTP client configured with connection pooling for external API calls.
    /// </summary>
    public HttpClient HttpClient { get; private set; } = null!;

    /// <summary>
    /// Example: Cached reference data that would be expensive to reload per-task.
    /// </summary>
    public Dictionary<string, string> ReferenceData { get; private set; } = null!;

    /// <summary>
    /// Timestamp when initialization completed.
    /// </summary>
    public DateTime InitializedAt { get; private set; }

    public async Task InitializeAsync()
    {
        Console.WriteLine("[MySharedState] Initializing shared resources...");

        // Create reusable HTTP client with connection pooling
        // This avoids TCP connection overhead for each request
        HttpClient = new HttpClient
        {
            BaseAddress = new Uri(
                Environment.GetEnvironmentVariable("API_BASE_URL")
                ?? "https://api.example.com"),
            Timeout = TimeSpan.FromSeconds(30)
        };

        // Simulate loading expensive reference data
        // In reality, this might load from S3, a database, or a model file
        await Task.Delay(100); // Simulate I/O delay
        ReferenceData = new Dictionary<string, string>
        {
            ["config_version"] = "1.0",
            ["model_name"] = "example-model",
            ["environment"] = Environment.GetEnvironmentVariable("ENVIRONMENT") ?? "development"
        };

        InitializedAt = DateTime.UtcNow;

        Console.WriteLine($"[MySharedState] Initialized successfully at {InitializedAt}");
        Console.WriteLine($"[MySharedState] Reference data keys: {string.Join(", ", ReferenceData.Keys)}");

        // Example: If using ML.NET, you would load the model here:
        // var mlContext = new MLContext();
        // var modelPath = Environment.GetEnvironmentVariable("MODEL_PATH") ?? "/models/model.zip";
        // Model = mlContext.Model.Load(modelPath, out _);
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
