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
using System.Text.Json.Serialization;
using AirflowWorker;

namespace AirflowWorker.Contracts;

/// <summary>
/// Unique identifier for a task instance, matching Airflow's TaskInstanceKey.
/// </summary>
public record TaskInstanceKey
{
    [JsonPropertyName("dag_id")]
    public required string DagId { get; init; }

    [JsonPropertyName("task_id")]
    public required string TaskId { get; init; }

    [JsonPropertyName("run_id")]
    public required string RunId { get; init; }

    [JsonPropertyName("try_number")]
    public required int TryNumber { get; init; }

    [JsonPropertyName("map_index")]
    public int MapIndex { get; init; } = -1;

    public override string ToString() =>
        $"{DagId}.{TaskId}[{RunId}]#{TryNumber}" + (MapIndex >= 0 ? $"[{MapIndex}]" : "");
}

/// <summary>
/// Interface for shared state that persists across task executions.
/// Implement this to initialize expensive resources once (ML models, DB pools, etc.)
///
/// Example implementation:
/// <code>
/// public class MySharedState : ISharedState
/// {
///     public MLContext MlContext { get; private set; }
///     public ITransformer Model { get; private set; }
///
///     public async Task InitializeAsync()
///     {
///         MlContext = new MLContext();
///         Model = MlContext.Model.Load("model.zip", out _);
///     }
///
///     public async Task DisposeAsync()
///     {
///         // Cleanup resources
///     }
/// }
/// </code>
/// </summary>
public interface ISharedState
{
    /// <summary>
    /// Called once when the worker starts. Initialize expensive resources here.
    /// </summary>
    Task InitializeAsync();

    /// <summary>
    /// Called when the worker shuts down. Clean up resources here.
    /// </summary>
    Task DisposeAsync();
}

/// <summary>
/// Interface for task handlers. Implement this to define how tasks execute.
///
/// Example implementation:
/// <code>
/// public class MyTaskHandler : ITaskHandler
/// {
///     public async Task ExecuteAsync(TaskExecutionContext context, CancellationToken ct)
///     {
///         var sharedState = context.SharedState as MySharedState;
///         var parameters = context.Workload.GetProperty("parameters");
///
///         // Use shared state and parameters to execute task
///     }
/// }
/// </code>
/// </summary>
public interface ITaskHandler
{
    /// <summary>
    /// Execute the task with access to shared state.
    /// </summary>
    /// <param name="context">Context containing task info, workload, and shared state.</param>
    /// <param name="cancellationToken">Token that will be cancelled if task times out.</param>
    Task ExecuteAsync(TaskExecutionContext context, CancellationToken cancellationToken);
}

/// <summary>
/// Context passed to task handlers during execution.
/// </summary>
public record TaskExecutionContext
{
    /// <summary>
    /// The unique key identifying this task instance.
    /// </summary>
    public required TaskInstanceKey TaskKey { get; init; }

    /// <summary>
    /// The workload JSON from Airflow containing task details.
    /// </summary>
    public required JsonElement Workload { get; init; }

    /// <summary>
    /// Executor configuration from the DAG's executor_config parameter.
    /// Use this to pass handler-specific settings.
    /// </summary>
    public required Dictionary<string, object> ExecutorConfig { get; init; }

    /// <summary>
    /// Shared state initialized at worker startup. May be null if no init assembly configured.
    /// Cast to your concrete shared state type to access resources.
    /// </summary>
    public ISharedState? SharedState { get; init; }
}

/// <summary>
/// Interface for communicating with the Airflow Execution API.
/// The Execution API is used to transition task states and report progress.
/// </summary>
public interface IAirflowExecutionApi
{
    /// <summary>
    /// Transition a task instance from QUEUED to RUNNING state.
    /// Must be called before executing the task.
    /// </summary>
    /// <param name="taskInstanceId">The UUID of the task instance (from workload.ti.id)</param>
    /// <param name="token">JWT authentication token (from workload.token)</param>
    /// <param name="hostname">The hostname of the worker</param>
    /// <param name="pid">The process ID of the worker</param>
    /// <param name="unixname">The unix username running the worker</param>
    /// <param name="cancellationToken">Cancellation token</param>
    /// <returns>Run context with additional task information, or null if the call failed</returns>
    Task<TIRunContext?> StartTaskAsync(
        Guid taskInstanceId,
        string token,
        string hostname,
        int pid,
        string unixname,
        CancellationToken cancellationToken = default);
}
