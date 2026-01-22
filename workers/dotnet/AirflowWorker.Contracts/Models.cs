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
using AirflowWorker.Contracts;

namespace AirflowWorker;

/// <summary>
/// Message received from SQS task queue.
/// Matches Python's TaskQueueMessage schema.
/// </summary>
public record TaskQueueMessage
{
    [JsonPropertyName("message_id")]
    public required string MessageId { get; init; }

    [JsonPropertyName("task_key")]
    public required TaskInstanceKey TaskKey { get; init; }

    /// <summary>
    /// The full Airflow workload as JSON. Contains task details, DAG path, bundle info, etc.
    /// </summary>
    [JsonPropertyName("workload_json")]
    public required string WorkloadJson { get; init; }

    /// <summary>
    /// Parsed workload JSON element for easy access to properties.
    /// </summary>
    [JsonIgnore]
    public JsonElement Workload =>
        JsonSerializer.Deserialize<JsonElement>(WorkloadJson);

    /// <summary>
    /// Parsed ExecuteTask workload with typed access to token and task instance.
    /// </summary>
    [JsonIgnore]
    public ExecuteTaskWorkload? ExecuteTaskWorkload =>
        JsonSerializer.Deserialize<ExecuteTaskWorkload>(WorkloadJson);

    /// <summary>
    /// Executor configuration from DAG definition.
    /// </summary>
    [JsonPropertyName("executor_config")]
    public Dictionary<string, object> ExecutorConfig { get; init; } = new();

    [JsonPropertyName("enqueued_at")]
    public DateTime EnqueuedAt { get; init; }

    /// <summary>
    /// URL of the Airflow Execution API for transitioning task state.
    /// Used by native workers to call PATCH /execution/task-instances/{id}/run.
    /// </summary>
    [JsonPropertyName("execution_api_url")]
    public string? ExecutionApiUrl { get; init; }
}

/// <summary>
/// Metadata about task execution result.
/// </summary>
public record TaskResultInfo
{
    [JsonPropertyName("worker_id")]
    public required string WorkerId { get; init; }

    [JsonPropertyName("batch_job_id")]
    public required string BatchJobId { get; init; }

    [JsonPropertyName("execution_time_seconds")]
    public required double ExecutionTimeSeconds { get; init; }

    [JsonPropertyName("error_message")]
    public string? ErrorMessage { get; init; }
}

/// <summary>
/// Message sent to SQS result queue after task execution.
/// Matches Python's TaskResultMessage schema.
/// </summary>
public record TaskResultMessage
{
    [JsonPropertyName("message_id")]
    public required string MessageId { get; init; }

    [JsonPropertyName("task_key")]
    public required TaskInstanceKey TaskKey { get; init; }

    /// <summary>
    /// Task completion state: "SUCCESS" or "FAILED"
    /// </summary>
    [JsonPropertyName("state")]
    public required string State { get; init; }

    [JsonPropertyName("info")]
    public required TaskResultInfo Info { get; init; }

    [JsonPropertyName("completed_at")]
    public DateTime CompletedAt { get; init; }
}

/// <summary>
/// Task instance details from the workload.
/// Matches Python's TaskInstance schema in workloads.py.
/// </summary>
public record WorkloadTaskInstance
{
    /// <summary>
    /// The unique UUID for this task instance. Used for Execution API calls.
    /// </summary>
    [JsonPropertyName("id")]
    public required Guid Id { get; init; }

    [JsonPropertyName("dag_version_id")]
    public Guid DagVersionId { get; init; }

    [JsonPropertyName("task_id")]
    public required string TaskId { get; init; }

    [JsonPropertyName("dag_id")]
    public required string DagId { get; init; }

    [JsonPropertyName("run_id")]
    public required string RunId { get; init; }

    [JsonPropertyName("try_number")]
    public int TryNumber { get; init; }

    [JsonPropertyName("map_index")]
    public int MapIndex { get; init; } = -1;

    [JsonPropertyName("pool_slots")]
    public int PoolSlots { get; init; }

    [JsonPropertyName("queue")]
    public string? Queue { get; init; }

    [JsonPropertyName("priority_weight")]
    public int PriorityWeight { get; init; }
}

/// <summary>
/// Bundle information for DAG versioning.
/// </summary>
public record BundleInfo
{
    [JsonPropertyName("name")]
    public required string Name { get; init; }

    [JsonPropertyName("version")]
    public string? Version { get; init; }
}

/// <summary>
/// The ExecuteTask workload from Airflow.
/// Matches Python's ExecuteTask schema in workloads.py.
/// </summary>
public record ExecuteTaskWorkload
{
    /// <summary>
    /// JWT token for authenticating with the Airflow Execution API.
    /// </summary>
    [JsonPropertyName("token")]
    public required string Token { get; init; }

    /// <summary>
    /// Task instance details including the UUID needed for API calls.
    /// </summary>
    [JsonPropertyName("ti")]
    public required WorkloadTaskInstance Ti { get; init; }

    [JsonPropertyName("dag_rel_path")]
    public string? DagRelPath { get; init; }

    [JsonPropertyName("bundle_info")]
    public BundleInfo? BundleInfo { get; init; }

    [JsonPropertyName("log_path")]
    public string? LogPath { get; init; }

    [JsonPropertyName("sentry_integration")]
    public string? SentryIntegration { get; init; }

    [JsonPropertyName("type")]
    public string Type { get; init; } = "ExecuteTask";
}

/// <summary>
/// Payload for the PATCH /execution/task-instances/{id}/run API call.
/// </summary>
public record TIEnterRunningPayload
{
    [JsonPropertyName("hostname")]
    public required string Hostname { get; init; }

    [JsonPropertyName("pid")]
    public required int Pid { get; init; }

    [JsonPropertyName("unixname")]
    public required string Unixname { get; init; }

    [JsonPropertyName("start_date")]
    public required DateTime StartDate { get; init; }
}

/// <summary>
/// Response from the PATCH /execution/task-instances/{id}/run API call.
/// Contains context needed for task execution.
/// </summary>
public record TIRunContext
{
    [JsonPropertyName("dag_run")]
    public JsonElement? DagRun { get; init; }

    [JsonPropertyName("max_tries")]
    public int MaxTries { get; init; }

    [JsonPropertyName("variables")]
    public JsonElement? Variables { get; init; }

    [JsonPropertyName("connections")]
    public JsonElement? Connections { get; init; }
}
