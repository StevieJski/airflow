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

namespace AirflowWorker;

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
    /// Executor configuration from DAG definition.
    /// </summary>
    [JsonPropertyName("executor_config")]
    public Dictionary<string, object> ExecutorConfig { get; init; } = new();

    [JsonPropertyName("enqueued_at")]
    public DateTime EnqueuedAt { get; init; }
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
