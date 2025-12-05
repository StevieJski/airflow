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
using FluentAssertions;
using Xunit;

namespace AirflowWorker.Tests;

public class TaskInstanceKeyTests
{
    [Fact]
    public void ToString_WithoutMapIndex_ReturnsExpectedFormat()
    {
        // Arrange
        var taskKey = new TaskInstanceKey
        {
            DagId = "test_dag",
            TaskId = "test_task",
            RunId = "run_123",
            TryNumber = 1,
            MapIndex = -1
        };

        // Act
        var result = taskKey.ToString();

        // Assert
        result.Should().Be("test_dag.test_task[run_123]#1");
    }

    [Fact]
    public void ToString_WithMapIndex_IncludesMapIndex()
    {
        // Arrange
        var taskKey = new TaskInstanceKey
        {
            DagId = "test_dag",
            TaskId = "test_task",
            RunId = "run_123",
            TryNumber = 2,
            MapIndex = 5
        };

        // Act
        var result = taskKey.ToString();

        // Assert
        result.Should().Be("test_dag.test_task[run_123]#2[5]");
    }

    [Fact]
    public void Serialization_RoundTrip_PreservesData()
    {
        // Arrange
        var original = new TaskInstanceKey
        {
            DagId = "my_dag",
            TaskId = "my_task",
            RunId = "scheduled__2024-01-01",
            TryNumber = 3,
            MapIndex = 10
        };

        // Act
        var json = JsonSerializer.Serialize(original);
        var deserialized = JsonSerializer.Deserialize<TaskInstanceKey>(json);

        // Assert
        deserialized.Should().NotBeNull();
        deserialized!.DagId.Should().Be(original.DagId);
        deserialized.TaskId.Should().Be(original.TaskId);
        deserialized.RunId.Should().Be(original.RunId);
        deserialized.TryNumber.Should().Be(original.TryNumber);
        deserialized.MapIndex.Should().Be(original.MapIndex);
    }

    [Fact]
    public void Deserialization_FromJsonWithSnakeCase_Works()
    {
        // Arrange
        var json = """
        {
            "dag_id": "example_dag",
            "task_id": "example_task",
            "run_id": "manual__2024-06-15",
            "try_number": 1,
            "map_index": -1
        }
        """;

        // Act
        var taskKey = JsonSerializer.Deserialize<TaskInstanceKey>(json);

        // Assert
        taskKey.Should().NotBeNull();
        taskKey!.DagId.Should().Be("example_dag");
        taskKey.TaskId.Should().Be("example_task");
        taskKey.RunId.Should().Be("manual__2024-06-15");
        taskKey.TryNumber.Should().Be(1);
        taskKey.MapIndex.Should().Be(-1);
    }

    [Fact]
    public void Equality_TwoIdenticalKeys_AreEqual()
    {
        // Arrange
        var key1 = new TaskInstanceKey
        {
            DagId = "dag",
            TaskId = "task",
            RunId = "run",
            TryNumber = 1,
            MapIndex = -1
        };

        var key2 = new TaskInstanceKey
        {
            DagId = "dag",
            TaskId = "task",
            RunId = "run",
            TryNumber = 1,
            MapIndex = -1
        };

        // Assert
        key1.Should().Be(key2);
        key1.GetHashCode().Should().Be(key2.GetHashCode());
    }
}

public class TaskQueueMessageTests
{
    [Fact]
    public void Deserialization_ValidJson_CreatesMessage()
    {
        // Arrange
        var json = """
        {
            "message_id": "msg-123",
            "task_key": {
                "dag_id": "test_dag",
                "task_id": "test_task",
                "run_id": "run_abc",
                "try_number": 1,
                "map_index": -1
            },
            "workload_json": "{\"type\": \"ExecuteTask\", \"bundle\": \"default\"}",
            "executor_config": {
                "memory": "2048Mi",
                "cpu": "1"
            },
            "enqueued_at": "2024-06-15T10:30:00Z"
        }
        """;

        // Act
        var message = JsonSerializer.Deserialize<TaskQueueMessage>(json);

        // Assert
        message.Should().NotBeNull();
        message!.MessageId.Should().Be("msg-123");
        message.TaskKey.DagId.Should().Be("test_dag");
        message.TaskKey.TaskId.Should().Be("test_task");
        message.ExecutorConfig.Should().ContainKey("memory");
        message.ExecutorConfig["memory"].ToString().Should().Be("2048Mi");
    }

    [Fact]
    public void Workload_Property_DeserializesWorkloadJson()
    {
        // Arrange
        var json = """
        {
            "message_id": "msg-456",
            "task_key": {
                "dag_id": "dag",
                "task_id": "task",
                "run_id": "run",
                "try_number": 1
            },
            "workload_json": "{\"type\": \"ExecuteTask\", \"data\": {\"value\": 42}}",
            "executor_config": {},
            "enqueued_at": "2024-01-01T00:00:00Z"
        }
        """;

        var message = JsonSerializer.Deserialize<TaskQueueMessage>(json)!;

        // Act
        var workload = message.Workload;

        // Assert
        workload.GetProperty("type").GetString().Should().Be("ExecuteTask");
        workload.GetProperty("data").GetProperty("value").GetInt32().Should().Be(42);
    }

    [Fact]
    public void Serialization_RoundTrip_PreservesData()
    {
        // Arrange
        var original = new TaskQueueMessage
        {
            MessageId = "test-msg-id",
            TaskKey = new TaskInstanceKey
            {
                DagId = "dag",
                TaskId = "task",
                RunId = "run",
                TryNumber = 1,
                MapIndex = -1
            },
            WorkloadJson = "{\"key\": \"value\"}",
            ExecutorConfig = new Dictionary<string, object>
            {
                ["setting1"] = "value1",
                ["setting2"] = 123
            },
            EnqueuedAt = new DateTime(2024, 6, 15, 12, 0, 0, DateTimeKind.Utc)
        };

        // Act
        var json = JsonSerializer.Serialize(original);
        var deserialized = JsonSerializer.Deserialize<TaskQueueMessage>(json);

        // Assert
        deserialized.Should().NotBeNull();
        deserialized!.MessageId.Should().Be(original.MessageId);
        deserialized.TaskKey.DagId.Should().Be(original.TaskKey.DagId);
        deserialized.WorkloadJson.Should().Be(original.WorkloadJson);
    }
}

public class TaskResultMessageTests
{
    [Fact]
    public void Serialization_SuccessResult_ProducesValidJson()
    {
        // Arrange
        var result = new TaskResultMessage
        {
            MessageId = "result-123",
            TaskKey = new TaskInstanceKey
            {
                DagId = "test_dag",
                TaskId = "test_task",
                RunId = "run_xyz",
                TryNumber = 1,
                MapIndex = -1
            },
            State = "SUCCESS",
            Info = new TaskResultInfo
            {
                WorkerId = "worker-001",
                BatchJobId = "batch-job-abc",
                ExecutionTimeSeconds = 5.25,
                ErrorMessage = null
            },
            CompletedAt = new DateTime(2024, 6, 15, 12, 30, 0, DateTimeKind.Utc)
        };

        // Act
        var json = JsonSerializer.Serialize(result);
        var parsed = JsonDocument.Parse(json);

        // Assert
        parsed.RootElement.GetProperty("message_id").GetString().Should().Be("result-123");
        parsed.RootElement.GetProperty("state").GetString().Should().Be("SUCCESS");
        parsed.RootElement.GetProperty("info").GetProperty("worker_id").GetString().Should().Be("worker-001");
        parsed.RootElement.GetProperty("info").GetProperty("execution_time_seconds").GetDouble().Should().Be(5.25);
    }

    [Fact]
    public void Serialization_FailedResult_IncludesErrorMessage()
    {
        // Arrange
        var result = new TaskResultMessage
        {
            MessageId = "result-456",
            TaskKey = new TaskInstanceKey
            {
                DagId = "dag",
                TaskId = "task",
                RunId = "run",
                TryNumber = 2,
                MapIndex = -1
            },
            State = "FAILED",
            Info = new TaskResultInfo
            {
                WorkerId = "worker-002",
                BatchJobId = "batch-job-def",
                ExecutionTimeSeconds = 1.5,
                ErrorMessage = "Task failed due to timeout"
            },
            CompletedAt = DateTime.UtcNow
        };

        // Act
        var json = JsonSerializer.Serialize(result);
        var deserialized = JsonSerializer.Deserialize<TaskResultMessage>(json);

        // Assert
        deserialized.Should().NotBeNull();
        deserialized!.State.Should().Be("FAILED");
        deserialized.Info.ErrorMessage.Should().Be("Task failed due to timeout");
    }

    [Fact]
    public void Deserialization_FromPythonCompatibleJson_Works()
    {
        // Arrange - JSON as it would come from Python worker pool executor
        var json = """
        {
            "message_id": "py-msg-001",
            "task_key": {
                "dag_id": "python_dag",
                "task_id": "python_task",
                "run_id": "scheduled__2024-01-01",
                "try_number": 1,
                "map_index": -1
            },
            "state": "SUCCESS",
            "info": {
                "worker_id": "python-worker-01",
                "batch_job_id": "job-12345",
                "execution_time_seconds": 10.5,
                "error_message": null
            },
            "completed_at": "2024-06-15T14:00:00Z"
        }
        """;

        // Act
        var result = JsonSerializer.Deserialize<TaskResultMessage>(json);

        // Assert
        result.Should().NotBeNull();
        result!.MessageId.Should().Be("py-msg-001");
        result.TaskKey.DagId.Should().Be("python_dag");
        result.State.Should().Be("SUCCESS");
        result.Info.WorkerId.Should().Be("python-worker-01");
        result.Info.ExecutionTimeSeconds.Should().Be(10.5);
        result.Info.ErrorMessage.Should().BeNull();
    }
}

public class TaskResultInfoTests
{
    [Fact]
    public void Serialization_WithAllFields_ProducesCorrectJson()
    {
        // Arrange
        var info = new TaskResultInfo
        {
            WorkerId = "worker-test",
            BatchJobId = "batch-123",
            ExecutionTimeSeconds = 2.75,
            ErrorMessage = "Test error"
        };

        // Act
        var json = JsonSerializer.Serialize(info);
        var parsed = JsonDocument.Parse(json);

        // Assert
        parsed.RootElement.GetProperty("worker_id").GetString().Should().Be("worker-test");
        parsed.RootElement.GetProperty("batch_job_id").GetString().Should().Be("batch-123");
        parsed.RootElement.GetProperty("execution_time_seconds").GetDouble().Should().Be(2.75);
        parsed.RootElement.GetProperty("error_message").GetString().Should().Be("Test error");
    }

    [Fact]
    public void Serialization_WithNullErrorMessage_IncludesNullInJson()
    {
        // Arrange
        var info = new TaskResultInfo
        {
            WorkerId = "worker",
            BatchJobId = "batch",
            ExecutionTimeSeconds = 1.0,
            ErrorMessage = null
        };

        // Act
        var json = JsonSerializer.Serialize(info);

        // Assert
        json.Should().Contain("\"error_message\":null");
    }
}
