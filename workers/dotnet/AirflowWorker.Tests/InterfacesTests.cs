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
using FluentAssertions;
using Moq;
using Xunit;

namespace AirflowWorker.Tests;

public class TaskExecutionContextTests
{
    [Fact]
    public void Constructor_WithAllProperties_SetsCorrectly()
    {
        // Arrange
        var taskKey = new TaskInstanceKey
        {
            DagId = "test_dag",
            TaskId = "test_task",
            RunId = "run_001",
            TryNumber = 1,
            MapIndex = -1
        };

        var workloadJson = "{\"type\": \"ExecuteTask\"}";
        var workload = JsonSerializer.Deserialize<JsonElement>(workloadJson);

        var executorConfig = new Dictionary<string, object>
        {
            ["memory"] = "2048Mi",
            ["vcpu"] = 2
        };

        var mockSharedState = new Mock<ISharedState>();

        // Act
        var context = new TaskExecutionContext
        {
            TaskKey = taskKey,
            Workload = workload,
            ExecutorConfig = executorConfig,
            SharedState = mockSharedState.Object
        };

        // Assert
        context.TaskKey.Should().Be(taskKey);
        context.Workload.GetProperty("type").GetString().Should().Be("ExecuteTask");
        context.ExecutorConfig.Should().ContainKey("memory");
        context.ExecutorConfig["memory"].Should().Be("2048Mi");
        context.SharedState.Should().NotBeNull();
    }

    [Fact]
    public void SharedState_CanBeNull_WhenNotConfigured()
    {
        // Arrange & Act
        var context = new TaskExecutionContext
        {
            TaskKey = new TaskInstanceKey
            {
                DagId = "dag",
                TaskId = "task",
                RunId = "run",
                TryNumber = 1
            },
            Workload = JsonSerializer.Deserialize<JsonElement>("{}"),
            ExecutorConfig = new Dictionary<string, object>(),
            SharedState = null
        };

        // Assert
        context.SharedState.Should().BeNull();
    }

    [Fact]
    public void ExecutorConfig_CanContainVariousTypes()
    {
        // Arrange
        var executorConfig = new Dictionary<string, object>
        {
            ["string_value"] = "test",
            ["int_value"] = 42,
            ["bool_value"] = true,
            ["nested"] = new Dictionary<string, object> { ["inner"] = "value" }
        };

        var context = new TaskExecutionContext
        {
            TaskKey = new TaskInstanceKey
            {
                DagId = "dag",
                TaskId = "task",
                RunId = "run",
                TryNumber = 1
            },
            Workload = JsonSerializer.Deserialize<JsonElement>("{}"),
            ExecutorConfig = executorConfig,
            SharedState = null
        };

        // Assert
        context.ExecutorConfig["string_value"].ToString().Should().Be("test");
        context.ExecutorConfig["int_value"].Should().Be(42);
        context.ExecutorConfig["bool_value"].Should().Be(true);
    }
}

public class ISharedStateTests
{
    private class TestSharedState : ISharedState
    {
        public bool Initialized { get; private set; }
        public bool Disposed { get; private set; }
        public string? TestResource { get; private set; }

        public async Task InitializeAsync()
        {
            await Task.Delay(10); // Simulate async initialization
            TestResource = "Initialized Resource";
            Initialized = true;
        }

        public async Task DisposeAsync()
        {
            await Task.Delay(10); // Simulate async cleanup
            TestResource = null;
            Disposed = true;
        }
    }

    [Fact]
    public async Task InitializeAsync_SetsUpResources()
    {
        // Arrange
        var sharedState = new TestSharedState();

        // Act
        await sharedState.InitializeAsync();

        // Assert
        sharedState.Initialized.Should().BeTrue();
        sharedState.TestResource.Should().Be("Initialized Resource");
    }

    [Fact]
    public async Task DisposeAsync_CleansUpResources()
    {
        // Arrange
        var sharedState = new TestSharedState();
        await sharedState.InitializeAsync();

        // Act
        await sharedState.DisposeAsync();

        // Assert
        sharedState.Disposed.Should().BeTrue();
        sharedState.TestResource.Should().BeNull();
    }

    [Fact]
    public async Task SharedState_CanBeReusedAcrossMultipleContexts()
    {
        // Arrange
        var sharedState = new TestSharedState();
        await sharedState.InitializeAsync();

        // Act - Create multiple contexts sharing the same state
        var context1 = new TaskExecutionContext
        {
            TaskKey = new TaskInstanceKey { DagId = "dag", TaskId = "task1", RunId = "run", TryNumber = 1 },
            Workload = JsonSerializer.Deserialize<JsonElement>("{}"),
            ExecutorConfig = new Dictionary<string, object>(),
            SharedState = sharedState
        };

        var context2 = new TaskExecutionContext
        {
            TaskKey = new TaskInstanceKey { DagId = "dag", TaskId = "task2", RunId = "run", TryNumber = 1 },
            Workload = JsonSerializer.Deserialize<JsonElement>("{}"),
            ExecutorConfig = new Dictionary<string, object>(),
            SharedState = sharedState
        };

        // Assert
        context1.SharedState.Should().BeSameAs(context2.SharedState);
        ((TestSharedState)context1.SharedState!).TestResource.Should().Be("Initialized Resource");
    }
}

public class ITaskHandlerTests
{
    private class TestTaskHandler : ITaskHandler
    {
        public bool Executed { get; private set; }
        public TaskExecutionContext? LastContext { get; private set; }
        public bool ShouldThrow { get; set; }
        public TimeSpan ExecutionDelay { get; set; } = TimeSpan.Zero;

        public async Task ExecuteAsync(TaskExecutionContext context, CancellationToken cancellationToken)
        {
            LastContext = context;

            if (ExecutionDelay > TimeSpan.Zero)
            {
                await Task.Delay(ExecutionDelay, cancellationToken);
            }

            if (ShouldThrow)
            {
                throw new InvalidOperationException("Task execution failed");
            }

            Executed = true;
        }
    }

    [Fact]
    public async Task ExecuteAsync_WithValidContext_ExecutesSuccessfully()
    {
        // Arrange
        var handler = new TestTaskHandler();
        var context = new TaskExecutionContext
        {
            TaskKey = new TaskInstanceKey
            {
                DagId = "test_dag",
                TaskId = "test_task",
                RunId = "run_001",
                TryNumber = 1
            },
            Workload = JsonSerializer.Deserialize<JsonElement>("{\"data\": \"test\"}"),
            ExecutorConfig = new Dictionary<string, object>(),
            SharedState = null
        };

        // Act
        await handler.ExecuteAsync(context, CancellationToken.None);

        // Assert
        handler.Executed.Should().BeTrue();
        handler.LastContext.Should().Be(context);
    }

    [Fact]
    public async Task ExecuteAsync_WithCancellation_ThrowsOperationCanceled()
    {
        // Arrange
        var handler = new TestTaskHandler { ExecutionDelay = TimeSpan.FromSeconds(5) };
        var context = new TaskExecutionContext
        {
            TaskKey = new TaskInstanceKey
            {
                DagId = "dag",
                TaskId = "task",
                RunId = "run",
                TryNumber = 1
            },
            Workload = JsonSerializer.Deserialize<JsonElement>("{}"),
            ExecutorConfig = new Dictionary<string, object>(),
            SharedState = null
        };

        using var cts = new CancellationTokenSource(TimeSpan.FromMilliseconds(50));

        // Act & Assert
        await Assert.ThrowsAsync<TaskCanceledException>(() =>
            handler.ExecuteAsync(context, cts.Token));
    }

    [Fact]
    public async Task ExecuteAsync_WhenThrows_PropagatesException()
    {
        // Arrange
        var handler = new TestTaskHandler { ShouldThrow = true };
        var context = new TaskExecutionContext
        {
            TaskKey = new TaskInstanceKey
            {
                DagId = "dag",
                TaskId = "task",
                RunId = "run",
                TryNumber = 1
            },
            Workload = JsonSerializer.Deserialize<JsonElement>("{}"),
            ExecutorConfig = new Dictionary<string, object>(),
            SharedState = null
        };

        // Act & Assert
        var exception = await Assert.ThrowsAsync<InvalidOperationException>(() =>
            handler.ExecuteAsync(context, CancellationToken.None));
        exception.Message.Should().Be("Task execution failed");
    }

    [Fact]
    public async Task ExecuteAsync_CanAccessSharedState()
    {
        // Arrange
        var mockSharedState = new Mock<ISharedState>();
        var handler = new TestTaskHandler();
        var context = new TaskExecutionContext
        {
            TaskKey = new TaskInstanceKey
            {
                DagId = "dag",
                TaskId = "task",
                RunId = "run",
                TryNumber = 1
            },
            Workload = JsonSerializer.Deserialize<JsonElement>("{}"),
            ExecutorConfig = new Dictionary<string, object>(),
            SharedState = mockSharedState.Object
        };

        // Act
        await handler.ExecuteAsync(context, CancellationToken.None);

        // Assert
        handler.LastContext!.SharedState.Should().NotBeNull();
        handler.LastContext.SharedState.Should().BeSameAs(mockSharedState.Object);
    }

    [Fact]
    public async Task ExecuteAsync_CanAccessExecutorConfig()
    {
        // Arrange
        var handler = new TestTaskHandler();
        var context = new TaskExecutionContext
        {
            TaskKey = new TaskInstanceKey
            {
                DagId = "dag",
                TaskId = "task",
                RunId = "run",
                TryNumber = 1
            },
            Workload = JsonSerializer.Deserialize<JsonElement>("{}"),
            ExecutorConfig = new Dictionary<string, object>
            {
                ["custom_setting"] = "custom_value",
                ["batch_size"] = 100
            },
            SharedState = null
        };

        // Act
        await handler.ExecuteAsync(context, CancellationToken.None);

        // Assert
        handler.LastContext!.ExecutorConfig.Should().ContainKey("custom_setting");
        handler.LastContext.ExecutorConfig["custom_setting"].Should().Be("custom_value");
        handler.LastContext.ExecutorConfig["batch_size"].Should().Be(100);
    }
}
