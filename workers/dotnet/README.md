# Airflow AWS Batch Worker Pool - .NET Worker

This is a .NET 8 implementation of the AWS Batch Worker Pool worker process. It provides
an alternative to the Python worker for scenarios that require .NET execution.

## Overview

The .NET worker differs from the Python worker in a key architectural aspect:

| Feature | Python Worker | .NET Worker |
|---------|---------------|-------------|
| Task Isolation | Fork subprocess per task | In-process execution |
| Memory Isolation | Full process isolation | Shared memory space |
| Crash Impact | Worker survives task crashes | Worker may crash |
| Performance | ~10ms fork overhead per task | No fork overhead |

The .NET CLR is not fork-safe, so we use in-process execution with robust error handling
and memory monitoring instead.

## Building

### Prerequisites

- .NET 8.0 SDK
- Docker (for container builds)

### Local Build

```bash
cd workers/dotnet
dotnet build
```

### Docker Build

```bash
cd workers/dotnet
docker build -t airflow-worker-dotnet:latest .
```

## Usage

### Command Line Arguments

```
--worker-id           Unique worker identifier (required)
--task-queue-url      SQS task queue URL (required)
--result-queue-url    SQS result queue URL (required)
--idle-timeout        Idle timeout in seconds (default: 300)
--visibility-timeout  SQS visibility timeout (default: 3600)
--max-tasks           Max tasks before terminating, 0=unlimited (default: 0)
--init-assembly       Assembly containing shared state initializer
--init-type           Type name for initializer (default: SharedStateInitializer)
```

### Running Locally

```bash
dotnet run --project AirflowWorker -- \
    --worker-id test-001 \
    --task-queue-url https://sqs.us-east-1.amazonaws.com/123456789/tasks \
    --result-queue-url https://sqs.us-east-1.amazonaws.com/123456789/results
```

### Running in Docker

```bash
docker run --rm \
    -e AWS_ACCESS_KEY_ID=... \
    -e AWS_SECRET_ACCESS_KEY=... \
    -e AWS_REGION=us-east-1 \
    airflow-worker-dotnet:latest \
    --worker-id test-001 \
    --task-queue-url https://sqs.us-east-1.amazonaws.com/123456789/tasks \
    --result-queue-url https://sqs.us-east-1.amazonaws.com/123456789/results
```

## Implementing Task Handlers

### 1. Create a Shared State Class (Optional)

Shared state is initialized once when the worker starts and reused across all tasks.
This is ideal for expensive resources like ML models or database connections.

```csharp
using AirflowWorker;

public class MySharedState : ISharedState
{
    public HttpClient HttpClient { get; private set; } = null!;
    public MyModel Model { get; private set; } = null!;

    public async Task InitializeAsync()
    {
        // Initialize expensive resources once
        HttpClient = new HttpClient();
        Model = await LoadModel();
    }

    public async Task DisposeAsync()
    {
        HttpClient?.Dispose();
    }
}
```

### 2. Create a Task Handler

```csharp
using AirflowWorker;

public class MyTaskHandler : ITaskHandler
{
    public async Task ExecuteAsync(TaskExecutionContext context, CancellationToken ct)
    {
        // Access shared state
        var sharedState = context.SharedState as MySharedState;

        // Get parameters from executor_config
        var param = context.ExecutorConfig["my_param"]?.ToString();

        // Execute your task logic
        await DoWork(sharedState, param, ct);
    }
}
```

### 3. Configure in Airflow DAG

```python
from airflow import DAG
from airflow.operators.python import PythonOperator

with DAG("dotnet_tasks") as dag:
    task = PythonOperator(
        task_id="my_task",
        python_callable=lambda: None,  # Placeholder
        executor_config={
            "worker_type": "dotnet",
            "dotnet_handler_assembly": "/app/handlers/MyHandlers.dll",
            "dotnet_handler_type": "MyNamespace.MyTaskHandler",
            "my_param": "value"
        }
    )
```

## Project Structure

```
workers/dotnet/
├── AirflowWorker/
│   ├── AirflowWorker.csproj      # Main worker project
│   ├── Program.cs                 # Entry point with CLI
│   ├── WorkerProcess.cs           # Main worker loop
│   ├── Interfaces.cs              # ISharedState, ITaskHandler
│   └── Models.cs                  # SQS message schemas
│
├── AirflowWorker.Example/
│   ├── AirflowWorker.Example.csproj
│   ├── MySharedState.cs           # Example shared state
│   └── MyTaskHandler.cs           # Example task handlers
│
├── AirflowWorker.Tests/
│   ├── AirflowWorker.Tests.csproj # Unit tests (xUnit)
│   ├── ModelsTests.cs             # Tests for message schemas
│   ├── InterfacesTests.cs         # Tests for interfaces
│   └── WorkerProcessTests.cs      # Tests for worker logic
│
├── AirflowWorker.sln              # Solution file
├── Directory.Build.props          # Common build settings
├── Dockerfile                     # Container build
└── README.md                      # This file
```

## Running Tests

```bash
cd workers/dotnet
dotnet test
```

To run with coverage:

```bash
dotnet test --collect:"XPlat Code Coverage"
```

## AWS Batch Job Definition

Example Batch job definition for the .NET worker:

```json
{
  "jobDefinitionName": "airflow-worker-dotnet",
  "type": "container",
  "platformCapabilities": ["FARGATE"],
  "containerProperties": {
    "image": "<ecr-repo>/airflow-worker-dotnet:latest",
    "resourceRequirements": [
      {"type": "VCPU", "value": "2"},
      {"type": "MEMORY", "value": "4096"}
    ],
    "executionRoleArn": "arn:aws:iam::123456789:role/ecsTaskExecutionRole",
    "jobRoleArn": "arn:aws:iam::123456789:role/airflowWorkerRole",
    "networkConfiguration": {
      "assignPublicIp": "ENABLED"
    }
  },
  "timeout": {
    "attemptDurationSeconds": 86400
  }
}
```

## License

Licensed under the Apache License, Version 2.0.
