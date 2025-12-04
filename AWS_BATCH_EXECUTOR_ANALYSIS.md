# AWS Batch Executor - Detailed Implementation Analysis

## Table of Contents

1. [Executive Summary](#executive-summary)
2. [Architecture Overview](#architecture-overview)
3. [Core Components](#core-components)
4. [Execution Flow](#execution-flow)
5. [Configuration System](#configuration-system)
6. [State Management](#state-management)
7. [Failure Handling & Retry Logic](#failure-handling--retry-logic)
8. [Health Monitoring & Connection Management](#health-monitoring--connection-management)
9. [Task Adoption Mechanism](#task-adoption-mechanism)
10. [Airflow 3.0 Workload Support](#airflow-30-workload-support)
11. [Integration with AWS Batch API](#integration-with-aws-batch-api)
12. [Comparison with BaseExecutor](#comparison-with-baseexecutor)
13. [Deployment Requirements](#deployment-requirements)
14. [Performance Considerations](#performance-considerations)
15. [Code References](#code-references)

---

## Executive Summary

The AWS Batch Executor (`AwsBatchExecutor`) is a **remote, containerized executor** that delegates each Airflow task to AWS Batch as an independent job. It follows a **queueing pattern** where tasks are first queued locally, then submitted to AWS Batch via the Boto3 API, and finally monitored for completion through periodic polling.

**Key Characteristics:**

- **Execution Model**: Each task runs in an isolated container managed by AWS Batch
- **Scalability**: Leverages AWS Batch's autoscaling capabilities (Fargate, EC2, or EKS)
- **Resilience**: Built-in retry logic with exponential backoff for both job submission and execution failures
- **Stateless Workers**: Task containers are ephemeral; no persistent worker processes
- **Asynchronous**: Non-blocking submission and polling model
- **Task Adoption**: Supports adopting orphaned tasks after scheduler restart

---

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────────────┐
│                        Airflow Scheduler                             │
│                                                                       │
│  ┌───────────────────────────────────────────────────────────────┐  │
│  │              AwsBatchExecutor                                  │  │
│  │                                                                 │  │
│  │  ┌──────────────┐    ┌──────────────┐   ┌──────────────────┐ │  │
│  │  │ queued_tasks │───>│pending_jobs  │──>│ active_workers   │ │  │
│  │  │  (dict)      │    │  (deque)     │   │ (BatchJobColl.)  │ │  │
│  │  └──────────────┘    └──────────────┘   └──────────────────┘ │  │
│  │         │                   │                      │           │  │
│  │         v                   v                      v           │  │
│  │   queue_workload()   execute_async()      attempt_submit()    │  │
│  │                            │                      │            │  │
│  │                            v                      v            │  │
│  │                      [BatchQueuedJob]      batch.submit_job() │  │
│  └────────────────────────────────────────────│───────────────────┘  │
└────────────────────────────────────────────────│──────────────────────┘
                                                 │
                                                 │ Boto3 API
                                                 v
┌─────────────────────────────────────────────────────────────────────┐
│                           AWS Batch                                  │
│                                                                       │
│  Job Queue → Compute Environment (Fargate/EC2/EKS)                  │
│                                                                       │
│  ┌─────────────┐   ┌─────────────┐   ┌─────────────┐              │
│  │ Task Job 1  │   │ Task Job 2  │   │ Task Job 3  │              │
│  │             │   │             │   │             │              │
│  │ Container   │   │ Container   │   │ Container   │              │
│  │ airflow task│   │ airflow task│   │ airflow task│              │
│  │   run       │   │   run       │   │   run       │              │
│  └─────────────┘   └─────────────┘   └─────────────┘              │
│         │                   │                 │                     │
│         └───────────────────┴─────────────────┘                     │
│                             │                                        │
│                             v                                        │
│                    Shared Database (RDS)                            │
└─────────────────────────────────────────────────────────────────────┘
                             │
                             │
                             v
                    ┌────────────────┐
                    │ Remote Logging │
                    │  (S3/CloudWatch)│
                    └────────────────┘
```

### Component Responsibilities

1. **Scheduler/Executor**: Queues tasks, submits to Batch, monitors status
2. **AWS Batch**: Provisions compute, schedules containers, manages lifecycle
3. **Task Containers**: Execute individual task commands, report status via DB
4. **Shared Database**: Central state store accessible by all components
5. **Remote Logging**: Persistent storage for task logs (S3 or CloudWatch)

---

## Core Components

### 1. AwsBatchExecutor Class

**Location**: `providers/amazon/src/airflow/providers/amazon/aws/executors/batch/batch_executor.py`

**Inheritance**: `BaseExecutor` → `AwsBatchExecutor`

**Key Attributes**:

```python
class AwsBatchExecutor(BaseExecutor):
    # Connection state
    batch: BatchClient                          # Boto3 Batch client
    IS_BOTO_CONNECTION_HEALTHY: bool           # Health flag
    attempts_since_last_successful_connection: int
    last_connection_reload: datetime

    # Task tracking
    active_workers: BatchJobCollection          # Running jobs
    pending_jobs: deque[BatchQueuedJob]        # Jobs awaiting submission
    queued_tasks: dict[TaskInstanceKey, workloads.ExecuteTask]  # Queued by scheduler

    # Configuration
    submit_job_kwargs: dict                     # Template for submit_job API calls
    MAX_SUBMIT_JOB_ATTEMPTS: int               # Retry limit (default: 3)
    DESCRIBE_JOBS_BATCH_SIZE: int              # AWS API limit (99)
```

### 2. BatchJobCollection

**Location**: `providers/amazon/src/airflow/providers/amazon/aws/executors/batch/utils.py:90-137`

**Purpose**: Thread-safe tracking of active AWS Batch jobs.

**Data Structures**:

```python
class BatchJobCollection:
    key_to_id: dict[TaskInstanceKey, str]              # Airflow key → Batch job ID
    id_to_key: dict[str, TaskInstanceKey]              # Batch job ID → Airflow key
    id_to_failure_counts: dict[str, int]               # Job failure counters
    id_to_job_info: dict[str, BatchJobInfo]            # Job metadata
```

**Key Methods**:

- `add_job()`: Register a new Batch job
- `pop_by_id()`: Remove and return job by Batch job ID
- `failure_count_by_id()`: Get failure count for retry logic
- `increment_failure_count()`: Track failed attempts
- `get_all_jobs()`: Return all active job IDs for polling

### 3. BatchQueuedJob (Dataclass)

**Location**: `providers/amazon/src/airflow/providers/amazon/aws/executors/batch/utils.py:42-52`

**Purpose**: Represents a job waiting to be submitted to Batch.

```python
@dataclass
class BatchQueuedJob:
    key: TaskInstanceKey              # Unique task identifier
    command: CommandType              # Shell command to execute
    queue: str                        # Airflow queue name
    executor_config: ExecutorConfigType  # Per-task configuration overrides
    attempt_number: int               # Current retry attempt
    next_attempt_time: datetime       # Exponential backoff timer
```

**Lifecycle**:

1. Created in `execute_async()` with `attempt_number=1`
2. Stored in `pending_jobs` deque
3. Popped and submitted in `attempt_submit_jobs()`
4. Re-queued with incremented `attempt_number` if submission fails
5. Dropped if `attempt_number >= MAX_SUBMIT_JOB_ATTEMPTS`

### 4. BatchJob (DTO)

**Location**: `providers/amazon/src/airflow/providers/amazon/aws/executors/batch/utils.py:63-88`

**Purpose**: Data Transfer Object for AWS Batch job responses.

```python
class BatchJob:
    STATE_MAPPINGS = {
        "SUBMITTED": State.QUEUED,
        "PENDING": State.QUEUED,
        "RUNNABLE": State.QUEUED,
        "STARTING": State.QUEUED,
        "RUNNING": State.RUNNING,
        "SUCCEEDED": State.SUCCESS,
        "FAILED": State.FAILED,
    }

    job_id: str
    status: str              # AWS Batch status
    status_reason: str       # Failure reason (if applicable)

    def get_job_state(self) -> str:
        return self.STATE_MAPPINGS.get(self.status, State.QUEUED)
```

**State Mapping Logic**:

- AWS Batch uses 7 states; Airflow uses 3 for executors
- Pre-launch states (SUBMITTED, PENDING, RUNNABLE, STARTING) → `State.QUEUED`
- Active execution → `State.RUNNING`
- Terminal states → `State.SUCCESS` or `State.FAILED`

### 5. Configuration Builder

**Location**: `providers/amazon/src/airflow/providers/amazon/aws/executors/batch/batch_executor_config.py`

**Purpose**: Build `submit_job()` kwargs from Airflow configuration.

```python
def build_submit_kwargs() -> dict:
    # Step 1: Load individual config keys
    job_kwargs = _fetch_config_values()

    # Step 2: Merge with templated JSON kwargs
    job_kwargs.update(_fetch_templated_kwargs())

    # Step 3: Ensure containerOverrides.command exists (empty array)
    if "containerOverrides" not in job_kwargs:
        job_kwargs["containerOverrides"] = {}
    job_kwargs["containerOverrides"]["command"] = []

    # Step 4: Validate unsupported features
    if "nodeOverrides" in job_kwargs:
        raise KeyError("Multi-node jobs are not currently supported.")
    if "eksPropertiesOverride" in job_kwargs:
        raise KeyError("Eks jobs are not currently supported.")

    # Step 5: Camelize keys (snake_case → camelCase)
    job_kwargs = camelize_dict_keys(job_kwargs)

    return job_kwargs
```

**Configuration Sources** (in order of precedence):

1. **Airflow Config Keys**: `[aws_batch_executor]` section
   - `job_name`, `job_queue`, `job_definition`, `region_name`
2. **JSON Template**: `submit_job_kwargs` config value
3. **Per-Task Overrides**: `executor_config` in task definition

### 6. Schema Validation (Marshmallow)

**Location**: `providers/amazon/src/airflow/providers/amazon/aws/executors/batch/boto_schema.py`

**Purpose**: Type-safe parsing of Boto3 API responses.

```python
class BatchSubmitJobResponseSchema(Schema):
    job_id = fields.String(data_key="jobId", required=True)

class BatchJobDetailSchema(Schema):
    job_id = fields.String(data_key="jobId", required=True)
    status = fields.String(required=True)
    status_reason = fields.String(data_key="statusReason")

    @post_load
    def make_job(self, data, **kwargs):
        return BatchJob(**data)  # Convert dict → BatchJob DTO
```

---

## Execution Flow

### Phase 1: Task Queuing (Scheduler → Executor)

**Entry Point**: `queue_workload(workload, session)` (line 122-128)

```python
def queue_workload(self, workload: workloads.All, session: Session | None) -> None:
    # Validate workload type
    if not isinstance(workload, workloads.ExecuteTask):
        raise RuntimeError(f"{type(self)} cannot handle workloads of type {type(workload)}")

    # Store in queued_tasks dict (keyed by TaskInstanceKey)
    ti = workload.ti
    self.queued_tasks[ti.key] = workload
```

**Triggered By**: Scheduler's `heartbeat()` → `trigger_tasks()` → `queue_workload()`

**State Transition**: Task moves from Scheduler → Executor's `queued_tasks` dict

---

### Phase 2: Task Processing (Heartbeat)

**Entry Point**: `_process_workloads(workloads)` (line 130-144)

```python
def _process_workloads(self, workloads: Sequence[workloads.All]) -> None:
    for w in workloads:
        # Extract task information
        command = [w]                           # Wrap workload
        key = w.ti.key                          # TaskInstanceKey
        queue = w.ti.queue                      # Queue name
        executor_config = w.ti.executor_config or {}

        # Remove from queued_tasks
        del self.queued_tasks[key]

        # Submit for async execution
        self.execute_async(key, command, queue, executor_config)

        # Mark as running
        self.running.add(key)
```

**Triggered By**: BaseExecutor's `heartbeat()` → `trigger_tasks()` → `_process_workloads()`

**State Transition**: `queued_tasks` → `pending_jobs` (via `execute_async`)

---

### Phase 3: Async Execution Preparation

**Entry Point**: `execute_async(key, command, queue, executor_config)` (line 378-410)

```python
def execute_async(self, key: TaskInstanceKey, command: CommandType,
                  queue=None, executor_config=None):
    # Validation: Prevent executor_config from overriding command
    if executor_config and "command" in executor_config:
        raise ValueError('Executor Config should never override "command"')

    # Airflow 3.0 compatibility: Convert ExecuteTask workload to CLI command
    if len(command) == 1 and isinstance(command[0], ExecuteTask):
        workload = command[0]
        ser_input = workload.model_dump_json()  # Serialize to JSON
        command = [
            "python",
            "-m",
            "airflow.sdk.execution_time.execute_workload",
            "--json-string",
            ser_input,
        ]

    # Create queued job with immediate retry eligibility
    self.pending_jobs.append(
        BatchQueuedJob(
            key=key,
            command=list(command),
            queue=queue,
            executor_config=executor_config or {},
            attempt_number=1,
            next_attempt_time=timezone.utcnow(),  # Submit immediately
        )
    )
```

**Key Decision Point**: Airflow 3.0 uses `ExecuteTask` workload (JSON-serialized), while Airflow 2.x uses shell commands.

**State Transition**: Task added to `pending_jobs` deque for submission in next `sync()` cycle.

---

### Phase 4: Job Submission Loop

**Entry Point**: `attempt_submit_jobs()` (line 296-364)

**Called By**: `sync()` method (invoked by scheduler's heartbeat)

**Algorithm**:

```python
def attempt_submit_jobs(self):
    # Process all pending jobs in FIFO order
    for _ in range(len(self.pending_jobs)):
        batch_job = self.pending_jobs.popleft()

        # Exponential backoff: Check if retry timer elapsed
        if timezone.utcnow() < batch_job.next_attempt_time:
            self.pending_jobs.append(batch_job)  # Re-queue for later
            continue

        # Attempt submission
        try:
            submit_job_response = self._submit_job(
                key=batch_job.key,
                cmd=batch_job.command,
                queue=batch_job.queue,
                exec_config=batch_job.executor_config
            )

            # SUCCESS: Register job in active_workers
            job_id = submit_job_response["job_id"]
            self.active_workers.add_job(
                job_id=job_id,
                airflow_task_key=batch_job.key,
                airflow_cmd=batch_job.command,
                queue=batch_job.queue,
                exec_config=batch_job.executor_config,
                attempt_number=batch_job.attempt_number,
            )
            self.running_state(batch_job.key, job_id)

        except (ClientError, NoCredentialsError) as e:
            # FAILURE: Re-queue with exponential backoff
            if batch_job.attempt_number >= MAX_SUBMIT_JOB_ATTEMPTS:
                self.fail(batch_job.key)  # Give up
            else:
                batch_job.attempt_number += 1
                batch_job.next_attempt_time = (
                    timezone.utcnow() +
                    calculate_next_attempt_delay(batch_job.attempt_number)
                )
                self.pending_jobs.append(batch_job)
```

**Exponential Backoff Formula** (from `exponential_backoff_retry.py:31-43`):

```python
def calculate_next_attempt_delay(attempt_number: int,
                                  max_delay: int = 120,
                                  exponent_base: int = 4) -> timedelta:
    return timedelta(seconds=min((exponent_base ** attempt_number), max_delay))
```

**Retry Delays**:

| Attempt | Delay (seconds) | Cumulative Wait |
|---------|-----------------|-----------------|
| 1       | 4               | 4s              |
| 2       | 16              | 20s             |
| 3       | 64              | 84s             |
| 4+      | 120 (max)       | 204s+           |

---

### Phase 5: Job Submission (Boto3 API Call)

**Entry Point**: `_submit_job(key, cmd, queue, exec_config)` (line 412-425)

```python
def _submit_job(self, key: TaskInstanceKey, cmd: CommandType,
                queue: str, exec_config: ExecutorConfigType) -> str:
    # Build API kwargs
    submit_job_api = self._submit_job_kwargs(key, cmd, queue, exec_config)

    # Call AWS Batch API
    boto_submit_job = self.batch.submit_job(**submit_job_api)

    # Parse and validate response
    submit_job_response = BatchSubmitJobResponseSchema().load(boto_submit_job)
    return submit_job_response
```

**API Kwargs Builder**: `_submit_job_kwargs()` (line 427-445)

```python
def _submit_job_kwargs(self, key: TaskInstanceKey, cmd: CommandType,
                       queue: str, exec_config: ExecutorConfigType) -> dict:
    # Start with base configuration
    submit_job_api = deepcopy(self.submit_job_kwargs)

    # Merge per-task executor_config
    submit_job_api = merge_dicts(submit_job_api, exec_config)

    # Override command (the task to execute)
    submit_job_api["containerOverrides"]["command"] = cmd

    # Inject executor flag environment variable
    if "environment" not in submit_job_api["containerOverrides"]:
        submit_job_api["containerOverrides"]["environment"] = []
    submit_job_api["containerOverrides"]["environment"].append(
        {"name": "AIRFLOW_IS_EXECUTOR_CONTAINER", "value": "true"}
    )

    return submit_job_api
```

**Example API Call**:

```json
{
  "jobName": "airflow-task-dag-id-task-id-2024-01-01",
  "jobQueue": "airflow-batch-queue",
  "jobDefinition": "airflow-task-definition:1",
  "containerOverrides": {
    "command": [
      "python", "-m", "airflow.sdk.execution_time.execute_workload",
      "--json-string", "{...serialized task...}"
    ],
    "environment": [
      {"name": "AIRFLOW_IS_EXECUTOR_CONTAINER", "value": "true"}
    ]
  }
}
```

---

### Phase 6: Job Monitoring (Sync Cycle)

**Entry Point**: `sync()` (line 205-228)

**Frequency**: Called periodically by `BaseExecutor.heartbeat()` (typically every 5 seconds)

```python
def sync(self):
    # Health check: Reconnect if unhealthy
    if not self.IS_BOTO_CONNECTION_HEALTHY:
        exponential_backoff_retry(
            self.last_connection_reload,
            self.attempts_since_last_successful_connection,
            self.load_batch_connection,
        )
        if not self.IS_BOTO_CONNECTION_HEALTHY:
            return  # Skip this cycle

    try:
        # Step 1: Check status of running jobs
        self.sync_running_jobs()

        # Step 2: Submit pending jobs
        self.attempt_submit_jobs()

    except (ClientError, NoCredentialsError) as error:
        # Credential errors: Mark connection unhealthy
        if error.response["Error"]["Code"] in INVALID_CREDENTIALS_EXCEPTIONS:
            self.IS_BOTO_CONNECTION_HEALTHY = False

    except Exception:
        # Catch-all: Log but don't kill scheduler
        self.log.exception("Failed to sync %s", self.__class__.__name__)
```

**Why Catch All Exceptions?**: If `sync()` raises an unhandled exception, it bubbles up to the scheduler and kills the entire scheduler process. The catch-all ensures executor failures are isolated.

---

### Phase 7: Job Status Synchronization

**Entry Point**: `sync_running_jobs()` (line 230-244)

```python
def sync_running_jobs(self):
    # Get all active Batch job IDs
    all_job_ids = self.active_workers.get_all_jobs()
    if not all_job_ids:
        return  # No jobs to check

    # Call AWS Batch describe_jobs API
    describe_job_response = self._describe_jobs(all_job_ids)

    # Process each job's status
    for job in describe_job_response:
        if job.get_job_state() == State.FAILED:
            self._handle_failed_job(job)  # Retry or fail task
        elif job.get_job_state() == State.SUCCESS:
            task_key = self.active_workers.pop_by_id(job.job_id)
            self.success(task_key)  # Mark task successful
```

**API Call Batching**: `_describe_jobs()` (line 366-376)

```python
def _describe_jobs(self, job_ids) -> list[BatchJob]:
    all_jobs = []

    # AWS Batch limits describe_jobs to 99 job IDs per call
    for i in range(0, len(job_ids), 99):
        batched_job_ids = job_ids[i:i+99]

        # Call AWS API
        boto_describe_tasks = self.batch.describe_jobs(jobs=batched_job_ids)

        # Parse response with Marshmallow schema
        describe_tasks_response = BatchDescribeJobsResponseSchema().load(boto_describe_tasks)
        all_jobs.extend(describe_tasks_response["jobs"])

    return all_jobs
```

**State Transitions**:

- `State.QUEUED` / `State.RUNNING` → No action (still running)
- `State.SUCCESS` → Remove from `active_workers`, call `self.success(key)`
- `State.FAILED` → Call `_handle_failed_job()` (retry or fail)

---

### Phase 8: Failure Handling

**Entry Point**: `_handle_failed_job(job)` (line 246-294)

**Purpose**: Distinguish between **AWS Batch failures** (infrastructure) and **DAG failures** (application logic).

```python
def _handle_failed_job(self, job):
    # AWS Batch marks a job FAILED if:
    # - Container fails to start
    # - Job definition is misconfigured
    # - Resource limits exceeded
    # - Spot instance interruption
    #
    # It does NOT mark FAILED if:
    # - Airflow task code raises an exception
    # - DAG logic fails
    # (In those cases, Batch job succeeds but task is marked failed by Airflow)

    # Retrieve job metadata
    job_info = self.active_workers.id_to_job_info[job.job_id]
    task_key = self.active_workers.id_to_key[job.job_id]
    failure_count = self.active_workers.failure_count_by_id(job.job_id)

    # Retry logic
    if int(failure_count) < int(self.MAX_SUBMIT_JOB_ATTEMPTS):
        self.log.warning(
            "Airflow task %s failed due to %s. Failure %s out of %s. Rescheduling.",
            task_key, job.status_reason, failure_count, self.MAX_SUBMIT_JOB_ATTEMPTS
        )

        # Increment failure counter
        self.active_workers.increment_failure_count(job.job_id)
        self.active_workers.pop_by_id(job.job_id)

        # Re-queue with exponential backoff
        self.pending_jobs.append(
            BatchQueuedJob(
                task_key,
                job_info.cmd,
                job_info.queue,
                job_info.config,
                failure_count + 1,
                timezone.utcnow() + calculate_next_attempt_delay(failure_count),
            )
        )
    else:
        # Max retries exceeded: Give up
        self.log.error("Task %s failed %s times. Marking as failed",
                       task_key, failure_count)
        self.active_workers.pop_by_id(job.job_id)
        self.fail(task_key)
```

**Failure Categories**:

1. **Infrastructure Failures** (retried by executor):
   - `ClientLimitExceeded`: Too many concurrent jobs
   - `ResourceInitializationError`: Failed to pull container image
   - `SpotInterruptionError`: Spot instance reclaimed
   - `InvalidParameterValue`: Misconfigured job definition

2. **Application Failures** (not retried by executor):
   - Task code raises exception → Job succeeds, task marked failed by Airflow
   - Database connection timeout → Logged as task failure
   - Import errors in DAG → Caught by Airflow, not Batch

---

## Configuration System

### Configuration Hierarchy

**Priority (highest to lowest)**:

1. **Per-Task `executor_config`** (in DAG definition)
2. **`submit_job_kwargs`** JSON (in `airflow.cfg`)
3. **Individual config keys** (in `airflow.cfg`)
4. **Default values** (hardcoded in `CONFIG_DEFAULTS`)

### Configuration Keys

**Location**: `providers/amazon/src/airflow/providers/amazon/aws/executors/batch/utils.py:139-157`

```python
class AllBatchConfigKeys(BatchSubmitJobKwargsConfigKeys):
    # Required keys
    JOB_NAME = "job_name"
    JOB_QUEUE = "job_queue"
    JOB_DEFINITION = "job_definition"
    REGION_NAME = "region_name"

    # Optional keys
    AWS_CONN_ID = "conn_id"
    MAX_SUBMIT_JOB_ATTEMPTS = "max_submit_job_attempts"
    SUBMIT_JOB_KWARGS = "submit_job_kwargs"
    CHECK_HEALTH_ON_STARTUP = "check_health_on_startup"
```

### Example Configuration

**airflow.cfg**:

```ini
[aws_batch_executor]
region_name = us-east-1
conn_id = aws_default
job_name = airflow-task
job_queue = airflow-batch-queue
job_definition = airflow-task-definition:5
max_submit_job_attempts = 3
check_health_on_startup = True

submit_job_kwargs = {
  "containerOverrides": {
    "vcpus": 1,
    "memory": 2048
  },
  "retryStrategy": {
    "attempts": 1
  },
  "timeout": {
    "attemptDurationSeconds": 3600
  }
}
```

**Per-Task Override** (in DAG):

```python
from airflow import DAG
from airflow.operators.python import PythonOperator

with DAG("example_dag") as dag:
    high_memory_task = PythonOperator(
        task_id="ml_training",
        python_callable=train_model,
        executor_config={
            "containerOverrides": {
                "vcpus": 4,
                "memory": 8192,
                "environment": [
                    {"name": "CUDA_VISIBLE_DEVICES", "value": "0"}
                ]
            }
        }
    )
```

**Merged Result**:

```json
{
  "jobName": "airflow-task",
  "jobQueue": "airflow-batch-queue",
  "jobDefinition": "airflow-task-definition:5",
  "containerOverrides": {
    "vcpus": 4,          // Overridden
    "memory": 8192,      // Overridden
    "command": [...],    // Injected by executor
    "environment": [
      {"name": "CUDA_VISIBLE_DEVICES", "value": "0"},  // Added
      {"name": "AIRFLOW_IS_EXECUTOR_CONTAINER", "value": "true"}  // Injected
    ]
  },
  "retryStrategy": {"attempts": 1},
  "timeout": {"attemptDurationSeconds": 3600}
}
```

---

## State Management

### State Tracking Data Structures

**Queued Tasks** (from BaseExecutor):

```python
self.queued_tasks: dict[TaskInstanceKey, workloads.ExecuteTask]
```

- **Populated By**: `queue_workload()` (called by scheduler)
- **Consumed By**: `_process_workloads()` (called by heartbeat)
- **State**: Tasks waiting for `trigger_tasks()` to move them to executor

**Pending Jobs** (executor-specific):

```python
self.pending_jobs: deque[BatchQueuedJob]
```

- **Populated By**: `execute_async()`
- **Consumed By**: `attempt_submit_jobs()`
- **State**: Tasks ready for AWS Batch submission (with retry metadata)

**Active Workers** (executor-specific):

```python
self.active_workers: BatchJobCollection
```

- **Populated By**: `attempt_submit_jobs()` (on successful submission)
- **Consumed By**: `sync_running_jobs()` (polling)
- **State**: Tasks running in AWS Batch (keyed by job ID)

**Running Set** (from BaseExecutor):

```python
self.running: set[TaskInstanceKey]
```

- **Populated By**: `_process_workloads()` (adds key after `execute_async`)
- **Consumed By**: BaseExecutor's heartbeat logic
- **Purpose**: Track which tasks are "in flight" from scheduler's perspective

### State Transition Diagram

```
┌─────────────────┐
│ Scheduler Queue │
└────────┬────────┘
         │ queue_workload()
         v
┌─────────────────┐
│ queued_tasks    │ (dict)
└────────┬────────┘
         │ _process_workloads()
         v
┌─────────────────┐
│ pending_jobs    │ (deque)
│ + running       │ (set)
└────────┬────────┘
         │ attempt_submit_jobs()
         │   ↓ (retry loop)
         v
┌─────────────────┐
│ active_workers  │ (BatchJobCollection)
│ + running       │ (set)
└────────┬────────┘
         │ sync_running_jobs()
         │
         ├─→ SUCCESS ──→ success(key) ──→ event_buffer
         │
         └─→ FAILED ──→ _handle_failed_job()
                            │
                            ├─→ Retry → pending_jobs
                            │
                            └─→ Give Up → fail(key) → event_buffer
```

### Event Buffer Communication

**Purpose**: Communicate task state changes from executor → scheduler.

**Populated By**: `self.success(key)`, `self.fail(key)`, `self.running_state(key, job_id)`

**Consumed By**: Scheduler's `BaseExecutor.get_event_buffer()` method

**Example**:

```python
# In executor
self.success(key)  # Sets event_buffer[key] = (State.SUCCESS, None)

# In scheduler
events = executor.get_event_buffer()
for key, (state, info) in events.items():
    if state == State.SUCCESS:
        ti = session.query(TaskInstance).filter_by(key=key).one()
        ti.state = State.SUCCESS
        ti.end_date = timezone.utcnow()
```

---

## Failure Handling & Retry Logic

### Two-Level Retry System

**Level 1: Job Submission Failures** (handled by `attempt_submit_jobs()`)

```python
MAX_SUBMIT_JOB_ATTEMPTS = 3  # Configurable

try:
    submit_job_response = self._submit_job(...)
except ClientError as e:
    if attempt_number < MAX_SUBMIT_JOB_ATTEMPTS:
        # Re-queue with exponential backoff
        batch_job.attempt_number += 1
        batch_job.next_attempt_time = (
            timezone.utcnow() +
            calculate_next_attempt_delay(attempt_number)
        )
        self.pending_jobs.append(batch_job)
    else:
        # Give up, mark task failed
        self.fail(key)
```

**Retry Triggers**:

- `ClientError`: AWS API failures (throttling, invalid parameters, etc.)
- `NoCredentialsError`: Missing or expired AWS credentials
- Generic `Exception`: Unexpected errors during submission

**Level 2: Job Execution Failures** (handled by `_handle_failed_job()`)

```python
if failure_count < MAX_SUBMIT_JOB_ATTEMPTS:
    # Increment counter and re-queue
    self.active_workers.increment_failure_count(job_id)
    self.active_workers.pop_by_id(job_id)
    self.pending_jobs.append(
        BatchQueuedJob(
            task_key,
            cmd,
            queue,
            exec_config,
            failure_count + 1,
            timezone.utcnow() + calculate_next_attempt_delay(failure_count),
        )
    )
else:
    # Max retries, give up
    self.fail(task_key)
```

**Retry Triggers**:

- AWS Batch marks job as `FAILED` status
- Typical causes: spot instance interruption, container startup failure, resource exhaustion

### Exponential Backoff Implementation

**Function**: `calculate_next_attempt_delay()` (from `exponential_backoff_retry.py`)

```python
def calculate_next_attempt_delay(
    attempt_number: int,
    max_delay: int = 120,      # 2 minutes
    exponent_base: int = 4
) -> timedelta:
    return timedelta(seconds=min((exponent_base ** attempt_number), max_delay))
```

**Backoff Schedule**:

```
Attempt 1:  4^1 =   4 seconds
Attempt 2:  4^2 =  16 seconds
Attempt 3:  4^3 =  64 seconds
Attempt 4:  4^4 = 256 seconds → capped at 120 seconds
```

**Why Exponential Backoff?**

1. **Rate Limiting**: Prevents overwhelming AWS Batch API with rapid retries
2. **Transient Failures**: Gives temporary issues (network glitches, spot interruptions) time to resolve
3. **Cost Efficiency**: Reduces unnecessary API calls during outages

### Failure Count Tracking

**Data Structure**: `BatchJobCollection.id_to_failure_counts`

```python
id_to_failure_counts: dict[str, int] = defaultdict(int)
```

**Lifecycle**:

1. **Initialization**: Set to `attempt_number` when job added to `active_workers`
2. **Increment**: `increment_failure_count()` called by `_handle_failed_job()`
3. **Reset**: Removed when job pops from `active_workers` (success or final failure)

**Why Track Per-Job ID?**: Same task may be retried multiple times with different Batch job IDs. Each new submission gets its own failure counter starting from the previous attempt number.

---

## Health Monitoring & Connection Management

### Health Check System

**Entry Point**: `check_health()` (line 146-171)

**Trigger Points**:

1. **Startup** (if `check_health_on_startup = True`):
   ```python
   def start(self):
       if conf.getboolean(CONFIG_GROUP_NAME, "CHECK_HEALTH_ON_STARTUP"):
           self.check_health()
   ```

2. **Connection Reload** (after authentication failure):
   ```python
   def load_batch_connection(self, check_connection=True):
       # ... reconnect logic ...
       if check_connection:
           self.check_health()
   ```

**Health Check Algorithm**:

```python
def check_health(self):
    try:
        # Send invalid job ID to test API connectivity
        invalid_job_id = "a" * 32
        self.batch.describe_jobs(jobs=[invalid_job_id])

        # If we get an empty response (not an error), connection is healthy
        self.IS_BOTO_CONNECTION_HEALTHY = True
        self.log.info("Batch Executor health check succeeded")

    except ClientError as ex:
        error_code = ex.response["Error"]["Code"]
        error_message = ex.response["Error"]["Message"]
        raise AirflowException(
            f"Health check failed: {error_code}: {error_message}. "
            "Executor will not run tasks until issue is resolved."
        )
```

**Why This Works**:

- Valid API credentials + network connectivity → Empty response (no matching job)
- Invalid credentials → `InvalidClientTokenId` exception
- Network issues → Connection timeout exception
- AWS outage → Service unavailable exception

### Connection Management

**State Tracking**:

```python
self.IS_BOTO_CONNECTION_HEALTHY: bool = False
self.attempts_since_last_successful_connection: int = 0
self.last_connection_reload: datetime
```

**Reconnection Logic** (in `sync()`, line 207-214):

```python
def sync(self):
    if not self.IS_BOTO_CONNECTION_HEALTHY:
        exponential_backoff_retry(
            self.last_connection_reload,
            self.attempts_since_last_successful_connection,
            self.load_batch_connection,
        )
        if not self.IS_BOTO_CONNECTION_HEALTHY:
            return  # Skip this sync cycle
```

**Exponential Backoff for Reconnection**:

```python
def exponential_backoff_retry(
    last_attempt_time: datetime,
    attempts_since_last_successful: int,
    callable_function: Callable,
    max_delay: int = 120,
    max_attempts: int = -1,  # No limit
    exponent_base: int = 4
):
    # Calculate next retry time
    next_retry_time = last_attempt_time + calculate_next_attempt_delay(
        attempts_since_last_successful, max_delay, exponent_base
    )

    # Only retry if enough time has passed
    if timezone.utcnow() >= next_retry_time:
        try:
            callable_function()  # Calls load_batch_connection()
        except Exception:
            log.exception("Reconnection failed")
```

**Connection Reload** (line 189-203):

```python
def load_batch_connection(self, check_connection=True):
    self.log.info("Loading Connection information")

    # Get AWS connection ID from config
    aws_conn_id = conf.get(CONFIG_GROUP_NAME, "conn_id", fallback="aws_default")
    region_name = conf.get(CONFIG_GROUP_NAME, "region_name")

    # Create new Boto3 client
    self.batch = BatchClientHook(
        aws_conn_id=aws_conn_id,
        region_name=region_name
    ).conn

    # Increment attempt counter
    self.attempts_since_last_successful_connection += 1
    self.last_connection_reload = timezone.utcnow()

    # Optional health check
    if check_connection:
        self.check_health()
        self.attempts_since_last_successful_connection = 0  # Reset on success
```

### Invalid Credential Detection

**Exception Codes** (line 70-74):

```python
INVALID_CREDENTIALS_EXCEPTIONS = [
    "ExpiredTokenException",        # IAM temporary credentials expired
    "InvalidClientTokenId",         # Access key ID not found
    "UnrecognizedClientException",  # Malformed credentials
]
```

**Handling in `sync()`** (line 218-224):

```python
def sync(self):
    try:
        self.sync_running_jobs()
        self.attempt_submit_jobs()
    except (ClientError, NoCredentialsError) as error:
        error_code = error.response["Error"]["Code"]
        if error_code in INVALID_CREDENTIALS_EXCEPTIONS:
            self.IS_BOTO_CONNECTION_HEALTHY = False
            self.log.warning("AWS credentials expired. Retrying connection")
```

**Handling in `attempt_submit_jobs()`** (line 318-325):

```python
def attempt_submit_jobs(self):
    for batch_job in self.pending_jobs:
        try:
            submit_job_response = self._submit_job(...)
        except (ClientError, NoCredentialsError) as e:
            error_code = e.response["Error"]["Code"]
            if error_code in INVALID_CREDENTIALS_EXCEPTIONS:
                # Re-queue job and mark unhealthy
                self.pending_jobs.append(batch_job)
                raise  # Propagate to sync() to trigger reconnection
```

---

## Task Adoption Mechanism

### Purpose

When the Airflow scheduler restarts (crash, upgrade, or deliberate restart), tasks that were running in AWS Batch become "orphaned" because the executor lost track of them. **Task adoption** allows the new scheduler instance to "adopt" these orphaned tasks and continue monitoring them.

### Implementation

**Entry Point**: `try_adopt_task_instances(tis)` (line 483-517)

**Called By**: Scheduler's `_process_executor_events()` during startup

**Algorithm**:

```python
def try_adopt_task_instances(self, tis: Sequence[TaskInstance]) -> Sequence[TaskInstance]:
    adopted_tis: list[TaskInstance] = []

    # Step 1: Extract Batch job IDs from task instances
    job_ids = [ti.external_executor_id for ti in tis if ti.external_executor_id]

    if not job_ids:
        return tis  # No jobs to adopt

    # Step 2: Query AWS Batch for job status
    batch_jobs = self._describe_jobs(job_ids)

    # Step 3: Re-register jobs in active_workers
    for batch_job in batch_jobs:
        # Find corresponding TaskInstance
        ti = next(ti for ti in tis if ti.external_executor_id == batch_job.job_id)

        # Re-create tracking entry
        self.active_workers.add_job(
            job_id=batch_job.job_id,
            airflow_task_key=ti.key,
            airflow_cmd=ti.command_as_list(),  # Reconstruct command
            queue=ti.queue,
            exec_config=ti.executor_config,
            attempt_number=ti.try_number,
        )

        adopted_tis.append(ti)

    # Step 4: Log adoption
    if adopted_tis:
        task_str = "\n\t".join([f"{ti} in state {ti.state}" for ti in adopted_tis])
        self.log.info(
            "Adopted %d tasks from dead executor:\n\t%s",
            len(adopted_tis),
            task_str
        )

    # Step 5: Return unadopted tasks (will be cleared and re-queued)
    not_adopted_tis = [ti for ti in tis if ti not in adopted_tis]
    return not_adopted_tis
```

### Adoption Flow Diagram

```
┌────────────────────────────────────────────────────────────┐
│ Scheduler Restart                                           │
└────────────────────┬───────────────────────────────────────┘
                     │
                     v
┌────────────────────────────────────────────────────────────┐
│ Query Database for Running Tasks                           │
│ SELECT * FROM task_instance WHERE state='running'          │
└────────────────────┬───────────────────────────────────────┘
                     │
                     v
┌────────────────────────────────────────────────────────────┐
│ Filter by external_executor_id IS NOT NULL                 │
│ (Only adopt tasks with Batch job IDs)                      │
└────────────────────┬───────────────────────────────────────┘
                     │
                     v
┌────────────────────────────────────────────────────────────┐
│ Call try_adopt_task_instances(tis)                         │
└────────────────────┬───────────────────────────────────────┘
                     │
                     v
┌────────────────────────────────────────────────────────────┐
│ AWS Batch: describe_jobs(job_ids)                          │
│ (Check if jobs still exist)                                │
└────────────────────┬───────────────────────────────────────┘
                     │
                     ├─→ Job Found ──→ Add to active_workers ──→ Adopted
                     │
                     └─→ Job Not Found ──→ Not Adopted ──→ Cleared & Re-queued
```

### External Executor ID

**Purpose**: Store the AWS Batch job ID in the Airflow database so it can be retrieved after restart.

**Set In**: `running_state(key, job_id)` (called by `attempt_submit_jobs()`)

```python
def running_state(self, key: TaskInstanceKey, job_id: str):
    # Update event buffer
    self.event_buffer[key] = (State.RUNNING, None)

    # Store job ID in TaskInstance.external_executor_id
    # (This happens in BaseExecutor.change_state())
```

**Database Column**: `task_instance.external_executor_id` (VARCHAR)

**Example Value**: `"3a1b5f2c-9d8e-4a7b-a1c3-2f5e8d9c7a6b"` (AWS Batch job ID)

### Adoption vs. Re-Execution

| Scenario | Behavior |
|----------|----------|
| Job still running in Batch | **Adopted** - Executor resumes monitoring |
| Job completed in Batch | **Adopted** - Executor syncs final state immediately |
| Job not found in Batch | **Not adopted** - Task cleared and re-queued |
| Job failed in Batch | **Adopted** - Executor handles failure (retry or fail) |

---

## Airflow 3.0 Workload Support

### Background

Airflow 3.0 introduces the **Task Execution Interface (TEI)** and **Task SDK**, which decouples task execution from core Airflow. Instead of shell commands, tasks are represented as `ExecuteTask` workload objects that are JSON-serializable.

### Workload Processing

**Entry Point**: `_process_workloads(workloads)` (line 130-144)

```python
def _process_workloads(self, workloads: Sequence[workloads.All]) -> None:
    from airflow.executors.workloads import ExecuteTask

    for w in workloads:
        # Validate workload type
        if not isinstance(w, ExecuteTask):
            raise RuntimeError(f"Cannot handle workload type {type(w)}")

        # Wrap workload as single-element list
        command = [w]  # NOT a shell command!

        # Extract task metadata
        key = w.ti.key
        queue = w.ti.queue
        executor_config = w.ti.executor_config or {}

        # Remove from queued_tasks and submit
        del self.queued_tasks[key]
        self.execute_async(key, command, queue, executor_config)
        self.running.add(key)
```

**Command Transformation** (in `execute_async()`, line 383-399):

```python
def execute_async(self, key, command, queue, executor_config):
    # Detect Airflow 3.0 workload
    if len(command) == 1 and isinstance(command[0], ExecuteTask):
        workload = command[0]

        # Serialize workload to JSON
        ser_input = workload.model_dump_json()

        # Build Python command to deserialize and execute
        command = [
            "python",
            "-m",
            "airflow.sdk.execution_time.execute_workload",
            "--json-string",
            ser_input,
        ]

    # Queue for submission
    self.pending_jobs.append(
        BatchQueuedJob(key, command, queue, executor_config, 1, timezone.utcnow())
    )
```

### Workload Execution in Container

**Container Entrypoint**: `python -m airflow.sdk.execution_time.execute_workload`

**Flow**:

```
┌────────────────────────────────────────────────────────────┐
│ AWS Batch Container Starts                                  │
└────────────────────┬───────────────────────────────────────┘
                     │
                     v
┌────────────────────────────────────────────────────────────┐
│ Python Module: airflow.sdk.execution_time.execute_workload │
│                                                             │
│ 1. Parse --json-string argument                            │
│ 2. Deserialize to ExecuteTask object                       │
│ 3. Load DAG and task definitions                           │
│ 4. Execute task logic                                      │
│ 5. Write results to database                               │
└────────────────────────────────────────────────────────────┘
```

**Advantages of Workload Approach**:

1. **Type Safety**: Strong typing throughout execution pipeline
2. **Versioning**: Workload schema can evolve independently
3. **SDK Isolation**: Task containers only need Task SDK, not full Airflow
4. **Cross-Language**: Future support for non-Python tasks (Go, Rust, etc.)

### Version Compatibility

**Type Annotation** (line 108-111):

```python
if TYPE_CHECKING and AIRFLOW_V_3_0_PLUS:
    # In Airflow 3.0+, queued_tasks stores workloads, not strings
    queued_tasks: dict[TaskInstanceKey, workloads.All]
```

**Runtime Detection** (line 383-386):

```python
if len(command) == 1:
    if isinstance(command[0], ExecuteTask):
        # Airflow 3.0 workload
        ...
    else:
        raise ValueError(f"Unknown workload type: {type(command[0])}")
```

**Backward Compatibility**: Airflow 2.x continues to use shell commands (e.g., `airflow tasks run dag_id task_id ...`).

---

## Integration with AWS Batch API

### Boto3 Client Initialization

**Hook**: `BatchClientHook` (from `airflow.providers.amazon.aws.hooks.batch_client`)

```python
def load_batch_connection(self):
    aws_conn_id = conf.get(CONFIG_GROUP_NAME, "conn_id", fallback="aws_default")
    region_name = conf.get(CONFIG_GROUP_NAME, "region_name")

    # BatchClientHook handles:
    # - Loading AWS credentials from Airflow connection
    # - Creating boto3.client('batch')
    # - Configuring retry logic
    self.batch = BatchClientHook(
        aws_conn_id=aws_conn_id,
        region_name=region_name
    ).conn
```

### API Calls

#### 1. submit_job

**Purpose**: Submit a new Batch job (task execution request)

**Call Site**: `_submit_job()` (line 423)

```python
boto_submit_job = self.batch.submit_job(**submit_job_api)
```

**Request Schema**:

```json
{
  "jobName": "string",               # Required
  "jobQueue": "string",              # Required
  "jobDefinition": "string",         # Required
  "containerOverrides": {
    "command": ["string"],           # Task execution command
    "vcpus": 1,
    "memory": 2048,
    "environment": [
      {"name": "KEY", "value": "VALUE"}
    ]
  },
  "retryStrategy": {
    "attempts": 1                    # AWS Batch retry (set to 1, Airflow handles retries)
  },
  "timeout": {
    "attemptDurationSeconds": 3600
  }
}
```

**Response Schema** (parsed by `BatchSubmitJobResponseSchema`):

```json
{
  "jobId": "3a1b5f2c-9d8e-4a7b-a1c3-2f5e8d9c7a6b",  # Required
  "jobName": "airflow-task-...",
  "jobArn": "arn:aws:batch:us-east-1:123456789012:job/..."
}
```

#### 2. describe_jobs

**Purpose**: Poll job status (monitoring loop)

**Call Site**: `_describe_jobs()` (line 372)

```python
boto_describe_tasks = self.batch.describe_jobs(jobs=batched_job_ids)
```

**Request**: List of job IDs (max 99 per call)

```json
{
  "jobs": [
    "3a1b5f2c-9d8e-4a7b-a1c3-2f5e8d9c7a6b",
    "5c3d7e9f-1a2b-4c5d-6e7f-8a9b0c1d2e3f"
  ]
}
```

**Response Schema** (parsed by `BatchDescribeJobsResponseSchema`):

```json
{
  "jobs": [
    {
      "jobId": "3a1b5f2c-...",
      "jobName": "airflow-task-...",
      "status": "RUNNING",              # Required
      "statusReason": "Task is running", # Optional
      "createdAt": 1640000000,
      "startedAt": 1640000010,
      "stoppedAt": null,
      "container": {
        "exitCode": null,
        "logStreamName": "airflow-task-..."
      }
    }
  ]
}
```

**Status Values**:

- `SUBMITTED`: Job received by Batch, awaiting scheduling
- `PENDING`: Waiting for compute resources
- `RUNNABLE`: Eligible to run, awaiting compute provisioning
- `STARTING`: Container is starting
- `RUNNING`: Container executing task
- `SUCCEEDED`: Container exited with code 0
- `FAILED`: Container exited with non-zero code or infra failure

#### 3. terminate_job

**Purpose**: Kill running jobs during executor shutdown

**Call Site**: `terminate()` (line 462)

```python
self.batch.terminate_job(
    jobId=job_id,
    reason="Airflow Executor received a SIGTERM"
)
```

**Request**:

```json
{
  "jobId": "3a1b5f2c-...",
  "reason": "Airflow Executor received a SIGTERM"
}
```

**Effect**: Job status changes to `FAILED` with given reason. Container receives SIGTERM, then SIGKILL after grace period.

### Rate Limiting & Throttling

**AWS Batch API Limits** (as of 2024):

| API Call | Limit | Burst |
|----------|-------|-------|
| submit_job | 50/sec | 100 |
| describe_jobs | 50/sec | 100 |
| terminate_job | 50/sec | 100 |

**Executor Mitigation**:

1. **Batching**: `describe_jobs` batches up to 99 job IDs per call
2. **Exponential Backoff**: `ClientError` with `ThrottlingException` triggers retry with delay
3. **Pending Queue**: Failed submissions are re-queued, not re-attempted immediately

---

## Comparison with BaseExecutor

### BaseExecutor Interface

**Mandatory Overrides**:

- `sync()`: Poll task status and update event buffer
- `_process_workloads()`: Convert queued workloads to executor-specific format

**Optional Overrides**:

- `start()`: One-time initialization (health check)
- `end()`: Graceful shutdown (wait for tasks)
- `terminate()`: Forced shutdown (kill tasks)
- `try_adopt_task_instances()`: Scheduler restart recovery

### AwsBatchExecutor Implementation

| BaseExecutor Method | AwsBatchExecutor Implementation |
|---------------------|----------------------------------|
| `queue_workload()` | ✅ Overridden (validation) |
| `_process_workloads()` | ✅ Overridden (convert workload → command) |
| `execute_async()` | ⚠️ Not in interface, but called by `_process_workloads()` |
| `sync()` | ✅ Overridden (health check + sync jobs + submit jobs) |
| `start()` | ✅ Overridden (health check) |
| `end()` | ✅ Overridden (wait for active_workers) |
| `terminate()` | ✅ Overridden (call terminate_job API) |
| `try_adopt_task_instances()` | ✅ Overridden (query Batch for job IDs) |

### Key Differences from Local/Celery Executors

**LocalExecutor**:

- Runs tasks as subprocesses on scheduler machine
- No external dependencies
- Limited scalability (single machine)
- No retry logic (scheduler handles retries)

**CeleryExecutor**:

- Runs tasks on persistent Celery workers
- Requires Redis/RabbitMQ message broker
- Workers must be pre-provisioned
- Horizontal scaling via worker pools

**AwsBatchExecutor**:

- Runs tasks in ephemeral containers on AWS Batch
- Requires AWS account + IAM permissions
- **Autoscaling**: Compute provisioned on-demand
- **Two-level retries**: Submission failures + execution failures
- **Stateless**: No persistent workers
- **Remote logging required**: Container logs not accessible after termination

---

## Deployment Requirements

### 1. Shared Database

**Requirement**: All components (scheduler, webserver, task containers) must access the same PostgreSQL/MySQL database.

**Setup**:

```bash
# AWS RDS PostgreSQL
aws rds create-db-instance \
  --db-instance-identifier airflow-db \
  --db-instance-class db.t3.medium \
  --engine postgres \
  --master-username airflow \
  --master-user-password <password> \
  --allocated-storage 20 \
  --vpc-security-group-ids sg-xxxxx
```

**Configuration** (in task container):

```bash
export AIRFLOW__DATABASE__SQL_ALCHEMY_CONN="postgresql://airflow:password@airflow-db.xxxxx.rds.amazonaws.com:5432/airflow"
```

### 2. Container Image

**Requirements**:

- Based on `apache/airflow:latest` or custom image
- **Must match scheduler's Airflow version** (e.g., 3.0.0)
- **Must match scheduler's Python version** (e.g., 3.11)
- Includes DAG files (or loads from S3)
- Configured for remote logging

**Dockerfile Example**:

```dockerfile
FROM apache/airflow:3.0.0-python3.11

# Install additional dependencies
COPY requirements.txt /tmp/
RUN pip install -r /tmp/requirements.txt

# Copy DAG files (or configure to load from S3)
COPY dags/ ${AIRFLOW_HOME}/dags/

# Configure remote logging
ENV AIRFLOW__LOGGING__REMOTE_LOGGING=True
ENV AIRFLOW__LOGGING__REMOTE_BASE_LOG_FOLDER=s3://airflow-logs/
ENV AIRFLOW__LOGGING__REMOTE_LOG_CONN_ID=aws_default
```

**Push to ECR**:

```bash
aws ecr get-login-password --region us-east-1 | docker login --username AWS --password-stdin 123456789012.dkr.ecr.us-east-1.amazonaws.com
docker build -t airflow-batch .
docker tag airflow-batch:latest 123456789012.dkr.ecr.us-east-1.amazonaws.com/airflow-batch:latest
docker push 123456789012.dkr.ecr.us-east-1.amazonaws.com/airflow-batch:latest
```

### 3. AWS Batch Resources

**a. Compute Environment**:

```bash
aws batch create-compute-environment \
  --compute-environment-name airflow-compute-env \
  --type MANAGED \
  --state ENABLED \
  --compute-resources \
    type=FARGATE,maxvCpus=256,subnets=subnet-xxxxx,securityGroupIds=sg-xxxxx
```

**b. Job Queue**:

```bash
aws batch create-job-queue \
  --job-queue-name airflow-batch-queue \
  --state ENABLED \
  --priority 1 \
  --compute-environment-order order=1,computeEnvironment=airflow-compute-env
```

**c. Job Definition**:

```bash
aws batch register-job-definition \
  --job-definition-name airflow-task-definition \
  --type container \
  --platform-capabilities FARGATE \
  --container-properties '{
    "image": "123456789012.dkr.ecr.us-east-1.amazonaws.com/airflow-batch:latest",
    "resourceRequirements": [
      {"type": "VCPU", "value": "1"},
      {"type": "MEMORY", "value": "2048"}
    ],
    "executionRoleArn": "arn:aws:iam::123456789012:role/ecsTaskExecutionRole",
    "jobRoleArn": "arn:aws:iam::123456789012:role/airflowBatchJobRole",
    "fargatePlatformConfiguration": {"platformVersion": "LATEST"},
    "networkConfiguration": {
      "assignPublicIp": "ENABLED"
    }
  }'
```

### 4. IAM Roles

**Execution Role** (for Batch to pull image and write logs):

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "ecr:GetAuthorizationToken",
        "ecr:BatchCheckLayerAvailability",
        "ecr:GetDownloadUrlForLayer",
        "ecr:BatchGetImage",
        "logs:CreateLogStream",
        "logs:PutLogEvents"
      ],
      "Resource": "*"
    }
  ]
}
```

**Job Role** (for task container to access AWS resources):

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "s3:GetObject",
        "s3:PutObject",
        "s3:ListBucket"
      ],
      "Resource": [
        "arn:aws:s3:::airflow-logs/*",
        "arn:aws:s3:::airflow-dags/*"
      ]
    }
  ]
}
```

### 5. Remote Logging

**S3 Configuration**:

```ini
[logging]
remote_logging = True
remote_base_log_folder = s3://airflow-logs/
remote_log_conn_id = aws_default
encrypt_s3_logs = True
```

**CloudWatch Configuration**:

```ini
[logging]
remote_logging = True
remote_base_log_folder = cloudwatch://airflow-logs
remote_log_conn_id = aws_default
```

**Why Required?**: Container logs are lost when container terminates. Remote logging persists logs for debugging.

### 6. Networking

**Requirements**:

- Task containers must reach RDS database (port 5432/3306)
- Task containers must reach S3/CloudWatch (HTTPS)
- Scheduler must reach AWS Batch API (HTTPS)

**Security Group Rules**:

```bash
# Allow scheduler → Batch API (implicit via AWS SDK)

# Allow task containers → RDS
aws ec2 authorize-security-group-ingress \
  --group-id sg-rds-xxxxx \
  --protocol tcp \
  --port 5432 \
  --source-group sg-batch-containers-xxxxx

# Allow task containers → Internet (for S3/CloudWatch)
# (If using private subnets, configure NAT Gateway or VPC endpoints)
```

---

## Performance Considerations

### Scalability

**Horizontal Scaling**:

- AWS Batch autoscales compute based on queue depth
- No limit on concurrent tasks (beyond AWS account limits)
- Scheduler bottleneck: `describe_jobs` polling frequency

**Vertical Scaling**:

- Per-task resource allocation via `executor_config`
- Can mix CPU-intensive and memory-intensive tasks in same queue

**Limitations**:

- Cold start latency (5-60 seconds per container)
- AWS API rate limits (50 calls/sec per API)

### Latency Sources

**Task Startup**:

```
Scheduler queues task           →  < 1 second
Executor submits to Batch       →  < 1 second
Batch schedules job             →  1-5 seconds
Compute provisioning            →  10-60 seconds (Fargate)
                                →  2-10 seconds (EC2 with pre-warmed instances)
Container image pull            →  5-30 seconds (first pull)
                                →  < 1 second (cached)
Airflow worker startup          →  2-5 seconds
───────────────────────────────────────────────────
Total cold start                →  20-100 seconds
Total warm start (cached image) →  10-20 seconds
```

**Mitigation**:

- Use **EC2 compute environments** with min vCPUs > 0 for pre-warmed instances
- Optimize Docker image size (multi-stage builds, minimal dependencies)
- Use **ECR image caching** for faster pulls

### Monitoring & Observability

**Metrics to Track**:

```python
# Executor metrics (built-in)
executor.open_slots                # Available parallelism
executor.queued_tasks              # Tasks waiting to submit
executor.running_tasks             # Tasks in Batch

# Custom metrics (via StatsD)
Stats.incr("batch_executor.submit_success")
Stats.incr("batch_executor.submit_failure")
Stats.timing("batch_executor.submit_duration", duration_ms)
```

**CloudWatch Metrics**:

- `AWS/Batch` → `CPUUtilization`, `MemoryUtilization`
- `AWS/Batch` → `RunningJobs`, `PendingJobs`, `SucceededJobs`, `FailedJobs`

**Logging**:

```python
self.log.info("Submitted job %s for task %s", job_id, task_key)
self.log.warning("Job %s failed due to %s", job_id, status_reason)
self.log.error("Max retries exceeded for task %s", task_key)
```

### Cost Optimization

**Strategies**:

1. **Spot Instances**: Use spot compute for non-critical tasks (50-90% savings)
   ```json
   "allocationStrategy": "SPOT_CAPACITY_OPTIMIZED"
   ```

2. **Right-Sizing**: Use small instances for light tasks
   ```python
   executor_config = {
       "containerOverrides": {
           "vcpus": 0.25,  # Minimum for Fargate
           "memory": 512
       }
   }
   ```

3. **Compute Environment Scaling**:
   ```json
   "minvCpus": 0,      # Scale to zero when idle
   "desiredvCpus": 0,
   "maxvCpus": 256
   ```

4. **Batch Job Retries**: Set `retryStrategy.attempts = 1` (Airflow handles retries more intelligently)

---

## Code References

### Primary Files

1. **batch_executor.py** (`providers/amazon/src/airflow/providers/amazon/aws/executors/batch/batch_executor.py`)
   - Core executor implementation (527 lines)
   - Key methods: `sync()`, `attempt_submit_jobs()`, `sync_running_jobs()`, `_handle_failed_job()`

2. **utils.py** (`providers/amazon/src/airflow/providers/amazon/aws/executors/batch/utils.py`)
   - Data structures: `BatchJobCollection`, `BatchQueuedJob`, `BatchJob`
   - Configuration keys and defaults

3. **batch_executor_config.py** (`providers/amazon/src/airflow/providers/amazon/aws/executors/batch/batch_executor_config.py`)
   - Configuration parsing and validation
   - `build_submit_kwargs()` function

4. **boto_schema.py** (`providers/amazon/src/airflow/providers/amazon/aws/executors/batch/boto_schema.py`)
   - Marshmallow schemas for API response parsing
   - Type-safe DTO conversions

5. **exponential_backoff_retry.py** (`providers/amazon/src/airflow/providers/amazon/aws/executors/utils/exponential_backoff_retry.py`)
   - Retry logic utilities
   - `calculate_next_attempt_delay()`, `exponential_backoff_retry()`

### Key Line References

- **Executor initialization**: `batch_executor.py:113-120`
- **Task queuing**: `batch_executor.py:122-128`
- **Task processing**: `batch_executor.py:130-144`
- **Async execution**: `batch_executor.py:378-410`
- **Job submission**: `batch_executor.py:296-364`
- **Status polling**: `batch_executor.py:230-244`
- **Failure handling**: `batch_executor.py:246-294`
- **Task adoption**: `batch_executor.py:483-517`
- **Health check**: `batch_executor.py:146-171`
- **Connection management**: `batch_executor.py:189-203`

### Test Files

1. **test_batch_executor.py** (`providers/amazon/tests/unit/amazon/aws/executors/batch/test_batch_executor.py`)
   - Unit tests for executor logic
   - Mocked Boto3 client
   - Test cases: job submission, failure handling, state transitions

2. **test_utils.py** (`providers/amazon/tests/unit/amazon/aws/executors/batch/test_utils.py`)
   - Tests for data structures (BatchJobCollection, BatchJob)

---

## Summary

The **AWS Batch Executor** is a sophisticated, production-ready executor that leverages AWS Batch's managed compute infrastructure to run Airflow tasks at scale. Its implementation demonstrates several advanced patterns:

1. **Two-Level Retry System**: Handles both submission failures (API errors) and execution failures (container crashes)
2. **Exponential Backoff**: Prevents API throttling and gives transient failures time to resolve
3. **Health Monitoring**: Proactive connection health checks with automatic reconnection
4. **Task Adoption**: Graceful recovery from scheduler restarts
5. **Airflow 3.0 Support**: First-class support for Task SDK workloads
6. **Type Safety**: Marshmallow schemas for API response validation

**Use Cases**:

- **Variable Workloads**: Tasks with unpredictable resource requirements
- **Batch Processing**: High-throughput, parallel task execution
- **Cost Optimization**: Scale-to-zero when idle, use spot instances
- **Isolation**: Tasks with conflicting dependencies or security requirements

**Trade-offs**:

- **Latency**: Cold start overhead (20-100 seconds)
- **Complexity**: Requires AWS infrastructure setup
- **Logging**: Remote logging mandatory
- **Debugging**: Container logs not accessible after termination

**Best Practices**:

1. Use EC2 compute environments with pre-warmed instances for latency-sensitive tasks
2. Set `retryStrategy.attempts = 1` in Batch job definition (Airflow handles retries)
3. Monitor `pending_jobs` length for API throttling issues
4. Configure health checks (`check_health_on_startup = True`)
5. Use per-task `executor_config` for resource heterogeneity
