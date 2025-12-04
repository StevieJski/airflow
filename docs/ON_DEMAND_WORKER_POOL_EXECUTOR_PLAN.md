# On-Demand Worker Pool Executor - Implementation Plan

## Overview

This document outlines the implementation plan for a new AWS Batch executor variant that spawns **on-demand, long-running worker processes** instead of one ephemeral job per task. Workers pull tasks from an SQS queue, execute them in isolated subprocesses, and terminate after an idle timeout.

### Design Decisions (Based on User Requirements)

| Aspect | Decision | Rationale |
|--------|----------|-----------|
| **Worker Lifecycle** | Hybrid (idle timeout + manual termination) | Workers run until DAG completes; idle timeout provides cleanup for orphaned workers |
| **Worker Scaling** | Respect BaseExecutor parallelism limit | Integrates with Airflow's existing concurrency controls |
| **State Reporting** | SQS result queue | Decouples workers from DB; executor polls results |
| **Task Isolation** | Fork subprocess per task | Prevents memory leaks; enables shared state in parent process |

---

## Compute Environment Compatibility

The executor is designed to be **compute-environment agnostic**. AWS Batch supports multiple compute environments, and this design works with all of them:

### Supported Compute Environments

| Compute Type | Supported | Notes |
|--------------|-----------|-------|
| **Fargate** | ✅ Yes | Serverless, per-second billing, no GPU |
| **EC2** | ✅ Yes | Full instance control, GPU support, spot instances |
| **EC2 Spot** | ✅ Yes | Cost savings, interruption handling via SQS visibility timeout |
| **EKS** | ❌ No | Blocked by upstream `eksPropertiesOverride` restriction |

### Compute-Agnostic Design Principles

The executor does **not** assume any specific compute environment. All compute-specific configuration is delegated to:

1. **Job Definition**: Platform capabilities, resource requirements, networking
2. **Compute Environment**: Instance types, scaling, spot/on-demand mix
3. **`submit_job_kwargs` config**: Any compute-specific overrides

### Compute Environment Considerations

| Aspect | Fargate | EC2 |
|--------|---------|-----|
| **Cold start** | 30-60s (container pull) | 2-10s (if warm pool) or 60-120s (scale up) |
| **Max duration** | Unlimited | Unlimited |
| **Spot interruption** | 2 min warning | 2 min warning |
| **GPU support** | ❌ No | ✅ Yes |
| **Networking** | `assignPublicIp` or NAT | VPC with NAT or public subnet |
| **Shared storage** | EFS only | EBS, EFS, instance store |

### Handling Spot Interruptions

For EC2 Spot or Fargate Spot, the worker handles interruptions gracefully:

1. **SIGTERM received**: Worker sets `self.running = False`
2. **Current message returned**: `ChangeMessageVisibility(VisibilityTimeout=0)` returns task to queue
3. **Task retried**: Another worker picks up the task from SQS

This is already implemented in the worker's `_handle_sigterm()` method.

### Example Job Definitions

**Fargate**:
```json
{
  "platformCapabilities": ["FARGATE"],
  "containerProperties": {
    "resourceRequirements": [
      {"type": "VCPU", "value": "2"},
      {"type": "MEMORY", "value": "4096"}
    ],
    "networkConfiguration": {
      "assignPublicIp": "ENABLED"
    }
  }
}
```

**EC2**:
```json
{
  "platformCapabilities": ["EC2"],
  "containerProperties": {
    "vcpus": 2,
    "memory": 4096,
    "resourceRequirements": [
      {"type": "GPU", "value": "1"}
    ]
  }
}
```

**EC2 with GPU** (for ML workloads):
```json
{
  "platformCapabilities": ["EC2"],
  "containerProperties": {
    "vcpus": 4,
    "memory": 16384,
    "resourceRequirements": [
      {"type": "GPU", "value": "1"}
    ],
    "linuxParameters": {
      "devices": [
        {"hostPath": "/dev/nvidia0", "containerPath": "/dev/nvidia0", "permissions": ["read", "write"]}
      ]
    }
  }
}
```

### Best Practices by Use Case

| Use Case | Recommended Compute | Rationale |
|----------|---------------------|-----------|
| **Variable workloads** | Fargate | No capacity management, pay-per-use |
| **Cost-sensitive** | EC2 Spot | Up to 90% savings, use with SQS retry |
| **GPU/ML inference** | EC2 (GPU instances) | GPU support not available on Fargate |
| **Long initialization** | EC2 with min capacity | Keep warm instances for faster cold start |
| **Predictable load** | EC2 On-Demand | Reserved capacity, no interruptions |

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                           Airflow Scheduler                                  │
│                                                                              │
│  ┌────────────────────────────────────────────────────────────────────────┐ │
│  │              AwsBatchWorkerPoolExecutor                                 │ │
│  │                                                                          │ │
│  │  ┌──────────────┐     ┌──────────────┐     ┌──────────────────────────┐│ │
│  │  │ queued_tasks │ ──> │ SQS Task     │ ──> │ active_workers           ││ │
│  │  │ (from sched) │     │ Queue        │     │ (BatchJobCollection)     ││ │
│  │  └──────────────┘     └──────────────┘     └──────────────────────────┘│ │
│  │         │                    │                         │                 │ │
│  │         │                    │                         │                 │ │
│  │         v                    v                         v                 │ │
│  │  _process_workloads()  Workers pull tasks      sync() polls workers     │ │
│  │  sends to SQS          from queue              & result queue           │ │
│  │                                                                          │ │
│  │  ┌──────────────────────────────────────────────────────────────────┐  │ │
│  │  │ SQS Result Queue  <──  Workers report SUCCESS/FAILED             │  │ │
│  │  └──────────────────────────────────────────────────────────────────┘  │ │
│  └────────────────────────────────────────────────────────────────────────┘ │
└──────────────────────────────────────────────────────────────────────────────┘
                                    │
                                    │ Boto3 API
                                    v
┌──────────────────────────────────────────────────────────────────────────────┐
│                              AWS Batch                                        │
│                                                                               │
│   ┌─────────────────────────────────────────────────────────────────────┐    │
│   │                    Long-Running Worker Containers                    │    │
│   │                                                                      │    │
│   │  ┌────────────────────────┐   ┌────────────────────────┐           │    │
│   │  │ Worker 1               │   │ Worker 2               │           │    │
│   │  │                        │   │                        │           │    │
│   │  │ ┌────────────────────┐ │   │ ┌────────────────────┐ │           │    │
│   │  │ │ Shared State       │ │   │ │ Shared State       │ │           │    │
│   │  │ │ - ML Models        │ │   │ │ - ML Models        │ │           │    │
│   │  │ │ - DB Pools         │ │   │ │ - DB Pools         │ │           │    │
│   │  │ │ - Cached Data      │ │   │ │ - Cached Data      │ │           │    │
│   │  │ └────────────────────┘ │   │ └────────────────────┘ │           │    │
│   │  │          │             │   │          │             │           │    │
│   │  │          v             │   │          v             │           │    │
│   │  │ ┌──────────────────┐   │   │ ┌──────────────────┐   │           │    │
│   │  │ │ Task Subprocess  │   │   │ │ Task Subprocess  │   │           │    │
│   │  │ │ (fork per task)  │   │   │ │ (fork per task)  │   │           │    │
│   │  │ └──────────────────┘   │   │ └──────────────────┘   │           │    │
│   │  │                        │   │                        │           │    │
│   │  │  Poll SQS ──> Execute ──> Report Result            │           │    │
│   │  │                        │   │                        │           │    │
│   │  │  Idle timeout: 5 min   │   │  Idle timeout: 5 min   │           │    │
│   │  └────────────────────────┘   └────────────────────────┘           │    │
│   │                                                                      │    │
│   └─────────────────────────────────────────────────────────────────────┘    │
│                                                                               │
│   Shared Database (RDS) <──────── Task subprocess updates TI state          │
│   Remote Logging (S3)   <──────── Task logs uploaded after execution        │
└───────────────────────────────────────────────────────────────────────────────┘
```

---

## Component Design

### 1. Configuration Schema

**New Config Section**: `[aws_batch_worker_pool_executor]`

```ini
[aws_batch_worker_pool_executor]
# AWS Connection
conn_id = aws_default
region_name = us-east-1

# AWS Batch Resources
job_queue = airflow-worker-pool-queue
job_definition = airflow-worker-pool:1
worker_job_name_prefix = airflow-worker

# SQS Queues (auto-created if not exists)
task_queue_url = https://sqs.us-east-1.amazonaws.com/123456789012/airflow-tasks
result_queue_url = https://sqs.us-east-1.amazonaws.com/123456789012/airflow-results

# Worker Lifecycle
worker_idle_timeout_seconds = 300          # 5 minutes idle before self-termination
worker_max_tasks = 0                       # 0 = unlimited (rely on idle timeout)
worker_visibility_timeout_seconds = 3600   # 1 hour per task (SQS visibility)

# Scaling (respects BaseExecutor parallelism by default)
min_workers = 0                            # Start with 0 workers (on-demand)
max_workers = 0                            # 0 = use parallelism setting
tasks_per_worker = 1                       # How many concurrent tasks per worker

# Health & Monitoring
check_health_on_startup = true
worker_health_check_interval_seconds = 60
max_worker_start_attempts = 3

# Shared State Initialization (optional)
shared_state_init_module =                 # e.g., myproject.worker_init
shared_state_init_function = initialize    # Function to call for initialization

# Submit Job kwargs (JSON)
submit_job_kwargs = {}
```

**New Config Key Classes**:

```python
# providers/amazon/src/airflow/providers/amazon/aws/executors/batch/worker_pool_utils.py

CONFIG_GROUP_NAME = "aws_batch_worker_pool_executor"

CONFIG_DEFAULTS = {
    "conn_id": "aws_default",
    "worker_idle_timeout_seconds": "300",
    "worker_max_tasks": "0",
    "worker_visibility_timeout_seconds": "3600",
    "min_workers": "0",
    "max_workers": "0",
    "tasks_per_worker": "1",
    "check_health_on_startup": "True",
    "worker_health_check_interval_seconds": "60",
    "max_worker_start_attempts": "3",
}

class WorkerPoolConfigKeys(BaseConfigKeys):
    # AWS
    CONN_ID = "conn_id"
    REGION_NAME = "region_name"

    # Batch
    JOB_QUEUE = "job_queue"
    JOB_DEFINITION = "job_definition"
    WORKER_JOB_NAME_PREFIX = "worker_job_name_prefix"
    SUBMIT_JOB_KWARGS = "submit_job_kwargs"

    # SQS
    TASK_QUEUE_URL = "task_queue_url"
    RESULT_QUEUE_URL = "result_queue_url"

    # Lifecycle
    WORKER_IDLE_TIMEOUT_SECONDS = "worker_idle_timeout_seconds"
    WORKER_MAX_TASKS = "worker_max_tasks"
    WORKER_VISIBILITY_TIMEOUT_SECONDS = "worker_visibility_timeout_seconds"

    # Scaling
    MIN_WORKERS = "min_workers"
    MAX_WORKERS = "max_workers"
    TASKS_PER_WORKER = "tasks_per_worker"

    # Health
    CHECK_HEALTH_ON_STARTUP = "check_health_on_startup"
    WORKER_HEALTH_CHECK_INTERVAL_SECONDS = "worker_health_check_interval_seconds"
    MAX_WORKER_START_ATTEMPTS = "max_worker_start_attempts"

    # Shared State
    SHARED_STATE_INIT_MODULE = "shared_state_init_module"
    SHARED_STATE_INIT_FUNCTION = "shared_state_init_function"
```

---

### 2. SQS Message Schemas

**Task Queue Message** (Executor → Workers):

```json
{
    "message_id": "uuid-v4",
    "task_key": {
        "dag_id": "example_dag",
        "task_id": "process_data",
        "run_id": "scheduled__2024-01-01T00:00:00+00:00",
        "try_number": 1,
        "map_index": -1
    },
    "workload": {
        "type": "ExecuteTask",
        "ti": { ... },
        "dag_rel_path": "dags/example_dag.py",
        "bundle_info": { ... },
        "token": "jwt-token",
        "log_path": "dag_id=example_dag/run_id=.../task_id=process_data/..."
    },
    "executor_config": {},
    "enqueued_at": "2024-01-01T12:00:00Z"
}
```

**Result Queue Message** (Workers → Executor):

```json
{
    "message_id": "uuid-v4",
    "task_key": {
        "dag_id": "example_dag",
        "task_id": "process_data",
        "run_id": "scheduled__2024-01-01T00:00:00+00:00",
        "try_number": 1,
        "map_index": -1
    },
    "state": "SUCCESS",  // or "FAILED"
    "info": {
        "worker_id": "worker-abc123",
        "batch_job_id": "batch-job-xyz",
        "execution_time_seconds": 45.2,
        "error_message": null  // Populated on failure
    },
    "completed_at": "2024-01-01T12:00:45Z"
}
```

**Pydantic Models**:

```python
# providers/amazon/src/airflow/providers/amazon/aws/executors/batch/worker_pool_schemas.py

from __future__ import annotations
from datetime import datetime
from typing import Any, Literal
from pydantic import BaseModel
from airflow.models.taskinstancekey import TaskInstanceKey
from airflow.executors.workloads import ExecuteTask


class TaskQueueMessage(BaseModel):
    """Message sent to SQS task queue for workers to process."""
    message_id: str
    task_key: TaskInstanceKey
    workload: ExecuteTask
    executor_config: dict[str, Any]
    enqueued_at: datetime


class TaskResultInfo(BaseModel):
    """Metadata about task execution result."""
    worker_id: str
    batch_job_id: str
    execution_time_seconds: float
    error_message: str | None = None


class TaskResultMessage(BaseModel):
    """Message sent to SQS result queue by workers."""
    message_id: str
    task_key: TaskInstanceKey
    state: Literal["SUCCESS", "FAILED"]
    info: TaskResultInfo
    completed_at: datetime
```

---

### 3. Executor Class Design

**File**: `providers/amazon/src/airflow/providers/amazon/aws/executors/batch/batch_worker_pool_executor.py`

```python
class AwsBatchWorkerPoolExecutor(BaseExecutor):
    """
    AWS Batch Executor with on-demand, long-running worker pools.

    Key differences from AwsBatchExecutor:
    - Workers are long-running Batch jobs that process multiple tasks
    - Tasks distributed via SQS queue (not one Batch job per task)
    - Workers fork subprocesses for task isolation
    - Workers self-terminate after idle timeout
    - Executor scales workers based on parallelism setting
    """

    # Class attributes
    is_local: bool = False
    is_production: bool = True
    supports_ad_hoc_ti_run: bool = False

    def __init__(self, parallelism: int = PARALLELISM):
        super().__init__(parallelism=parallelism)

        # Load configuration
        self._load_config()

        # AWS clients (lazy initialization)
        self._batch_client = None
        self._sqs_client = None

        # Worker tracking
        self.active_workers: WorkerCollection = WorkerCollection()
        self.pending_worker_starts: deque[WorkerStartRequest] = deque()

        # Task tracking
        self.tasks_in_queue: dict[TaskInstanceKey, TaskQueueMessage] = {}

        # Health state
        self.IS_HEALTHY = False
        self.last_health_check = None
        self.last_worker_scale_check = None

    #
    # Lifecycle Methods
    #

    def start(self):
        """Initialize executor, validate connections, optionally start min_workers."""
        if self.check_health_on_startup:
            self.check_health()

        # Start minimum workers if configured
        if self.min_workers > 0:
            for i in range(self.min_workers):
                self._request_worker_start(worker_id=f"init-{i}")

    def end(self, heartbeat_interval=10):
        """Wait for all tasks to complete, then terminate workers."""
        self.log.info("Ending worker pool executor, waiting for tasks...")

        while self.tasks_in_queue or self.running:
            self.sync()
            if not self.tasks_in_queue and not self.running:
                break
            time.sleep(heartbeat_interval)

        # Terminate all workers
        self._terminate_all_workers(reason="Executor shutdown")

    def terminate(self):
        """Force terminate all workers immediately."""
        self.log.info("Terminating worker pool executor")
        self._terminate_all_workers(reason="Executor SIGTERM")

    #
    # Task Queueing
    #

    def queue_workload(self, workload: workloads.All, session: Session | None) -> None:
        """Queue a workload for execution."""
        if not isinstance(workload, workloads.ExecuteTask):
            raise RuntimeError(f"Cannot handle workload type {type(workload)}")

        ti = workload.ti
        self.queued_tasks[ti.key] = workload

    def _process_workloads(self, workloads_list: Sequence[workloads.All]) -> None:
        """Send tasks to SQS queue for workers to pick up."""
        for w in workloads_list:
            if not isinstance(w, workloads.ExecuteTask):
                raise RuntimeError(f"Cannot handle workload type {type(w)}")

            key = w.ti.key
            executor_config = w.ti.executor_config or {}

            # Create task message
            message = TaskQueueMessage(
                message_id=str(uuid.uuid4()),
                task_key=key,
                workload=w,
                executor_config=executor_config,
                enqueued_at=timezone.utcnow()
            )

            # Send to SQS
            self._send_task_to_queue(message)

            # Track locally
            self.tasks_in_queue[key] = message
            del self.queued_tasks[key]
            self.running.add(key)

            self.log.info("Enqueued task %s to worker pool", key)

    def _send_task_to_queue(self, message: TaskQueueMessage):
        """Send task message to SQS task queue."""
        self.sqs_client.send_message(
            QueueUrl=self.task_queue_url,
            MessageBody=message.model_dump_json(),
            MessageAttributes={
                'task_id': {
                    'StringValue': str(message.task_key),
                    'DataType': 'String'
                },
                'dag_id': {
                    'StringValue': message.task_key.dag_id,
                    'DataType': 'String'
                }
            }
        )

    #
    # Sync Loop
    #

    def sync(self):
        """
        Periodically called by scheduler heartbeat.
        - Check worker health
        - Scale workers up/down
        - Poll result queue for completed tasks
        - Submit pending worker starts
        """
        if not self.IS_HEALTHY:
            self._attempt_reconnect()
            if not self.IS_HEALTHY:
                return

        try:
            # 1. Poll result queue for completed tasks
            self._poll_result_queue()

            # 2. Check worker health
            self._check_worker_health()

            # 3. Scale workers based on queue depth and parallelism
            self._scale_workers()

            # 4. Submit pending worker start requests
            self._submit_pending_worker_starts()

        except (ClientError, NoCredentialsError) as e:
            self._handle_aws_error(e)
        except Exception:
            self.log.exception("Failed to sync worker pool executor")

    def _poll_result_queue(self):
        """Poll SQS result queue for completed task results."""
        while True:
            response = self.sqs_client.receive_message(
                QueueUrl=self.result_queue_url,
                MaxNumberOfMessages=10,  # Batch for efficiency
                WaitTimeSeconds=0,       # Non-blocking
                MessageAttributeNames=['All']
            )

            messages = response.get('Messages', [])
            if not messages:
                break

            for msg in messages:
                try:
                    result = TaskResultMessage.model_validate_json(msg['Body'])
                    self._handle_task_result(result)

                    # Delete processed message
                    self.sqs_client.delete_message(
                        QueueUrl=self.result_queue_url,
                        ReceiptHandle=msg['ReceiptHandle']
                    )
                except Exception:
                    self.log.exception("Failed to process result message: %s", msg)

    def _handle_task_result(self, result: TaskResultMessage):
        """Handle a task completion result from a worker."""
        key = result.task_key

        # Remove from tracking
        self.tasks_in_queue.pop(key, None)

        # Update executor state
        if result.state == "SUCCESS":
            self.success(key, info=result.info.model_dump())
        else:
            self.fail(key, info=result.info.model_dump())

        self.log.info(
            "Task %s completed with state %s (worker: %s, time: %.2fs)",
            key, result.state, result.info.worker_id, result.info.execution_time_seconds
        )

    #
    # Worker Scaling
    #

    def _scale_workers(self):
        """Scale workers based on queue depth and parallelism."""
        # Calculate desired workers
        tasks_pending = self._get_queue_depth()
        current_workers = len(self.active_workers)

        # Respect parallelism limit
        max_allowed = self.max_workers if self.max_workers > 0 else self.parallelism

        # Calculate tasks per worker capacity
        tasks_per_worker = self.tasks_per_worker
        desired_workers = min(
            max_allowed,
            max(self.min_workers, (tasks_pending + tasks_per_worker - 1) // tasks_per_worker)
        )

        # Scale up if needed
        if desired_workers > current_workers:
            workers_to_add = desired_workers - current_workers
            self.log.info(
                "Scaling up: adding %d workers (pending tasks: %d, current workers: %d)",
                workers_to_add, tasks_pending, current_workers
            )
            for i in range(workers_to_add):
                self._request_worker_start(worker_id=f"scale-{timezone.utcnow().timestamp()}-{i}")

        # Note: Scale down happens automatically via worker idle timeout

    def _get_queue_depth(self) -> int:
        """Get approximate number of messages in task queue."""
        response = self.sqs_client.get_queue_attributes(
            QueueUrl=self.task_queue_url,
            AttributeNames=['ApproximateNumberOfMessages', 'ApproximateNumberOfMessagesNotVisible']
        )
        attrs = response.get('Attributes', {})
        visible = int(attrs.get('ApproximateNumberOfMessages', 0))
        in_flight = int(attrs.get('ApproximateNumberOfMessagesNotVisible', 0))
        return visible + in_flight

    def _request_worker_start(self, worker_id: str):
        """Queue a worker start request."""
        self.pending_worker_starts.append(
            WorkerStartRequest(
                worker_id=worker_id,
                attempt_number=1,
                next_attempt_time=timezone.utcnow()
            )
        )

    def _submit_pending_worker_starts(self):
        """Submit pending worker start requests to AWS Batch."""
        for _ in range(len(self.pending_worker_starts)):
            request = self.pending_worker_starts.popleft()

            # Check if we've hit the max workers limit
            max_allowed = self.max_workers if self.max_workers > 0 else self.parallelism
            if len(self.active_workers) >= max_allowed:
                self.log.debug("Max workers reached, skipping worker start")
                continue

            # Check retry timing
            if timezone.utcnow() < request.next_attempt_time:
                self.pending_worker_starts.append(request)
                continue

            try:
                job_id = self._start_worker(request.worker_id)
                self.active_workers.add_worker(
                    job_id=job_id,
                    worker_id=request.worker_id,
                    started_at=timezone.utcnow()
                )
                self.log.info("Started worker %s (Batch job: %s)", request.worker_id, job_id)

            except ClientError as e:
                if request.attempt_number >= self.max_worker_start_attempts:
                    self.log.error(
                        "Failed to start worker %s after %d attempts: %s",
                        request.worker_id, request.attempt_number, e
                    )
                else:
                    request.attempt_number += 1
                    request.next_attempt_time = timezone.utcnow() + calculate_next_attempt_delay(
                        request.attempt_number
                    )
                    self.pending_worker_starts.append(request)

    def _start_worker(self, worker_id: str) -> str:
        """Submit a worker job to AWS Batch. Returns job ID."""
        job_name = f"{self.worker_job_name_prefix}-{worker_id}"

        submit_kwargs = self._build_worker_submit_kwargs(worker_id)

        response = self.batch_client.submit_job(
            jobName=job_name,
            jobQueue=self.job_queue,
            jobDefinition=self.job_definition,
            **submit_kwargs
        )

        return response['jobId']

    def _build_worker_submit_kwargs(self, worker_id: str) -> dict:
        """Build kwargs for Batch submit_job API."""
        base_kwargs = deepcopy(self.submit_job_kwargs)

        if 'containerOverrides' not in base_kwargs:
            base_kwargs['containerOverrides'] = {}

        # Set worker command
        base_kwargs['containerOverrides']['command'] = [
            'python', '-m',
            'airflow.providers.amazon.aws.executors.batch.worker_pool_worker',
            '--worker-id', worker_id,
            '--task-queue-url', self.task_queue_url,
            '--result-queue-url', self.result_queue_url,
            '--idle-timeout', str(self.worker_idle_timeout_seconds),
            '--visibility-timeout', str(self.worker_visibility_timeout_seconds),
        ]

        # Add shared state init if configured
        if self.shared_state_init_module:
            base_kwargs['containerOverrides']['command'].extend([
                '--init-module', self.shared_state_init_module,
                '--init-function', self.shared_state_init_function or 'initialize',
            ])

        # Set environment variables
        if 'environment' not in base_kwargs['containerOverrides']:
            base_kwargs['containerOverrides']['environment'] = []

        base_kwargs['containerOverrides']['environment'].extend([
            {'name': 'AIRFLOW_WORKER_POOL_MODE', 'value': 'true'},
            {'name': 'AIRFLOW_WORKER_ID', 'value': worker_id},
        ])

        return base_kwargs

    #
    # Worker Health Monitoring
    #

    def _check_worker_health(self):
        """Check health of active workers via AWS Batch describe_jobs."""
        if not self.active_workers:
            return

        # Rate limit health checks
        now = timezone.utcnow()
        if (self.last_worker_scale_check and
            (now - self.last_worker_scale_check).total_seconds() < self.worker_health_check_interval_seconds):
            return
        self.last_worker_scale_check = now

        job_ids = self.active_workers.get_all_job_ids()

        for i in range(0, len(job_ids), 99):
            batch = job_ids[i:i+99]
            response = self.batch_client.describe_jobs(jobs=batch)

            for job in response['jobs']:
                job_id = job['jobId']
                status = job['status']

                if status in ['FAILED', 'SUCCEEDED']:
                    # Worker terminated
                    worker_info = self.active_workers.pop_by_job_id(job_id)
                    self.log.warning(
                        "Worker %s (job %s) terminated with status %s",
                        worker_info.worker_id, job_id, status
                    )

                    # Note: We don't automatically restart - scaling logic will
                    # spawn new workers if there are pending tasks

    def _terminate_all_workers(self, reason: str):
        """Terminate all active workers."""
        for job_id in self.active_workers.get_all_job_ids():
            try:
                self.batch_client.terminate_job(jobId=job_id, reason=reason)
            except Exception:
                self.log.exception("Failed to terminate worker job %s", job_id)

        self.active_workers.clear()

    #
    # Task Adoption
    #

    def try_adopt_task_instances(self, tis: Sequence[TaskInstance]) -> Sequence[TaskInstance]:
        """
        Attempt to adopt task instances from a dead executor.

        For worker pool executor, we check if tasks are still in the SQS queue
        or being processed by workers.
        """
        adopted = []
        not_adopted = []

        for ti in tis:
            # Check if task is in our local tracking
            if ti.key in self.tasks_in_queue:
                adopted.append(ti)
                continue

            # Check if task is in SQS queue (approximate)
            # This is tricky with SQS - we can't easily check for specific messages
            # For now, we'll consider tasks with recent queue time as potentially in-flight
            # and let them time out naturally

            # If task has an external_executor_id that looks like a worker job ID,
            # check if that worker is still running
            if ti.external_executor_id:
                if self.active_workers.has_job_id(ti.external_executor_id):
                    adopted.append(ti)
                    self.tasks_in_queue[ti.key] = None  # Mark as in-flight
                    self.running.add(ti.key)
                    continue

            not_adopted.append(ti)

        if adopted:
            self.log.info("Adopted %d tasks from dead executor", len(adopted))

        return not_adopted

    #
    # Properties (Lazy AWS Client Initialization)
    #

    @property
    def batch_client(self):
        if self._batch_client is None:
            self._batch_client = BatchClientHook(
                aws_conn_id=self.conn_id,
                region_name=self.region_name
            ).conn
        return self._batch_client

    @property
    def sqs_client(self):
        if self._sqs_client is None:
            from airflow.providers.amazon.aws.hooks.sqs import SqsHook
            self._sqs_client = SqsHook(
                aws_conn_id=self.conn_id,
                region_name=self.region_name
            ).conn
        return self._sqs_client
```

---

### 4. Worker Process Design

**File**: `providers/amazon/src/airflow/providers/amazon/aws/executors/batch/worker_pool_worker.py`

```python
"""
Long-running worker process for AWS Batch Worker Pool Executor.

This process runs inside AWS Batch containers and:
1. Initializes shared state (ML models, DB pools, etc.)
2. Polls SQS task queue for work
3. Forks a subprocess for each task (isolation)
4. Reports results to SQS result queue
5. Self-terminates after idle timeout
"""

from __future__ import annotations

import argparse
import importlib
import logging
import os
import signal
import sys
import time
import uuid
from datetime import datetime, timedelta
from multiprocessing import Process, Queue
from typing import Any

import boto3

from airflow.providers.amazon.aws.executors.batch.worker_pool_schemas import (
    TaskQueueMessage,
    TaskResultMessage,
    TaskResultInfo,
)

try:
    from airflow.sdk import timezone
except ImportError:
    from airflow.utils import timezone

log = logging.getLogger(__name__)

# Global shared state (initialized once, available to forked tasks)
SHARED_STATE: dict[str, Any] = {}


class WorkerProcess:
    """Long-running worker that polls SQS and executes tasks in subprocesses."""

    def __init__(
        self,
        worker_id: str,
        task_queue_url: str,
        result_queue_url: str,
        idle_timeout_seconds: int = 300,
        visibility_timeout_seconds: int = 3600,
        max_tasks: int = 0,
        init_module: str | None = None,
        init_function: str = 'initialize',
    ):
        self.worker_id = worker_id
        self.task_queue_url = task_queue_url
        self.result_queue_url = result_queue_url
        self.idle_timeout_seconds = idle_timeout_seconds
        self.visibility_timeout_seconds = visibility_timeout_seconds
        self.max_tasks = max_tasks  # 0 = unlimited
        self.init_module = init_module
        self.init_function = init_function

        # AWS clients
        self.sqs = boto3.client('sqs')

        # State
        self.running = True
        self.tasks_executed = 0
        self.last_task_time = timezone.utcnow()
        self.current_receipt_handle = None

        # Get Batch job ID from environment
        self.batch_job_id = os.environ.get('AWS_BATCH_JOB_ID', 'unknown')

        # Signal handlers
        signal.signal(signal.SIGTERM, self._handle_sigterm)
        signal.signal(signal.SIGINT, self._handle_sigterm)

    def _handle_sigterm(self, signum, frame):
        """Handle termination signal - finish current task, then exit."""
        log.info("Received signal %d, will exit after current task", signum)
        self.running = False

        # If we have a message in-flight, return it to queue
        if self.current_receipt_handle:
            try:
                self.sqs.change_message_visibility(
                    QueueUrl=self.task_queue_url,
                    ReceiptHandle=self.current_receipt_handle,
                    VisibilityTimeout=0  # Immediate return to queue
                )
            except Exception:
                log.exception("Failed to return message to queue")

    def run(self):
        """Main worker loop."""
        log.info("Worker %s starting (Batch job: %s)", self.worker_id, self.batch_job_id)

        # Initialize shared state
        self._initialize_shared_state()

        log.info("Worker %s entering main loop", self.worker_id)

        while self.running:
            # Check idle timeout
            if self._should_terminate_on_idle():
                log.info(
                    "Worker %s idle for %d seconds, terminating",
                    self.worker_id, self.idle_timeout_seconds
                )
                break

            # Check task limit
            if self.max_tasks > 0 and self.tasks_executed >= self.max_tasks:
                log.info(
                    "Worker %s reached max tasks (%d), terminating",
                    self.worker_id, self.max_tasks
                )
                break

            # Poll for task
            task_message = self._poll_for_task()
            if task_message is None:
                continue

            # Execute task in subprocess
            self._execute_task(task_message)

        log.info(
            "Worker %s shutting down (executed %d tasks)",
            self.worker_id, self.tasks_executed
        )

    def _initialize_shared_state(self):
        """Initialize expensive shared resources."""
        global SHARED_STATE

        if not self.init_module:
            log.info("No shared state initialization configured")
            return

        log.info("Initializing shared state from %s.%s", self.init_module, self.init_function)

        try:
            module = importlib.import_module(self.init_module)
            init_func = getattr(module, self.init_function)
            SHARED_STATE = init_func() or {}
            log.info("Shared state initialized: %s", list(SHARED_STATE.keys()))
        except Exception:
            log.exception("Failed to initialize shared state")
            # Continue anyway - tasks can still run without shared state

    def _should_terminate_on_idle(self) -> bool:
        """Check if we should terminate due to idle timeout."""
        idle_duration = (timezone.utcnow() - self.last_task_time).total_seconds()
        return idle_duration >= self.idle_timeout_seconds

    def _poll_for_task(self) -> TaskQueueMessage | None:
        """Poll SQS for next task. Returns None if no task available."""
        try:
            response = self.sqs.receive_message(
                QueueUrl=self.task_queue_url,
                MaxNumberOfMessages=1,
                WaitTimeSeconds=20,  # Long-polling
                VisibilityTimeout=self.visibility_timeout_seconds,
                MessageAttributeNames=['All']
            )

            messages = response.get('Messages', [])
            if not messages:
                return None

            msg = messages[0]
            self.current_receipt_handle = msg['ReceiptHandle']

            # Parse message
            task_message = TaskQueueMessage.model_validate_json(msg['Body'])
            return task_message

        except Exception:
            log.exception("Failed to poll task queue")
            return None

    def _execute_task(self, task_message: TaskQueueMessage):
        """Execute a task in a subprocess for isolation."""
        start_time = time.time()
        task_key = task_message.task_key

        log.info("Executing task %s", task_key)

        # Create result queue for subprocess communication
        result_queue: Queue = Queue()

        # Fork subprocess
        process = Process(
            target=self._run_task_subprocess,
            args=(task_message, result_queue)
        )
        process.start()

        # Wait for completion with timeout
        process.join(timeout=self.visibility_timeout_seconds - 60)  # Leave margin

        execution_time = time.time() - start_time

        # Check result
        if process.is_alive():
            # Task timed out
            log.error("Task %s timed out after %.2f seconds", task_key, execution_time)
            process.terminate()
            process.join(timeout=30)
            if process.is_alive():
                process.kill()

            state = "FAILED"
            error_message = f"Task timed out after {execution_time:.2f} seconds"

        elif not result_queue.empty():
            # Get result from subprocess
            result = result_queue.get_nowait()
            state = result['state']
            error_message = result.get('error_message')

        else:
            # Subprocess crashed without reporting
            state = "FAILED"
            error_message = f"Subprocess exited with code {process.exitcode}"

        # Report result to SQS
        self._report_result(task_key, state, execution_time, error_message)

        # Delete message from queue (task processed)
        try:
            self.sqs.delete_message(
                QueueUrl=self.task_queue_url,
                ReceiptHandle=self.current_receipt_handle
            )
        except Exception:
            log.exception("Failed to delete task message")

        self.current_receipt_handle = None
        self.tasks_executed += 1
        self.last_task_time = timezone.utcnow()

        log.info(
            "Task %s completed: %s (%.2fs)",
            task_key, state, execution_time
        )

    @staticmethod
    def _run_task_subprocess(task_message: TaskQueueMessage, result_queue: Queue):
        """
        Run in forked subprocess. Executes the actual Airflow task.

        This subprocess has access to SHARED_STATE via copy-on-write from parent.
        """
        import builtins

        try:
            # Make shared state available
            builtins.AIRFLOW_WORKER_SHARED_STATE = SHARED_STATE

            # Import supervisor
            from airflow.sdk.execution_time.supervisor import supervise

            workload = task_message.workload

            # Execute task via supervisor
            supervise(
                ti=workload.ti,
                dag_rel_path=workload.dag_rel_path,
                bundle_info=workload.bundle_info,
                token=workload.token,
                server=None,  # Will use default from config
                log_path=workload.log_path,
            )

            result_queue.put({'state': 'SUCCESS', 'error_message': None})

        except Exception as e:
            log.exception("Task execution failed")
            result_queue.put({
                'state': 'FAILED',
                'error_message': str(e)
            })

    def _report_result(
        self,
        task_key,
        state: str,
        execution_time: float,
        error_message: str | None
    ):
        """Send task result to SQS result queue."""
        result = TaskResultMessage(
            message_id=str(uuid.uuid4()),
            task_key=task_key,
            state=state,
            info=TaskResultInfo(
                worker_id=self.worker_id,
                batch_job_id=self.batch_job_id,
                execution_time_seconds=execution_time,
                error_message=error_message
            ),
            completed_at=timezone.utcnow()
        )

        try:
            self.sqs.send_message(
                QueueUrl=self.result_queue_url,
                MessageBody=result.model_dump_json(),
                MessageAttributes={
                    'task_id': {
                        'StringValue': str(task_key),
                        'DataType': 'String'
                    },
                    'state': {
                        'StringValue': state,
                        'DataType': 'String'
                    }
                }
            )
        except Exception:
            log.exception("Failed to send result to queue")


def main():
    """Entry point for worker process."""
    parser = argparse.ArgumentParser(description='Airflow Batch Worker Pool Worker')
    parser.add_argument('--worker-id', required=True, help='Unique worker identifier')
    parser.add_argument('--task-queue-url', required=True, help='SQS task queue URL')
    parser.add_argument('--result-queue-url', required=True, help='SQS result queue URL')
    parser.add_argument('--idle-timeout', type=int, default=300, help='Idle timeout in seconds')
    parser.add_argument('--visibility-timeout', type=int, default=3600, help='SQS visibility timeout')
    parser.add_argument('--max-tasks', type=int, default=0, help='Max tasks before terminating (0=unlimited)')
    parser.add_argument('--init-module', help='Module containing shared state init function')
    parser.add_argument('--init-function', default='initialize', help='Shared state init function name')

    args = parser.parse_args()

    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s [%(levelname)s] %(name)s: %(message)s'
    )

    worker = WorkerProcess(
        worker_id=args.worker_id,
        task_queue_url=args.task_queue_url,
        result_queue_url=args.result_queue_url,
        idle_timeout_seconds=args.idle_timeout,
        visibility_timeout_seconds=args.visibility_timeout,
        max_tasks=args.max_tasks,
        init_module=args.init_module,
        init_function=args.init_function,
    )

    try:
        worker.run()
        sys.exit(0)
    except Exception:
        log.exception("Worker crashed")
        sys.exit(1)


if __name__ == '__main__':
    main()
```

---

### 4b. Multi-Language Worker Support

The Python worker above uses `fork()` for task isolation, which works well for Python but is **unsafe for other runtimes**:

| Runtime | Fork Safety | Issue |
|---------|-------------|-------|
| **Python** | ✅ Safe | GIL prevents threading issues during fork |
| **.NET CLR** | ❌ Unsafe | CLR threads, GC, and finalizers don't survive fork |
| **JVM** | ❌ Unsafe | JVM threads and GC don't survive fork |
| **Go** | ❌ Unsafe | Goroutines and runtime don't survive fork |
| **Node.js** | ⚠️ Partial | Event loop survives but worker threads don't |

For non-Python runtimes, we provide an **in-process execution model** where tasks run directly in the worker process with shared state.

#### Task Isolation Strategies

```
┌─────────────────────────────────────────────────────────────────────────┐
│                        PYTHON WORKER                                     │
│  (Fork-based isolation - subprocess per task)                           │
│                                                                          │
│  ┌──────────────────────────────────────────────────────────────────┐   │
│  │ Parent Process (long-running)                                     │   │
│  │ ┌─────────────────────┐                                          │   │
│  │ │ Shared State        │  ← Initialized once                      │   │
│  │ │ - ML Models         │                                          │   │
│  │ │ - DB Pools          │                                          │   │
│  │ └─────────────────────┘                                          │   │
│  │          │                                                        │   │
│  │          │ fork() ──────────────────┐                            │   │
│  │          │                          │                            │   │
│  │          v                          v                            │   │
│  │  ┌─────────────────┐       ┌─────────────────┐                  │   │
│  │  │ Task 1 Process  │       │ Task 2 Process  │   (isolated)     │   │
│  │  │ (copy-on-write) │       │ (copy-on-write) │                  │   │
│  │  └─────────────────┘       └─────────────────┘                  │   │
│  └──────────────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────────────┐
│                     .NET / JVM / GO WORKER                               │
│  (In-process execution - shared memory, no fork)                        │
│                                                                          │
│  ┌──────────────────────────────────────────────────────────────────┐   │
│  │ Single Process (long-running)                                     │   │
│  │ ┌─────────────────────┐                                          │   │
│  │ │ Shared State        │  ← Initialized once, directly accessible │   │
│  │ │ - ML Models         │                                          │   │
│  │ │ - DB Pools          │                                          │   │
│  │ │ - Cached Data       │                                          │   │
│  │ └─────────────────────┘                                          │   │
│  │          │                                                        │   │
│  │          │ direct call                                           │   │
│  │          v                                                        │   │
│  │  ┌─────────────────────────────────────────────────────────┐     │   │
│  │  │ Task Execution (in-process)                              │     │   │
│  │  │ - Runs in same AppDomain/JVM/Process                     │     │   │
│  │  │ - Direct access to shared state                          │     │   │
│  │  │ - Must handle exceptions to prevent worker crash         │     │   │
│  │  └─────────────────────────────────────────────────────────┘     │   │
│  └──────────────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────────────┘
```

#### .NET 8 Worker Implementation

**File**: `workers/dotnet/AirflowWorker/Worker.cs`

This is a standalone .NET 8 application that runs as the container entrypoint.

```csharp
// AirflowWorker/Program.cs
using System.CommandLine;
using AirflowWorker;

var rootCommand = new RootCommand("Airflow Batch Worker Pool - .NET Worker");

var workerIdOption = new Option<string>("--worker-id", "Unique worker identifier") { IsRequired = true };
var taskQueueOption = new Option<string>("--task-queue-url", "SQS task queue URL") { IsRequired = true };
var resultQueueOption = new Option<string>("--result-queue-url", "SQS result queue URL") { IsRequired = true };
var idleTimeoutOption = new Option<int>("--idle-timeout", () => 300, "Idle timeout in seconds");
var visibilityTimeoutOption = new Option<int>("--visibility-timeout", () => 3600, "SQS visibility timeout");
var initAssemblyOption = new Option<string?>("--init-assembly", "Assembly containing shared state initializer");
var initTypeOption = new Option<string>("--init-type", () => "SharedStateInitializer", "Type name for initializer");

rootCommand.AddOption(workerIdOption);
rootCommand.AddOption(taskQueueOption);
rootCommand.AddOption(resultQueueOption);
rootCommand.AddOption(idleTimeoutOption);
rootCommand.AddOption(visibilityTimeoutOption);
rootCommand.AddOption(initAssemblyOption);
rootCommand.AddOption(initTypeOption);

rootCommand.SetHandler(async (context) =>
{
    var workerId = context.ParseResult.GetValueForOption(workerIdOption)!;
    var taskQueueUrl = context.ParseResult.GetValueForOption(taskQueueOption)!;
    var resultQueueUrl = context.ParseResult.GetValueForOption(resultQueueOption)!;
    var idleTimeout = context.ParseResult.GetValueForOption(idleTimeoutOption);
    var visibilityTimeout = context.ParseResult.GetValueForOption(visibilityTimeoutOption);
    var initAssembly = context.ParseResult.GetValueForOption(initAssemblyOption);
    var initType = context.ParseResult.GetValueForOption(initTypeOption)!;

    var worker = new WorkerProcess(
        workerId,
        taskQueueUrl,
        resultQueueUrl,
        TimeSpan.FromSeconds(idleTimeout),
        TimeSpan.FromSeconds(visibilityTimeout),
        initAssembly,
        initType
    );

    await worker.RunAsync(context.GetCancellationToken());
});

return await rootCommand.InvokeAsync(args);
```

```csharp
// AirflowWorker/WorkerProcess.cs
using System.Diagnostics;
using System.Reflection;
using System.Text.Json;
using Amazon.SQS;
using Amazon.SQS.Model;
using Microsoft.Extensions.Logging;

namespace AirflowWorker;

/// <summary>
/// Long-running worker process that polls SQS and executes tasks in-process.
/// Shared state is initialized once and reused across all tasks.
/// </summary>
public class WorkerProcess
{
    private readonly string _workerId;
    private readonly string _taskQueueUrl;
    private readonly string _resultQueueUrl;
    private readonly TimeSpan _idleTimeout;
    private readonly TimeSpan _visibilityTimeout;
    private readonly string? _initAssembly;
    private readonly string _initType;

    private readonly IAmazonSQS _sqsClient;
    private readonly ILogger<WorkerProcess> _logger;
    private readonly string _batchJobId;

    private bool _running = true;
    private int _tasksExecuted = 0;
    private DateTime _lastTaskTime;
    private string? _currentReceiptHandle;

    // Shared state - initialized once, used by all tasks
    private ISharedState? _sharedState;

    public WorkerProcess(
        string workerId,
        string taskQueueUrl,
        string resultQueueUrl,
        TimeSpan idleTimeout,
        TimeSpan visibilityTimeout,
        string? initAssembly,
        string initType)
    {
        _workerId = workerId;
        _taskQueueUrl = taskQueueUrl;
        _resultQueueUrl = resultQueueUrl;
        _idleTimeout = idleTimeout;
        _visibilityTimeout = visibilityTimeout;
        _initAssembly = initAssembly;
        _initType = initType;

        _sqsClient = new AmazonSQSClient();
        _logger = LoggerFactory.Create(b => b.AddConsole()).CreateLogger<WorkerProcess>();
        _batchJobId = Environment.GetEnvironmentVariable("AWS_BATCH_JOB_ID") ?? "unknown";
        _lastTaskTime = DateTime.UtcNow;

        // Register for graceful shutdown
        Console.CancelKeyPress += (_, e) =>
        {
            e.Cancel = true;
            HandleShutdown();
        };
        AppDomain.CurrentDomain.ProcessExit += (_, _) => HandleShutdown();
    }

    private void HandleShutdown()
    {
        _logger.LogInformation("Received shutdown signal, finishing current task...");
        _running = false;

        // Return current message to queue if processing
        if (_currentReceiptHandle != null)
        {
            try
            {
                _sqsClient.ChangeMessageVisibilityAsync(new ChangeMessageVisibilityRequest
                {
                    QueueUrl = _taskQueueUrl,
                    ReceiptHandle = _currentReceiptHandle,
                    VisibilityTimeout = 0 // Immediate return to queue
                }).GetAwaiter().GetResult();
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Failed to return message to queue");
            }
        }
    }

    public async Task RunAsync(CancellationToken cancellationToken)
    {
        _logger.LogInformation("Worker {WorkerId} starting (Batch job: {JobId})", _workerId, _batchJobId);

        // Initialize shared state
        await InitializeSharedStateAsync();

        _logger.LogInformation("Worker {WorkerId} entering main loop", _workerId);

        while (_running && !cancellationToken.IsCancellationRequested)
        {
            // Check idle timeout
            if (DateTime.UtcNow - _lastTaskTime > _idleTimeout)
            {
                _logger.LogInformation(
                    "Worker {WorkerId} idle for {Timeout}, terminating",
                    _workerId, _idleTimeout);
                break;
            }

            // Poll for task
            var taskMessage = await PollForTaskAsync(cancellationToken);
            if (taskMessage == null)
                continue;

            // Execute task IN-PROCESS (no fork)
            await ExecuteTaskAsync(taskMessage);
        }

        _logger.LogInformation(
            "Worker {WorkerId} shutting down (executed {Count} tasks)",
            _workerId, _tasksExecuted);
    }

    private async Task InitializeSharedStateAsync()
    {
        if (string.IsNullOrEmpty(_initAssembly))
        {
            _logger.LogInformation("No shared state initialization configured");
            return;
        }

        _logger.LogInformation(
            "Initializing shared state from {Assembly}.{Type}",
            _initAssembly, _initType);

        try
        {
            var assembly = Assembly.LoadFrom(_initAssembly);
            var type = assembly.GetType(_initType)
                ?? throw new InvalidOperationException($"Type {_initType} not found");

            if (!typeof(ISharedState).IsAssignableFrom(type))
                throw new InvalidOperationException($"Type {_initType} must implement ISharedState");

            _sharedState = (ISharedState)Activator.CreateInstance(type)!;
            await _sharedState.InitializeAsync();

            _logger.LogInformation("Shared state initialized successfully");
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to initialize shared state");
            // Continue anyway - tasks can still run without shared state
        }
    }

    private async Task<TaskQueueMessage?> PollForTaskAsync(CancellationToken cancellationToken)
    {
        try
        {
            var response = await _sqsClient.ReceiveMessageAsync(new ReceiveMessageRequest
            {
                QueueUrl = _taskQueueUrl,
                MaxNumberOfMessages = 1,
                WaitTimeSeconds = 20, // Long-polling
                VisibilityTimeout = (int)_visibilityTimeout.TotalSeconds,
                MessageAttributeNames = ["All"]
            }, cancellationToken);

            if (response.Messages.Count == 0)
                return null;

            var msg = response.Messages[0];
            _currentReceiptHandle = msg.ReceiptHandle;

            return JsonSerializer.Deserialize<TaskQueueMessage>(msg.Body);
        }
        catch (OperationCanceledException)
        {
            return null;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to poll task queue");
            return null;
        }
    }

    private async Task ExecuteTaskAsync(TaskQueueMessage taskMessage)
    {
        var stopwatch = Stopwatch.StartNew();
        var taskKey = taskMessage.TaskKey;

        _logger.LogInformation("Executing task {TaskKey}", taskKey);

        string state;
        string? errorMessage = null;

        try
        {
            // Create task context with shared state
            var context = new TaskExecutionContext
            {
                TaskKey = taskKey,
                Workload = taskMessage.Workload,
                ExecutorConfig = taskMessage.ExecutorConfig,
                SharedState = _sharedState
            };

            // Execute task IN-PROCESS
            // The task implementation accesses shared state via context
            await ExecuteTaskInProcessAsync(context);

            state = "SUCCESS";
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Task {TaskKey} execution failed", taskKey);
            state = "FAILED";
            errorMessage = ex.ToString();
        }

        stopwatch.Stop();
        var executionTime = stopwatch.Elapsed.TotalSeconds;

        // Report result to SQS
        await ReportResultAsync(taskKey, state, executionTime, errorMessage);

        // Delete message from queue
        try
        {
            await _sqsClient.DeleteMessageAsync(_taskQueueUrl, _currentReceiptHandle);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to delete task message");
        }

        _currentReceiptHandle = null;
        _tasksExecuted++;
        _lastTaskTime = DateTime.UtcNow;

        _logger.LogInformation(
            "Task {TaskKey} completed: {State} ({Time:F2}s)",
            taskKey, state, executionTime);
    }

    private async Task ExecuteTaskInProcessAsync(TaskExecutionContext context)
    {
        // The workload contains the task definition as JSON
        // For .NET tasks, we deserialize and invoke the task handler

        var workload = context.Workload;

        // Option 1: Load task handler from assembly specified in executor_config
        if (context.ExecutorConfig.TryGetValue("dotnet_handler_assembly", out var assemblyPath) &&
            context.ExecutorConfig.TryGetValue("dotnet_handler_type", out var typeName))
        {
            var assembly = Assembly.LoadFrom(assemblyPath.ToString()!);
            var handlerType = assembly.GetType(typeName.ToString()!)
                ?? throw new InvalidOperationException($"Handler type {typeName} not found");

            if (!typeof(ITaskHandler).IsAssignableFrom(handlerType))
                throw new InvalidOperationException($"Type {typeName} must implement ITaskHandler");

            var handler = (ITaskHandler)Activator.CreateInstance(handlerType)!;
            await handler.ExecuteAsync(context);
        }
        // Option 2: Execute a command (for hybrid Python/.NET scenarios)
        else if (context.ExecutorConfig.TryGetValue("command", out var command))
        {
            var process = new Process
            {
                StartInfo = new ProcessStartInfo
                {
                    FileName = "/bin/bash",
                    Arguments = $"-c \"{command}\"",
                    RedirectStandardOutput = true,
                    RedirectStandardError = true,
                    UseShellExecute = false
                }
            };

            process.Start();
            await process.WaitForExitAsync();

            if (process.ExitCode != 0)
            {
                var stderr = await process.StandardError.ReadToEndAsync();
                throw new InvalidOperationException($"Command failed with exit code {process.ExitCode}: {stderr}");
            }
        }
        else
        {
            throw new InvalidOperationException(
                "No task handler configured. Set 'dotnet_handler_assembly' and 'dotnet_handler_type' in executor_config.");
        }
    }

    private async Task ReportResultAsync(
        TaskInstanceKey taskKey,
        string state,
        double executionTime,
        string? errorMessage)
    {
        var result = new TaskResultMessage
        {
            MessageId = Guid.NewGuid().ToString(),
            TaskKey = taskKey,
            State = state,
            Info = new TaskResultInfo
            {
                WorkerId = _workerId,
                BatchJobId = _batchJobId,
                ExecutionTimeSeconds = executionTime,
                ErrorMessage = errorMessage
            },
            CompletedAt = DateTime.UtcNow
        };

        try
        {
            await _sqsClient.SendMessageAsync(new SendMessageRequest
            {
                QueueUrl = _resultQueueUrl,
                MessageBody = JsonSerializer.Serialize(result),
                MessageAttributes = new Dictionary<string, MessageAttributeValue>
                {
                    ["task_id"] = new() { StringValue = taskKey.ToString(), DataType = "String" },
                    ["state"] = new() { StringValue = state, DataType = "String" }
                }
            });
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to send result to queue");
        }
    }
}
```

```csharp
// AirflowWorker/Interfaces.cs
namespace AirflowWorker;

/// <summary>
/// Interface for shared state that persists across task executions.
/// Implement this to initialize expensive resources once (ML models, DB pools, etc.)
/// </summary>
public interface ISharedState
{
    /// <summary>Called once when worker starts.</summary>
    Task InitializeAsync();

    /// <summary>Called when worker shuts down.</summary>
    Task DisposeAsync();
}

/// <summary>
/// Interface for task handlers. Implement this to define how tasks execute.
/// </summary>
public interface ITaskHandler
{
    /// <summary>Execute the task with access to shared state.</summary>
    Task ExecuteAsync(TaskExecutionContext context);
}

/// <summary>
/// Context passed to task handlers during execution.
/// </summary>
public record TaskExecutionContext
{
    public required TaskInstanceKey TaskKey { get; init; }
    public required JsonElement Workload { get; init; }
    public required Dictionary<string, object> ExecutorConfig { get; init; }
    public ISharedState? SharedState { get; init; }
}
```

```csharp
// AirflowWorker/Models.cs
using System.Text.Json.Serialization;

namespace AirflowWorker;

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

    public override string ToString() => $"{DagId}.{TaskId}[{RunId}]#{TryNumber}";
}

public record TaskQueueMessage
{
    [JsonPropertyName("message_id")]
    public required string MessageId { get; init; }

    [JsonPropertyName("task_key")]
    public required TaskInstanceKey TaskKey { get; init; }

    [JsonPropertyName("workload")]
    public required JsonElement Workload { get; init; }

    [JsonPropertyName("executor_config")]
    public Dictionary<string, object> ExecutorConfig { get; init; } = new();

    [JsonPropertyName("enqueued_at")]
    public DateTime EnqueuedAt { get; init; }
}

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

public record TaskResultMessage
{
    [JsonPropertyName("message_id")]
    public required string MessageId { get; init; }

    [JsonPropertyName("task_key")]
    public required TaskInstanceKey TaskKey { get; init; }

    [JsonPropertyName("state")]
    public required string State { get; init; }

    [JsonPropertyName("info")]
    public required TaskResultInfo Info { get; init; }

    [JsonPropertyName("completed_at")]
    public DateTime CompletedAt { get; init; }
}
```

#### .NET Worker Project Structure

```
workers/dotnet/
├── AirflowWorker/
│   ├── AirflowWorker.csproj
│   ├── Program.cs                 # Entry point with CLI parsing
│   ├── WorkerProcess.cs           # Main worker loop
│   ├── Interfaces.cs              # ISharedState, ITaskHandler
│   └── Models.cs                  # Message schemas (matching Python)
│
├── AirflowWorker.Example/
│   ├── AirflowWorker.Example.csproj
│   ├── MySharedState.cs           # Example shared state implementation
│   └── MyTaskHandler.cs           # Example task handler
│
├── Dockerfile                     # .NET 8 container image
└── Directory.Build.props          # Common build settings
```

#### .NET Worker Dockerfile

```dockerfile
# workers/dotnet/Dockerfile
FROM mcr.microsoft.com/dotnet/sdk:8.0 AS build
WORKDIR /src

# Copy project files
COPY AirflowWorker/*.csproj AirflowWorker/
COPY AirflowWorker.Example/*.csproj AirflowWorker.Example/

# Restore dependencies
RUN dotnet restore AirflowWorker/AirflowWorker.csproj
RUN dotnet restore AirflowWorker.Example/AirflowWorker.Example.csproj

# Copy source and build
COPY . .
RUN dotnet publish AirflowWorker/AirflowWorker.csproj -c Release -o /app
RUN dotnet publish AirflowWorker.Example/AirflowWorker.Example.csproj -c Release -o /app/handlers

# Runtime image
FROM mcr.microsoft.com/dotnet/aspnet:8.0
WORKDIR /app
COPY --from=build /app .

# Set entrypoint
ENTRYPOINT ["dotnet", "AirflowWorker.dll"]
```

#### Example .NET Shared State and Task Handler

```csharp
// AirflowWorker.Example/MySharedState.cs
using Microsoft.ML;
using AirflowWorker;

namespace AirflowWorker.Example;

/// <summary>
/// Example shared state that loads an ML model once and reuses it across tasks.
/// </summary>
public class MySharedState : ISharedState
{
    public MLContext MlContext { get; private set; } = null!;
    public ITransformer Model { get; private set; } = null!;
    public HttpClient HttpClient { get; private set; } = null!;

    public async Task InitializeAsync()
    {
        Console.WriteLine("Initializing shared state...");

        // Initialize ML.NET context
        MlContext = new MLContext(seed: 42);

        // Load pre-trained model (expensive operation - do once)
        var modelPath = Environment.GetEnvironmentVariable("MODEL_PATH")
            ?? "/models/sentiment-model.zip";

        using var stream = File.OpenRead(modelPath);
        Model = await Task.Run(() => MlContext.Model.Load(stream, out _));

        // Create reusable HTTP client with connection pooling
        HttpClient = new HttpClient
        {
            BaseAddress = new Uri(Environment.GetEnvironmentVariable("API_BASE_URL")
                ?? "https://api.example.com"),
            Timeout = TimeSpan.FromSeconds(30)
        };

        Console.WriteLine("Shared state initialized successfully");
    }

    public async Task DisposeAsync()
    {
        HttpClient?.Dispose();
        await Task.CompletedTask;
    }
}
```

```csharp
// AirflowWorker.Example/MyTaskHandler.cs
using System.Text.Json;
using AirflowWorker;

namespace AirflowWorker.Example;

/// <summary>
/// Example task handler that uses shared ML model for inference.
/// </summary>
public class SentimentAnalysisHandler : ITaskHandler
{
    public async Task ExecuteAsync(TaskExecutionContext context)
    {
        // Access shared state (no initialization needed!)
        var sharedState = context.SharedState as MySharedState
            ?? throw new InvalidOperationException("Shared state not available");

        // Get task parameters from workload
        var parameters = context.Workload.GetProperty("parameters");
        var inputText = parameters.GetProperty("text").GetString()!;

        // Use the pre-loaded ML model (no loading delay!)
        var predictionEngine = sharedState.MlContext
            .Model.CreatePredictionEngine<SentimentInput, SentimentOutput>(sharedState.Model);

        var prediction = predictionEngine.Predict(new SentimentInput { Text = inputText });

        Console.WriteLine($"Sentiment for '{inputText}': {prediction.Sentiment} ({prediction.Probability:P})");

        // Optionally call external API using pooled HttpClient
        var response = await sharedState.HttpClient.PostAsJsonAsync("/results", new
        {
            task_id = context.TaskKey.ToString(),
            sentiment = prediction.Sentiment,
            probability = prediction.Probability
        });

        response.EnsureSuccessStatusCode();

        await Task.CompletedTask;
    }
}

public record SentimentInput { public string Text { get; init; } = ""; }
public record SentimentOutput { public bool Sentiment { get; init; } public float Probability { get; init; } }
```

#### DAG Configuration for .NET Tasks

```python
# Example DAG using .NET worker
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

with DAG("dotnet_ml_pipeline", start_date=datetime(2024, 1, 1)) as dag:

    # This task will execute on the .NET worker
    analyze_sentiment = PythonOperator(
        task_id="analyze_sentiment",
        python_callable=lambda: None,  # Placeholder - actual execution in .NET
        executor_config={
            # Tell executor to use .NET worker
            "worker_type": "dotnet",

            # .NET task handler configuration
            "dotnet_handler_assembly": "/app/handlers/AirflowWorker.Example.dll",
            "dotnet_handler_type": "AirflowWorker.Example.SentimentAnalysisHandler",

            # Task-specific parameters (passed to handler)
            "parameters": {
                "text": "This product is amazing!"
            }
        }
    )
```

#### Executor Configuration for Multi-Language Support

Update the executor to support different worker types:

```ini
[aws_batch_worker_pool_executor]
# ... existing config ...

# Worker type configuration
worker_type = python                    # Default: python, dotnet, java, go
worker_python_job_definition = airflow-worker-python:1
worker_dotnet_job_definition = airflow-worker-dotnet:1
worker_java_job_definition = airflow-worker-java:1
```

```python
# In executor: select job definition based on task's executor_config
def _build_worker_submit_kwargs(self, worker_id: str, worker_type: str = "python") -> dict:
    base_kwargs = deepcopy(self.submit_job_kwargs)

    # Select job definition based on worker type
    job_def_key = f"worker_{worker_type}_job_definition"
    job_definition = conf.get(CONFIG_GROUP_NAME, job_def_key, fallback=self.job_definition)

    if worker_type == "python":
        base_kwargs['containerOverrides']['command'] = [
            'python', '-m',
            'airflow.providers.amazon.aws.executors.batch.worker_pool_worker',
            '--worker-id', worker_id,
            # ... other args ...
        ]
    elif worker_type == "dotnet":
        base_kwargs['containerOverrides']['command'] = [
            'dotnet', 'AirflowWorker.dll',
            '--worker-id', worker_id,
            # ... other args ...
        ]

    return base_kwargs
```

#### Comparison: Fork vs In-Process Execution

| Aspect | Python (Fork) | .NET/Java/Go (In-Process) |
|--------|---------------|---------------------------|
| **Isolation** | Full process isolation | Shared memory space |
| **Memory leaks** | Cleaned up per task | Can accumulate |
| **Crash handling** | Worker survives | Worker may crash |
| **Shared state** | Copy-on-write (read-only effective) | Direct mutable access |
| **Performance** | ~10ms fork overhead | No overhead |
| **Concurrency** | Safe by default | Must handle thread safety |
| **Debugging** | Harder (separate process) | Easier (same process) |

#### Mitigation for In-Process Risks

For .NET/Java/Go workers without fork isolation:

1. **Exception handling**: Wrap all task execution in try-catch to prevent worker crash
2. **Memory monitoring**: Track memory usage, restart worker if threshold exceeded
3. **Task timeout**: Use CancellationToken with timeout to prevent hung tasks
4. **AppDomain isolation** (.NET): Optionally load task handlers in separate AppDomain
5. **Health checks**: Implement `/health` endpoint for liveness probes

```csharp
// In WorkerProcess.cs - add memory monitoring
private async Task ExecuteTaskAsync(TaskQueueMessage taskMessage)
{
    // Check memory before starting
    var memoryBefore = GC.GetTotalMemory(forceFullCollection: false);
    const long MaxMemoryBytes = 2L * 1024 * 1024 * 1024; // 2GB limit

    if (memoryBefore > MaxMemoryBytes)
    {
        _logger.LogWarning(
            "Memory usage {Memory:N0} bytes exceeds threshold, terminating worker",
            memoryBefore);
        _running = false;
        return;
    }

    // Execute with timeout
    using var cts = new CancellationTokenSource(_visibilityTimeout - TimeSpan.FromMinutes(1));

    try
    {
        await ExecuteTaskInProcessAsync(context).WaitAsync(cts.Token);
        state = "SUCCESS";
    }
    catch (OperationCanceledException)
    {
        state = "FAILED";
        errorMessage = $"Task timed out after {_visibilityTimeout.TotalSeconds}s";
    }
    catch (Exception ex)
    {
        state = "FAILED";
        errorMessage = ex.ToString();
    }

    // Force GC after task to reclaim memory
    GC.Collect(generation: 2, mode: GCCollectionMode.Optimized);
}
```

---

### 5. Supporting Data Structures

**File**: `providers/amazon/src/airflow/providers/amazon/aws/executors/batch/worker_pool_utils.py`

```python
"""Utility classes and configuration for AWS Batch Worker Pool Executor."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import TYPE_CHECKING

from airflow.providers.amazon.aws.executors.utils.base_config_keys import BaseConfigKeys

if TYPE_CHECKING:
    pass

# Config group name
CONFIG_GROUP_NAME = "aws_batch_worker_pool_executor"

# Default configuration values
CONFIG_DEFAULTS = {
    "conn_id": "aws_default",
    "worker_idle_timeout_seconds": "300",
    "worker_max_tasks": "0",
    "worker_visibility_timeout_seconds": "3600",
    "min_workers": "0",
    "max_workers": "0",
    "tasks_per_worker": "1",
    "check_health_on_startup": "True",
    "worker_health_check_interval_seconds": "60",
    "max_worker_start_attempts": "3",
    "worker_job_name_prefix": "airflow-worker",
    "shared_state_init_function": "initialize",
}


class WorkerPoolConfigKeys(BaseConfigKeys):
    """Configuration keys for AWS Batch Worker Pool Executor."""

    # AWS Connection
    CONN_ID = "conn_id"
    REGION_NAME = "region_name"

    # Batch Resources
    JOB_QUEUE = "job_queue"
    JOB_DEFINITION = "job_definition"
    WORKER_JOB_NAME_PREFIX = "worker_job_name_prefix"
    SUBMIT_JOB_KWARGS = "submit_job_kwargs"

    # SQS Queues
    TASK_QUEUE_URL = "task_queue_url"
    RESULT_QUEUE_URL = "result_queue_url"

    # Worker Lifecycle
    WORKER_IDLE_TIMEOUT_SECONDS = "worker_idle_timeout_seconds"
    WORKER_MAX_TASKS = "worker_max_tasks"
    WORKER_VISIBILITY_TIMEOUT_SECONDS = "worker_visibility_timeout_seconds"

    # Scaling
    MIN_WORKERS = "min_workers"
    MAX_WORKERS = "max_workers"
    TASKS_PER_WORKER = "tasks_per_worker"

    # Health
    CHECK_HEALTH_ON_STARTUP = "check_health_on_startup"
    WORKER_HEALTH_CHECK_INTERVAL_SECONDS = "worker_health_check_interval_seconds"
    MAX_WORKER_START_ATTEMPTS = "max_worker_start_attempts"

    # Shared State
    SHARED_STATE_INIT_MODULE = "shared_state_init_module"
    SHARED_STATE_INIT_FUNCTION = "shared_state_init_function"


@dataclass
class WorkerInfo:
    """Information about an active worker."""
    worker_id: str
    job_id: str
    started_at: datetime
    tasks_assigned: int = 0
    last_seen: datetime | None = None


@dataclass
class WorkerStartRequest:
    """Request to start a new worker."""
    worker_id: str
    attempt_number: int
    next_attempt_time: datetime


class WorkerCollection:
    """Collection to track active workers."""

    def __init__(self):
        self.job_id_to_worker: dict[str, WorkerInfo] = {}
        self.worker_id_to_job_id: dict[str, str] = {}

    def add_worker(self, job_id: str, worker_id: str, started_at: datetime):
        """Add a worker to the collection."""
        info = WorkerInfo(
            worker_id=worker_id,
            job_id=job_id,
            started_at=started_at
        )
        self.job_id_to_worker[job_id] = info
        self.worker_id_to_job_id[worker_id] = job_id

    def pop_by_job_id(self, job_id: str) -> WorkerInfo:
        """Remove and return worker by job ID."""
        info = self.job_id_to_worker.pop(job_id)
        del self.worker_id_to_job_id[info.worker_id]
        return info

    def get_all_job_ids(self) -> list[str]:
        """Get all active worker job IDs."""
        return list(self.job_id_to_worker.keys())

    def has_job_id(self, job_id: str) -> bool:
        """Check if job ID is in collection."""
        return job_id in self.job_id_to_worker

    def clear(self):
        """Clear all workers."""
        self.job_id_to_worker.clear()
        self.worker_id_to_job_id.clear()

    def __len__(self):
        return len(self.job_id_to_worker)

    def __bool__(self):
        return bool(self.job_id_to_worker)
```

---

### 6. AWS Resource Requirements

#### SQS Queues

**Task Queue**:
```bash
aws sqs create-queue \
  --queue-name airflow-worker-pool-tasks \
  --attributes '{
    "VisibilityTimeout": "3600",
    "MessageRetentionPeriod": "86400",
    "ReceiveMessageWaitTimeSeconds": "20"
  }'
```

**Result Queue**:
```bash
aws sqs create-queue \
  --queue-name airflow-worker-pool-results \
  --attributes '{
    "VisibilityTimeout": "30",
    "MessageRetentionPeriod": "3600",
    "ReceiveMessageWaitTimeSeconds": "0"
  }'
```

**Dead Letter Queue** (for failed tasks):
```bash
aws sqs create-queue \
  --queue-name airflow-worker-pool-tasks-dlq

aws sqs set-queue-attributes \
  --queue-url <task-queue-url> \
  --attributes '{
    "RedrivePolicy": "{\"deadLetterTargetArn\":\"<dlq-arn>\",\"maxReceiveCount\":\"3\"}"
  }'
```

#### IAM Policies

**Executor Role** (Scheduler):
```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "BatchPermissions",
      "Effect": "Allow",
      "Action": [
        "batch:SubmitJob",
        "batch:DescribeJobs",
        "batch:TerminateJob"
      ],
      "Resource": "*"
    },
    {
      "Sid": "SQSTaskQueue",
      "Effect": "Allow",
      "Action": [
        "sqs:SendMessage",
        "sqs:GetQueueAttributes"
      ],
      "Resource": "arn:aws:sqs:*:*:airflow-worker-pool-tasks"
    },
    {
      "Sid": "SQSResultQueue",
      "Effect": "Allow",
      "Action": [
        "sqs:ReceiveMessage",
        "sqs:DeleteMessage",
        "sqs:GetQueueAttributes"
      ],
      "Resource": "arn:aws:sqs:*:*:airflow-worker-pool-results"
    }
  ]
}
```

**Worker Role** (Batch containers):
```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "SQSTaskQueue",
      "Effect": "Allow",
      "Action": [
        "sqs:ReceiveMessage",
        "sqs:DeleteMessage",
        "sqs:ChangeMessageVisibility",
        "sqs:GetQueueAttributes"
      ],
      "Resource": "arn:aws:sqs:*:*:airflow-worker-pool-tasks"
    },
    {
      "Sid": "SQSResultQueue",
      "Effect": "Allow",
      "Action": [
        "sqs:SendMessage"
      ],
      "Resource": "arn:aws:sqs:*:*:airflow-worker-pool-results"
    },
    {
      "Sid": "S3Logs",
      "Effect": "Allow",
      "Action": [
        "s3:PutObject",
        "s3:GetObject"
      ],
      "Resource": "arn:aws:s3:::airflow-logs/*"
    }
  ]
}
```

#### Batch Job Definition

```bash
aws batch register-job-definition \
  --job-definition-name airflow-worker-pool \
  --type container \
  --platform-capabilities FARGATE \
  --container-properties '{
    "image": "<ecr-repo>/airflow-worker:latest",
    "resourceRequirements": [
      {"type": "VCPU", "value": "2"},
      {"type": "MEMORY", "value": "4096"}
    ],
    "executionRoleArn": "arn:aws:iam::<account>:role/ecsTaskExecutionRole",
    "jobRoleArn": "arn:aws:iam::<account>:role/airflowWorkerPoolRole",
    "fargatePlatformConfiguration": {"platformVersion": "LATEST"},
    "networkConfiguration": {
      "assignPublicIp": "ENABLED"
    }
  }' \
  --timeout '{"attemptDurationSeconds": 86400}'
```

Note: The `attemptDurationSeconds` should be set high (24 hours) since workers are long-running. Workers self-terminate via idle timeout.

---

## File Structure

```
providers/amazon/src/airflow/providers/amazon/aws/executors/batch/
├── __init__.py
├── batch_executor.py                    # Existing (one job per task)
├── batch_executor_config.py             # Existing
├── boto_schema.py                       # Existing
├── utils.py                             # Existing
│
├── batch_worker_pool_executor.py        # NEW: Main executor class
├── worker_pool_worker.py                # NEW: Worker process entry point
├── worker_pool_utils.py                 # NEW: Config keys, data classes
└── worker_pool_schemas.py               # NEW: Pydantic message schemas

providers/amazon/tests/unit/amazon/aws/executors/batch/
├── test_batch_executor.py               # Existing
├── test_batch_worker_pool_executor.py   # NEW: Executor tests
└── test_worker_pool_worker.py           # NEW: Worker tests
```

---

## Implementation Steps

### Phase 1: Core Infrastructure

1. **Create configuration classes** (`worker_pool_utils.py`)
   - Config keys enum
   - Default values
   - WorkerCollection, WorkerInfo, WorkerStartRequest dataclasses

2. **Create message schemas** (`worker_pool_schemas.py`)
   - TaskQueueMessage (Pydantic)
   - TaskResultMessage (Pydantic)
   - Validation and serialization

3. **Create worker process** (`worker_pool_worker.py`)
   - Command-line argument parsing
   - SQS polling loop
   - Subprocess task execution
   - Result reporting
   - Idle timeout handling
   - Signal handling (SIGTERM)

### Phase 2: Executor Implementation

4. **Create executor class** (`batch_worker_pool_executor.py`)
   - Inherit from BaseExecutor
   - Implement `queue_workload()` and `_process_workloads()`
   - Implement `sync()` with:
     - Result queue polling
     - Worker health checking
     - Worker scaling
   - Implement lifecycle methods: `start()`, `end()`, `terminate()`
   - Implement `try_adopt_task_instances()`

### Phase 3: Testing

5. **Unit tests for worker**
   - SQS message parsing
   - Subprocess execution
   - Idle timeout
   - Signal handling

6. **Unit tests for executor**
   - Task queuing
   - Worker scaling logic
   - Result processing
   - Graceful shutdown

7. **Integration tests** (in Breeze)
   - End-to-end with LocalStack or actual AWS
   - Multiple workers
   - Task failures
   - Worker termination

### Phase 4: Documentation & Polish

8. **Documentation**
   - Update provider docs
   - Configuration reference
   - Deployment guide
   - Migration from AwsBatchExecutor

9. **Provider integration**
   - Add to provider.yaml entry points
   - Export in `__init__.py`

---

## Comparison: Current vs. Worker Pool

| Aspect | AwsBatchExecutor | AwsBatchWorkerPoolExecutor |
|--------|------------------|----------------------------|
| Jobs per task | 1 Batch job per task | 1 Batch job per N tasks |
| Initialization | Every task | Once per worker |
| Cold start | 20-100 seconds | First worker only |
| State sharing | None | Yes (via SHARED_STATE) |
| Cost model | Per-task overhead | Per-worker overhead |
| Scaling | Automatic (per task) | On-demand (per parallelism) |
| Complexity | Lower | Higher |
| Best for | Variable, isolated tasks | Stateful, homogeneous tasks |

---

## Risks & Mitigations

| Risk | Mitigation |
|------|------------|
| Worker hangs indefinitely | Idle timeout auto-terminates |
| SQS message loss | DLQ + visibility timeout |
| Memory leaks across tasks | Subprocess isolation |
| Orphaned workers after scheduler crash | Workers self-terminate on idle |
| Race condition in result processing | Idempotent state updates |
| Worker dies mid-task | Message returns to queue (visibility timeout) |

---

## Future Enhancements

1. **Worker Groups**: Different worker pools for different task types
2. **Spot Instance Support**: Configuration for spot capacity
3. **Metrics/Monitoring**: CloudWatch metrics for worker utilization
4. **Warm Pool**: Keep min_workers running for instant task execution
5. **Task Priority**: SQS FIFO queue with priority-based ordering
