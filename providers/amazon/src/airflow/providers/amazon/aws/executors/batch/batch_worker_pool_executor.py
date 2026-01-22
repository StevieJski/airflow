# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
"""
AWS Batch Worker Pool Executor.

This executor spawns on-demand, long-running worker processes in AWS Batch
that pull tasks from an SQS queue, execute them in isolated subprocesses,
and terminate after an idle timeout.
"""

from __future__ import annotations

import json
import time
import uuid
from collections import deque
from collections.abc import Sequence
from copy import deepcopy
from typing import TYPE_CHECKING, Any

from botocore.exceptions import ClientError, NoCredentialsError

from airflow.configuration import conf
from airflow.exceptions import AirflowException
from airflow.executors.base_executor import BaseExecutor
from airflow.providers.amazon.aws.executors.batch.worker_pool_schemas import (
    TaskInstanceKeySchema,
    TaskQueueMessage,
    TaskResultMessage,
)
from airflow.providers.amazon.aws.executors.batch.worker_pool_utils import (
    CONFIG_DEFAULTS,
    CONFIG_GROUP_NAME,
    WorkerCollection,
    WorkerPoolConfigKeys,
    WorkerStartRequest,
)
from airflow.providers.amazon.aws.executors.utils.exponential_backoff_retry import (
    calculate_next_attempt_delay,
    exponential_backoff_retry,
)
from airflow.providers.amazon.aws.hooks.batch_client import BatchClientHook

try:
    from airflow.sdk import timezone
except ImportError:
    from airflow.utils import timezone  # type: ignore[attr-defined,no-redef]

if TYPE_CHECKING:
    from sqlalchemy.orm import Session

    from airflow.executors import workloads
    from airflow.models.taskinstance import TaskInstance, TaskInstanceKey

INVALID_CREDENTIALS_EXCEPTIONS = [
    "ExpiredTokenException",
    "InvalidClientTokenId",
    "UnrecognizedClientException",
]


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

    # AWS only allows a maximum number of JOBs in the describe_jobs function
    DESCRIBE_JOBS_BATCH_SIZE = 99

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

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
        self.IS_BOTO_CONNECTION_HEALTHY = True  # Assume healthy until proven otherwise
        self.last_connection_reload = timezone.utcnow()  # Initialize to avoid None errors
        self.last_worker_health_check = None
        self.attempts_since_last_successful_connection = 0

    def _load_config(self):
        """Load configuration from airflow.cfg."""
        # AWS Connection
        self.conn_id = conf.get(
            CONFIG_GROUP_NAME, WorkerPoolConfigKeys.CONN_ID, fallback=CONFIG_DEFAULTS["conn_id"]
        )
        self.region_name = conf.get(CONFIG_GROUP_NAME, WorkerPoolConfigKeys.REGION_NAME, fallback=None)

        # Batch Resources
        self.job_queue = conf.get(CONFIG_GROUP_NAME, WorkerPoolConfigKeys.JOB_QUEUE, fallback=None)
        self.job_definition = conf.get(CONFIG_GROUP_NAME, WorkerPoolConfigKeys.JOB_DEFINITION, fallback=None)
        self.worker_job_name_prefix = conf.get(
            CONFIG_GROUP_NAME,
            WorkerPoolConfigKeys.WORKER_JOB_NAME_PREFIX,
            fallback=CONFIG_DEFAULTS["worker_job_name_prefix"],
        )

        # SQS Queues
        self.task_queue_url = conf.get(CONFIG_GROUP_NAME, WorkerPoolConfigKeys.TASK_QUEUE_URL, fallback=None)
        self.result_queue_url = conf.get(
            CONFIG_GROUP_NAME, WorkerPoolConfigKeys.RESULT_QUEUE_URL, fallback=None
        )

        # Worker Lifecycle
        self.worker_idle_timeout_seconds = conf.getint(
            CONFIG_GROUP_NAME,
            WorkerPoolConfigKeys.WORKER_IDLE_TIMEOUT_SECONDS,
            fallback=int(CONFIG_DEFAULTS["worker_idle_timeout_seconds"]),
        )
        self.worker_visibility_timeout_seconds = conf.getint(
            CONFIG_GROUP_NAME,
            WorkerPoolConfigKeys.WORKER_VISIBILITY_TIMEOUT_SECONDS,
            fallback=int(CONFIG_DEFAULTS["worker_visibility_timeout_seconds"]),
        )

        # Scaling
        self.min_workers = conf.getint(
            CONFIG_GROUP_NAME, WorkerPoolConfigKeys.MIN_WORKERS, fallback=int(CONFIG_DEFAULTS["min_workers"])
        )
        self.max_workers = conf.getint(
            CONFIG_GROUP_NAME, WorkerPoolConfigKeys.MAX_WORKERS, fallback=int(CONFIG_DEFAULTS["max_workers"])
        )
        self.tasks_per_worker = conf.getint(
            CONFIG_GROUP_NAME,
            WorkerPoolConfigKeys.TASKS_PER_WORKER,
            fallback=int(CONFIG_DEFAULTS["tasks_per_worker"]),
        )

        # Health
        self.check_health_on_startup = conf.getboolean(
            CONFIG_GROUP_NAME,
            WorkerPoolConfigKeys.CHECK_HEALTH_ON_STARTUP,
            fallback=CONFIG_DEFAULTS["check_health_on_startup"].lower() == "true",
        )
        self.worker_health_check_interval_seconds = conf.getint(
            CONFIG_GROUP_NAME,
            WorkerPoolConfigKeys.WORKER_HEALTH_CHECK_INTERVAL_SECONDS,
            fallback=int(CONFIG_DEFAULTS["worker_health_check_interval_seconds"]),
        )
        self.max_worker_start_attempts = conf.getint(
            CONFIG_GROUP_NAME,
            WorkerPoolConfigKeys.MAX_WORKER_START_ATTEMPTS,
            fallback=int(CONFIG_DEFAULTS["max_worker_start_attempts"]),
        )

        # Shared State
        self.shared_state_init_module = conf.get(
            CONFIG_GROUP_NAME, WorkerPoolConfigKeys.SHARED_STATE_INIT_MODULE, fallback=None
        )
        self.shared_state_init_function = conf.get(
            CONFIG_GROUP_NAME,
            WorkerPoolConfigKeys.SHARED_STATE_INIT_FUNCTION,
            fallback=CONFIG_DEFAULTS["shared_state_init_function"],
        )

        # Submit Job kwargs (JSON)
        submit_job_kwargs_str = conf.get(
            CONFIG_GROUP_NAME, WorkerPoolConfigKeys.SUBMIT_JOB_KWARGS, fallback="{}"
        )
        self.submit_job_kwargs = json.loads(submit_job_kwargs_str) if submit_job_kwargs_str else {}

        # Native Worker mode (for .NET or other non-Python workers with their own entrypoint)
        self.use_native_worker = conf.getboolean(
            CONFIG_GROUP_NAME,
            WorkerPoolConfigKeys.USE_NATIVE_WORKER,
            fallback=CONFIG_DEFAULTS["use_native_worker"].lower() == "true",
        )

        # Airflow Execution API URL (required for native workers to transition task state)
        # Falls back to api.base_url if not explicitly configured
        self.execution_api_url = conf.get(
            CONFIG_GROUP_NAME, WorkerPoolConfigKeys.EXECUTION_API_URL, fallback=None
        ) or conf.get("api", "base_url", fallback=None)

    #
    # Lifecycle Methods
    #

    def start(self):
        """Initialize executor, validate connections, optionally start min_workers."""
        self.log.info("Starting AWS Batch Worker Pool Executor...")

        if self.check_health_on_startup:
            try:
                self.check_health()
            except AirflowException:
                self.log.error("Stopping the Airflow Scheduler from starting until the issue is resolved.")
                raise

        # Start minimum workers if configured
        if self.min_workers > 0:
            self.log.info("Starting %d minimum workers", self.min_workers)
            for i in range(self.min_workers):
                self._request_worker_start(worker_id=f"init-{i}")

    def check_health(self):
        """Make a test API call to check the health of the executor."""
        success_status = "succeeded."
        status = success_status

        try:
            # Test Batch connection
            invalid_job_id = "a" * 32
            self.batch_client.describe_jobs(jobs=[invalid_job_id])

            # Test SQS connections
            self.sqs_client.get_queue_attributes(
                QueueUrl=self.task_queue_url, AttributeNames=["ApproximateNumberOfMessages"]
            )
            self.sqs_client.get_queue_attributes(
                QueueUrl=self.result_queue_url, AttributeNames=["ApproximateNumberOfMessages"]
            )
        except ClientError as ex:
            error_code = ex.response["Error"]["Code"]
            error_message = ex.response["Error"]["Message"]
            status = f"failed because: {error_code}: {error_message}. "
        except Exception as e:
            status = f"failed because: {e}. "
        finally:
            msg_prefix = "Batch Worker Pool Executor health check has %s"
            if status == success_status:
                self.IS_BOTO_CONNECTION_HEALTHY = True
                self.attempts_since_last_successful_connection = 0
                self.log.info(msg_prefix, status)
            else:
                msg_error_suffix = (
                    "The Batch Worker Pool executor will not be able to run Airflow tasks "
                    "until the issue is addressed."
                )
                raise AirflowException(msg_prefix % status + msg_error_suffix)

    def end(self, heartbeat_interval=10):
        """Wait for all tasks to complete, then terminate workers."""
        self.log.info("Ending worker pool executor, waiting for tasks...")

        try:
            while self.tasks_in_queue or self.running:
                self.sync()
                if not self.tasks_in_queue and not self.running:
                    break
                time.sleep(heartbeat_interval)
        except Exception:
            self.log.exception("Failed to end %s", self.__class__.__name__)

        # Terminate all workers
        self._terminate_all_workers(reason="Executor shutdown")

    def terminate(self):
        """Force terminate all workers immediately."""
        self.log.info("Terminating worker pool executor")
        try:
            self._terminate_all_workers(reason="Executor SIGTERM")
            self.end()
        except Exception:
            self.log.exception("Failed to terminate %s", self.__class__.__name__)

    #
    # Task Queueing
    #

    def queue_workload(self, workload: workloads.All, session: Session | None) -> None:
        """Queue a workload for execution."""
        from airflow.executors import workloads as workloads_module

        if not isinstance(workload, workloads_module.ExecuteTask):
            raise RuntimeError(f"{type(self)} cannot handle workloads of type {type(workload)}")

        ti = workload.ti
        self.queued_tasks[ti.key] = workload

    def _process_workloads(self, workloads_list: Sequence[workloads.All]) -> None:
        """Send tasks to SQS queue for workers to pick up."""
        from airflow.executors import workloads as workloads_module

        for w in workloads_list:
            if not isinstance(w, workloads_module.ExecuteTask):
                raise RuntimeError(f"{type(self)} cannot handle workloads of type {type(w)}")

            key = w.ti.key
            executor_config = w.ti.executor_config or {}

            # Serialize the workload
            workload_json = w.model_dump_json()

            # Create task message
            message = TaskQueueMessage(
                message_id=str(uuid.uuid4()),
                task_key=TaskInstanceKeySchema.from_task_instance_key(key),
                workload_json=workload_json,
                executor_config=executor_config,
                enqueued_at=timezone.utcnow(),
                execution_api_url=self.execution_api_url,
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
                "task_id": {"StringValue": str(message.task_key), "DataType": "String"},
                "dag_id": {"StringValue": message.task_key.dag_id, "DataType": "String"},
            },
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
        # DEBUG: Log when sync is called
        self.log.warning(
            "SYNC CALLED! tasks_in_queue=%d, running=%d, queued_tasks=%d",
            len(self.tasks_in_queue),
            len(self.running),
            len(self.queued_tasks),
        )
        if not self.IS_BOTO_CONNECTION_HEALTHY:
            exponential_backoff_retry(
                self.last_connection_reload,
                self.attempts_since_last_successful_connection,
                self._attempt_reconnect,
            )
            if not self.IS_BOTO_CONNECTION_HEALTHY:
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

        except (ClientError, NoCredentialsError) as error:
            error_code = error.response.get("Error", {}).get("Code")
            if error_code in INVALID_CREDENTIALS_EXCEPTIONS:
                self.IS_BOTO_CONNECTION_HEALTHY = False
                self.log.warning(
                    "AWS credentials are either missing or expired: %s.\nRetrying connection", error
                )
        except Exception:
            self.log.exception("Failed to sync %s", self.__class__.__name__)

    def _attempt_reconnect(self):
        """Attempt to reconnect to AWS services."""
        self.log.info("Attempting to reconnect to AWS services...")
        self.attempts_since_last_successful_connection += 1
        self.last_connection_reload = timezone.utcnow()

        # Reset cached clients to force reconnection
        self._batch_client = None
        self._sqs_client = None

        try:
            self.check_health()
        except AirflowException:
            self.log.warning("Reconnection attempt failed")

    def _poll_result_queue(self):
        """Poll SQS result queue for completed task results."""
        while True:
            response = self.sqs_client.receive_message(
                QueueUrl=self.result_queue_url,
                MaxNumberOfMessages=10,  # Batch for efficiency
                WaitTimeSeconds=0,  # Non-blocking
                MessageAttributeNames=["All"],
            )

            messages = response.get("Messages", [])
            if not messages:
                break

            for msg in messages:
                try:
                    result = TaskResultMessage.model_validate_json(msg["Body"])
                    self._handle_task_result(result)

                    # Delete processed message
                    self.sqs_client.delete_message(
                        QueueUrl=self.result_queue_url,
                        ReceiptHandle=msg["ReceiptHandle"],
                    )
                except Exception:
                    self.log.exception("Failed to process result message: %s", msg)

    def _handle_task_result(self, result: TaskResultMessage):
        """Handle a task completion result from a worker."""
        key = result.task_key.to_task_instance_key()

        # Remove from tracking
        self.tasks_in_queue.pop(key, None)

        # Update executor state
        if result.state == "SUCCESS":
            self.success(key, info=result.info.model_dump())
        else:
            self.fail(key, info=result.info.model_dump())

        self.log.info(
            "Task %s completed with state %s (worker: %s, time: %.2fs)",
            key,
            result.state,
            result.info.worker_id,
            result.info.execution_time_seconds,
        )

    #
    # Worker Scaling
    #

    def _scale_workers(self):
        """Scale workers based on queue depth and parallelism."""
        # DEBUG: Log scaling check
        self.log.warning("SCALE_WORKERS called!")
        # Calculate desired workers
        tasks_pending = self._get_queue_depth()
        current_workers = len(self.active_workers)
        self.log.warning(
            "SCALE CHECK: tasks_pending=%d, current_workers=%d, max_workers=%d, parallelism=%d",
            tasks_pending,
            current_workers,
            self.max_workers,
            self.parallelism,
        )

        # Respect parallelism limit
        max_allowed = self.max_workers if self.max_workers > 0 else self.parallelism

        # Calculate tasks per worker capacity
        tasks_per_worker = self.tasks_per_worker if self.tasks_per_worker > 0 else 1
        desired_workers = min(
            max_allowed, max(self.min_workers, (tasks_pending + tasks_per_worker - 1) // tasks_per_worker)
        )

        # Scale up if needed
        if desired_workers > current_workers:
            workers_to_add = desired_workers - current_workers
            self.log.info(
                "Scaling up: adding %d workers (pending tasks: %d, current workers: %d)",
                workers_to_add,
                tasks_pending,
                current_workers,
            )
            # Queue batch of workers to add - will be submitted as array job if > 1
            base_worker_id = f"scale-{int(timezone.utcnow().timestamp())}"
            self._request_workers_batch(base_worker_id=base_worker_id, count=workers_to_add)

        # Note: Scale down happens automatically via worker idle timeout

    def _get_queue_depth(self) -> int:
        """Get approximate number of messages in task queue."""
        response = self.sqs_client.get_queue_attributes(
            QueueUrl=self.task_queue_url,
            AttributeNames=["ApproximateNumberOfMessages", "ApproximateNumberOfMessagesNotVisible"],
        )
        attrs = response.get("Attributes", {})
        visible = int(attrs.get("ApproximateNumberOfMessages", 0))
        in_flight = int(attrs.get("ApproximateNumberOfMessagesNotVisible", 0))
        return visible + in_flight

    def _request_workers_batch(self, base_worker_id: str, count: int):
        """Queue a batch of workers to start.

        If count > 1, will be submitted as an array job.
        If count == 1, will be submitted as individual job.
        """
        self.pending_worker_starts.append(
            WorkerStartRequest(
                worker_id=base_worker_id,
                attempt_number=1,
                next_attempt_time=timezone.utcnow(),
                batch_size=count,  # New field for batch size
            )
        )

    def _submit_pending_worker_starts(self):
        """Submit pending worker start requests to AWS Batch.

        Uses array jobs when batch_size > 1 for efficiency.
        """
        self.log.warning(
            "SUBMIT_PENDING_WORKER_STARTS: %d pending requests", len(self.pending_worker_starts)
        )
        for _ in range(len(self.pending_worker_starts)):
            request = self.pending_worker_starts.popleft()

            # Check if we've hit the max workers limit
            max_allowed = self.max_workers if self.max_workers > 0 else self.parallelism
            current_count = len(self.active_workers)
            if current_count >= max_allowed:
                self.log.warning(
                    "Max workers reached (%d >= %d), skipping worker start",
                    current_count,
                    max_allowed,
                )
                continue

            # Adjust batch size if it would exceed max workers
            batch_size = request.batch_size
            if current_count + batch_size > max_allowed:
                batch_size = max_allowed - current_count
                self.log.info(
                    "Reducing batch size from %d to %d to respect max_workers limit",
                    request.batch_size,
                    batch_size,
                )

            # Check retry timing
            if timezone.utcnow() < request.next_attempt_time:
                self.log.warning("Worker batch %s not ready yet, re-queuing", request.worker_id)
                self.pending_worker_starts.append(request)
                continue

            try:
                if batch_size == 1:
                    # Single worker - use individual job submission
                    self.log.info("Submitting single worker job: %s", request.worker_id)
                    job_id = self._start_worker(f"{request.worker_id}-0")
                    self.active_workers.add_worker(
                        job_id=job_id,
                        worker_id=f"{request.worker_id}-0",
                        started_at=timezone.utcnow(),
                    )
                    self.log.info("Started worker %s-0 (Batch job: %s)", request.worker_id, job_id)
                else:
                    # Multiple workers - use array job
                    self.log.info(
                        "Submitting array job with %d workers: base_id=%s",
                        batch_size,
                        request.worker_id,
                    )
                    parent_job_id = self._start_array_job(request.worker_id, batch_size)
                    self.active_workers.add_array_job(
                        parent_job_id=parent_job_id,
                        base_worker_id=request.worker_id,
                        array_size=batch_size,
                        started_at=timezone.utcnow(),
                    )
                    self.log.info(
                        "Started array job %s with %d workers (base: %s)",
                        parent_job_id,
                        batch_size,
                        request.worker_id,
                    )

            except ClientError as e:
                if request.attempt_number >= self.max_worker_start_attempts:
                    self.log.error(
                        "Failed to start workers %s after %d attempts: %s",
                        request.worker_id,
                        request.attempt_number,
                        e,
                    )
                else:
                    request.attempt_number += 1
                    request.next_attempt_time = timezone.utcnow() + calculate_next_attempt_delay(
                        request.attempt_number
                    )
                    self.pending_worker_starts.append(request)
            except Exception as e:
                self.log.error(
                    "UNEXPECTED ERROR starting workers %s: %s", request.worker_id, e, exc_info=True
                )
                # Re-queue for retry
                if request.attempt_number < self.max_worker_start_attempts:
                    request.attempt_number += 1
                    request.next_attempt_time = timezone.utcnow() + calculate_next_attempt_delay(
                        request.attempt_number
                    )
                    self.pending_worker_starts.append(request)

    def _start_worker(self, worker_id: str) -> str:
        """Submit a single worker job to AWS Batch. Returns job ID."""
        job_name = f"{self.worker_job_name_prefix}-{worker_id}"
        self.log.info("Submitting single worker job: %s", job_name)

        submit_kwargs = self._build_worker_submit_kwargs(worker_id)

        try:
            response = self.batch_client.submit_job(
                jobName=job_name,
                jobQueue=self.job_queue,
                jobDefinition=self.job_definition,
                **submit_kwargs,
            )
            self.log.info("Started worker %s (job_id: %s)", worker_id, response["jobId"])
            return response["jobId"]
        except Exception as e:
            self.log.error("Failed to start worker %s: %s", worker_id, e, exc_info=True)
            raise

    def _start_array_job(self, base_worker_id: str, array_size: int) -> str:
        """Submit an array job to AWS Batch. Returns parent job ID.

        Array jobs create multiple child jobs (indexed 0 to array_size-1).
        Each child gets AWS_BATCH_JOB_ARRAY_INDEX environment variable set automatically.
        """
        job_name = f"{self.worker_job_name_prefix}-array-{base_worker_id}"
        self.log.info("Submitting array job: %s (size=%d)", job_name, array_size)

        # Build kwargs with base worker ID - workers will append array index
        submit_kwargs = self._build_worker_submit_kwargs(base_worker_id, is_array_job=True)

        # Add array properties
        submit_kwargs["arrayProperties"] = {"size": array_size}

        try:
            response = self.batch_client.submit_job(
                jobName=job_name,
                jobQueue=self.job_queue,
                jobDefinition=self.job_definition,
                **submit_kwargs,
            )
            parent_job_id = response["jobId"]
            self.log.info(
                "Started array job %s with %d workers (parent_job_id: %s)",
                job_name,
                array_size,
                parent_job_id,
            )
            return parent_job_id
        except Exception as e:
            self.log.error("Failed to start array job %s: %s", job_name, e, exc_info=True)
            raise

    def _build_worker_submit_kwargs(self, worker_id: str, is_array_job: bool = False) -> dict[str, Any]:
        """Build kwargs for Batch submit_job API.

        Args:
            worker_id: Worker ID (or base worker ID for array jobs)
            is_array_job: If True, worker ID is a base ID and workers should append
                         AWS_BATCH_JOB_ARRAY_INDEX to construct their full ID
        """
        base_kwargs = deepcopy(self.submit_job_kwargs)

        if "containerOverrides" not in base_kwargs:
            base_kwargs["containerOverrides"] = {}

        # Set environment variables first (needed for both native and Python workers)
        if "environment" not in base_kwargs["containerOverrides"]:
            base_kwargs["containerOverrides"]["environment"] = []

        # Core environment variables for all workers
        # For array jobs, AIRFLOW_WORKER_ID_BASE is set and worker appends array index
        # For individual jobs, AIRFLOW_WORKER_ID is the full worker ID
        env_vars = [
            {"name": "AIRFLOW_WORKER_POOL_MODE", "value": "true"},
            {"name": "AIRFLOW_TASK_QUEUE_URL", "value": self.task_queue_url},
            {"name": "AIRFLOW_RESULT_QUEUE_URL", "value": self.result_queue_url},
            {"name": "AIRFLOW_WORKER_IDLE_TIMEOUT", "value": str(self.worker_idle_timeout_seconds)},
            {"name": "AIRFLOW_WORKER_VISIBILITY_TIMEOUT", "value": str(self.worker_visibility_timeout_seconds)},
        ]

        if is_array_job:
            # For array jobs, pass base worker ID - workers construct full ID as {base}-{array_index}
            # AWS_BATCH_JOB_ARRAY_INDEX is automatically set by AWS Batch for each child job
            env_vars.append({"name": "AIRFLOW_WORKER_ID_BASE", "value": worker_id})
            env_vars.append({"name": "AIRFLOW_IS_ARRAY_JOB", "value": "true"})
        else:
            # For individual jobs, pass the full worker ID
            env_vars.append({"name": "AIRFLOW_WORKER_ID", "value": worker_id})

        # Add Execution API URL for native workers to transition task state
        if self.execution_api_url:
            env_vars.append({"name": "AIRFLOW_EXECUTION_API_URL", "value": self.execution_api_url})
        elif self.use_native_worker:
            self.log.warning(
                "use_native_worker is enabled but execution_api_url is not configured. "
                "Native workers will not be able to transition tasks from QUEUED to RUNNING, "
                "which will cause 'state mismatch' errors. Set execution_api_url in [%s] config.",
                CONFIG_GROUP_NAME,
            )

        base_kwargs["containerOverrides"]["environment"].extend(env_vars)

        if self.use_native_worker:
            # Native worker mode: don't override command, let container use its default entrypoint
            # The native worker (e.g., .NET) reads configuration from environment variables
            self.log.info(
                "Using native worker mode - container will use its default entrypoint. "
                "Worker ID: %s, Task Queue: %s",
                worker_id,
                self.task_queue_url,
            )
        else:
            # Python worker mode: set command to run the airflow worker module
            command = [
                "python",
                "-m",
                "airflow.providers.amazon.aws.executors.batch.worker_pool_worker",
                "--worker-id",
                worker_id,
                "--task-queue-url",
                self.task_queue_url,
                "--result-queue-url",
                self.result_queue_url,
                "--idle-timeout",
                str(self.worker_idle_timeout_seconds),
                "--visibility-timeout",
                str(self.worker_visibility_timeout_seconds),
            ]

            # Add shared state init if configured
            if self.shared_state_init_module:
                command.extend(
                    [
                        "--init-module",
                        self.shared_state_init_module,
                        "--init-function",
                        self.shared_state_init_function or "initialize",
                    ]
                )

            base_kwargs["containerOverrides"]["command"] = command

        return base_kwargs

    #
    # Worker Health Monitoring
    #

    def _check_worker_health(self):
        """Check health of active workers via AWS Batch describe_jobs.

        For array jobs, we query child job IDs (format: parent_id:index).
        AWS Batch describe_jobs API accepts both individual and child job IDs.
        """
        if not self.active_workers:
            return

        # Rate limit health checks
        now = timezone.utcnow()
        if self.last_worker_health_check and (
            now - self.last_worker_health_check
        ).total_seconds() < self.worker_health_check_interval_seconds:
            return
        self.last_worker_health_check = now

        # Get all job IDs (includes array job child IDs like "parent_id:0", "parent_id:1", etc.)
        job_ids = self.active_workers.get_all_job_ids()

        for i in range(0, len(job_ids), self.DESCRIBE_JOBS_BATCH_SIZE):
            batch = job_ids[i : i + self.DESCRIBE_JOBS_BATCH_SIZE]
            response = self.batch_client.describe_jobs(jobs=batch)

            for job in response.get("jobs", []):
                job_id = job["jobId"]
                status = job["status"]

                if status in ["FAILED", "SUCCEEDED"]:
                    # Worker terminated
                    worker_info = self.active_workers.pop_by_job_id(job_id)
                    self.log.warning(
                        "Worker %s (job %s) terminated with status %s",
                        worker_info.worker_id,
                        job_id,
                        status,
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

            # If task has an external_executor_id that looks like a worker job ID,
            # check if that worker is still running
            if ti.external_executor_id:
                if self.active_workers.has_job_id(ti.external_executor_id):
                    adopted.append(ti)
                    self.tasks_in_queue[ti.key] = None  # type: ignore[assignment]
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
        """Get or create the Batch client."""
        if self._batch_client is None:
            self._batch_client = BatchClientHook(
                aws_conn_id=self.conn_id, region_name=self.region_name
            ).conn
        return self._batch_client

    @property
    def sqs_client(self):
        """Get or create the SQS client."""
        if self._sqs_client is None:
            from airflow.providers.amazon.aws.hooks.sqs import SqsHook

            self._sqs_client = SqsHook(aws_conn_id=self.conn_id, region_name=self.region_name).conn
        return self._sqs_client
