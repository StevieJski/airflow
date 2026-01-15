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
Azure Batch Worker Pool Executor.

This executor spawns on-demand, long-running worker processes in Azure Batch
that process multiple Airflow tasks. Workers:
- Pull tasks from Azure Service Bus task queue
- Execute tasks in forked subprocesses for isolation
- Report results to Service Bus result queue
- Self-terminate after idle timeout

Unlike one-job-per-task executors, workers amortize container startup cost
across many tasks and can maintain shared state (ML models, DB pools, etc.)
"""

from __future__ import annotations

import json
import logging
import uuid
from copy import deepcopy
from datetime import datetime, timedelta
from functools import cached_property
from typing import TYPE_CHECKING, Any

from azure.batch import models as batch_models

from airflow.configuration import conf
from airflow.executors.base_executor import BaseExecutor
from airflow.providers.microsoft.azure.executors.batch.worker_pool_schemas import (
    TaskInstanceKeySchema,
    TaskQueueMessage,
    TaskResultMessage,
)
from airflow.providers.microsoft.azure.executors.batch.worker_pool_utils import (
    CONFIG_DEFAULTS,
    CONFIG_GROUP_NAME,
    WorkerCollection,
    WorkerPoolConfigKeys,
    WorkerStartRequest,
)
from airflow.providers.microsoft.azure.hooks.asb import MessageHook
from airflow.providers.microsoft.azure.hooks.batch import AzureBatchHook

try:
    from airflow.sdk import timezone
except ImportError:
    from airflow.utils import timezone  # type: ignore[attr-defined,no-redef]

if TYPE_CHECKING:
    from collections.abc import Sequence

    from airflow.executors.base_executor import CommandType
    from airflow.executors.workloads import All
    from airflow.models.taskinstance import TaskInstance
    from airflow.models.taskinstancekey import TaskInstanceKey

log = logging.getLogger(__name__)


class AzureBatchWorkerPoolExecutor(BaseExecutor):
    """
    Azure Batch Executor with on-demand, long-running worker pools.

    Workers are Azure Batch tasks that process multiple Airflow tasks.
    Tasks are distributed via Azure Service Bus queues.

    Configuration is loaded from [azure_batch_worker_pool_executor] section in airflow.cfg.
    """

    supports_pickling = False

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        # Workers tracking
        self.active_workers = WorkerCollection()
        self.pending_worker_starts: list[WorkerStartRequest] = []

        # Tasks tracking: task_key -> TaskQueueMessage
        self.tasks_in_queue: dict[str, TaskQueueMessage] = {}

        # Health check tracking
        self._last_health_check = timezone.utcnow()

        # Lazy-loaded hooks
        self._batch_hook: AzureBatchHook | None = None
        self._service_bus_hook: MessageHook | None = None

    # ---- Configuration Properties ----

    def _get_config(self, key: str, fallback: str | None = None) -> str:
        """Get configuration value from airflow.cfg."""
        default = fallback or CONFIG_DEFAULTS.get(key, "")
        return conf.get(CONFIG_GROUP_NAME, key, fallback=default)

    def _get_bool_config(self, key: str, fallback: bool = False) -> bool:
        """Get boolean configuration value."""
        return conf.getboolean(CONFIG_GROUP_NAME, key, fallback=fallback)

    def _get_int_config(self, key: str, fallback: int = 0) -> int:
        """Get integer configuration value."""
        default = CONFIG_DEFAULTS.get(key)
        return conf.getint(CONFIG_GROUP_NAME, key, fallback=int(default) if default else fallback)

    @cached_property
    def azure_batch_conn_id(self) -> str:
        return self._get_config(WorkerPoolConfigKeys.AZURE_BATCH_CONN_ID)

    @cached_property
    def azure_service_bus_conn_id(self) -> str:
        return self._get_config(WorkerPoolConfigKeys.AZURE_SERVICE_BUS_CONN_ID)

    @cached_property
    def pool_id(self) -> str:
        return self._get_config(WorkerPoolConfigKeys.POOL_ID)

    @cached_property
    def job_id(self) -> str:
        return self._get_config(WorkerPoolConfigKeys.JOB_ID)

    @cached_property
    def worker_task_name_prefix(self) -> str:
        return self._get_config(WorkerPoolConfigKeys.WORKER_TASK_NAME_PREFIX)

    @cached_property
    def task_queue_name(self) -> str:
        return self._get_config(WorkerPoolConfigKeys.TASK_QUEUE_NAME)

    @cached_property
    def result_queue_name(self) -> str:
        return self._get_config(WorkerPoolConfigKeys.RESULT_QUEUE_NAME)

    @cached_property
    def service_bus_namespace(self) -> str:
        return self._get_config(WorkerPoolConfigKeys.SERVICE_BUS_NAMESPACE)

    @cached_property
    def worker_idle_timeout_seconds(self) -> int:
        return self._get_int_config(WorkerPoolConfigKeys.WORKER_IDLE_TIMEOUT_SECONDS)

    @cached_property
    def worker_visibility_timeout_seconds(self) -> int:
        return self._get_int_config(WorkerPoolConfigKeys.WORKER_VISIBILITY_TIMEOUT_SECONDS)

    @cached_property
    def min_workers(self) -> int:
        return self._get_int_config(WorkerPoolConfigKeys.MIN_WORKERS)

    @cached_property
    def max_workers(self) -> int:
        return self._get_int_config(WorkerPoolConfigKeys.MAX_WORKERS)

    @cached_property
    def tasks_per_worker(self) -> int:
        return self._get_int_config(WorkerPoolConfigKeys.TASKS_PER_WORKER, fallback=1)

    @cached_property
    def worker_health_check_interval_seconds(self) -> int:
        return self._get_int_config(WorkerPoolConfigKeys.WORKER_HEALTH_CHECK_INTERVAL_SECONDS)

    @cached_property
    def max_worker_start_attempts(self) -> int:
        return self._get_int_config(WorkerPoolConfigKeys.MAX_WORKER_START_ATTEMPTS, fallback=3)

    @cached_property
    def shared_state_init_module(self) -> str | None:
        value = self._get_config(WorkerPoolConfigKeys.SHARED_STATE_INIT_MODULE)
        return value if value else None

    @cached_property
    def shared_state_init_function(self) -> str:
        return self._get_config(WorkerPoolConfigKeys.SHARED_STATE_INIT_FUNCTION)

    @cached_property
    def shared_state_blob_uri(self) -> str | None:
        value = self._get_config(WorkerPoolConfigKeys.SHARED_STATE_BLOB_URI)
        return value if value else None

    # ---- Hooks ----

    @property
    def batch_hook(self) -> AzureBatchHook:
        """Get Azure Batch hook."""
        if self._batch_hook is None:
            self._batch_hook = AzureBatchHook(azure_batch_conn_id=self.azure_batch_conn_id)
        return self._batch_hook

    @property
    def service_bus_hook(self) -> MessageHook:
        """Get Azure Service Bus hook."""
        if self._service_bus_hook is None:
            self._service_bus_hook = MessageHook(
                azure_service_bus_conn_id=self.azure_service_bus_conn_id
            )
        return self._service_bus_hook

    # ---- Lifecycle Methods ----

    def start(self):
        """Called when the executor starts."""
        log.info("Starting Azure Batch Worker Pool Executor")

        # Validate required configuration
        if not self.pool_id:
            raise ValueError("pool_id must be configured")
        if not self.job_id:
            raise ValueError("job_id must be configured")
        if not self.task_queue_name:
            raise ValueError("task_queue_name must be configured")
        if not self.result_queue_name:
            raise ValueError("result_queue_name must be configured")

        # Create pool/job if they don't exist
        self._ensure_pool_exists()
        self._ensure_job_exists()

        # Start minimum workers if configured
        if self.min_workers > 0:
            log.info("Starting %d minimum workers", self.min_workers)
            for _ in range(self.min_workers):
                self._request_worker_start()

    def _ensure_pool_exists(self):
        """Create the Batch pool if it doesn't exist."""
        try:
            pool = self.batch_hook.connection.pool.get(self.pool_id)
            log.info("Pool %s exists with %d nodes", self.pool_id, pool.current_dedicated_nodes or 0)
        except batch_models.BatchErrorException as e:
            if e.error and e.error.code == "PoolNotFound":
                log.info("Pool %s not found, creating...", self.pool_id)
                self._create_default_pool()
            else:
                raise

    def _create_default_pool(self):
        """Create a default pool with configuration from airflow.cfg."""
        vm_size = self._get_config(WorkerPoolConfigKeys.POOL_VM_SIZE, fallback="Standard_D2_v3")
        vm_count = self._get_int_config(WorkerPoolConfigKeys.POOL_VM_COUNT, fallback=1)
        vm_publisher = self._get_config(WorkerPoolConfigKeys.POOL_VM_PUBLISHER, fallback="canonical")
        vm_offer = self._get_config(WorkerPoolConfigKeys.POOL_VM_OFFER, fallback="0001-com-ubuntu-server-jammy")
        vm_sku = self._get_config(WorkerPoolConfigKeys.POOL_VM_SKU, fallback="22_04-lts")
        vm_node_agent_sku_id = self._get_config(
            WorkerPoolConfigKeys.POOL_VM_NODE_AGENT_SKU_ID,
            fallback="batch.node.ubuntu 22.04"
        )

        pool = self.batch_hook.configure_pool(
            pool_id=self.pool_id,
            vm_size=vm_size,
            vm_publisher=vm_publisher,
            vm_offer=vm_offer,
            vm_sku=vm_sku,
            vm_node_agent_sku_id=vm_node_agent_sku_id,
            target_dedicated_nodes=vm_count,
        )

        self.batch_hook.create_pool(pool)
        log.info("Created pool %s", self.pool_id)

    def _ensure_job_exists(self):
        """Create the Batch job if it doesn't exist."""
        try:
            self.batch_hook.connection.job.get(self.job_id)
            log.info("Job %s exists", self.job_id)
        except batch_models.BatchErrorException as e:
            if e.error and e.error.code == "JobNotFound":
                log.info("Job %s not found, creating...", self.job_id)
                self._create_default_job()
            else:
                raise

    def _create_default_job(self):
        """Create a default job."""
        job = self.batch_hook.configure_job(job_id=self.job_id, pool_id=self.pool_id)
        self.batch_hook.create_job(job)
        log.info("Created job %s", self.job_id)

    def end(self):
        """Called when the executor shuts down."""
        log.info("Shutting down Azure Batch Worker Pool Executor")

        # Terminate active workers
        for worker in self.active_workers:
            try:
                log.info("Terminating worker task %s", worker.task_id)
                self.batch_hook.connection.task.terminate(self.job_id, worker.task_id)
            except Exception:
                log.exception("Failed to terminate worker %s", worker.task_id)

        self.active_workers.clear()

    def terminate(self):
        """Emergency stop."""
        log.info("Terminating Azure Batch Worker Pool Executor")
        self.end()

    # ---- Task Queue Methods ----

    def queue_workload(self, key: TaskInstanceKey, workload: All, executor_config: dict[str, Any] | None = None):
        """Queue a task workload for execution."""
        # Create message for Service Bus
        task_message = TaskQueueMessage(
            message_id=str(uuid.uuid4()),
            task_key=TaskInstanceKeySchema.from_task_instance_key(key),
            workload_json=workload.model_dump_json(),
            executor_config=executor_config or {},
            enqueued_at=timezone.utcnow(),
        )

        # Send to Service Bus task queue
        self._send_task_to_queue(task_message)

        # Track locally
        self.tasks_in_queue[str(key)] = task_message

        log.debug("Queued task %s", key)

    def _send_task_to_queue(self, task_message: TaskQueueMessage):
        """Send task message to Service Bus task queue."""
        self.service_bus_hook.send_message(
            queue_name=self.task_queue_name,
            messages=task_message.model_dump_json(),
            message_id=task_message.message_id,
        )

    def _process_workloads(self, workloads: list[tuple[TaskInstanceKey, All, dict[str, Any] | None]]):
        """Process a batch of workloads."""
        for key, workload, executor_config in workloads:
            self.queue_workload(key, workload, executor_config)

    # ---- Sync (Main Heartbeat) ----

    def sync(self):
        """
        Synchronize executor state.

        Called periodically by the scheduler to:
        1. Poll result queue for completed tasks
        2. Check worker health
        3. Scale workers based on queue depth
        4. Submit pending worker start requests
        """
        # Poll result queue
        self._poll_result_queue()

        # Check worker health periodically
        if self._should_check_health():
            self._check_worker_health()
            self._last_health_check = timezone.utcnow()

        # Scale workers
        self._scale_workers()

        # Process pending worker starts
        self._process_pending_worker_starts()

    def _poll_result_queue(self):
        """Poll Service Bus result queue for completed tasks."""
        try:
            while True:
                # Read one message at a time
                msg = self.service_bus_hook.read_message(
                    queue_name=self.result_queue_name,
                    max_wait_time=0.1,  # Short wait, non-blocking
                )

                if msg is None:
                    break

                # Parse result message
                try:
                    result = TaskResultMessage.model_validate_json(str(msg))
                    self._handle_task_result(result)
                except Exception:
                    log.exception("Failed to parse result message: %s", msg)

        except Exception:
            log.exception("Error polling result queue")

    def _handle_task_result(self, result: TaskResultMessage):
        """Handle a task completion result."""
        task_key = result.task_key.to_task_instance_key()
        key_str = str(task_key)

        # Remove from tracking
        self.tasks_in_queue.pop(key_str, None)

        # Update task state
        if result.state == "SUCCESS":
            self.success(task_key)
            log.info("Task %s succeeded (worker: %s)", task_key, result.info.worker_id)
        else:
            self.fail(task_key)
            log.error(
                "Task %s failed (worker: %s): %s",
                task_key,
                result.info.worker_id,
                result.info.error_message,
            )

    def _should_check_health(self) -> bool:
        """Check if it's time for health check."""
        elapsed = (timezone.utcnow() - self._last_health_check).total_seconds()
        return elapsed >= self.worker_health_check_interval_seconds

    def _check_worker_health(self):
        """Check health of active workers via Batch API."""
        if not self.active_workers:
            return

        task_ids = self.active_workers.get_all_task_ids()
        if not task_ids:
            return

        try:
            # List tasks in the job to get their states
            tasks = list(self.batch_hook.connection.task.list(self.job_id))

            # Build task_id -> state map
            task_states = {task.id: task.state for task in tasks}

            # Check each active worker
            for task_id in task_ids:
                state = task_states.get(task_id)
                if state == batch_models.TaskState.completed:
                    worker = self.active_workers.pop_by_task_id(task_id)
                    log.info("Worker %s (task %s) completed", worker.worker_id, task_id)
                elif state is None:
                    # Task no longer exists
                    worker = self.active_workers.pop_by_task_id(task_id)
                    log.warning("Worker %s (task %s) no longer exists", worker.worker_id, task_id)

        except Exception:
            log.exception("Error checking worker health")

    def _scale_workers(self):
        """Scale workers based on queue depth."""
        queue_depth = len(self.tasks_in_queue)
        current_workers = len(self.active_workers) + len(self.pending_worker_starts)

        # Calculate desired workers
        if self.max_workers > 0:
            max_needed = min(
                self.max_workers,
                (queue_depth + self.tasks_per_worker - 1) // self.tasks_per_worker,
            )
        else:
            max_needed = (queue_depth + self.tasks_per_worker - 1) // self.tasks_per_worker

        # Ensure minimum workers
        desired = max(self.min_workers, max_needed)

        # Request more workers if needed
        workers_to_start = desired - current_workers
        if workers_to_start > 0:
            log.debug(
                "Scaling workers: queue=%d, current=%d, desired=%d, starting=%d",
                queue_depth,
                current_workers,
                desired,
                workers_to_start,
            )
            for _ in range(workers_to_start):
                self._request_worker_start()

    def _request_worker_start(self):
        """Queue a request to start a new worker."""
        worker_id = str(uuid.uuid4())[:8]
        request = WorkerStartRequest(
            worker_id=worker_id, attempt_number=1, next_attempt_time=timezone.utcnow()
        )
        self.pending_worker_starts.append(request)

    def _process_pending_worker_starts(self):
        """Process pending worker start requests."""
        now = timezone.utcnow()
        remaining = []

        for request in self.pending_worker_starts:
            if request.next_attempt_time > now:
                remaining.append(request)
                continue

            # Try to start the worker
            try:
                task_id = self._start_worker(request.worker_id)
                self.active_workers.add_worker(
                    task_id=task_id, worker_id=request.worker_id, started_at=now
                )
                log.info("Started worker %s (task %s)", request.worker_id, task_id)
            except Exception:
                log.exception("Failed to start worker %s (attempt %d)", request.worker_id, request.attempt_number)

                # Retry with backoff
                if request.attempt_number < self.max_worker_start_attempts:
                    backoff = min(30, 2**request.attempt_number)
                    request.attempt_number += 1
                    request.next_attempt_time = now + timedelta(seconds=backoff)
                    remaining.append(request)
                else:
                    log.error(
                        "Giving up on starting worker %s after %d attempts",
                        request.worker_id,
                        request.attempt_number,
                    )

        self.pending_worker_starts = remaining

    def _start_worker(self, worker_id: str) -> str:
        """Start a new worker as a Batch task. Returns the task ID."""
        task_id = f"{self.worker_task_name_prefix}-{worker_id}"

        # Build command line for worker process
        command_parts = [
            "python",
            "-m",
            "airflow.providers.microsoft.azure.executors.batch.worker_pool_worker",
            "--worker-id",
            worker_id,
            "--task-queue-name",
            self.task_queue_name,
            "--result-queue-name",
            self.result_queue_name,
            "--idle-timeout",
            str(self.worker_idle_timeout_seconds),
            "--visibility-timeout",
            str(self.worker_visibility_timeout_seconds),
        ]

        if self.service_bus_namespace:
            command_parts.extend(["--service-bus-namespace", self.service_bus_namespace])

        if self.shared_state_init_module:
            command_parts.extend(["--init-module", self.shared_state_init_module])
            command_parts.extend(["--init-function", self.shared_state_init_function])

        command_line = " ".join(command_parts)

        # Build environment variables
        env_settings = []

        # Add Service Bus connection string if available (from Airflow connection)
        try:
            conn = self.service_bus_hook.get_connection(self.azure_service_bus_conn_id)
            if conn.schema:  # Connection string is stored in schema field
                env_settings.append(
                    batch_models.EnvironmentSetting(
                        name="AZURE_SERVICE_BUS_CONNECTION_STRING", value=conn.schema
                    )
                )
        except Exception:
            log.debug("No Service Bus connection string found, workers will use Managed Identity")

        # Add shared state blob URI if configured
        if self.shared_state_blob_uri:
            env_settings.append(
                batch_models.EnvironmentSetting(
                    name="AIRFLOW_SHARED_STATE_BLOB_URI", value=self.shared_state_blob_uri
                )
            )

        # Create task
        task = batch_models.TaskAddParameter(
            id=task_id,
            command_line=command_line,
            environment_settings=env_settings if env_settings else None,
        )

        self.batch_hook.connection.task.add(self.job_id, task)

        return task_id

    # ---- Adoption (for HA failover) ----

    def try_adopt_task_instances(self, tis: Sequence[TaskInstance]) -> Sequence[TaskInstance]:
        """
        Try to adopt task instances from a failed executor.

        For worker pool executor, tasks are in Service Bus queue and will be
        processed by workers. We just need to track them.
        """
        adopted = []
        for ti in tis:
            key = ti.key
            # We can't know if the task is truly in the queue or being processed,
            # so we conservatively adopt it
            self.tasks_in_queue[str(key)] = TaskQueueMessage(
                message_id="adopted",
                task_key=TaskInstanceKeySchema.from_task_instance_key(key),
                workload_json="",  # Not needed for tracking
                executor_config={},
                enqueued_at=timezone.utcnow(),
            )
            adopted.append(ti)
            log.info("Adopted task %s", key)

        return adopted

    # ---- Legacy methods (for compatibility) ----

    def queue_command(
        self,
        task_instance: TaskInstance,
        command: CommandType,
        priority: int = 1,
        queue: str | None = None,
    ):
        """Legacy method - not used by worker pool executor."""
        raise NotImplementedError("Worker pool executor uses queue_workload instead of queue_command")

    def execute_async(
        self,
        key: TaskInstanceKey,
        command: CommandType,
        queue: str | None = None,
        executor_config: Any | None = None,
    ):
        """Legacy method - not used by worker pool executor."""
        raise NotImplementedError("Worker pool executor uses queue_workload instead of execute_async")
