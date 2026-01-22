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
"""Utility classes and configuration for AWS Batch Worker Pool Executor."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import TYPE_CHECKING

from airflow.providers.amazon.aws.executors.utils.base_config_keys import BaseConfigKeys

if TYPE_CHECKING:
    pass

# Config group name for the worker pool executor
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
    "use_native_worker": "False",
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

    # Native Worker (e.g., .NET worker that has its own entrypoint)
    USE_NATIVE_WORKER = "use_native_worker"

    # Airflow Execution API URL (required for native workers to transition task state)
    EXECUTION_API_URL = "execution_api_url"


@dataclass
class WorkerInfo:
    """Information about an active worker."""

    worker_id: str
    job_id: str
    started_at: datetime
    tasks_assigned: int = 0
    last_seen: datetime | None = None
    # For array job children, this is the parent job ID
    parent_job_id: str | None = None
    # For array job children, this is the array index
    array_index: int | None = None


@dataclass
class WorkerStartRequest:
    """Request to start new worker(s).

    If batch_size > 1, will be submitted as an array job.
    """

    worker_id: str  # Base worker ID (array index appended for array jobs)
    attempt_number: int
    next_attempt_time: datetime
    batch_size: int = 1  # Number of workers to start (1 = individual, >1 = array job)


@dataclass
class ArrayJobInfo:
    """Information about an array job (parent)."""

    parent_job_id: str
    base_worker_id: str
    array_size: int
    child_job_ids: list[str]
    started_at: datetime


class WorkerCollection:
    """Collection to track active workers."""

    def __init__(self):
        self.job_id_to_worker: dict[str, WorkerInfo] = {}
        self.worker_id_to_job_id: dict[str, str] = {}
        # Track array jobs: parent_job_id -> ArrayJobInfo
        self.array_jobs: dict[str, ArrayJobInfo] = {}

    def add_worker(self, job_id: str, worker_id: str, started_at: datetime):
        """Add a worker to the collection."""
        info = WorkerInfo(worker_id=worker_id, job_id=job_id, started_at=started_at)
        self.job_id_to_worker[job_id] = info
        self.worker_id_to_job_id[worker_id] = job_id

    def add_array_job(
        self, parent_job_id: str, base_worker_id: str, array_size: int, started_at: datetime
    ):
        """Add an array job and all its child workers to the collection.

        For array jobs, child job IDs are formatted as "parent_job_id:index".
        """
        child_job_ids = [f"{parent_job_id}:{i}" for i in range(array_size)]

        # Store array job info
        self.array_jobs[parent_job_id] = ArrayJobInfo(
            parent_job_id=parent_job_id,
            base_worker_id=base_worker_id,
            array_size=array_size,
            child_job_ids=child_job_ids,
            started_at=started_at,
        )

        # Add each child as a worker
        for i, child_job_id in enumerate(child_job_ids):
            worker_id = f"{base_worker_id}-{i}"
            info = WorkerInfo(
                worker_id=worker_id,
                job_id=child_job_id,
                started_at=started_at,
                parent_job_id=parent_job_id,
                array_index=i,
            )
            self.job_id_to_worker[child_job_id] = info
            self.worker_id_to_job_id[worker_id] = child_job_id

    def pop_by_job_id(self, job_id: str) -> WorkerInfo:
        """Remove and return worker by job ID."""
        info = self.job_id_to_worker.pop(job_id)
        del self.worker_id_to_job_id[info.worker_id]
        return info

    def get_by_job_id(self, job_id: str) -> WorkerInfo | None:
        """Get worker info by job ID."""
        return self.job_id_to_worker.get(job_id)

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
        self.array_jobs.clear()

    def __len__(self):
        return len(self.job_id_to_worker)

    def __bool__(self):
        return bool(self.job_id_to_worker)

    def __iter__(self):
        return iter(self.job_id_to_worker.values())
