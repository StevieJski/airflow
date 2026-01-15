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
"""Utility classes and configuration for Azure Batch Worker Pool Executor."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    pass

# Config group name for the worker pool executor
CONFIG_GROUP_NAME = "azure_batch_worker_pool_executor"

# Default configuration values
CONFIG_DEFAULTS = {
    "azure_batch_conn_id": "azure_batch_default",
    "azure_service_bus_conn_id": "azure_service_bus_default",
    "worker_idle_timeout_seconds": "300",
    "worker_max_tasks": "0",
    "worker_visibility_timeout_seconds": "3600",
    "min_workers": "0",
    "max_workers": "0",
    "tasks_per_worker": "1",
    "check_health_on_startup": "True",
    "worker_health_check_interval_seconds": "60",
    "max_worker_start_attempts": "3",
    "worker_task_name_prefix": "airflow-worker",
    "shared_state_init_function": "initialize",
}


class WorkerPoolConfigKeys:
    """Configuration keys for Azure Batch Worker Pool Executor."""

    # Azure Connections
    AZURE_BATCH_CONN_ID = "azure_batch_conn_id"
    AZURE_SERVICE_BUS_CONN_ID = "azure_service_bus_conn_id"

    # Batch Resources
    POOL_ID = "pool_id"
    JOB_ID = "job_id"
    WORKER_TASK_NAME_PREFIX = "worker_task_name_prefix"

    # Pool Configuration (used if pool needs to be created)
    POOL_VM_SIZE = "pool_vm_size"
    POOL_VM_COUNT = "pool_vm_count"
    POOL_VM_PUBLISHER = "pool_vm_publisher"
    POOL_VM_OFFER = "pool_vm_offer"
    POOL_VM_SKU = "pool_vm_sku"
    POOL_VM_NODE_AGENT_SKU_ID = "pool_vm_node_agent_sku_id"

    # Service Bus Queues
    TASK_QUEUE_NAME = "task_queue_name"
    RESULT_QUEUE_NAME = "result_queue_name"
    SERVICE_BUS_NAMESPACE = "service_bus_namespace"

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
    SHARED_STATE_BLOB_URI = "shared_state_blob_uri"


@dataclass
class WorkerInfo:
    """Information about an active worker."""

    worker_id: str
    task_id: str  # Azure Batch task ID
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
        self.task_id_to_worker: dict[str, WorkerInfo] = {}
        self.worker_id_to_task_id: dict[str, str] = {}

    def add_worker(self, task_id: str, worker_id: str, started_at: datetime):
        """Add a worker to the collection."""
        info = WorkerInfo(worker_id=worker_id, task_id=task_id, started_at=started_at)
        self.task_id_to_worker[task_id] = info
        self.worker_id_to_task_id[worker_id] = task_id

    def pop_by_task_id(self, task_id: str) -> WorkerInfo:
        """Remove and return worker by task ID."""
        info = self.task_id_to_worker.pop(task_id)
        del self.worker_id_to_task_id[info.worker_id]
        return info

    def get_by_task_id(self, task_id: str) -> WorkerInfo | None:
        """Get worker info by task ID."""
        return self.task_id_to_worker.get(task_id)

    def get_all_task_ids(self) -> list[str]:
        """Get all active worker task IDs."""
        return list(self.task_id_to_worker.keys())

    def has_task_id(self, task_id: str) -> bool:
        """Check if task ID is in collection."""
        return task_id in self.task_id_to_worker

    def clear(self):
        """Clear all workers."""
        self.task_id_to_worker.clear()
        self.worker_id_to_task_id.clear()

    def __len__(self):
        return len(self.task_id_to_worker)

    def __bool__(self):
        return bool(self.task_id_to_worker)

    def __iter__(self):
        return iter(self.task_id_to_worker.values())
