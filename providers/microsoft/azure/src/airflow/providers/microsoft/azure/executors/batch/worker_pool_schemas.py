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
"""Pydantic schemas for Service Bus messages used by Azure Batch Worker Pool Executor."""

from __future__ import annotations

from datetime import datetime
from typing import Any, Literal

from pydantic import BaseModel, Field


class TaskInstanceKeySchema(BaseModel):
    """Schema for TaskInstanceKey to enable serialization."""

    dag_id: str
    task_id: str
    run_id: str
    try_number: int = 1
    map_index: int = -1

    def to_task_instance_key(self):
        """Convert to a TaskInstanceKey NamedTuple."""
        from airflow.models.taskinstancekey import TaskInstanceKey

        return TaskInstanceKey(
            dag_id=self.dag_id,
            task_id=self.task_id,
            run_id=self.run_id,
            try_number=self.try_number,
            map_index=self.map_index,
        )

    @classmethod
    def from_task_instance_key(cls, key) -> TaskInstanceKeySchema:
        """Create from a TaskInstanceKey NamedTuple."""
        return cls(
            dag_id=key.dag_id,
            task_id=key.task_id,
            run_id=key.run_id,
            try_number=key.try_number,
            map_index=key.map_index,
        )

    def __str__(self):
        return f"{self.dag_id}.{self.task_id}[{self.run_id}]#{self.try_number}"


class TaskQueueMessage(BaseModel):
    """Message sent to Service Bus task queue for workers to process."""

    message_id: str
    task_key: TaskInstanceKeySchema
    workload_json: str = Field(description="JSON-serialized ExecuteTask workload")
    executor_config: dict[str, Any] = Field(default_factory=dict)
    enqueued_at: datetime


class TaskResultInfo(BaseModel):
    """Metadata about task execution result."""

    worker_id: str
    batch_task_id: str
    execution_time_seconds: float
    error_message: str | None = None


class TaskResultMessage(BaseModel):
    """Message sent to Service Bus result queue by workers."""

    message_id: str
    task_key: TaskInstanceKeySchema
    state: Literal["SUCCESS", "FAILED"]
    info: TaskResultInfo
    completed_at: datetime
