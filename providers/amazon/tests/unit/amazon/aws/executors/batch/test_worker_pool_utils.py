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
from __future__ import annotations

from datetime import datetime, timezone

import pytest

from airflow.models.taskinstancekey import TaskInstanceKey
from airflow.providers.amazon.aws.executors.batch.worker_pool_schemas import (
    TaskInstanceKeySchema,
    TaskQueueMessage,
    TaskResultInfo,
    TaskResultMessage,
)
from airflow.providers.amazon.aws.executors.batch.worker_pool_utils import (
    WorkerCollection,
    WorkerInfo,
    WorkerStartRequest,
)


class TestWorkerCollection:
    """Tests WorkerCollection Class."""

    @pytest.fixture(autouse=True)
    def setup_method(self):
        """Create a WorkerCollection and add workers."""
        self.collection = WorkerCollection()
        self.now = datetime.now(timezone.utc)

        # Add first worker
        self.first_job_id = "job-001"
        self.first_worker_id = "worker-001"
        self.collection.add_worker(
            job_id=self.first_job_id,
            worker_id=self.first_worker_id,
            started_at=self.now,
        )

        # Add second worker
        self.second_job_id = "job-002"
        self.second_worker_id = "worker-002"
        self.collection.add_worker(
            job_id=self.second_job_id,
            worker_id=self.second_worker_id,
            started_at=self.now,
        )

    def test_add_worker(self):
        """Test adding workers to collection."""
        assert len(self.collection) == 2
        assert self.collection.has_job_id(self.first_job_id)
        assert self.collection.has_job_id(self.second_job_id)

    def test_get_all_job_ids(self):
        """Test getting all job IDs."""
        job_ids = self.collection.get_all_job_ids()
        assert self.first_job_id in job_ids
        assert self.second_job_id in job_ids

    def test_get_by_job_id(self):
        """Test getting worker info by job ID."""
        info = self.collection.get_by_job_id(self.first_job_id)
        assert info is not None
        assert info.worker_id == self.first_worker_id
        assert info.job_id == self.first_job_id

    def test_pop_by_job_id(self):
        """Test removing worker by job ID."""
        info = self.collection.pop_by_job_id(self.first_job_id)
        assert info.worker_id == self.first_worker_id
        assert len(self.collection) == 1
        assert not self.collection.has_job_id(self.first_job_id)

    def test_clear(self):
        """Test clearing all workers."""
        self.collection.clear()
        assert len(self.collection) == 0
        assert not self.collection.has_job_id(self.first_job_id)
        assert not self.collection.has_job_id(self.second_job_id)

    def test_bool(self):
        """Test boolean conversion."""
        assert bool(self.collection) is True
        self.collection.clear()
        assert bool(self.collection) is False

    def test_iteration(self):
        """Test iterating over workers."""
        workers = list(self.collection)
        assert len(workers) == 2
        worker_ids = [w.worker_id for w in workers]
        assert self.first_worker_id in worker_ids
        assert self.second_worker_id in worker_ids


class TestWorkerInfo:
    """Tests WorkerInfo dataclass."""

    def test_worker_info_creation(self):
        """Test creating WorkerInfo."""
        now = datetime.now(timezone.utc)
        info = WorkerInfo(
            worker_id="worker-001",
            job_id="job-001",
            started_at=now,
        )
        assert info.worker_id == "worker-001"
        assert info.job_id == "job-001"
        assert info.started_at == now
        assert info.tasks_assigned == 0
        assert info.last_seen is None

    def test_worker_info_with_optional_fields(self):
        """Test WorkerInfo with optional fields."""
        now = datetime.now(timezone.utc)
        info = WorkerInfo(
            worker_id="worker-001",
            job_id="job-001",
            started_at=now,
            tasks_assigned=5,
            last_seen=now,
        )
        assert info.tasks_assigned == 5
        assert info.last_seen == now


class TestWorkerStartRequest:
    """Tests WorkerStartRequest dataclass."""

    def test_worker_start_request_creation(self):
        """Test creating WorkerStartRequest."""
        now = datetime.now(timezone.utc)
        request = WorkerStartRequest(
            worker_id="worker-001",
            attempt_number=1,
            next_attempt_time=now,
        )
        assert request.worker_id == "worker-001"
        assert request.attempt_number == 1
        assert request.next_attempt_time == now


class TestTaskInstanceKeySchema:
    """Tests TaskInstanceKeySchema."""

    def test_from_task_instance_key(self):
        """Test creating schema from TaskInstanceKey."""
        key = TaskInstanceKey(
            dag_id="test_dag",
            task_id="test_task",
            run_id="test_run",
            try_number=2,
            map_index=3,
        )
        schema = TaskInstanceKeySchema.from_task_instance_key(key)
        assert schema.dag_id == "test_dag"
        assert schema.task_id == "test_task"
        assert schema.run_id == "test_run"
        assert schema.try_number == 2
        assert schema.map_index == 3

    def test_to_task_instance_key(self):
        """Test converting schema to TaskInstanceKey."""
        schema = TaskInstanceKeySchema(
            dag_id="test_dag",
            task_id="test_task",
            run_id="test_run",
            try_number=2,
            map_index=3,
        )
        key = schema.to_task_instance_key()
        assert isinstance(key, TaskInstanceKey)
        assert key.dag_id == "test_dag"
        assert key.task_id == "test_task"
        assert key.run_id == "test_run"
        assert key.try_number == 2
        assert key.map_index == 3

    def test_str_representation(self):
        """Test string representation."""
        schema = TaskInstanceKeySchema(
            dag_id="test_dag",
            task_id="test_task",
            run_id="test_run",
            try_number=1,
            map_index=-1,
        )
        assert str(schema) == "test_dag.test_task[test_run]#1"


class TestTaskQueueMessage:
    """Tests TaskQueueMessage schema."""

    def test_serialization_roundtrip(self):
        """Test JSON serialization and deserialization."""
        now = datetime.now(timezone.utc)
        message = TaskQueueMessage(
            message_id="msg-001",
            task_key=TaskInstanceKeySchema(
                dag_id="test_dag",
                task_id="test_task",
                run_id="test_run",
            ),
            workload_json='{"type": "ExecuteTask"}',
            executor_config={"key": "value"},
            enqueued_at=now,
        )

        json_str = message.model_dump_json()
        restored = TaskQueueMessage.model_validate_json(json_str)

        assert restored.message_id == message.message_id
        assert restored.task_key.dag_id == message.task_key.dag_id
        assert restored.workload_json == message.workload_json
        assert restored.executor_config == message.executor_config


class TestTaskResultMessage:
    """Tests TaskResultMessage schema."""

    def test_success_message(self):
        """Test creating a success result message."""
        now = datetime.now(timezone.utc)
        result = TaskResultMessage(
            message_id="msg-001",
            task_key=TaskInstanceKeySchema(
                dag_id="test_dag",
                task_id="test_task",
                run_id="test_run",
            ),
            state="SUCCESS",
            info=TaskResultInfo(
                worker_id="worker-001",
                batch_job_id="job-001",
                execution_time_seconds=10.5,
            ),
            completed_at=now,
        )
        assert result.state == "SUCCESS"
        assert result.info.error_message is None

    def test_failure_message(self):
        """Test creating a failure result message."""
        now = datetime.now(timezone.utc)
        result = TaskResultMessage(
            message_id="msg-001",
            task_key=TaskInstanceKeySchema(
                dag_id="test_dag",
                task_id="test_task",
                run_id="test_run",
            ),
            state="FAILED",
            info=TaskResultInfo(
                worker_id="worker-001",
                batch_job_id="job-001",
                execution_time_seconds=5.0,
                error_message="Task failed with error",
            ),
            completed_at=now,
        )
        assert result.state == "FAILED"
        assert result.info.error_message == "Task failed with error"

    def test_serialization_roundtrip(self):
        """Test JSON serialization and deserialization."""
        now = datetime.now(timezone.utc)
        result = TaskResultMessage(
            message_id="msg-001",
            task_key=TaskInstanceKeySchema(
                dag_id="test_dag",
                task_id="test_task",
                run_id="test_run",
            ),
            state="SUCCESS",
            info=TaskResultInfo(
                worker_id="worker-001",
                batch_job_id="job-001",
                execution_time_seconds=10.5,
            ),
            completed_at=now,
        )

        json_str = result.model_dump_json()
        restored = TaskResultMessage.model_validate_json(json_str)

        assert restored.message_id == result.message_id
        assert restored.state == result.state
        assert restored.info.worker_id == result.info.worker_id
