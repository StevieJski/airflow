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

from unittest import mock
from unittest.mock import MagicMock, patch

import pytest
from azure.batch import models as batch_models

from airflow.providers.microsoft.azure.executors.batch.batch_worker_pool_executor import (
    AzureBatchWorkerPoolExecutor,
)
from airflow.providers.microsoft.azure.executors.batch.worker_pool_schemas import (
    TaskInstanceKeySchema,
    TaskQueueMessage,
    TaskResultInfo,
    TaskResultMessage,
)
from airflow.providers.microsoft.azure.executors.batch.worker_pool_utils import WorkerCollection


class TestWorkerCollection:
    """Tests for WorkerCollection utility class."""

    def test_add_and_get_worker(self):
        """Test adding and retrieving workers."""
        from datetime import datetime

        collection = WorkerCollection()
        now = datetime.utcnow()

        collection.add_worker(task_id="task-1", worker_id="worker-1", started_at=now)

        assert len(collection) == 1
        assert collection.has_task_id("task-1")

        worker = collection.get_by_task_id("task-1")
        assert worker is not None
        assert worker.worker_id == "worker-1"
        assert worker.task_id == "task-1"

    def test_pop_worker(self):
        """Test removing workers."""
        from datetime import datetime

        collection = WorkerCollection()
        now = datetime.utcnow()

        collection.add_worker(task_id="task-1", worker_id="worker-1", started_at=now)
        assert len(collection) == 1

        worker = collection.pop_by_task_id("task-1")
        assert worker.worker_id == "worker-1"
        assert len(collection) == 0
        assert not collection.has_task_id("task-1")

    def test_clear_collection(self):
        """Test clearing all workers."""
        from datetime import datetime

        collection = WorkerCollection()
        now = datetime.utcnow()

        collection.add_worker(task_id="task-1", worker_id="worker-1", started_at=now)
        collection.add_worker(task_id="task-2", worker_id="worker-2", started_at=now)
        assert len(collection) == 2

        collection.clear()
        assert len(collection) == 0


class TestTaskSchemas:
    """Tests for message schemas."""

    def test_task_instance_key_schema(self):
        """Test TaskInstanceKeySchema serialization."""
        key_schema = TaskInstanceKeySchema(
            dag_id="test_dag",
            task_id="test_task",
            run_id="test_run",
            try_number=1,
            map_index=-1,
        )

        # Test string representation
        assert str(key_schema) == "test_dag.test_task[test_run]#1"

        # Test to_task_instance_key
        with mock.patch("airflow.models.taskinstancekey.TaskInstanceKey") as MockKey:
            key_schema.to_task_instance_key()
            MockKey.assert_called_once_with(
                dag_id="test_dag",
                task_id="test_task",
                run_id="test_run",
                try_number=1,
                map_index=-1,
            )

    def test_task_queue_message_serialization(self):
        """Test TaskQueueMessage JSON serialization."""
        from datetime import datetime

        message = TaskQueueMessage(
            message_id="msg-123",
            task_key=TaskInstanceKeySchema(
                dag_id="test_dag",
                task_id="test_task",
                run_id="test_run",
                try_number=1,
                map_index=-1,
            ),
            workload_json='{"test": "data"}',
            executor_config={"key": "value"},
            enqueued_at=datetime(2024, 1, 1, 12, 0, 0),
        )

        json_str = message.model_dump_json()
        parsed = TaskQueueMessage.model_validate_json(json_str)

        assert parsed.message_id == "msg-123"
        assert parsed.task_key.dag_id == "test_dag"
        assert parsed.workload_json == '{"test": "data"}'

    def test_task_result_message_serialization(self):
        """Test TaskResultMessage JSON serialization."""
        from datetime import datetime

        result = TaskResultMessage(
            message_id="result-123",
            task_key=TaskInstanceKeySchema(
                dag_id="test_dag",
                task_id="test_task",
                run_id="test_run",
                try_number=1,
                map_index=-1,
            ),
            state="SUCCESS",
            info=TaskResultInfo(
                worker_id="worker-1",
                batch_task_id="batch-task-1",
                execution_time_seconds=5.5,
                error_message=None,
            ),
            completed_at=datetime(2024, 1, 1, 12, 5, 0),
        )

        json_str = result.model_dump_json()
        parsed = TaskResultMessage.model_validate_json(json_str)

        assert parsed.state == "SUCCESS"
        assert parsed.info.worker_id == "worker-1"
        assert parsed.info.execution_time_seconds == 5.5


class TestAzureBatchWorkerPoolExecutor:
    """Tests for the Azure Batch Worker Pool Executor."""

    @pytest.fixture
    def mock_config(self):
        """Mock configuration values."""
        config_values = {
            "azure_batch_conn_id": "test_batch_conn",
            "azure_service_bus_conn_id": "test_sb_conn",
            "pool_id": "test-pool",
            "job_id": "test-job",
            "task_queue_name": "test-task-queue",
            "result_queue_name": "test-result-queue",
            "service_bus_namespace": "test-namespace",
            "worker_idle_timeout_seconds": "300",
            "min_workers": "0",
            "max_workers": "5",
            "tasks_per_worker": "1",
            "worker_health_check_interval_seconds": "60",
            "max_worker_start_attempts": "3",
        }

        with patch(
            "airflow.providers.microsoft.azure.executors.batch."
            "batch_worker_pool_executor.conf.get"
        ) as mock_get, patch(
            "airflow.providers.microsoft.azure.executors.batch."
            "batch_worker_pool_executor.conf.getint"
        ) as mock_getint, patch(
            "airflow.providers.microsoft.azure.executors.batch."
            "batch_worker_pool_executor.conf.getboolean"
        ) as mock_getbool:

            def get_side_effect(section, key, fallback=""):
                return config_values.get(key, fallback)

            def getint_side_effect(section, key, fallback=0):
                value = config_values.get(key)
                return int(value) if value else fallback

            mock_get.side_effect = get_side_effect
            mock_getint.side_effect = getint_side_effect
            mock_getbool.return_value = True

            yield

    @pytest.fixture
    def executor(self, mock_config):
        """Create executor instance with mocked hooks."""
        executor = AzureBatchWorkerPoolExecutor()

        # Mock hooks
        executor._batch_hook = MagicMock()
        executor._service_bus_hook = MagicMock()

        return executor

    def test_executor_initialization(self, executor):
        """Test executor initializes with empty state."""
        assert len(executor.active_workers) == 0
        assert len(executor.pending_worker_starts) == 0
        assert len(executor.tasks_in_queue) == 0

    def test_start_ensures_pool_and_job_exist(self, executor):
        """Test start() creates pool and job if they don't exist."""
        # Mock pool.get to raise not found error
        error = batch_models.BatchErrorException(MagicMock(), MagicMock())
        error.error = MagicMock()
        error.error.code = "PoolNotFound"
        executor.batch_hook.connection.pool.get.side_effect = error

        # Mock job.get to succeed
        executor.batch_hook.connection.job.get.return_value = MagicMock()

        # Mock pool creation
        executor.batch_hook.configure_pool.return_value = MagicMock()
        executor.batch_hook.create_pool.return_value = None

        executor.start()

        # Verify pool was created
        executor.batch_hook.configure_pool.assert_called_once()
        executor.batch_hook.create_pool.assert_called_once()

    def test_scale_workers(self, executor):
        """Test worker scaling based on queue depth."""
        # Add tasks to queue
        executor.tasks_in_queue = {f"key-{i}": MagicMock() for i in range(3)}

        # Run scaling logic
        executor._scale_workers()

        # Should have pending worker starts
        assert len(executor.pending_worker_starts) >= 1

    def test_handle_task_result_success(self, executor):
        """Test handling successful task result."""
        from datetime import datetime

        result = TaskResultMessage(
            message_id="result-123",
            task_key=TaskInstanceKeySchema(
                dag_id="test_dag",
                task_id="test_task",
                run_id="test_run",
                try_number=1,
                map_index=-1,
            ),
            state="SUCCESS",
            info=TaskResultInfo(
                worker_id="worker-1",
                batch_task_id="batch-task-1",
                execution_time_seconds=5.5,
                error_message=None,
            ),
            completed_at=datetime.utcnow(),
        )

        # Add task to tracking
        task_key = result.task_key.to_task_instance_key()
        executor.tasks_in_queue[str(task_key)] = MagicMock()

        with patch.object(executor, "success") as mock_success:
            executor._handle_task_result(result)
            mock_success.assert_called_once()

        # Task should be removed from tracking
        assert str(task_key) not in executor.tasks_in_queue

    def test_handle_task_result_failure(self, executor):
        """Test handling failed task result."""
        from datetime import datetime

        result = TaskResultMessage(
            message_id="result-123",
            task_key=TaskInstanceKeySchema(
                dag_id="test_dag",
                task_id="test_task",
                run_id="test_run",
                try_number=1,
                map_index=-1,
            ),
            state="FAILED",
            info=TaskResultInfo(
                worker_id="worker-1",
                batch_task_id="batch-task-1",
                execution_time_seconds=5.5,
                error_message="Test error",
            ),
            completed_at=datetime.utcnow(),
        )

        # Add task to tracking
        task_key = result.task_key.to_task_instance_key()
        executor.tasks_in_queue[str(task_key)] = MagicMock()

        with patch.object(executor, "fail") as mock_fail:
            executor._handle_task_result(result)
            mock_fail.assert_called_once()

    def test_worker_health_check(self, executor):
        """Test worker health check removes completed workers."""
        from datetime import datetime

        # Add active worker
        executor.active_workers.add_worker(
            task_id="task-1", worker_id="worker-1", started_at=datetime.utcnow()
        )

        # Mock Batch API to return completed task
        mock_task = MagicMock()
        mock_task.id = "task-1"
        mock_task.state = batch_models.TaskState.completed
        executor.batch_hook.connection.task.list.return_value = [mock_task]

        executor._check_worker_health()

        # Worker should be removed
        assert len(executor.active_workers) == 0

    def test_end_terminates_workers(self, executor):
        """Test end() terminates all active workers."""
        from datetime import datetime

        # Add active workers
        executor.active_workers.add_worker(
            task_id="task-1", worker_id="worker-1", started_at=datetime.utcnow()
        )
        executor.active_workers.add_worker(
            task_id="task-2", worker_id="worker-2", started_at=datetime.utcnow()
        )

        executor.end()

        # Should have called terminate for each worker
        assert executor.batch_hook.connection.task.terminate.call_count == 2
        assert len(executor.active_workers) == 0
