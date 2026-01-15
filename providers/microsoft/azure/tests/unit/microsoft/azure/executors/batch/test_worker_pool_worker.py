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

import os
from datetime import datetime
from unittest import mock
from unittest.mock import MagicMock, patch

import pytest

from airflow.providers.microsoft.azure.executors.batch.worker_pool_schemas import (
    TaskInstanceKeySchema,
    TaskQueueMessage,
)
from airflow.providers.microsoft.azure.executors.batch.worker_pool_worker import WorkerProcess


class TestWorkerProcess:
    """Tests for the worker process."""

    @pytest.fixture
    def worker(self):
        """Create a worker instance with mocked Service Bus client."""
        with patch.dict(os.environ, {"AZURE_SERVICE_BUS_CONNECTION_STRING": "test-conn-str"}):
            worker = WorkerProcess(
                worker_id="test-worker",
                task_queue_name="test-task-queue",
                result_queue_name="test-result-queue",
                idle_timeout_seconds=300,
                visibility_timeout_seconds=3600,
            )
            # Mock the Service Bus client
            worker._service_bus_client = MagicMock()
            return worker

    def test_worker_initialization(self, worker):
        """Test worker initializes with correct values."""
        assert worker.worker_id == "test-worker"
        assert worker.task_queue_name == "test-task-queue"
        assert worker.result_queue_name == "test-result-queue"
        assert worker.idle_timeout_seconds == 300
        assert worker.running is True
        assert worker.tasks_executed == 0

    def test_get_service_bus_client_with_connection_string(self):
        """Test Service Bus client creation with connection string."""
        with patch.dict(os.environ, {"AZURE_SERVICE_BUS_CONNECTION_STRING": "test-conn-str"}):
            with patch(
                "airflow.providers.microsoft.azure.executors.batch."
                "worker_pool_worker.ServiceBusClient"
            ) as mock_client:
                worker = WorkerProcess(
                    worker_id="test-worker",
                    task_queue_name="test-task-queue",
                    result_queue_name="test-result-queue",
                )
                _ = worker.service_bus_client

                mock_client.from_connection_string.assert_called_once_with(
                    "test-conn-str", logging_enable=True
                )

    def test_get_service_bus_client_with_managed_identity(self):
        """Test Service Bus client creation with managed identity."""
        # Clear connection string env var
        env = os.environ.copy()
        env.pop("AZURE_SERVICE_BUS_CONNECTION_STRING", None)

        with patch.dict(os.environ, env, clear=True):
            with patch(
                "airflow.providers.microsoft.azure.executors.batch."
                "worker_pool_worker.ServiceBusClient"
            ) as mock_client, patch(
                "azure.identity.DefaultAzureCredential"
            ) as mock_credential:
                worker = WorkerProcess(
                    worker_id="test-worker",
                    task_queue_name="test-task-queue",
                    result_queue_name="test-result-queue",
                    service_bus_namespace="test-namespace.servicebus.windows.net",
                )
                _ = worker.service_bus_client

                mock_credential.assert_called_once()
                mock_client.assert_called_once()

    def test_should_terminate_on_idle(self, worker):
        """Test idle timeout detection."""
        from datetime import timedelta

        from airflow.providers.microsoft.azure.executors.batch.worker_pool_worker import timezone

        # Just started - should not terminate
        worker.last_task_time = timezone.utcnow()
        assert worker._should_terminate_on_idle() is False

        # Idle for longer than timeout - should terminate
        worker.last_task_time = timezone.utcnow() - timedelta(seconds=400)
        assert worker._should_terminate_on_idle() is True

    def test_poll_for_task_returns_none_when_empty(self, worker):
        """Test polling returns None when queue is empty."""
        # Mock receiver to return empty list
        mock_receiver = MagicMock()
        mock_receiver.__enter__ = MagicMock(return_value=mock_receiver)
        mock_receiver.__exit__ = MagicMock(return_value=False)
        mock_receiver.receive_messages.return_value = []

        worker.service_bus_client.get_queue_receiver.return_value = mock_receiver

        result = worker._poll_for_task()
        assert result is None

    def test_poll_for_task_returns_message(self, worker):
        """Test polling returns parsed message when available."""
        # Create a test message
        task_message = TaskQueueMessage(
            message_id="msg-123",
            task_key=TaskInstanceKeySchema(
                dag_id="test_dag",
                task_id="test_task",
                run_id="test_run",
                try_number=1,
                map_index=-1,
            ),
            workload_json='{"test": "data"}',
            executor_config={},
            enqueued_at=datetime.utcnow(),
        )

        # Mock Service Bus message
        mock_sb_message = MagicMock()
        mock_sb_message.__str__ = MagicMock(return_value=task_message.model_dump_json())

        # Mock receiver
        mock_receiver = MagicMock()
        mock_receiver.__enter__ = MagicMock(return_value=mock_receiver)
        mock_receiver.__exit__ = MagicMock(return_value=False)
        mock_receiver.receive_messages.return_value = [mock_sb_message]

        worker.service_bus_client.get_queue_receiver.return_value = mock_receiver

        result = worker._poll_for_task()

        assert result is not None
        assert result.message_id == "msg-123"
        assert result.task_key.dag_id == "test_dag"

    def test_report_result(self, worker):
        """Test result reporting to Service Bus."""
        task_key = TaskInstanceKeySchema(
            dag_id="test_dag",
            task_id="test_task",
            run_id="test_run",
            try_number=1,
            map_index=-1,
        )

        # Mock sender
        mock_sender = MagicMock()
        mock_sender.__enter__ = MagicMock(return_value=mock_sender)
        mock_sender.__exit__ = MagicMock(return_value=False)

        worker.service_bus_client.get_queue_sender.return_value = mock_sender

        worker._report_result(
            task_key=task_key,
            state="SUCCESS",
            execution_time=5.5,
            error_message=None,
        )

        # Verify message was sent
        mock_sender.send_messages.assert_called_once()

    def test_sigterm_handler(self, worker):
        """Test SIGTERM handler sets running to False."""
        assert worker.running is True
        worker._handle_sigterm(15, None)  # SIGTERM signal
        assert worker.running is False


class TestBlobStorageDownload:
    """Tests for Blob Storage shared state download."""

    @pytest.fixture
    def worker(self):
        """Create a worker instance."""
        with patch.dict(os.environ, {"AZURE_SERVICE_BUS_CONNECTION_STRING": "test-conn-str"}):
            worker = WorkerProcess(
                worker_id="test-worker",
                task_queue_name="test-task-queue",
                result_queue_name="test-result-queue",
            )
            worker._service_bus_client = MagicMock()
            return worker

    def test_download_skipped_when_no_uri(self, worker):
        """Test blob download is skipped when no URI configured."""
        with patch.dict(os.environ, {}, clear=True):
            with patch(
                "airflow.providers.microsoft.azure.executors.batch."
                "worker_pool_worker.log"
            ) as mock_log:
                worker._download_shared_state_from_blob()
                mock_log.info.assert_called()

    def test_download_with_connection_string(self, worker):
        """Test blob download with connection string auth."""
        env_vars = {
            "AIRFLOW_SHARED_STATE_BLOB_URI": "https://account.blob.core.windows.net/container/path",
            "AZURE_STORAGE_CONNECTION_STRING": "test-storage-conn-str",
        }

        with patch.dict(os.environ, env_vars):
            with patch(
                "airflow.providers.microsoft.azure.executors.batch."
                "worker_pool_worker.Path"
            ) as mock_path:
                mock_path_instance = MagicMock()
                mock_path.return_value = mock_path_instance

                with patch(
                    "azure.storage.blob.BlobServiceClient"
                ) as mock_blob_client:
                    mock_container = MagicMock()
                    mock_container.list_blobs.return_value = []
                    mock_blob_client.from_connection_string.return_value.get_container_client.return_value = (
                        mock_container
                    )

                    worker._download_shared_state_from_blob()

                    mock_blob_client.from_connection_string.assert_called_once_with(
                        "test-storage-conn-str"
                    )


class TestSharedStateInitialization:
    """Tests for shared state initialization."""

    @pytest.fixture
    def worker(self):
        """Create a worker instance with init module configured."""
        with patch.dict(os.environ, {"AZURE_SERVICE_BUS_CONNECTION_STRING": "test-conn-str"}):
            worker = WorkerProcess(
                worker_id="test-worker",
                task_queue_name="test-task-queue",
                result_queue_name="test-result-queue",
                init_module="test.init_module",
                init_function="initialize",
            )
            worker._service_bus_client = MagicMock()
            return worker

    def test_shared_state_initialization(self, worker):
        """Test shared state initialization from module."""
        mock_module = MagicMock()
        mock_module.initialize.return_value = {"model": "test_model"}

        with patch("importlib.import_module", return_value=mock_module):
            worker._initialize_shared_state()

        mock_module.initialize.assert_called_once()

    def test_shared_state_initialization_handles_error(self, worker):
        """Test shared state initialization handles errors gracefully."""
        with patch("importlib.import_module", side_effect=ImportError("Module not found")):
            # Should not raise, just log error
            worker._initialize_shared_state()
