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

import json
import logging
from datetime import datetime, timezone
from unittest import mock

import pytest
from botocore.exceptions import ClientError

from airflow.exceptions import AirflowException
from airflow.executors.base_executor import BaseExecutor
from airflow.models.taskinstancekey import TaskInstanceKey
from airflow.providers.amazon.aws.executors.batch.batch_worker_pool_executor import (
    AwsBatchWorkerPoolExecutor,
)
from airflow.providers.amazon.aws.executors.batch.worker_pool_schemas import (
    TaskInstanceKeySchema,
    TaskResultInfo,
    TaskResultMessage,
)
from airflow.providers.amazon.aws.executors.batch.worker_pool_utils import CONFIG_GROUP_NAME

from tests_common.test_utils.config import conf_vars
from tests_common.test_utils.version_compat import AIRFLOW_V_3_0_PLUS

MOCK_JOB_ID = "batch-job-id"


@pytest.fixture
def set_env_vars():
    overrides: dict[tuple[str, str], str] = {
        (CONFIG_GROUP_NAME, "region_name"): "us-east-1",
        (CONFIG_GROUP_NAME, "job_queue"): "test-job-queue",
        (CONFIG_GROUP_NAME, "job_definition"): "test-job-def",
        (CONFIG_GROUP_NAME, "task_queue_url"): "https://sqs.us-east-1.amazonaws.com/123456789/task-queue",
        (CONFIG_GROUP_NAME, "result_queue_url"): "https://sqs.us-east-1.amazonaws.com/123456789/result-queue",
        (CONFIG_GROUP_NAME, "check_health_on_startup"): "False",
        (CONFIG_GROUP_NAME, "min_workers"): "0",
        (CONFIG_GROUP_NAME, "max_workers"): "10",
    }
    with conf_vars(overrides):
        yield


@pytest.fixture
def mock_executor(set_env_vars) -> AwsBatchWorkerPoolExecutor:
    """Mock Worker Pool Executor to a repeatable starting state."""
    executor = AwsBatchWorkerPoolExecutor()
    executor.IS_BOTO_CONNECTION_HEALTHY = True

    # Replace boto3 Batch client with mock
    batch_mock = mock.Mock()
    batch_mock.submit_job.return_value = {"jobId": MOCK_JOB_ID, "jobName": "test-worker"}
    batch_mock.describe_jobs.return_value = {"jobs": []}
    executor._batch_client = batch_mock

    # Replace boto3 SQS client with mock
    sqs_mock = mock.Mock()
    sqs_mock.send_message.return_value = {}
    sqs_mock.receive_message.return_value = {"Messages": []}
    sqs_mock.get_queue_attributes.return_value = {
        "Attributes": {"ApproximateNumberOfMessages": "0", "ApproximateNumberOfMessagesNotVisible": "0"}
    }
    executor._sqs_client = sqs_mock

    return executor


class TestAwsBatchWorkerPoolExecutor:
    """Tests the AWS Batch Worker Pool Executor."""

    def test_init(self, mock_executor):
        """Test executor initialization."""
        assert mock_executor.job_queue == "test-job-queue"
        assert mock_executor.job_definition == "test-job-def"
        assert mock_executor.max_workers == 10
        assert len(mock_executor.active_workers) == 0

    def test_start_without_health_check(self, mock_executor):
        """Test executor start without health check."""
        mock_executor.start()
        # No workers should be started when min_workers=0
        assert len(mock_executor.active_workers) == 0

    def test_start_with_min_workers(self, set_env_vars):
        """Test executor start with minimum workers."""
        overrides = {(CONFIG_GROUP_NAME, "min_workers"): "2"}
        with conf_vars(overrides):
            executor = AwsBatchWorkerPoolExecutor()
            executor.IS_BOTO_CONNECTION_HEALTHY = True

            # Mock clients
            batch_mock = mock.Mock()
            batch_mock.submit_job.return_value = {"jobId": MOCK_JOB_ID}
            executor._batch_client = batch_mock

            sqs_mock = mock.Mock()
            executor._sqs_client = sqs_mock

            executor.start()

            # Should have requested 2 workers
            assert len(executor.pending_worker_starts) == 2

    def test_health_check_success(self, mock_executor):
        """Test successful health check."""
        mock_executor.IS_BOTO_CONNECTION_HEALTHY = False
        mock_executor._batch_client.describe_jobs.return_value = {}
        mock_executor._sqs_client.get_queue_attributes.return_value = {"Attributes": {}}

        mock_executor.check_health()
        assert mock_executor.IS_BOTO_CONNECTION_HEALTHY is True

    def test_health_check_failure(self, mock_executor):
        """Test failed health check."""
        mock_executor.IS_BOTO_CONNECTION_HEALTHY = False
        mock_resp = {"Error": {"Code": "AccessDeniedException", "Message": "Access denied"}}
        mock_executor._batch_client.describe_jobs.side_effect = ClientError(mock_resp, "DescribeJobs")

        with pytest.raises(AirflowException, match="Access denied"):
            mock_executor.check_health()

    @pytest.mark.skipif(not AIRFLOW_V_3_0_PLUS, reason="Test requires Airflow 3+")
    def test_queue_workload(self, mock_executor):
        """Test queueing a workload."""
        from airflow.executors.workloads import ExecuteTask

        workload = mock.Mock(spec=ExecuteTask)
        workload.ti = mock.Mock()
        workload.ti.key = TaskInstanceKey("test_dag", "test_task", "test_run", 1, -1)
        workload.ti.executor_config = {}

        mock_executor.queue_workload(workload, None)
        assert workload.ti.key in mock_executor.queued_tasks

    @pytest.mark.skipif(not AIRFLOW_V_3_0_PLUS, reason="Test requires Airflow 3+")
    def test_process_workloads(self, mock_executor):
        """Test processing workloads."""
        from airflow.executors.workloads import ExecuteTask

        workload = mock.Mock(spec=ExecuteTask)
        workload.ti = mock.Mock()
        workload.ti.key = TaskInstanceKey("test_dag", "test_task", "test_run", 1, -1)
        workload.ti.executor_config = {}
        workload.model_dump_json.return_value = '{"type": "ExecuteTask"}'

        mock_executor.queued_tasks[workload.ti.key] = workload

        mock_executor._process_workloads([workload])

        # Task should be moved from queued to running
        assert workload.ti.key not in mock_executor.queued_tasks
        assert workload.ti.key in mock_executor.running
        assert workload.ti.key in mock_executor.tasks_in_queue

        # SQS should have received message
        mock_executor._sqs_client.send_message.assert_called_once()

    def test_sync_polls_result_queue(self, mock_executor):
        """Test that sync polls the result queue."""
        mock_executor.sync()
        mock_executor._sqs_client.receive_message.assert_called()

    @mock.patch.object(BaseExecutor, "success")
    def test_handle_successful_task_result(self, success_mock, mock_executor):
        """Test handling a successful task result."""
        task_key = TaskInstanceKey("test_dag", "test_task", "test_run", 1, -1)
        mock_executor.tasks_in_queue[task_key] = mock.Mock()
        mock_executor.running.add(task_key)

        result = TaskResultMessage(
            message_id="msg-001",
            task_key=TaskInstanceKeySchema.from_task_instance_key(task_key),
            state="SUCCESS",
            info=TaskResultInfo(
                worker_id="worker-001",
                batch_job_id="job-001",
                execution_time_seconds=10.0,
            ),
            completed_at=datetime.now(timezone.utc),
        )

        mock_executor._handle_task_result(result)

        success_mock.assert_called_once()
        assert task_key not in mock_executor.tasks_in_queue

    @mock.patch.object(BaseExecutor, "fail")
    def test_handle_failed_task_result(self, fail_mock, mock_executor):
        """Test handling a failed task result."""
        task_key = TaskInstanceKey("test_dag", "test_task", "test_run", 1, -1)
        mock_executor.tasks_in_queue[task_key] = mock.Mock()
        mock_executor.running.add(task_key)

        result = TaskResultMessage(
            message_id="msg-001",
            task_key=TaskInstanceKeySchema.from_task_instance_key(task_key),
            state="FAILED",
            info=TaskResultInfo(
                worker_id="worker-001",
                batch_job_id="job-001",
                execution_time_seconds=5.0,
                error_message="Task failed",
            ),
            completed_at=datetime.now(timezone.utc),
        )

        mock_executor._handle_task_result(result)

        fail_mock.assert_called_once()
        assert task_key not in mock_executor.tasks_in_queue

    def test_scale_workers_up(self, mock_executor):
        """Test scaling workers up based on queue depth."""
        # Mock queue depth with pending tasks
        mock_executor._sqs_client.get_queue_attributes.return_value = {
            "Attributes": {"ApproximateNumberOfMessages": "5", "ApproximateNumberOfMessagesNotVisible": "0"}
        }

        mock_executor._scale_workers()

        # Should have requested workers
        assert len(mock_executor.pending_worker_starts) > 0

    def test_scale_workers_respects_max(self, mock_executor):
        """Test that scaling respects max_workers limit."""
        # Add workers up to max
        for i in range(10):
            mock_executor.active_workers.add_worker(
                job_id=f"job-{i}",
                worker_id=f"worker-{i}",
                started_at=datetime.now(timezone.utc),
            )

        # Mock queue depth with pending tasks
        mock_executor._sqs_client.get_queue_attributes.return_value = {
            "Attributes": {"ApproximateNumberOfMessages": "100", "ApproximateNumberOfMessagesNotVisible": "0"}
        }

        mock_executor._scale_workers()

        # Should not request more workers since at max
        assert len(mock_executor.pending_worker_starts) == 0

    def test_start_worker(self, mock_executor):
        """Test starting a worker via Batch."""
        job_id = mock_executor._start_worker("worker-001")
        assert job_id == MOCK_JOB_ID
        mock_executor._batch_client.submit_job.assert_called_once()

    def test_build_worker_submit_kwargs(self, mock_executor):
        """Test building worker submit kwargs."""
        kwargs = mock_executor._build_worker_submit_kwargs("worker-001")

        assert "containerOverrides" in kwargs
        assert "command" in kwargs["containerOverrides"]
        assert "environment" in kwargs["containerOverrides"]

        # Check command includes worker module
        command = kwargs["containerOverrides"]["command"]
        assert "airflow.providers.amazon.aws.executors.batch.worker_pool_worker" in " ".join(command)
        assert "--worker-id" in command
        assert "worker-001" in command

    def test_check_worker_health(self, mock_executor):
        """Test checking worker health."""
        # Add a worker
        mock_executor.active_workers.add_worker(
            job_id="job-001",
            worker_id="worker-001",
            started_at=datetime.now(timezone.utc),
        )

        # Mock successful worker
        mock_executor._batch_client.describe_jobs.return_value = {
            "jobs": [{"jobId": "job-001", "status": "RUNNING"}]
        }

        mock_executor._check_worker_health()

        # Worker should still be active
        assert len(mock_executor.active_workers) == 1

    def test_check_worker_health_removes_failed_workers(self, mock_executor):
        """Test that failed workers are removed."""
        # Add a worker
        mock_executor.active_workers.add_worker(
            job_id="job-001",
            worker_id="worker-001",
            started_at=datetime.now(timezone.utc),
        )

        # Mock failed worker
        mock_executor._batch_client.describe_jobs.return_value = {
            "jobs": [{"jobId": "job-001", "status": "FAILED"}]
        }

        mock_executor._check_worker_health()

        # Worker should be removed
        assert len(mock_executor.active_workers) == 0

    def test_terminate_all_workers(self, mock_executor):
        """Test terminating all workers."""
        # Add workers
        for i in range(3):
            mock_executor.active_workers.add_worker(
                job_id=f"job-{i}",
                worker_id=f"worker-{i}",
                started_at=datetime.now(timezone.utc),
            )

        mock_executor._terminate_all_workers(reason="Test termination")

        # All workers should be terminated
        assert mock_executor._batch_client.terminate_job.call_count == 3
        assert len(mock_executor.active_workers) == 0

    def test_terminate(self, mock_executor):
        """Test executor terminate."""
        mock_executor.active_workers.add_worker(
            job_id="job-001",
            worker_id="worker-001",
            started_at=datetime.now(timezone.utc),
        )

        mock_executor.terminate()
        mock_executor._batch_client.terminate_job.assert_called()

    def test_get_queue_depth(self, mock_executor):
        """Test getting queue depth."""
        mock_executor._sqs_client.get_queue_attributes.return_value = {
            "Attributes": {"ApproximateNumberOfMessages": "5", "ApproximateNumberOfMessagesNotVisible": "3"}
        }

        depth = mock_executor._get_queue_depth()
        assert depth == 8


class TestAwsBatchWorkerPoolExecutorConfig:
    """Tests configuration loading."""

    def test_config_defaults(self, set_env_vars):
        """Test default configuration values."""
        executor = AwsBatchWorkerPoolExecutor()
        assert executor.worker_idle_timeout_seconds == 300
        assert executor.worker_visibility_timeout_seconds == 3600
        assert executor.tasks_per_worker == 1
        assert executor.max_worker_start_attempts == 3

    def test_config_overrides(self, set_env_vars):
        """Test configuration overrides."""
        overrides = {
            (CONFIG_GROUP_NAME, "worker_idle_timeout_seconds"): "600",
            (CONFIG_GROUP_NAME, "tasks_per_worker"): "5",
        }
        with conf_vars(overrides):
            executor = AwsBatchWorkerPoolExecutor()
            assert executor.worker_idle_timeout_seconds == 600
            assert executor.tasks_per_worker == 5

    def test_submit_job_kwargs_config(self, set_env_vars):
        """Test submit_job_kwargs JSON configuration."""
        submit_kwargs = {"containerOverrides": {"memory": 4096, "vcpus": 2}}
        overrides = {(CONFIG_GROUP_NAME, "submit_job_kwargs"): json.dumps(submit_kwargs)}
        with conf_vars(overrides):
            executor = AwsBatchWorkerPoolExecutor()
            assert executor.submit_job_kwargs == submit_kwargs

    def test_shared_state_config(self, set_env_vars):
        """Test shared state configuration."""
        overrides = {
            (CONFIG_GROUP_NAME, "shared_state_init_module"): "myproject.worker_init",
            (CONFIG_GROUP_NAME, "shared_state_init_function"): "setup",
        }
        with conf_vars(overrides):
            executor = AwsBatchWorkerPoolExecutor()
            assert executor.shared_state_init_module == "myproject.worker_init"
            assert executor.shared_state_init_function == "setup"
