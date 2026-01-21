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
System test for AWS Batch Worker Pool pattern with external infrastructure.

This test verifies the worker pool pattern using pre-existing AWS infrastructure.
Unlike test_batch_worker_pool_dotnet.py, this test assumes all infrastructure
already exists and is identified via environment variables:

- AWS Batch Job Definition (AWS_BATCH_JOB_DEFINITION)
- AWS Batch Job Queue (AWS_BATCH_JOB_QUEUE)
- Container image (DOTNET_WORKER_IMAGE_URI)
- S3 bucket for shared state (AIRFLOW_SHARED_STATE_S3_URI)
- SQS Task Queue (AIRFLOW_TASK_QUEUE_URL)
- SQS Result Queue (AIRFLOW_RESULT_QUEUE_URL)

The test reads task definitions from pool_worker_test_data.json and:
1. Uploads SharedState to S3 bucket
2. Creates Airflow tasks from the Tasks array
3. Passes each task's JSON content via executor_config["GridTask"]
4. Submits workers to process tasks via SQS
5. Verifies task results

This conforms to the schema defined in workers/dotnet/AirflowWorker.Contracts/Models.cs
and follows the ON_DEMAND_WORKER_POOL_EXECUTOR_PLAN.md design.

Prerequisites:
- AWS credentials with permissions for Batch, SQS, S3
- Environment variables:
  - AWS_BATCH_JOB_DEFINITION: Name or ARN of pre-existing Batch job definition
  - AWS_BATCH_JOB_QUEUE: Name or ARN of pre-existing Batch job queue
  - DOTNET_WORKER_IMAGE_URI: URI of the .NET worker container image
  - AIRFLOW_SHARED_STATE_S3_URI: S3 URI for shared state (e.g., s3://bucket/path/)
  - AIRFLOW_TASK_QUEUE_URL: URL of pre-existing SQS task queue
  - AIRFLOW_RESULT_QUEUE_URL: URL of pre-existing SQS result queue

To run:
    export AWS_BATCH_JOB_DEFINITION=my-job-def
    export AWS_BATCH_JOB_QUEUE=my-job-queue
    export DOTNET_WORKER_IMAGE_URI=123456789.dkr.ecr.region.amazonaws.com/worker:latest
    export AIRFLOW_SHARED_STATE_S3_URI=s3://my-bucket/shared-state/
    export AIRFLOW_TASK_QUEUE_URL=https://sqs.region.amazonaws.com/123456789/task-queue
    export AIRFLOW_RESULT_QUEUE_URL=https://sqs.region.amazonaws.com/123456789/result-queue
    breeze testing system-tests \
        providers/amazon/tests/system/amazon/aws/tests/test_batch_worker_pool_external_infra.py
"""
from __future__ import annotations

import json
import logging
import os
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path

import boto3

from tests_common.test_utils.version_compat import AIRFLOW_V_3_0_PLUS

if AIRFLOW_V_3_0_PLUS:
    from airflow.sdk import DAG, chain, task
else:
    from airflow.decorators import task  # type: ignore[attr-defined,no-redef]
    from airflow.models.baseoperator import chain  # type: ignore[attr-defined,no-redef]
    from airflow.models.dag import DAG  # type: ignore[attr-defined,no-redef,assignment]

try:
    from airflow.sdk import TriggerRule
except ImportError:
    from airflow.utils.trigger_rule import TriggerRule  # type: ignore[no-redef,attr-defined]

from system.amazon.aws.utils import (
    ENV_ID_KEY,
    SystemTestContextBuilder,
    prune_logs,
)

log = logging.getLogger(__name__)

DAG_ID = "test_batch_worker_pool_external_infra"

# Path to test data file (relative to this test file)
TEST_DATA_FILE = Path(__file__).parent / "pool_worker_test_data.json"

# Required environment variables for external infrastructure
AWS_BATCH_JOB_DEFINITION = os.environ.get("AWS_BATCH_JOB_DEFINITION")
AWS_BATCH_JOB_QUEUE = os.environ.get("AWS_BATCH_JOB_QUEUE")
DOTNET_WORKER_IMAGE_URI = os.environ.get("DOTNET_WORKER_IMAGE_URI")
AIRFLOW_SHARED_STATE_S3_URI = os.environ.get("AIRFLOW_SHARED_STATE_S3_URI")
AIRFLOW_TASK_QUEUE_URL = os.environ.get("AIRFLOW_TASK_QUEUE_URL")
AIRFLOW_RESULT_QUEUE_URL = os.environ.get("AIRFLOW_RESULT_QUEUE_URL")

# Check which required environment variables are missing
MISSING_ENV_VARS = []
if not AWS_BATCH_JOB_DEFINITION:
    MISSING_ENV_VARS.append("AWS_BATCH_JOB_DEFINITION")
if not AWS_BATCH_JOB_QUEUE:
    MISSING_ENV_VARS.append("AWS_BATCH_JOB_QUEUE")
if not DOTNET_WORKER_IMAGE_URI:
    MISSING_ENV_VARS.append("DOTNET_WORKER_IMAGE_URI")
if not AIRFLOW_SHARED_STATE_S3_URI:
    MISSING_ENV_VARS.append("AIRFLOW_SHARED_STATE_S3_URI")
if not AIRFLOW_TASK_QUEUE_URL:
    MISSING_ENV_VARS.append("AIRFLOW_TASK_QUEUE_URL")
if not AIRFLOW_RESULT_QUEUE_URL:
    MISSING_ENV_VARS.append("AIRFLOW_RESULT_QUEUE_URL")

sys_test_context_task = SystemTestContextBuilder().build()


def load_test_data() -> dict:
    """Load test data from pool_worker_test_data.json."""
    with open(TEST_DATA_FILE) as f:
        return json.load(f)


def parse_s3_uri(s3_uri: str) -> tuple[str, str]:
    """Parse S3 URI into bucket and key prefix.

    Args:
        s3_uri: S3 URI in format s3://bucket/path/

    Returns:
        Tuple of (bucket_name, key_prefix)
    """
    if not s3_uri.startswith("s3://"):
        raise ValueError(f"Invalid S3 URI: {s3_uri}")
    path = s3_uri[5:]  # Remove 's3://'
    parts = path.split("/", 1)
    bucket = parts[0]
    prefix = parts[1] if len(parts) > 1 else ""
    return bucket, prefix


@task
def upload_shared_state(env_id: str) -> dict:
    """Upload SharedState from test data to S3.

    Reads the SharedState section from pool_worker_test_data.json and uploads
    it to the S3 bucket specified by AIRFLOW_SHARED_STATE_S3_URI.

    Returns:
        Dict with S3 location details
    """
    s3 = boto3.client("s3")
    test_data = load_test_data()
    shared_state = test_data.get("SharedState", {})
    runName = (
        shared_state.get("RiskRun", {}).get("RunId", {}).get("Name")
        or f"external_test_run_{uuid.uuid4().hex[:8]}"
    )

    bucket, prefix = parse_s3_uri(AIRFLOW_SHARED_STATE_S3_URI)

    # Upload shared state as JSON
    shared_state_key = f"{prefix}{runName}.json".lstrip("/")
    s3.put_object(
        Bucket=bucket,
        Key=shared_state_key,
        Body=json.dumps(shared_state, indent=2),
        ContentType="application/json",
    )

    log.info(f"Uploaded shared state to s3://{bucket}/{shared_state_key}")

    # Upload a marker file to confirm the location is accessible
    marker_key = f"{prefix}marker-{env_id}.txt".lstrip("/")
    s3.put_object(
        Bucket=bucket,
        Key=marker_key,
        Body=f"Test marker for {env_id} - {datetime.now(timezone.utc).isoformat()}",
        ContentType="text/plain",
    )

    return {
        "bucket": bucket,
        "prefix": prefix,
        "shared_state_key": shared_state_key,
        "marker_key": marker_key,
        "s3_uri": AIRFLOW_SHARED_STATE_S3_URI,
    }


@task
def get_sqs_queue_info() -> dict:
    """Return pre-existing SQS queue URLs from environment variables.

    Unlike the other test which creates SQS queues, this test uses
    pre-existing queues identified by environment variables.

    Returns:
        Dict containing task_queue_url and result_queue_url
    """
    log.info(f"Using pre-existing task queue: {AIRFLOW_TASK_QUEUE_URL}")
    log.info(f"Using pre-existing result queue: {AIRFLOW_RESULT_QUEUE_URL}")

    return {
        "task_queue_url": AIRFLOW_TASK_QUEUE_URL,
        "result_queue_url": AIRFLOW_RESULT_QUEUE_URL,
    }


@task
def submit_worker(
    sqs_info: dict,
    s3_info: dict,
) -> str:
    """Submit a worker job to AWS Batch using pre-existing infrastructure.

    Uses the externally defined job definition and job queue specified via
    environment variables.

    Args:
        sqs_info: Dict containing task_queue_url and result_queue_url
        s3_info: Dict containing S3 shared state location details
    """
    batch = boto3.client("batch")
    worker_id = f"external-worker-{uuid.uuid4().hex[:8]}"

    # Build container command for .NET worker
    command = [
        "--run-name", "AwsBatchSystemTest1549dd69-ae6e-4a35-bdf9-286dde7915b9",
        "--worker-id", worker_id,
        "--task-queue-url", sqs_info["task_queue_url"],
        "--result-queue-url", sqs_info["result_queue_url"],
        "--idle-timeout","120",  # 2 minute idle timeout for test
        "--max-tasks", "0",  # Process all available tasks
    ]

    # Build container overrides
    container_overrides = {
        "command": command,
        "environment": [
            {"name": "AIRFLOW_SHARED_STATE_S3_URI", "value": s3_info["s3_uri"]},
            {"name": "AIRFLOW_WORKER_ID", "value": worker_id},
            {"name": "AIRFLOW_WORKER_POOL_MODE", "value": "true"},
            {"name": "Logging__LogLevel__Default", "value": "Information"},
        ],
    }

    log.info(f"Submitting worker with job definition: {AWS_BATCH_JOB_DEFINITION}")
    log.info(f"Job queue: {AWS_BATCH_JOB_QUEUE}")
    log.info(f"Shared state S3 URI: {s3_info['s3_uri']}")

    response = batch.submit_job(
        jobName=f"dotnet-worker-{worker_id}",
        jobQueue=AWS_BATCH_JOB_QUEUE,
        jobDefinition=AWS_BATCH_JOB_DEFINITION,
        containerOverrides=container_overrides,
    )

    job_id = response["jobId"]
    log.info(f"Submitted worker job: {job_id} (worker_id: {worker_id})")

    return json.dumps({"job_id": job_id, "worker_id": worker_id})


@task
def wait_for_worker_running(worker_info_json: str) -> str:
    """Wait for the worker to start running."""
    batch = boto3.client("batch")
    worker_info = json.loads(worker_info_json)
    job_id = worker_info["job_id"]

    log.info(f"Waiting for worker job {job_id} to start running...")

    for attempt in range(60):
        response = batch.describe_jobs(jobs=[job_id])
        if not response["jobs"]:
            raise RuntimeError(f"Job {job_id} not found")

        status = response["jobs"][0]["status"]
        log.info(f"Job {job_id} status: {status}")

        if status == "RUNNING":
            log.info(f"Worker job {job_id} is now RUNNING")
            return worker_info_json

        if status in ["FAILED", "SUCCEEDED"]:
            reason = response["jobs"][0].get("statusReason", "Unknown")
            if status == "SUCCEEDED":
                log.info(f"Worker job {job_id} already completed (no tasks)")
                return worker_info_json
            raise RuntimeError(f"Worker job failed before running: {reason}")

        time.sleep(10)

    raise TimeoutError(f"Worker job {job_id} did not start running in time")


def get_queue_depth(sqs_client, queue_url: str) -> dict:
    """Get the number of messages in an SQS queue."""
    response = sqs_client.get_queue_attributes(
        QueueUrl=queue_url,
        AttributeNames=["ApproximateNumberOfMessages", "ApproximateNumberOfMessagesNotVisible"],
    )
    attrs = response.get("Attributes", {})
    visible = int(attrs.get("ApproximateNumberOfMessages", 0))
    in_flight = int(attrs.get("ApproximateNumberOfMessagesNotVisible", 0))
    return {"visible": visible, "in_flight": in_flight, "total": visible + in_flight}


@task
def send_grid_tasks(task_queue_url: str) -> str:
    """Send grid tasks from test data to the SQS queue.

    Reads the Tasks array from pool_worker_test_data.json and creates
    an Airflow task message for each item. The task data is passed via
    executor_config["GridTask"] as per the AirflowWorker.Contracts schema.

    Args:
        task_queue_url: URL of the SQS task queue

    Returns:
        JSON string containing list of task keys sent
    """
    sqs = boto3.client("sqs")

    # Log the queue URL being used
    log.info(f"Sending tasks to queue URL: {task_queue_url}")

    # Check queue depth before sending
    depth_before = get_queue_depth(sqs, task_queue_url)
    log.info(f"Queue depth BEFORE sending: {depth_before}")

    test_data = load_test_data()
    tasks = test_data.get("Tasks", [])[:10]

    if not tasks:
        raise ValueError("No tasks found in test data file")

    task_keys = []
    run_id = f"external_test_run_{uuid.uuid4().hex[:8]}"

    for idx, grid_task in enumerate(tasks):
        # Extract task name for identification
        task_name = grid_task.get("TaskName", f"task_{idx}")

        task_key = {
            "dag_id": DAG_ID,
            "task_id": task_name,
            "run_id": run_id,
            "try_number": 1,
            "map_index": idx,
        }

        # Create task message matching TaskQueueMessage schema from Models.cs
        # The GridTask is passed via executor_config as per the design
        task_message = {
            "message_id": str(uuid.uuid4()),
            "task_key": task_key,
            "workload_json": json.dumps(
                {
                    "type": "ExecuteTask",
                    "grid_task": True,
                }
            ),
            "executor_config": {
                "GridTask": grid_task,  # Full task data from pool_worker_test_data.json
            },
            "enqueued_at": datetime.now(timezone.utc).isoformat(),
        }

        response = sqs.send_message(
            QueueUrl=task_queue_url,
            MessageBody=json.dumps(task_message),
            MessageAttributes={
                "task_id": {"StringValue": task_name, "DataType": "String"},
                "dag_id": {"StringValue": DAG_ID, "DataType": "String"},
                "map_index": {"StringValue": str(idx), "DataType": "Number"},
            },
        )

        log.info(f"Sent task {task_name} (index {idx}): message_id={response['MessageId']}")
        task_keys.append(task_key)

    log.info(f"Sent {len(task_keys)} grid tasks to queue")

    # Check queue depth after sending
    depth_after = get_queue_depth(sqs, task_queue_url)
    log.info(f"Queue depth AFTER sending: {depth_after}")

    return json.dumps({"task_keys": task_keys, "run_id": run_id, "total_tasks": len(task_keys)})


@task
def verify_task_results(result_queue_url: str, tasks_info_json: str) -> str:
    """Verify workers processed the tasks and sent results.

    Polls the result queue and verifies that results are received for the
    submitted tasks.

    Args:
        result_queue_url: URL of the SQS result queue
        tasks_info_json: JSON string with task submission info

    Returns:
        Summary of task results
    """
    sqs = boto3.client("sqs")
    tasks_info = json.loads(tasks_info_json)
    total_tasks = tasks_info["total_tasks"]
    run_id = tasks_info["run_id"]

    log.info(f"Waiting for results for {total_tasks} tasks (run_id: {run_id})")

    results_received = []
    failed_tasks = []
    succeeded_tasks = []

    # Poll for results with timeout
    max_attempts = 60  # 10 minutes with 10s waits
    for attempt in range(max_attempts):
        response = sqs.receive_message(
            QueueUrl=result_queue_url,
            MaxNumberOfMessages=10,
            WaitTimeSeconds=10,
            MessageAttributeNames=["All"],
        )

        if "Messages" in response and response["Messages"]:
            for msg in response["Messages"]:
                try:
                    result = json.loads(msg["Body"])
                    result_task_key = result.get("task_key", {})

                    # Verify this result is for our run
                    if result_task_key.get("run_id") == run_id:
                        state = result.get("state")
                        info = result.get("info", {})
                        worker_id = info.get("worker_id", "unknown")
                        execution_time = info.get("execution_time_seconds", 0)
                        task_id = result_task_key.get("task_id", "unknown")

                        log.info(
                            f"Result for {task_id}: state={state}, worker={worker_id}, time={execution_time}s"
                        )

                        results_received.append(result)
                        if state == "SUCCESS":
                            succeeded_tasks.append(task_id)
                        else:
                            failed_tasks.append(task_id)
                            error = info.get("error_message", "Unknown error")
                            log.warning(f"Task {task_id} failed: {error}")

                    # Delete processed message
                    sqs.delete_message(
                        QueueUrl=result_queue_url,
                        ReceiptHandle=msg["ReceiptHandle"],
                    )

                except json.JSONDecodeError:
                    log.warning(f"Invalid JSON in message: {msg['Body']}")

        # Check if we've received all results
        if len(results_received) >= total_tasks:
            log.info(f"Received all {total_tasks} task results")
            break

        log.info(f"Attempt {attempt + 1}: received {len(results_received)}/{total_tasks} results")

    summary = {
        "total_tasks": total_tasks,
        "results_received": len(results_received),
        "succeeded": len(succeeded_tasks),
        "failed": len(failed_tasks),
        "succeeded_tasks": succeeded_tasks,
        "failed_tasks": failed_tasks,
    }

    if len(results_received) < total_tasks:
        log.warning(f"Only received {len(results_received)}/{total_tasks} results")

    return json.dumps(summary)


@task
def wait_for_worker_completion(worker_info_json: str) -> str:
    """Wait for the worker to complete (idle timeout or max tasks)."""
    batch = boto3.client("batch")
    worker_info = json.loads(worker_info_json)
    job_id = worker_info["job_id"]

    log.info(f"Waiting for worker job {job_id} to complete...")

    for attempt in range(90):  # 15 minutes max
        response = batch.describe_jobs(jobs=[job_id])
        if not response["jobs"]:
            raise RuntimeError(f"Job {job_id} not found")

        status = response["jobs"][0]["status"]
        log.info(f"Job {job_id} status: {status}")

        if status == "SUCCEEDED":
            log.info(f"Worker job {job_id} completed successfully")
            return "Worker completed successfully"

        if status == "FAILED":
            reason = response["jobs"][0].get("statusReason", "Unknown")
            log.error(f"Worker job {job_id} failed: {reason}")
            return f"Worker failed: {reason}"

        time.sleep(10)

    log.warning(f"Worker job {job_id} did not complete in time")
    return "Worker still running (test timeout)"


# ============== CLEANUP TASKS ==============


@task(trigger_rule=TriggerRule.ALL_DONE)
def terminate_worker_job(worker_info_json: str):
    """Terminate the worker job if still running."""
    batch = boto3.client("batch")

    try:
        worker_info = json.loads(worker_info_json)
        job_id = worker_info["job_id"]

        response = batch.describe_jobs(jobs=[job_id])
        if response["jobs"]:
            status = response["jobs"][0]["status"]
            if status in ["SUBMITTED", "PENDING", "RUNNABLE", "STARTING", "RUNNING"]:
                batch.terminate_job(jobId=job_id, reason="Test cleanup")
                log.info(f"Terminated worker job: {job_id}")
            else:
                log.info(f"Worker job {job_id} already in terminal state: {status}")
    except Exception as e:
        log.warning(f"Failed to terminate worker job: {e}")


@task(trigger_rule=TriggerRule.ALL_DONE)
def cleanup_s3_test_files(s3_info: dict, env_id: str):
    """Clean up test files from S3 (but not the bucket itself since it's external)."""
    s3 = boto3.client("s3")

    try:
        bucket = s3_info.get("bucket")
        marker_key = s3_info.get("marker_key")

        if bucket and marker_key:
            # Only delete the marker file we created (leave shared state)
            s3.delete_object(Bucket=bucket, Key=marker_key)
            log.info(f"Deleted marker file: s3://{bucket}/{marker_key}")

        # Optionally clean up shared state if we created it
        shared_state_key = s3_info.get("shared_state_key")
        if bucket and shared_state_key:
            s3.delete_object(Bucket=bucket, Key=shared_state_key)
            log.info(f"Deleted shared state file: s3://{bucket}/{shared_state_key}")

    except Exception as e:
        log.warning(f"Failed to clean up S3 test files: {e}")


# ============== DAG DEFINITION ==============

with DAG(
    dag_id=DAG_ID,
    schedule="@once",
    start_date=datetime(2021, 1, 1),
    tags=["system-test", "executor", "batch", "worker-pool", "external-infra"],
    catchup=False,
) as dag:
    test_context = sys_test_context_task()
    env_id = test_context[ENV_ID_KEY]

    # === SETUP PHASE ===
    # Get pre-existing SQS queue URLs from environment variables
    sqs_queues = get_sqs_queue_info()

    # Upload shared state from test data to S3
    s3_shared_state = upload_shared_state(env_id)

    # === TEST PHASE ===
    # Send grid tasks FIRST (before worker starts) so we can verify they're in the queue
    tasks_info = send_grid_tasks(task_queue_url=sqs_queues["task_queue_url"])

    # Submit worker using external Batch infrastructure
    worker_info = submit_worker(
        sqs_info=sqs_queues,
        s3_info=s3_shared_state,
    )

    worker_running = wait_for_worker_running(worker_info_json=worker_info)

    # Verify task results
    task_results = verify_task_results(
        result_queue_url=sqs_queues["result_queue_url"],
        tasks_info_json=tasks_info,
    )

    worker_completed = wait_for_worker_completion(worker_info_json=worker_info)

    # === CLEANUP PHASE ===
    # Note: SQS queues are external and NOT deleted
    terminate_worker = terminate_worker_job(worker_info_json=worker_info)

    cleanup_s3 = cleanup_s3_test_files(s3_info=s3_shared_state, env_id=env_id)

    log_cleanup = prune_logs(
        [
            ("/aws/batch/job", env_id),
        ],
    )

    # Define task dependencies
    chain(
        # Setup
        test_context,
        [sqs_queues, s3_shared_state],
        # Test - send tasks first, then start worker
        tasks_info,
        worker_info,
        worker_running,
        task_results,
        worker_completed,
        # Cleanup
        terminate_worker,
        cleanup_s3,
        log_cleanup,
    )

    from tests_common.test_utils.watcher import watcher

    list(dag.tasks) >> watcher()

from tests_common.test_utils.system_tests import get_test_run  # noqa: E402

import pytest  # noqa: E402

# Skip the test if required environment variables are not set
if MISSING_ENV_VARS:
    test_run = pytest.mark.skip(
        reason=f"Missing required environment variables: {', '.join(MISSING_ENV_VARS)}"
    )(get_test_run(dag))
else:
    test_run = get_test_run(dag)
