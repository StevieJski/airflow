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
Example DAG demonstrating AWS Batch Worker Pool pattern.

This DAG shows how to use AWS Batch with a worker pool pattern where:
1. Shared state is uploaded to S3
2. Grid tasks are sent to an SQS task queue
3. A worker is submitted to AWS Batch to process tasks
4. Results are collected from the SQS result queue

Prerequisites:
- AWS credentials configured (via Airflow connection or environment)
- Environment variables set:
  - AWS_BATCH_JOB_DEFINITION: Name or ARN of Batch job definition
  - AWS_BATCH_JOB_QUEUE: Name or ARN of Batch job queue
  - DOTNET_WORKER_IMAGE_URI: URI of the worker container image
  - AIRFLOW_SHARED_STATE_S3_URI: S3 URI for shared state (e.g., s3://bucket/path/)
  - AIRFLOW_TASK_QUEUE_URL: URL of SQS task queue
  - AIRFLOW_RESULT_QUEUE_URL: URL of SQS result queue

To run this DAG:
1. Set the required environment variables
2. Place this file in your Airflow DAGs folder
3. Trigger the DAG from the Airflow UI

The DAG uses Airflow Variables for configuration (with environment fallbacks):
- batch_worker_pool.job_definition
- batch_worker_pool.job_queue
- batch_worker_pool.shared_state_s3_uri
- batch_worker_pool.task_queue_url
- batch_worker_pool.result_queue_url
"""
from __future__ import annotations

import json
import logging
import os
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path

from airflow.sdk import DAG, dag, task
from airflow.models import Variable
from airflow.utils.trigger_rule import TriggerRule
from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook

log = logging.getLogger(__name__)

DAG_ID = "example_batch_worker_pool"

# Path to test data file (in tests subdirectory)
TEST_DATA_FILE = Path(__file__).parent /  "pool_worker_test_data.json"


def get_config(key: str, env_var: str) -> str | None:
    """Get configuration from Airflow Variable or environment variable."""
    try:
        return Variable.get(f"batch_worker_pool.{key}")
    except Exception:
        return os.environ.get(env_var)


# Configuration - loaded at DAG parse time for validation
AWS_BATCH_JOB_DEFINITION = get_config("job_definition", "AWS_BATCH_JOB_DEFINITION")
AWS_BATCH_JOB_QUEUE = get_config("job_queue", "AWS_BATCH_JOB_QUEUE")
AIRFLOW_SHARED_STATE_S3_URI = get_config("shared_state_s3_uri", "AIRFLOW_SHARED_STATE_S3_URI")
AIRFLOW_TASK_QUEUE_URL = get_config("task_queue_url", "AIRFLOW_TASK_QUEUE_URL")
AIRFLOW_RESULT_QUEUE_URL = get_config("result_queue_url", "AIRFLOW_RESULT_QUEUE_URL")


def load_test_data() -> dict:
    """Load test data from pool_worker_test_data.json."""
    if not TEST_DATA_FILE.exists():
        # Return sample data if test file doesn't exist
        return {
            "SharedState": {
                "RiskRun": {
                    "RunId": {
                        "Name": f"example_run_{uuid.uuid4().hex[:8]}"
                    }
                }
            },
            "Tasks": [
                {"TaskName": "sample_task_1", "data": "example_data_1"},
                {"TaskName": "sample_task_2", "data": "example_data_2"},
                {"TaskName": "sample_task_3", "data": "example_data_3"},
            ]
        }
    with open(TEST_DATA_FILE) as f:
        return json.load(f)


def parse_s3_uri(s3_uri: str) -> tuple[str, str]:
    """Parse S3 URI into bucket and key prefix."""
    if not s3_uri.startswith("s3://"):
        raise ValueError(f"Invalid S3 URI: {s3_uri}")
    path = s3_uri[5:]  # Remove 's3://'
    parts = path.split("/", 1)
    bucket = parts[0]
    prefix = parts[1] if len(parts) > 1 else ""
    return bucket, prefix


# AWS connection ID - configure this connection in Airflow UI (Admin > Connections)
AWS_CONN_ID = "aws_default"


def get_aws_client(service_name: str):
    """Get a boto3 client using Airflow's AWS connection."""
    hook = AwsBaseHook(aws_conn_id=AWS_CONN_ID, client_type=service_name)
    return hook.get_conn()


@task
def upload_shared_state() -> dict:
    """Upload SharedState to S3.

    Reads the SharedState section from pool_worker_test_data.json and uploads
    it to the configured S3 bucket.

    Returns:
        Dict with S3 location details
    """
    s3_uri = get_config("shared_state_s3_uri", "AIRFLOW_SHARED_STATE_S3_URI")
    if not s3_uri:
        raise ValueError("AIRFLOW_SHARED_STATE_S3_URI not configured")

    s3 = get_aws_client("s3")
    test_data = load_test_data()
    shared_state = test_data.get("SharedState", {})

    run_name = (
        shared_state.get("RiskRun", {}).get("RunId", {}).get("Name")
        or f"example_run_{uuid.uuid4().hex[:8]}"
    )

    bucket, prefix = parse_s3_uri(s3_uri)

    # Upload shared state as JSON
    shared_state_key = f"{prefix}{run_name}.json".lstrip("/")
    s3.put_object(
        Bucket=bucket,
        Key=shared_state_key,
        Body=json.dumps(shared_state, indent=2),
        ContentType="application/json",
    )

    log.info(f"Uploaded shared state to s3://{bucket}/{shared_state_key}")

    return {
        "bucket": bucket,
        "prefix": prefix,
        "shared_state_key": shared_state_key,
        "s3_uri": s3_uri,
        "run_name": run_name,
    }


@task
def get_sqs_queue_info() -> dict:
    """Return SQS queue URLs from configuration.

    Returns:
        Dict containing task_queue_url and result_queue_url
    """
    task_queue_url = get_config("task_queue_url", "AIRFLOW_TASK_QUEUE_URL")
    result_queue_url = get_config("result_queue_url", "AIRFLOW_RESULT_QUEUE_URL")

    if not task_queue_url:
        raise ValueError("AIRFLOW_TASK_QUEUE_URL not configured")
    if not result_queue_url:
        raise ValueError("AIRFLOW_RESULT_QUEUE_URL not configured")

    log.info(f"Using task queue: {task_queue_url}")
    log.info(f"Using result queue: {result_queue_url}")

    return {
        "task_queue_url": task_queue_url,
        "result_queue_url": result_queue_url,
    }


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
def send_grid_tasks(sqs_info: dict, s3_info: dict) -> str:
    """Send grid tasks to the SQS queue.

    Reads the Tasks array from pool_worker_test_data.json and creates
    task messages for the worker pool to process.

    Args:
        sqs_info: Dict containing task_queue_url and result_queue_url
        s3_info: Dict containing S3 shared state location details

    Returns:
        JSON string containing list of task keys sent
    """
    sqs = get_aws_client("sqs")
    task_queue_url = sqs_info["task_queue_url"]

    log.info(f"Sending tasks to queue URL: {task_queue_url}")

    # Check queue depth before sending
    depth_before = get_queue_depth(sqs, task_queue_url)
    log.info(f"Queue depth BEFORE sending: {depth_before}")

    test_data = load_test_data()
    tasks = test_data.get("Tasks", [])[:10]  # Limit to 10 tasks for example

    if not tasks:
        raise ValueError("No tasks found in test data")

    task_keys = []
    run_id = f"example_run_{uuid.uuid4().hex[:8]}"

    for idx, grid_task in enumerate(tasks):
        task_name = grid_task.get("TaskName", f"task_{idx}")

        task_key = {
            "dag_id": DAG_ID,
            "task_id": task_name,
            "run_id": run_id,
            "try_number": 1,
            "map_index": idx,
        }

        # Create task message matching TaskQueueMessage schema
        task_message = {
            "message_id": str(uuid.uuid4()),
            "task_key": task_key,
            "workload_json": json.dumps({
                "type": "ExecuteTask",
                "grid_task": True,
            }),
            "executor_config": {
                "GridTask": grid_task,
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
def submit_worker(sqs_info: dict, s3_info: dict) -> str:
    """Submit a worker job to AWS Batch.

    Args:
        sqs_info: Dict containing task_queue_url and result_queue_url
        s3_info: Dict containing S3 shared state location details

    Returns:
        JSON string with job_id and worker_id
    """
    job_definition = get_config("job_definition", "AWS_BATCH_JOB_DEFINITION")
    job_queue = get_config("job_queue", "AWS_BATCH_JOB_QUEUE")

    if not job_definition:
        raise ValueError("AWS_BATCH_JOB_DEFINITION not configured")
    if not job_queue:
        raise ValueError("AWS_BATCH_JOB_QUEUE not configured")

    batch = get_aws_client("batch")
    worker_id = f"worker-{uuid.uuid4().hex[:8]}"

    # Build container command for the worker
    command = [
        "--run-name", s3_info.get("run_name", "example_run"),
        "--worker-id", worker_id,
        "--task-queue-url", sqs_info["task_queue_url"],
        "--result-queue-url", sqs_info["result_queue_url"],
        "--idle-timeout", "120",  # 2 minute idle timeout
        "--max-tasks", "0",  # Process all available tasks
    ]

    container_overrides = {
        "command": command,
        "environment": [
            {"name": "AIRFLOW_SHARED_STATE_S3_URI", "value": s3_info["s3_uri"]},
            {"name": "AIRFLOW_WORKER_ID", "value": worker_id},
            {"name": "AIRFLOW_WORKER_POOL_MODE", "value": "true"},
            {"name": "Logging__LogLevel__Default", "value": "Information"},
        ],
    }

    log.info(f"Submitting worker with job definition: {job_definition}")
    log.info(f"Job queue: {job_queue}")
    log.info(f"Shared state S3 URI: {s3_info['s3_uri']}")

    response = batch.submit_job(
        jobName=f"worker-{worker_id}",
        jobQueue=job_queue,
        jobDefinition=job_definition,
        containerOverrides=container_overrides,
    )

    job_id = response["jobId"]
    log.info(f"Submitted worker job: {job_id} (worker_id: {worker_id})")

    return json.dumps({"job_id": job_id, "worker_id": worker_id})


@task
def wait_for_worker_running(worker_info_json: str) -> str:
    """Wait for the worker to start running."""
    batch = get_aws_client("batch")
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
                log.info(f"Worker job {job_id} already completed")
                return worker_info_json
            raise RuntimeError(f"Worker job failed before running: {reason}")

        time.sleep(10)

    raise TimeoutError(f"Worker job {job_id} did not start running in time")


@task
def verify_task_results(sqs_info: dict, tasks_info_json: str) -> str:
    """Verify workers processed the tasks and sent results.

    Args:
        sqs_info: Dict containing result_queue_url
        tasks_info_json: JSON string with task submission info

    Returns:
        Summary of task results
    """
    sqs = get_aws_client("sqs")
    result_queue_url = sqs_info["result_queue_url"]
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
                            f"Result for {task_id}: state={state}, "
                            f"worker={worker_id}, time={execution_time}s"
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

    return json.dumps(summary, indent=2)


@task
def wait_for_worker_completion(worker_info_json: str) -> str:
    """Wait for the worker to complete (idle timeout or max tasks)."""
    batch = get_aws_client("batch")
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
    return "Worker still running (timeout reached)"


@task(trigger_rule=TriggerRule.ALL_DONE)
def cleanup_worker(worker_info_json: str) -> str:
    """Terminate the worker job if still running."""
    batch = get_aws_client("batch")

    try:
        worker_info = json.loads(worker_info_json)
        job_id = worker_info["job_id"]

        response = batch.describe_jobs(jobs=[job_id])
        if response["jobs"]:
            status = response["jobs"][0]["status"]
            if status in ["SUBMITTED", "PENDING", "RUNNABLE", "STARTING", "RUNNING"]:
                batch.terminate_job(jobId=job_id, reason="DAG cleanup")
                log.info(f"Terminated worker job: {job_id}")
                return f"Terminated job {job_id}"
            else:
                log.info(f"Worker job {job_id} already in terminal state: {status}")
                return f"Job already completed: {status}"
    except Exception as e:
        log.warning(f"Failed to terminate worker job: {e}")
        return f"Cleanup error: {e}"

    return "Cleanup complete"


# ============== DAG DEFINITION ==============

with DAG(
    dag_id=DAG_ID,
    schedule=None,  # Manual trigger only
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["example", "aws", "batch", "worker-pool", "sqs"],
    doc_md=__doc__,
    default_args={
        "retries": 0,
    },
) as dag:
    # === SETUP PHASE ===
    sqs_queues = get_sqs_queue_info()
    s3_shared_state = upload_shared_state()

    # === EXECUTION PHASE ===
    # Send tasks to queue first
    tasks_info = send_grid_tasks(
        sqs_info=sqs_queues,
        s3_info=s3_shared_state,
    )

    # Submit worker to process tasks
    worker_info = submit_worker(
        sqs_info=sqs_queues,
        s3_info=s3_shared_state,
    )

    # Wait for worker to start
    worker_running = wait_for_worker_running(worker_info_json=worker_info)

    # Verify task results
    task_results = verify_task_results(
        sqs_info=sqs_queues,
        tasks_info_json=tasks_info,
    )

    # Wait for worker to finish
    worker_completed = wait_for_worker_completion(worker_info_json=worker_info)

    # === CLEANUP PHASE ===
    cleanup = cleanup_worker(worker_info_json=worker_info)

    # Define task dependencies
    # Setup runs in parallel
    [sqs_queues, s3_shared_state]

    # Then send tasks (needs both setup outputs)
    [sqs_queues, s3_shared_state] >> tasks_info

    # Submit worker after tasks are queued
    tasks_info >> worker_info

    # Wait for worker to run
    worker_info >> worker_running

    # Verify results after worker is running
    worker_running >> task_results

    # Wait for worker completion after results verified
    task_results >> worker_completed

    # Cleanup after worker completes
    worker_completed >> cleanup
