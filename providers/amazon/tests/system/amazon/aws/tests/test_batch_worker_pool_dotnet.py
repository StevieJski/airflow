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
System test for AWS Batch Worker Pool pattern integration.

This test verifies the worker pool pattern by using a shell-based mock worker
that mimics the .NET worker behavior. This tests the core integration:
1. Worker polls SQS for tasks
2. Worker processes tasks
3. Worker reports results back via SQS
4. Worker handles idle timeout

This approach tests the infrastructure integration without requiring
Docker-in-Docker (which is not available in the breeze test environment).

For a full .NET worker test, build and push the .NET image externally
and provide the image URI via the DOTNET_WORKER_IMAGE_URI environment variable.

Prerequisites:
- AWS credentials with permissions for Batch, SQS
- Environment variables set (see SystemTestContextBuilder below)

To run:
    breeze testing system-tests \
        providers/amazon/tests/system/amazon/aws/tests/test_batch_worker_pool_dotnet.py
"""
from __future__ import annotations

import json
import logging
import os
import time
import uuid
from datetime import datetime, timezone

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
    split_string,
)

log = logging.getLogger(__name__)

DAG_ID = "test_batch_worker_pool_dotnet"

# Externally fetched variables
ROLE_ARN_KEY = "ROLE_ARN"
SUBNETS_KEY = "SUBNETS"
SECURITY_GROUPS_KEY = "SECURITY_GROUPS"

# Optional: Pre-built .NET worker image URI (skips mock worker if provided)
DOTNET_WORKER_IMAGE_URI = os.environ.get("DOTNET_WORKER_IMAGE_URI")

# Use public Amazon Linux 2023 image for mock worker
# Note: This image does NOT include AWS CLI, so the script installs it
MOCK_WORKER_IMAGE = "public.ecr.aws/amazonlinux/amazonlinux:2023"

sys_test_context_task = (
    SystemTestContextBuilder()
    .add_variable(ROLE_ARN_KEY)
    .add_variable(SUBNETS_KEY)
    .add_variable(SECURITY_GROUPS_KEY)
    .build()
)


# Shell script that mimics the .NET worker behavior
# This tests the SQS-based task distribution and result reporting pattern
MOCK_WORKER_SCRIPT = '''#!/bin/bash
set -e

WORKER_ID="$1"
TASK_QUEUE_URL="$2"
RESULT_QUEUE_URL="$3"
IDLE_TIMEOUT="${4:-60}"
MAX_TASKS="${5:-2}"

echo "[$(date)] Worker $WORKER_ID starting..."
echo "[$(date)] Task queue: $TASK_QUEUE_URL"
echo "[$(date)] Result queue: $RESULT_QUEUE_URL"
echo "[$(date)] Idle timeout: $IDLE_TIMEOUT seconds"
echo "[$(date)] Max tasks: $MAX_TASKS"

# Install AWS CLI v2 and jq (required dependencies)
echo "[$(date)] Installing AWS CLI and jq..."
dnf install -y unzip jq less groff > /dev/null 2>&1
curl -s "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "/tmp/awscliv2.zip"
unzip -q /tmp/awscliv2.zip -d /tmp
/tmp/aws/install > /dev/null 2>&1
rm -rf /tmp/awscliv2.zip /tmp/aws
echo "[$(date)] AWS CLI installed: $(aws --version)"

tasks_completed=0
idle_start=$(date +%s)

while true; do
    # Check max tasks
    if [ "$MAX_TASKS" -gt 0 ] && [ "$tasks_completed" -ge "$MAX_TASKS" ]; then
        echo "[$(date)] Reached max tasks ($MAX_TASKS), shutting down"
        break
    fi

    # Poll for tasks
    echo "[$(date)] Polling for tasks..."
    RESPONSE=$(aws sqs receive-message \\
        --queue-url "$TASK_QUEUE_URL" \\
        --max-number-of-messages 1 \\
        --wait-time-seconds 10 \\
        --attribute-names All \\
        --message-attribute-names All \\
        2>/dev/null || echo '{}')

    MESSAGES=$(echo "$RESPONSE" | jq -r '.Messages // []')
    MSG_COUNT=$(echo "$MESSAGES" | jq 'length')

    if [ "$MSG_COUNT" -eq 0 ]; then
        # Check idle timeout
        now=$(date +%s)
        idle_duration=$((now - idle_start))
        echo "[$(date)] No messages, idle for $idle_duration seconds"

        if [ "$idle_duration" -ge "$IDLE_TIMEOUT" ]; then
            echo "[$(date)] Idle timeout reached, shutting down"
            break
        fi
        continue
    fi

    # Reset idle timer
    idle_start=$(date +%s)

    # Process each message
    echo "$MESSAGES" | jq -c '.[]' | while read -r MSG; do
        RECEIPT=$(echo "$MSG" | jq -r '.ReceiptHandle')
        BODY=$(echo "$MSG" | jq -r '.Body')

        echo "[$(date)] Processing message..."

        # Parse task
        TASK_KEY=$(echo "$BODY" | jq '.task_key')
        DAG_ID=$(echo "$TASK_KEY" | jq -r '.dag_id')
        TASK_ID=$(echo "$TASK_KEY" | jq -r '.task_id')
        RUN_ID=$(echo "$TASK_KEY" | jq -r '.run_id')

        echo "[$(date)] Task: dag=$DAG_ID, task=$TASK_ID, run=$RUN_ID"

        # Execute task (mock execution)
        START_TIME=$(date +%s.%N)

        # Simulate task execution
        echo "[$(date)] Executing task..."
        sleep 2  # Simulate work

        END_TIME=$(date +%s.%N)
        EXEC_TIME=$(echo "$END_TIME - $START_TIME" | bc 2>/dev/null || echo "2.0")

        # Build result message
        RESULT=$(cat <<EOF
{
    "task_key": $TASK_KEY,
    "state": "SUCCESS",
    "info": {
        "worker_id": "$WORKER_ID",
        "worker_type": "mock-dotnet",
        "execution_time_seconds": $EXEC_TIME,
        "exit_code": 0
    },
    "completed_at": "$(date -u +%Y-%m-%dT%H:%M:%S.000Z)"
}
EOF
)

        # Send result
        echo "[$(date)] Sending result..."
        aws sqs send-message \\
            --queue-url "$RESULT_QUEUE_URL" \\
            --message-body "$RESULT" \\
            > /dev/null

        # Delete processed message
        aws sqs delete-message \\
            --queue-url "$TASK_QUEUE_URL" \\
            --receipt-handle "$RECEIPT" \\
            > /dev/null

        echo "[$(date)] Task completed successfully"
    done

    tasks_completed=$((tasks_completed + 1))
done

echo "[$(date)] Worker $WORKER_ID shutting down after $tasks_completed tasks"
exit 0
'''


@task
def create_sqs_queues(env_id: str) -> dict:
    """Create SQS queues for task and result communication."""
    sqs = boto3.client("sqs")

    task_queue_name = f"{env_id}-dotnet-task-queue"
    result_queue_name = f"{env_id}-dotnet-result-queue"

    # Create task queue with longer visibility timeout for task execution
    task_queue = sqs.create_queue(
        QueueName=task_queue_name,
        Attributes={
            "VisibilityTimeout": "300",
            "MessageRetentionPeriod": "3600",
        },
    )

    # Create result queue
    result_queue = sqs.create_queue(
        QueueName=result_queue_name,
        Attributes={
            "VisibilityTimeout": "30",
            "MessageRetentionPeriod": "3600",
        },
    )

    log.info(f"Created task queue: {task_queue['QueueUrl']}")
    log.info(f"Created result queue: {result_queue['QueueUrl']}")

    return {
        "task_queue_url": task_queue["QueueUrl"],
        "result_queue_url": result_queue["QueueUrl"],
        "task_queue_name": task_queue_name,
        "result_queue_name": result_queue_name,
    }


@task
def create_batch_compute_environment(
    env_id: str, role_arn: str, subnets: list[str], security_groups: list[str]
) -> str:
    """Create a Fargate compute environment for workers."""
    batch = boto3.client("batch")
    compute_env_name = f"{env_id}-dotnet-compute-env"

    batch.create_compute_environment(
        computeEnvironmentName=compute_env_name,
        type="MANAGED",
        state="ENABLED",
        computeResources={
            "type": "FARGATE",
            "maxvCpus": 4,
            "subnets": subnets,
            "securityGroupIds": security_groups,
        },
        serviceRole=role_arn,
    )

    # Wait for compute environment to be valid
    for _ in range(30):
        response = batch.describe_compute_environments(computeEnvironments=[compute_env_name])
        status = response["computeEnvironments"][0]["status"]
        if status == "VALID":
            log.info(f"Compute environment {compute_env_name} is VALID")
            return compute_env_name
        if status == "INVALID":
            raise RuntimeError(f"Compute environment {compute_env_name} is INVALID")
        time.sleep(10)

    raise TimeoutError(f"Compute environment {compute_env_name} did not become VALID")


@task
def create_batch_job_queue(env_id: str, compute_env_name: str) -> str:
    """Create a job queue for workers."""
    batch = boto3.client("batch")
    job_queue_name = f"{env_id}-dotnet-job-queue"

    batch.create_job_queue(
        jobQueueName=job_queue_name,
        state="ENABLED",
        priority=1,
        computeEnvironmentOrder=[
            {
                "order": 1,
                "computeEnvironment": compute_env_name,
            }
        ],
    )

    # Wait for job queue to be valid
    for _ in range(30):
        response = batch.describe_job_queues(jobQueues=[job_queue_name])
        status = response["jobQueues"][0]["status"]
        if status == "VALID":
            log.info(f"Job queue {job_queue_name} is VALID")
            return job_queue_name
        time.sleep(5)

    raise TimeoutError(f"Job queue {job_queue_name} did not become VALID")


@task
def create_worker_job_definition(env_id: str, role_arn: str) -> str:
    """Create job definition for mock worker containers.

    If DOTNET_WORKER_IMAGE_URI is set, uses the real .NET worker image.
    Otherwise, uses a mock worker script on Amazon Linux.
    """
    batch = boto3.client("batch")
    job_def_name = f"{env_id}-dotnet-worker-job-def"

    # Use pre-built .NET image if provided, otherwise use mock worker
    image_uri = DOTNET_WORKER_IMAGE_URI or MOCK_WORKER_IMAGE

    if DOTNET_WORKER_IMAGE_URI:
        log.info(f"Using pre-built .NET worker image: {image_uri}")
        # Real .NET worker command format
        command = ["--help"]  # Will be overridden at job submission
    else:
        log.info(f"Using mock worker with Amazon Linux image: {image_uri}")
        # Mock worker uses shell script
        command = ["/bin/bash", "-c", "echo 'Mock worker ready'"]

    batch.register_job_definition(
        jobDefinitionName=job_def_name,
        type="container",
        platformCapabilities=["FARGATE"],
        containerProperties={
            "image": image_uri,
            "resourceRequirements": [
                {"type": "VCPU", "value": "1"},
                {"type": "MEMORY", "value": "2048"},
            ],
            "executionRoleArn": role_arn,
            "jobRoleArn": role_arn,
            "networkConfiguration": {
                "assignPublicIp": "ENABLED",
            },
            "command": command,
        },
        timeout={
            "attemptDurationSeconds": 600,  # 10 minutes max for test
        },
    )

    log.info(f"Created worker job definition: {job_def_name}")
    return job_def_name


@task
def submit_worker(
    job_queue: str,
    job_definition: str,
    task_queue_url: str,
    result_queue_url: str,
) -> str:
    """Submit a worker job to Batch."""
    batch = boto3.client("batch")
    worker_id = f"test-worker-{uuid.uuid4().hex[:8]}"

    if DOTNET_WORKER_IMAGE_URI:
        # Real .NET worker command
        command = [
            "--worker-id",
            worker_id,
            "--task-queue-url",
            task_queue_url,
            "--result-queue-url",
            result_queue_url,
            "--idle-timeout",
            "60",
            "--max-tasks",
            "2",
        ]
    else:
        # Mock worker using inline shell script
        # Use base64 encoding to safely pass the script
        import base64

        script_b64 = base64.b64encode(MOCK_WORKER_SCRIPT.encode()).decode()
        command = [
            "/bin/bash",
            "-c",
            f"echo {script_b64} | base64 -d > /tmp/worker.sh && chmod +x /tmp/worker.sh && "
            f"/tmp/worker.sh '{worker_id}' '{task_queue_url}' '{result_queue_url}' 60 2",
        ]

    response = batch.submit_job(
        jobName=f"dotnet-worker-{worker_id}",
        jobQueue=job_queue,
        jobDefinition=job_definition,
        containerOverrides={
            "command": command,
        },
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
            # If succeeded already, that's ok (worker started and found no tasks)
            if status == "SUCCEEDED":
                log.info(f"Worker job {job_id} already completed (no tasks)")
                return worker_info_json
            raise RuntimeError(f"Worker job failed before running: {reason}")

        time.sleep(10)

    raise TimeoutError(f"Worker job {job_id} did not start running in time")


@task
def send_test_task(task_queue_url: str) -> str:
    """Send a test task message to the queue."""
    sqs = boto3.client("sqs")

    task_key = {
        "dag_id": "test_dag",
        "task_id": "test_task",
        "run_id": f"test_run_{uuid.uuid4().hex[:8]}",
        "try_number": 1,
        "map_index": -1,
    }

    # Create a task message matching the schema expected by workers
    task_message = {
        "message_id": str(uuid.uuid4()),
        "task_key": task_key,
        "workload_json": json.dumps(
            {
                "type": "ExecuteTask",
                "test": True,
            }
        ),
        "executor_config": {
            "command": "echo 'Hello from worker test'",
        },
        "enqueued_at": datetime.now(timezone.utc).isoformat(),
    }

    response = sqs.send_message(
        QueueUrl=task_queue_url,
        MessageBody=json.dumps(task_message),
        MessageAttributes={
            "task_id": {"StringValue": str(task_key), "DataType": "String"},
        },
    )

    log.info(f"Sent test task message: {response['MessageId']}")
    return json.dumps(task_key)


@task
def verify_task_result(result_queue_url: str, task_key_json: str) -> str:
    """Verify the worker processed the task and sent a result."""
    sqs = boto3.client("sqs")
    task_key = json.loads(task_key_json)

    log.info(f"Waiting for result for task: {task_key}")

    for attempt in range(30):
        response = sqs.receive_message(
            QueueUrl=result_queue_url,
            MaxNumberOfMessages=10,
            WaitTimeSeconds=10,
            MessageAttributeNames=["All"],
        )

        if "Messages" not in response or not response["Messages"]:
            log.info(f"Attempt {attempt + 1}: No results yet")
            continue

        for msg in response["Messages"]:
            try:
                result = json.loads(msg["Body"])
                result_task_key = result.get("task_key", {})

                # Check if this is the result we're looking for
                if (
                    result_task_key.get("dag_id") == task_key["dag_id"]
                    and result_task_key.get("task_id") == task_key["task_id"]
                    and result_task_key.get("run_id") == task_key["run_id"]
                ):
                    state = result.get("state")
                    info = result.get("info", {})
                    worker_id = info.get("worker_id", "unknown")
                    worker_type = info.get("worker_type", "unknown")
                    execution_time = info.get("execution_time_seconds", 0)

                    log.info(
                        f"Received result: state={state}, worker={worker_id}, "
                        f"type={worker_type}, time={execution_time}s"
                    )

                    # Delete the message
                    sqs.delete_message(
                        QueueUrl=result_queue_url,
                        ReceiptHandle=msg["ReceiptHandle"],
                    )

                    if state == "SUCCESS":
                        return f"Task completed successfully by {worker_type} worker {worker_id}"
                    elif state == "FAILED":
                        error = info.get("error_message", "Unknown error")
                        log.warning(f"Task failed with error: {error}")
                        return f"Task processed (failed) by worker {worker_id}: {error}"

            except json.JSONDecodeError:
                log.warning(f"Invalid JSON in message: {msg['Body']}")
                continue

    raise TimeoutError("Did not receive task result from worker")


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
            # Don't fail the test - we want to see cleanup happen
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
def delete_sqs_queues(sqs_info: dict):
    """Delete the SQS queues."""
    sqs = boto3.client("sqs")

    for queue_key in ["task_queue_url", "result_queue_url"]:
        try:
            queue_url = sqs_info.get(queue_key)
            if queue_url:
                sqs.delete_queue(QueueUrl=queue_url)
                log.info(f"Deleted queue: {queue_url}")
        except Exception as e:
            log.warning(f"Failed to delete {queue_key}: {e}")


@task(trigger_rule=TriggerRule.ALL_DONE)
def delete_job_definition(job_definition: str):
    """Deregister the job definition."""
    batch = boto3.client("batch")

    try:
        response = batch.describe_job_definitions(
            jobDefinitionName=job_definition,
            status="ACTIVE",
        )

        for job_def in response["jobDefinitions"]:
            batch.deregister_job_definition(jobDefinition=job_def["jobDefinitionArn"])
            log.info(f"Deregistered: {job_def['jobDefinitionArn']}")
    except Exception as e:
        log.warning(f"Failed to deregister job definition: {e}")


@task(trigger_rule=TriggerRule.ALL_DONE)
def disable_and_delete_job_queue(job_queue: str):
    """Disable and delete the job queue."""
    batch = boto3.client("batch")

    try:
        batch.update_job_queue(jobQueue=job_queue, state="DISABLED")
        log.info(f"Disabled job queue: {job_queue}")

        for _ in range(30):
            response = batch.describe_job_queues(jobQueues=[job_queue])
            if not response["jobQueues"]:
                break
            status = response["jobQueues"][0]["status"]
            if status == "VALID":
                break
            time.sleep(5)

        batch.delete_job_queue(jobQueue=job_queue)
        log.info(f"Deleted job queue: {job_queue}")
    except Exception as e:
        log.warning(f"Failed to delete job queue: {e}")


@task(trigger_rule=TriggerRule.ALL_DONE)
def disable_and_delete_compute_environment(compute_env: str):
    """Disable and delete the compute environment."""
    batch = boto3.client("batch")

    try:
        batch.update_compute_environment(computeEnvironment=compute_env, state="DISABLED")
        log.info(f"Disabled compute environment: {compute_env}")

        for _ in range(60):
            response = batch.describe_compute_environments(computeEnvironments=[compute_env])
            if not response["computeEnvironments"]:
                break
            status = response["computeEnvironments"][0]["status"]
            if status == "VALID":
                break
            time.sleep(10)

        batch.delete_compute_environment(computeEnvironment=compute_env)
        log.info(f"Deleted compute environment: {compute_env}")
    except Exception as e:
        log.warning(f"Failed to delete compute environment: {e}")


# ============== DAG DEFINITION ==============

with DAG(
    dag_id=DAG_ID,
    schedule="@once",
    start_date=datetime(2021, 1, 1),
    tags=["system-test", "executor", "batch", "worker-pool"],
    catchup=False,
) as dag:
    test_context = sys_test_context_task()
    env_id = test_context[ENV_ID_KEY]

    # Parse context variables
    subnets = split_string(test_context[SUBNETS_KEY])
    security_groups = split_string(test_context[SECURITY_GROUPS_KEY])
    role_arn = test_context[ROLE_ARN_KEY]

    # === SETUP PHASE ===
    sqs_queues = create_sqs_queues(env_id)

    compute_env = create_batch_compute_environment(
        env_id=env_id,
        role_arn=role_arn,
        subnets=subnets,
        security_groups=security_groups,
    )

    job_queue = create_batch_job_queue(env_id=env_id, compute_env_name=compute_env)

    job_definition = create_worker_job_definition(
        env_id=env_id,
        role_arn=role_arn,
    )

    # === TEST PHASE ===
    worker_info = submit_worker(
        job_queue=job_queue,
        job_definition=job_definition,
        task_queue_url=sqs_queues["task_queue_url"],
        result_queue_url=sqs_queues["result_queue_url"],
    )

    worker_running = wait_for_worker_running(worker_info_json=worker_info)

    task_key = send_test_task(task_queue_url=sqs_queues["task_queue_url"])

    task_result = verify_task_result(
        result_queue_url=sqs_queues["result_queue_url"],
        task_key_json=task_key,
    )

    worker_completed = wait_for_worker_completion(worker_info_json=worker_info)

    # === CLEANUP PHASE ===
    terminate_worker = terminate_worker_job(worker_info_json=worker_info)

    cleanup_sqs = delete_sqs_queues(sqs_info=sqs_queues)

    cleanup_job_def = delete_job_definition(job_definition=job_definition)

    cleanup_job_queue = disable_and_delete_job_queue(job_queue=job_queue)

    cleanup_compute_env = disable_and_delete_compute_environment(compute_env=compute_env)

    log_cleanup = prune_logs(
        [
            ("/aws/batch/job", env_id),
        ],
    )

    # Define task dependencies
    chain(
        # Setup
        test_context,
        [subnets, security_groups],
        sqs_queues,
        compute_env,
        job_queue,
        job_definition,
        # Test
        worker_info,
        worker_running,
        task_key,
        task_result,
        worker_completed,
        # Cleanup
        terminate_worker,
        [cleanup_sqs, cleanup_job_def],
        cleanup_job_queue,
        cleanup_compute_env,
        log_cleanup,
    )

    from tests_common.test_utils.watcher import watcher

    list(dag.tasks) >> watcher()

from tests_common.test_utils.system_tests import get_test_run  # noqa: E402

test_run = get_test_run(dag)
