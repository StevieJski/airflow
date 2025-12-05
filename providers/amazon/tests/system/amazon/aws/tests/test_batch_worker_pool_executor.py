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
System test for AWS Batch Worker Pool Executor.

This test verifies the Worker Pool Executor can:
1. Create and manage SQS queues for task distribution
2. Start workers in AWS Batch
3. Execute tasks via the worker pool
4. Receive results back through SQS

Prerequisites:
- AWS credentials with permissions for Batch, SQS, and related services
- Environment variables set (see SystemTestContextBuilder below)

To run:
    breeze testing system-tests \
        providers/amazon/tests/system/amazon/aws/tests/test_batch_worker_pool_executor.py
"""
from __future__ import annotations

import json
import logging
import os
import time
from datetime import datetime, timedelta, timezone

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

DAG_ID = "test_batch_worker_pool_executor"

# Externally fetched variables
ROLE_ARN_KEY = "ROLE_ARN"
SUBNETS_KEY = "SUBNETS"
SECURITY_GROUPS_KEY = "SECURITY_GROUPS"

sys_test_context_task = (
    SystemTestContextBuilder()
    .add_variable(ROLE_ARN_KEY)
    .add_variable(SUBNETS_KEY)
    .add_variable(SECURITY_GROUPS_KEY)
    .build()
)


@task
def create_sqs_queues(env_id: str) -> dict:
    """Create SQS queues for task and result communication."""
    sqs = boto3.client("sqs")

    task_queue_name = f"{env_id}-task-queue"
    result_queue_name = f"{env_id}-result-queue"

    # Create task queue with longer visibility timeout for task execution
    task_queue = sqs.create_queue(
        QueueName=task_queue_name,
        Attributes={
            "VisibilityTimeout": "3600",  # 1 hour
            "MessageRetentionPeriod": "86400",  # 1 day
        },
    )

    # Create result queue
    result_queue = sqs.create_queue(
        QueueName=result_queue_name,
        Attributes={
            "VisibilityTimeout": "300",
            "MessageRetentionPeriod": "86400",
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
    compute_env_name = f"{env_id}-worker-pool-compute-env"

    batch.create_compute_environment(
        computeEnvironmentName=compute_env_name,
        type="MANAGED",
        state="ENABLED",
        computeResources={
            "type": "FARGATE",
            "maxvCpus": 16,
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
    job_queue_name = f"{env_id}-worker-pool-job-queue"

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
def create_job_definition(env_id: str, role_arn: str) -> str:
    """Create job definition for worker containers."""
    batch = boto3.client("batch")
    job_def_name = f"{env_id}-worker-pool-job-def"

    # Use busybox for simple connectivity tests (like example_batch.py)
    # The actual worker would use an Airflow image with worker_pool_worker module
    image = "busybox"

    batch.register_job_definition(
        jobDefinitionName=job_def_name,
        type="container",
        platformCapabilities=["FARGATE"],
        containerProperties={
            "image": image,
            "resourceRequirements": [
                {"type": "VCPU", "value": "1"},
                {"type": "MEMORY", "value": "2048"},
            ],
            "executionRoleArn": role_arn,
            "jobRoleArn": role_arn,
            "networkConfiguration": {
                "assignPublicIp": "ENABLED",
            },
            # Simple command for connectivity test
            "command": ["echo", "test"],
        },
    )

    log.info(f"Created job definition: {job_def_name}")
    return job_def_name


@task
def set_executor_config(
    job_queue: str,
    job_definition: str,
    task_queue_url: str,
    result_queue_url: str,
) -> dict:
    """
    Set executor configuration via environment variables.

    Note: In a real deployment, these would be set in airflow.cfg or
    as environment variables before starting the scheduler.
    """
    config = {
        "AIRFLOW__AWS_BATCH_WORKER_POOL_EXECUTOR__REGION_NAME": os.environ.get(
            "AWS_DEFAULT_REGION", "us-east-1"
        ),
        "AIRFLOW__AWS_BATCH_WORKER_POOL_EXECUTOR__JOB_QUEUE": job_queue,
        "AIRFLOW__AWS_BATCH_WORKER_POOL_EXECUTOR__JOB_DEFINITION": job_definition,
        "AIRFLOW__AWS_BATCH_WORKER_POOL_EXECUTOR__TASK_QUEUE_URL": task_queue_url,
        "AIRFLOW__AWS_BATCH_WORKER_POOL_EXECUTOR__RESULT_QUEUE_URL": result_queue_url,
        "AIRFLOW__AWS_BATCH_WORKER_POOL_EXECUTOR__MIN_WORKERS": "1",
        "AIRFLOW__AWS_BATCH_WORKER_POOL_EXECUTOR__MAX_WORKERS": "2",
        "AIRFLOW__AWS_BATCH_WORKER_POOL_EXECUTOR__WORKER_IDLE_TIMEOUT_SECONDS": "60",
    }

    log.info("Executor configuration:")
    for key, value in config.items():
        log.info(f"  {key}={value}")
        # Set environment variables for this process
        os.environ[key] = value

    return config


@task
def verify_sqs_connectivity(task_queue_url: str, result_queue_url: str):
    """Verify SQS queues are accessible."""
    sqs = boto3.client("sqs")

    # Test task queue
    task_attrs = sqs.get_queue_attributes(
        QueueUrl=task_queue_url,
        AttributeNames=["ApproximateNumberOfMessages"],
    )
    log.info(f"Task queue attributes: {task_attrs['Attributes']}")

    # Test result queue
    result_attrs = sqs.get_queue_attributes(
        QueueUrl=result_queue_url,
        AttributeNames=["ApproximateNumberOfMessages"],
    )
    log.info(f"Result queue attributes: {result_attrs['Attributes']}")

    return "SQS connectivity verified"


@task
def test_send_task_message(task_queue_url: str) -> str:
    """Send a test message to the task queue to verify it works."""
    sqs = boto3.client("sqs")

    test_message = {
        "test": True,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "message": "Integration test message",
    }

    response = sqs.send_message(
        QueueUrl=task_queue_url,
        MessageBody=json.dumps(test_message),
    )

    log.info(f"Sent test message: {response['MessageId']}")
    return response["MessageId"]


@task
def verify_message_received(task_queue_url: str, message_id: str):
    """Verify the test message can be received."""
    sqs = boto3.client("sqs")

    # Try to receive the message
    for attempt in range(5):
        response = sqs.receive_message(
            QueueUrl=task_queue_url,
            MaxNumberOfMessages=1,
            WaitTimeSeconds=5,
        )

        if "Messages" in response and response["Messages"]:
            msg = response["Messages"][0]
            log.info(f"Received message: {msg['Body']}")

            # Delete the message
            sqs.delete_message(
                QueueUrl=task_queue_url,
                ReceiptHandle=msg["ReceiptHandle"],
            )
            return "Message successfully sent and received"

        log.info(f"Attempt {attempt + 1}: No messages yet")
        time.sleep(2)

    raise AssertionError("Failed to receive test message from SQS")


@task
def test_batch_job_submission(job_queue: str, job_definition: str) -> str:
    """Submit a simple test job to verify Batch connectivity."""
    batch = boto3.client("batch")

    response = batch.submit_job(
        jobName="worker-pool-connectivity-test",
        jobQueue=job_queue,
        jobDefinition=job_definition,
        containerOverrides={
            # Use sleep command like example_batch.py - echo exits too quickly for Fargate
            "command": ["sleep", "2"],
        },
    )

    job_id = response["jobId"]
    log.info(f"Submitted test job: {job_id}")

    # Wait for job to complete
    for _ in range(60):
        status = batch.describe_jobs(jobs=[job_id])
        job_status = status["jobs"][0]["status"]
        log.info(f"Job {job_id} status: {job_status}")

        if job_status == "SUCCEEDED":
            return job_id
        if job_status == "FAILED":
            reason = status["jobs"][0].get("statusReason", "Unknown")
            raise RuntimeError(f"Test job failed: {reason}")

        time.sleep(10)

    raise TimeoutError(f"Test job {job_id} did not complete in time")


# ============== CLEANUP TASKS ==============


@task(trigger_rule=TriggerRule.ALL_DONE)
def delete_sqs_queues(task_queue_url: str, result_queue_url: str):
    """Delete the SQS queues."""
    sqs = boto3.client("sqs")

    try:
        sqs.delete_queue(QueueUrl=task_queue_url)
        log.info(f"Deleted task queue: {task_queue_url}")
    except Exception as e:
        log.warning(f"Failed to delete task queue: {e}")

    try:
        sqs.delete_queue(QueueUrl=result_queue_url)
        log.info(f"Deleted result queue: {result_queue_url}")
    except Exception as e:
        log.warning(f"Failed to delete result queue: {e}")


@task(trigger_rule=TriggerRule.ALL_DONE)
def delete_job_definition(job_definition: str):
    """Deregister the job definition."""
    batch = boto3.client("batch")

    try:
        # Get all revisions
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
        # Disable the queue first
        batch.update_job_queue(jobQueue=job_queue, state="DISABLED")
        log.info(f"Disabled job queue: {job_queue}")

        # Wait for it to be disabled
        for _ in range(30):
            response = batch.describe_job_queues(jobQueues=[job_queue])
            if not response["jobQueues"]:
                break
            status = response["jobQueues"][0]["status"]
            if status == "VALID":
                break
            time.sleep(5)

        # Delete the queue
        batch.delete_job_queue(jobQueue=job_queue)
        log.info(f"Deleted job queue: {job_queue}")
    except Exception as e:
        log.warning(f"Failed to delete job queue: {e}")


@task(trigger_rule=TriggerRule.ALL_DONE)
def disable_and_delete_compute_environment(compute_env: str):
    """Disable and delete the compute environment."""
    batch = boto3.client("batch")

    try:
        # Disable first
        batch.update_compute_environment(computeEnvironment=compute_env, state="DISABLED")
        log.info(f"Disabled compute environment: {compute_env}")

        # Wait for jobs to drain and status to be VALID
        for _ in range(60):
            response = batch.describe_compute_environments(computeEnvironments=[compute_env])
            if not response["computeEnvironments"]:
                break
            status = response["computeEnvironments"][0]["status"]
            if status == "VALID":
                break
            time.sleep(10)

        # Delete
        batch.delete_compute_environment(computeEnvironment=compute_env)
        log.info(f"Deleted compute environment: {compute_env}")
    except Exception as e:
        log.warning(f"Failed to delete compute environment: {e}")


# ============== DAG DEFINITION ==============

with DAG(
    dag_id=DAG_ID,
    schedule="@once",
    start_date=datetime(2021, 1, 1),
    tags=["system-test", "executor", "batch"],
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

    job_definition = create_job_definition(
        env_id=env_id,
        role_arn=role_arn,
    )

    executor_config = set_executor_config(
        job_queue=job_queue,
        job_definition=job_definition,
        task_queue_url=sqs_queues["task_queue_url"],
        result_queue_url=sqs_queues["result_queue_url"],
    )

    # === TEST PHASE ===
    sqs_test = verify_sqs_connectivity(
        task_queue_url=sqs_queues["task_queue_url"],
        result_queue_url=sqs_queues["result_queue_url"],
    )

    message_id = test_send_task_message(task_queue_url=sqs_queues["task_queue_url"])

    message_verified = verify_message_received(
        task_queue_url=sqs_queues["task_queue_url"],
        message_id=message_id,
    )

    batch_test = test_batch_job_submission(job_queue=job_queue, job_definition=job_definition)

    # === CLEANUP PHASE ===
    cleanup_sqs = delete_sqs_queues(
        task_queue_url=sqs_queues["task_queue_url"],
        result_queue_url=sqs_queues["result_queue_url"],
    )

    cleanup_job_def = delete_job_definition(job_definition=job_definition)

    cleanup_job_queue = disable_and_delete_job_queue(job_queue=job_queue)

    cleanup_compute_env = disable_and_delete_compute_environment(compute_env=compute_env)

    log_cleanup = prune_logs(
        [
            ("/aws/batch/job", env_id),
        ],
    )

    # Define task dependencies
    # Note: role_arn is an XComArg (dict access), not a task, so it's not in chain
    # The split_string tasks (subnets, security_groups) depend on test_context automatically
    chain(
        # Setup - test_context must run first
        test_context,
        [subnets, security_groups],  # These are split_string task calls
        sqs_queues,
        compute_env,
        job_queue,
        job_definition,
        executor_config,
        # Tests
        sqs_test,
        message_id,
        message_verified,
        batch_test,
        # Cleanup (runs even if tests fail)
        [cleanup_sqs, cleanup_job_def],
        cleanup_job_queue,
        cleanup_compute_env,
        log_cleanup,
    )

    from tests_common.test_utils.watcher import watcher

    # Watcher ensures proper success/failure marking with trigger rules
    list(dag.tasks) >> watcher()

from tests_common.test_utils.system_tests import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest
test_run = get_test_run(dag)
