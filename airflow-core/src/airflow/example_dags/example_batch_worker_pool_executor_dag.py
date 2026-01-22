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
Example DAG demonstrating the AwsBatchWorkerPoolExecutor architecture.

This DAG reads task definitions from pool_worker_test_data.json and dynamically
generates Airflow tasks that are executed on long-running AWS Batch workers
(including .NET workers) via the AwsBatchWorkerPoolExecutor.

Architecture Overview:
======================

┌─────────────────────────────────────────────────────────────────────────────┐
│                           Airflow Scheduler                                  │
│  ┌────────────────────────────────────────────────────────────────────────┐ │
│  │              AwsBatchWorkerPoolExecutor                                 │ │
│  │                                                                          │ │
│  │  1. Receives tasks from scheduler                                        │ │
│  │  2. Sends TaskQueueMessage to SQS Task Queue                            │ │
│  │     - task_key: identifies the Airflow task                             │ │
│  │     - executor_config: contains GridTask data for .NET worker           │ │
│  │  3. Spawns/scales AWS Batch workers as needed                           │ │
│  │  4. Polls SQS Result Queue for TaskResultMessage                        │ │
│  │  5. Reports task state back to scheduler                                │ │
│  └────────────────────────────────────────────────────────────────────────┘ │
└─────────────────────────────────────────────────────────────────────────────┘
                                    │
                                    │ SQS Messages (shared schema)
                                    v
┌──────────────────────────────────────────────────────────────────────────────┐
│                    AWS Batch Worker Pool (.NET or Python)                    │
│                                                                               │
│   ┌─────────────────────────────────────────────────────────────────────┐    │
│   │  Long-Running Worker Containers                                      │    │
│   │                                                                      │    │
│   │  - Workers poll SQS Task Queue for TaskQueueMessage                  │    │
│   │  - Extract GridTask from executor_config["GridTask"]                 │    │
│   │  - Load shared state from S3 (cached in parent process)              │    │
│   │  - Execute task in isolated subprocess                               │    │
│   │  - Send TaskResultMessage to SQS Result Queue                        │    │
│   │  - Self-terminate after idle timeout                                 │    │
│   └─────────────────────────────────────────────────────────────────────┘    │
└───────────────────────────────────────────────────────────────────────────────┘

Key Differences from example_batch_worker_pool_dag.py:
======================================================

| Aspect              | example_batch_worker_pool_dag.py | This DAG (executor-based)      |
|---------------------|----------------------------------|--------------------------------|
| SQS calls           | Explicit send_message/receive    | None - executor handles        |
| Batch job submit    | Explicit submit_job              | None - executor handles        |
| Task generation     | Single task sends all to SQS     | Dynamic Airflow @task mapping  |
| Task tracking       | Manual polling of result queue   | Executor handles automatically |
| Worker management   | Manual submit/wait/cleanup       | Executor scales automatically  |
| Task data passing   | JSON in SQS message body         | executor_config on @task       |

Prerequisites:
==============

1. Configure the executor in airflow.cfg or via environment variables:

   [core]
   executor = airflow.providers.amazon.aws.executors.batch.AwsBatchWorkerPoolExecutor

   [aws_batch_worker_pool_executor]
   conn_id = aws_default
   region_name = us-east-1
   job_queue = airflow-worker-pool-queue
   job_definition = airflow-worker-pool:1
   task_queue_url = https://sqs.us-east-1.amazonaws.com/123456789012/airflow-tasks
   result_queue_url = https://sqs.us-east-1.amazonaws.com/123456789012/airflow-results
   worker_idle_timeout_seconds = 300

2. Place pool_worker_test_data.json in the tests/ subdirectory

3. Configure aws_default connection in Airflow UI

4. The job_definition should point to your .NET worker container image
"""
from __future__ import annotations

import hashlib
import json
import logging
import os
import uuid
from datetime import datetime
from pathlib import Path

from botocore.exceptions import ClientError

from airflow.sdk import DAG, task
from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook
from airflow.operators.empty import EmptyOperator

log = logging.getLogger(__name__)

DAG_ID = "example_batch_worker_pool_executor_2"

# Path to test data file (same directory as DAG)
TEST_DATA_FILE = Path(__file__).parent / "pool_worker_test_data.json"

# Maximum number of tasks to generate (set to 0 or negative for all tasks)
MAX_TASKS = int(os.environ.get("AIRFLOW_WORKER_POOL_MAX_TASKS", "10"))

# AWS connection ID
AWS_CONN_ID = "aws_default"


def load_test_data() -> dict:
    """Load test data from pool_worker_test_data.json."""
    if not TEST_DATA_FILE.exists():
        log.warning(f"Test data file not found: {TEST_DATA_FILE}")
        return {"SharedState": {}, "Tasks": []}

    with open(TEST_DATA_FILE) as f:
        return json.load(f)


def get_task_definitions() -> list[dict]:
    """Get task definitions from test data file.

    Returns a list of task definitions, limited by MAX_TASKS.
    """
    test_data = load_test_data()
    tasks = test_data.get("Tasks", [])

    if MAX_TASKS and MAX_TASKS > 0:
        tasks = tasks[:MAX_TASKS]

    return tasks


def get_shared_state() -> dict:
    """Get shared state configuration from test data file."""
    test_data = load_test_data()
    return test_data.get("SharedState", {})


def get_aws_client(service_name: str):
    """Get a boto3 client using Airflow's AWS connection."""
    hook = AwsBaseHook(aws_conn_id=AWS_CONN_ID, client_type=service_name)
    return hook.get_conn()


def parse_s3_uri(s3_uri: str) -> tuple[str, str]:
    """Parse S3 URI into bucket and key prefix."""
    if not s3_uri.startswith("s3://"):
        raise ValueError(f"Invalid S3 URI: {s3_uri}")
    path = s3_uri[5:]  # Remove 's3://'
    parts = path.split("/", 1)
    bucket = parts[0]
    prefix = parts[1] if len(parts) > 1 else ""
    return bucket, prefix


def compute_content_hash(data: dict) -> str:
    """Compute MD5 hash of JSON content for deduplication."""
    content = json.dumps(data, sort_keys=True, separators=(",", ":"))
    return hashlib.md5(content.encode()).hexdigest()[:16]


def extract_run_name(shared_state: dict) -> str:
    """Extract run name from shared state or generate one."""
    return (
        shared_state.get("RiskRun", {}).get("RunId", {}).get("Name")
        or f"executor_run_{uuid.uuid4().hex[:8]}"
    )


def upload_shared_state_at_parse_time() -> dict:
    """Upload shared state to S3 during DAG parse time.

    Uses S3 object metadata to store content hash for idempotency. Compares
    the hash of current content against the stored metadata to avoid redundant
    uploads on repeated DAG parses.

    The filename is kept consistent (based on run_name only, no hash), so
    downstream workers don't need to handle changing filenames.

    Note: Uses boto3 directly (not AwsBaseHook) to avoid database access
    at parse time. Relies on default AWS credentials (env vars, IAM role, etc).

    Returns:
        Dict with S3 location info, or error info if upload fails
    """
    s3_uri = os.environ.get("AIRFLOW_SHARED_STATE_S3_URI")
    if not s3_uri:
        log.warning("AIRFLOW_SHARED_STATE_S3_URI not configured - shared state will not be uploaded")
        return {"error": "AIRFLOW_SHARED_STATE_S3_URI not configured"}

    shared_state = get_shared_state()
    if not shared_state:
        log.warning("No shared state found in test data")
        return {"error": "No shared state found"}

    content_hash = compute_content_hash(shared_state)
    bucket, prefix = parse_s3_uri(s3_uri)
    run_name = extract_run_name(shared_state)
    # Use consistent filename without hash - hash is stored in metadata
    shared_state_key = f"{prefix}{run_name}.json".lstrip("/")

    try:
        # Use boto3 directly to avoid database access at parse time
        import boto3

        s3 = boto3.client("s3")

        # Check if object already exists and has matching content hash in metadata
        needs_upload = True
        try:
            response = s3.head_object(Bucket=bucket, Key=shared_state_key)
            existing_hash = response.get("Metadata", {}).get("content-hash")
            if existing_hash == content_hash:
                log.debug(f"Shared state unchanged (hash={content_hash}): s3://{bucket}/{shared_state_key}")
                needs_upload = False
            else:
                log.info(f"Shared state changed (old={existing_hash}, new={content_hash}), re-uploading")
        except ClientError as e:
            if e.response["Error"]["Code"] == "404":
                log.debug(f"Shared state does not exist, will upload: s3://{bucket}/{shared_state_key}")
            else:
                raise

        if needs_upload:
            s3.put_object(
                Bucket=bucket,
                Key=shared_state_key,
                Body=json.dumps(shared_state, indent=2),
                ContentType="application/json",
                Metadata={"content-hash": content_hash},
            )
            log.info(f"Uploaded shared state to s3://{bucket}/{shared_state_key} (hash={content_hash})")

        return {
            "bucket": bucket,
            "prefix": prefix,
            "shared_state_key": shared_state_key,
            "s3_uri": s3_uri,
            "run_name": run_name,
            "content_hash": content_hash,
        }

    except Exception as e:
        log.warning(f"Failed to upload shared state at parse time: {e}")
        return {"error": str(e), "s3_uri": s3_uri, "run_name": run_name}


# =============================================================================
# TASK DEFINITIONS
#
# These tasks use executor_config to pass data to the .NET worker.
# When using AwsBatchWorkerPoolExecutor:
# 1. Executor serializes the task and sends TaskQueueMessage to SQS
# 2. The executor_config dict is included in the message
# 3. .NET worker extracts GridTask from executor_config["GridTask"]
# 4. Worker processes the task and sends TaskResultMessage
# 5. Executor polls results and updates Airflow task state
# =============================================================================


def create_grid_task(
    task_index: int, task_data: dict, run_name: str, shared_state_info: dict
) -> callable:
    """Create a task function for a specific grid task.

    This factory function creates individual Airflow tasks that pass
    GridTask data to the .NET worker via executor_config.

    Args:
        task_index: Index of the task (for task_id naming)
        task_data: The task definition from pool_worker_test_data.json
        run_name: The run name for this execution
        shared_state_info: S3 location info for shared state (uploaded at parse time)

    Returns:
        A decorated task function
    """
    task_name = task_data.get("TaskName", f"task_{task_index}")

    @task(
        task_id=task_name,
        # executor_config is passed to the worker via TaskQueueMessage
        # The .NET worker extracts GridTask from executor_config["GridTask"]
        executor_config={
            "GridTask": task_data,
            "run_name": run_name,
            "task_index": task_index,
            "shared_state_info": shared_state_info,
        },
    )
    def grid_task() -> dict:
        """Process a grid task on the worker pool.

        When executed via AwsBatchWorkerPoolExecutor:
        - This function's executor_config is sent to the worker
        - The .NET worker extracts GridTask and processes it
        - Results are reported back via SQS

        When executed locally (without the executor):
        - This function runs as a placeholder, logging the task info
        """
        log.info(f"Grid task {task_name} (index {task_index})")
        log.info(f"  SharedState S3: {shared_state_info.get('s3_uri')}")
        log.info(f"  Run name: {shared_state_info.get('run_name')}")

        # When running locally (not via executor), just return placeholder result
        # The actual processing happens in the .NET worker
        return {
            "task_name": task_name,
            "task_index": task_index,
            "status": "completed",
            "run_name": shared_state_info.get("run_name"),
        }

    return grid_task


# =============================================================================
# DAG DEFINITION
#
# This DAG dynamically generates one Airflow task per task record in
# pool_worker_test_data.json. Each task passes its GridTask data via
# executor_config, which the AwsBatchWorkerPoolExecutor includes in the
# SQS message for the .NET worker to process.
#
# Shared state is uploaded to S3 at DAG parse time (not as a task), so
# grid tasks can start immediately without waiting for an upload task.
# =============================================================================

# Load task definitions and upload shared state at DAG parse time
# Wrapped in try/except to ensure DAG still loads even if there are issues
try:
    TASK_DEFINITIONS = get_task_definitions()
    SHARED_STATE = get_shared_state()
    RUN_NAME = extract_run_name(SHARED_STATE)
    SHARED_STATE_INFO = upload_shared_state_at_parse_time()
except Exception as e:
    log.error(f"Error during DAG parse-time initialization: {e}")
    TASK_DEFINITIONS = []
    SHARED_STATE = {}
    RUN_NAME = "error"
    SHARED_STATE_INFO = {"error": str(e)}

with DAG(
    dag_id=DAG_ID,
    schedule=None,  # Manual trigger only
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["example", "aws", "batch", "worker-pool", "executor", "dynamic"],
    doc_md=__doc__,
    default_args={
        "retries": 1,
    },
) as dag:

    # Placeholder start task to ensure DAG is valid even with no grid tasks
    start_task = EmptyOperator(task_id="start")

    # === EXECUTION: Create a task for each grid task definition ===
    # Each task passes GridTask data to the worker via executor_config
    # Shared state info is embedded in executor_config (uploaded at parse time)
    for idx, task_def in enumerate(TASK_DEFINITIONS):
        grid_task_fn = create_grid_task(idx, task_def, RUN_NAME, SHARED_STATE_INFO)
        start_task >> grid_task_fn()
