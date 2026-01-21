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

import json
import logging
import os
import uuid
from datetime import datetime, timezone
from pathlib import Path

from airflow.sdk import DAG, task
from airflow.models import Variable
from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook

log = logging.getLogger(__name__)

DAG_ID = "example_batch_worker_pool_executor"

# Path to test data file (same directory as DAG)
TEST_DATA_FILE = Path(__file__).parent / "pool_worker_test_data.json"

# Maximum number of tasks to generate (set to 0 or negative for all tasks)
MAX_TASKS = int(os.environ.get("AIRFLOW_WORKER_POOL_MAX_TASKS", "10"))

# AWS connection ID
AWS_CONN_ID = "aws_default"


def get_config(key: str, env_var: str) -> str | None:
    """Get configuration from Airflow Variable or environment variable."""
    try:
        return Variable.get(f"batch_worker_pool.{key}")
    except Exception:
        return os.environ.get(env_var)


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


@task
def upload_shared_state() -> dict:
    """Upload SharedState to S3 for workers to access.

    This task uploads the shared state configuration that workers will
    load at startup. The executor passes the S3 URI to workers.

    Returns:
        Dict with S3 location details
    """
    s3_uri = get_config("shared_state_s3_uri", "AIRFLOW_SHARED_STATE_S3_URI")
    if not s3_uri:
        raise ValueError("AIRFLOW_SHARED_STATE_S3_URI not configured")

    s3 = get_aws_client("s3")
    shared_state = get_shared_state()

    run_name = (
        shared_state.get("RiskRun", {}).get("RunId", {}).get("Name")
        or f"executor_run_{uuid.uuid4().hex[:8]}"
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


def create_grid_task(task_index: int, task_data: dict, run_name: str) -> callable:
    """Create a task function for a specific grid task.

    This factory function creates individual Airflow tasks that pass
    GridTask data to the .NET worker via executor_config.

    Args:
        task_index: Index of the task (for task_id naming)
        task_data: The task definition from pool_worker_test_data.json
        run_name: The run name for this execution

    Returns:
        A decorated task function
    """
    task_name = task_data.get("TaskName", f"task_{task_index}")

    @task(
        task_id=f"grid_task_{task_index}",
        # executor_config is passed to the worker via TaskQueueMessage
        # The .NET worker extracts GridTask from executor_config["GridTask"]
        executor_config={
            "GridTask": task_data,
            "run_name": run_name,
            "task_index": task_index,
        },
    )
    def grid_task(shared_state_info: dict) -> dict:
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


@task
def aggregate_results(results: list[dict], shared_state_info: dict) -> dict:
    """Aggregate results from all grid tasks.

    Args:
        results: List of results from all grid task executions
        shared_state_info: S3 location info for shared state

    Returns:
        Aggregated summary of all task results
    """
    total_tasks = len(results)
    completed = sum(1 for r in results if r.get("status") == "completed")
    failed = sum(1 for r in results if r.get("status") == "failed")

    summary = {
        "run_name": shared_state_info.get("run_name"),
        "total_tasks": total_tasks,
        "completed": completed,
        "failed": failed,
        "completed_at": datetime.now(timezone.utc).isoformat(),
    }

    log.info(f"Run {summary['run_name']} completed:")
    log.info(f"  Total tasks: {total_tasks}")
    log.info(f"  Completed: {completed}")
    log.info(f"  Failed: {failed}")

    return summary


# =============================================================================
# DAG DEFINITION
#
# This DAG dynamically generates one Airflow task per task record in
# pool_worker_test_data.json. Each task passes its GridTask data via
# executor_config, which the AwsBatchWorkerPoolExecutor includes in the
# SQS message for the .NET worker to process.
# =============================================================================

# Load task definitions at DAG parse time
TASK_DEFINITIONS = get_task_definitions()
SHARED_STATE = get_shared_state()
RUN_NAME = (
    SHARED_STATE.get("RiskRun", {}).get("RunId", {}).get("Name")
    or f"executor_run_{uuid.uuid4().hex[:8]}"
)

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
    # === SETUP: Upload shared state to S3 ===
    shared_state_info = upload_shared_state()

    # === EXECUTION: Create a task for each grid task definition ===
    # Each task passes GridTask data to the worker via executor_config
    grid_tasks = []
    for idx, task_def in enumerate(TASK_DEFINITIONS):
        grid_task_fn = create_grid_task(idx, task_def, RUN_NAME)
        grid_task_instance = grid_task_fn(shared_state_info=shared_state_info)
        grid_tasks.append(grid_task_instance)

    # === AGGREGATION: Collect results from all tasks ===
    if grid_tasks:
        summary = aggregate_results(
            results=grid_tasks,
            shared_state_info=shared_state_info,
        )
