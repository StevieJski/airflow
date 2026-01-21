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

This DAG shows how normal Airflow tasks are automatically executed on
long-running AWS Batch worker processes when using the AwsBatchWorkerPoolExecutor.

Architecture Overview:
======================

┌─────────────────────────────────────────────────────────────────────────────┐
│                           Airflow Scheduler                                  │
│  ┌────────────────────────────────────────────────────────────────────────┐ │
│  │              AwsBatchWorkerPoolExecutor                                 │ │
│  │                                                                          │ │
│  │  1. Receives tasks from scheduler                                        │ │
│  │  2. Sends task messages to SQS Task Queue                               │ │
│  │  3. Spawns/scales AWS Batch workers as needed                           │ │
│  │  4. Polls SQS Result Queue for completions                              │ │
│  │  5. Reports task state back to scheduler                                │ │
│  └────────────────────────────────────────────────────────────────────────┘ │
└─────────────────────────────────────────────────────────────────────────────┘
                                    │
                                    │ Tasks auto-routed via SQS
                                    v
┌──────────────────────────────────────────────────────────────────────────────┐
│                         AWS Batch Worker Pool                                │
│                                                                               │
│   ┌─────────────────────────────────────────────────────────────────────┐    │
│   │  Long-Running Worker Containers                                      │    │
│   │                                                                      │    │
│   │  - Workers poll SQS Task Queue                                       │    │
│   │  - Execute tasks in isolated subprocesses                            │    │
│   │  - Send results to SQS Result Queue                                  │    │
│   │  - Self-terminate after idle timeout                                 │    │
│   │  - Can share state (models, connections) across tasks                │    │
│   └─────────────────────────────────────────────────────────────────────┘    │
└───────────────────────────────────────────────────────────────────────────────┘

Key Benefits:
- DAG code contains NO explicit SQS or Batch API calls
- Tasks are just normal Python functions with @task decorators
- Executor handles all infrastructure orchestration
- Workers can share expensive resources (ML models, DB pools) across tasks
- Automatic scaling based on task queue depth

Prerequisites:
==============

1. Configure the executor in airflow.cfg:

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
   shared_state_s3_uri = s3://my-bucket/airflow/shared-state/

2. Ensure AWS infrastructure exists:
   - AWS Batch job queue and compute environment
   - Job definition with worker container image
   - SQS queues for task and result messages
   - S3 bucket for shared state (optional)

3. Configure aws_default connection in Airflow UI

Usage:
======

Simply trigger this DAG from the Airflow UI. The executor will:
1. Queue tasks to SQS
2. Spawn workers on AWS Batch
3. Workers execute tasks and report results
4. Executor marks tasks as success/failed

The DAG code itself is just normal Airflow task definitions!
"""
from __future__ import annotations

import logging
import time
from datetime import datetime

from airflow.sdk import DAG, task

log = logging.getLogger(__name__)

DAG_ID = "example_batch_worker_pool_executor"


# =============================================================================
# EXAMPLE TASKS
#
# These are regular Airflow tasks. When using AwsBatchWorkerPoolExecutor,
# these tasks are automatically:
# 1. Serialized and sent to SQS by the executor
# 2. Picked up by long-running AWS Batch workers
# 3. Executed in isolated subprocess on the worker
# 4. Results reported back via SQS result queue
#
# NO explicit SQS or Batch code needed in the DAG!
# =============================================================================


@task
def extract_data() -> dict:
    """Extract data from a source.

    This task simulates extracting data. When executed on a worker pool,
    the worker can maintain persistent connections to data sources.
    """
    log.info("Extracting data from source...")

    # Simulate data extraction
    data = {
        "records": [
            {"id": 1, "name": "Alice", "value": 100},
            {"id": 2, "name": "Bob", "value": 200},
            {"id": 3, "name": "Charlie", "value": 300},
        ],
        "source": "example_database",
        "extracted_at": datetime.utcnow().isoformat(),
    }

    log.info(f"Extracted {len(data['records'])} records")
    return data


@task
def transform_data(raw_data: dict) -> dict:
    """Transform the extracted data.

    This task processes the data. On a worker pool, expensive resources
    like ML models can be loaded once and reused across multiple tasks.
    """
    log.info("Transforming data...")

    records = raw_data.get("records", [])
    transformed = []

    for record in records:
        transformed.append({
            "id": record["id"],
            "name": record["name"].upper(),
            "value": record["value"] * 2,
            "category": "high" if record["value"] > 150 else "low",
        })

    result = {
        "records": transformed,
        "source": raw_data.get("source"),
        "transformed_at": datetime.utcnow().isoformat(),
        "record_count": len(transformed),
    }

    log.info(f"Transformed {len(transformed)} records")
    return result


@task
def load_data(transformed_data: dict) -> str:
    """Load transformed data to destination.

    This task writes data to a destination. Worker pools can maintain
    connection pools for efficient database writes.
    """
    log.info("Loading data to destination...")

    record_count = transformed_data.get("record_count", 0)

    # Simulate loading data
    time.sleep(1)

    log.info(f"Loaded {record_count} records to destination")

    return f"Successfully loaded {record_count} records"


@task
def validate_results(load_status: str, transformed_data: dict) -> dict:
    """Validate the ETL pipeline results.

    This task performs validation checks on the pipeline output.
    """
    log.info("Validating results...")

    validation = {
        "status": "passed",
        "checks": [],
        "validated_at": datetime.utcnow().isoformat(),
    }

    # Check 1: Load was successful
    if "Successfully" in load_status:
        validation["checks"].append({"check": "load_success", "passed": True})
    else:
        validation["checks"].append({"check": "load_success", "passed": False})
        validation["status"] = "failed"

    # Check 2: Records were processed
    record_count = transformed_data.get("record_count", 0)
    if record_count > 0:
        validation["checks"].append({"check": "records_processed", "passed": True, "count": record_count})
    else:
        validation["checks"].append({"check": "records_processed", "passed": False})
        validation["status"] = "failed"

    log.info(f"Validation {validation['status']}: {len(validation['checks'])} checks completed")

    return validation


@task
def notify_completion(validation: dict) -> str:
    """Send notification about pipeline completion.

    This is the final task that reports the pipeline outcome.
    """
    status = validation.get("status", "unknown")

    if status == "passed":
        message = "ETL pipeline completed successfully!"
    else:
        message = "ETL pipeline completed with validation failures"

    log.info(f"Pipeline notification: {message}")

    return message


# =============================================================================
# DAG DEFINITION
#
# This is a standard Airflow DAG with TaskFlow API.
# The AwsBatchWorkerPoolExecutor handles all the infrastructure.
# =============================================================================

with DAG(
    dag_id=DAG_ID,
    schedule=None,  # Manual trigger only
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["example", "aws", "batch", "worker-pool", "executor"],
    doc_md=__doc__,
    default_args={
        "retries": 1,
    },
) as dag:
    # Define the ETL pipeline
    raw = extract_data()
    transformed = transform_data(raw)
    loaded = load_data(transformed)
    validated = validate_results(loaded, transformed)
    notify = notify_completion(validated)

    # Dependencies are automatically inferred from task inputs/outputs
    # raw >> transformed >> loaded >> validated >> notify
