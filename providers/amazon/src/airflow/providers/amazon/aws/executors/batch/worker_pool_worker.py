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
Long-running worker process for AWS Batch Worker Pool Executor.

This process runs inside AWS Batch containers and:
1. Initializes shared state (ML models, DB pools, etc.)
2. Polls SQS task queue for work
3. Forks a subprocess for each task (isolation)
4. Reports results to SQS result queue
5. Self-terminates after idle timeout
"""

from __future__ import annotations

import argparse
import importlib
import logging
import os
import signal
import sys
import time
import uuid
from datetime import datetime
from multiprocessing import Process, Queue
from typing import Any

import boto3

from airflow.providers.amazon.aws.executors.batch.worker_pool_schemas import (
    TaskInstanceKeySchema,
    TaskQueueMessage,
    TaskResultInfo,
    TaskResultMessage,
)

try:
    from airflow.sdk import timezone
except ImportError:
    from airflow.utils import timezone  # type: ignore[attr-defined,no-redef]

log = logging.getLogger(__name__)

# Global shared state (initialized once, available to forked tasks)
SHARED_STATE: dict[str, Any] = {}


class WorkerProcess:
    """Long-running worker that polls SQS and executes tasks in subprocesses."""

    def __init__(
        self,
        worker_id: str,
        task_queue_url: str,
        result_queue_url: str,
        idle_timeout_seconds: int = 300,
        visibility_timeout_seconds: int = 3600,
        max_tasks: int = 0,
        init_module: str | None = None,
        init_function: str = "initialize",
    ):
        self.worker_id = worker_id
        self.task_queue_url = task_queue_url
        self.result_queue_url = result_queue_url
        self.idle_timeout_seconds = idle_timeout_seconds
        self.visibility_timeout_seconds = visibility_timeout_seconds
        self.max_tasks = max_tasks  # 0 = unlimited
        self.init_module = init_module
        self.init_function = init_function

        # AWS clients
        self.sqs = boto3.client("sqs")

        # State
        self.running = True
        self.tasks_executed = 0
        self.last_task_time = timezone.utcnow()
        self.current_receipt_handle: str | None = None

        # Get Batch job ID from environment
        self.batch_job_id = os.environ.get("AWS_BATCH_JOB_ID", "unknown")

        # Signal handlers
        signal.signal(signal.SIGTERM, self._handle_sigterm)
        signal.signal(signal.SIGINT, self._handle_sigterm)

    def _handle_sigterm(self, signum, frame):
        """Handle termination signal - finish current task, then exit."""
        log.info("Received signal %d, will exit after current task", signum)
        self.running = False

        # If we have a message in-flight, return it to queue
        if self.current_receipt_handle:
            try:
                self.sqs.change_message_visibility(
                    QueueUrl=self.task_queue_url,
                    ReceiptHandle=self.current_receipt_handle,
                    VisibilityTimeout=0,  # Immediate return to queue
                )
                log.info("Returned in-flight message to queue")
            except Exception:
                log.exception("Failed to return message to queue")

    def run(self):
        """Main worker loop."""
        log.info("Worker %s starting (Batch job: %s)", self.worker_id, self.batch_job_id)

        # Initialize shared state
        self._initialize_shared_state()

        log.info("Worker %s entering main loop", self.worker_id)

        while self.running:
            # Check idle timeout
            if self._should_terminate_on_idle():
                log.info(
                    "Worker %s idle for %d seconds, terminating",
                    self.worker_id,
                    self.idle_timeout_seconds,
                )
                break

            # Check task limit
            if self.max_tasks > 0 and self.tasks_executed >= self.max_tasks:
                log.info(
                    "Worker %s reached max tasks (%d), terminating",
                    self.worker_id,
                    self.max_tasks,
                )
                break

            # Poll for task
            task_message = self._poll_for_task()
            if task_message is None:
                continue

            # Execute task in subprocess
            self._execute_task(task_message)

        log.info(
            "Worker %s shutting down (executed %d tasks)",
            self.worker_id,
            self.tasks_executed,
        )

    def _initialize_shared_state(self):
        """Initialize expensive shared resources."""
        global SHARED_STATE

        if not self.init_module:
            log.info("No shared state initialization configured")
            return

        log.info("Initializing shared state from %s.%s", self.init_module, self.init_function)

        try:
            module = importlib.import_module(self.init_module)
            init_func = getattr(module, self.init_function)
            SHARED_STATE = init_func() or {}
            log.info("Shared state initialized: %s", list(SHARED_STATE.keys()))
        except Exception:
            log.exception("Failed to initialize shared state")
            # Continue anyway - tasks can still run without shared state

    def _should_terminate_on_idle(self) -> bool:
        """Check if we should terminate due to idle timeout."""
        idle_duration = (timezone.utcnow() - self.last_task_time).total_seconds()
        return idle_duration >= self.idle_timeout_seconds

    def _poll_for_task(self) -> TaskQueueMessage | None:
        """Poll SQS for next task. Returns None if no task available."""
        try:
            response = self.sqs.receive_message(
                QueueUrl=self.task_queue_url,
                MaxNumberOfMessages=1,
                WaitTimeSeconds=20,  # Long-polling
                VisibilityTimeout=self.visibility_timeout_seconds,
                MessageAttributeNames=["All"],
            )

            messages = response.get("Messages", [])
            if not messages:
                return None

            msg = messages[0]
            self.current_receipt_handle = msg["ReceiptHandle"]

            # Parse message
            task_message = TaskQueueMessage.model_validate_json(msg["Body"])
            return task_message

        except Exception:
            log.exception("Failed to poll task queue")
            return None

    def _execute_task(self, task_message: TaskQueueMessage):
        """Execute a task in a subprocess for isolation."""
        start_time = time.time()
        task_key = task_message.task_key

        log.info("Executing task %s", task_key)

        # Create result queue for subprocess communication
        result_queue: Queue = Queue()

        # Fork subprocess
        process = Process(target=self._run_task_subprocess, args=(task_message, result_queue))
        process.start()

        # Wait for completion with timeout (leave margin for cleanup)
        process.join(timeout=self.visibility_timeout_seconds - 60)

        execution_time = time.time() - start_time

        # Check result
        if process.is_alive():
            # Task timed out
            log.error("Task %s timed out after %.2f seconds", task_key, execution_time)
            process.terminate()
            process.join(timeout=30)
            if process.is_alive():
                process.kill()

            state = "FAILED"
            error_message = f"Task timed out after {execution_time:.2f} seconds"

        elif not result_queue.empty():
            # Get result from subprocess
            result = result_queue.get_nowait()
            state = result["state"]
            error_message = result.get("error_message")

        else:
            # Subprocess crashed without reporting
            state = "FAILED"
            error_message = f"Subprocess exited with code {process.exitcode}"

        # Report result to SQS
        self._report_result(task_key, state, execution_time, error_message)

        # Delete message from queue (task processed)
        try:
            self.sqs.delete_message(
                QueueUrl=self.task_queue_url,
                ReceiptHandle=self.current_receipt_handle,
            )
        except Exception:
            log.exception("Failed to delete task message")

        self.current_receipt_handle = None
        self.tasks_executed += 1
        self.last_task_time = timezone.utcnow()

        log.info("Task %s completed: %s (%.2fs)", task_key, state, execution_time)

    @staticmethod
    def _run_task_subprocess(task_message: TaskQueueMessage, result_queue: Queue):
        """
        Run in forked subprocess. Executes the actual Airflow task.

        This subprocess has access to SHARED_STATE via copy-on-write from parent.
        """
        import builtins

        try:
            # Make shared state available
            builtins.AIRFLOW_WORKER_SHARED_STATE = SHARED_STATE  # type: ignore[attr-defined]

            # Parse the workload JSON
            from pydantic import TypeAdapter

            from airflow.executors import workloads

            decoder = TypeAdapter[workloads.All](workloads.All)
            workload = decoder.validate_json(task_message.workload_json)

            # Execute via the same mechanism as other containerized executors
            from airflow.sdk.execution_time.execute_workload import execute_workload

            execute_workload(workload)

            result_queue.put({"state": "SUCCESS", "error_message": None})

        except Exception as e:
            log.exception("Task execution failed")
            result_queue.put({"state": "FAILED", "error_message": str(e)})

    def _report_result(
        self,
        task_key: TaskInstanceKeySchema,
        state: str,
        execution_time: float,
        error_message: str | None,
    ):
        """Send task result to SQS result queue."""
        result = TaskResultMessage(
            message_id=str(uuid.uuid4()),
            task_key=task_key,
            state=state,  # type: ignore[arg-type]
            info=TaskResultInfo(
                worker_id=self.worker_id,
                batch_job_id=self.batch_job_id,
                execution_time_seconds=execution_time,
                error_message=error_message,
            ),
            completed_at=timezone.utcnow(),
        )

        try:
            self.sqs.send_message(
                QueueUrl=self.result_queue_url,
                MessageBody=result.model_dump_json(),
                MessageAttributes={
                    "task_id": {"StringValue": str(task_key), "DataType": "String"},
                    "state": {"StringValue": state, "DataType": "String"},
                },
            )
        except Exception:
            log.exception("Failed to send result to queue")


def main():
    """Entry point for worker process."""
    parser = argparse.ArgumentParser(description="Airflow Batch Worker Pool Worker")
    parser.add_argument("--worker-id", required=True, help="Unique worker identifier")
    parser.add_argument("--task-queue-url", required=True, help="SQS task queue URL")
    parser.add_argument("--result-queue-url", required=True, help="SQS result queue URL")
    parser.add_argument("--idle-timeout", type=int, default=300, help="Idle timeout in seconds")
    parser.add_argument("--visibility-timeout", type=int, default=3600, help="SQS visibility timeout")
    parser.add_argument("--max-tasks", type=int, default=0, help="Max tasks before terminating (0=unlimited)")
    parser.add_argument("--init-module", help="Module containing shared state init function")
    parser.add_argument("--init-function", default="initialize", help="Shared state init function name")

    args = parser.parse_args()

    # Configure logging
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")

    worker = WorkerProcess(
        worker_id=args.worker_id,
        task_queue_url=args.task_queue_url,
        result_queue_url=args.result_queue_url,
        idle_timeout_seconds=args.idle_timeout,
        visibility_timeout_seconds=args.visibility_timeout,
        max_tasks=args.max_tasks,
        init_module=args.init_module,
        init_function=args.init_function,
    )

    try:
        worker.run()
        sys.exit(0)
    except Exception:
        log.exception("Worker crashed")
        sys.exit(1)


if __name__ == "__main__":
    main()
