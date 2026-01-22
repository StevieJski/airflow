---
name: aws-batch
description: Work with AWS Batch jobs, job queues, job definitions, and compute environments using the aws-cli MCP server. Use this skill when the user needs to submit, monitor, cancel, or manage Batch jobs.
argument-hint: [operation] [job-queue/job-id]
allowed-tools: mcp__aws-cli__aws_cli_pipeline, mcp__aws-cli__aws_cli_help
---

# AWS Batch Operations

Use the `aws-cli` MCP server tools for all Batch operations. Always include `--profile default --region us-east-1` unless the user specifies otherwise.

## Job Operations

### List jobs by status
```
# List jobs in a queue by status
aws batch list-jobs --job-queue QUEUE_NAME --job-status SUBMITTED --profile default --region us-east-1
aws batch list-jobs --job-queue QUEUE_NAME --job-status PENDING --profile default --region us-east-1
aws batch list-jobs --job-queue QUEUE_NAME --job-status RUNNABLE --profile default --region us-east-1
aws batch list-jobs --job-queue QUEUE_NAME --job-status STARTING --profile default --region us-east-1
aws batch list-jobs --job-queue QUEUE_NAME --job-status RUNNING --profile default --region us-east-1
aws batch list-jobs --job-queue QUEUE_NAME --job-status SUCCEEDED --profile default --region us-east-1
aws batch list-jobs --job-queue QUEUE_NAME --job-status FAILED --profile default --region us-east-1

# Get just job IDs
aws batch list-jobs --job-queue QUEUE_NAME --job-status RUNNING --query 'jobSummaryList[].jobId' --output text --profile default --region us-east-1
```

### Describe job details
```
# Single job
aws batch describe-jobs --jobs JOB_ID --profile default --region us-east-1

# Multiple jobs
aws batch describe-jobs --jobs JOB_ID_1 JOB_ID_2 JOB_ID_3 --profile default --region us-east-1

# Get specific fields
aws batch describe-jobs --jobs JOB_ID --query 'jobs[0].{status:status,reason:statusReason,started:startedAt,stopped:stoppedAt}' --profile default --region us-east-1
```

### Submit a job
```
# Submit using existing job definition
aws batch submit-job \
  --job-name my-job \
  --job-queue QUEUE_NAME \
  --job-definition JOB_DEF_NAME \
  --profile default --region us-east-1

# Submit with container overrides
aws batch submit-job \
  --job-name my-job \
  --job-queue QUEUE_NAME \
  --job-definition JOB_DEF_NAME \
  --container-overrides '{"command":["echo","hello"],"environment":[{"name":"VAR","value":"value"}]}' \
  --profile default --region us-east-1

# Submit array job
aws batch submit-job \
  --job-name my-array-job \
  --job-queue QUEUE_NAME \
  --job-definition JOB_DEF_NAME \
  --array-properties size=10 \
  --profile default --region us-east-1
```

### Cancel/terminate jobs
```
# Cancel a job (for SUBMITTED, PENDING, RUNNABLE)
aws batch cancel-job --job-id JOB_ID --reason "Cancellation reason" --profile default --region us-east-1

# Terminate a job (for STARTING, RUNNING)
aws batch terminate-job --job-id JOB_ID --reason "Termination reason" --profile default --region us-east-1
```

## Job Queues

### List job queues
```
aws batch describe-job-queues --profile default --region us-east-1

# Get queue names only
aws batch describe-job-queues --query 'jobQueues[].jobQueueName' --output text --profile default --region us-east-1
```

### Describe specific queue
```
aws batch describe-job-queues --job-queues QUEUE_NAME --profile default --region us-east-1
```

### Update queue state
```
# Disable a queue
aws batch update-job-queue --job-queue QUEUE_NAME --state DISABLED --profile default --region us-east-1

# Enable a queue
aws batch update-job-queue --job-queue QUEUE_NAME --state ENABLED --profile default --region us-east-1
```

## Job Definitions

### List job definitions
```
aws batch describe-job-definitions --status ACTIVE --profile default --region us-east-1

# Get definition names
aws batch describe-job-definitions --status ACTIVE --query 'jobDefinitions[].jobDefinitionName' --output text --profile default --region us-east-1
```

### Describe specific definition
```
aws batch describe-job-definitions --job-definition-name JOB_DEF_NAME --profile default --region us-east-1
```

## Compute Environments

### List compute environments
```
aws batch describe-compute-environments --profile default --region us-east-1

# Get names and states
aws batch describe-compute-environments --query 'computeEnvironments[].{name:computeEnvironmentName,state:state,status:status}' --profile default --region us-east-1
```

## Useful Queries

### Get failed job reasons
```
aws batch describe-jobs --jobs JOB_ID --query 'jobs[0].{status:status,reason:statusReason,container:container.reason}' --profile default --region us-east-1
```

### Get job logs location
```
aws batch describe-jobs --jobs JOB_ID --query 'jobs[0].container.logStreamName' --output text --profile default --region us-east-1
```

### Count jobs by status in a queue
```
aws batch list-jobs --job-queue QUEUE_NAME --job-status RUNNING --query 'length(jobSummaryList)' --profile default --region us-east-1
```

## Tips
- Job status progression: SUBMITTED → PENDING → RUNNABLE → STARTING → RUNNING → SUCCEEDED/FAILED
- Use `cancel-job` for jobs not yet running, `terminate-job` for running jobs
- Completed jobs (SUCCEEDED/FAILED) cannot be deleted - they auto-expire after 24 hours
- Array jobs have child jobs with IDs like `JOB_ID:0`, `JOB_ID:1`, etc.
- Check CloudWatch Logs for job output using the logStreamName from describe-jobs
