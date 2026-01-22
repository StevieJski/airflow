---
name: aws-sqs
description: Work with AWS SQS queues and messages using the aws-cli MCP server. Use this skill when the user needs to send, receive, delete messages, or manage SQS queues.
argument-hint: [operation] [queue-name/url]
allowed-tools: mcp__aws-cli__aws_cli_pipeline, mcp__aws-cli__aws_cli_help
---

# AWS SQS Operations

Use the `aws-cli` MCP server tools for all SQS operations. Always include `--profile default --region us-east-1` unless the user specifies otherwise.

## Queue Discovery

### List all queues
```
aws sqs list-queues --profile default --region us-east-1

# List queues with prefix
aws sqs list-queues --queue-name-prefix my-prefix --profile default --region us-east-1

# Get just URLs
aws sqs list-queues --query 'QueueUrls[]' --output text --profile default --region us-east-1
```

### Get queue URL by name
```
aws sqs get-queue-url --queue-name QUEUE_NAME --profile default --region us-east-1
```

### Get queue attributes
```
# All attributes
aws sqs get-queue-attributes --queue-url QUEUE_URL --attribute-names All --profile default --region us-east-1

# Specific attributes
aws sqs get-queue-attributes --queue-url QUEUE_URL --attribute-names ApproximateNumberOfMessages ApproximateNumberOfMessagesNotVisible --profile default --region us-east-1
```

## Message Operations

### Send a message
```
# Simple message
aws sqs send-message --queue-url QUEUE_URL --message-body "Hello World" --profile default --region us-east-1

# With message attributes
aws sqs send-message \
  --queue-url QUEUE_URL \
  --message-body "Message content" \
  --message-attributes '{"Attribute1":{"DataType":"String","StringValue":"value1"}}' \
  --profile default --region us-east-1

# With delay
aws sqs send-message --queue-url QUEUE_URL --message-body "Delayed message" --delay-seconds 60 --profile default --region us-east-1
```

### Send batch of messages
```
aws sqs send-message-batch \
  --queue-url QUEUE_URL \
  --entries '[{"Id":"1","MessageBody":"Message 1"},{"Id":"2","MessageBody":"Message 2"}]' \
  --profile default --region us-east-1
```

### Receive messages
```
# Receive up to 10 messages
aws sqs receive-message --queue-url QUEUE_URL --max-number-of-messages 10 --profile default --region us-east-1

# With long polling (wait up to 20 seconds)
aws sqs receive-message --queue-url QUEUE_URL --wait-time-seconds 20 --profile default --region us-east-1

# Include message attributes
aws sqs receive-message --queue-url QUEUE_URL --message-attribute-names All --attribute-names All --profile default --region us-east-1
```

### Delete a message
```
aws sqs delete-message --queue-url QUEUE_URL --receipt-handle RECEIPT_HANDLE --profile default --region us-east-1
```

### Delete batch of messages
```
aws sqs delete-message-batch \
  --queue-url QUEUE_URL \
  --entries '[{"Id":"1","ReceiptHandle":"handle1"},{"Id":"2","ReceiptHandle":"handle2"}]' \
  --profile default --region us-east-1
```

### Change message visibility
```
# Extend visibility timeout
aws sqs change-message-visibility --queue-url QUEUE_URL --receipt-handle RECEIPT_HANDLE --visibility-timeout 300 --profile default --region us-east-1
```

## Queue Management

### Create a queue
```
# Standard queue
aws sqs create-queue --queue-name my-queue --profile default --region us-east-1

# FIFO queue
aws sqs create-queue --queue-name my-queue.fifo --attributes FifoQueue=true --profile default --region us-east-1

# With custom attributes
aws sqs create-queue --queue-name my-queue --attributes '{"VisibilityTimeout":"60","MessageRetentionPeriod":"86400"}' --profile default --region us-east-1
```

### Update queue attributes
```
aws sqs set-queue-attributes --queue-url QUEUE_URL --attributes '{"VisibilityTimeout":"120"}' --profile default --region us-east-1
```

### Purge queue (delete all messages)
```
aws sqs purge-queue --queue-url QUEUE_URL --profile default --region us-east-1
```

### Delete queue
```
aws sqs delete-queue --queue-url QUEUE_URL --profile default --region us-east-1
```

## Dead Letter Queue Operations

### Get DLQ configuration
```
aws sqs get-queue-attributes --queue-url QUEUE_URL --attribute-names RedrivePolicy --profile default --region us-east-1
```

### List DLQ source queues
```
aws sqs list-dead-letter-source-queues --queue-url DLQ_URL --profile default --region us-east-1
```

### Start message move (redrive from DLQ)
```
aws sqs start-message-move-task \
  --source-arn arn:aws:sqs:us-east-1:ACCOUNT:dlq-name \
  --destination-arn arn:aws:sqs:us-east-1:ACCOUNT:main-queue \
  --profile default --region us-east-1
```

## Useful Queries

### Get message count
```
aws sqs get-queue-attributes --queue-url QUEUE_URL --attribute-names ApproximateNumberOfMessages --query 'Attributes.ApproximateNumberOfMessages' --output text --profile default --region us-east-1
```

### Get in-flight message count
```
aws sqs get-queue-attributes --queue-url QUEUE_URL --attribute-names ApproximateNumberOfMessagesNotVisible --query 'Attributes.ApproximateNumberOfMessagesNotVisible' --output text --profile default --region us-east-1
```

### Check queue health (messages + in-flight + delayed)
```
aws sqs get-queue-attributes --queue-url QUEUE_URL --attribute-names ApproximateNumberOfMessages ApproximateNumberOfMessagesNotVisible ApproximateNumberOfMessagesDelayed --profile default --region us-east-1
```

## Tips
- Queue URLs follow pattern: `https://sqs.REGION.amazonaws.com/ACCOUNT_ID/QUEUE_NAME`
- FIFO queue names must end with `.fifo`
- Use long polling (`--wait-time-seconds`) to reduce API calls and costs
- Receipt handles are needed to delete or change visibility of messages
- Purge queue has a 60-second cooldown between calls
- Standard queues offer at-least-once delivery; FIFO queues offer exactly-once
- Maximum message size is 256 KB; use S3 for larger payloads
