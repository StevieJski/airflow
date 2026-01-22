---
name: aws-s3
description: Work with AWS S3 buckets and objects using the aws-cli MCP server. Use this skill when the user needs to list, upload, download, copy, sync, or manage S3 objects and buckets.
argument-hint: [operation] [bucket/path]
allowed-tools: mcp__aws-cli__aws_cli_pipeline, mcp__aws-cli__aws_cli_help
---

# AWS S3 Operations

Use the `aws-cli` MCP server tools for all S3 operations. Always include `--profile default --region us-east-1` unless the user specifies otherwise.

## Common Operations

### List buckets
```
aws s3 ls --profile default --region us-east-1
```

### List objects in a bucket
```
aws s3 ls s3://bucket-name/ --profile default --region us-east-1
aws s3 ls s3://bucket-name/prefix/ --recursive --profile default --region us-east-1
```

### Copy files
```
# Upload local file to S3
aws s3 cp local-file.txt s3://bucket-name/path/ --profile default --region us-east-1

# Download from S3
aws s3 cp s3://bucket-name/path/file.txt ./local-path/ --profile default --region us-east-1

# Copy between S3 locations
aws s3 cp s3://source-bucket/file s3://dest-bucket/file --profile default --region us-east-1
```

### Sync directories
```
# Sync local to S3
aws s3 sync ./local-dir s3://bucket-name/prefix/ --profile default --region us-east-1

# Sync S3 to local
aws s3 sync s3://bucket-name/prefix/ ./local-dir --profile default --region us-east-1

# Sync with delete (mirror)
aws s3 sync ./local-dir s3://bucket-name/prefix/ --delete --profile default --region us-east-1
```

### Remove objects
```
# Remove single object
aws s3 rm s3://bucket-name/path/file.txt --profile default --region us-east-1

# Remove recursively
aws s3 rm s3://bucket-name/prefix/ --recursive --profile default --region us-east-1
```

### Get object metadata
```
aws s3api head-object --bucket bucket-name --key path/to/object --profile default --region us-east-1
```

### Generate presigned URL
```
aws s3 presign s3://bucket-name/path/file.txt --expires-in 3600 --profile default --region us-east-1
```

## Advanced Operations

### List with filtering (using --query)
```
# List objects modified after a date
aws s3api list-objects-v2 --bucket bucket-name --query "Contents[?LastModified>='2024-01-01']" --profile default --region us-east-1

# Get total size of a prefix
aws s3api list-objects-v2 --bucket bucket-name --prefix prefix/ --query "sum(Contents[].Size)" --profile default --region us-east-1
```

### Bucket operations
```
# Create bucket
aws s3 mb s3://new-bucket-name --profile default --region us-east-1

# Get bucket location
aws s3api get-bucket-location --bucket bucket-name --profile default --region us-east-1

# Get bucket versioning
aws s3api get-bucket-versioning --bucket bucket-name --profile default --region us-east-1
```

## Tips
- Use `--dryrun` to preview sync/copy operations before executing
- Use `--exclude` and `--include` patterns with sync/cp for filtering
- For large files, multipart upload is automatic
- Use `s3api` commands for more granular control than `s3` commands
