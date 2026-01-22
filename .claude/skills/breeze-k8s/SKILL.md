---
name: breeze-k8s
description: Manage Kubernetes clusters and deploy Airflow for testing using Breeze. Use this skill for Kubernetes development and helm chart testing.
argument-hint: [operation] [options]
allowed-tools: Bash
---

# Breeze Kubernetes Operations

Manage KinD (Kubernetes in Docker) clusters for Airflow testing and development.

## Setup Environment

### Initialize K8s development environment
```bash
breeze k8s setup-env
```

## Cluster Management

### Create cluster
```bash
# Default cluster
breeze k8s create-cluster

# With specific Kubernetes version
breeze k8s create-cluster --kubernetes-version v1.28.0
breeze k8s create-cluster --kubernetes-version v1.29.0
breeze k8s create-cluster --kubernetes-version v1.30.0

# With specific Python version for Airflow
breeze k8s create-cluster --python 3.11
```

### Delete cluster
```bash
breeze k8s delete-cluster
```

### Check cluster status
```bash
breeze k8s status
```

### Cleanup cluster resources
```bash
breeze k8s cleanup-cluster
```

## Image Management

### Build K8s-ready image
```bash
breeze k8s build-k8s-image
breeze k8s build-k8s-image --python 3.11
```

### Upload image to KinD cluster
```bash
breeze k8s upload-k8s-image
```

### Build and upload in one step
```bash
breeze k8s build-k8s-image && breeze k8s upload-k8s-image
```

## Deploy Airflow

### Deploy Airflow to cluster
```bash
breeze k8s deploy-airflow
```

### Deploy with specific executor
```bash
breeze k8s deploy-airflow --executor CeleryExecutor
breeze k8s deploy-airflow --executor KubernetesExecutor
breeze k8s deploy-airflow --executor LocalExecutor
```

### Deploy with custom values
```bash
breeze k8s deploy-airflow --helm-set "workers.replicas=2"
```

## Interactive Access

### Open shell in K8s pod
```bash
# Shell into scheduler pod
breeze k8s shell

# Shell into specific component
breeze k8s shell --component webserver
breeze k8s shell --component worker
breeze k8s shell --component triggerer
```

### View pod logs
```bash
# Scheduler logs
breeze k8s logs

# Specific component logs
breeze k8s logs --component webserver
breeze k8s logs --component worker

# Follow logs
breeze k8s logs --follow
```

## Testing

### Run complete K8s tests
```bash
breeze k8s run-complete-tests
```

### Run with specific test file
```bash
breeze k8s run-complete-tests --test-args "tests/kubernetes/test_example.py"
```

## Full Development Workflow

### Complete setup from scratch
```bash
# 1. Setup environment
breeze k8s setup-env

# 2. Create cluster
breeze k8s create-cluster --kubernetes-version v1.29.0

# 3. Build and upload image
breeze k8s build-k8s-image
breeze k8s upload-k8s-image

# 4. Deploy Airflow
breeze k8s deploy-airflow --executor KubernetesExecutor

# 5. Check status
breeze k8s status

# 6. Access shell
breeze k8s shell
```

### Reset and redeploy
```bash
breeze k8s cleanup-cluster
breeze k8s deploy-airflow
```

## kubectl Commands (while cluster is running)

### Get pods
```bash
kubectl get pods -n airflow
```

### Describe pod
```bash
kubectl describe pod -n airflow -l component=scheduler
```

### Port forward to webserver
```bash
kubectl port-forward svc/airflow-webserver 8080:8080 -n airflow
```

### Get all resources
```bash
kubectl get all -n airflow
```

## Tips
- KinD (Kubernetes in Docker) is used for local K8s testing
- Ensure Docker has enough resources allocated (4+ GB RAM recommended)
- The cluster persists until explicitly deleted
- Use `status` to verify cluster and deployment health
- K8s tests use the helm chart in `chart/` directory
- Image uploads can take time for large images
- Delete cluster when done to free resources
- Use `--verbose` for detailed command output
