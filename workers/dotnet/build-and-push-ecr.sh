#!/bin/bash
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

# =============================================================================
# Build and Push .NET Worker to ECR
# =============================================================================
#
# This script builds the .NET Airflow worker Docker image and pushes it to
# Amazon ECR for use with the AWS Batch Worker Pool Executor.
#
# Usage:
#   ./build-and-push-ecr.sh [OPTIONS]
#
# Options:
#   --region REGION       AWS region (default: us-east-1)
#   --repo-name NAME      ECR repository name (default: airflow-worker-dotnet)
#   --tag TAG             Image tag (default: latest)
#   --skip-create         Skip ECR repository creation
#   --run-tests           Run system tests after push
#   --help                Show this help message
#
# Examples:
#   ./build-and-push-ecr.sh
#   ./build-and-push-ecr.sh --region us-west-2 --tag v1.0.0
#   ./build-and-push-ecr.sh --run-tests
#
# =============================================================================

set -e

# Default configuration
AWS_REGION="${AWS_REGION:-us-east-1}"
ECR_REPO_NAME="airflow-worker-dotnet"
IMAGE_TAG="latest"
SKIP_CREATE=false
RUN_TESTS=false

# Script directory
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
AIRFLOW_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Helper functions
log_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

log_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

log_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

show_help() {
    head -40 "$0" | grep -E "^#" | sed 's/^# //' | sed 's/^#//'
    exit 0
}

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --region)
            AWS_REGION="$2"
            shift 2
            ;;
        --repo-name)
            ECR_REPO_NAME="$2"
            shift 2
            ;;
        --tag)
            IMAGE_TAG="$2"
            shift 2
            ;;
        --skip-create)
            SKIP_CREATE=true
            shift
            ;;
        --run-tests)
            RUN_TESTS=true
            shift
            ;;
        --help|-h)
            show_help
            ;;
        *)
            log_error "Unknown option: $1"
            echo "Use --help for usage information"
            exit 1
            ;;
    esac
done

echo "=============================================="
echo " .NET Worker - Build and Push to ECR"
echo "=============================================="
echo ""
log_info "Configuration:"
echo "  AWS Region:      $AWS_REGION"
echo "  ECR Repository:  $ECR_REPO_NAME"
echo "  Image Tag:       $IMAGE_TAG"
echo "  Script Dir:      $SCRIPT_DIR"
echo ""

# Step 1: Verify prerequisites
log_info "Step 1: Verifying prerequisites..."

if ! command -v aws &> /dev/null; then
    log_error "AWS CLI is not installed"
    exit 1
fi

if ! command -v docker &> /dev/null; then
    log_error "Docker is not installed"
    exit 1
fi

# Check AWS credentials
if ! aws sts get-caller-identity &> /dev/null; then
    log_error "AWS credentials not configured or invalid"
    exit 1
fi

AWS_ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
log_success "AWS Account: $AWS_ACCOUNT_ID"

# Check Docker daemon
if ! docker info &> /dev/null; then
    log_error "Docker daemon is not running"
    exit 1
fi
log_success "Docker is running"

# Step 2: Create ECR repository (if needed)
log_info "Step 2: Setting up ECR repository..."

if [ "$SKIP_CREATE" = false ]; then
    if aws ecr describe-repositories --repository-names "$ECR_REPO_NAME" --region "$AWS_REGION" &> /dev/null; then
        log_warning "Repository '$ECR_REPO_NAME' already exists"
    else
        log_info "Creating ECR repository '$ECR_REPO_NAME'..."
        aws ecr create-repository \
            --repository-name "$ECR_REPO_NAME" \
            --image-scanning-configuration scanOnPush=false \
            --image-tag-mutability MUTABLE \
            --region "$AWS_REGION" > /dev/null
        log_success "Repository created"
    fi
fi

# Get repository URI
ECR_REPO_URI=$(aws ecr describe-repositories \
    --repository-names "$ECR_REPO_NAME" \
    --query 'repositories[0].repositoryUri' \
    --output text \
    --region "$AWS_REGION")

if [ -z "$ECR_REPO_URI" ] || [ "$ECR_REPO_URI" = "None" ]; then
    log_error "Failed to get ECR repository URI"
    exit 1
fi

log_success "Repository URI: $ECR_REPO_URI"

# Step 3: Authenticate with ECR
log_info "Step 3: Authenticating with ECR..."

REGISTRY=$(echo "$ECR_REPO_URI" | cut -d'/' -f1)
aws ecr get-login-password --region "$AWS_REGION" | \
    docker login --username AWS --password-stdin "$REGISTRY" 2>/dev/null

log_success "Docker authenticated with ECR"

# Step 4: Build Docker image
log_info "Step 4: Building Docker image..."

cd "$SCRIPT_DIR"

# Build with progress output
docker build \
    --tag "$ECR_REPO_NAME:$IMAGE_TAG" \
    --tag "$ECR_REPO_URI:$IMAGE_TAG" \
    --file Dockerfile \
    .

log_success "Image built: $ECR_REPO_NAME:$IMAGE_TAG"

# Step 5: Push to ECR
log_info "Step 5: Pushing image to ECR..."

docker push "$ECR_REPO_URI:$IMAGE_TAG"

log_success "Image pushed: $ECR_REPO_URI:$IMAGE_TAG"

# Step 6: Verify
log_info "Step 6: Verifying image in ECR..."

IMAGE_DIGEST=$(aws ecr describe-images \
    --repository-name "$ECR_REPO_NAME" \
    --image-ids imageTag="$IMAGE_TAG" \
    --query 'imageDetails[0].imageDigest' \
    --output text \
    --region "$AWS_REGION")

log_success "Image verified: $IMAGE_DIGEST"

# Summary
echo ""
echo "=============================================="
echo " Build Complete!"
echo "=============================================="
echo ""
echo "Image URI:    $ECR_REPO_URI:$IMAGE_TAG"
echo "Digest:       $IMAGE_DIGEST"
echo ""
echo "To use this image in system tests, run:"
echo ""
echo "  export DOTNET_WORKER_IMAGE_URI=$ECR_REPO_URI:$IMAGE_TAG"
echo "  breeze testing system-tests --forward-credentials \\"
echo "      providers/amazon/tests/system/amazon/aws/tests/test_batch_worker_pool_dotnet.py"
echo ""

# Optionally run tests
if [ "$RUN_TESTS" = true ]; then
    echo "=============================================="
    echo " Running System Tests"
    echo "=============================================="
    echo ""

    export DOTNET_WORKER_IMAGE_URI="$ECR_REPO_URI:$IMAGE_TAG"

    cd "$AIRFLOW_ROOT"
    breeze testing system-tests --forward-credentials \
        providers/amazon/tests/system/amazon/aws/tests/test_batch_worker_pool_dotnet.py
fi
