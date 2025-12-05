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
# Airflow Batch Worker Pool - .NET 8 Worker
# =============================================================================
#
# Build:
#   docker build -t airflow-worker-dotnet:latest .
#
# Run locally for testing:
#   docker run --rm \
#     -e AWS_ACCESS_KEY_ID=... \
#     -e AWS_SECRET_ACCESS_KEY=... \
#     -e AWS_REGION=us-east-1 \
#     airflow-worker-dotnet:latest \
#     --worker-id test-001 \
#     --task-queue-url https://sqs.us-east-1.amazonaws.com/123456789/tasks \
#     --result-queue-url https://sqs.us-east-1.amazonaws.com/123456789/results
#
# =============================================================================

# Build stage
FROM mcr.microsoft.com/dotnet/sdk:8.0 AS build
WORKDIR /src

# Copy project files first for layer caching
COPY AirflowWorker/*.csproj AirflowWorker/
COPY AirflowWorker.Example/*.csproj AirflowWorker.Example/

# Restore dependencies
RUN dotnet restore AirflowWorker/AirflowWorker.csproj
RUN dotnet restore AirflowWorker.Example/AirflowWorker.Example.csproj

# Copy all source files
COPY . .

# Build and publish worker
RUN dotnet publish AirflowWorker/AirflowWorker.csproj \
    -c Release \
    -o /app

# Build and publish example handlers
RUN dotnet publish AirflowWorker.Example/AirflowWorker.Example.csproj \
    -c Release \
    -o /app/handlers

# Runtime stage - use smaller runtime image
FROM mcr.microsoft.com/dotnet/aspnet:8.0

# Add labels for identification
LABEL org.opencontainers.image.source="https://github.com/apache/airflow"
LABEL org.opencontainers.image.title="Airflow Batch Worker Pool - .NET Worker"
LABEL org.opencontainers.image.description="Long-running .NET worker for AWS Batch Worker Pool Executor"

WORKDIR /app

# Copy published application
COPY --from=build /app .

# Create non-root user for security
RUN groupadd -r airflow && useradd -r -g airflow airflow
RUN chown -R airflow:airflow /app
USER airflow

# Health check - worker is healthy if process is running
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD pgrep -f "AirflowWorker" > /dev/null || exit 1

# Entry point
ENTRYPOINT ["dotnet", "AirflowWorker.dll"]

# Default arguments (override with docker run args or Batch containerOverrides)
CMD ["--help"]
