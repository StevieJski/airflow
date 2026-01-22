---
name: breeze-test
description: Run Airflow tests using Breeze - unit tests, integration tests, provider tests, helm tests, and more. Use this skill for testing Airflow code changes.
argument-hint: [test-type] [test-path] [options]
allowed-tools: Bash
---

# Breeze Testing Commands

Run various types of Airflow tests through Breeze. All commands run from the repository root.

## Unit Tests

### Run core Airflow tests
```bash
# Run all core tests
breeze testing core-tests

# Run specific test file
breeze testing core-tests --test-args "tests/core/test_example.py"

# Run specific test class
breeze testing core-tests --test-args "tests/core/test_example.py::TestClassName"

# Run specific test method
breeze testing core-tests --test-args "tests/core/test_example.py::TestClassName::test_method"

# Run with verbose output
breeze testing core-tests --test-args "-v tests/core/test_example.py"
```

### Run provider tests
```bash
# Run all provider tests
breeze testing providers-tests

# Run tests for specific provider
breeze testing providers-tests --test-args "tests/providers/amazon/"
breeze testing providers-tests --test-args "tests/providers/google/"

# Run specific provider test file
breeze testing providers-tests --test-args "tests/providers/amazon/aws/hooks/test_s3.py"
```

### Run Task SDK tests
```bash
breeze testing task-sdk-tests
breeze testing task-sdk-tests --test-args "tests/task_sdk/test_example.py"
```

### Run AirflowCTL tests
```bash
breeze testing airflow-ctl-tests
```

## Integration Tests

### Core integration tests
```bash
breeze testing core-integration-tests

# With specific integration
breeze testing core-integration-tests --integration celery
breeze testing core-integration-tests --integration kafka
breeze testing core-integration-tests --integration redis
```

### Provider integration tests
```bash
breeze testing integration-providers-tests
breeze testing integration-providers-tests --test-args "tests/providers/amazon/"
```

### Task SDK integration tests
```bash
breeze testing task-sdk-integration-tests
```

### AirflowCTL integration tests
```bash
breeze testing airflow-ctl-integration-tests
```

## Specialized Tests

### Helm chart tests
```bash
breeze testing helm-tests

# Specific helm test
breeze testing helm-tests --test-args "tests/charts/test_worker.py"
```

### Docker Compose tests
```bash
breeze testing docker-compose-tests
```

### System tests
```bash
breeze testing system-tests

# Specific system test
breeze testing system-tests --test-args "tests/system/providers/amazon/"
```

### Python API client tests
```bash
breeze testing python-api-client-tests
```

### End-to-end tests
```bash
# Airflow E2E tests
breeze testing airflow-e2e-tests

# UI E2E tests
breeze testing ui-e2e-tests
```

## Test Configuration Options

### With specific Python version
```bash
breeze testing core-tests --python 3.11
```

### With specific backend
```bash
breeze testing core-tests --backend postgres
breeze testing core-tests --backend mysql
```

### Enable test coverage
```bash
breeze testing core-tests --enable-coverage
```

### Parallel execution
```bash
breeze testing core-tests --parallelism 4
breeze testing core-tests --parallel-test-types "Providers Core"
```

### With database tests
```bash
breeze testing core-tests --run-db-tests-only
breeze testing core-tests --skip-db-tests
```

### Test timeout
```bash
breeze testing core-tests --test-timeout 300
```

## Common Patterns

### Run tests matching a pattern (pytest -k)
```bash
breeze testing core-tests --test-args "-k test_pattern"
```

### Run tests with markers
```bash
breeze testing core-tests --test-args "-m 'not integration'"
```

### Run failed tests only (from last run)
```bash
breeze testing core-tests --test-args "--lf"
```

### Stop on first failure
```bash
breeze testing core-tests --test-args "-x"
```

### Show local variables in tracebacks
```bash
breeze testing core-tests --test-args "-l"
```

## Tips
- Use `--parallelism` to speed up test runs on multi-core machines
- Add `-v` or `-vv` to test args for more verbose pytest output
- Use `--run-db-tests-only` for database-specific tests
- Provider tests require the provider to be installed in the image
- Integration tests may require additional services (Redis, Kafka, etc.)
- Use `--dry-run` to see the command that would be executed
- Check `breeze testing --help` for all available options
