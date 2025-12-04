# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## About Apache Airflow

Apache Airflow is a platform to programmatically author, schedule, and monitor workflows. This is a **monorepo** containing multiple independently versioned packages organized as a UV workspace. The current version is 3.2.0 (main branch is development version).

## Repository Architecture

### Monorepo Structure

This repository is NOT a single package - it's a workspace containing:

- **airflow-core** - Main orchestration engine, scheduler, web UI, and API
- **task-sdk** - Task Execution SDK (independently versioned, e.g., 1.x while core is 3.x)
- **providers/** - 70+ integration providers (each with independent versioning)
- **shared/** - Common code shared via symlinks across distributions
- **airflow-ctl** - CLI tool for remote Airflow instance management
- **go-sdk** - Experimental Go language task SDK

### Key Architectural Principles

1. **UV Workspace**: All packages are managed in a single workspace with shared dependency resolution
2. **Symlink-based sharing**: Shared code lives in `shared/` and is symlinked into `_shared/` in each distribution to avoid version conflicts
3. **Provider discovery**: Providers register via Python entry points and are dynamically loaded
4. **Independent versioning**: Core, Task SDK, and each provider version independently following SemVer
5. **Constraint-based dependencies**: Core has minimal upper bounds; constraint files provide tested combinations

### Directory Layout

```
airflow/
├── airflow-core/            # Core Airflow package
│   ├── src/airflow/        # Core source code
│   │   ├── api/            # REST API
│   │   ├── api_fastapi/    # FastAPI implementation
│   │   ├── executors/      # LocalExecutor, CeleryExecutor, KubernetesExecutor
│   │   ├── models/         # Database models, DAG representations
│   │   ├── jobs/           # Scheduler, triggerer
│   │   ├── cli/            # Command-line interface
│   │   ├── ui/             # Web interface
│   │   └── _shared/        # Symlinked shared code
│   └── tests/              # Unit, integration, system tests
├── task-sdk/               # Task Execution SDK (AIP-72)
├── providers/              # Integration providers
│   ├── amazon/            # Each provider has:
│   │   ├── provider.yaml  #   - Metadata
│   │   ├── pyproject.toml #   - Package definition
│   │   ├── src/           #   - Source code
│   │   └── tests/         #   - Tests (unit/integration/system)
│   ├── google/
│   └── ...
├── shared/                 # Reusable code (symlinked into distributions)
│   ├── configuration/
│   ├── logging/
│   ├── secrets_backend/
│   ├── secrets_masker/
│   └── timezones/
├── dev/breeze/             # Docker-based development environment
└── pyproject.toml          # Root workspace configuration (1,500+ lines)
```

## Development Setup

### Prerequisites

- Python 3.10, 3.11, 3.12, or 3.13
- UV package manager (required)
- Docker (optional, for Breeze)
- Increase `ulimit` on macOS: `ulimit -n 2048`

### Quick Start with UV

```bash
# Install UV (if not already installed)
curl -LsSf https://astral.sh/uv/install.sh | sh

# Clone the repository
git clone https://github.com/apache/airflow.git
cd airflow

# Sync all dependencies (creates .venv automatically)
uv sync

# Activate virtualenv
source .venv/bin/activate  # On Unix/macOS
# or
.venv\Scripts\activate     # On Windows

# Install development tools
uv tool install -e ./dev/breeze --force
uv tool install prek
```

### Using Breeze (Docker-based Development)

Breeze replicates the CI environment locally:

```bash
# Enter development shell
breeze shell

# Run tests in Breeze
breeze testing tests
```

## Common Development Commands

### Testing

```bash
# Run all unit tests
pytest airflow-core/tests/unit/

# Run specific test file
pytest airflow-core/tests/unit/models/test_dag.py

# Run specific test
pytest airflow-core/tests/unit/models/test_dag.py::TestDag::test_dag_params

# Run provider tests
pytest providers/amazon/tests/unit/

# Run with specific database backend (in Breeze)
breeze testing tests --backend postgres
```

### Linting and Formatting

```bash
# Run pre-commit checks (Ruff, MyPy, etc.)
prek --all-files

# Run specific checks
prek run ruff
prek run mypy
```

### Building and Installing

```bash
# Sync workspace after changes
uv sync

# Build distributions
uv build

# Install Airflow locally with extras
uv pip install -e ".[devel,postgres,google]"
```

### Working with Providers

```bash
# Sync specific provider
cd providers/amazon
uv sync

# Run provider tests
pytest tests/unit/

# Build provider package
uv build
```

## Testing Architecture

### Test Categories

1. **Unit Tests** (`tests/unit/`)
   - Fast, isolated tests with no external dependencies
   - Run in both local virtualenv and Breeze
   - **Required for all PRs** (unless docs-only)
   - Location: `airflow-core/tests/unit/` or `providers/*/tests/unit/`

2. **Integration Tests** (`tests/integration/`)
   - Require additional services (Postgres, MySQL, Kerberos)
   - Run in Breeze environment
   - Location: `airflow-core/tests/integration/` or `providers/*/tests/integration/`

3. **System Tests** (`tests/system/`)
   - End-to-end tests with real external services (AWS, GCP, etc.)
   - Run by provider teams, results shown in dashboards
   - Location: `airflow-core/tests/system/` or `providers/*/tests/system/`

### Test Configuration

- **Framework**: pytest
- **Configuration**: `pyproject.toml` (tool.pytest section)
- **Fixtures**: `conftest.py` files throughout the codebase
- **Parallel execution**: Supported via pytest-xdist

## Code Style and Standards

### Import Standards

```python
# Required: All files must start with future annotations
from __future__ import annotations

# Import order (enforced by ruff):
# 1. Future imports
# 2. Standard library
# 3. Third-party
# 4. First-party (airflow, airflow_shared)
# 5. Local/relative
# 6. Testing imports (dev, tests, etc.)
```

### Shared Code Guidelines

- Shared libraries (`shared/`) **MUST use relative imports** for cross-references
- Other code uses absolute imports
- Never import from `providers` as a top-level module
- Use `from airflow.providers.amazon` not `from providers.amazon`

### Type Checking

- MyPy is configured with strict settings
- Type hints are required for new code
- Use `from typing import TYPE_CHECKING` for import-only types

### Docstring Style

- Follow Google-style docstrings
- Required for public modules, classes, and functions
- Not required for tests, private methods, or `__init__` files

## Important Constraints

### Dependency Management

Upper-bounded dependencies (commented in `pyproject.toml`):
- **SQLAlchemy**: Upper-bound to MINOR (predictable breaking changes)
- **Alembic**: Stable in MINOR versions, developed with SQLAlchemy
- **Flask**: MAJOR version bound (significant breaking changes)
- **werkzeug**: Tightly coupled with Flask
- **celery**: Follows SemVer, bound to next MAJOR
- **kubernetes**: Follows SemVer, bound to next MAJOR

### Provider Dependency Philosophy

- By default, **no upper bounds** on provider dependencies
- Maintainers may add bounds with justification
- Constraints files provide tested combinations

## Git Workflow

### Branches

- **main**: Development branch for next MINOR release
- **v3-1-stable**: Stable branch for 3.1.x releases
- **v2-11-stable**: Stable branch for 2.x releases

### Release Process

1. PRs merge to `main` and are included in next MINOR release
2. Bug fixes can be cherry-picked to stable branches for PATCH releases
3. Milestones indicate target release but don't guarantee inclusion
4. First RC candidate: `v3-*-test` and `v3-*-stable` branches stop rebasing

### Commit Requirements

- Commits need +1 from committer who is not the author
- Include tests unless documentation-only change
- Follow conventional commit format when possible

## Key Files and Locations

- **pyproject.toml**: Root workspace configuration, all dependencies
- **uv.lock**: Lockfile for reproducible builds
- **CONTRIBUTING.rst**: Detailed contribution guidelines
- **contributing-docs/**: Extensive documentation on development
- **Dockerfile.ci**: CI environment definition (reference for dependencies)
- **provider.yaml**: Provider metadata in each provider directory

## Running Airflow Locally

```bash
# Set Airflow home
export AIRFLOW_HOME=~/airflow

# Initialize database
airflow db migrate

# Create admin user
airflow users create \
    --username admin \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email admin@example.com

# Run in standalone mode (all components)
airflow standalone

# Or run components separately:
airflow scheduler      # Start scheduler
airflow webserver      # Start web server (port 8080)
```

## Common Pitfalls

1. **Don't treat this as a single package**: It's a monorepo with 70+ packages
2. **Provider versions don't match Airflow version**: They version independently
3. **Constraints are important**: Use constraint files for reproducible installs
4. **UV is required for development**: Other tools may not handle workspace correctly
5. **Shared code uses relative imports**: Exception to the absolute import rule
6. **Tests are categorized**: Unit vs integration vs system have different requirements
7. **Mac users need higher ulimit**: Run `ulimit -n 2048` before `uv sync`

## Useful Commands Reference

```bash
# Development environment
uv sync                           # Sync all dependencies
uv tool install prek              # Install pre-commit tool
prek --all-files                  # Run all checks

# Testing
pytest airflow-core/tests/unit/   # Run unit tests
pytest -k "test_name"             # Run specific test by name
pytest --last-failed              # Re-run only failed tests

# Breeze
breeze shell                      # Enter dev environment
breeze testing tests              # Run tests in Breeze

# Git
git fetch --all                   # Fetch all remotes
git rebase -i main                # Interactive rebase on main

# Documentation
uv run --group docs build-docs    # Build documentation
uv run --group docs build-docs --autobuild  # Auto-refresh build
```

## Additional Resources

- [Contributing Guide](./CONTRIBUTING.rst) - Comprehensive contribution guidelines
- [Local Virtualenv Guide](./contributing-docs/07_local_virtualenv.rst) - Detailed setup instructions
- [Testing Guide](./contributing-docs/09_testing.rst) - Complete testing documentation
- [Breeze Documentation](./dev/breeze/doc/README.rst) - Docker environment guide
- [Official Docs](https://airflow.apache.org/docs/) - User documentation
- [Release Process](https://airflow.apache.org/docs/apache-airflow/stable/release-process.html) - How releases work
