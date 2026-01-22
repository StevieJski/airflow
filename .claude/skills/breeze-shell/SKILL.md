---
name: breeze-shell
description: Manage Breeze development environment - enter shell, execute commands, start/stop containers. Use this skill for interactive development with Airflow Breeze.
argument-hint: [command] [options]
allowed-tools: Bash
---

# Breeze Shell & Environment Management

Use Breeze for managing your Airflow development environment. Run commands from the repository root.

## IMPORTANT: Avoid Interactive tmux Sessions

**`breeze start-airflow` uses tmux which blocks automation.** Use these alternatives instead:

### Agent-Friendly Alternatives

```bash
# PREFERRED: Use breeze exec for specific commands in running container
breeze exec "airflow dags list"

# PREFERRED: Use breeze run for one-off tasks (runs and exits)
breeze run "airflow db migrate"

# Start services in daemon mode (background)
breeze exec "airflow webserver -D"
breeze exec "airflow scheduler -D"

# For testing, use breeze shell with specific commands
breeze shell -c "pytest tests/core/test_example.py"
```

### If You Must Use start-airflow

```bash
# Start in background, then monitor via exec
breeze start-airflow &
sleep 30  # Wait for startup
breeze exec "airflow jobs check"

# Capture tmux pane output for monitoring
tmux capture-pane -t airflow -p | tail -50
```

## Enter Breeze Shell

### Default shell (SQLite, Sequential executor)
```bash
breeze
# or explicitly:
breeze shell
```

### Shell with specific backend
```bash
# PostgreSQL backend
breeze shell --backend postgres

# MySQL backend
breeze shell --backend mysql

# Specific database versions
breeze shell --backend postgres --postgres-version 16
breeze shell --backend mysql --mysql-version 8.0
```

### Shell with specific Python version
```bash
breeze shell --python 3.10
breeze shell --python 3.11
breeze shell --python 3.12
```

### Shell with specific executor
```bash
breeze shell --executor LocalExecutor
breeze shell --executor CeleryExecutor --celery-broker redis
breeze shell --executor CeleryExecutor --celery-broker rabbitmq
```

### Full example with multiple options
```bash
breeze shell \
  --python 3.11 \
  --backend postgres \
  --postgres-version 16 \
  --executor CeleryExecutor \
  --celery-broker redis
```

## Execute Commands in Running Container

### Run a single command
```bash
breeze exec "airflow version"
breeze exec "python --version"
breeze exec "pip list | grep apache-airflow"
```

### Run airflow CLI commands
```bash
breeze exec "airflow dags list"
breeze exec "airflow tasks list example_bash_operator"
breeze exec "airflow db check"
```

## Container Lifecycle

### Stop Breeze containers
```bash
breeze down
```

### Stop and clean volumes
```bash
breeze down --cleanup-mypy-cache
```

### Check running containers
```bash
docker ps --filter "name=breeze"
```

## Environment Configuration

### Skip mounting local sources (use installed packages)
```bash
breeze shell --skip-environment-initialization
```

### Mount additional directories
```bash
breeze shell --mount-sources tests,dev
```

### Forward additional ports
```bash
breeze shell --forward-ports 8793  # Forward scheduler port
```

## Quick Commands

### Reset database
```bash
breeze exec "airflow db reset -y"
```

### Initialize fresh database
```bash
breeze exec "airflow db migrate"
```

### Create admin user
```bash
breeze exec "airflow users create --username admin --password admin --firstname Admin --lastname User --role Admin --email admin@example.com"
```

## Tips
- **AVOID `breeze start-airflow` for automation** - it uses tmux which blocks agents
- Use `breeze exec` or `breeze run` for agent-friendly execution
- Default Breeze shell uses SQLite backend and SequentialExecutor for quick startup
- Use `--backend postgres` for production-like testing
- The `exec` command requires Breeze containers to be running
- Use `breeze down` before switching backends to avoid conflicts
- Add `--verbose` to any command for detailed output
- Use `--dry-run` to see what commands would be executed without running them
- For background services, use daemon mode (`-D` flag) instead of tmux
