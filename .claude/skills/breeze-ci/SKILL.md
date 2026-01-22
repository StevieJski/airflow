---
name: breeze-ci
description: Run CI-related Breeze tasks - resource checks, cleanup, selective checks, and CI troubleshooting. Use this skill for CI pipeline operations and debugging.
argument-hint: [operation] [options]
allowed-tools: Bash
---

# Breeze CI Tasks

Manage CI operations, resource checks, and CI-specific utilities.

## Resource Management

### Check available resources
```bash
breeze ci resource-check
```

### Free up disk/memory space
```bash
# Basic cleanup
breeze ci free-space

# Aggressive cleanup for CI
breeze ci free-space --remove-swap
```

### Fix file ownership (Linux)
```bash
# Fix ownership issues from Docker root processes
breeze ci fix-ownership
```

## Selective Checks

### Determine which tests to run for a PR
```bash
# For a specific commit
breeze ci selective-check --commit-ref HEAD

# For a specific commit range
breeze ci selective-check --commit-ref HEAD~3..HEAD
```

### Get workflow information
```bash
breeze ci get-workflow-info
```

## CI Upgrades

### Perform CI dependency upgrades
```bash
breeze ci upgrade
```

## Environment Verification

### Check Docker is working
```bash
docker info
docker version
```

### Check Breeze version
```bash
breeze version
```

### Verify Breeze installation
```bash
breeze setup version
```

### Check available disk space
```bash
df -h /
df -h /var/lib/docker
```

### Check available memory
```bash
free -h
```

### Check Docker disk usage
```bash
docker system df
docker system df -v
```

## CI Cleanup Operations

### Clean Docker system
```bash
# Remove unused containers, networks, images
docker system prune -f

# Also remove unused volumes
docker system prune -f --volumes

# Remove all unused images (not just dangling)
docker system prune -a -f
```

### Clean Breeze caches
```bash
breeze cleanup --all
```

### Remove specific Breeze artifacts
```bash
# Clean parameter cache
breeze cleanup

# Clean with additional options
breeze cleanup --cleanup-mypy-cache
```

## CI Debugging

### Check container status
```bash
docker ps -a
docker logs <container_id>
```

### Check Breeze container logs
```bash
docker logs $(docker ps -q --filter "name=breeze")
```

### Run with debug output
```bash
breeze --verbose shell
```

### Check environment variables
```bash
breeze exec "env | sort"
```

## GitHub Actions Specific

### Typical CI workflow commands
```bash
# Start of CI job - free space
breeze ci free-space

# Check resources before tests
breeze ci resource-check

# Build image
breeze ci-image build --github-cache-mode pull

# Run selective checks
breeze ci selective-check --commit-ref $GITHUB_SHA

# After tests - fix ownership
breeze ci fix-ownership
```

### Debug CI failures locally
```bash
# Reproduce CI environment
breeze ci-image build --force-build
breeze shell --backend postgres --python 3.11

# Inside shell, run the failing test
pytest tests/path/to/failing_test.py -v
```

## Pre-commit Checks

### Run all pre-commit checks
```bash
pre-commit run --all-files
```

### Run specific check
```bash
pre-commit run pylint --all-files
pre-commit run mypy --all-files
pre-commit run ruff --all-files
```

### Update pre-commit hooks
```bash
pre-commit autoupdate
```

## Tips
- Run `resource-check` before intensive operations
- Use `free-space` in CI before building images
- `fix-ownership` resolves permission issues on Linux
- Selective checks optimize CI by running only necessary tests
- Always clean up resources after CI runs
- Use `--verbose` for debugging CI issues
- Check Docker daemon logs for container issues
- Pre-commit hooks run the same checks as CI
