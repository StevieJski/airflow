---
name: breeze-setup
description: Install, configure, and maintain Breeze itself - setup autocomplete, upgrade, manage configuration. Use this skill for Breeze installation and maintenance.
argument-hint: [operation] [options]
allowed-tools: Bash
---

# Breeze Setup & Configuration

Install, configure, and maintain the Breeze development environment.

## Installation

### Install Breeze (recommended - using uv)
```bash
cd dev/breeze
uv tool install -e . --force
```

### Install Breeze (alternative - using pipx)
```bash
cd dev/breeze
pipx install -e .
```

### Verify installation
```bash
breeze version
breeze --help
```

## Self-Upgrade

### Upgrade Breeze to latest
```bash
breeze setup self-upgrade
```

### Force reinstall
```bash
cd dev/breeze
uv tool install -e . --force
```

## Autocomplete Setup

### Install bash autocomplete
```bash
breeze setup autocomplete --shell bash
```

### Install zsh autocomplete
```bash
breeze setup autocomplete --shell zsh
```

### Install fish autocomplete
```bash
breeze setup autocomplete --shell fish
```

### Reload shell after autocomplete setup
```bash
# Bash
source ~/.bashrc

# Zsh
source ~/.zshrc
```

## Configuration

### View current configuration
```bash
breeze setup config
```

### Set configuration options
```bash
# Set default Python version
breeze setup config --python 3.11

# Set default backend
breeze setup config --backend postgres
```

## Environment Verification

### Check prerequisites
```bash
# Docker
docker --version
docker compose version

# Python
python --version

# Breeze
breeze version
```

### Verify Docker is running
```bash
docker info
```

### Check Docker resources
```bash
# Memory and CPU available to Docker
docker info --format '{{.MemTotal}}'
docker system info
```

## Cleanup and Reset

### Full cleanup
```bash
breeze cleanup --all
```

### Clean specific components
```bash
# Clean parameter cache
breeze cleanup

# Clean mypy cache
breeze cleanup --cleanup-mypy-cache
```

### Uninstall Airflow (from Breeze environment)
```bash
breeze cleanup --uninstall-airflow
```

### Remove Breeze completely
```bash
# If installed with uv
uv tool uninstall airflow-breeze

# If installed with pipx
pipx uninstall airflow-breeze
```

## Sync and Regenerate

### Sync Docker mounts
```bash
breeze setup sync-mounts
```

### Regenerate command documentation images
```bash
breeze setup regenerate-command-images
```

### Check parameter groups
```bash
breeze setup check-all-params-in-groups
```

## First-Time Setup Checklist

```bash
# 1. Clone Airflow repository
git clone https://github.com/apache/airflow.git
cd airflow

# 2. Install Breeze
cd dev/breeze
uv tool install -e . --force
cd ../..

# 3. Setup autocomplete (optional but recommended)
breeze setup autocomplete --shell bash

# 4. Verify installation
breeze version

# 5. Pull or build CI image
breeze ci-image pull
# or
breeze ci-image build

# 6. Enter Breeze shell
breeze shell
```

## Environment Variables

### Common Breeze environment variables
```bash
# Skip image upgrade check
export SKIP_IMAGE_UPGRADE_CHECK=true

# Set default Python
export PYTHON_MAJOR_MINOR_VERSION=3.11

# Set default backend
export BACKEND=postgres

# Enable verbose output
export VERBOSE=true
```

### Add to shell profile
```bash
# Add to ~/.bashrc or ~/.zshrc
export SKIP_IMAGE_UPGRADE_CHECK=true
```

## Troubleshooting

### Reset Breeze state
```bash
breeze cleanup --all
breeze ci-image pull
```

### Fix permission issues
```bash
breeze ci fix-ownership
```

### Docker issues
```bash
# Restart Docker daemon
sudo systemctl restart docker

# Check Docker status
sudo systemctl status docker
```

### Network issues
```bash
# Reset Docker networks
docker network prune -f
```

## Tips
- Run `breeze setup autocomplete` for better CLI experience
- Use `breeze cleanup` when things get stuck
- Environment variables can set defaults to avoid typing flags
- Keep Breeze updated with `self-upgrade`
- Check Docker resources if builds are slow or failing
- Breeze requires Docker Compose v2
- Minimum recommended: 4GB RAM, 40GB disk for Docker
