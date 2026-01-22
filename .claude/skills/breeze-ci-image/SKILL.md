---
name: breeze-ci-image
description: Build, pull, verify, and manage Breeze CI Docker images. Use this skill for CI image operations during development and testing.
argument-hint: [operation] [options]
allowed-tools: Bash
---

# Breeze CI Image Management

Manage CI (development) Docker images used for testing and development.

## Build CI Image

### Default build
```bash
breeze ci-image build
```

### Build for specific Python version
```bash
breeze ci-image build --python 3.10
breeze ci-image build --python 3.11
breeze ci-image build --python 3.12
```

### Build with specific platform
```bash
breeze ci-image build --platform linux/amd64
breeze ci-image build --platform linux/arm64
```

### Force rebuild (ignore cache)
```bash
breeze ci-image build --force-build
```

### Build with GitHub cache
```bash
breeze ci-image build --github-cache-mode pull
breeze ci-image build --github-cache-mode disabled
```

### Build with specific Airflow constraints
```bash
breeze ci-image build --airflow-constraints-reference constraints-main
```

### Full build with multiple options
```bash
breeze ci-image build \
  --python 3.11 \
  --platform linux/amd64 \
  --force-build \
  --verbose
```

## Pull CI Image

### Pull default image
```bash
breeze ci-image pull
```

### Pull for specific Python version
```bash
breeze ci-image pull --python 3.11
```

### Pull from specific registry
```bash
breeze ci-image pull --image-tag latest
```

### Pull and verify
```bash
breeze ci-image pull --verify
```

## Verify CI Image

### Verify image integrity
```bash
breeze ci-image verify
```

### Verify specific Python version
```bash
breeze ci-image verify --python 3.11
```

## Save and Load Images

### Save image to file
```bash
breeze ci-image save --file /tmp/ci-image.tar
```

### Save specific Python version
```bash
breeze ci-image save --python 3.11 --file /tmp/ci-image-3.11.tar
```

### Load image from file
```bash
breeze ci-image load --file /tmp/ci-image.tar
```

## Cache Management

### Export mount cache
```bash
breeze ci-image export-mount-cache
```

### Import mount cache
```bash
breeze ci-image import-mount-cache
```

## Advanced Options

### Build with extra pip packages
```bash
breeze ci-image build --additional-pip-install-flags "--extra-index-url https://..."
```

### Build with specific extras
```bash
breeze ci-image build --airflow-extras "celery,redis,postgres"
```

### Skip image verification
```bash
breeze ci-image build --skip-image-upgrade-check
```

### Use specific builder
```bash
breeze ci-image build --builder autodetect
breeze ci-image build --builder docker-buildx
```

## Check Image Information

### List available CI images
```bash
docker images | grep -E "apache/airflow.*ci"
```

### Inspect image
```bash
docker inspect apache/airflow:ci-python3.11
```

### Check image size
```bash
docker images apache/airflow --format "{{.Repository}}:{{.Tag}} {{.Size}}"
```

## Tips
- CI images are used for development and testing, not production
- Use `--force-build` when you need to rebuild from scratch
- Pull images from registry to save build time
- The CI image includes all development dependencies
- Use `--verbose` for detailed build output
- Build times vary based on cache availability
- Multi-platform builds take longer but support different architectures
- Save/load is useful for sharing images between machines
