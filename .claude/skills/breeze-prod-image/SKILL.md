---
name: breeze-prod-image
description: Build, pull, verify, and manage production Airflow Docker images. Use this skill for creating production-ready Airflow images.
argument-hint: [operation] [options]
allowed-tools: Bash
---

# Breeze Production Image Management

Manage production Docker images for deploying Airflow.

## Build Production Image

### Default build
```bash
breeze prod-image build
```

### Build for specific Python version
```bash
breeze prod-image build --python 3.10
breeze prod-image build --python 3.11
breeze prod-image build --python 3.12
```

### Build from sources (current code)
```bash
breeze prod-image build --install-airflow-version ""
```

### Build with specific Airflow version
```bash
breeze prod-image build --install-airflow-version 2.8.0
breeze prod-image build --install-airflow-version 2.9.0
```

### Build with specific extras
```bash
breeze prod-image build --airflow-extras "celery,redis,postgres,amazon"
```

### Build for specific platform
```bash
breeze prod-image build --platform linux/amd64
breeze prod-image build --platform linux/arm64
breeze prod-image build --platform "linux/amd64,linux/arm64"
```

### Force rebuild
```bash
breeze prod-image build --force-build
```

## Build with Constraints

### Use specific constraints
```bash
breeze prod-image build --airflow-constraints-reference constraints-2-8
```

### Build with constraints from main
```bash
breeze prod-image build --airflow-constraints-reference constraints-main
```

### Skip constraints
```bash
breeze prod-image build --install-airflow-with-constraints false
```

## Build with Custom Packages

### Add additional Python packages
```bash
breeze prod-image build --additional-python-deps "pandas numpy scikit-learn"
```

### Add additional apt packages
```bash
breeze prod-image build --additional-runtime-apt-deps "libpq-dev gcc"
```

### Add dev apt dependencies
```bash
breeze prod-image build --additional-dev-apt-deps "build-essential"
```

## Pull Production Image

### Pull default image
```bash
breeze prod-image pull
```

### Pull specific Python version
```bash
breeze prod-image pull --python 3.11
```

### Pull and verify
```bash
breeze prod-image pull --verify
```

## Verify Production Image

### Verify image
```bash
breeze prod-image verify
```

### Verify specific Python version
```bash
breeze prod-image verify --python 3.11
```

## Save and Load Images

### Save image to file
```bash
breeze prod-image save --file /tmp/prod-image.tar
```

### Load image from file
```bash
breeze prod-image load --file /tmp/prod-image.tar
```

## Advanced Build Options

### Build with custom image tag
```bash
breeze prod-image build --image-tag my-custom-tag
```

### Build with GitHub cache
```bash
breeze prod-image build --github-cache-mode pull
```

### Build slim image (no build dependencies)
```bash
breeze prod-image build --disable-airflow-repo-cache
```

### Full production build example
```bash
breeze prod-image build \
  --python 3.11 \
  --platform linux/amd64 \
  --airflow-extras "celery,redis,postgres,amazon,google" \
  --additional-python-deps "pandas pyarrow" \
  --install-airflow-version 2.9.0 \
  --force-build \
  --verbose
```

## Check Image Information

### List production images
```bash
docker images | grep -E "apache/airflow.*prod"
```

### Inspect image
```bash
docker inspect apache/airflow:prod-python3.11
```

### Check installed packages in image
```bash
docker run --rm apache/airflow:prod-python3.11 pip list
```

### Check Airflow version in image
```bash
docker run --rm apache/airflow:prod-python3.11 airflow version
```

## Tips
- Production images are optimized for size and security
- Use `--airflow-extras` to include only necessary providers
- Multi-platform builds create images for different architectures
- CI images contain dev tools; prod images are lean for deployment
- Always verify production images before deployment
- Use constraints to ensure reproducible builds
- The `--additional-python-deps` flag adds packages at build time
- Consider using official images from Docker Hub for production
