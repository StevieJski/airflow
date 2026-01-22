---
name: breeze-docs
description: Build and manage Airflow documentation using Breeze - build docs, check spelling, publish. Use this skill for documentation tasks.
argument-hint: [operation] [options]
allowed-tools: Bash
---

# Breeze Documentation Commands

Build, verify, and manage Airflow documentation.

## Build Documentation

### Build all documentation
```bash
breeze build-docs
```

### Build specific package documentation
```bash
# Core Airflow docs
breeze build-docs --package-filter apache-airflow

# Specific provider docs
breeze build-docs --package-filter apache-airflow-providers-amazon
breeze build-docs --package-filter apache-airflow-providers-google
breeze build-docs --package-filter apache-airflow-providers-microsoft-azure

# Helm chart docs
breeze build-docs --package-filter helm-chart
```

### Build multiple packages
```bash
breeze build-docs --package-filter apache-airflow --package-filter helm-chart
```

### Build with spell checking
```bash
breeze build-docs --spellcheck-only
```

### Build docs for all providers
```bash
breeze build-docs --package-filter "apache-airflow-providers-*"
```

## Documentation Options

### Fast build (skip some checks)
```bash
breeze build-docs --fast
```

### Build with warnings as errors
```bash
breeze build-docs --docs-only
```

### Clean build (remove cached docs)
```bash
breeze build-docs --clean
```

### Verbose output
```bash
breeze build-docs --verbose
```

## View Built Documentation

### Documentation output location
```bash
# Docs are built to:
ls docs/_build/
```

### Open docs locally
```bash
# After building, open in browser
python -m http.server 8000 --directory docs/_build/
# Then visit http://localhost:8000
```

## Publishing Documentation

### Publish docs to S3
```bash
breeze release-management publish-docs-to-s3
```

### Trigger publish workflow
```bash
breeze workflow-run publish-docs
```

## Spell Checking

### Run spell check only
```bash
breeze build-docs --spellcheck-only
```

### Add words to dictionary
```bash
# Add to docs/spelling_wordlist.txt
echo "newword" >> docs/spelling_wordlist.txt
```

## Link Checking

### Check for broken links
```bash
breeze build-docs --link-check
```

## UI Translations

### Check translation completeness
```bash
breeze ui check-translation-completeness
```

## Common Documentation Tasks

### Build and preview locally
```bash
# Build docs
breeze build-docs --package-filter apache-airflow

# Start local server
cd docs/_build && python -m http.server 8000
```

### Fix documentation issues
```bash
# Build with all checks
breeze build-docs --package-filter apache-airflow

# Review errors in output
# Fix RST/Markdown issues in source files
# Rebuild to verify fixes
```

### Update provider documentation
```bash
# Build specific provider docs
breeze build-docs --package-filter apache-airflow-providers-amazon

# Check the output for warnings/errors
```

## Documentation Structure

### Key documentation directories
```
docs/
├── apache-airflow/           # Core Airflow docs
├── apache-airflow-providers/ # Provider docs
├── helm-chart/               # Helm chart docs
├── _build/                   # Built documentation output
└── spelling_wordlist.txt     # Custom dictionary
```

### Provider documentation
```
providers/
└── <provider>/
    └── docs/
        └── index.rst         # Provider doc entry point
```

## Add Back References

### Add documentation back-references
```bash
breeze release-management add-back-references
```

## Tips
- Build specific packages to save time during development
- Use `--fast` for quicker iteration during doc writing
- Spell check catches common typos - add legitimate words to wordlist
- Documentation uses RST (reStructuredText) format
- Provider docs are in `providers/<name>/docs/`
- Always build docs locally before submitting PRs
- Link checking helps find broken cross-references
- The `_build` directory can be cleaned with `--clean`
