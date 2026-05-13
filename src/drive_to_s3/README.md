# Drive -> S3 ingestion

This package contains a production-oriented, metadata-driven ingestion pipeline to copy files from a Google Drive folder into Amazon S3.

## Features
- Config-driven (YAML) pipeline definition
- Optional manifest to run multiple pipelines
- Idempotent behavior (skip unchanged files; optional keep-existing)
- Structured JSON logging via `structlog`
- Concurrent downloads/uploads

## Quickstart

### 1) Install
```bash
python -m venv .venv
source .venv/bin/activate
pip install -e .
```

### 2) Configure
Edit `config/example.yml` with:
- Google Drive `folder_id`
- S3 `bucket` and `prefix`
- auth mode + credential file path

### 3) Run a single pipeline
```bash
drive-to-s3 run --config config/example.yml
```

### 4) Run multiple pipelines via manifest
Edit `config/manifest.yml`:
```yaml
pipelines:
  - name: default
    config_path: config/example.yml
```

Run:
```bash
drive-to-s3 run-manifest --manifest config/manifest.yml
```

## Credentials (no secrets in repo)
- For Drive service accounts: store the JSON file securely and point to it from config.
- For AWS: use standard AWS credential resolution (env, profiles, IAM role).
