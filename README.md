# Drive -> S3 ingestion

Config-driven ingestion pipeline to download files from a Google Drive folder and upload them into S3.

## Features
- Config-driven (YAML)
- Recursive folder traversal
- Include/exclude glob patterns
- Concurrency with per-file error isolation
- Idempotency:
  - optional `skip_if_unchanged` based on Drive `modifiedTime` + `size`
  - optional `keep_existing` to not overwrite existing S3 keys
- Structured JSON logs
- Local state store (`./state/drive_to_s3-state.json` by default)

## Prereqs
- Python 3.10+
- AWS credentials available in environment (AWS_PROFILE / env vars / IAM role)
- Google Drive API access:
  - Service account JSON file OR
  - OAuth refresh token JSON file

## Usage

1) Install
```bash
python -m venv .venv
source .venv/bin/activate
pip install -e .
```

2) Copy and edit config
```bash
cp config/example.yml config/my_run.yml
```

3) Run
```bash
drive-to-s3 --config config/my_run.yml
```

Or via env:
```bash
export DRIVE_TO_S3_CONFIG=config/my_run.yml
drive-to-s3
```

## Config
See `config/example.yml`.

### Credentials (recommended)
- Do not commit secrets.
- Use file paths for Google credentials.
- Use AWS credentials via IAM role / profile / env vars.
