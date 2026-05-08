# Drive → S3 Ingestion Pipeline (configurable)

This is a generic, production-oriented ingestion pipeline to pull files from Google Drive (Including Shared Drives) and land them in Amazon S3 in an idempotent, incremental way.


## What it does
- Lists files in one or more Drive folders (folder IDs)
- Filters incrementally using a watermark (drive `modifiedTime`) stored in S3
- Downloads binary files via Drive API, or exports Google Docs/Sheets to a chosen format
- Writes to S3 with deterministic keys and a run manifest (JSONL)
- Skips re-download/re-upload when the same (`file_id`, `modifiedTime`) version has already been ingested


## S3 layout (default)
```
s3://<BUKET>/<PREFIX>/
  raw/drive/files/
    file_id=</<file_id>/
      version_ts=</<2026-01-01T12:34:56Z>/
      original_name=<url-encoded>
      /...
  raw/drive/manifests/run_ts=<iso>run.jsonl
  state/watermark.json
```

> The script is idempotent: running again for the same source state won't duplicate bytes.

## Quick start
### 1) Install
```bash
cd pipelines/drive_to_s3
python -m pip install -u
pip install -e .
```

### 2) Configure env variables
```bash
# Google Drive auth
# Option A: Point to a service account key JSON file path
DTK=edit_me
GDRIVE_SERVICE_ACCOUNT_KEY_FILE=/secrets/gs_creds.json
GDIRE_FOLDER_IDS="1Aaa...,2Bbb..."

# Allow Shared Drives (required for Shared Drive)
GDRIVE_SUPPORTS_ALL_DRIVES=true


# S3 target
AWS_REGION=us-east-1
S3_BUCKET=my-bucket
S3_PREFIX=raw

# Optional: watermark state location (will default under S3_PREFIX)
CONTENT_STATE_KEY=state/watermark.json

# Export formats for Google editor types
FORMAT_GDOC=application/pdf
FORMAT_GSHEET=text/csv
FORMAT_GSLIDE=application/pdf

# Runtime
LOG_LEVEL=INFO
MAX_FILES=1000
MAX_BYTES=500000000 # 500MBs per run (optional cap)
ENABLE_DATE_PARTITIONING=true
```

### 3) Run it
```bash
drive-to-s3
```

## Auth options (no secrets in repo)
This project supports two common approaches:

- **Service Account** (best for batch/scheduled runs): store the JSON in your secret store and mount it on the runner, expose only the file path via `GDRIVE_SERVICE_ACCOUNT_KEY_FILE`.
  - Service account must have access to the target folder(s) or shared drive.
  - For Shared Drives, enable `GDRIVE_SUPPORTS_ALL_DRIVES}=true` and provide `DdriveId` if you want to restrict to a single drive.
- **Application Default Credentials(** (local dev): set up h `gcloud auth application-default login`.

## Notes
- Incremental is based on Drive `modifiedTime`. If you need stricter dedupe, configure to also check `md5checksum`.
- The pipeline writes a manifest file per run for audit/replay.
 