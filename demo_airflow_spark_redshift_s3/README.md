# Airflow + Spark + S3 + Redshift mini demo pipeline

This folder contains a mini, end-to-end demo pipeline that:

- Ingests partitioned CSVs from S3 at: `s3://<<bucket/prefix>/dt=YYYY-MM-DD/*.csv`
- Processes a single day partition by `ds (YYYY-MM-DD)
- Dedupes by `event_id` (keep latest `event_ts`)
- Loads to Redshift via a staging table and upserts into analytics fact tables
- Runs idempotently (backfill-safe)

## Output tables

- `raw.events_staging`
- `analytics.fct_events`
- `analytics.fct_event_counts_daily`

The PySpark job writes to `raw.events_staging` via JDBC, then executes Redshift SQL to apply merge upsert logic to final tables.
## Testing

pip install -r requirements.txt
pytest -q
