# Airflow + Spark + Redshift + S3 Demo Pipeline

This directory contains a mini end-to-end demo pipeline that shows how to orchestrate a daily incremental ETL with:

- *Airflow* for orchestration
- *PySpark* for transformations
- *S3* for raw input data landing
- *Redshift* for staging + analytics tables


## What this pipeline does

Daily for a partition date (`ds`=YYYY-MM-DD)i:
1) Read CSV files from S3: `s3://<bucket>/raw/events/dt=YYYY-MM-DD/*.csv`
2) Load into Redshift staging table (via JDBC)
3) Dedupe by `event_id` and upsert into `analytics.fct_events` (idempotent merge)
4) Compute a daily aggregate into `analytics.fct_event_counts_daily`
5) Run data quality checks in Airflow (empty day, nulls and dupes)


## Repo layout

- `dags/events_daily_spark_redshift_s3.py`: Airflow DAG
- `spark_jobs/events_etl.py`: PySpark ETL job
m - `sql/redshift/ddl.sql`: init tables
- `sql/redshift/merge.sql`: merge + agg steps
- `pipelines/common/redshift.py`: Redshift connection helpers

## Prerequisites (assumptions)

This is a demo codebase. You'll need to hook it up to your environment by creating Airflow Connections/Variables (do *NOT* commit secrets):

Airflow Connections:
- `redshift_default`: Redshift Postgres-compatible connection
  - host, db/database, user, password/role
- `aws_default`: AWS credentials for S3 (or use IAM Roles on your runtime)

Variables (or DAG params):
- `events_s3_prefix` (e.g. `s3://my-bucket/raw/events`)
- `redshift_jdbc_url` (e.g. `jdbc:redshift://<host>:5439/<db>`)
- `redshift_jdbc_user`, `redshift_jdbc_password` (recommend: use Airflow Conn extras or secret manager)

## Running locally (Spark local mode)

``b
bash
pip install -r requirements.txt
# example: run the spark job locally (you'll need to config jdbc access)
spark-submit \
  --master local[2] \
  spark_jobs/events_etl.py \
  --ds 2026-05-14 \
  --s3-prefix s3://my-bucket/raw/events \
  --jdbc-url "jdbc:redshift://..." \
  --jdbc-user user \
  --jdbc-password "not-set"
```
