"""Airflow DAG: daily Spark ETL from S3 -> Redshift and DQ checks.

Assumptions:
- Spark is submitted via SparkSubmitOperator to a cluster that can access S3 and Redshift.
- Redshift connection is configured in Airflow as `redshift_default`.
- AWS credentials are handled by the runtime (IAM role) or `aws_default` Airflow connection.

Configure via Airflow Variables (or replace with DAG params):
- events_s3_prefix: e.g. s3://my-bucket/raw/events
- redshift_jdbc_url: e.g. jdbc:redshift://host:5439/dev
- redshift_jdbc_user
- redshift_jdbc_password (recommend secret backend)

The DAG runs for each ds (execution date), reading from:
  {events_s3_prefix}/dt={{ ds }}/
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator


DEFAULT_ARGS = {
    "owner": "data-eng",
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(minutes=60),
}


def _var(name: str, default: str | None = None) -> str:
    v = Variable.get(name, default_var=default)
    if v is None or str(v).strip() == "":
        raise ValueError(f"Airflow Variable '{name}' is required")
    return v


with DAG(
    dag_id="demo_events_daily_spark_redshift_s3",
    start_date=datetime(2025, 1, 1),
    schedule="0 3 * * *",
    catchup=True,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["demo", "events", "spark", "redshift", "s3"],
) as dag:

    # S3 sensor is optional; keep it lightweight by checking for any object in the partition.
    events_s3_prefix = _var("events_s3_prefix")  # s3://bucket/raw/events

    # Parse bucket/key for the sensor
    # Assumes prefix starts with s3://
    _no_scheme = events_s3_prefix.replace("s3://", "", 1)
    bucket = _no_scheme.split("/", 1)[0]
    key_prefix = _no_scheme.split("/", 1)[1] if "/" in _no_scheme else ""

    wait_for_partition = S3KeySensor(
        task_id="wait_for_s3_partition",
        bucket_name=bucket,
        bucket_key=f"{key_prefix}/dt={{{{ ds }}}}/*",
        wildcard_match=True,
        aws_conn_id="aws_default",
        poke_interval=60,
        timeout=60 * 60,
        mode="reschedule",
    )

    spark_etl = SparkSubmitOperator(
        task_id="spark_events_etl",
        application="/opt/airflow/dags/demo_airflow_spark_redshift_s3/spark_jobs/events_etl.py",
        conn_id="spark_default",
        application_args=[
            "--ds",
            "{{ ds }}",
            "--s3-prefix",
            events_s3_prefix,
            "--jdbc-url",
            _var("redshift_jdbc_url"),
            "--jdbc-user",
            _var("redshift_jdbc_user"),
            "--jdbc-password",
            _var("redshift_jdbc_password"),
        ],
        # Add Redshift JDBC driver if your cluster does not already include it.
        # jars="/path/to/redshift-jdbc42.jar",
        name="events_etl",
        verbose=True,
    )

    # DQ checks in Redshift (PostgresOperator works because Redshift is Postgres-compatible)
    dq_non_empty = PostgresOperator(
        task_id="dq_non_empty",
        postgres_conn_id="redshift_default",
        sql="""
        select case when count(*) > 0 then 1 else 0 end as ok
        from analytics.fct_events
        where ds = %(ds)s
        """,
        parameters={"ds": "{{ ds }}"},
    )

    dq_no_dupes = PostgresOperator(
        task_id="dq_no_duplicate_event_id",
        postgres_conn_id="redshift_default",
        sql="""
        select case when count(*) = 0 then 1 else 0 end as ok
        from (
          select event_id
          from analytics.fct_events
          where ds = %(ds)s
          group by 1
          having count(*) > 1
        ) d
        """,
        parameters={"ds": "{{ ds }}"},
    )

    wait_for_partition >> spark_etl >> [dq_non_empty, dq_no_dupes]
