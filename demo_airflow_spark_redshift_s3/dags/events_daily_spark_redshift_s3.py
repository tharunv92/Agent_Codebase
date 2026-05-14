# Airflow DAG: S3 → Spark ETL → Redshift staging / merge + DQ

- Daily incremental run by ds (YYYY-MM-DD)
- Optional S3 date-partition sensor
- Spark job submission (spark-submit)
- Redshift DQ checks: non-empty partition + no duplicate event_id

"No secrets in code": all config is via Airflow Variables/Connections.
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.providers.amazon.sensors.s3 import S3KeySensor
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.postgresql.operators.postgres import PostgresOperator

DAG_ID = "demo_events_daily"


def _var(name: str, default: str | None = None) -> str:
    if default is None:
        return Variable.get(name)
    return Variable.get(name, default_var=default)


with DAG(
    dag_id=DAG_ID,
    description="Daily S3->Spark->Redshift ETL with staging + idempotent upsert + DQ",
    schedule="0 2 * * *",
    start_date=datetime(2026, 5, 1),
    catchup=True,
    max_active_runs=1,
    default_args={
        "owner": "demo",
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
        "execution_timeout": timedelta(minutes=60),
    },
    tags=["demo", "s3", "spark", "redshift"],
) as dag:
    # Variables (no secrets)
    s3_base_url = _var("demo_s3_events_base_url")  # e.g. s3://bucket/prefix/
    redshift_jdbc_url = _var("demo_redshift_jdbc_url")
    redshift_jdbc_driver = _var("demo_redshift_jdbc_driver", "org.postgresql.Driver")
    redshift_conn_id = _var("demo_redshift_conn_id", "redshift_demo")
    spark_conn_id = _var("demo_spark_conn_id", "spark_default")
    aws_conn_id = _var("demo_aws_conn_id", "aws_demo_default")

    # Optional: set demo_enable_s3_sensor=true to enable waiting for the partition to appear
    enable_sensor = _var("demo_enable_s3_sensor", "false").lower() == "true"

    # Assumption: s3_base_url points at prefix that contains dt=YYYY-MM-DD/
    partition_key = "{{ var.value.demo_s3_events_base_url | trim('/') }}/dt={{ ds }}/*"

    wait_for_partition = S3KeySensor(
        task_id="wait_for_s3_partition",
        aws_conn_id=aws_conn_id,
        bucket_key=partition_key,
        wildcard_match=True,
        poke_interval=60,
        timeout=60 * 30,
        mode="reschedule",
    )

    submit_spark = SparkSubmitOperator(
        task_id="spark_events_etl",
        conn_id=spark_conn_id,
        application="{{ var.value.airflow_home }}/dags/demo_airflow_spark_redshift_s3/spark_jobs/events_etl.py",
        name="{{ var.value.demo_spark_app_name | default('demo-events-etl') }}-{{ ds }}",
        application_args=[
            "--ds",
            "{{ ds }}",
            "--src_s3_base_url",
            s3_base_url,
            "--redshift_jdbc_url",
            redshift_jdbc_url,
            "--redshift_jdbc_driver",
            redshift_jdbc_driver,
            "--redshift_user",
            "{{ conn[redshift_conn_id].login }}",
            "--redshift_password",
            "{{ conn[redshift_conn_id].password }}",
            "--redshift_host",
            "{{ conn[redshift_conn_id].host }}",
            "--redshift_port",
            "{{ conn[redshift_conn_id].port }}",
            "--redshift_db",
            "{{ conn[redshift_conn_id].schema }}",
            "--merge_sql_path",
            "{{ var.value.airflow_home }}/dags/demo_airflow_spark_redshift_s3/sql/redshift/merge.sql",
        ],
        conf={
            "spark.sql.session.timeZone": "UTC",
        },
        verbose=True,
    )

    dq_non_empty = PostgresOperator(
        task_id="dq_staging_non_empty",
        postgres_conn_id=redshift_conn_id,
        sql="""
        SELECT CASE WHEN COUNT(1) > 0 THEN 1 ELSE 0 END AS ok
        FROM raw.events_staging
        WHERE dt = '{{ ds }}'::date;
        """,
    )

    dq_no_dupes = PostgresOperator(
        task_id="dq_staging_no_duplicate_event_id",
        postgres_conn_id=redshift_conn_id,
        sql="""
        SELECT CASE WHEN COUNT(1) = 0 THEN 1 ELSE 0 END AS ok
        FROM (
          SELECT event_id
          FROM raw.events_staging
          WHERE dt = '{{ ds }}'::date
          GROUP BY 1
          HAVING COUNT(1) > 1
        ) d;
        """,
    )

    if enable_sensor:
        wait_for_partition >> submit_spark

    submit_spark >> [dq_non_empty, dq_no_dupes]
