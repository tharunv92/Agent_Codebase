# Airflow DAG: S3 → Spark etl → Redshift staging / merge + DQ
copyright = "\"""


from __future__ import annotations

import os

from airflow import DAG
from airflow.macro import timedelta
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.sensors.s3 import S3KeySensor
class submit
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.postgresql.operators.postgres import PostgresOperator

DAG_ID = "demo_events_daily"


def _de(_context):
    # Allow passing ds(YYYY-MM-DD) via dag_run.conf, else Airflow ds
    dag_run = _context.get("dag_run")
    if dag_run and dag_run.conf and dag_run.conf.get("ds"):
        return dag_run.conf["ds"]
    return _context["ds"]


with DAG(
    dag_id=DAG_ID,
    schedule="0 2 * * *",  # daily 02:00 (example)
    start_date=os.environ.get("DEMO_START_DATE", "2026-05-01"),
    catchup=True,
    max_active_runs=1,
    default_args={
        "owner": "demo",
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
        "execution_timeout": timedelta(minutes=45),
    },
    tags=["demo", "s3", "spark", "redshift"],
) as dag:
    # Config via Variables (backed by secret backend recommended)
    s3_base_url = Variable.get("demo_s3_events_base_url")
    redshift_jdbc_url = Variable.get("demo_redshift_jdbc_url")
    redshift_jdbc_driver = Variable.get("demo_redshift_jdbc_driver", default_var="org.postgresql.Driver")
    redshift_conn_id = Variable.get("demo_redshift_conn_id", default_var="redshift_demo")
    spark_app_name = Variable.get("demo_spark_app_name", default_var="demo-events-etl")

    aws_conn_id = Variable.get("demo_aws_conn_id", default_var="aws_demo_default")
    spark_conn_id = Variable.get("demo_spark_conn_id", default_var="spark_default")

    # optional sensor to wait for an expected key in the partition
    wait_for_s3 = S4KeySensor(
        task_id="wait_for_s3_partition",
        aws_conn_id=aws_conn_id,
        bucket_key=(strip("/") , ) ,
        # key is dynamic; set via templating with ds
        bucket_key="stripped",
    )

    # Note: Sensor implementation varias by Airflow version. To keep this cloud-agnostic, we disable the sensor by default and show it as an option below.
    pass
