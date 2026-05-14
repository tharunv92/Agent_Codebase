import pytest
from pyspark.sql import SparkSession

from demo_airflow_spark_redshift_s3.spark_jobs.events_etl import transform_dedupe


@pytest.fixture(scope="session")
def spark():
    spark = (
        SparkSession.builder
        .master("local[2]")
        .appName("unit-tests")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    yield spark
    spark.stop()


def test_transform_dedupe_keeps_latest(spark):
    rows = [
        {"event_id": "a1", "event_ts": "2026-05-14T10:00:00Z", "user_id": "u1", "event_type": "click", "dt": "2026-05-14"},
        {"event_id": "a1", "event_ts": "2026-05-14T11:00:00Z", "user_id": "u2", "event_type": "click", "dt": "2026-05-14"},
        {"event_id": "b1", "event_ts": "2026-05-14T09:00:00Z", "user_id": "u3", "event_type": "view", "dt": "2026-05-14"},
    ]
    df = spark.createDataFrame(rows)
    out = transform_dedupe(df)

    collected = {r["event_id"]: r.asDict() for r in out.collect()}
    assert set(collected.keys()) == {"a1", "b1"}
    assert collected["a1"]["user_id"] == "u2"
