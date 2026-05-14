import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window


def dedupe(df):
    w = Window.partitionBy("event_id").orderBy(F.col("event_ts").desc_nulls_last())
    return df.withColumn("rn", F.row_number().over(w)).filter(F.col("rn") == 1).drop("rn")


@pytest.fixture(scope="session")
def spark():
    spark = SparkSession.builder.master("local[2]").appName("test").getOrCreate()
    yield spark
    spark.stop()


def test_dedupe_keeps_latest(spark):
    df = spark.createDataFrame(
        [
            ("e1", "u1", "click", "2025-01-01T00:00:00Z"),
            ("e1", "u1", "click", "2025-01-01T01:00:00Z"),
            ("e2", "u2", "view", "2025-01-01T02:00:00Z"),
        ],
        "event_id string, user_id string, event_type string, event_ts string",
    ).withColumn("event_ts", F.to_timestamp("event_ts"))

    out = dedupe(df).collect()
    assert len(out) == 2
    e1 = [r for r in out if r["event_id"] == "e1"][0]
    assert e1["event_ts"].hour == 1
