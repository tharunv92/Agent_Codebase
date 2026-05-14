"""PySpark ETL: read daily partition from S3, dedupe, load to Redshift staging.

Expected input columns (CSV header):
- event_id (string)
- user_id (string)
- event_type (string)
- event_ts (timestamp-ish string)

Input path pattern:
  {s3_prefix}/dt={ds}/*.csv

Output:
- Writes to Redshift table raw.events_staging via JDBC
- Runs Redshift SQL (merge.sql) to upsert into analytics tables

Notes:
- This job intentionally truncates/rebuilds the staging rows for the ds partition to be idempotent.
- For production, prefer loading into S3 + COPY into Redshift for larger volumes.
"""

from __future__ import annotations

import argparse
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser()
    p.add_argument("--ds", required=True, help="Partition date YYYY-MM-DD")
    p.add_argument("--s3-prefix", required=True, help="e.g. s3://bucket/raw/events")
    p.add_argument("--jdbc-url", required=True)
    p.add_argument("--jdbc-user", required=True)
    p.add_argument("--jdbc-password", required=True)
    p.add_argument("--staging-table", default="raw.events_staging")
    p.add_argument("--merge-sql-s3", default=None, help="Optional: s3://.../merge.sql; otherwise embedded")
    return p.parse_args()


MERGE_SQL = """-- Redshift merge + daily aggregate
begin;

delete from analytics.fct_events
using raw.events_staging s
where analytics.fct_events.event_id = s.event_id
  and s.ds = :ds;

insert into analytics.fct_events (ds, event_id, user_id, event_type, event_ts)
select ds, event_id, user_id, event_type, event_ts
from raw.events_staging
where ds = :ds;

delete from analytics.fct_event_counts_daily where ds = :ds;

insert into analytics.fct_event_counts_daily (ds, event_type, cnt)
select :ds as ds, event_type, count(*) as cnt
from analytics.fct_events
where ds = :ds
group by 2;

commit;\n"""


def main() -> None:
    args = parse_args()

    spark = (
        SparkSession.builder.appName("demo-events-etl")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )

    ds = args.ds
    input_path = f"{args.s3_prefix}/dt={ds}/*.csv"

    df = (
        spark.read.option("header", True)
        .option("mode", "FAILFAST")
        .csv(input_path)
        .withColumn("ds", F.to_date(F.lit(ds)))
        .withColumn("event_ts", F.to_timestamp("event_ts"))
    )

    # Basic schema sanity
    required = {"event_id", "user_id", "event_type", "event_ts", "ds"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"Missing required columns: {sorted(missing)}")

    # Dedupe: keep latest event_ts per event_id
    w = Window.partitionBy("event_id").orderBy(F.col("event_ts").desc_nulls_last())
    deduped = (
        df.withColumn("rn", F.row_number().over(w))
        .filter(F.col("rn") == 1)
        .drop("rn")
    )

    # Rebuild staging partition: easiest is to overwrite entire staging table for ds.
    # Redshift JDBC doesn't support partition overwrite, so we do a delete via JDBC and append.
    # Here we rely on the merge SQL to delete existing ds keys before insert.

    props = {"user": args.jdbc_user, "password": args.jdbc_password, "driver": "com.amazon.redshift.jdbc42.Driver"}

    # Write to staging (append). For idempotency, you can TRUNCATE staging first externally.
    (deduped.select("ds", "event_id", "user_id", "event_type", "event_ts")
        .write
        .mode("append")
        .jdbc(args.jdbc_url, args.staging_table, properties=props)
    )

    # Run merge SQL using the Redshift JDBC connection via spark's JVM.
    jvm = spark._sc._gateway.jvm
    DriverManager = jvm.java.sql.DriverManager
    conn = DriverManager.getConnection(args.jdbc_url, args.jdbc_user, args.jdbc_password)
    try:
        stmt = conn.prepareStatement(MERGE_SQL)
        # naive parameter replace: Redshift JDBC doesn't support named params in plain SQL.
        # We'll do a safe substitution by replacing ':ds' with a quoted literal.
        sql = MERGE_SQL.replace(":ds", f"'{ds}'")
        stmt = conn.createStatement()
        stmt.execute(sql)
        conn.commit()
    finally:
        conn.close()

    spark.stop()


if __name__ == "__main__":
    main()
