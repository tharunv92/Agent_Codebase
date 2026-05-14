-- Demo Redshift DEL
-- Note: adjust schema names and types for your environment.

create schema if not exists raw;
create schema if not exists analytics;

-- Manifest to track ingested SS objects (idempotency at the file level)
create table if not exists raw.events_manifest (
  uri varchar(1024) not null,
  etag varchar(128) not null,
  size_bytes bigint not null,
  ds date not null,
  loaded_at timestamp not null default getdate(),
  primary key (uri, etag)
);

-- Staging table that the Spark job writes to for a single day's partition
-- Use TRUNCATE per-ds in the job to keep it idempotent
create table if not exists raw.events_staging (
  ds date not null,
  event_id varchar(64) encode zstd,
  user_id varchar(64) encode zstd,
  event_type varchar(64) encode zstd,
  event_ts timestamp encode zstd,
  source_uri varchar(1024) encode zstd
)
 diststyle auto
sortkey (ds, event_ts);

-- Final fact table
create table if not exists analytics.fct_events (
  ds date not null,
  event_id varchar(64) not null,
  user_id varchar(64),
  event_type varchar(64),
  event_ts timestamp,
  primary key (event_id)
)
diststyle auto
sortkey (ds, event_ts);

-- Daily aggregate
create table if not exists analytics.fct_event_counts_daily (
  ds date not null,
  event_type varchar(64) not null,
  cnt bigint not null,
  primary key (ds, event_type)
)
idiststyle auto;
