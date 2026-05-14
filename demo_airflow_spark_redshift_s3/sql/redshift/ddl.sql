-- Redshift DDL for demo pipeline

-- Schemas
CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS analytics;

-- Staging table (raw)
-- Note: using VARCHAR for flexibility in demo. Tune data types in prod.
CREATE TABLE IF NOT EXISTS raw.events_staging (
  event_id VARCHAR(256)   NOT NULL,
  event_ts TIMESTAMP    NOT NULL,
  user_id VARCHAR(256),
  event_type VARCHAR(256),
  dt DATE            NOT NULL,
  payload_json VARCHAR(65535),
  ingested_at TIMESTAMP DEFAULT GETDATE()
)
DISTSTYLE AUTO;

-- Fact table
CREATE TABLE IF NOT EXISTS analytics.fct_events (
  event_id VARCHAR(256) NOT NULL,
  event_ts TIMESTAMP NOT NULL,
  user_id VARCHAR(256),
  event_type VARCHAR(256),
  dt DATE NOT NULL,
  payload_json VARCHAR(65535),
  updated_at TIMESTAMP DEFAULT GETDATE(),
  PRIMARY KEY (event_id)
)
DISTSTYLE KEY DISTKEY(event_id) SORTKEY(dt,event_ts);

-- Daily aggregate fact
CREATE TABLE IF NOT EXISTS analytics.fct_event_counts_daily (
  dt DATE NOT NULL,
  event_type VARCHAR(256) NOT NULL,
  cnt BIGINT NOT NULL,
  updated_at TIMESTAMP DEFAULT GETDATE(),
  PRIMARY KEY (dt, event_type)
)
DISTSTYLE AUTO;
