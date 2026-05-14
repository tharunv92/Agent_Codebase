-- Merge/upsert logic for demo pipeline
-- Parameterize ds
 -- Uses :ds as a placeholder (when executed from Python, it's replaced safely)

-- 1) Upsert into analytics.fct_events (de-duped in staging)
-- Delete any existing rows for the ds to make the run idempotent.
BEGIN;

DELETE FROM analytics.fct_events
USING raw.events_staging s
WHERE analytics.fct_events.event_id = s.event_id
  AND s.dt = ':{ds}';

INSERT INTO analytics.fct_events (event_id, event_ts, user_id, event_type, dt, payload_json, updated_at)
SELECT
  event_id,
  event_ts,
  user_id,
  event_type,
  dt,
  payload_json,
  GETDATE() AS updated_at
 FROM raw.events_staging
 WHERE dt = ':{ds}';

-- 2) Recompute daily aggregate for the day id
DELETE FROM analytics.fct_event_counts_daily
WHERE dt = ':{ds}';

INSERT INTO analytics.fct_event_counts_daily (dt, event_type, cnt, updated_at)
SELECT
  dt,
  event_type,
  COUNT(1) AS cnt,
  GETDATE() AS updated_at
FROM analytics.fct_events
WHERE dt = ':{ds}'
GROUP BY 1,2;

COMMIT;
