-- Redshift merge + daily aggregate
-- Execute in a transaction from Airflow.

begin;

-- Upsert (idempotent for the partition): delete matching event_ids for ds, then insert.
-- (For production, consider MERGE if available in your Redshift version.)

delete from analytics.fct_events
using raw.events_staging s
where analytics.fct_events.event_id = s.event_id
  and s.ds = :ds;

insert into analytics.fct_events (ds, event_id, user_id, event_type, event_ts)
select ds, event_id, user_id, event_type, event_ts
from raw.events_staging
where ds = :ds;

-- Daily aggregate rebuild for the partition

delete from analytics.fct_event_counts_daily where ds = :ds;

insert into analytics.fct_event_counts_daily (ds, event_type, cnt)
select :ds as ds, event_type, count(*) as cnt
from analytics.fct_events
where ds = :ds
group by 2;

commit;
