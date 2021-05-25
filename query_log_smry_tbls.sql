-------------------------------------------------------------------------
drop table neo4jusersummarytables.queryLogData.calendar;
create table if not exists neo4jusersummarytables.queryLogData.calendar
as
(SELECT timestamp(day) as calendar_dt
FROM UNNEST(
    GENERATE_DATE_ARRAY(DATE('2019-02-05'), CURRENT_DATE(), INTERVAL 1 DAY)
) AS day
);
-------------------------------------------------------------------------
drop table neo4jusersummarytables.queryLogData.daily_active_aura_dbs;
create table neo4jusersummarytables.queryLogData.daily_active_aura_dbs
as
(with d as (
select distinct c.calendar_dt
  from `akshayscratchpad.mixpanel_data.calendar` c
 where c.calendar_dt <= timestamp_trunc(current_timestamp(),DAY)
)
select calendar_dt,
       l.*
  from d
  join `neo4j-cloud-misc.aura_usage_metrics.v_canonical_database_list` l
  on d.calendar_dt >= timestamp_trunc(l.db_created_at,DAY)
  and d.calendar_dt <= timestamp_trunc(ifnull(l.db_destroyed_at,current_timestamp()),DAY)
where db_created_at is not NULL
order by 1);
----------User level data with email-level attributes-------------------------
drop table neo4jusersummarytables.queryLogData.canonical_db_user_data;
create table neo4jusersummarytables.queryLogData.canonical_db_user_data
  as
  select email
  , min(db_create_dt) as first_db_create_dt
  , min(db_created_at) as first_db_create_dt_ts
  , count(distinct dbid) as total_db_created
  , count(distinct case when db_destroy_dt is not null then dbid else null end) as total_db_destroyed
  , min(case when db_sequence = 1 then initial_size_gb else null end) as first_db_size
  , max(initial_size_gb) as max_db_size_before_resizing
  , max(most_recent_size_gb) as max_db_size_after_resizing
  , min(case when db_reverse_sequence = 1 then most_recent_size_gb else null end) as most_recent_db_size
  , sum(resize_count) as resize_count
  , sum(db_lifetime_days) as sum_of_all_db_lifetime
  , sum(total_cost_dollars) as total_cost_dollars
  , sum(mrr_num) as total_mrr_dollars
  , min(case when lower(db_billing_identity) = "gds" then db_created_at else null end) as first_gds_db_create_dt_ts
  , min(case when lower(db_billing_identity) = "direct" then db_created_at else null end) as first_direct_db_create_dt_ts
  , min(case when lower(db_billing_identity) = "gcp" then db_created_at else null end) as first_gcp_db_create_dt_ts
  , min(case when lower(db_billing_identity) = "enterprise" then db_created_at else null end) as first_enterprise_db_create_dt_ts
  , min(case when lower(db_billing_identity) = "free" then db_created_at else null end) as first_free_db_create_dt_ts
  , count(distinct case when lower(db_billing_identity) = "gds" then dbid else null end) as count_of_gds_db_created
  , count(distinct case when lower(db_billing_identity) = "direct" then dbid else null end) as count_of_direct_db_created
  , count(distinct case when lower(db_billing_identity) = "gcp" then dbid else null end) as count_of_gcp_db_created
  , count(distinct case when lower(db_billing_identity) = "enterprise" then dbid else null end) as count_of_enterprise_db_created
  , count(distinct case when lower(db_billing_identity) = "free" then dbid else null end) as count_of_free_db_created
  , count(distinct case when lower(db_billing_identity) = "free" and db_destroy_dt is not null then dbid else null end) as count_of_free_db_destroyed
  , count(distinct case when lower(db_billing_identity) in ("direct","gcp","enterprise","gds") then dbid else null end) as count_of_paid_db_created
  , count(distinct case when lower(db_billing_identity) in ("direct","gcp","enterprise","gds") and db_destroy_dt is not null then dbid else null end) as count_of_paid_db_destroyed
  from
  (select DATE(TIMESTAMP(db_created_at)) as db_create_dt, DATE(TIMESTAMP(db_destroyed_at)) as db_destroy_dt,db_created_at,db_destroyed_at
  ,db_billing_identity, email, initial_size_gb, dbid,user_db_sequence as db_sequence, most_recent_size_gb, resize_count, db_lifetime_days,total_cost_dollars,mrr_num
  ,user_db_rsequence as db_reverse_sequence
  --,rank() over (partition by email order by db_created_at desc) as db_create_order
  from `neo4j-cloud-misc.aura_usage_metrics.canonical_database_list`
  where email not like "%neo4j.com%"
  and email not like "%neotechnology.com%"
  )a
  group by 1
  order by 1;
