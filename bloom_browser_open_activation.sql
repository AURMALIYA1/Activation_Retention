------------------------------------------------------------------------------------------
drop table `akshayscratchpad.mixpanel_data.daily_aura_login_users`;
create table `akshayscratchpad.mixpanel_data.daily_aura_login_users`
as
 select DATE(TIMESTAMP_SECONDS(time)) as calendar_date,distinct_id,email,count(*) as login_event_cnt
 from `neo4j-cloud-misc.mixpanel_exports.aura_login`
 where DATE(TIMESTAMP_SECONDS(time)) between "2021-03-01" and "2021-03-31"
 group by 1,2,3
 order by 1;
------------------------------------------------------------------------------------------
drop table `akshayscratchpad.mixpanel_data.daily_aura_aura_open_db_users`;
create table `akshayscratchpad.mixpanel_data.daily_aura_aura_open_db_users`
as
 select DATE(TIMESTAMP_SECONDS(time)) as calendar_date,event_label,distinct_id,count(*) as open_db_cnt
 from `neo4j-cloud-misc.mixpanel_exports.aura_open_db`
 where DATE(TIMESTAMP_SECONDS(time)) between "2021-03-01" and "2021-03-31"
 group by 1,2,3
 order by 1;
 ------------------------------------------------------------------------------------------
drop table `akshayscratchpad.mixpanel_data.daily_aura_create_db_users`;
create table `akshayscratchpad.mixpanel_data.daily_aura_create_db_users`
as
 select DATE(TIMESTAMP_SECONDS(time)) as calendar_date,distinct_id,count(*) as open_db_cnt
 from `neo4j-cloud-misc.mixpanel_exports.aura_create_db`
 where DATE(TIMESTAMP_SECONDS(time)) between "2021-03-01" and "2021-03-31"
 group by 1,2
 order by 1;
  ------------------------------------------------------------------------------------------
drop table `akshayscratchpad.mixpanel_data.daily_aura_create_free_db_users`;
create table `akshayscratchpad.mixpanel_data.daily_aura_create_free_db_users`
as
 select DATE(TIMESTAMP_SECONDS(time)) as calendar_date,distinct_id,count(*) as open_db_cnt
 from `neo4j-cloud-misc.mixpanel_exports.aura_create_free_db`
 where DATE(TIMESTAMP_SECONDS(time)) between "2021-03-01" and "2021-03-31"
 group by 1,2
 order by 1;

 ------------------------------------------------------------------------------------------
 select a.first_login_dt
 ,a.distinct_id
 ,a.email
 ,c.first_db_create_dt
 ,b.event_label
 ,b.first_launch_dt
 from
 (select distinct_id, email, min(calendar_date) as first_login_dt
 from `akshayscratchpad.mixpanel_data.daily_aura_login_users`
 group by 1,2) a
 left join
 (select distinct_id, event_label, min(calendar_date) as first_launch_dt
 from `akshayscratchpad.mixpanel_data.daily_aura_aura_open_db_users`
 group by 1,2)b
 on a.distinct_id = b.distinct_id
 left join
 (
 select distinct_id,min(first_db_create_dt) as first_db_create_dt
 from
 (select distinct_id, min(calendar_date) as first_db_create_dt
 from `akshayscratchpad.mixpanel_data.daily_aura_create_db_users`
 group by 1
 union all
 select distinct_id, min(calendar_date) as first_db_create_dt
 from `akshayscratchpad.mixpanel_data.daily_aura_create_free_db_users`
 group by 1
 )a
 group by 1)c
 on a.distinct_id = c.distinct_id
 group by 1,2,3,4,5,6
 order by 1;
 ------------------------------------------------------------------------------------------

 select a.first_login_dt
 ,a.distinct_id
 ,a.email
 ,b.event_label
 ,b.first_launch_dt
 from
 (select distinct_id, email, min(calendar_date) as first_login_dt
 from `akshayscratchpad.mixpanel_data.daily_aura_login_users`
 group by 1,2) a
 left join
 (select distinct_id, event_label, min(calendar_date) as first_launch_dt
 from `akshayscratchpad.mixpanel_data.daily_aura_aura_open_db_users`
 group by 1,2)b
 on a.distinct_id = b.distinct_id
 left join
 (select email, )
 group by 1,2,3,4,5,6
 order by 1;
------------------------------------------------------------------------------------------  
 select email,max(activated_yn) as activated_yn
 from
 (select a.email,a.dbid,a.db_tier
 ,max(case when a.dbid = b.dbid then 1 else 0 end) as activated_yn
 from
 (select email, dbid, db_tier
 from `neo4j-cloud-misc.aura_usage_metrics.canonical_database_list`
 where date(db_created_at) between "2021-03-01" and "2021-03-31"
 and db_tier not in ("enterprise")
 group by 1,2,3
 )a
 left join
 (select dbid, sum(queries_completed) as query_count
 from `neo4j-cloud-misc.query_log_transforms.per_minute_completion_summaries`
 where DATE(TIMESTAMP(minute)) between "2021-03-01" and current_date()
 group by 1
 )b
 on a.dbid = b.dbid
 group by 1,2,3
 )c
 group by 1;
