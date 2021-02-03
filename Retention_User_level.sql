----------------------------------------------------------------------------
drop table akshayscratchpad.mixpanel_data.calendar;
create table if not exists akshayscratchpad.mixpanel_data.calendar
as
(SELECT timestamp(day) as calendar_dt
FROM UNNEST(
    GENERATE_DATE_ARRAY(DATE('2019-02-05'), CURRENT_DATE(), INTERVAL 1 DAY)
) AS day
);
----------------------------------------------------------------------------
drop table akshayscratchpad.mixpanel_data.daily_active_aura_dbs;
create table akshayscratchpad.mixpanel_data.daily_active_aura_dbs
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
----------------------------------------------------------------------------
drop table akshayscratchpad.mixpanel_data.database_create_log;
create table akshayscratchpad.mixpanel_data.database_create_log
as (select dbid
, date_diff(date(calendar_dt) ,date("2019-02-01"), month) as db_running_month
from `akshayscratchpad.mixpanel_data.daily_active_aura_dbs`
group by 1,2
order by 1,2);
