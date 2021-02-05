----------------------------------------------------------------------------
------------To have dates starting from first aura self-serve db------------
drop table akshayscratchpad.mixpanel_data.calendar;
create table if not exists akshayscratchpad.mixpanel_data.calendar
as
(SELECT timestamp(day) as calendar_dt
FROM UNNEST(
    GENERATE_DATE_ARRAY(DATE('2019-02-05'), CURRENT_DATE(), INTERVAL 1 DAY)
) AS day
);
----------------------------------------------------------------------------
-----------To have a list of dbs by calendar day----------------------------
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
-----------To have a list of users with db_create_dt------------------------
drop table akshayscratchpad.mixpanel_data.db_create_logs;
create table akshayscratchpad.mixpanel_data.db_create_logs
as
select
extract(month from a.db_created_at) as db_create_mnth
,extract(year from a.db_created_at) as db_create_yr
,a.email
,count(distinct a.dbid) as db_created_count
from akshayscratchpad.mixpanel_data.daily_active_aura_dbs a
group by 1,2,3;
--------------------------------------------------------------------
drop table akshayscratchpad.mixpanel_data.db_destroy_logs;
create table akshayscratchpad.mixpanel_data.db_destroy_logs
as
select
extract(year from a.db_destroyed_at) as db_destroy_yr
,extract(month from a.db_destroyed_at) as db_destroy_mnth
,a.email
,count(distinct case when a.db_destroyed_at is not null then a.dbid else null end) as db_destroyed_count
from akshayscratchpad.mixpanel_data.daily_active_aura_dbs a
group by 1,2,3;
--------------------------------------------------------------------
-----To have db create and destroy logs-----------------------------
drop table akshayscratchpad.mixpanel_data.db_create_destroy_logsv1;
create table akshayscratchpad.mixpanel_data.db_create_destroy_logsv1
as
select
extract (year from a.calendar_dt) as yr
,extract (month from a.calendar_dt) as mnth
,a.email
,c.db_created_count
,b.db_destroyed_count
from akshayscratchpad.mixpanel_data.daily_active_aura_dbs a
left join akshayscratchpad.mixpanel_data.db_destroy_logs b
on extract (month from a.calendar_dt) = b.db_destroy_mnth
and extract (year from a.calendar_dt) = b.db_destroy_yr
and a.email = b.email
left join akshayscratchpad.mixpanel_data.db_create_logs c
on extract (month from a.calendar_dt) = c.db_create_mnth
and extract (year from a.calendar_dt) = c.db_create_yr
and a.email = c.email
--where a.email = "aslak@hu.ma"
group by 1,2,3,4,5;
---------------------------------------------------------------
-----------To get user history with calendar_dt----------------
drop table akshayscratchpad.mixpanel_data.active_user_historyv1;
create table akshayscratchpad.mixpanel_data.active_user_historyv1
  as
select yr,mnth
,email
,db_created_count
,db_destroyed_count
,total_db_created_till_dt
,case when total_db_destroyed_till_dt is null then 0 else total_db_destroyed_till_dt end as total_db_destroyed_till_dt
,rank() over (partition by email order by yr,mnth) as user_order
from
(select yr,mnth
,email
,case when db_created_count is null then 0 else db_created_count end as db_created_count
,case when db_destroyed_count is null then 0 else db_destroyed_count end as db_destroyed_count
,sum(db_created_count) over (partition by email order by yr,mnth rows between unbounded preceding and current row) as total_db_created_till_dt
,sum(db_destroyed_count) over (partition by email order by yr,mnth rows between unbounded preceding and current row) as total_db_destroyed_till_dt
from akshayscratchpad.mixpanel_data.db_create_destroy_logsv1
--where email = "akshay.urmaliya@neotechnology.com"
)a;
----------------------------------------------------------------
-----------To get complete user history-------------------------
drop table akshayscratchpad.mixpanel_data.complete_user_history;
create table akshayscratchpad.mixpanel_data.complete_user_history
as
select  a.*
,case when a.total_db_created_till_dt>a.total_db_destroyed_till_dt OR user_order=1 then yr else null end as user_active_yr
,case when a.total_db_created_till_dt>a.total_db_destroyed_till_dt OR user_order=1 then mnth else null end as user_active_mnth
from akshayscratchpad.mixpanel_data.active_user_historyv1 a
--where email = "julien.grobbelaar@neotechnology.com"
order by 1,2
--------------------------------------------------------------------------------
-------------------Visit_log table for tracking users---------------------------
drop table akshayscratchpad.mixpanel_data.user_logv1;
create table akshayscratchpad.mixpanel_data.user_logv1
as
select
user_active_yr
,user_active_mnth
,email
--,total_db_created_till_dt
--,total_db_destroyed_till_dt
,(user_active_yr - 2019)*12 + (user_active_mnth - 2) as visit_month
from akshayscratchpad.mixpanel_data.complete_user_history
where user_active_yr is not null
order by 1,2;
---------------------------------------------------------------------------------
--------------------ACTUAL RETENTION LOGIC AND NUMBERS---------------------------
---------------------------------------------------------------------------------
-------------------------------------------------------------------------------------
---------------To create lag timelapse for New/Retained/reactivated------------------
drop table akshayscratchpad.mixpanel_data.lag_timelapse;
create table akshayscratchpad.mixpanel_data.lag_timelapse
as
select email, visit_month , lag(visit_month, 1) over (partition BY email ORDER BY email, visit_month) as lag_month
from akshayscratchpad.mixpanel_data.user_logv1;
------------------------------------------------------------------------------------------
drop table akshayscratchpad.mixpanel_data.time_diff_lag;
create table akshayscratchpad.mixpanel_data.time_diff_lag
as select email, visit_month, visit_month - lag_month as time_diff
from akshayscratchpad.mixpanel_data.lag_timelapse ;
------------------------------------------------------------------------------------------
drop table akshayscratchpad.mixpanel_data.cust_categorized_lag;
create table akshayscratchpad.mixpanel_data.cust_categorized_lag
as select email, visit_month
, case when time_diff = 1 then "retained"
       when time_diff > 1 then "reactivated"
       when time_diff is null then "new" else null end as cust_type
       from akshayscratchpad.mixpanel_data.time_diff_lag ;
------------------------------------------------------------------------------------------
---------------To create lead timelapse for Retained/Churned/reactivated------------------
drop table akshayscratchpad.mixpanel_data.lead_timelapse;
create table akshayscratchpad.mixpanel_data.lead_timelapse
as
select email, visit_month , lead(visit_month, 1) over (partition BY email ORDER BY email, visit_month) as lead_month
from akshayscratchpad.mixpanel_data.user_logv1;
---------------------------------------------------------------------------
drop table akshayscratchpad.mixpanel_data.time_diff_lead;
create table akshayscratchpad.mixpanel_data.time_diff_lead
as select email, visit_month, lead_month - visit_month as time_diff
from akshayscratchpad.mixpanel_data.lead_timelapse ;
---------------------------------------------------------------------------
drop table akshayscratchpad.mixpanel_data.cust_categorized_lead;
create table akshayscratchpad.mixpanel_data.cust_categorized_lead
as select email, visit_month
, case when time_diff = 1 then "retained"
       when time_diff > 1 then "churned"
       when time_diff is null then "NA" else null end as cust_type
       from akshayscratchpad.mixpanel_data.time_diff_lead ;
----------------------------------------------------------------
drop table `akshayscratchpad.mixpanel_data.churned_user_month`;
create table `akshayscratchpad.mixpanel_data.churned_user_month`
as
select a.email
,visit_month as visit_month_orig
,cust_type
,case when cust_type = "churned" then visit_month+1 else visit_month end as visit_month
 from akshayscratchpad.mixpanel_data.cust_categorized_lead a ;
--where email = "julien.grobbelaar@neotechnology.com"
order by 2


---------------------Deprecated Queries-------------------------
drop table akshayscratchpad.mixpanel_data.db_create_destroy_logs;
create table akshayscratchpad.mixpanel_data.db_create_destroy_logs
as
select
 a.db_create_mnth
, a.db_create_yr
, a.email
, a.db_created_count
, b.db_destroyed_count
from akshayscratchpad.mixpanel_data.db_create_logs a
left join akshayscratchpad.mixpanel_data.db_destroy_logs b
on a.db_create_mnth = b.db_destroy_mnth
and a.db_create_yr = b.db_destroy_yr
and a.email = b.email
--where a.email = "aslak@hu.ma"
group by 1,2,3,4,5;
---------------------------------------------------------------
drop table akshayscratchpad.mixpanel_data.active_users;
create table akshayscratchpad.mixpanel_data.active_users
as
select a.*, rank() over (partition by email order by yr,mnth) as user_order
from
(
select extract (year from calendar_dt) as yr
,extract (month from calendar_dt) as mnth
,b.db_create_yr
,b.db_create_mnth
,b.db_created_count
,case when b.db_destroyed_count is null then 0 else b.db_destroyed_count end as db_destroyed_count
,a.email
# ,extract(month from a.db_created_at) as db_create_mnth
# ,extract(year from a.db_created_at) as db_create_yr
# ,extract(year from a.db_destroyed_at) as db_destroy_yr
# ,extract(month from a.db_destroyed_at) as db_destroy_mnth
from akshayscratchpad.mixpanel_data.daily_active_aura_dbs a
left join akshayscratchpad.mixpanel_data.db_create_destroy_logs b
on a.email = b.email
and extract(year from a.calendar_dt) = b.db_create_yr
and extract(month from a.calendar_dt) = b.db_create_mnth
--where a.email =  "aslak@hu.ma"
group by 1,2,3,4,5,6,7
)a;
---------------------------------------------------------------
create table akshayscratchpad.mixpanel_data.active_user_history
  as
select yr,mnth
,email
,db_create_yr
,db_create_mnth
,db_created_count
,db_destroyed_count
,total_db_created_till_dt
,case when total_db_destroyed_till_dt is null then 0 else total_db_destroyed_till_dt end as total_db_destroyed_till_dt
,case when a.total_db_created_till_dt>a.total_db_destroyed_till_dt then yr else null end as user_active_yr
,case when a.total_db_created_till_dt>a.total_db_destroyed_till_dt then mnth else null end as user_active_mnth
from
(select yr,mnth
,email
,db_create_yr
,db_create_mnth
,db_created_count
,db_destroyed_count
,sum(db_created_count) over (partition by email order by yr,mnth rows between unbounded preceding and current row) as total_db_created_till_dt
,sum(db_destroyed_count) over (partition by email order by yr,mnth rows between unbounded preceding and current row) as total_db_destroyed_till_dt
from akshayscratchpad.mixpanel_data.active_users
--where email = "akshay.urmaliya@neotechnology.com"
)a;
