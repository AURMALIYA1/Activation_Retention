---------------------------------------------------------------------------
-----------------Start_month_users_direct----------------------------------
drop table akshayscratchpad.mixpanel_data.user_pay_churn_dt_direct;
create table akshayscratchpad.mixpanel_data.user_pay_churn_dt_direct
as
select a.email
,b.db_create_dts
,b.db_destroy_dts
,min (date(a.db_created_at)) as date_of_pymnt_start
,max (case when db_create_dts = db_destroy_dts then date(a.db_destroyed_at) else null end) as date_of_pymnt_stop
,max (case when a.db_sequence = 1 then initial_size_label else null end) as initial_size_label
from `neo4j-cloud-misc.aura_usage_metrics.v_canonical_database_list` a
left join
(select email
,count(distinct case when db_created_at is not null then db_created_at else null end) as db_create_dts
,count(distinct case when db_destroyed_at is not null then db_destroyed_at else null end) as db_destroy_dts
from `neo4j-cloud-misc.aura_usage_metrics.v_canonical_database_list`
--where email = "alex@graphstem.com"
where db_billing_identity = "Direct"
group by 1
) b
on a.email = b.email
where a.email_domain not in ("neotechnology.com","neo4j.com")
--and a.billed_hours>=2
--where b.db_create_dts = b.db_destroy_dts
group by 1,2,3;
-------------------------------------------------------------------------
select extract(year from date_of_pymnt_start) as yr
,extract(month from date_of_pymnt_start) as mnth
,count(distinct email) count_of_users
from akshayscratchpad.mixpanel_data.user_pay_churn_dt_direct
where db_create_dts is not null
group by 1,2
order by 1,2;
---------------------------------------------------------------------------
-----------------Start_month_users_gcp-------------------------------------
drop table akshayscratchpad.mixpanel_data.user_pay_churn_dt_gcp;
create table akshayscratchpad.mixpanel_data.user_pay_churn_dt_gcp
as
select a.email
,b.db_create_dts
,b.db_destroy_dts
,min (date(a.db_created_at)) as date_of_pymnt_start
,max (case when db_create_dts = db_destroy_dts then date(a.db_destroyed_at) else null end) as date_of_pymnt_stop
,max (case when a.db_sequence = 1 then initial_size_label else null end) as initial_size_label
from `neo4j-cloud-misc.aura_usage_metrics.v_canonical_database_list` a
left join
(select email
,count(distinct case when db_created_at is not null then db_created_at else null end) as db_create_dts
,count(distinct case when db_destroyed_at is not null then db_destroyed_at else null end) as db_destroy_dts
from `neo4j-cloud-misc.aura_usage_metrics.v_canonical_database_list`
--where email = "alex@graphstem.com"
where db_billing_identity = "Direct"
group by 1
) b
on a.email = b.email
where a.email_domain not in ("neotechnology.com","neo4j.com")
--and a.billed_hours>=2
--where b.db_create_dts = b.db_destroy_dts
group by 1,2,3;
-------------------------------------------------------------------------
select extract(year from date_of_pymnt_start) as yr
,extract(month from date_of_pymnt_start) as mnth
,count(distinct email) count_of_users
from akshayscratchpad.mixpanel_data.user_pay_churn_dt_gcp
where db_create_dts is not null
group by 1,2
order by 1,2;
