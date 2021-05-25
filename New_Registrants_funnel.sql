---------- AURA Registrants to DB creation ----------------------
drop table neo4j-cloud-misc.user_summary_tables.daily_user_registration;
create table neo4j-cloud-misc.user_summary_tables.daily_user_registration
as
select
user_create_dt
,registrants as email_address
,count(distinct registrants) as registrants
,count(distinct case when db_creaters is not null then db_creaters else null end) as db_creators
from
(select user_create_dt, a.email as registrants, b.email as db_creaters
# ,user_created_at
# ,case when user_created_at is not null then date(timestamp(user_created_at)) else "2019-02-01" end as user_create_dt
from
(select distinct email,date(timestamp(user_created_at)) as user_create_dt
from `neo4j-cloud-misc.aura_usage_metrics.v_canonical_user_list`
where user_created_at is not null
and email not like "%neo4j.com%"
and email not like "%neotechnology.com%" ) a
left join
(select
distinct email, date(timestamp(db_created_at)) as db_create_dt
from `neo4j-cloud-misc.aura_usage_metrics.v_canonical_database_list`
where user_db_sequence=1
and db_created_at is not null
and email not like "%neo4j.com%"
and email not like "%neotechnology.com%" ) b
on a.email = b.email
and a.user_create_dt = b.db_create_dt
)group by 1,2
order by 1;
------------AURA Registrants to Terms and Condition Acceptance ----------------------
select user_create_dt
,sum(registrants) as registrants
,sum(ts_cs_accept_cnt) as ts_cs_accept
from
(select a.user_create_dt
,a.email_address
,a.registrants
,b.ts_cs_accept_cnt
from neo4j-cloud-misc.user_summary_tables.daily_user_registration a
left join
(select b.calendar_dt,b.distinct_id,c.email_address,b.ts_cs_accept_cnt
from neo4jusersummarytables.mixpanelData.daily_user_interaction_table b
inner join neo4jusersummarytables.mixpanelData.user_mapping_table c
on b.distinct_id = c.distinct_id
)b
on a.email_address = b.email_address
and a.user_create_dt = b.calendar_dt
) group by 1
order by 1 desc;
---------------------------------------------------------------------------------------
select user_create_dt
,email_address
,sum(registrants) as registrants
,sum(ts_cs_accept_cnt_flag) as ts_cs_accept
,sum(create_db_pageview_flag) as create_db_pageview_flag
,sum(create_db_step1_pageview_flag) as create_db_step1_pageview_flag
,sum(create_db_payment_pageview_flag) as create_db_payment_pageview_flag
,sum(db_created_flag) as db_created_flag
,sum(db_creators) as db_creators
,sum(import_pageview_flag) as import_pageview_flag
,sum(help_link_flag) as help_link_flag
,sum(feedback_click_cnt_flag) as feedback_click_cnt_flag
,sum(account_pageview_cnt_flag) as account_pageview_cnt_flag
,sum(console_dashboard_connect_flag) as console_dashboard_connect_flag
from
(select a.user_create_dt
,a.email_address
,max(a.registrants) as registrants
,max(a.db_creators) as db_creators
,max(b.ts_cs_accept_cnt_flag) as ts_cs_accept_cnt_flag
,max(b.create_db_pageview_flag) as create_db_pageview_flag
,max(b.create_db_step1_pageview_flag) as create_db_step1_pageview_flag
,max(b.create_db_payment_pageview_flag) as create_db_payment_pageview_flag
,max(b.db_created_flag) as db_created_flag
,max(b.import_pageview_flag) as import_pageview_flag
,max(b.help_link_flag) as help_link_flag
,max(b.feedback_click_cnt_flag) as feedback_click_cnt_flag
,max(b.account_pageview_cnt_flag) as account_pageview_cnt_flag
,max(b.console_dashboard_connect_flag) as console_dashboard_connect_flag
from neo4j-cloud-misc.user_summary_tables.daily_user_registration a
left join
(select b.calendar_dt,b.distinct_id,c.email_address
,max(case when ts_cs_accept_cnt_flag>0 then 1 else 0 end) as ts_cs_accept_cnt_flag
,max(case when create_db_pageview_flag>0 then 1 else 0 end) as create_db_pageview_flag
,max(case when create_db_step1_pageview_flag>0 then 1 else 0 end) as create_db_step1_pageview_flag
,max(case when create_db_payment_pageview_flag>0 then 1 else 0 end) as create_db_payment_pageview_flag
,max(case when db_created_flag>0 then 1 else 0 end) as db_created_flag
,max(case when import_pageview_flag>0 then 1 else 0 end) as import_pageview_flag
,max(case when help_link_flag>0 then 1 else 0 end) as help_link_flag
,max(case when feedback_click_cnt_flag>0 then 1 else 0 end) as feedback_click_cnt_flag
,max(case when account_pageview_cnt_flag>0 then 1 else 0 end) as account_pageview_cnt_flag
,max(case when console_dashboard_connect_flag>0 then 1 else 0 end) as console_dashboard_connect_flag
from neo4jusersummarytables.mixpanelData.user_funnel b
inner join
(select distinct_id, email_address
from neo4jusersummarytables.mixpanelData.user_mapping_table
group by 1,2) c
on b.distinct_id = c.distinct_id
group by 1,2,3
)b
on a.email_address = b.email_address
and a.user_create_dt = b.calendar_dt
group by 1,2
)
group by 1
order by 1 desc;
