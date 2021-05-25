--------------DAILY_USER_EVENT_TABLE--------------------------------------------
--------------ALL REPORTING TABLES SHOULD EXCLUDE INTERNAL USERS----------------
drop table  neo4j-cloud-misc.user_summary_tables.user_event_table;
create table  neo4j-cloud-misc.user_summary_tables.user_event_table
  as
select *
from
(select TIMESTAMP_SECONDS(time) as event_ts,raw_event_type as event_name, distinct_id, email as email_address, null as event_label,null as version,null as size,null as region
,null as installtype,null as step, null as view,null as language,null as dbid_created,null as db_resized,null as db_destroyed
,null as old_size, null as new_size,null as readytorunpushcommand,device_id,user_id,distinct_id_before_identity
from `neo4j-cloud-misc.mixpanel_exports_dev.aura_login`
where project_id = "4cf820094a8c88a45ca1a474080590a2"
union all
select TIMESTAMP_SECONDS(time) as event_ts,raw_event_type as event_name, distinct_id, null as email_address, null as event_label,null as version,null as size,null as region
,null as installtype,null as step, view,null as language,case when current_url like "https://%/#databases/%/created" then substr(current_url,-16,8) else null end as dbid_created
,null as db_resized,null as db_destroyed
,null as old_size, null as new_size,null as readytorunpushcommand,device_id,user_id,distinct_id_before_identity
from `neo4j-cloud-misc.mixpanel_exports_dev.aura_navigate_to`
where project_id = "4cf820094a8c88a45ca1a474080590a2"
union all
select TIMESTAMP_SECONDS(time) as event_ts,raw_event_type as event_name, distinct_id, null as email_address, null as event_label,version,size,region
,null as installtype,null as step, null as view,null as language,null as dbid_created,null as db_resized,null as db_destroyed
,null as old_size, null as new_size,null as readytorunpushcommand,device_id,user_id,distinct_id_before_identity
from `neo4j-cloud-misc.mixpanel_exports_dev.aura_create_db`
where project_id = "4cf820094a8c88a45ca1a474080590a2"
union all
select TIMESTAMP_SECONDS(time) as event_ts,raw_event_type as event_name, distinct_id, null as email_address, null as event_label,null as version,null as size,null as region
,installtype,null as step, null as view,null as language,null as dbid_created,null as db_resized,null as db_destroyed
,null as old_size, null as new_size,readytorunpushcommand,device_id,user_id,distinct_id_before_identity
from `neo4j-cloud-misc.mixpanel_exports_dev.aura_set_import_option`
where project_id = "4cf820094a8c88a45ca1a474080590a2"
union all
select TIMESTAMP_SECONDS(time) as event_ts,raw_event_type as event_name, distinct_id, null as email_address, null as event_label,null as version,null as size,null as region
,null as installtype,step, null as view,null as language,null as dbid_created,null as db_resized,null as db_destroyed
,null as old_size, null as new_size,null as readytorunpushcommand,device_id,user_id,distinct_id_before_identity
from `neo4j-cloud-misc.mixpanel_exports_dev.aura_next_step`
where project_id = "4cf820094a8c88a45ca1a474080590a2"
union all
select TIMESTAMP_SECONDS(time) as event_ts,raw_event_type as event_name, distinct_id, null as email_address, null as event_label,null as version,null as size,null as region
,null as installtype,null as step, null as view,null as language,null as dbid_created,null as db_resized,null as db_destroyed
,null as old_size, null as new_size,null as readytorunpushcommand,device_id,user_id,distinct_id_before_identity
from `neo4j-cloud-misc.mixpanel_exports_dev.aura_finish_import`
where project_id = "4cf820094a8c88a45ca1a474080590a2"
union all
select TIMESTAMP_SECONDS(time),raw_event_type as event_name, distinct_id, null as email_address, event_label,null as version,null as size,null as region
,null as installtype,null as step, null as view,null as language,null as dbid_created,null as db_resized,null as db_destroyed
,null as old_size, null as new_size,null as readytorunpushcommand,device_id,user_id,distinct_id_before_identity
from `neo4j-cloud-misc.mixpanel_exports_dev.aura_open_db`
where project_id = "4cf820094a8c88a45ca1a474080590a2"
union all
select TIMESTAMP_SECONDS(time) as event_ts,raw_event_type as event_name, distinct_id, null as email_address, null as event_label,null as version,null as size,null as region
,null as installtype,null as step, null as view,null as language,null as dbid_created,null as db_resized,null as db_destroyed
,null as old_size, null as new_size,null as readytorunpushcommand,device_id,user_id,distinct_id_before_identity
from `neo4j-cloud-misc.mixpanel_exports_dev.aura_help_link`
where project_id = "4cf820094a8c88a45ca1a474080590a2"
union all
select TIMESTAMP_SECONDS(time) as event_ts,raw_event_type as event_name, distinct_id, null as email_address, null as event_label,null as version,null as size,null as region
,null as installtype,null as step, null as view,null as language,null as dbid_created,null as db_resized,null as db_destroyed
,null as old_size, null as new_size,null as readytorunpushcommand,device_id,user_id,distinct_id_before_identity
from `neo4j-cloud-misc.mixpanel_exports_dev.aura_feedback_link`
where project_id = "4cf820094a8c88a45ca1a474080590a2"
union all
select TIMESTAMP_SECONDS(time) as event_ts,raw_event_type, distinct_id, null as email_address, null as event_label,null as version,null as size,null as region
,null as installtype,null as step, null as view,null as language,null as dbid_created,null as db_resized,null as db_destroyed
,null as old_size, null as new_size,null as readytorunpushcommand,device_id,user_id,distinct_id_before_identity
from `neo4j-cloud-misc.mixpanel_exports_dev.aura_expand_resize_db`
where project_id = "4cf820094a8c88a45ca1a474080590a2"
union all
select TIMESTAMP_SECONDS(time) as event_ts,raw_event_type as event_name, distinct_id, null as email_address, null as event_label,null as version,null as size,null as region
,null as installtype,null as step, null as view,null as language,null as dbid_created,case when current_url like "https://%/#databases/%/detail" then
substr(current_url,-15,8) else null end as dbid_resized,null as db_destroyed,old_size,new_size,null as readytorunpushcommand,device_id,user_id,distinct_id_before_identity
from `neo4j-cloud-misc.mixpanel_exports_dev.aura_resize_db`
where project_id = "4cf820094a8c88a45ca1a474080590a2"
union all
select TIMESTAMP_SECONDS(time) as event_ts,raw_event_type, distinct_id, null as email_address, null as event_label,null as version,null as size,null as region
,null as installtype,null as step, null as view,null as language,null as dbid_created,null as db_resized,null as db_destroyed
,null as old_size, null as new_size,null as readytorunpushcommand,device_id,user_id,distinct_id_before_identity
from `neo4j-cloud-misc.mixpanel_exports_dev.aura_restore_db`
where project_id = "4cf820094a8c88a45ca1a474080590a2"
union all
select TIMESTAMP_SECONDS(time) as event_ts,raw_event_type as event_name, distinct_id, null as email_address, null as event_label,null as version,null as size,null as region
,null as installtype,null as step, null as view,null as language,null as dbid_created,null as db_resized,null as db_destroyed
,null as old_size, null as new_size,null as readytorunpushcommand,device_id,user_id,distinct_id_before_identity
from `neo4j-cloud-misc.mixpanel_exports_dev.aura_export_db_backup`
where project_id = "4cf820094a8c88a45ca1a474080590a2"
union all
select TIMESTAMP_SECONDS(time) as event_ts,raw_event_type as event_name, distinct_id, null as email_address, null as event_label,null as version,null as size,null as region
,null as installtype,null as step, null as view,null as language,null as dbid_created,null as db_resized,case when current_url like "https://%/#databases/%/detail" then
substr(current_url,-15,8) else null end as dbid_destroyed
,null as old_size, null as new_size,null as readytorunpushcommand,device_id,user_id,distinct_id_before_identity
from `neo4j-cloud-misc.mixpanel_exports_dev.aura_destroy_db`
where project_id = "4cf820094a8c88a45ca1a474080590a2"
union all
select TIMESTAMP_SECONDS(time) as event_ts,raw_event_type as event_name, distinct_id, null as email_address, null as event_label,null as version,null as size,null as region
,null as installtype,null as step, null as view,null as language,null as dbid_created,null as db_resized,null as db_destroyed
,null as old_size, null as new_size,null as readytorunpushcommand,device_id,user_id,distinct_id_before_identity
from `neo4j-cloud-misc.mixpanel_exports_dev.aura_logout`
where project_id = "4cf820094a8c88a45ca1a474080590a2"
union all
select TIMESTAMP_SECONDS(time) as event_ts,raw_event_type, distinct_id, null as email_address, null as event_label,null as version,null as size,null as region
,null as installtype,null as step, null as view,null as language,null as dbid_created,null as db_resized,null as db_destroyed
,null as old_size, null as new_size,null as readytorunpushcommand,device_id,user_id,distinct_id_before_identity
from `neo4j-cloud-misc.mixpanel_exports_dev.aura_take_snapshot_db`
where project_id = "4cf820094a8c88a45ca1a474080590a2"
union all
select TIMESTAMP_SECONDS(time) as event_ts,raw_event_type as event_name, distinct_id, null as email_address, null as event_label,null as version,null as size,null as region
,null as installtype,null as step, null as view,null as language,null as dbid_created,null as db_resized,null as db_destroyed
,null as old_size, null as new_size,null as readytorunpushcommand,device_id,user_id,distinct_id_before_identity
from `neo4j-cloud-misc.mixpanel_exports_dev.aura_create_from_snapshot`
where project_id = "4cf820094a8c88a45ca1a474080590a2"
union all
select TIMESTAMP_SECONDS(time) as event_ts,raw_event_type as event_name, distinct_id, null as email_address, null as event_label,null as version,null as size,null as region
,null as installtype,null as step,view,language,null as dbid_created,null as db_resized,null as db_destroyed
,null as old_size, null as new_size,null as readytorunpushcommand,device_id,user_id,distinct_id_before_identity
from `neo4j-cloud-misc.mixpanel_exports_dev.aura_change_language`
where project_id = "4cf820094a8c88a45ca1a474080590a2"
union all
select TIMESTAMP_SECONDS(time) as event_ts,raw_event_type as event_name, distinct_id, null as email_address, null as event_label,null as version,null as size,null as region
,null as installtype,null as step,null as view,null as language,null as dbid_created,null as db_resized,null as db_destroyed
,null as old_size, null as new_size,null as readytorunpushcommand,device_id,user_id,distinct_id_before_identity
from `neo4j-cloud-misc.mixpanel_exports_dev.aura_ts_and_cs_accepted`
where project_id = "4cf820094a8c88a45ca1a474080590a2"
);
-------------------USER_MAPPING_TABLE---------------------
DROP TABLE
  `neo4j-cloud-misc.user_summary_tables.user_mapping_table`;
CREATE TABLE
  `neo4j-cloud-misc.user_summary_tables.user_mapping_table` AS
SELECT
  a.distinct_id,
  b.user_id,
  c.email_address,
  d.distinct_id_before_identity,
  --first_login_dt
FROM
  neo4j-cloud-misc.user_summary_tables.user_event_table a
LEFT JOIN (
  SELECT
    DISTINCT distinct_id,
    user_id
  FROM
    neo4j-cloud-misc.user_summary_tables.user_event_table
  WHERE
    user_id IS NOT NULL
  GROUP BY
    1,
    2) b
ON
  a.distinct_id = b.distinct_id
LEFT JOIN (
  SELECT
    distinct_id,
    email_address
  FROM
   neo4j-cloud-misc.user_summary_tables.user_event_table
  WHERE
    email_address IS NOT NULL
  GROUP BY
    1,
    2) c
ON
  a.distinct_id = c.distinct_id
LEFT JOIN (
  SELECT
    distinct_id,
    distinct_id_before_identity
  FROM
    neo4j-cloud-misc.user_summary_tables.user_event_table
  WHERE
    distinct_id_before_identity IS NOT NULL
  GROUP BY
    1,
    2) d
ON
  a.distinct_id = d.distinct_id
GROUP BY
  1,
  2,
  3,
  4;
-------------------DAILY_USER_SESSION_TABLE---------------------
drop table neo4j-cloud-misc.user_summary_tables.daily_user_interaction_table;
create table neo4j-cloud-misc.user_summary_tables.daily_user_interaction_table
  as
SELECT date(event_ts) as calendar_dt,distinct_id
,count(*) as event_count
,sum(case when lower(event_name) = "aura_login" then 1 else 0 end) as aura_login_cnt
,sum(case when lower(event_name) = "aura_open_db" and event_label= "neo4j-bloom" then 1 else 0 end) as bloom_open_cnt
,sum(case when lower(event_name) = "aura_open_db" and event_label= "neo4j-browser" then 1 else 0 end) as browser_open_cnt
,sum(case when lower(event_name) = "aura_navigate_to_create-database" then 1 else 0 end) as create_db_pageview_cnt
,sum(case when lower(event_name) = "aura_navigate_to_step1" then 1 else 0 end) as create_db_step1_pageview_cnt
,sum(case when lower(event_name) = "aura_navigate_to_step2" then 1 else 0 end) as create_db_payment_pageview_cnt
,sum(case when lower(event_name) = "aura_create_db" then 1 else 0 end) as db_created_cnt
,sum(case when lower(event_name) = "aura_navigate_to_import-instructions" then 1 else 0 end) as import_pageview_cnt
,sum(case when lower(event_name) = "aura_set_import_option" and installtype is not null then 1 else 0 end) as import_db_select_cnt
,sum(case when lower(event_name) = "aura_next_step" and step = "Ensure Neo4j Admin command" then 1 else 0 end) as import_ensure_admin_command_pageview_cnt
,sum(case when lower(event_name) = "aura_set_import_option" and readyToRunPushCommand is not null then 1 else 0 end) as import_ensure_admin_command_selection
,sum(case when lower(event_name) = "aura_next_step" and step = "Run Neo4j Admin command" then 1 else 0 end) as import_run_admin_command_pageview_cnt
,sum(case when lower(event_name) = "aura_next_step" and step = "Complete the process" then 1 else 0 end) as import_complete_process_pageview_cnt
,sum(case when lower(event_name) = "aura_finish_import" then 1 else 0 end) as  import_finish_instructions_cnt
,sum(case when lower(event_name) = "aura_change_language" then 1 else 0 end) as change_language_pageview_cnt
,sum(case when lower(event_name) = "aura_help_link" then 1 else 0 end) as help_click_cnt
,sum(case when lower(event_name) = "aura_feedback_link" then 1 else 0 end) as feedback_click_cnt
,sum(case when lower(event_name) = "aura_navigate_to_databases/detail" then 1 else 0 end) as db_details_pageview_cnt
,sum(case when lower(event_name) = "aura_navigate_to_databases/detail/import" then 1 else 0 end) as db_details_import_pageview_cnt
,sum(case when lower(event_name) = "aura_navigate_to_databases/detail/connect" then 1 else 0 end) as db_details_connect_pageview_cnt
,sum(case when lower(event_name) = "aura_navigate_to_databases/detail/snapshots" then 1 else 0 end) as db_details_snapshots_pageview_cnt
,sum(case when lower(event_name) = "aura_navigate_to_databases/detail/settings" then 1 else 0 end) as db_details_settings_pageview_cnt
,sum(case when lower(event_name) = "aura_expand_resize_db" then 1 else 0 end) as db_details_settings_resizeDb_pageview_cnt
,sum(case when lower(event_name) = "aura_resize_db" then 1 else 0 end) as db_resize_cnt
,sum(case when lower(event_name) = "aura_restore_db" then 1 else 0 end) as db_restore_cnt
,sum(case when lower(event_name) = "aura_export_db_backup" then 1 else 0 end) as export_db_backup_click_cnt
,sum(case when lower(event_name) = "aura_destroy_db" then 1 else 0 end) as db_destroy_cnt
,sum(case when lower(event_name) IN ("aura_navigate_to_account","aura_navigate_to_account/usage") then 1 else 0 end) as account_pageview_cnt
,sum(case when lower(event_name) IN ("aura_navigate_to_" , "aura_navigate_to_databases") then 1 else 0 end) as console_dashboard_pageview_cnt
,sum(case when lower(event_name) = "aura_logout" then 1 else 0 end) as console_logout_cnt
,sum(case when lower(event_name) = "aura_take_snapshot_db" then 1 else 0 end) as take_snapshot_click_cnt
,sum(case when lower(event_name) = "aura_create_from_snapshot" then 1 else 0 end) as create_db_from_snapshot_cnt
,sum(case when lower(event_name) = "aura_navigate_to_how-to-connect" then 1 else 0 end) as console_dashboard_connect_cnt
,sum(case when lower(event_name) = "aura_ts&cs_accepted" then 1 else 0 end) as ts_cs_accept_cnt
from neo4j-cloud-misc.user_summary_tables.user_event_table
group by 1,2;
--------------------------------------------------------------------------------------
drop table neo4j-cloud-misc.user_summary_tables.user_funnel;
create table neo4j-cloud-misc.user_summary_tables.user_funnel
as
select calendar_dt
,distinct_id
,max(case when aura_login_flag >0 then 1 else 0 end) as aura_login_flag
,max(case when console_dashboard_pageview_flag >0 then 1 else 0 end) as console_dashboard_pageview_flag
,max(case when create_db_pageview_flag >0 then 1 else 0 end) as create_db_pageview_flag
,max(case when create_db_step1_pageview_flag>0 then 1 else 0 end) as create_db_step1_pageview_flag
,max(case when create_db_payment_pageview_flag >0 then 1 else 0 end) as create_db_payment_pageview_flag
,max(case when db_created_flag >0 then 1 else 0 end) as db_created_flag
,max(case when import_pageview_flag >0 then 1 else 0 end) as import_pageview_flag
,max(case when help_link_flag>0 then 1 else 0 end) as help_link_flag
,max(case when console_dashboard_connect_flag>0 then 1 else 0 end) as console_dashboard_connect_flag
,max(case when feedback_click_cnt_flag>0 then 1 else 0 end) as feedback_click_cnt_flag
,max(case when account_pageview_cnt_flag>0 then 1 else 0 end) as account_pageview_cnt_flag
,max(case when ts_cs_accept_cnt_flag>0 then 1 else 0 end) as ts_cs_accept_cnt_flag
from
(select a.calendar_dt
,a.distinct_id
,max(case when aura_login_cnt >0 then 1 else 0 end) as aura_login_flag
,max(case when console_dashboard_pageview_cnt >0 then 1 else 0 end) as console_dashboard_pageview_flag
,max(case when create_db_pageview_cnt >0 then 1 else 0 end) as create_db_pageview_flag
,max(case when create_db_step1_pageview_cnt>0 then 1 else 0 end) as create_db_step1_pageview_flag
,max(case when create_db_payment_pageview_cnt >0 then 1 else 0 end) as create_db_payment_pageview_flag
,max(case when db_created_cnt >0 then 1 else 0 end) as db_created_flag
,max(case when import_pageview_cnt >0 then 1 else 0 end) as import_pageview_flag
,max(case when help_click_cnt>0 then 1 else 0 end) as help_link_flag
,max(case when console_dashboard_connect_cnt>0 then 1 else 0 end) as console_dashboard_connect_flag
,max(case when feedback_click_cnt>0 then 1 else 0 end) as feedback_click_cnt_flag
,max(case when account_pageview_cnt>0 then 1 else 0 end) as account_pageview_cnt_flag
,max(case when ts_cs_accept_cnt>0 then 1 else 0 end) as ts_cs_accept_cnt_flag
from neo4j-cloud-misc.user_summary_tables.daily_user_interaction_table a
left join neo4j-cloud-misc.user_summary_tables.user_mapping_table b
on a.distinct_id = b.distinct_id
group by 1,2
union all
select a.calendar_dt
,a.distinct_id
,max(case when aura_login_cnt >0 then 1 else 0 end) as aura_login_flag
,max(case when console_dashboard_pageview_cnt >0 then 1 else 0 end) as console_dashboard_pageview_flag
,max(case when create_db_pageview_cnt >0 then 1 else 0 end) as create_db_pageview_flag
,max(case when create_db_step1_pageview_cnt>0 then 1 else 0 end) as create_db_step1_pageview_flag
,max(case when create_db_payment_pageview_cnt >0 then 1 else 0 end) as create_db_payment_pageview_flag
,max(case when db_created_cnt >0 then 1 else 0 end) as db_created_flag
,max(case when import_pageview_cnt >0 then 1 else 0 end) as import_pageview_flag
,max(case when help_click_cnt>0 then 1 else 0 end) as help_link_flag
,max(case when console_dashboard_connect_cnt>0 then 1 else 0 end) as console_dashboard_connect_flag
,max(case when feedback_click_cnt>0 then 1 else 0 end) as feedback_click_cnt_flag
,max(case when account_pageview_cnt>0 then 1 else 0 end) as account_pageview_cnt_flag
,max(case when ts_cs_accept_cnt>0 then 1 else 0 end) as ts_cs_accept_cnt_flag
from neo4j-cloud-misc.user_summary_tables.daily_user_interaction_table a
left join neo4j-cloud-misc.user_summary_tables.user_mapping_table b
on a.distinct_id = b.user_id
group by 1,2
union all
select a.calendar_dt
,a.distinct_id
,max(case when aura_login_cnt >0 then 1 else 0 end) as aura_login_flag
,max(case when console_dashboard_pageview_cnt >0 then 1 else 0 end) as console_dashboard_pageview_flag
,max(case when create_db_pageview_cnt >0 then 1 else 0 end) as create_db_pageview_flag
,max(case when create_db_step1_pageview_cnt>0 then 1 else 0 end) as create_db_step1_pageview_flag
,max(case when create_db_payment_pageview_cnt >0 then 1 else 0 end) as create_db_payment_pageview_flag
,max(case when db_created_cnt >0 then 1 else 0 end) as db_created_flag
,max(case when import_pageview_cnt >0 then 1 else 0 end) as import_pageview_flag
,max(case when help_click_cnt>0 then 1 else 0 end) as help_link_flag
,max(case when console_dashboard_connect_cnt>0 then 1 else 0 end) as console_dashboard_connect_flag
,max(case when feedback_click_cnt>0 then 1 else 0 end) as feedback_click_cnt_flag
,max(case when account_pageview_cnt>0 then 1 else 0 end) as account_pageview_cnt_flag
,max(case when ts_cs_accept_cnt>0 then 1 else 0 end) as ts_cs_accept_cnt_flag
from neo4j-cloud-misc.user_summary_tables.daily_user_interaction_table a
left join neo4j-cloud-misc.user_summary_tables.user_mapping_table b
on a.distinct_id = b.distinct_id_before_identity
group by 1,2
)a group by 1,2;
--------------------------------------------------------------------------------------
select calendar_dt
,sum(case when aura_login_flag>0 then 1 else 0 end) as aura_login_flag
,sum(case when console_dashboard_pageview_flag>0 then 1 else 0 end) as console_dashboard_pageview_flag
,sum(case when create_db_pageview_flag>0 then 1 else 0 end) as create_db_pageview_flag
,sum(case when create_db_payment_pageview_flag>0 then 1 else 0 end) as create_db_payment_pageview_flag
,sum(case when db_created_flag>0 then 1 else 0 end) as db_created_flag
,sum(case when feedback_click_cnt_flag>0 then 1 else 0 end) as feedback_click_cnt_flag
,sum(case when account_pageview_cnt_flag>0 then 1 else 0 end) as account_pageview_cnt_flag
,sum(case when help_link_flag>0 then 1 else 0 end) as help_link_flag
,sum(case when import_pageview_flag>0 then 1 else 0 end) as import_pageview_flag
,sum(case when console_dashboard_connect_flag>0 then 1 else 0 end) as console_dashboard_connect_flag
,count(distinct_id) as user_cnt
from neo4jusersummarytables.mixpanelData.user_funnel
where distinct_id not in
(select distinct_id
from neo4jusersummarytables.mixpanelData.user_mapping_table
where email_address not like "%neo4j.com%"
and email_address not like "%neotechnology.com%")
group by 1
order by 1;
