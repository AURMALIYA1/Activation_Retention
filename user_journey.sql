drop table akshayscratchpad.mixpanel_data.mp_aura_funnel_user_level;
create table akshayscratchpad.mixpanel_data.mp_aura_funnel_user_level
as
select DATE(TIMESTAMP_SECONDS(properties.time)) as calendar_date
,properties.distinct_id as distinct_id
,max(case when event = "AURA_NAVIGATE_TO_AURA_LANDING_PAGE" then 1 else 0 end) as Aura_landing_pageview_yn
,max(case when event = "AURA_PAGE_SIGNUP_CLICK" then 1 else 0 end) as Aura_signup_click_yn
,max(case when event = "AURA_NAVIGATE_TO_" then 1 else 0 end) as Aura_console_pageview_yn
,max(case when event = "AURA_LOGIN" then 1 else 0 end) as Aura_login_yn
,max(case when event = "AURA_NAVIGATE_TO_CREATE_DATABASE" then 1 else 0 end) as aura_create_db_pageview_yn
,max(case when event = "AURA_CREATE_DB" then 1 else 0 end) as Aura_create_db_yn
,max(case when event = "AURA_OPEN_DB" then 1 else 0 end) as aura_db_opened_yn
,max(case when event = "AURA_OPEN_DB" and properties.event_label = "neo4j_browser" then 1 else 0 end) as aura_db_opened_browser_yn
,max(case when event = "AURA_OPEN_DB" and properties.event_label = "neo4j_bloom" then 1 else 0 end) as aura_db_opened_bloom_yn
,max(case when event = "AURA_NAVIGATE_TO_EMAIL_VERIFICATION_REQUIRED" then 1 else 0 end) as aura_email_verification_pageview_yn
,max(case when event = "AURA_DESTROY_DB" then 1 else 0 end) as aura_db_destroyed_yn
,max(case when event = "AURA_NAVIGATE_TO_HOW_TO_CONNECT" then 1 else 0 end) as aura_connect_pageview_yn
,max(case when event = "AURA_NAVIGATE_TO_DATABASES" then 1 else 0 end) as aura_navigate_to_db
,max(case when event = "AURA_CHANGE_LANGUAGE" then 1 else 0 end) as aura_connect_language_change_yn
,max(case when event = "AURA_NAVIGATE_TO_IMPORT_INSTRUCTIONS" then 1 else 0 end) as aura_import_instructions_pageview_yn
,max(case when event = "AURA_FEEDBACK_LINK" then 1 else 0 end) as aura_feedback_pageview_yn
,max(case when event = "AURA_NAVIGATE_TO_ADD_PAYMENT" then 1 else 0 end) as aura_add_payment_pageview_yn
,max(case when event = "AURA_ADD_PAYMENT" then 1 else 0 end) as aura_add_payment_yn
from `akshayscratchpad.mixpanel_data.mp_aura_live_dec_2020`
group by 1,2;

select calendar_date
,count(distinct_id) as total_unique_users
,sum(Aura_landing_pageview_yn) as Aura_landing_pageview_yn
,sum(Aura_signup_click_yn) as Aura_signup_click_yn
,sum(aura_login_yn) as aura_login_yn
,sum(Aura_console_pageview_yn) as Aura_console_pageview_yn
,sum(aura_create_db_pageview_yn) as aura_create_db_pageview_yn
,sum(Aura_create_db_yn) as Aura_create_db_yn
,sum(aura_db_opened_yn) as aura_db_opened_yn
,sum(aura_navigate_to_db) as aura_navigate_to_db
,sum(aura_connect_pageview_yn) as aura_connect_pageview_yn
,sum(aura_import_instructions_pageview_yn) as aura_import_instructions_pageview_yn
,sum(aura_connect_language_change_yn) as aura_connect_language_change_yn
,sum(aura_email_verification_pageview_yn) as aura_email_verification_pageview_yn
,sum(aura_db_destroyed_yn) as aura_db_destroyed_yn
,sum(aura_add_payment_pageview_yn) as aura_add_payment_pageview_yn
,sum(aura_add_payment_yn) as aura_add_payment_yn
from `akshayscratchpad.mixpanel_data.mp_aura_funnel_user_level`
group by 1;
