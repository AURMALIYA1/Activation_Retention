/*
  Destination Table: neo4j-cloud-misc.user_summary_tables.user_summary
  Authors / Blame: Thao Duong, Stephen Fritz
*/

-- Temporary table for review
--drop table `data_experiments.unified_canonical_user_list`
-- select * from `data_experiments.unified_canonical_user_list` limit 100
create table `data_experiments.unified_canonical_user_list` as

-- Mixpanel data from Pageview table
with mp_events as (
select distinct distinct_id, 
       time, 
       device_id, 
       mp_country_code, 
       region, 
       utm_source, 
       utm_medium, 
       utm_campaign, 
       initial_referrer, 
       initial_referring_domain 
  from `neo4j-cloud-misc.mixpanel_exports_dev.page_view` 
 where project_id = "4cf820094a8c88a45ca1a474080590a2" -- universal prod
UNION ALL 
select distinct distinct_id, 
       time, 
       device_id, 
       mp_country_code, 
       region, 
       utm_source, 
       utm_medium, 
       utm_campaign, 
       initial_referrer, 
       initial_referring_domain
  from `neo4j-cloud-misc.mixpanel_exports_dev.aura_navigate_to`
 where project_id = "4cf820094a8c88a45ca1a474080590a2"


), mp_pageview as (
select distinct m.email_address as email,
       first_value(device_id) over (partition by email_address order by time) as device_id,
       first_value(mp_country_code) over (partition by email_address order by time) as mp_country,
       first_value(region) over (partition by email_address order by time) as mp_region,
       first_value(utm_source) over (partition by email_address order by time) as utm_source,
       first_value(utm_medium) over (partition by email_address order by time) as utm_medium,
       first_value(utm_campaign) over (partition by email_address order by time) as utm_campaign,
       first_value(initial_referrer) over (partition by email_address order by time) as initial_referrer,
       first_value(initial_referring_domain) over (partition by email_address order by time) as initial_referring_domain,
       rank() over (partition by email_address order by time) as time_rank
  from `neo4j-cloud-misc.user_summary_tables.user_mapping_table` m
  join mp_events e on m.distinct_id = e.distinct_id


), mp_attributes as (
select *
  from mp_pageview 
 where time_rank = 1


-- Aggregate DB data on the user level
), user_db_summary as (
select userkey,
       string_agg(distinct(db_billing_identity),"," order by db_billing_identity) as channels,
       count(distinct dbid) as total_db_count,
       count(case when db_destroyed_at is null then dbid end) as live_db_count,
       min(db_created_at) as start_date,
       round(sum(if(db_destroyed_at is NULL, most_recent_size_mb ,0)/1024),2) as live_gb,
       max(ifnull(db_destroyed_at,current_timestamp())) as most_recently_live, 
       case when max(case when db_destroyed_at is null then 1 else 0 end) = 0 then max(db_destroyed_at) end as churn_date,

    --Free tier
       min(case when db_billing_identity = "Free" then db_created_at end) as start_date_free,
       -- Return max Free Tier db_destroyed_at IF there are no free tier DBs where db_destroyed_at is null
       case when max(case when db_billing_identity = "Free" and db_destroyed_at is null then 1 else 0 end) = 0 then max(case when db_billing_identity = "Free" then db_destroyed_at end) end as churn_date_free,
       max(if(db_billing_identity = "Free",True,False)) as free_dbs_detected,
       sum(if(db_billing_identity = "Free" and db_destroyed_at is NULL,1,0)) as live_db_count_free,
       sum(if(db_billing_identity = "Free" and db_destroyed_at is NULL,most_recent_size_mb,0)) as live_gb_free,

    --Pro tier
       min(case when db_billing_identity in ("Direct", "GCP") then db_created_at end) as start_date_pro,
       -- Return max Pro Tier db_destroyed_at IF there are no Pro Tier DBs where db_destroyed_at is null
       case when max(case when db_billing_identity in ("Direct", "GCP") and db_destroyed_at is null then 1 else 0 end) = 0 then max(case when db_billing_identity in ("Direct", "GCP") then db_destroyed_at end) end as churn_date_pro,
       max(if(db_billing_identity = "Direct",True,False)) as direct_dbs_detected,
       sum(if(db_billing_identity = "Direct" and db_destroyed_at is NULL,1,0)) as live_db_count_direct,
       sum(if(db_billing_identity = "Direct" and db_destroyed_at is NULL,most_recent_size_mb,0)) as live_gb_direct,
       max(if(db_billing_identity = "GCP",True,False)) as gcp_dbs_detected,
       sum(if(db_billing_identity = "GCP" and db_destroyed_at is NULL,1,0)) as live_db_count_gcp,
       sum(if(db_billing_identity = "GCP" and db_destroyed_at is NULL,most_recent_size_mb,0)) as live_gb_gcp,

    --Enterprise tier
       min(case when db_billing_identity = "Enterprise" then db_created_at end) as start_date_enterprise,
       -- Return max Enterprise Tier db_destroyed_at IF there are no Pro Tier DBs where db_destroyed_at is null
       case when max(case when db_billing_identity = "Enterprise" and db_destroyed_at is null then 1 else 0 end) = 0 then max(case when db_billing_identity = "Enterprise" then db_destroyed_at end) end as churn_date_enterprise,
       max(if(db_billing_identity = "Enterprise",True,False)) as enterprise_dbs_detected,
       sum(if(db_billing_identity = "Enterprise" and db_destroyed_at is NULL,1,0)) as live_db_count_enterprise,
       sum(if(db_billing_identity = "Enterprise" and db_destroyed_at is NULL,most_recent_size_mb,0)) as live_gb_enterprise,

    --GDS tier
       min(case when db_billing_identity = "GDS" then db_created_at end) as start_date_gds,
       -- Return max GDS Tier db_destroyed_at IF there are no Pro Tier DBs where db_destroyed_at is null
       case when max(case when db_billing_identity = "GDS" and db_destroyed_at is null then 1 else 0 end) = 0 then max(case when db_billing_identity = "GDS" then db_destroyed_at end) end as churn_date_gds,
       max(if(db_billing_identity = "GDS",True,False)) as gds_dbs_detected,
       sum(if(db_billing_identity = "GDS" and db_destroyed_at is NULL,1,0)) as live_db_count_gds,
       sum(if(db_billing_identity = "GDS" and db_destroyed_at is NULL,most_recent_size_mb,0)) as live_gb_gds,

  from `neo4j-cloud-misc.aura_usage_metrics.canonical_database_list`
 group by userkey


-- Collect hourly usage & estimated billing info
), hourly_usage_by_tier as (
select l.userkey,
       l.db_billing_identity,
       bh.hour_of_day,
       count(bh.dbid) as db_count,
       sum(memory_gb) as memory_gb
  from `aura_usage_metrics.calculated_db_billing_per_hour` bh
  join `aura_usage_metrics.canonical_database_list` l on bh.dbid = l.dbid
 group by userkey,db_billing_identity,hour_of_day

), user_billing_summary as (
select userkey,
       sum(if(db_billing_identity = "Free",1,0)) as live_hours_free,
       sum(if(db_billing_identity = "Enterprise",1,0)) as live_hours_enterprise,
       sum(if(db_billing_identity = "GDS",1,0)) as live_hours_gds,
       sum(if(db_billing_identity in ("GCP","Direct"),1,0)) as live_hours_pro,
       
       sum(if(db_billing_identity = "Free",memory_gb,0)) as total_gb_hours_free,
       sum(if(db_billing_identity = "Enterprise",memory_gb,0)) as total_gb_hours_enterprise,
       sum(if(db_billing_identity = "GDS",memory_gb,0)) as total_gb_hours_gds,
       sum(if(db_billing_identity in ("GCP","Direct"),memory_gb,0)) as total_gb_hours_pro,
       
       round(sum(if(db_billing_identity in ("GCP","Direct"),memory_gb,0))*0.09,2) as total_revenue_dollars_pro
       
  from hourly_usage_by_tier
 group by userkey

-- Collect users first query and first load timestamps:
), first_query as (
select l.userkey,
       min(minute) as first_query,
       min(case when db_billing_identity = "Free" then minute end) as first_query_free,
       min(case when db_billing_identity = "GDS" then minute end) as first_query_gds,
       min(case when db_billing_identity in ("Direct", "GCP") then minute end) as first_query_pro,
  from `aura_usage_metrics.v_canonical_database_list` l 
  left join `query_log_transforms.extended_per_minute_query_stats` s on l.dbid = s.dbid
 where imputed_net_user_queries > 0
 group by userkey


), first_load as (
select l.userkey,
       min(minute) as first_load,
       min(case when db_billing_identity = "Free" then minute end) as first_load_free,
       min(case when db_billing_identity = "GDS" then minute end) as first_load_gds,
       min(case when db_billing_identity in ("Direct", "GCP") then minute end) as first_load_pro,
  from `aura_usage_metrics.v_canonical_database_list` l
  join `query_log_transforms.extended_per_minute_query_stats` s on l.dbid = s.dbid
 where imputed_load_statements > 0
 group by userkey

), first_tools_connection as (
-- Find the first example of a browser or bloom connection
select l.userkey,
       min(minute) as first_tools_connection,
       min(case when db_billing_identity = "Free" then minute end) as first_tools_connection_free,
       min(case when db_billing_identity = "GDS" then minute end) as first_tools_connection_gds,
       min(case when db_billing_identity in ("Direct", "GCP") then minute end) as first_tools_connection_pro,
  from `aura_usage_metrics.v_canonical_database_list` l
  join `query_log_transforms.extended_user_agent_per_minute_submission_stats` s on l.dbid = s.dbid
 where user_agent_name in ( "neo4j-browser", "neo4j-bloom")
    or user_agent_version in ('dev','0.0.0-dev')
 group by userkey

), first_programmatic_connection as (
-- Find the first example of a programmatic connection, IE with a driver such as the Python driver
select l.userkey,
       min(minute) as first_programmatic_connection,
       min(case when db_billing_identity = "Free" then minute end) as first_programmatic_connection_free,
       min(case when db_billing_identity = "GDS" then minute end) as first_programmatic_connection_gds,
       min(case when db_billing_identity in ("Direct", "GCP") then minute end) as first_programmatic_connection_pro,
  from `aura_usage_metrics.v_canonical_database_list` l
  join `query_log_transforms.extended_user_agent_per_minute_submission_stats` s on l.dbid = s.dbid
 where user_agent_name not in (
                               "Embedded Session",
                               "neo4j-browser",
                               "neo4j-bloom",
                               "neo4j-desktop",
                               "neo4j-cypher-shell"
                              )
   and user_agent_version not in ('dev','0.0.0-dev')
 group by userkey                             

), first_dump_upload as (
-- Note that we only have dump upload data as of mid-march 2021
select l.userkey,
        min(timestamp) as first_dump_upload,
        min(case when db_billing_identity = "Free" then timestamp end) as first_dump_upload_free,
        min(case when db_billing_identity = "GDS" then timestamp end) as first_dump_upload_gds,
        min(case when db_billing_identity in ("Direct", "GCP") then timestamp end) as first_dump_upload_pro,
   from `aura_usage_metrics.dump_file_uploads` du
   join `aura_usage_metrics.canonical_database_list` l on du.owner = l.email
  group by userkey

-- Summarize live days
), live_days as (
-- Count of days where user had at least one DB live
select userkey,
       sum(case when total_db > 0 then 1 else 0 end) as total_live_days,
       sum(case when total_db > 0 and db_billing_identity in ("Direct", "GCP") then 1 else 0 end) as live_days_pro,
       sum(case when total_db > 0 and db_billing_identity = "Free" then 1 else 0 end) as live_days_free,
       sum(case when total_db > 0 and db_billing_identity = "Enterprise" then 1 else 0 end) as live_days_enterprise,
       sum(case when total_db > 0 and db_billing_identity = "GDS" then 1 else 0 end) as live_days_gds,
  from `neo4j-cloud-misc.user_summary_tables.daily_db_per_user`
 group by userkey


-- Summarize User Monthly Recurring Revenue data
), mrr_customer as (
select email, 
       userkey,
       min(case when timestamp_diff(date_add(date_trunc(db_created_at, DAY), INTERVAL 1 DAY), db_created_at, HOUR) < 2 then date(date_add(date_trunc(db_created_at, DAY), INTERVAL 1 DAY)) else date(db_created_at) end) as mrr_customer_since,
  from `neo4j-cloud-misc.aura_usage_metrics.canonical_database_list`
 where db_billing_identity in ("Direct", "GCP")
   and timestamp_diff(ifnull(db_destroyed_at, current_timestamp()), db_created_at, HOUR) >= 2
   and date_diff(date(ifnull(db_destroyed_at, current_timestamp())), date(db_created_at), MONTH) > 0
 group by email,userkey


), user_first_appearance as (
-- Get the users first "PlanType" from the Datastore "User" snapshots
-- Note that PlanType became a thing in Aug 2020. For all users with a "NULL"
-- initial plan, we impute "self_serve"
select replace(regexp_extract(__key__.path,r".*, (.*)"),'"','') as userkey,
       min(snapshottime) as mts
  from `neo4j-cloud-misc.aura_dsfs_exports.datastore_User_snapshots`
 group by userkey

), user_initial_plan_type as (
select replace(regexp_extract(__key__.path,r".*, (.*)"),'"','') as userkey,
       ifnull(PlanType,"self_serve") as initial_plan_type
  from `neo4j-cloud-misc.aura_dsfs_exports.datastore_User_snapshots`
  join user_first_appearance ufp on replace(regexp_extract(__key__.path,r".*, (.*)"),'"','') = ufp.userkey and snapshottime = mts


), sfdc_contacts as (
-- Collect salesforce data
select email,
       "Contact" as sfdc_type,
       max(c.name) as contact_name,
       string_agg(distinct title) as titles_list,
       string_agg(distinct c.id) as contact_id_list,
       string_agg(distinct account_id) as account_id_list,
       string_agg(distinct industry) as industry_list,
       max(a.name) as account_name,
       string_agg(distinct a.type) as account_type_list,
       max(annual_revenue) as approx_annual_revenue,
  from `mixpanel-segment-exports.salesforce.contacts_view` c
  left join `mixpanel-segment-exports.salesforce.accounts_view` a on c.account_id = a.id
 where email is not NULL
 group by email

), sfdc_leads as (
select email,
       "Lead" as sfdc_type,
       max(name) as name,
       string_agg(distinct id) as lead_id_list, 
       string_agg(distinct industry) as industry_list,
       string_agg(distinct title) as titles_list
  from `mixpanel-segment-exports.salesforce.leads_view` 
 where converted_contact_id is NULL
 group by email



),final as (
-- Collate the final results

select -- User metadata
       distinct u.createdat as user_created_at,
       u.userkey,
       u.email, 
       u.user_classification,
       coalesce(contacts.sfdc_type,leads.sfdc_type) as sfdc_type,
       coalesce(contacts.titles_list,leads.titles_list) as sfdc_titles_list,
       coalesce(contacts.industry_list,leads.industry_list) as sfdc_industry_list,
       contacts.approx_annual_revenue,
       uipt.initial_plan_type,
       u.PlanType as current_plan_type,
       if(u.user_classification = "External Private User",u.userkey,u.email_domain) as account_id,
       zdu.id as zendesk_id,
       zdu.tags as zendesk_tags,
       s.subject_id,
       s.short_subject_id,
       first_db.db_billing_identity as joined_tier,
       timestamp_diff(first_db.db_created_at,user_created_at,SECOND) as seconds_from_register_to_first_db,
       timestamp_diff(first_tools_connection,first_db.db_created_at,SECOND) as seconds_from_first_db_to_first_tools_connection,
       timestamp_diff(first_query,first_db.db_created_at,SECOND) as seconds_from_first_db_to_first_query,
       timestamp_diff(first_load,first_db.db_created_at,SECOND) as seconds_from_first_db_to_first_load,
       timestamp_diff(first_dump_upload,first_db.db_created_at,SECOND) as seconds_from_first_db_to_first_upload,
       timestamp_diff(first_programmatic_connection,first_db.db_created_at,SECOND) as seconds_from_first_db_to_first_programmatic_connection,

       date_diff(date(first_db.db_created_at),date(user_created_at),DAY) as days_from_register_to_first_db,
       date_diff(date(first_tools_connection),date(first_db.db_created_at),DAY) as days_from_first_db_to_first_tools_connection,
       date_diff(date(first_query),date(first_db.db_created_at),DAY) as days_from_first_db_to_first_query,
       date_diff(date(first_load),date(first_db.db_created_at),DAY) as days_from_first_db_to_first_load,
       date_diff(date(first_dump_upload),date(first_db.db_created_at),DAY) as days_from_first_db_to_first_upload,
       date_diff(date(first_programmatic_connection),date(first_db.db_created_at),DAY) as days_from_first_db_to_first_programmatic_connection,

       db.channels,
       case when db.channels = "Free" then "Free"
            when db.channels in ("GCP", "Direct") then "Professional"
            when db.channels in ("Enterprise", "GDS") then db.channels
            when db.channels like "%Free%" and (db.channels like "%Direct%" or db.channels like "%GCP%") then "Free & Professional"
            when db.channels like "%Enterprise%" and (db.channels like "%Direct%" or db.channels like "%GCP%") then "Enterprise & Professional"
            when db.channels like "%Free%" and db.channels like "%Enterprise%" then "Free & Enterprise"
        end as tier,
       total_live_days,

    -- mixpanel
       device_id,
       mp_country, 
       mp_region,
       initial_referrer as first_touch_referrer, 
       initial_referring_domain as first_touch_referring_domain,
       utm_source as first_touch_utm_source, 
       utm_medium as first_touch_utm_medium, 
       utm_campaign as first_touch_utm_campaign,

    -- Channel Grouping - logic might be updated overtime
       case  -- accounts.google.com
            when lower(initial_referrer) like "%accounts.google.com%" and (utm_medium not in ("cpc", "ppc") or utm_medium is null) then "Google Oauth"
            -- display
            when (lower(utm_campaign) like "%display%" or lower(initial_referring_domain) like "%doubleclick%" or lower(utm_medium) = "banner") 
                and lower(utm_medium) not like "%paid-social%" then "Display"
            -- email
            when lower(utm_source) like "%email%" or lower(utm_medium) like "%email%" or lower(initial_referrer) like "%message.neo4j.com%" then "Email"
            -- paid search
            when (lower(utm_campaign) like "%search%" or lower(utm_medium) in ("cpc", "ppc")) and (lower(utm_campaign) not like "%display%" or utm_campaign is null) then "Paid Search"
            -- owned social
            when (lower(utm_medium) = "social" or lower(utm_source) like "%gaggle%" or initial_referrer like "%facebook%" or initial_referrer like "%linkedin%" or initial_referrer like "%twitter%" or initial_referrer like "%//t.co%")
                and (utm_medium = "social" or utm_medium is null or lower(utm_medium) like "%gaggle%") then "Owned Social"
            -- paid social
            when (lower(utm_source) like "%facebook%" or lower(utm_source) like "%linkedin%" or lower(utm_source) like "%twitter%" or lower(utm_medium) = "paid social")
                and (lower(utm_medium) like "%paid%" or lower(utm_medium) in ("cpc", "ppc") ) then "Paid Social"
            -- organic search
            when (lower(initial_referring_domain) like "%google%" or lower(initial_referring_domain) like "%bing%" or lower(initial_referring_domain) like "%yandex%" or lower(initial_referring_domain) like "%baidu%" or lower(initial_referring_domain) like "%duckduckgo%")
                and (lower(utm_medium) not in ("cpc", "ppc", "banner") or utm_medium is null) then "Organic Search"
            -- internal referral
            when lower(initial_referring_domain) like "%neo4j.com%" or lower(initial_referring_domain) like "%neotechnology.com%" or lower(initial_referring_domain) like "%neo4j.brand.live%" or lower(initial_referring_domain) like "%console.neo4j.io%" then "Internal Referral"
            -- external referral
            when (lower(initial_referring_domain) is not null and lower(initial_referrer) != "$direct") or (lower(initial_referrer) = "$direct" and utm_source is not null) then "External Referral"
            --direct
            when initial_referrer = "$direct" then "Direct"
        end as first_touch_channel,

    -- logs
       case when total_db_count is NULL then 0 else 1 end as status_db_created,
       case when first_query is null then 0 else 1 end as status_activation,
       case when first_load is null then 0 else 1 end as status_load_data,
       case when first_dump_upload is null then 0 else 1 end as status_load_dumpfile,
       case when first_programmatic_connection is null then 0 else 1 end as status_programmatic_connection,
       case when first_tools_connection is null then 0 else 1 end as status_tools_connection,
       ifnull(total_db_count,0) as total_db_count,
       ifnull(live_db_count,0) as live_db_count,
       start_date,
       churn_date,

    -- Aura Free Activity Summary
       ifnull(live_db_count_free,0) as live_db_count_free,
       start_date_free,
       churn_date_free,
       ifnull(live_days_free,0) as live_days_free,
       ifnull(live_hours_free,0) as live_hours_free,
       ifnull(total_gb_hours_free,0) as total_gb_hours_free,
       date(first_query_free) as first_query_free_date,
       date(first_tools_connection_free) as first_tools_connection_free_date,
       date(first_load_free) as first_load_free_date,
       date(first_programmatic_connection_free) as first_programmatic_connection_free_date,
       date(first_dump_upload_free) as first_dump_upload_free_date,
       case when first_query_free is not null then 1 else 0 end as status_first_query_free,
       case when first_load_free is not null then 1 else 0 end as status_first_load_free,
       case when first_dump_upload_free is not null then 1 else 0 end as status_first_dump_upload_free,
       case when first_programmatic_connection_free is not null then 1 else 0 end as status_first_programmatic_connection_free,
       case when first_tools_connection_free is not null then 1 else 0 end as status_first_tools_connection_free,

    -- Aura Pro Activity Summary
       ifnull(live_db_count_direct,0) as live_direct_db_count,
       ifnull(live_db_count_gcp,0) as live_gcp_db_count,
       start_date_pro, 
       churn_date_pro, 
       ifnull(live_days_pro,0) as live_days_pro,
       ifnull(live_hours_pro,0) as live_hours_pro,
       ifnull(total_gb_hours_pro,0) as total_gb_hours_pro,
       ifnull(total_revenue_dollars_pro,0) as total_revenue_dollars_pro,
       date(first_query_pro) as first_query_pro_date,
       date(first_tools_connection_pro) as first_tools_connection_pro,
       date(first_load_pro) as first_load_pro_date,
       date(first_programmatic_connection_pro) as first_programmatic_connection_pro,
       date(first_dump_upload_pro) as first_dump_upload_pro,
       case when first_query_pro is not null then 1 else 0 end as first_query_pro,
       case when first_load_pro is not null then 1 else 0 end as first_load_pro,
       case when first_dump_upload_pro is not null then 1 else 0 end as status_first_dump_upload_pro,
       case when first_programmatic_connection_pro is not null then 1 else 0 end as status_first_programmatic_connection_pro,
       case when first_tools_connection_pro is not null then 1 else 0 end as status_first_tools_connection_pro,

    -- Aura Enterprise Activity Summary
       ifnull(live_db_count_enterprise,0) as live_enterprise_db_count,
       start_date_enterprise, 
       churn_date_enterprise, 
       ifnull(live_days_enterprise,0) as live_days_enterprise,
       ifnull(live_hours_enterprise,0) as live_hours_enterprise,
       ifnull(total_gb_hours_enterprise,0) as total_gb_hours_enterprise,
       -- We do not have query logs for enterprise customers, hence some metrics are missing
       -- I include them here as placeholders
       --date(first_query_enterprise) as first_query_enterprise_date,
       --date(first_tools_connection_enterprise) as first_tools_connection_enterprise,
       --date(first_load_enterprise) as first_load_enterprise_date,
       --date(first_programmatic_connection_enterprise) as first_programmatic_connection_enterprise,
       --date(first_dump_upload_enterprise) as first_dump_upload_enterprise,
       --case when first_query_enterprise is not null then 1 else 0 end as first_query_enterprise,
       --case when first_load_enterprise is not null then 1 else 0 end as first_load_enterprise,

    -- Aura GDS Activity Summary
       ifnull(live_db_count_gds,0) as live_gds_db_count,
       start_date_gds, 
       churn_date_gds, 
       ifnull(live_days_gds,0) as live_days_gds,
       ifnull(live_hours_gds,0) as live_hours_gds,
       ifnull(total_gb_hours_gds,0) as total_gb_hours_gds,
       date(first_query_gds) as first_query_gds_date,
       date(first_tools_connection_gds) as first_tools_connection_gds,
       date(first_load_gds) as first_load_gds_date,
       date(first_programmatic_connection_gds) as first_programmatic_connection_gds,
       date(first_dump_upload_gds) as first_dump_upload_gds,
       case when first_query_gds is not null then 1 else 0 end as first_query_gds,
       case when first_load_gds is not null then 1 else 0 end as first_load_gds,
       case when first_dump_upload_gds is not null then 1 else 0 end as status_first_dump_upload_gds,
       case when first_programmatic_connection_gds is not null then 1 else 0 end as status_first_programmatic_connection_gds,
       case when first_tools_connection_gds is not null then 1 else 0 end as status_first_tools_connection_gds,

       mrr_customer_since ,

  from `neo4j-cloud-misc.aura_dsfs_exports.v_User` u 
  left join `mixpanel-segment-exports.zendesk.users_view` zdu on zdu.email = u.email 
  left join `aura_dsfs_exports.v_SubjectId` s on u.userkey = s.userkey
  left join `aura_usage_metrics.canonical_database_list` first_db on first_db.userkey = u.userkey and first_db.user_db_sequence = 1
  left join user_initial_plan_type uipt on uipt.userkey = u.userkey
  left join user_db_summary db on db.userkey = u.userkey
  left join user_billing_summary b on b.userkey = u.userkey
  left join mp_attributes mp on u.email = mp.email
  left join first_query q on u.userkey = q.userkey
  left join first_load l on u.userkey =  l.userkey
  left join first_programmatic_connection pc on u.userkey = pc.userkey
  left join first_tools_connection tc on u.userkey = tc.userkey
  left join first_dump_upload du on u.userkey = du.userkey
  left join live_days a on u.userkey = a.userkey
  left join mrr_customer m on u.userkey = m.userkey
  left join sfdc_leads leads on leads.email = u.email
  left join sfdc_contacts contacts on contacts.email = u.email
)
select *
  from final
