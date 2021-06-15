/*
  Destination Table: neo4j-cloud-misc.user_summary_tables.user_summary
  Authors / Blame: Thao Duong, Stephen Fritz
*/

-- Mixpanel data from Pageview table
create table `data_experiments.unified_canonical_user_list` as
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


), user_db_summary as (
-- Aggregate all dbs on the user level
select userkey,
       string_agg(distinct(db_billing_identity),"," order by db_billing_identity) as channels,
       count(distinct dbid) as total_db_count,
       count(case when db_destroyed_at is null then dbid end) as live_db_count,
       round(sum(if(db_destroyed_at is NULL, most_recent_size_mb ,0)/1024),2) as live_gb,
       max(ifnull(db_destroyed_at,current_timestamp())) as most_recently_live, 

    --Free tier
       min(case when db_billing_identity = "Free" then db_created_at end) as start_free_date,
       -- Return max Free Tier db_destroyed_at IF there are no free tier DBs where db_destroyed_at is null
       case when max(case when db_billing_identity = "Free" and db_destroyed_at is null then 1 else 0 end) = 0 then max(case when db_billing_identity = "Free" then db_destroyed_at end) end as churn_free_date,
       max(if(db_billing_identity = "Free",True,False)) as free_dbs_detected,
       sum(if(db_billing_identity = "Free" and db_destroyed_at is NULL,1,0)) as live_free_db_count,
       sum(if(db_billing_identity = "Free" and db_destroyed_at is NULL,most_recent_size_mb,0)) as live_free_gb,

    --Pro tier
       min(case when db_billing_identity in ("Direct", "GCP") then db_created_at end) as start_pro_date,
       -- Return max Pro Tier db_destroyed_at IF there are no Pro Tier DBs where db_destroyed_at is null
       case when max(case when db_billing_identity in ("Direct", "GCP") and db_destroyed_at is null then 1 else 0 end) = 0 then max(case when db_billing_identity in ("Direct", "GCP") then db_destroyed_at end) end as churn_pro_date,
       max(if(db_billing_identity = "Direct",True,False)) as direct_dbs_detected,
       sum(if(db_billing_identity = "Direct" and db_destroyed_at is NULL,1,0)) as live_direct_db_count,
       sum(if(db_billing_identity = "Direct" and db_destroyed_at is NULL,most_recent_size_mb,0)) as live_direct_gb,
       max(if(db_billing_identity = "GCP",True,False)) as gcp_dbs_detected,
       sum(if(db_billing_identity = "GCP" and db_destroyed_at is NULL,1,0)) as live_gcp_db_count,
       sum(if(db_billing_identity = "GCP" and db_destroyed_at is NULL,most_recent_size_mb,0)) as live_gcp_gb,

    --Enterprise tier
       min(case when db_billing_identity = "Enterprise" then db_created_at end) as start_enterprise_date,
       -- Return max Enterprise Tier db_destroyed_at IF there are no Pro Tier DBs where db_destroyed_at is null
       case when max(case when db_billing_identity = "Enterprise" and db_destroyed_at is null then 1 else 0 end) = 0 then max(case when db_billing_identity = "Enterprise" then db_destroyed_at end) end as churn_enterprise_date,
       max(if(db_billing_identity = "Enterprise",True,False)) as enterprise_dbs_detected,
       sum(if(db_billing_identity = "Enterprise" and db_destroyed_at is NULL,1,0)) as live_enterprise_db_count,
       sum(if(db_billing_identity = "Enterprise" and db_destroyed_at is NULL,most_recent_size_mb,0)) as live_enterprise_gb,

    --GDS tier
       min(case when db_billing_identity = "GDS" then db_created_at end) as start_gds_date,
       -- Return max GDS Tier db_destroyed_at IF there are no Pro Tier DBs where db_destroyed_at is null
       case when max(case when db_billing_identity = "GDS" and db_destroyed_at is null then 1 else 0 end) = 0 then max(case when db_billing_identity = "GDS" then db_destroyed_at end) end as churn_gds_date,
       max(if(db_billing_identity = "GDS",True,False)) as gds_dbs_detected,
       sum(if(db_billing_identity = "GDS" and db_destroyed_at is NULL,1,0)) as live_gds_db_count,
       sum(if(db_billing_identity = "GDS" and db_destroyed_at is NULL,most_recent_size_mb,0)) as live_gds_gb,

  from `neo4j-cloud-misc.aura_usage_metrics.canonical_database_list`
 group by userkey


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
-- Fritz
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

), first_query as (
--first query and first load
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


), mrr_customer as (
-- become mrr_customer
select email, 
       userkey,
       min(case when timestamp_diff(date_add(date_trunc(db_created_at, DAY), INTERVAL 1 DAY), db_created_at, HOUR) < 2 then date(date_add(date_trunc(db_created_at, DAY), INTERVAL 1 DAY)) else date(db_created_at) end) as mrr_customer_since,
  from `neo4j-cloud-misc.aura_usage_metrics.canonical_database_list`
 where db_billing_identity in ("Direct", "GCP")
   and timestamp_diff(ifnull(db_destroyed_at, current_timestamp()), db_created_at, HOUR) >= 2
   and date_diff(date(ifnull(db_destroyed_at, current_timestamp())), date(db_created_at), MONTH) > 0
 group by email,userkey


), user_first_appearance as (
-- Get the first "PlanType" from the Datastore "User" snapshots
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


), final as (
select -- User metadata
       distinct u.createdat as user_created_at,
       u.userkey,
       u.email, 
       u.user_classification,
       uipt.initial_plan_type,
       u.PlanType as current_plan_type,
       if(u.user_classification = "External Private User",u.userkey,u.email_domain) as account_id,
       zdu.id as zendesk_id,
       zdu.tags as zendesk_tags,
       s.subject_id,
       s.short_subject_id,

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
       case when total_db_count = 0 then 0 else 1 end as status_db_created,
       case when first_query is null then 0 else 1 end as status_activation,
       case when first_load is null then 0 else 1 end as status_load_data,
       total_db_count, 
       live_db_count,

       live_free_db_count,
       start_free_date, 
       churn_free_date, 
       live_days_free,
       live_hours_free,
       total_gb_hours_free,
       date(first_query_free) as first_query_free_date,
       date(first_load_free) as first_load_free_date,
       case when first_query_free is not null then 1 else 0 end as first_query_free,
       case when first_load_free is not null then 1 else 0 end as first_load_free,

       live_direct_db_count,
       live_gcp_db_count,
       start_pro_date, 
       churn_pro_date, 
       live_days_pro,
       live_hours_pro,
       total_gb_hours_pro,
       total_revenue_dollars_pro,
       date(first_query_pro) as first_query_pro_date,
       date(first_load_pro) as first_load_pro_date,
       case when first_query_pro is not null then 1 else 0 end as first_query_pro,
       case when first_load_pro is not null then 1 else 0 end as first_load_pro,

       live_enterprise_db_count,
       start_enterprise_date, 
       churn_enterprise_date, 
       live_hours_enterprise,
       total_gb_hours_enterprise,
       -- We do not have query logs for enterprise customers, hence some metrics are missing
       --date(first_query_gds) as first_query_gds_date,
       --date(first_load_gds) as first_load_gds_date,
       --case when first_query_pro is not null then 1 else 0 end as first_query_gds,
       --case when first_load_pro is not null then 1 else 0 end as first_load_gds,

       live_gds_db_count,
       start_gds_date, 
       churn_gds_date, 
       live_days_gds,
       live_hours_gds,
       total_gb_hours_gds,
       date(first_query_gds) as first_query_gds_date,
       date(first_load_gds) as first_load_gds_date,
       case when first_query_pro is not null then 1 else 0 end as first_query_gds,
       case when first_load_pro is not null then 1 else 0 end as first_load_gds,

       mrr_customer_since ,

  from `neo4j-cloud-misc.aura_dsfs_exports.v_User` u 
  left join `mixpanel-segment-exports.zendesk.users_view` zdu on zdu.email = u.email 
  left join `aura_dsfs_exports.v_SubjectId` s on u.userkey = s.userkey
  left join user_initial_plan_type uipt on uipt.userkey = u.userkey
  left join user_db_summary db on db.userkey = u.userkey
  left join user_billing_summary b on b.userkey = u.userkey
  left join mp_attributes mp on u.email = mp.email
  left join first_query q on u.userkey = q.userkey
  left join first_load l on u.userkey =  l.userkey
  left join live_days a on u.userkey = a.userkey
  left join mrr_customer m on u.userkey = m.userkey
)
select *
  from final
