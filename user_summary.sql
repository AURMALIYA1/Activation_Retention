-- Mixpanel data from Pageview table
with mp_pageview as (
    select distinct m.email_address as email
    , first_value(device_id) over (partition by email_address order by time) as device_id
    , first_value(mp_country_code) over (partition by email_address order by time) as mp_country
    , first_value(region) over (partition by email_address order by time) as mp_region
    , first_value(utm_source) over (partition by email_address order by time) as utm_source
    , first_value(utm_medium) over (partition by email_address order by time) as utm_medium
    , first_value(utm_campaign) over (partition by email_address order by time) as utm_campaign
    , first_value(utm_content) over (partition by email_address order by time) as utm_content
    , first_value(initial_referrer) over (partition by email_address order by time) as initial_referrer
    , first_value(initial_referring_domain) over (partition by email_address order by time) as initial_referring_domain
    , rank() over (partition by email_address order by time) as time_rank
    from `neo4j-cloud-misc.user_summary_tables.user_mapping_table` m
    join `neo4j-cloud-misc.mixpanel_exports_dev.page_view` e
    --join `neo4j-cloud-misc.mixpanel_exports_dev.aura_navigate_to` e
    on m.distinct_id = e.distinct_id
    where project_id = "4cf820094a8c88a45ca1a474080590a2" -- universal prod
)
, mp_attributes as (
    select *
    from mp_pageview 
    where time_rank = 1
)
-- Aggregate all dbs on the user level
, user_db_summary as (
    select email, userkey
    , if(user_classification = "External Private User",userkey,email_domain) as account_id
    , string_agg(distinct(db_billing_identity),"," order by db_billing_identity) as channels
    , count(distinct dbid) as total_db
    , count(case when db_destroyed_at is null then dbid end) as live_db
    --Free tier
    , min(case when db_billing_identity = "Free" then db_created_at end) as start_free_date
    , case when max(case when db_billing_identity = "Free" and db_destroyed_at is null then 1 else 0 end) = 0 then max(case when db_billing_identity = "Free" then db_destroyed_at end) end as churn_free_date
    --Pro tier
    , min(case when db_billing_identity in ("Direct", "GCP") then db_created_at end) as start_pro_date
    , case when max(case when db_billing_identity in ("Direct", "GCP") and db_destroyed_at is null then 1 else 0 end) = 0 then max(case when db_billing_identity in ("Direct", "GCP") then db_destroyed_at end) end as churn_pro_date
    from `neo4j-cloud-misc.aura_usage_metrics.canonical_database_list`
    group by 1,2,3
)

--first query and first load
,first_query as (
select l.email, l.userkey
       , min(case when db_billing_identity = "Free" then minute end) as first_query_free
       , min(case when db_billing_identity in ("Direct", "GCP") then minute end) as first_query_pro
  from `query_log_transforms.extended_per_minute_query_stats` s
  join `aura_usage_metrics.v_canonical_database_list` l on l.dbid = s.dbid
 where imputed_net_user_queries > 0
 group by 1,2
)
, first_load as (
select l.email, l.userkey
       , min(case when db_billing_identity = "Free" then minute end) as first_load_free
       , min(case when db_billing_identity in ("Direct", "GCP") then minute end) as first_load_pro
  from `query_log_transforms.extended_per_minute_query_stats` s
  join `aura_usage_metrics.v_canonical_database_list` l on l.dbid = s.dbid
 where imputed_load_statements > 0
 group by 1,2
)
-- live days
, active_days as (
    select email, userkey
    , sum(case when total_db > 0 then 1 else 0 end) as active_days
    , sum(case when total_db > 0 and db_billing_identity in ("Direct", "GCP") then 1 else 0 end) as active_days_pro
    , sum(case when total_db > 0 and db_billing_identity = "Free" then 1 else 0 end) as active_days_free
    from `neo4j-cloud-misc.user_summary_tables.daily_db_per_user`
    group by 1,2
)
-- become mrr_customer
, mrr_customer as (
    select email, userkey
    , min(case when timestamp_diff(date_add(date_trunc(db_created_at, DAY), INTERVAL 1 DAY), db_created_at, HOUR) < 2 then date(date_add(date_trunc(db_created_at, DAY), INTERVAL 1 DAY)) else date(db_created_at) end) as mrr_customer_since
    from `neo4j-cloud-misc.aura_usage_metrics.canonical_database_list`
    where db_billing_identity in ("Direct", "GCP")
    and timestamp_diff(ifnull(db_destroyed_at, current_timestamp()), db_created_at, HOUR) >= 2
    and date_diff(date(ifnull(db_destroyed_at, current_timestamp())), date(db_created_at), MONTH) > 0
    group by 1,2
)
, final as (
    select u.email, u.userkey
    , u.user_classification
    , db.account_id, db.channels
    , case when db.channels = "Free" then "Free"
        when db.channels in ("GCP", "Direct") then "Professional"
        when db.channels in ("Enterprise", "GDS") then db.channels
        when db.channels like "%Free%" and (db.channels like "%Direct%" or db.channels like "%GCP%") then "Free & Professional"
        when db.channels like "%Enterprise%" and (db.channels like "%Direct%" or db.channels like "%GCP%") then "Enterprise & Professional"
        when db.channels like "%Free%" and db.channels like "%Enterprise%" then "Free & Enterprise"
        end as tier
    , user_created_at
    -- mixpanel
    , device_id
    , mp_country, mp_region
    , initial_referrer as first_touch_referrer, initial_referring_domain as first_touch_referring_domain 
    , utm_source as first_touch_utm_source, utm_medium as first_touch_utm_medium, utm_campaign as first_touch_utm_campaign, utm_content as first_touch_utm_content
    -- Channel Grouping - logic might be updated overtime
    , case  -- display
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
            when lower(initial_referring_domain) like "%neo4j.com%" or lower(initial_referring_domain) like "%neotechnology.com%" then "Internal Referral"
            -- external referral
            when (lower(initial_referring_domain) is not null and lower(initial_referrer) != "$direct") or (lower(initial_referrer) = "$direct" and utm_source is not null) then "External Referral"
            --direct
            when initial_referrer = "$direct" then "Direct"
            end as first_touch_channel
    -- logs
    , case when total_db_count = 0 then 0 else 1 end as status_db_created
    , case when first_query is null then 0 else 1 end as status_activation
    , case when first_load is null then 0 else 1 end as status_load_data
    , total_db, live_db
    , active_days
    , start_free_date, churn_free_date, active_days_free
    , date(first_query_free) as first_query_free_date
    , date(first_load_free) as first_load_free_date
    , case when first_query_free is not null then 1 else 0 end as first_query_free
    , case when first_load_free is not null then 1 else 0 end as first_load_free
    , start_pro_date, churn_pro_date, active_days_pro
    , date(first_query_pro) as first_query_pro_date
    , date(first_load_pro) as first_load_pro_date
    , case when first_query_pro is not null then 1 else 0 end as first_query_pro
    , case when first_load_pro is not null then 1 else 0 end as first_load_pro
    , mrr_customer_since 
    from `neo4j-cloud-misc.aura_usage_metrics.canonical_user_list` u 
    left join mp_attributes mp
        on u.email = mp.email
    left join user_db_summary db 
        on u.email = db.email
        and u.userkey = db.userkey
    left join first_query q 
        on u.email = q.email 
        and u.userkey = q.userkey
    left join first_load l 
        on u.email = l.email
        and u.userkey =  l.userkey
    left join active_days a
        on u.email = a.email
        and u.userkey = a.userkey
    left join mrr_customer m
        on u.email = m.email
        and u.userkey = m.userkey
)
select *
from final