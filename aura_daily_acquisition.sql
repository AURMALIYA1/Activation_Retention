-- get first touch attribution from neo4j.com and console Mixpanel events
with first_touch as (
    select distinct distinct_id
    , first_value(initial_referrer) over (partition by distinct_id order by mp_processing_time_ms) as initial_referrer 
    , first_value(initial_referring_domain) over (partition by distinct_id order by mp_processing_time_ms) as initial_referring_domain
    , first_value(utm_source) over (partition by distinct_id order by mp_processing_time_ms) as utm_source
    , first_value(utm_medium) over (partition by distinct_id order by mp_processing_time_ms) as utm_medium
    , first_value(utm_campaign) over (partition by distinct_id order by mp_processing_time_ms) as utm_campaign
    , date(timestamp_millis(first_value(mp_processing_time_ms) over (partition by distinct_id order by mp_processing_time_ms))) as first_visit
    from 
    (select distinct_id, mp_processing_time_ms, initial_referrer, initial_referring_domain, utm_source, utm_medium, utm_campaign
        from `neo4j-cloud-misc.mixpanel_exports_dev.page_view`
        where project_id = "4cf820094a8c88a45ca1a474080590a2" 
        union all 
     select distinct_id, mp_processing_time_ms, initial_referrer, initial_referring_domain, utm_source, utm_medium, utm_campaign
        from `neo4j-cloud-misc.mixpanel_exports_dev.aura_navigate_to`
        where project_id = "4cf820094a8c88a45ca1a474080590a2" ) T
)
-- logic to group first touch attribution to different channels
, first_touch_channel as (
    select distinct distinct_id, first_visit
    -- first touch
    , initial_referrer, initial_referring_domain
    , utm_source, utm_medium, utm_campaign
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
            when lower(initial_referring_domain) like "%neo4j.com%" or lower(initial_referring_domain) like "%neotechnology.com%" or lower(initial_referring_domain) like "%neo4j.brand.live%" or lower(initial_referring_domain) like "%console.neo4j.io%" then "Internal Referral"
            -- external referral
            when (lower(initial_referring_domain) is not null and lower(initial_referrer) != "$direct") or (lower(initial_referrer) = "$direct" and utm_source is not null) then "External Referral"
            --direct
            when initial_referrer = "$direct" then "Direct"
            end as first_touch_channel
    from first_touch 
)
-- all visits to Aura LP and Aura Pricing Page
, visitors as (
    select distinct p.distinct_id, first_visit, date(timestamp_millis(mp_processing_time_ms)) as visited_date, timestamp_millis(mp_processing_time_ms) as visited_time, pathname
    -- first touch
    , f.initial_referrer, f.initial_referring_domain
    , f.utm_source, f.utm_medium, f.utm_campaign
    , first_touch_channel
    -- last touch
    , referrer, referring_domain
    , case  -- null
            when referrer = "" then "Direct or N/A"
            -- display
            when (lower(referrer) like "%campaign%display%" or lower(referrer) like "%doubleclick%" or lower(referrer) like "%utm_medium%banner%") 
                and lower(referrer) not like "%utm_medium%paid-social%" then "Display"
            -- email
            when lower(referrer) like "%utm_%email%" or lower(referrer) like "%message.neo4j.com%" then "Email"
            -- paid search
            when (lower(referrer) like "%utm_campaign%search%" or lower(referrer) like "%utm_medium%cpc%" or lower(referrer) like "%utm_medium%ppc%") and (lower(referrer) not like "%utm_campaign%display%" or lower(referrer) not like "%utm_campaign%") then "Paid Search"
            -- paid social
            when (lower(referrer) like "%gaggle%" or referrer like "%facebook%" or referrer like "%linkedin%" or referrer like "%twitter%" or referrer like "%//t.co%" or lower(referrer) = "%utm_medium%paid%social%")
                and (lower(referrer) like "%utm_medium%paid%" or lower(referrer) like "%utm_medium%cpc%" or lower(referrer) like "%utm_medium%ppc%") then "Paid Social"
            -- owned social
            when (lower(referrer) like "%gaggle%" or referrer like "%facebook%" or referrer like "%linkedin%" or referrer like "%twitter%" or referrer like "%//t.co%")
                and (lower(referrer) like "%utm_medium%social%" or lower(referrer) not like "%utm_medium%" or lower(referrer) like "%utm_medium%gaggle%" ) then "Owned Social"
            -- organic search
            when (lower(referrer) like "%google%" or lower(referrer) like "%bing%" or lower(referrer) like "%yandex%" or lower(referrer) like "%baidu%" or lower(referrer) like "%duckduckgo%")
                and (lower(referrer) not like "%utm_medium%cpc%" and lower(referrer) not like "%utm_medium%ppc%") then "Organic Search"
            -- internal referral
            when lower(referrer) like "%neo4j.com%" or lower(referrer) like "%neotechnology.com%" then "Internal Referral"
            -- external referral
            when (lower(referrer) is not null and lower(referrer) != "$direct") or (lower(referrer) = "$direct" and lower(referrer) like "%utm_source%") then "External Referral"
            --direct (referrer does not contain $direct as an option, which is different from first_touch)
            --when referrer = "$direct" then "Direct"
            end as last_touch_channel
    , mp_country_code
    from `neo4j-cloud-misc.mixpanel_exports_dev.page_view` p
    join first_touch_channel f 
    on p.distinct_id = f.distinct_id
    where project_id = "4cf820094a8c88a45ca1a474080590a2" 
    and pathname in ("https://neo4j.com/cloud/aura/", "https://neo4j.com/cloud/aura/pricing/")
    and timestamp_millis(mp_processing_time_ms) between "2021-01-01" and current_timestamp() 
)
-- group user activities by day to look at the last touch attribution by user by day (proxy for sessions)
, daily_visitors as (
    select distinct distinct_id, first_visit, visited_date
    , case when visited_date > first_visit then "Returning" 
        when visited_date = first_visit then "New"
        end as user_type
    , first_value(initial_referrer) over (partition by distinct_id, visited_date order by visited_time) as first_touch_referrer
    , first_value(initial_referring_domain) over (partition by distinct_id, visited_date order by visited_time) as first_touch_referring_domain
    , first_value(utm_source) over (partition by distinct_id, visited_date order by visited_time) as first_touch_utm_source
    , first_value(utm_medium) over (partition by distinct_id, visited_date order by visited_time) as first_touch_utm_medium
    , first_value(utm_campaign) over (partition by distinct_id, visited_date order by visited_time) as first_touch_utm_campaign
    , first_value(first_touch_channel) over (partition by distinct_id, visited_date order by visited_time) as first_touch_channel
    , first_value(referrer) over (partition by distinct_id, visited_date order by visited_time) as last_touch_referrer
    , first_value(referring_domain) over (partition by distinct_id, visited_date order by visited_time) as last_touch_referring_domain
    , first_value(last_touch_channel) over (partition by distinct_id, visited_date order by visited_time) as last_touch_channel
    , first_value(mp_country_code) over (partition by distinct_id, visited_date order by visited_time) as mp_country_code
    from visitors
)
-- user registration table
, registers as (
    select distinct distinct_id, date(user_created_at) as registered_date
    from `neo4j-cloud-misc.aura_usage_metrics.canonical_user_list` u 
    join `neo4j-cloud-misc.user_summary_tables.user_mapping_table` m 
    on u.email = m.email_address
)

select v.*, case when r.registered_date is not null then v.distinct_id end as registered_id
from daily_visitors v 
left join registers r 
on v.distinct_id = r.distinct_id
and v.visited_date  = r.registered_date 
order by 1,2,3