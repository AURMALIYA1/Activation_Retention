with ts as (
-- Collect the last minute of the day
    select distinct timestamp_sub(date,INTERVAL 1 SECOND) as calculated_date
    from `aura_usage_metrics._calendar_and_clock`
    where date < current_timestamp()
        and extract(hour from date) = 0
        and extract(minute from date) = 0
        and extract(second from date) = 0
)

-- GET ALL DBs
, db_table as (
    select distinct calculated_date
    , date(calculated_date) as as_of_date
    , email
    , userkey
    , db_billing_identity
    , user_classification
    , if(user_classification = "External Private User",userkey,email_domain) as account_id
    , p.dbid 
    , db_created_at
    , db_destroyed_at
    , h.db_size /1024 as db_size
    from ts
    join `neo4j-cloud-misc.aura_usage_metrics.v_canonical_database_list` p 
        on ts.calculated_date > p.db_created_at
        and date(ifnull(db_destroyed_at, current_timestamp())) >= date(ts.calculated_date)
    -- get the gb_size on historical date 
    left join `aura_usage_metrics.database_size_history` h on h.dbid = p.dbid
        and ts.calculated_date > ts_effective_from
        and ts.calculated_date <  ts_effective_thru
    where db_billing_identity in ("Free", "Direct", "GCP")
)

-- GET START AND CHURN DATE FOR EACH TIER PER USER
, user_range as (
    select email, userkey, account_id, user_classification
    , date(min(db_created_at)) as start_date  
    , case when max(case when db_destroyed_at is null then 1 else 0 end) = 0 then date(max(db_destroyed_at)) else null end as churn_date
    from db_table
    group by 1,2,3,4
)

select date(ts.calculated_date) as as_of_date
    , r.email
    , r.userkey
    , r.account_id
    , r.user_classification
    , db_billing_identity
    -- raw count of dbs
    , sum(case when date_diff(date(ifnull(db_destroyed_at, current_timestamp())), as_of_date, day) >= 0 then 1 end) as total_db
    -- qualified db last through the next day
    , sum(case when date_diff(date(ifnull(db_destroyed_at, current_timestamp())), as_of_date, day) > 0 and timestamp_diff(db.db_destroyed_at,db_created_at,DAY) > 0 then 1 end) as qualified_db
    , sum(case when date_diff(date(ifnull(db_destroyed_at, current_timestamp())), as_of_date, day) > 0 and timestamp_diff(db.db_destroyed_at,db_created_at,DAY) > 0 then db_size end) as qualified_gb
    , sum(case when date_diff(date(db_destroyed_at), as_of_date, day) = 0 and timestamp_diff(db.db_destroyed_at,db_created_at,DAY) > 0 then 1 end) as qualified_db_deleted
    , sum(case when date_diff(date(db_created_at), as_of_date, day) = 0 and timestamp_diff(db.db_destroyed_at,db_created_at,DAY) > 0 then 1 end) as qualified_db_new
    from ts
    cross join user_range r 
    left join db_table db
        on ts.calculated_date = db.calculated_date
        and r.email = db.email
        and r.userkey = db.userkey
    where date(ts.calculated_date) >= r.start_date
    and date(ts.calculated_date) <= ifnull(r.churn_date, date(current_timestamp()))
    and db_billing_identity is not null
    group by 1,2,3,4,5,6
    order by 2,1
