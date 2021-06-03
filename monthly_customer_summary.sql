with ts as (
-- Collect the last minute of the month
    select distinct timestamp_sub(date,INTERVAL 1 SECOND) as calculated_date
    from `aura_usage_metrics._calendar_and_clock`
    where date < current_timestamp()
        and extract(day from date)= 1
        and extract(hour from date) = 0
        and extract(minute from date) = 0
        and extract(second from date) = 0
)

-- GET ALL QUALIFIED DBs (MUST BE LIVE AT LEAST 2 HOURS)
, db_table as (
    select calculated_date
    , date(date_trunc(calculated_date, MONTH)) as as_of_date
    , email
    , userkey
    , db_billing_identity
    , if(user_classification = "External Private User",userkey,email_domain) as account_id
    , p.dbid 
    , db_created_at
    , db_destroyed_at
    , h.db_size/1024 as db_size
    , TRUNC((h.db_size/1024)*0.09*24*30,2) as mrr
    from ts
    join `neo4j-cloud-misc.aura_usage_metrics.v_canonical_database_list` p 
        on ts.calculated_date > p.db_created_at
        and date_trunc(ifnull(db_destroyed_at, current_timestamp()), month) >= date_trunc(ts.calculated_date, month)
        -- a db must be live for at least 2 hours and through the next day to be counted
        and timestamp_diff(ifnull(db_destroyed_at, current_timestamp() ),p.db_created_at,HOUR) >= 2
        and date_trunc(ifnull(db_destroyed_at, current_timestamp()), month) > date_trunc(db_created_at, month)
    -- get the gb_size on historical date 
    left join `aura_usage_metrics.database_size_history` h on h.dbid = p.dbid
        and ts.calculated_date >= ts_effective_from
        and ts.calculated_date <  ts_effective_thru
    where db_billing_identity in ("Free", "Direct", "GCP")
    and user_classification != "Internal"
)

-- GET START AND CHURN DATE FOR EACH TIER PER USER
, user_range as (
    select email, userkey, account_id
    , date(min(db_created_at)) as start_date  
    , case when max(case when db_destroyed_at is null then 1 else 0 end) = 0 then date(max(db_destroyed_at)) else null end as churn_date
    , date(min(case when db_billing_identity = "Free" then db_created_at end)) as free_start_date
    , case when max(case when db_billing_identity = "Free" and db_destroyed_at is null then 1 else 0 end) = 0 then date(max(case when db_billing_identity = "Free" then db_destroyed_at end)) else null end as free_churn_date
    , date(min(case when db_billing_identity in ("Direct", "GCP") then db_created_at end)) as pro_start_date
    , case when max(case when db_billing_identity in ("Direct", "GCP") and db_destroyed_at is null then 1 else 0 end) = 0 then date(max(case when db_billing_identity in ("Direct", "GCP") then db_destroyed_at end)) else null end as pro_churn_date
    , date(min(case when db_billing_identity in ("GCP") then db_created_at end)) as gcp_start_date
    , case when max(case when db_billing_identity in ("GCP") and db_destroyed_at is null then 1 else 0 end) = 0 then date(max(case when db_billing_identity in ("GCP") then db_destroyed_at end)) else null end as gcp_churn_date
    , date(min(case when db_billing_identity in ("Direct") then db_created_at end)) as direct_start_date
    , case when max(case when db_billing_identity in ("Direct") and db_destroyed_at is null then 1 else 0 end) = 0 then date(max(case when db_billing_identity in ("Direct") then db_destroyed_at end)) else null end as direct_churn_date
    from db_table
    group by 1,2,3
)

-- GET DAILY USAGE FOR FREE USERS WHEN THEY'RE LIVE
, free_user as (
    select calculated_date
    , as_of_date
    , db.email
    , db.userkey
    , db.account_id
    , free_start_date
    , free_churn_date
    -- only count active dbs that last through the end of the month
    , sum(case when date_diff(date(ifnull(db_destroyed_at, current_timestamp())), date(calculated_date), day) > 0 and timestamp_diff(calculated_date,db_created_at,HOUR) >= 2 then 1 end) as num_free_db
    , sum(case when date_diff(date(ifnull(db_destroyed_at, current_timestamp())), date(calculated_date), day) > 0 and timestamp_diff(calculated_date,db_created_at,HOUR) >= 2 then db_size end) as num_free_gb
    , 0 as num_free_mrr
    , lag(as_of_date) over (partition by db.email, db.userkey order by as_of_date asc) as previous_free_appearance
    , lead(as_of_date) over (partition by db.email, db.userkey order by as_of_date asc) as next_free_appearance
    from db_table db
    join user_range u 
        on db.email = u.email
        and db.userkey = u.userkey
    where db_billing_identity in ("Free")
    group by 1,2,3,4,5,6,7
)

-- GET DAILY USAGE FOR PRO USERS WHEN THEY'RE LIVE
, pro_user as (
    select calculated_date
    , as_of_date
    , db.email
    , db.userkey
    , db.account_id
    , pro_start_date
    , pro_churn_date
    -- only count active dbs that last through the next day
    , sum(case when date_diff(date(ifnull(db_destroyed_at, current_timestamp())), date(calculated_date), day) > 0 and timestamp_diff(calculated_date,db_created_at,HOUR) >= 2 then 1 end) as num_pro_db
    , sum(case when date_diff(date(ifnull(db_destroyed_at, current_timestamp())), date(calculated_date), day) > 0 and timestamp_diff(calculated_date,db_created_at,HOUR) >= 2 then db_size end) as num_pro_gb
    , sum(case when date_diff(date(ifnull(db_destroyed_at, current_timestamp())), date(calculated_date), day) > 0 and timestamp_diff(calculated_date,db_created_at,HOUR) >= 2 then mrr end) as num_pro_mrr
    , lag(as_of_date) over (partition by db.email, db.userkey order by as_of_date asc) as previous_pro_appearance
    , lead(as_of_date) over (partition by db.email, db.userkey order by as_of_date asc) as next_pro_appearance
    from db_table db
    join user_range u 
        on db.email = u.email
        and db.userkey = u.userkey
    where db_billing_identity in ("Direct", "GCP")
    group by 1,2,3,4,5,6,7
)

-- GET DAILY USAGE FOR GCP USERS WHEN THEY'RE LIVE
, gcp_user as (
    select calculated_date
    , as_of_date
    , db.email
    , db.userkey
    , db.account_id
    , gcp_start_date
    , gcp_churn_date
    -- only count active dbs that last through the next day
    , sum(case when date_diff(date(ifnull(db_destroyed_at, current_timestamp())), date(calculated_date), day) > 0 and timestamp_diff(calculated_date,db_created_at,HOUR) >= 2 then 1 end) as num_gcp_db
    , sum(case when date_diff(date(ifnull(db_destroyed_at, current_timestamp())), date(calculated_date), day) > 0 and timestamp_diff(calculated_date,db_created_at,HOUR) >= 2 then db_size end) as num_gcp_gb
    , sum(case when date_diff(date(ifnull(db_destroyed_at, current_timestamp())), date(calculated_date), day) > 0 and timestamp_diff(calculated_date,db_created_at,HOUR) >= 2 then mrr end) as num_gcp_mrr
    , lag(as_of_date) over (partition by db.email, db.userkey order by as_of_date asc) as previous_gcp_appearance
    , lead(as_of_date) over (partition by db.email, db.userkey order by as_of_date asc) as next_gcp_appearance
    from db_table db
    join user_range u 
        on db.email = u.email
        and db.userkey = u.userkey
    where db_billing_identity in ("GCP")
    group by 1,2,3,4,5,6,7
)

-- GET DAILY USAGE FOR DIRECT USERS WHEN THEY'RE LIVE
, direct_user as (
    select calculated_date
    , as_of_date
    , db.email
    , db.userkey
    , db.account_id
    , direct_start_date
    , direct_churn_date
    -- only count active dbs that last through the next day
    , sum(case when date_diff(date(ifnull(db_destroyed_at, current_timestamp())), date(calculated_date), day) > 0 and timestamp_diff(calculated_date,db_created_at,HOUR) >= 2 then 1 end) as num_direct_db
    , sum(case when date_diff(date(ifnull(db_destroyed_at, current_timestamp())), date(calculated_date), day) > 0 and timestamp_diff(calculated_date,db_created_at,HOUR) >= 2 then db_size end) as num_direct_gb
    , sum(case when date_diff(date(ifnull(db_destroyed_at, current_timestamp())), date(calculated_date), day) > 0 and timestamp_diff(calculated_date,db_created_at,HOUR) >= 2 then mrr end) as num_direct_mrr
    , lag(as_of_date) over (partition by db.email, db.userkey order by as_of_date asc) as previous_direct_appearance
    , lead(as_of_date) over (partition by db.email, db.userkey order by as_of_date asc) as next_direct_appearance
    from db_table db
    join user_range u 
        on db.email = u.email
        and db.userkey = u.userkey
    where db_billing_identity in ("Direct")
    group by 1,2,3,4,5,6,7
)

-- GET DAILY STATUS FOR USERS SINCE THEY START - EVERY DATE FOR EVERY USER
, user_table as (
    select ts.calculated_date
    , date(date_trunc(ts.calculated_date, MONTH)) as as_of_date
    , r.email
    , r.userkey
    , r.account_id
    , r.free_start_date
    , r.free_churn_date
    , ifnull(num_free_db,ifnull(lag(num_free_db) over (partition by r.email, r.userkey order by ts.calculated_date) *-1,0)) as num_free_db
    , ifnull(num_free_gb,ifnull(lag(num_free_gb) over (partition by r.email, r.userkey order by ts.calculated_date) *-1,0)) as num_free_gb
    , ifnull(num_free_mrr,ifnull(lag(num_free_mrr) over (partition by r.email, r.userkey order by ts.calculated_date) *-1,0)) as num_free_mrr
    , previous_free_appearance
    , next_free_appearance
    , case when num_free_db > 0 and previous_free_appearance is null then "New"
        when f.num_free_db > 0 and previous_free_appearance is not null and date_diff(date(ts.calculated_date), previous_free_appearance, month) = 1 then "Recurring"
        when f.num_free_db > 0 and previous_free_appearance is not null and date_diff(date(ts.calculated_date), previous_free_appearance, month) > 1 then "Reactivating"
        when f.num_free_db is null and previous_free_appearance is not null and date_diff(date(ts.calculated_date), previous_free_appearance, month) = 1 then "Churn"
        when f.num_free_db is null and previous_free_appearance is null and r.free_start_date < date(ts.calculated_date) and date(ts.calculated_date) < ifnull(r.free_churn_date, date(current_timestamp())) then "Dormant"
        end as free_status
    , r.pro_start_date
    , r.pro_churn_date
    , ifnull(num_pro_db,ifnull(lag(num_pro_db) over (partition by r.email, r.userkey order by ts.calculated_date) *-1,0)) as num_pro_db
    , ifnull(num_pro_gb,ifnull(lag(num_pro_gb) over (partition by r.email, r.userkey order by ts.calculated_date) *-1,0)) as num_pro_gb
    , ifnull(num_pro_mrr,ifnull(lag(num_pro_mrr) over (partition by r.email, r.userkey order by ts.calculated_date) *-1,0)) as num_pro_mrr
    , previous_pro_appearance
    , next_pro_appearance
    , case when num_pro_db > 0 and previous_pro_appearance is null then "New"
        when p.num_pro_db > 0 and previous_pro_appearance is not null and date_diff(date(ts.calculated_date), previous_pro_appearance, month) = 1 then "Recurring"
        when p.num_pro_db > 0 and previous_pro_appearance is not null and date_diff(date(ts.calculated_date), previous_pro_appearance, month) > 1 then "Reactivating"
        when p.num_pro_db is null and previous_pro_appearance is not null and date_diff(date(ts.calculated_date), previous_pro_appearance, month) = 1 then "Churn"
        when p.num_pro_db is null and previous_pro_appearance is null and r.pro_start_date < date(ts.calculated_date) and date(ts.calculated_date) < ifnull(r.pro_churn_date, date(current_timestamp())) then "Dormant"
        end as pro_status
    , r.gcp_start_date
    , r.gcp_churn_date
    , ifnull(num_gcp_db,ifnull(lag(num_gcp_db) over (partition by r.email, r.userkey order by ts.calculated_date) *-1,0)) as num_gcp_db
    , ifnull(num_gcp_gb,ifnull(lag(num_gcp_gb) over (partition by r.email, r.userkey order by ts.calculated_date) *-1,0)) as num_gcp_gb
    , ifnull(num_gcp_mrr,ifnull(lag(num_gcp_mrr) over (partition by r.email, r.userkey order by ts.calculated_date) *-1,0)) as num_gcp_mrr
    , previous_gcp_appearance
    , next_gcp_appearance
    , case when num_gcp_db > 0 and previous_gcp_appearance is null then "New"
        when num_gcp_db > 0 and previous_gcp_appearance is not null and date_diff(date(ts.calculated_date), previous_gcp_appearance, month) = 1 then "Recurring"
        when num_gcp_db > 0 and previous_gcp_appearance is not null and date_diff(date(ts.calculated_date), previous_gcp_appearance, month) > 1 then "Reactivating"
        when num_gcp_db is null and previous_gcp_appearance is not null and date_diff(date(ts.calculated_date), previous_gcp_appearance, month) = 1 then "Churn"
        when num_gcp_db is null and previous_gcp_appearance is null and r.gcp_start_date < date(ts.calculated_date) and date(ts.calculated_date) < ifnull(r.gcp_churn_date, date(current_timestamp())) then "Dormant"
        end as gcp_status
    , r.direct_start_date
    , r.direct_churn_date
    , ifnull(num_direct_db,ifnull(lag(num_direct_db) over (partition by r.email, r.userkey order by ts.calculated_date) *-1,0)) as num_direct_db
    , ifnull(num_direct_gb,ifnull(lag(num_direct_gb) over (partition by r.email, r.userkey order by ts.calculated_date) *-1,0)) as num_direct_gb
    , ifnull(num_direct_mrr,ifnull(lag(num_direct_mrr) over (partition by r.email, r.userkey order by ts.calculated_date) *-1,0)) as num_direct_mrr
    , previous_direct_appearance
    , next_direct_appearance
    , case when num_direct_db > 0 and previous_direct_appearance is null then "New"
        when num_direct_db > 0 and previous_direct_appearance is not null and date_diff(date(ts.calculated_date), previous_direct_appearance, month) = 1 then "Recurring"
        when num_direct_db > 0 and previous_direct_appearance is not null and date_diff(date(ts.calculated_date), previous_direct_appearance, month) > 1 then "Reactivating"
        when num_direct_db is null and previous_direct_appearance is not null and date_diff(date(ts.calculated_date), previous_direct_appearance, month) = 1 then "Churn"
        when num_direct_db is null and previous_direct_appearance is null and r.direct_start_date < date(ts.calculated_date) and date(ts.calculated_date) < ifnull(r.direct_churn_date, date(current_timestamp())) then "Dormant"
        end as direct_status
    from ts 
    cross join user_range r 
    left join pro_user p 
        on ts.calculated_date  = p.calculated_date 
        and r.email = p.email
        and r.userkey = p.userkey
    left join free_user f 
        on ts.calculated_date = f.calculated_date
        and r.userkey = f.userkey
        and r.email = f.email
    left join gcp_user g
        on ts.calculated_date = g.calculated_date
        and r.userkey = g.userkey
        and r.email = g.email
    left join direct_user d 
        on ts.calculated_date = d.calculated_date
        and r.userkey = d.userkey
        and r.email = d.email
    where date(ts.calculated_date) >= r.start_date
    and date_trunc(date(ts.calculated_date), month) <= date_trunc(ifnull(r.churn_date, date(current_timestamp())),month)
)

select as_of_date
    , calculated_date
    , email
    , userkey
    , account_id
    , num_free_db 
    , num_free_gb 
    , num_free_mrr
    , free_status
    , num_pro_db 
    , num_pro_gb 
    , num_pro_mrr
    , pro_status
    , num_direct_db 
    , num_direct_gb 
    , num_direct_mrr
    , direct_status
    , num_gcp_db 
    , num_gcp_gb 
    , num_gcp_mrr
    , gcp_status
from user_table

