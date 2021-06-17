/* ************************************************************************** 
   STAGES 1 - 3 extract the structured logs and perform the (relatively few)
   bits of regular expression that need to happen before storing the result.
* **************************************************************************/

-- STAGE 1: Extract yesterdays structured logs from Aura Pro
--          and create a temp table
create table `query_log_transforms._tmp_yesterday_qlog_snapshot` as
select timestamp,
       severity,
       jsonPayload.dbid,
       jsonPayload.neo4jversion,
       jsonPayload.username,
       jsonPayload.database,
       jsonPayload.event,
       jsonPayload.id,
       split(jsonPayload.source,"\t")[OFFSET(0)] as bolt_session,
       case when array_length(split(jsonPayload.source,"\t")) > 1 THEN split(jsonPayload.source,"\t")[OFFSET(1)] end as bolt,
       case when array_length(split(jsonPayload.source,"\t")) > 2 THEN split(jsonPayload.source,"\t")[OFFSET(2)] end as driver_info,
       case when array_length(split(jsonPayload.source,"\t")) > 3 THEN split(jsonPayload.source,"\t")[OFFSET(3)] end as gap,
       case when array_length(split(jsonPayload.source,"\t")) > 4 THEN split(jsonPayload.source,"\t")[OFFSET(4)] end as client,
       case when array_length(split(jsonPayload.source,"\t")) > 5 THEN split(jsonPayload.source,"\t")[OFFSET(5)] end as server,
       if(0 > jsonPayload.allocatedbytes,0,jsonPayload.allocatedbytes) as allocatedbytes,
       if(0 > jsonPayload.elapsedtimems,0,jsonPayload.elapsedtimems) as elapsedtimems,
       jsonPayload.pagehits,
       jsonPayload.pagefaults,
       jsonPayload.query,
       jsonPayload.runtime,
       jsonPayload.annotationdata,
       jsonPayload.failurereason,
       case when jsonPayload.stacktrace is not NULL then split(jsonPayload.stacktrace,":")[OFFSET(0)] end as exception,
       jsonPayload.stacktrace
 from `neo4j-cloud.production_query_logs.neo4j_query_*`  
where _TABLE_SUFFIX = FORMAT_DATE("%Y%m%d",date_sub(current_date(),INTERVAL 1 DAY)) -- Production Query Logs
  and jsonPayload.query is not NULL;

------------------------------------
-- STAGE 2: Extract yesterdays structured logs from Aura Free
--          and insert them into the temp table created in
--          the previous step
insert into `query_log_transforms._tmp_yesterday_qlog_snapshot` 
select timestamp,
       severity,
       jsonPayload.dbid,
       jsonPayload.neo4jversion,
       jsonPayload.username,
       jsonPayload.database,
       jsonPayload.event,
       jsonPayload.id,
       split(jsonPayload.source,"\t")[OFFSET(0)] as bolt_session,
       case when array_length(split(jsonPayload.source,"\t")) > 1 THEN split(jsonPayload.source,"\t")[OFFSET(1)] end as bolt,
       case when array_length(split(jsonPayload.source,"\t")) > 2 THEN split(jsonPayload.source,"\t")[OFFSET(2)] end as driver_info,
       case when array_length(split(jsonPayload.source,"\t")) > 3 THEN split(jsonPayload.source,"\t")[OFFSET(3)] end as gap,
       case when array_length(split(jsonPayload.source,"\t")) > 4 THEN split(jsonPayload.source,"\t")[OFFSET(4)] end as client,
       case when array_length(split(jsonPayload.source,"\t")) > 5 THEN split(jsonPayload.source,"\t")[OFFSET(5)] end as server,
       if(0 > jsonPayload.allocatedbytes,0,jsonPayload.allocatedbytes) as allocatedbytes,
       if(0 > jsonPayload.elapsedtimems,0,jsonPayload.elapsedtimems) as elapsedtimems,
       jsonPayload.pagehits,
       jsonPayload.pagefaults,
       jsonPayload.query,
       jsonPayload.runtime,
       jsonPayload.annotationdata,
       jsonPayload.failurereason,
       case when jsonPayload.stacktrace is not NULL then split(jsonPayload.stacktrace,":")[OFFSET(0)] end as exception,
       jsonPayload.stacktrace
  from `ni-production-ro5f.free_tier_query_logs.neo4j_query` 
 WHERE date(timestamp) = date_sub(current_date(),INTERVAL 1 DAY)                  -- Free Tier Query Logs
   and jsonPayload.query is not NULL;

------------------------------------
-- STAGE 3: A little extra parsing to extract the driver name
--          and version along with the annotation data, and
--          insert them into the structured log
insert into  `query_log_transforms._structured_query_log_extract`
WITH driver_info_extracted as (
-- The jsonPayload.source field contains connection & driver information
-- Which needs a little massaging
select timestamp,
       severity,
       dbid,
       neo4jversion,
       username,
       database,
       event,
       id,
       case array_length(regexp_extract_all(driver_info,r"(/)")) 
          WHEN 2 THEN regexp_extract(driver_info,r"^(.*)/.* .*/.* \(.*\)$")
          WHEN 1 THEN regexp_extract(driver_info,r"^(.*)/.*$")
          --WHEN 0 THEN regexp_extract(driver_info,r"^(.*)\s+.*$")
          ELSE CASE
                 WHEN regexp_contains(driver_info,r"\s+") THEN regexp_extract(driver_info,r"^.*\s+(.*)$")
                 WHEN bolt_session = "embedded-session" then bolt_session
                 WHEN regexp_contains(driver_info,r"shipment-registry") then driver_info
                END              
        END as driver_name,
       case array_length(regexp_extract_all(driver_info,r"(/)")) 
          WHEN 2 THEN regexp_extract(driver_info,r"^.*/(.*) .*/.* \(.*\)$")
          WHEN 1 THEN regexp_extract(driver_info,r"^.*/(.*)$")
          --WHEN 0 THEN regexp_extract(driver_info,r"^.*\s+(.*)$")
          ELSE CASE
                 WHEN regexp_contains(driver_info,r"\s+") THEN regexp_extract(driver_info,r"^.*\s+(.*)$")
                 WHEN bolt_session = "embedded-session" then "NA"
                 WHEN regexp_contains(driver_info,r"shipment-registry") then "NA"
                END
              
        END as driver_version,
       --client,
       --server,
       elapsedtimems,
       allocatedbytes,
       pagehits,
       pagefaults,
       query,
       runtime,
       annotationdata,
       regexp_extract(annotationdata,r"^{([a-z]+):\s'.*',\s[a-z]+: '.*'}") as annotationdata_field1_key,
       regexp_extract(annotationdata,r"^{[a-z]+:\s'(.*)',\s[a-z]+: '.*'}") as annotationdata_field1_val,
       regexp_extract(annotationdata,r"^{[a-z]+:\s'.*',\s([a-z]+): '.*'}") as annotationdata_field2_key,
       regexp_extract(annotationdata,r"^{[a-z]+:\s'.*',\s[a-z]+: '(.*)'}") as annotationdata_field2_val,
       failurereason,
       exception,
       stacktrace,
  from `query_log_transforms._tmp_yesterday_qlog_snapshot`

)

-- The annotationdata field is used by browser & bloom to note which queries
-- were executed by the user directly ('user-direct'), which were executed in
-- support of some user action ('user-action'), or as internal system maintenance
-- queries ('system'). Halin is a known tool that (at a high level) performs a 
-- similar function, but doesn't use the annotationdata field. This CTE 
-- normalizes for such things, and should be updated as new tools are written and/
-- or discovered
select timestamp,
       severity,
       dbid,
       neo4jversion,
       username,
       database,
       event,
       id,
       driver_name,
       driver_version,
       elapsedtimems,
       allocatedbytes,
       pagehits,
       pagefaults,
       query,
       runtime,
       annotationdata,
       CASE
         WHEN driver_name = "halin" THEN "user-action"
         WHEN annotationdata_field1_key = "type" THEN annotationdata_field1_val
         WHEN annotationdata_field2_key = "type" THEN annotationdata_field2_val
         WHEN annotationdata = "{}" THEN "user-direct"
        END as query_submission_type,
       CASE
         WHEN driver_name = "halin" THEN concat("halin_",split(driver_version," ")[OFFSET(0)])
         WHEN annotationdata_field1_key = "app" THEN annotationdata_field1_val
         WHEN annotationdata_field2_key = "app" THEN annotationdata_field2_val
         --WHEN annotationdata = "{}" THEN "user-direct"
        END as query_submission_app,
       failurereason,
       exception,
       stacktrace,
  from driver_info_extracted;


/* **************************************************************************
   STAGES 4 - 6 summarize the STRUCTURED query logs in in the same way as the
   unstructured logs, to provide an apples-to-apples bridge for analytics. 
   
* **************************************************************************/

-- STAGE 4: Summarize the extracted structured logs by dbid & minute:
--drop table `data_experiments._tmp_extended_per_minute_query_stats_structured`
--create table `data_experiments._tmp_extended_per_minute_query_stats_structured` as 
insert into `query_log_transforms.extended_per_minute_query_stats`
select timestamp_trunc(timestamp,MINUTE) as minute,
       dbid,
       sum(if(event = "start",1,0)) as submitted_queries,
       sum(if(event = "start" and query_submission_type = "user-direct",1,0)) as imputed_net_user_queries,
       sum(if(event = "start" and failurereason is not NULL,1,0)) as total_submit_errors,
       sum(if(event = "start" and query_submission_type = "user-direct",array_length(regexp_extract_all(query,r"\n"))+1,0)) as imputed_submitted_query_lines, 
       sum(if(event = "start" and query_submission_type = "user-direct",length(query),0)) as imputed_total_submitted_cypher_characters,
       round(ifnull(safe_divide(sum(if(event = "start" and query_submission_type = "user-direct",array_length(regexp_extract_all(query,r"\n"))+1,0)), sum(if(event = "start" and query_submission_type = "user-direct",1,0))),0)) imputed_avg_lines_per_submitted_query ,
       round(ifnull(safe_divide(sum(if(event = "start" and query_submission_type = "user-direct",length(query),0)),sum(if(event = "start" and query_submission_type = "user-direct",1,0))),0)) imputed_avg_characters_per_submitted_query,
       cast(sum(if(event = "start" ,elapsedtimems,0)) as int64) as submitted_millis,
       0 as submitted_planning_millis,
       0 as submitted_waiting_millis,
       cast(sum(if(event = "start" ,allocatedbytes,0)) as int64) as submitted_memory_bytes,
       cast(sum(if(event = "start" ,pagehits,0)) as int64) as submitted_page_hits, 
       cast(sum(if(event = "start" ,pagefaults,0)) as int64) as submitted_page_faults, 
       sum(if(event = "commit",1,0)) as completed_queries ,
       sum(if(event = "commit" and query_submission_type = "user-direct",1,0)) as imputed_net_completed_queries ,
       --sum(if(event = "commit" and query_submission_type = "user-direct",1,0)) as imputed_net_user_queries,
       sum(if(event = "commit" and NULL in (failurereason,stacktrace),1,0)) as completed_errors,
       sum(if(event = "commit" and query_submission_type = "user-direct",array_length(regexp_extract_all(query,r"\n"))+1,0)) as imputed_completed_query_lines , 
       sum(if(event = "commit" and query_submission_type = "user-direct",length(query),0)) as imputed_total_completed_cypher_characters ,
       round(ifnull(safe_divide(sum(if(event = "commit" and query_submission_type = "user-direct",array_length(regexp_extract_all(query,r"\n"))+1,0)), sum(if(event = "commit" and query_submission_type = "user-direct",1,0))),0)) imputed_avg_lines_per_completed_query ,
       round(ifnull(safe_divide(sum(if(event = "commit" and query_submission_type = "user-direct",length(query),0)),sum(if(event = "commit" and query_submission_type = "user-direct",1,0))),0)) imputed_avg_characters_per_completed_query,
       cast(sum(if(event = "commit",elapsedtimems,0)) as int64) as completed_millis ,
       0 as completed_planning_millis ,
       0 as completed_waiting_millis ,
       cast(sum(if(event = "commit",allocatedbytes,0)) as int64) as completed_memory_bytes ,
       cast(sum(if(event = "commit",pagehits,0)) as int64) as completed_page_hits , 
       cast(sum(if(event = "commit",pagefaults,0)) as int64) as completed_page_faults , 
       sum(if(query_submission_type = "user-direct" and stacktrace is not NULL,1,0)) as exceptions,
       sum(if(query_submission_type = "user-direct",ifnull(array_length(regexp_extract_all(stacktrace,r"\n"))+1,0),0)) stacktrace_lines,
       sum(ifnull(length(exception),0)) as error_message_characters,
       sum(if(event = "start" and query_submission_type = "user-direct" and (lower(query) like "%load csv %" or lower(query) like "% load csv %"),1,0)) as imputed_load_statements,
       sum(if(event = "start" and query_submission_type = "user-direct" and (lower(query) like "match %" or lower(query) like "% match %"),1,0)) as imputed_match_lines,
       sum(if(event = "start" and query_submission_type = "user-direct" and (lower(query) like "merge %" or lower(query) like "% merge %"),1,0)) as imputed_merge_lines,
       sum(if(event = "start" and query_submission_type = "user-direct" and (lower(query) like "create %" or lower(query) like "% create %"),1,0)) as imputed_create_lines,
       count(distinct username) as named_users,
       STRING_AGG(distinct username) as userlist

  from  `query_log_transforms._structured_query_log_extract`
 where date(timestamp) = date_sub(current_date(),INTERVAL 1 DAY)
 group by dbid, minute;
 
-------------------------------------------------------------------------

-- STAGE 2: Summarizes yesterdays SUBMITTED queries by driver and version

-- drop table `data_experiments._tmp_extended_user_agent_per_minute_submission_stats`
--create table `data_experiments._tmp_extended_user_agent_per_minute_submission_stats` as 
insert into `query_log_transforms.extended_user_agent_per_minute_submission_stats`
select timestamp_trunc(timestamp,MINUTE) as minute,
       dbid,
       driver_name as user_agent_name,
       driver_version as user_agent_version,
       sum(if(query_submission_type="user-direct",1,0)) as submitted_queries,
       sum(if(query_submission_type="user-direct" and failurereason is not NULL,1,0)) as submit_errors,
       count(distinct username) as named_users,
       cast(sum(elapsedtimems) as int64) as submitted_millis,
       0 as submitted_planning_millis,
       0 as submitted_waiting_millis,
       cast(sum(allocatedbytes) as int64) as submitted_memory_bytes,
       cast(sum(pagehits) as int64) as submitted_page_hits,
       cast(sum(pagefaults) as int64) as submitted_page_faults
  from `query_log_transforms._structured_query_log_extract`
 where date(timestamp) = date_sub(current_date(),INTERVAL 1 DAY)
   and event = "start"
 group by minute,dbid,user_agent_name,user_agent_version;

-------------------------------------------------------------------------

-- STAGE 3: Summarizes yesterdays COMPLETED queries by driver and version

-- drop table `data_experiments._tmp_extended_user_agent_per_minute_completion_stats`
--create table `data_experiments._tmp_extended_user_agent_per_minute_completion_stats` as 
insert into `query_log_transforms.extended_user_agent_per_minute_completion_stats`
select timestamp_trunc(timestamp,MINUTE) as minute,
       dbid,
       driver_name as user_agent_name,
       driver_version as user_agent_version,
       sum(if(query_submission_type="user-direct",1,0)) as completed_queries ,
       sum(if(query_submission_type="user-direct" and NULL in (failurereason,stacktrace),1,0)) as completed_errors ,
       count(distinct username) as named_users,
       cast(sum(elapsedtimems) as int64) as completed_millis ,
       0 as completed_planning_millis ,
       0 as completed_waiting_millis ,
       cast(sum(allocatedbytes) as int64) as completed_memory_bytes ,
       cast(sum(pagehits) as int64) as completed_page_hits ,
       cast(sum(pagefaults) as int64) as completed_page_faults
  from `query_log_transforms._structured_query_log_extract`
 where date(timestamp) = date_sub(current_date(),INTERVAL 1 DAY)
   and event = "commit"
 group by minute,dbid,user_agent_name,user_agent_version

