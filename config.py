
import datetime
#***********************************
##  Health
##*************************************************************************
#-- License Check
#SELECT GET_COMPLIANCE_STATUS();
#-- Events that should be checked
alarms=[]
msg=[]
drill=[]
#mon_time=datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")


mon = {
    "WARNING_EVENTS_COUNT": {
        "query": " select count(*) from active_events where event_severity in ('Error','Warning') "
                 " and event_posted_timestamp >= sysdate() - interval '%(INTERVAL)s minute' ",
        "query1": " select 10 ",

        "error_msg": "There are %(RES)s Error and Warning events in Vertica. "
                     "Hint: query table active_events where event_severity in ('Error','Warning') ...",
        "threshold": 1 ,
       # "thresholdc": "1",
        "component": "alarm",
        "severity": "Warning",
        "interval": "30"
    },
    "CRITICAL_EVENTS_COUNT": {
        "query": "select count(*) from active_events where event_severity in ('Emergency','Alert','Critical')"
                 " and event_posted_timestamp >= sysdate() - interval '%(INTERVAL)s minute' ",
        "error_msg": "There are %(RES)s Critical events in Vertica. "
                     "Hint: query table active_events where event_severity in ('Error','Warning') ...",
        "threshold": 1,
        "component": "alarm",
        "severity": "Critical",
        "interval": "30"
    },
    "NODES_DOWN": {
        "query": "select count(*) from v_catalog.nodes where node_state <> 'UP'  ",
        "error_msg": "There are %(RES)s Nodes Down in Vertica ",
        "threshold_c": 1,
        "component": "alarm",
        "severity": "Critical",
        "interval": "5"
    },
#ToDo NODES_LESS_40_PERCENT active_events
    "NODES_LESS_40_PERCENT": {
        "query": "SELECT count(*) FROM   v_monitor.disk_storage s "
                 " WHERE  (disk_space_free_mb*100) / ( disk_space_used_mb + disk_space_free_mb ) < %(MAX)s "
                 " AND storage_usage = 'DATA,TEMP' ",
        "error_msg": "There are %(RES)s Disks with less %(MAX)s percent free in Vertica",
        "threshold": 1,
        "max": 40,
        "threshold_c": 1,
        "max_c": 20,
        "component": "alarm",
        "severity": "Warning",
        "interval": "12 hours"
    },
    "CPE_LGE": {
        "query": " SELECT  get_current_epoch() - get_last_good_epoch() as ce_lge  ",
        "error_msg": "The difference between current and last good epoch is %(RES)s in Vertica",
        "threshold": 500,
        "threshold_c": 1000,
        "rerun": False,
        "component": "alarm",
        "severity": "Warning",
        "interval": "60"
    },
    "CPE_AHM": {
        "query": " SELECT  get_current_epoch() - get_last_good_epoch() as ce_lge  ",
        "error_msg": "The difference between current epoch and Ancient History Mark is %(RES)s in Vertica",
        "threshold": 500,
        "threshold_c": 1000,
        "rerun": False,
        "component": "alarm",
        "severity": "Warning",
        "interval": "12 hours"
    },
    "MAX_SESSIONS_PERCENT": {
        "query": " WITH maxs as "
                 "(SELECT  CURRENT_VALUE as val FROM CONFIGURATION_PARAMETERS WHERE parameter_name='MaxClientSessions')"
                 " select max(sessPercent)::int from "
                 "(select (count(*)*100)/val as sessPercent from sessions, maxs group by node_name,val) sess ",
        "error_msg": "There number of sessions in one node is %(RES)s percent of MaxClientSessions in Vertica",
        "threshold": 80,
        "threshold_c": 90,
        "rerun": False,
        "component": "alarm",
        "severity": "Warning",

        "interval": "30"
    },
    "REJECTED_RESOURCES_COUNT": {
        "query": """
                SELECT COUNT(*)
                      FROM   v_monitor.resource_rejection_details
                      where rejected_timestamp >= sysdate() - interval '%(INTERVAL)s '
                """,
        "error_msg": "There were %(RES)s rejected resources last 30 minutes in Vertica",
        "threshold": 5,
        "threshold_c": 10,
        "rerun": False,
        "component": "alarm",
        "severity": "Warning",
        "interval": "30 minute"
    },
    "PROJECTION_ROS_PERCENT": {
        "query_drill": """ select projection_schema,  projection_name , anchor_table_name , ros_count
                      , (ros_count*100 / CURRENT_VALUE)::int
                      FROM projection_storage , CONFIGURATION_PARAMETERS
                      where projection_schema = 'public' and parameter_name = 'MaxPartitionCount'
                      order by ros_count desc limit 1
                 """,

        "query": """ select (max(ros_count)*100 / max(CURRENT_VALUE) )::int
                      FROM projection_storage , CONFIGURATION_PARAMETERS
                      where projection_schema = 'public' and parameter_name = 'MaxPartitionCount'
                  """,
        "error_msg": "The number of ROS is %(RES)s of MaxPartitionCount in Vertica",
        "threshold": 85,
        "threshold_c": 95,
        "rerun": False,
        "component": "alarm",
        "severity": "Warning",
        "interval": "12 hours"
    },
    "SCHEMA_COUNT": {
        "query_drill": """ select projection_schema,  projection_name , anchor_table_name , ros_count
                      , (ros_count*100 / CURRENT_VALUE)::int
                      FROM projection_storage , CONFIGURATION_PARAMETERS
                      where projection_schema = 'public' and parameter_name = 'MaxPartitionCount'
                      order by ros_count desc limit 1
                 """,

        "query": """ select count(*) from v_catalog.schemata """,
        "error_msg": "The number of schemas is %(RES)s  in Vertica. Recommended less than %(MAX)s.",
        "threshold": 200,
        "threshold_c": 400,
        "max": 200,
        "max_c": 200,
        "rerun": False,
        "component": "alarm",
        "severity": "Warning",
        "interval": "12 hours"
    },
    "UNREFRESHED_PROJECTIONS": {
        "query_drill": """ select projection_schema,  projection_name , anchor_table_name
                            from projections
                            where not is_up_to_date
                 """,

        "query": """ select count(*)
                            from projections
                            where not is_up_to_date """,
        "error_msg": "The number of unrefreshed projections is %(RES)s  in Vertica.",
        "threshold": 1,
        "threshold_c": 2,
        "rerun": False,
        "component": "alarm",
        "severity": "Warning",
        "interval": "day"
    },
    "DELETED_ROWS": {
        "query_drill": """
             select dv.schema_name,ps.anchor_table_name,dv.projection_name,deleted_row_count*100/row_count as PctDeleted
              from   delete_vectors dv
                join projection_storage ps
                on ps.projection_schema = dv.schema_name and ps.projection_name = dv.projection_name
              order by projection_name
                """,

        "query": """ select (deleted_row_count*100/row_count)::int as PctDeleted
                        from delete_vectors dv
                           join projection_storage ps
                           on ps.projection_schema = dv.schema_name and ps.projection_name = dv.projection_name
                        order by deleted_row_count*100/row_count desc limit 1;
                """,
        "error_msg": "The max percent of deleted rows is %(RES)s  in Vertica.",
        "threshold": 15,
        "threshold_c": 25,
        "rerun": False,
        "component": "alarm",
        "severity": "Warning",
        "interval": "12 hours"
    }

}
#ToDo not refreshed projection count
#ToDo schema count threshold = 200
#ToDo delete vectors = 200

perf = {
    "CPU_USAGE": {
        "query": "select 1 as MeasureType, round(avg(average_cpu_usage_percent),1) as average_cpu_usage_percent"
                 ", start_time as CreateTime "
                 "from v_monitor.cpu_usage "
                 "where start_time > sysdate() - interval '%(INTERVAL)s minute' "
                 "group by start_time order by start_time ",
        "sqltable": "dbo.VerticaMonitor",
        "connection": "mssql",
        "sqlinsert": "insert into leo.mon_cpu_usage values(?,?) "
                     " where start_time > (select max(start_time) from leo.mon_cpu_usage) ",
        "error_msg": "CPU usage too high %s",
        "threshold": "95",
        "component": "perf",
        "severity": "ERROR",
        "interval": "30"
    },
    "QUERY_PERFORMANCE": {
        "query": """SELECT /*+label(monitor_query_details)*/  DISTINCT r.node_name,
                          q.query_start::timestamp ,
                          q.transaction_id,
                          q.statement_id,
                          q.identifier,
                          q.table_name,
                          (q.query_duration_us / 1000/1000 )::numeric(10,2) duration_sec,
                          q.processed_row_count,
                          q.query_type,
                          q.user_name,
                          r.pool_name,
                          r.duration_ms,
                          q.is_executing,
                          (r.memory_inuse_kb/1024 - q.reserved_extra_memory / 1024^2)::int real_memory_mb,
                          r.thread_count as threads,
                          r.open_file_handle_count   filehandles,
                          datediff('ms',r.queue_entry_timestamp ,r.acquisition_timestamp  ) AS res_wait_ms ,
                          q.query
                FROM      query_profiles q
                LEFT OUTER JOIN resource_acquisitions r
                ON        r.transaction_id = q.transaction_id
                AND       q.statement_id = r.statement_id
                WHERE     q.query_start::timestamp > '%s'
                   AND pool_name <> 'sysquery'
                   AND q.query_duration_us / 1000/1000 > 3
                   AND r.REQUEST_TYPE = 'Reserve' -- related to queries
                   AND q.identifier not ilike 'monitor%%'
                   LIMIT 1000
                    """,
        "sqltable": "leo.query_performance",
        "connection": "vertica",
        "sqlinsert": "",
        "error_msg": "CPU usage too high %s",
        "threshold": "95",
        "component": "perf",
        "severity": "ERROR",
        "interval": "select ifnull(max(query_start),sysdate() - interval '30 minute')::varchar from %s "
    },
    "QUERY_EVENTS": {
        "query": """SELECT /*+label(monitor_query_events)*/
                       DISTINCT qr.query_start,
                                qr.transaction_id,
                                qr.statement_id,
                                qr.identifier,
                                qe.EVENT_CATEGORY,
                                qe.EVENT_DESCRIPTION,
                                qe.event_type,
                                LEFT(qr.query,200) AS request
                                ,event_details,suggested_action
                       FROM   v_monitor.query_events qe
                       JOIN leo.query_performance qr
                         ON qr.transaction_id = qe.transaction_id
                        AND qr.statement_id = qe.statement_id
                WHERE  qe.event_type IN ( 'GROUP_BY_SPILLED', 'JOIN_SPILLED' ,'RESEGMENTED_MANY_ROWS','NO HISTOGRAM','MEMORY LIMIT HIT')
                       AND qr.query_start::timestamp > '%s'
                ORDER  BY qr.query_start desc
                    """,
        "sqltable": "leo.query_events",
        "connection": "vertica",
        "sqlinsert": "insert into leo.mon_cpu_usage values(?,?) "
                     " where start_time > (select max(start_time) from leo.mon_cpu_usage) ",
        "error_msg": "CPU usage too high %s",
        "threshold": "95",
        "component": "perf",
        "severity": "ERROR",
        "interval": "select ifnull(max(query_start),sysdate() - interval '30 minute')::varchar from %s "
    },
    "RESOURCE_QUEUES": {
        "query": """SELECT DISTINCT node_name AS 'Node Name', transaction_id AS 'Transaction ID', statement_id AS 'Statement ID'
                         , pool_name AS 'Pool Name'
                         , memory_requested_kb AS 'Memory Requested (KB)'
                         , priority AS 'Priority'
                         , position_in_queue AS 'Position in Queue'
                         , queue_entry_timestamp AS 'Queue Entry Timestamp'
                      FROM v_monitor.resource_queues
                    """,
        "sqltable": "leo.resource_queues",
        "connection": "vertica",
        "sqlinsert": "insert into leo.mon_cpu_usage values(?,?) "
                     " where start_time > (select max(start_time) from leo.mon_cpu_usage) ",
        "error_msg": "CPU usage too high %s",
        "threshold": "95",
        "component": "perf",
        "severity": "ERROR",
        "interval": "30"
    },
    "REJECTED_RESOURCES_DETAILS": {
        "query": """SELECT r.rejected_timestamp, r.node_name, r.user_name, r.pool_name, r.reason, r.resource_type, r.rejected_value
                           ,r.transaction_id , r.statement_id
                    FROM   v_monitor.resource_rejection_details r
                    WHERE rejected_timestamp >= sysdate() - interval '1 hour' limit 30;
                    """,
        "sqltable": "leo.rejected_resources",
        "connection": "vertica" ,
        "sqlinsert": " ",
        "error_msg": "CPU usage too high %s" ,
        "threshold": "95",
        "component": "perf",
        "severity": "ERROR",
        "interval": "30"
    },
        "TEST_QUERY_PERFORMANCE": {
        "query": """insert into leo.test123 values(1 , 'o')
                    """,
        "sqltable": "leo.query_performance",
        "connection": "vertica",
        "sqlinsert": "insert into leo.mon_cpu_usage values(?,?) "
                     " where start_time > (select max(start_time) from leo.mon_cpu_usage) ",
        "error_msg": "CPU usage too high %s",
        "threshold": "95",
        "component": "perf",
        "severity": "ERROR",
        "interval": "30"
    }

}
