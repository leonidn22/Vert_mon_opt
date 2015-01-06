
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
        "error_msg": "There are %s Warning events in Vertica",
        "threshold": "1",
        "component": "alarm",
        "severity": "Warning",
        "interval": "60"
    },
    "CRITICAL_EVENTS_COUNT": {
        "query": "select count(*) from active_events where event_severity in ('Emergency','Alert','Critical')"
                 " and event_posted_timestamp >= sysdate() - interval '%(INTERVAL)s minute' ",
        "error_msg": "There are %s Critical events in Vertica",
        "threshold": "1",
        "component": "alarm",
        "severity": "Critical",
        "interval": "60"
    },
    "NODES_DOWN": {
        "query": "select node_name , node_state from v_catalog.nodes where node_state <> 'UP'  ",
        "error_msg": "There are %s Nodes Down in Vertica",
        "threshold": "1",
        "component": "alarm",
        "severity": "Critical",
        "interval": ""
    },
    "NODES_LESS_40_PERCENT": {
        "query": "SELECT count(*) FROM   v_monitor.disk_storage s join v_catalog.nodes n using (NODE_NAME)  "
                 " WHERE  (disk_space_free_mb*100) / ( disk_space_used_mb + disk_space_free_mb ) < 40 "
                 " AND storage_usage = 'DATA,TEMP' ",
        "error_msg": "There are %s Disks with less 40% free in Vertica",
        "threshold": "1",
        "component": "alarm",
        "severity": "Warning",
        "interval": ""
    },
    "CPE_LGE": {
        "query": " SELECT  get_current_epoch() - get_last_good_epoch() as ce_lge  ",
        "error_msg": "There is %s difference between current and last good epoch in Vertica",
        "threshold": "1000",
        "component": "alarm",
        "severity": "Warning",
        "interval": ""
    },
    "CPE_AHM": {
        "query": " SELECT  get_current_epoch() - get_last_good_epoch() as ce_lge  ",
        "error_msg": "There is %s difference between current epoch and Ancient History Mark  in Vertica",
        "threshold": "1000",
        "component": "alarm",
        "severity": "Warning",
        "interval": ""
    },
    "CPE_AHM": {
        "query": " SELECT COUNT(*) FROM   v_monitor.resource_rejection_details  "
                 " where rejected_timestamp >= sysdate() - interval '%(INTERVAL)s minute' ",
        "error_msg": "There were %s rejected resource last hour in Vertica",
        "threshold": "1",
        "component": "alarm",
        "severity": "Warning",
        "interval": "60"
    },
    "MAX_SESSIONS_PERCENT": {
        "query": " WITH maxs as "
                 "(SELECT  CURRENT_VALUE as val FROM CONFIGURATION_PARAMETERS WHERE parameter_name='MaxClientSessions')"
                 " select max(sessPercent)::int from "
                 "(select (count(*)*100)/val as sessPercent, node_name from sessions,maxs group by node_name,val) sess",
        "error_msg": "There is %s percent of number of sessions in one node in Vertica",
        "threshold": "80",
        "component": "alarm",
        "severity": "Warning",
        "interval": "30"
    },
    "REJECTED_RESOURCES_COUNT": {
        "query": " SELECT COUNT(*) "
                 " FROM   v_monitor.resource_rejection_details "
                 " where rejected_timestamp >= sysdate() - interval '1 hour' ",
        "error_msg": "There were %s rejected resources last Hour in Vertica",
        "threshold": "80",
        "component": "alarm",
        "severity": "Warning",
        "interval": "30"
    }

}

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
