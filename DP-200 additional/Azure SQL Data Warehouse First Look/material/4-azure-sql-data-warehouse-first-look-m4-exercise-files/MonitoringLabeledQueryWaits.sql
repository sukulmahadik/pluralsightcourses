-- monitor wait types per query by label
SELECT  [type]
,       [object_type]
,       [object_name]
,       r.[request_id]
,       [request_time]
,       [acquire_time]
,       DATEDIFF(ms,[request_time],[acquire_time])  AS acquire_duration_ms
,       [concurrency_slots_used]                    AS concurrency_slots_reserved
,       r.[resource_class]
,       [wait_id]                                   AS queue_position
FROM    sys.dm_pdw_resource_waits w
JOIN    sys.dm_pdw_exec_requests r  ON w.[request_id] = r.[request_id]
WHERE   r.[label] = 'Products History';