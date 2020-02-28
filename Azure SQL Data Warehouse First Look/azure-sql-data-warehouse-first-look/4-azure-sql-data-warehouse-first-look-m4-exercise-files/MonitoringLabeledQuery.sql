-- monitor at the request level
SELECT *
FROM sys.dm_pdw_exec_requests r 
WHERE r.[label] = 'Query: Products and History'
order by r.start_time desc;

-- monitor at the worker level
SELECT w.*
FROM sys.dm_pdw_exec_requests r 
JOIN sys.dm_pdw_dms_workers w on r.request_id = w.request_id
WHERE r.[label] = 'Query: Products and History'
order by w.start_time asc, step_index asc;

-- monitor at the distribution level
SELECT req.* 
FROM sys.dm_pdw_exec_requests r 
JOIN sys.dm_pdw_sql_requests req on r.request_id = req.request_id
WHERE r.[label] = 'Query: Products and History'
order by req.step_index,req.distribution_id



