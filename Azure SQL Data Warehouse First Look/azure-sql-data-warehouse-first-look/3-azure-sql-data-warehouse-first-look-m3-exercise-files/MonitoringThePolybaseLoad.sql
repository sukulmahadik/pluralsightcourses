-- check the progress of the load
SELECT * FROM sys.dm_pdw_exec_requests r 
JOIN sys.dm_pdw_dms_workers w on r.request_id = w.request_id
WHERE r.[label] = 'Load [prod].[FactTransactionHistory]'
order by w.start_time desc;