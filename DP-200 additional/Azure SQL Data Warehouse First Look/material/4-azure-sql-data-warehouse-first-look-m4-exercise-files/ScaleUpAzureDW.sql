-- first, let's verify what we have
SELECT
 db.name [Database],
 ds.edition [Edition],
 ds.service_objective [Service Objective]
FROM
 sys.database_service_objectives ds
 JOIN sys.databases db ON ds.database_id = db.database_id

-- second, let's modify the performance level
ALTER DATABASE warnerwarehouse
MODIFY (SERVICE_OBJECTIVE = 'DW100');

-- finally, if we don't need to have the warehouse running, let's pause it on the portal.
