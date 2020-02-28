USE [master]
RESTORE DATABASE [AdventureWorksDW] 
FROM  DISK = N'C:\Program Files\Microsoft SQL Server\MSSQL13.SQL2016\MSSQL\Backup\AdventureWorksDW2014.bak' WITH  FILE = 1,  
MOVE N'AdventureWorksDW2014_Data' TO N'C:\Program Files\Microsoft SQL Server\MSSQL13.SQL2016\MSSQL\DATA\AdventureWorksDW_Data.mdf',  
MOVE N'AdventureWorksDW2014_Log' TO N'C:\Program Files\Microsoft SQL Server\MSSQL13.SQL2016\MSSQL\DATA\AdventureWorksDW_Log.ldf',  
NOUNLOAD,  STATS = 5

GO

