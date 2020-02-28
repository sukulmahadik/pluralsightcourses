-- 1. We first need to create a master key if we haven't done that already

CREATE MASTER KEY;
GO

-- 2. Second, we need a credential to the azure storage. 
-- The user value doesn't really matter, the secret does and we can get it from the portal.


CREATE DATABASE SCOPED CREDENTIAL WarehouseStorageCredential
WITH
    IDENTITY = 'user',  
	SECRET = 'xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx'   
;
GO

-- 3. We create an external data source that references the Azure storage account.
-- We use the credential we just created above.
-- Note the format using both the blob container name as well as the account endpoint.

CREATE EXTERNAL DATA SOURCE AzureWarehouseStorage
WITH (
    TYPE = HADOOP,
    LOCATION = 'wasbs://datawarehouse@dataimports.blob.core.windows.net',
    CREDENTIAL = WarehouseStorageCredential
);
GO

-- 4. We create an external file format to Azure SQL DW knows how to read the file

CREATE EXTERNAL FILE FORMAT CSVFileFormat 
WITH 
(   FORMAT_TYPE = DELIMITEDTEXT
,   FORMAT_OPTIONS  (   FIELD_TERMINATOR = ','
                    ,   STRING_DELIMITER = ''
                    ,   DATE_FORMAT      = 'yyyy-MM-dd HH:mm:ss'
                    ,   USE_TYPE_DEFAULT = FALSE 
                    )
);
GO

-- 5. We create an external table that maps back to the FactTransactionHistory table:
CREATE SCHEMA [stage];
GO

CREATE EXTERNAL TABLE [stage].FactTransactionHistory 
(
    [TransactionID] [int] NOT NULL,
	[ProductKey] [int] NOT NULL,
	[OrderDate] [datetime] NULL,
	[Quantity] [int] NULL,
	[ActualCost] [money] NULL
)
WITH
(
    LOCATION='/FactTransactionHistory/' 
,   DATA_SOURCE = AzureWarehouseStorage
,   FILE_FORMAT = CSVFileFormat
,   REJECT_TYPE = VALUE
,   REJECT_VALUE = 0
)
GO

-- 6. We use CTAS to bring the data from the file and through the external table definition
CREATE SCHEMA [prod];
GO

CREATE TABLE [prod].[FactTransactionHistory]       
WITH (DISTRIBUTION = HASH([ProductKey]  ) ) 
AS 
SELECT * FROM [stage].[FactTransactionHistory]        
OPTION (LABEL = 'Load [prod].[FactTransactionHistory]');

-- verify the data was loaded into the 60 distributions
-- Find data skew for a distributed table
DBCC PDW_SHOWSPACEUSED('prod.FactTransactionHistory');