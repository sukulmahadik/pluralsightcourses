USE [HappyScoopers_DW]
GO

--Execute the stored procedure that loads the fact table
EXEC [dbo].[Load_FactSales]
GO
-- Take a look at the data stored in Fact_Sales
SELECT *
  FROM [HappyScoopers_DW].[dbo].[Fact_Sales]