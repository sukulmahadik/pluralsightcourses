CREATE SCHEMA [prod];
GO

CREATE TABLE [prod].[DimBigProduct](
	[ProductKey] [int] NOT NULL,
	[EnglishProductName] [varchar](80) NULL,
	[ProductAlternateKey] [nvarchar](56) NULL,
	[FinishedGoodsFlag] [bit] NOT NULL,
	[Color] [nvarchar](15) NOT NULL,
	[SafetyStockLevel] [smallint] NULL,
	[ReorderPoint] [smallint] NULL,
	[StandardCost] [money] NULL,
	[ListPrice] [money] NULL,
	[Size] [nvarchar](50) NULL,
	[SizeUnitMeasureCode] [nchar](3) NULL,
	[WeightUnitMeasureCode] [nchar](3) NULL,
	[Weight] [float] NULL,
	[DaysToManufacture] [int] NULL,
	[ProductLine] [nchar](2) NULL,
	[Class] [nchar](2) NULL,
	[Style] [nchar](2) NULL,
	[ProductSubcategoryKey] [int] NULL,
	[ModelName] [nvarchar](50) NULL,
	[StartDate] [datetime] NULL,
	[EndDate] [datetime] NULL)
WITH (CLUSTERED INDEX (ProductKey)); 

-- Load with SSIS package then test table contents
SELECT * FROM [prod].[DimBigProduct];

GO


