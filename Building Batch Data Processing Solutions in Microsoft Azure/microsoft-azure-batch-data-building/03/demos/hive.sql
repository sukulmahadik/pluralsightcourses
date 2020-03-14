CREATE TABLE [dbo].[delays](
[origin_city_name] [nvarchar](50) NOT NULL,
[weather_delay] float,
CONSTRAINT [PK_delays] PRIMARY KEY CLUSTERED
([origin_city_name] ASC))
GO

SELECT * FROM information_schema.tables
GO


