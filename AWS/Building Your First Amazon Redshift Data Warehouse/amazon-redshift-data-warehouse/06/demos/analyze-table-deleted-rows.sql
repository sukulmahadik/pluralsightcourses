select
   (select cast(count(*) as numeric(12,0)) from factsales) /
    cast(tbl_rows as numeric(12,0))
    as "percentage of soft deletes"
from svv_table_info where "table" = 'factsales'