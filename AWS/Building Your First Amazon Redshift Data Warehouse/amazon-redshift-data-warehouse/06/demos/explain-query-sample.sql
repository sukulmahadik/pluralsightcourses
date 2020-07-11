
explain
with aggquery as (
select dp.productname,
       avg(fs.unitprice) as avgunitprice,
       sum(fs.salesamount) as totalsalesamt,
       dd.monthofyear,
       month_name(dd.monthofyear) as monthname
from dimproduct dp
     inner join factsales fs on dp.productkey = fs.productkey
     inner join dimdate dd on dd.datekey = fs.orderdatekey
group by productname, monthofyear)
select productname,
       avgunitprice,
       totalsalesamt,
       monthofyear,
       monthname,
       ntile(5) over (order by avgunitprice desc) as pricerank
from aggquery
