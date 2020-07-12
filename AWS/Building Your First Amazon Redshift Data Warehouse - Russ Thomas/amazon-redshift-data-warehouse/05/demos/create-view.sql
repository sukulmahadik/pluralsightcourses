-- create view for simplified order information
create view simpleorders as
select  productkey, 
        discountamount,
        salesamount,
        taxamt,
        orderdate
from factsales;

-- grant select rights on view to finance
grant select on simpleorders to group finance;

-- only columns in view are accessible to finance through the view