SELECT 
SUM(fth.Quantity) Quantities,Max(fth.Quantity) BiggestOrder, Min(fth.Quantity) SmallestOrder,
AVG(fth.Quantity) AverageQuantity, AVG(fth.ActualCost) AverageCost,Max(fth.ActualCost) BiggestCost, 
Min(fth.ActualCost) SmallestCost, SUM(fth.ActualCost) TotalCost, fth.ProductKey ProductKey,
DATEPART(yy,fth.OrderDate) [Year]
FROM prod.FactTransactionHistory fth 
INNER JOIN prod.FactTransactionHistory fth2 
on fth.ProductKey=fth2.ProductKey AND DATEPART(yy,fth.OrderDate)=DATEPART(yy,fth2.OrderDate)+1
INNER JOIN prod.DimBigProduct dbp on fth.ProductKey=dbp.ProductKey
GROUP BY fth.ProductKey,DATEPART(yy,fth.OrderDate);


