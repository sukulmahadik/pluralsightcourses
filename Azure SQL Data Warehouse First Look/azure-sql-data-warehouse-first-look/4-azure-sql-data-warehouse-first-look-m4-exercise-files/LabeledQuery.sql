SELECT 
SUM(Quantity) Quantities,
AVG(ActualCost) ProductCost, dbp.EnglishProductName,
DATEPART(yy,OrderDate) [Year]
FROM prod.FactTransactionHistory fth
INNER JOIN prod.DimBigProduct dbp
on fth.ProductKey=dbp.ProductKey
WHERE fth.ProductKey BETWEEN 2000 AND 40000
GROUP BY fth.ProductKey, dbp.EnglishProductName,DATEPART(yy,OrderDate)
OPTION (LABEL = 'Query: Products and History');


