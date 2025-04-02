SELECT 
       p.StockCode,
       p.Description,
       AVG(p.UnitPrice) AS AveragePrice,
       SUM(o.Quantity) AS TotalSalesVolume
  FROM orders o JOIN products p ON o.StockCode = p.StockCode
 GROUP BY p.StockCode, p.Description, p.UnitPrice
 ORDER BY TotalSalesVolume DESC
 LIMIT 10;