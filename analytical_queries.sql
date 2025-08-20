-- Analytical Queries for Sales Data Warehouse
-- Data Warehouse Intern Assessment - CSG-CDOI-TD

-- Query 1: Total revenue for each product category for each month (Main Business Question)
SELECT 
    d.Year,
    d.Month,
    d.MonthName,
    p.Category,
    ROUND(SUM(f.Revenue), 2) as TotalRevenue,
    COUNT(f.OrderID) as NumberOfOrders,
    SUM(f.Quantity) as TotalQuantitySold
FROM FactSales f
JOIN DimProduct p ON f.ProductID = p.ProductID
JOIN DimDate d ON f.DateKey = d.DateKey
GROUP BY d.Year, d.Month, d.MonthName, p.Category
ORDER BY d.Year, d.Month, p.Category;

-- Query 2: Monthly sales summary across all categories
SELECT 
    d.Year,
    d.Month,
    d.MonthName,
    COUNT(DISTINCT f.ProductID) as UniqueProducts,
    COUNT(f.OrderID) as TotalOrders,
    SUM(f.Quantity) as TotalQuantity,
    ROUND(SUM(f.Revenue), 2) as TotalRevenue,
    ROUND(AVG(f.Revenue), 2) as AverageOrderValue
FROM FactSales f
JOIN DimDate d ON f.DateKey = d.DateKey
GROUP BY d.Year, d.Month, d.MonthName
ORDER BY d.Year, d.Month;

-- Query 3: Product performance analysis
SELECT 
    p.ProductName,
    p.Category,
    COUNT(f.OrderID) as TimesOrdered,
    SUM(f.Quantity) as TotalQuantitySold,
    ROUND(SUM(f.Revenue), 2) as TotalRevenue,
    ROUND(AVG(f.Price), 2) as AverageSellingPrice,
    ROUND(SUM(f.Revenue) - SUM(p.Cost * f.Quantity), 2) as TotalProfit,
    ROUND((SUM(f.Revenue) - SUM(p.Cost * f.Quantity)) / SUM(f.Revenue) * 100, 2) as ProfitMarginPercent
FROM FactSales f
JOIN DimProduct p ON f.ProductID = p.ProductID
GROUP BY p.ProductID, p.ProductName, p.Category
ORDER BY TotalRevenue DESC;

-- Query 4: Customer analysis
SELECT 
    f.CustomerID,
    COUNT(f.OrderID) as NumberOfOrders,
    SUM(f.Quantity) as TotalItemsPurchased,
    ROUND(SUM(f.Revenue), 2) as TotalSpent,
    ROUND(AVG(f.Revenue), 2) as AverageOrderValue,
    COUNT(DISTINCT f.ProductID) as UniqueProductsPurchased
FROM FactSales f
GROUP BY f.CustomerID
ORDER BY TotalSpent DESC;

-- Query 5: Daily sales trend
SELECT 
    d.DateKey,
    d.FullDate,
    COUNT(f.OrderID) as DailyOrders,
    SUM(f.Quantity) as DailyQuantity,
    ROUND(SUM(f.Revenue), 2) as DailyRevenue
FROM FactSales f
JOIN DimDate d ON f.DateKey = d.DateKey
GROUP BY d.DateKey, d.FullDate
ORDER BY d.FullDate;

-- Query 6: Category performance comparison
SELECT 
    p.Category,
    COUNT(f.OrderID) as TotalOrders,
    COUNT(DISTINCT f.CustomerID) as UniqueCustomers,
    SUM(f.Quantity) as TotalQuantitySold,
    ROUND(SUM(f.Revenue), 2) as TotalRevenue,
    ROUND(AVG(f.Revenue), 2) as AverageOrderValue,
    ROUND(SUM(f.Revenue) / COUNT(DISTINCT f.CustomerID), 2) as RevenuePerCustomer
FROM FactSales f
JOIN DimProduct p ON f.ProductID = p.ProductID
GROUP BY p.Category
ORDER BY TotalRevenue DESC;

-- Query 7: Data quality check - Verify no missing relationships
SELECT 'Missing Product References' as CheckType, COUNT(*) as Count
FROM FactSales f
LEFT JOIN DimProduct p ON f.ProductID = p.ProductID
WHERE p.ProductID IS NULL

UNION ALL

SELECT 'Missing Date References' as CheckType, COUNT(*) as Count
FROM FactSales f
LEFT JOIN DimDate d ON f.DateKey = d.DateKey
WHERE d.DateKey IS NULL

UNION ALL

SELECT 'Missing Customer References' as CheckType, COUNT(*) as Count
FROM FactSales f
LEFT JOIN DimCustomer c ON f.CustomerID = c.CustomerID
WHERE c.CustomerID IS NULL;

-- Query 8: Revenue breakdown by time periods
SELECT 
    'Total' as Period,
    ROUND(SUM(Revenue), 2) as Revenue
FROM FactSales

UNION ALL

SELECT 
    'Q' || d.Quarter as Period,
    ROUND(SUM(f.Revenue), 2) as Revenue
FROM FactSales f
JOIN DimDate d ON f.DateKey = d.DateKey
GROUP BY d.Quarter

ORDER BY Period;
