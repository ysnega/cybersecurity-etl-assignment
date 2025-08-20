# Cybersecurity-ETL-Assignment

# Instructions 

1. Install dependencies  (pip install pandas tabulate)
2. Run `etl_pipeline.py`  in terminal using "python etl_pipeline.py"
3. Run `query_runner.py` in terminal using "python query_runner.py"
4. View results in your terminal!


# Task 2

Data model description:

fact_sales
- stores sales transaction 
- columns:
    -'OrderID' : unique order identifier 
    -'ProductID': references the products sold 
    -'CustomerID': references customer who made the purchase 
    - 'DateKey': references the date of the sale 
    - 'Quantity': number of items sold 
    - 'Price': price per item 
    - 'Revenue': Total revenue for the order f
    (Quantity x Price)
dimension Tables
dim_product
- stores product details.
- columns:
  - 'ProductID': Unique product identifier.
  -'ProductName': Name of the product.
  -'Category': Product category (e.g., Peripherals, Displays).
  -'Cost': Cost per item.
dim_date
- stores date information for each transaction.
- columns:
  -'DateKey': Unique date identifier (YYYY-MM-DD).
  -'Year': Year of the transaction.
  -'Month': Month number.
  -'MonthName': Name of the month.
  -'Day': Day of the month.
  -'Quarter': Quarter of the year.

dim_customer
- stores customer information.
- columns:
  -'CustomerID': Unique customer identifier.

Relationships
- The fact_sales table links to dim_product, dim_date, and dim_customer using foreign keys.
- This structure makes it easy to analyze sales by product, time, and customer.
            
# Task 3

What is the total revenue for each product category for each month?
```sql
SELECT 
    d.Year,
    d.Month,
    d.MonthName,
    p.Category,
    ROUND(SUM(f.Revenue), 2) AS TotalRevenue,
    COUNT(f.OrderID) AS NumberOfOrders,
    SUM(f.Quantity) AS TotalQuantitySold
FROM FactSales f
JOIN DimProduct p ON f.ProductID = p.ProductID
JOIN DimDate d ON f.DateKey = d.DateKey
GROUP BY d.Year, d.Month, d.MonthName, p.Category
ORDER BY d.Year, d.Month, p.Category;
```

# Task 4

The designed star schema data model consists of:
1.fact_sales: contains all sales transactions, customer, and date dimensions, quantity, price, revenue.
2. dim_product: stores product details such as product name, category, and cost.
3. dim_date: breaks down each date into year, month, day, month name, and quarter, enabling flexible time-based analysis.
4. dim_customer: contains customer identifiers.
# Key metrics 
1. Fast aggregration: allows for rapid aggregration of sales metrics (e.g, total revenue, quantity sold and etc)
2. Flexible slicing and dicing: filter and group data by product category, time period, customer and etc 
3. Simplified queries: analytical queries are straightforward, making it easy to build visualizaations and KPIs
