#!/usr/bin/env python3
"""
Query Runner for Sales Data Warehouse

"""

import sqlite3
import pandas as pd
from tabulate import tabulate
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class QueryRunner:
    def __init__(self, db_path='sales_datawarehouse.db'):
        self.db_path = db_path
    
    def run_query(self, query, description="Query Result"):
        """run a SQL query and return formatted results"""
        try:
            conn = sqlite3.connect(self.db_path)
            df = pd.read_sql_query(query, conn)
            conn.close()
            
            print(f"\n{'='*80}")
            print(f"{description}")
            print('='*80)
            
            if df.empty:
                print("No results found.")
            else:
                print(tabulate(df, headers='keys', tablefmt='grid', showindex=False))
                print(f"\nTotal rows: {len(df)}")
            
            return df
            
        except Exception as e:
            logger.error(f"Query execution failed: {e}")
            print(f"Error executing query: {e}")
            return None
    
    def run_main_business_query(self):
        """run the main business question query"""
        query = """
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
        """
        
        return self.run_query(
            query, 
            "MAIN BUSINESS QUESTION: Total revenue for each product category for each month"
        )
    
    def run_monthly_summary(self):
        """Monthly sales summary"""
        query = """
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
        """
        
        return self.run_query(query, "MONTHLY SALES SUMMARY")
    
    def run_product_performance(self):
        """Product performance analysis"""
        query = """
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
        """
        
        return self.run_query(query, "PRODUCT PERFORMANCE ANALYSIS")
    
    def run_customer_analysis(self):
        """Customer analysis"""
        query = """
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
        """
        
        return self.run_query(query, "CUSTOMER ANALYSIS")
    
    def run_category_comparison(self):
        """category performance comparison"""
        query = """
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
        """
        
        return self.run_query(query, "CATEGORY PERFORMANCE COMPARISON")
    
    def run_data_quality_check(self):
        """data quality verification"""
        query = """
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
        """
        
        return self.run_query(query, "DATA QUALITY CHECK")
    
    def run_all_queries(self):
        """run all analytical queries"""
        print("Database:", self.db_path)
        
    
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.execute("SELECT name FROM sqlite_master WHERE type='table';")
            tables = [row[0] for row in cursor.fetchall()]
            conn.close()
            
            if not tables:
                print("Error: Database is empty or doesn't exist.")
                return
                
            print(f"Found tables: {', '.join(tables)}")
            
        except Exception as e:
            print(f"Error connecting to database: {e}")
            return
        
    
        self.run_main_business_query()
        self.run_monthly_summary()
        self.run_product_performance()
        self.run_customer_analysis()
        self.run_category_comparison()
        self.run_data_quality_check()
        
        print(f"\n{'='*80}")
        print("All queries completed!")
        print('='*80)

def main():
    runner = QueryRunner()
    runner.run_all_queries()

if __name__ == "__main__":
    main()
