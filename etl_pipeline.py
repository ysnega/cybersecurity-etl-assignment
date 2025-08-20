#!/usr/bin/env python3
"""
Sales Data ETL Pipeline
-----------------------
This script extracts sales and product data from CSV files, transforms it into a 
star-schema format suitable for analytics, and loads it into a SQLite database.
"""

import pandas as pd
import sqlite3
import os
from datetime import datetime

class SalesETL:
    """ETL pipeline for sales data."""
    
    def __init__(self, db_name='sales_data.db'):
        self.db_name = db_name
        self.conn = None  

    def setup_database(self):
        """
        sets up the SQLite database schema.
        drops existing tables if they exist
        """
        self.conn = sqlite3.connect(self.db_name)
        cursor = self.conn.cursor()
        
        
        tables_to_drop = ['fact_sales', 'dim_product', 'dim_date', 'dim_customer']
        for table in tables_to_drop:
            cursor.execute(f"DROP TABLE IF EXISTS {table}")
        
        #create dimension tables
        cursor.execute('''
            CREATE TABLE dim_product (
                product_id TEXT PRIMARY KEY,
                product_name TEXT,
                category TEXT,
                cost REAL
            )
        ''')
        
        cursor.execute('''
            CREATE TABLE dim_date (
                date_key TEXT PRIMARY KEY,
                full_date DATE,
                year INTEGER,
                month INTEGER,
                day INTEGER,
                month_name TEXT,
                quarter INTEGER
            )
        ''')
        
        cursor.execute('''
            CREATE TABLE dim_customer (
                customer_id TEXT PRIMARY KEY
            )
        ''')
        
        #create fact table for sales
        cursor.execute('''
            CREATE TABLE fact_sales (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                order_id INTEGER,
                product_id TEXT,
                customer_id TEXT,
                date_key TEXT,
                quantity INTEGER,
                price REAL,
                revenue REAL,
                FOREIGN KEY (product_id) REFERENCES dim_product(product_id),
                FOREIGN KEY (customer_id) REFERENCES dim_customer(customer_id),
                FOREIGN KEY (date_key) REFERENCES dim_date(date_key)
            )
        ''')
        
        self.conn.commit()
        print(f"Database setup complete: {self.db_name}")

    def load_csv_files(self, orders_file='data/orders.csv', products_file='data/products.csv'):
        """
        loads orders and products CSV files into pandas DataFrames.
        returns False if any file is missing.
        """
        try:
            self.orders = pd.read_csv(orders_file)
            self.products = pd.read_csv(products_file)
            print(f"Loaded {len(self.orders)} orders and {len(self.products)} products")
            return True
        except FileNotFoundError as e:
            print(f"Missing file: {e}")
            return False

    def process_data(self):
        """
        transforms raw CSV data into a format suitable for the star schema.
        computes revenue, date dimensions, and prepares dimension tables.
        """
        orders = self.orders.copy()
        #compute revenue per order 
        orders['revenue'] = orders['Quantity'] * orders['Price']
        orders['OrderDate'] = pd.to_datetime(orders['OrderDate'])
        orders['date_key'] = orders['OrderDate'].dt.strftime('%Y-%m-%d')
        
        #merge orders with product info
        self.sales_data = pd.merge(orders, self.products, on='ProductID', how='left')
        
        #prepare product dimension
        self.product_dim = self.products.rename(columns={
            'ProductID': 'product_id',
            'ProductName': 'product_name', 
            'Category': 'category',
            'Cost': 'cost'
        })
        
        #prepare customer dimension
        self.customer_dim = pd.DataFrame({
            'customer_id': orders['CustomerID'].unique()
        })
        
        #prepare date dimension
        unique_dates = orders['OrderDate'].dt.date.unique()
        date_rows = []
        for date in unique_dates:
            dt = pd.to_datetime(date)
            quarter = (dt.month - 1) // 3 + 1
            date_rows.append({
                'date_key': date.strftime('%Y-%m-%d'),
                'full_date': date,
                'year': dt.year,
                'month': dt.month,
                'day': dt.day,
                'month_name': dt.strftime('%B'),
                'quarter': quarter
            })
        
        self.date_dim = pd.DataFrame(date_rows)
        print("Data processing is completed")

    def save_to_database(self):
        """
        saves all prepared data into the SQLite database.
        inserts dimension tables first, then the fact table.
        """
        self.product_dim.to_sql('dim_product', self.conn, if_exists='append', index=False)
        self.customer_dim.to_sql('dim_customer', self.conn, if_exists='append', index=False)
        self.date_dim.to_sql('dim_date', self.conn, if_exists='append', index=False)
        
        #prepare fact table columns
        fact_data = self.sales_data.rename(columns={
            'OrderID': 'order_id',
            'ProductID': 'product_id',
            'CustomerID': 'customer_id',
            'Quantity': 'quantity',
            'Price': 'price'
        })[['order_id', 'product_id', 'customer_id', 'date_key', 'quantity', 'price', 'revenue']]
        
        fact_data.to_sql('fact_sales', self.conn, if_exists='append', index=False)
        self.conn.commit()
        
        print
        (f"Saved {len(fact_data)} sales records to database")

    def run_pipeline(self, orders_file='data/orders.csv', products_file='data/products.csv'):
        """
        Executes the complete ETL pipeline:
        1. Set up database
        2. Load CSV files
        3. Process and transform data
        4. Save data to database
        Returns True if successful, False otherwise.
        """        
        try:
            self.setup_database()\
            
            if not self.load_csv_files(orders_file, products_file):
                return False
                
            self.process_data()
            self.save_to_database()
            
            print("ETL completed successfully!")
            return True
            
        except Exception as e:
            print(f"Something went wrong: {e}")
            return False
        finally:
            if self.conn:
                self.conn.close()

    def check_results(self):
        """
        prints row counts and sample sales records.
        helps ensure ETL ran correctly.
        """
        conn = sqlite3.connect(self.db_name)
        
        tables = ['fact_sales', 'dim_product', 'dim_date', 'dim_customer']
        for table in tables:
            cursor = conn.execute(f"SELECT COUNT(*) FROM {table}")
            count = cursor.fetchone()[0]
            print(f"{table}: {count} rows")
        
        print("\nSample sales data:")
        cursor = conn.execute("SELECT * FROM fact_sales LIMIT 3")
        for row in cursor.fetchall():
            print(row)
            
        conn.close()


def create_test_data():
   
    if not os.path.exists('data'):
        os.makedirs('data')
    
    orders = [
        "OrderID,ProductID,CustomerID,OrderDate,Quantity,Price",
        "1001,P001,C101,2024-01-05,2,15.50",
        "1002,P002,C102,2024-01-05,1,25.00", 
        "1003,P001,C103,2024-01-06,3,15.50",
        "1004,P003,C101,2024-01-07,1,50.00",
        "1005,P002,C104,2024-01-08,2,25.00",
        "1006,P004,C105,2024-01-08,1,120.00",
        "1007,P001,C102,2024-01-09,1,15.50"
    ]
    
    with open('data/orders.csv', 'w') as f:
        f.write('\n'.join(orders))
    
    products = [
        "ProductID,ProductName,Category,Cost",
        "P001,Keyboard,Peripherals,10.00",
        "P002,Mouse,Peripherals,18.00", 
        "P003,Monitor,Displays,40.00",
        "P004,Webcam,Peripherals,80.00"
    ]
    
    with open('data/products.csv', 'w') as f:
        f.write('\n'.join(products))
    
    print("Test data created")


if __name__ == "__main__":
    if not os.path.exists('data/orders.csv'):
        create_test_data()
    
    # Instantiate ETL pipeline and run
    etl = SalesETL()
    
    if etl.run_pipeline():
        etl.check_results()
    else:
        print("Pipeline failed")
