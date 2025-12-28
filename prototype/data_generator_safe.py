# data_generator_safe.py - Alternative safer version
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random
import sqlite3
from contextlib import contextmanager

@contextmanager
def foreign_keys_disabled(conn):
    """Context manager to temporarily disable foreign key constraints"""
    cursor = conn.cursor()
    try:
        cursor.execute("PRAGMA foreign_keys = OFF")
        yield
    finally:
        cursor.execute("PRAGMA foreign_keys = ON")
        conn.commit()

def generate_safe_test_data(conn):
    """Generate test data safely without foreign key violations"""
    cursor = conn.cursor()
    
    # Start a transaction
    cursor.execute("BEGIN TRANSACTION")
    
    try:
        # Clear data in correct order
        cursor.execute("DELETE FROM sales")
        cursor.execute("DELETE FROM transfer_recommendations")
        cursor.execute("DELETE FROM stock_levels")
        cursor.execute("DELETE FROM products")
        
        # Generate products
        products = []
        styles = ['RUN001', 'RUN002', 'CAS001', 'FOR001']
        
        for style in styles:
            for size in [7, 8, 9, 10, 11]:
                sku = f"{style}-{size}"
                products.append((
                    sku, style, f"{style} Shoes", 
                    "Footwear", size, "U",
                    50.0, 100.0
                ))
        
        # Insert products
        cursor.executemany(
            "INSERT INTO products (sku, style_code, style_name, category, size, gender, cost_price, retail_price) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            products
        )
        
        # Get stores
        cursor.execute("SELECT store_id FROM stores")
        stores = [row[0] for row in cursor.fetchall()]
        
        # Generate stock levels
        stock_data = []
        for store_id in stores:
            # Pick 70% of products for each store
            store_products = random.sample(products, k=int(len(products) * 0.7))
            
            for product in store_products:
                sku = product[0]
                quantity = random.randint(1, 15) if store_id != 'ONLINE' else random.randint(10, 25)
                stock_data.append((store_id, sku, quantity))
        
        cursor.executemany(
            "INSERT INTO stock_levels (store_id, sku, quantity) VALUES (?, ?, ?)",
            stock_data
        )
        
        # Generate sales (last 30 days)
        sales_data = []
        for i in range(30):
            date = (datetime.now() - timedelta(days=i)).strftime('%Y-%m-%d')
            
            for store_id in stores:
                # 1-5 sales per store per day
                for _ in range(random.randint(1, 5)):
                    # Pick a random product that exists in stock_levels
                    cursor.execute(
                        "SELECT sku FROM stock_levels WHERE store_id = ? ORDER BY RANDOM() LIMIT 1",
                        (store_id,)
                    )
                    result = cursor.fetchone()
                    if result:
                        sku = result[0]
                        quantity = random.randint(1, 2)
                        revenue = quantity * 100.0  # Simplified
                        sales_data.append((store_id, sku, date, quantity, revenue))
        
        cursor.executemany(
            "INSERT INTO sales (store_id, sku, sale_date, quantity, revenue) VALUES (?, ?, ?, ?, ?)",
            sales_data
        )
        
        # Commit the transaction
        conn.commit()
        return True
        
    except Exception as e:
        # Rollback on error
        conn.rollback()
        raise e