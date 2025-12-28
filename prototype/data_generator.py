# data_generator.py
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random
import sqlite3

def generate_test_data():
    """Generate basic test data for demonstration"""
    
    # Generate simple products
    styles = ['RUN001', 'RUN002', 'CAS001', 'FOR001', 'BOO001', 'SAN001']
    sizes = [7, 7.5, 8, 8.5, 9, 9.5, 10, 10.5, 11]
    categories = ['Running', 'Running', 'Casual', 'Formal', 'Boots', 'Sandals']
    
    products = []
    for i, style in enumerate(styles):
        for size in random.sample(sizes, k=random.randint(3, 6)):
            sku = f"{style}-{size}"
            products.append({
                'sku': sku,
                'style_code': style,
                'style_name': f"{categories[i]} Shoes {style}",
                'category': categories[i],
                'size': size,
                'gender': random.choice(['M', 'F', 'U']),
                'cost_price': round(random.uniform(40, 80), 2),
                'retail_price': round(random.uniform(80, 160), 2)
            })
    
    return pd.DataFrame(products)

def clear_and_regenerate_data(conn):
    """Clear all existing data and regenerate fresh test data"""
    cursor = conn.cursor()
    
    # Disable foreign keys temporarily
    cursor.execute("PRAGMA foreign_keys = OFF")
    
    try:
        # Clear tables in correct order (children first, then parents)
        cursor.execute("DELETE FROM sales")
        cursor.execute("DELETE FROM transfer_recommendations")
        cursor.execute("DELETE FROM stock_levels")
        cursor.execute("DELETE FROM products")
        
        # Re-enable foreign keys
        cursor.execute("PRAGMA foreign_keys = ON")
        
        # Regenerate all data
        generate_complete_test_dataset(conn)
        
    except Exception as e:
        # Always re-enable foreign keys even if error occurs
        cursor.execute("PRAGMA foreign_keys = ON")
        raise e

def generate_complete_test_dataset(conn):
    """Generate a complete test dataset with all tables populated"""
    cursor = conn.cursor()
    
    # Generate and insert products
    products_df = generate_test_data()
    products_df.to_sql('products', conn, if_exists='append', index=False)
    
    # Get all stores
    cursor.execute("SELECT store_id FROM stores WHERE is_active = 1")
    stores = [row[0] for row in cursor.fetchall()]
    
    # Get all products
    products = pd.read_sql("SELECT sku FROM products", conn)
    skus = products['sku'].tolist()
    
    # Generate initial stock levels
    stock_data = []
    for store_id in stores:
        # Each store gets a random subset of products
        store_skus = random.sample(skus, k=random.randint(len(skus)//2, len(skus)-3))
        
        for sku in store_skus:
            quantity = random.randint(0, 10) if store_id != 'ONLINE' else random.randint(5, 20)
            stock_data.append({
                'store_id': store_id,
                'sku': sku,
                'quantity': quantity
            })
    
    stock_df = pd.DataFrame(stock_data)
    stock_df.to_sql('stock_levels', conn, if_exists='append', index=False)
    
    # Generate sales data for the past 30 days
    sales_data = []
    start_date = datetime.now() - timedelta(days=30)
    
    for day in range(30):
        sale_date = start_date + timedelta(days=day)
        date_str = sale_date.strftime('%Y-%m-%d')
        
        for store_id in stores:
            # Number of sales per day varies by store
            if store_id == 'ONLINE':
                daily_sales = random.randint(8, 15)
            else:
                daily_sales = random.randint(3, 8)
            
            for _ in range(daily_sales):
                # Get a random product that this store actually has in stock
                cursor.execute(
                    "SELECT sku FROM stock_levels WHERE store_id = ? AND quantity > 0",
                    (store_id,)
                )
                available_skus = [row[0] for row in cursor.fetchall()]
                
                if available_skus:
                    sku = random.choice(available_skus)
                    quantity = 1 if random.random() > 0.1 else 2
                    
                    # Get retail price
                    cursor.execute(
                        "SELECT retail_price FROM products WHERE sku = ?",
                        (sku,)
                    )
                    result = cursor.fetchone()
                    retail_price = result[0] if result else 100.0
                    
                    sales_data.append({
                        'store_id': store_id,
                        'sku': sku,
                        'sale_date': date_str,
                        'quantity': quantity,
                        'revenue': quantity * retail_price
                    })
    
    # Insert sales in batches
    if sales_data:
        sales_df = pd.DataFrame(sales_data)
        sales_df.to_sql('sales', conn, if_exists='append', index=False)
    
    conn.commit()
    return True

def generate_sales_data(conn, days=30):
    """Generate sales data without clearing existing data"""
    cursor = conn.cursor()
    
    # Get stores and products
    stores_df = pd.read_sql("SELECT store_id FROM stores WHERE is_active = 1", conn)
    if stores_df.empty:
        return
    
    sales_data = []
    start_date = datetime.now() - timedelta(days=days)
    
    for day in range(days):
        sale_date = start_date + timedelta(days=day)
        date_str = sale_date.strftime('%Y-%m-%d')
        
        for _, store_row in stores_df.iterrows():
            store_id = store_row['store_id']
            
            # Number of sales per day varies by store
            if store_id == 'ONLINE':
                daily_sales = random.randint(5, 12)
            else:
                daily_sales = random.randint(2, 6)
            
            for _ in range(daily_sales):
                # Get a random product
                cursor.execute(
                    "SELECT p.sku, p.retail_price FROM products p LIMIT 1 OFFSET ABS(RANDOM()) % MAX(1, (SELECT COUNT(*) FROM products))"
                )
                result = cursor.fetchone()
                if result:
                    sku, retail_price = result
                    quantity = 1 if random.random() > 0.15 else 2
                    
                    sales_data.append({
                        'store_id': store_id,
                        'sku': sku,
                        'sale_date': date_str,
                        'quantity': quantity,
                        'revenue': quantity * retail_price
                    })
    
    # Insert in batches
    if sales_data:
        sales_df = pd.DataFrame(sales_data)
        sales_df.to_sql('sales', conn, if_exists='append', index=False)
        conn.commit()
    
    return len(sales_data)