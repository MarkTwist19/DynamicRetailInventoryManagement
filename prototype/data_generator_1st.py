# data_generator.py
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random

def generate_products(num_styles=50, sizes=[6, 6.5, 7, 7.5, 8, 8.5, 9, 9.5, 10, 10.5, 11, 12]):
    """Generate realistic footwear products"""
    styles = []
    categories = ['Running', 'Casual', 'Formal', 'Boots', 'Sandals', 'Basketball']
    brands = ['Nike', 'Adidas', 'New Balance', 'Clarks', 'Dr. Martens', 'ASICS']
    
    for i in range(1, num_styles + 1):
        category = random.choice(categories)
        brand = random.choice(brands)
        style_code = f"{brand[:3].upper()}{category[:3].upper()}{i:03d}"
        
        for size in random.sample(sizes, k=random.randint(6, len(sizes))):
            sku = f"{style_code}-{size}"
            styles.append({
                'sku': sku,
                'style_code': style_code,
                'style_name': f"{brand} {category} {random.choice(['Pro', 'Air', 'Ultra', 'Classic'])}",
                'category': category,
                'size': size,
                'gender': random.choice(['M', 'F', 'U']),
                'cost_price': round(random.uniform(40, 120), 2),
                'retail_price': round(random.uniform(80, 250), 2)
            })
    
    return pd.DataFrame(styles)

def generate_sales_data(conn, days=90):
    """Generate 90 days of sales data"""
    cursor = conn.cursor()
    
    # Get all stores and products
    stores = pd.read_sql("SELECT store_id FROM stores", conn)
    products = pd.read_sql("SELECT sku, retail_price FROM products", conn)
    
    if len(products) == 0:
        products = generate_products()
        products.to_sql('products', conn, if_exists='replace', index=False)
        products = pd.read_sql("SELECT sku, retail_price FROM products", conn)
    
    sales_data = []
    
    # Generate sales with store-specific patterns
    store_patterns = {
        'ONLINE': {'volume': 0.3, 'peak_days': [0, 1, 6]},  # Weekend online sales
        'STORE01': {'volume': 1.0, 'peak_days': [4, 5]},    # Sydney CBD - Thu, Fri
        'STORE02': {'volume': 0.9, 'peak_days': [5, 6]},    # Melbourne - Fri, Sat
        # ... patterns for all stores
    }
    
    for day in range(days):
        date = datetime.now() - timedelta(days=days - day)
        
        for _, store in stores.iterrows():
            store_id = store['store_id']
            pattern = store_patterns.get(store_id, {'volume': 0.7, 'peak_days': []})
            
            # Base daily transactions
            daily_transactions = int(random.normalvariate(15 * pattern['volume'], 5))
            if date.weekday() in pattern['peak_days']:
                daily_transactions = int(daily_transactions * 1.5)
            
            for _ in range(max(1, daily_transactions)):
                # Pick a product (some products more popular than others)
                product = products.sample(1).iloc[0]
                sku = product['sku']
                
                # Quantity (usually 1, sometimes 2)
                quantity = 1 if random.random() > 0.1 else 2
                
                sales_data.append({
                    'store_id': store_id,
                    'sku': sku,
                    'sale_date': date.strftime('%Y-%m-%d'),
                    'quantity': quantity,
                    'revenue': quantity * product['retail_price']
                })
    
    sales_df = pd.DataFrame(sales_data)
    sales_df.to_sql('sales', conn, if_exists='append', index=False)
    
    # Generate initial stock levels
    generate_stock_levels(conn)
    
    return sales_df

def generate_stock_levels(conn):
    """Generate initial stock distribution"""
    cursor = conn.cursor()
    
    # Get all stores and products
    stores = pd.read_sql("SELECT store_id FROM stores", conn)
    products = pd.read_sql("SELECT sku FROM products", conn)
    
    stock_data = []
    
    for _, store in stores.iterrows():
        store_id = store['store_id']
        
        # Online store gets more stock initially
        base_stock = 20 if store_id == 'ONLINE' else random.randint(3, 15)
        
        # Randomly select products for this store (stores don't carry all SKUs)
        store_products = products.sample(frac=random.uniform(0.6, 0.9))
        
        for _, product in store_products.iterrows():
            sku = product['sku']
            
            # Generate stock quantity with some variance
            if store_id == 'ONLINE':
                quantity = random.randint(base_stock - 5, base_stock + 10)
            else:
                quantity = random.randint(max(1, base_stock - 3), base_stock + 3)
            
            stock_data.append({
                'store_id': store_id,
                'sku': sku,
                'quantity': quantity
            })
    
    stock_df = pd.DataFrame(stock_data)
    stock_df.to_sql('stock_levels', conn, if_exists='replace', index=False)
    
    return stock_df