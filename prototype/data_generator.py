# data_generator.py
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random
import sqlite3
from typing import List, Dict, Tuple

def generate_large_product_catalog(num_styles=200):
    """Generate a large catalog of footwear products"""
    
    # Product categories and brands for realistic data
    categories = {
        'Running': ['Nike Air Max', 'Adidas Ultraboost', 'New Balance Fresh Foam', 'ASICS Gel-Kayano', 'Brooks Ghost'],
        'Casual': ['Converse Chuck Taylor', 'Vans Old Skool', 'Adidas Stan Smith', 'Nike Air Force 1', 'Puma Suede'],
        'Basketball': ['Nike LeBron', 'Adidas Harden', 'Under Armour Curry', 'Jordan Retro', 'Reebok Question'],
        'Trail': ['Salomon Speedcross', 'Merrell Moab', 'Hoka One One', 'The North Face', 'Columbia'],
        'Formal': ['Cole Haan', 'Clarks Desert', 'Ecco Soft', 'Rockport', 'Florsheim'],
        'Boots': ['Dr. Martens', 'Timberland', 'Blundstone', 'Red Wing', 'CAT'],
        'Sandals': ['Birkenstock', 'Teva', 'Chaco', 'Reef', 'Havaianas']
    }
    
    # Size ranges by gender and category
    size_ranges = {
        'M': {'Running': [7, 8, 9, 10, 11, 12, 13], 'Casual': [7, 8, 9, 10, 11, 12], 'Basketball': [8, 9, 10, 11, 12, 13, 14, 15]},
        'F': {'Running': [5, 6, 7, 8, 9, 10], 'Casual': [5, 6, 7, 8, 9], 'Sandals': [5, 6, 7, 8, 9, 10]},
        'U': {'Trail': [7, 8, 9, 10, 11], 'Boots': [7, 8, 9, 10, 11, 12], 'Formal': [7, 8, 9, 10, 11]}
    }
    
    products = []
    style_counter = 1
    
    for category, brand_models in categories.items():
        for brand_model in brand_models:
            if style_counter > num_styles:
                break
            
            # Determine gender based on category
            if category in ['Running', 'Casual', 'Basketball']:
                gender_options = ['M', 'F']
            elif category in ['Boots', 'Trail']:
                gender_options = ['M', 'U']
            else:
                gender_options = ['M', 'F', 'U']
            
            gender = random.choice(gender_options)
            
            # Get size range for this gender and category
            if gender in size_ranges and category in size_ranges[gender]:
                sizes = size_ranges[gender][category]
            else:
                sizes = [7, 8, 9, 10, 11]  # Default sizes
            
            # Generate multiple color variants per style
            colors = ['Black', 'White', 'Navy', 'Gray', 'Red', 'Blue', 'Green']
            color = random.choice(colors)
            
            style_code = f"{brand_model[:3].upper()}{category[:3].upper()}{style_counter:04d}"
            
            # Generate 70% of available sizes for each style
            num_sizes = max(3, int(len(sizes) * 0.7))
            selected_sizes = random.sample(sizes, k=num_sizes)
            
            for size in selected_sizes:
                sku = f"{style_code}-{size}-{color[:3].upper()}"
                
                # Price ranges based on category
                if category in ['Formal', 'Boots']:
                    cost_price = round(random.uniform(60, 150), 2)
                    retail_price = round(cost_price * random.uniform(2.0, 2.8), 2)
                elif category in ['Basketball', 'Trail']:
                    cost_price = round(random.uniform(50, 120), 2)
                    retail_price = round(cost_price * random.uniform(2.0, 2.5), 2)
                else:
                    cost_price = round(random.uniform(30, 90), 2)
                    retail_price = round(cost_price * random.uniform(1.8, 2.3), 2)
                
                products.append({
                    'sku': sku,
                    'style_code': style_code,
                    'style_name': f"{brand_model} {color}",
                    'category': category,
                    'size': size,
                    'gender': gender,
                    'color': color,
                    'cost_price': cost_price,
                    'retail_price': retail_price
                })
            
            style_counter += 1
    
    return pd.DataFrame(products)

def generate_high_volume_stock(conn, min_stock_records=10000):
    """Generate high volume stock distribution across all stores"""
    cursor = conn.cursor()
    
     # Disable foreign keys temporarily
    cursor.execute("PRAGMA foreign_keys = OFF")
    
    try:
        # Clear tables in correct order (children first, then parents)
        cursor.execute("DELETE FROM sales")
        cursor.execute("DELETE FROM transfer_recommendations")
        cursor.execute("DELETE FROM stock_levels")
        
        # Re-enable foreign keys
        cursor.execute("PRAGMA foreign_keys = ON")

    except Exception as e:
        # Always re-enable foreign keys even if error occurs
        cursor.execute("PRAGMA foreign_keys = ON")
        raise e

    # Get all active stores
    cursor.execute("SELECT store_id FROM stores WHERE is_active = 1")
    stores = [row[0] for row in cursor.fetchall()]
    
    # Get all products
    products_df = pd.read_sql("SELECT sku FROM products", conn)
    if products_df.empty:
        st.warning("No products found. Generating product catalog first...")
        products_df = generate_large_product_catalog(150)
        products_df.to_sql('products', conn, if_exists='append', index=False)
        products_df = pd.read_sql("SELECT sku FROM products", conn)
    
    skus = products_df['sku'].tolist()
    
    print(f"Generating stock for {len(stores)} stores and {len(skus)} products...")
    
    # Clear existing stock
    cursor.execute("DELETE FROM stock_levels")
    
    stock_data = []
    total_stock_records = 0
    
    # Store-specific stock patterns
    store_patterns = {
        'ONLINE': {'base': 30, 'range': (20, 50), 'coverage': 0.95},  # Online has most SKUs
        'STORE01': {'base': 15, 'range': (10, 30), 'coverage': 0.85},  # Sydney CBD - flagship
        'STORE02': {'base': 12, 'range': (8, 25), 'coverage': 0.80},  # Melbourne
        'STORE03': {'base': 10, 'range': (5, 20), 'coverage': 0.75},  # Brisbane
        'STORE04': {'base': 8, 'range': (3, 15), 'coverage': 0.70},   # Perth
        'STORE05': {'base': 6, 'range': (2, 12), 'coverage': 0.65},   # Adelaide
        'STORE06': {'base': 7, 'range': (3, 14), 'coverage': 0.68},   # Canberra
        'STORE07': {'base': 9, 'range': (4, 18), 'coverage': 0.72}    # Gold Coast
    }
    
    for store_id in stores:
        pattern = store_patterns.get(store_id, {'base': 5, 'range': (2, 10), 'coverage': 0.6})
        
        # Determine which SKUs this store carries
        num_skus_for_store = int(len(skus) * pattern['coverage'])
        store_skus = random.sample(skus, k=num_skus_for_store)
        
        for sku in store_skus:
            base_qty = pattern['base']
            min_qty, max_qty = pattern['range']
            
            # Add some randomness and ensure minimum
            quantity = random.randint(min_qty, max_qty)
            
            # Make some SKUs out of stock
            if random.random() < 0.05:  # 5% chance of being out of stock
                quantity = 0
            elif random.random() < 0.1:  # 10% chance of being low stock
                quantity = random.randint(1, 2)
            
            stock_data.append((store_id, sku, quantity))
            total_stock_records += 1
            
            # Insert in batches to avoid memory issues
            if len(stock_data) >= 1000:
                cursor.executemany(
                    "INSERT INTO stock_levels (store_id, sku, quantity) VALUES (?, ?, ?)",
                    stock_data
                )
                stock_data = []
    
    # Insert remaining records
    if stock_data:
        cursor.executemany(
            "INSERT INTO stock_levels (store_id, sku, quantity) VALUES (?, ?, ?)",
            stock_data
        )
    
    conn.commit()
    print(f"Generated {total_stock_records:,} stock records")
    return total_stock_records

def generate_high_volume_sales(conn, min_sales=20000, days=90):
    """Generate high volume sales data for all stores"""
    cursor = conn.cursor()
    
    # Get all stores and products with prices
    stores_df = pd.read_sql("SELECT store_id FROM stores WHERE is_active = 1", conn)
    products_df = pd.read_sql("SELECT sku, retail_price FROM products", conn)
    
    if stores_df.empty or products_df.empty:
        print("No stores or products found")
        return 0
    
    # Get stock levels to know what's actually available
    stock_df = pd.read_sql("SELECT store_id, sku FROM stock_levels WHERE quantity > 0", conn)
    
    if stock_df.empty:
        print("No stock available for sales")
        return 0
    
    # Create sales patterns by store
    sales_patterns = {
        'ONLINE': {
            'daily_range': (80, 150),  # 80-150 sales per day
            'weekend_boost': 1.3,       # 30% more on weekends
            'peak_hours': [(18, 23)],   # Evening sales
            'avg_items_per_sale': 1.2
        },
        'STORE01': {  # Sydney CBD - busy store
            'daily_range': (40, 80),
            'weekend_boost': 1.5,
            'peak_hours': [(12, 14), (17, 19)],
            'avg_items_per_sale': 1.3
        },
        'STORE02': {  # Melbourne
            'daily_range': (35, 70),
            'weekend_boost': 1.4,
            'peak_hours': [(12, 14), (17, 19)],
            'avg_items_per_sale': 1.2
        },
        'STORE03': {  # Brisbane
            'daily_range': (25, 50),
            'weekend_boost': 1.3,
            'peak_hours': [(11, 14)],
            'avg_items_per_sale': 1.1
        },
        'STORE04': {  # Perth
            'daily_range': (20, 40),
            'weekend_boost': 1.2,
            'peak_hours': [(11, 14)],
            'avg_items_per_sale': 1.1
        },
        'STORE05': {  # Adelaide
            'daily_range': (18, 35),
            'weekend_boost': 1.2,
            'peak_hours': [(11, 14)],
            'avg_items_per_sale': 1.1
        },
        'STORE06': {  # Canberra
            'daily_range': (15, 30),
            'weekend_boost': 1.2,
            'peak_hours': [(11, 14)],
            'avg_items_per_sale': 1.1
        },
        'STORE07': {  # Gold Coast
            'daily_range': (30, 60),
            'weekend_boost': 1.4,
            'peak_hours': [(10, 13), (16, 19)],
            'avg_items_per_sale': 1.2
        }
    }
    
    sales_data = []
    total_sales_count = 0
    
    # Generate sales for each day
    for day in range(days):
        sale_date = datetime.now() - timedelta(days=days - day - 1)
        date_str = sale_date.strftime('%Y-%m-%d')
        
        # Day of week effects
        is_weekend = sale_date.weekday() >= 5  # Saturday=5, Sunday=6
        
        for _, store_row in stores_df.iterrows():
            store_id = store_row['store_id']
            pattern = sales_patterns.get(store_id, {'daily_range': (10, 20), 'weekend_boost': 1.1})
            
            # Get stock available at this store
            store_stock = stock_df[stock_df['store_id'] == store_id]
            
            if store_stock.empty:
                continue
            
            # Determine daily sales volume
            min_daily, max_daily = pattern['daily_range']
            daily_sales = random.randint(min_daily, max_daily)
            
            # Boost on weekends
            if is_weekend:
                daily_sales = int(daily_sales * pattern['weekend_boost'])
            
            # Seasonal effects (simulate busier periods)
            month = sale_date.month
            if month in [11, 12]:  # Nov-Dec (holiday season)
                daily_sales = int(daily_sales * 1.5)
            elif month in [6, 7]:  # Jun-Jul (winter in Australia)
                daily_sales = int(daily_sales * 1.2)
            elif month in [1, 2]:  # Jan-Feb (summer)
                daily_sales = int(daily_sales * 1.3)
            
            # Generate individual sales
            for _ in range(daily_sales):
                # Pick a random product that this store has in stock
                available_skus = store_stock['sku'].tolist()
                if not available_skus:
                    continue
                
                sku = random.choice(available_skus)
                
                # Determine quantity (usually 1, sometimes 2, rarely more)
                rand = random.random()
                if rand < 0.8:
                    quantity = 1
                elif rand < 0.95:
                    quantity = 2
                else:
                    quantity = random.randint(3, 5)
                
                # Get retail price
                price_row = products_df[products_df['sku'] == sku]
                retail_price = price_row['retail_price'].iloc[0] if not price_row.empty else 100.0
                
                revenue = quantity * retail_price
                
                sales_data.append({
                    'store_id': store_id,
                    'sku': sku,
                    'sale_date': date_str,
                    'quantity': quantity,
                    'revenue': round(revenue, 2)
                })
                
                total_sales_count += 1
                
                # Insert in batches
                if len(sales_data) >= 5000:
                    insert_batch(cursor, sales_data)
                    sales_data = []
    
    # Insert remaining sales
    if sales_data:
        insert_batch(cursor, sales_data)
    
    conn.commit()
    print(f"Generated {total_sales_count:,} sales records over {days} days")
    return total_sales_count

def insert_batch(cursor, sales_data):
    """Insert a batch of sales records efficiently"""
    batch_values = [(s['store_id'], s['sku'], s['sale_date'], s['quantity'], s['revenue']) 
                    for s in sales_data]
    
    cursor.executemany(
        "INSERT INTO sales (store_id, sku, sale_date, quantity, revenue) VALUES (?, ?, ?, ?, ?)",
        batch_values
    )

def generate_massive_dataset(conn):
    """Generate a complete massive dataset with >10k stock and >20k sales"""
    print("Starting massive dataset generation...")
    
    # Step 1: Generate large product catalog
    print("1. Generating product catalog...")
    products_df = generate_large_product_catalog(250)
    products_df.to_sql('products', conn, if_exists='replace', index=False)
    print(f"   Generated {len(products_df):,} products")
    
    # Step 2: Generate high volume stock
    print("2. Generating stock levels...")
    stock_count = generate_high_volume_stock(conn, min_stock_records=15000)
    
    # Step 3: Generate high volume sales
    print("3. Generating sales data...")
    sales_count = generate_high_volume_sales(conn, min_sales=25000, days=120)
    
    print("\n" + "="*50)
    print("DATASET GENERATION COMPLETE")
    print("="*50)
    print(f"Total Products: {len(products_df):,}")
    print(f"Total Stock Records: {stock_count:,}")
    print(f"Total Sales Records: {sales_count:,}")
    print("="*50)
    
    return {
        'products': len(products_df),
        'stock_records': stock_count,
        'sales_records': sales_count
    }

# Add this to your data_generator.py for more realistic velocity patterns

def generate_realistic_sales_with_velocity(conn, days=90):
    """Generate sales with realistic velocity patterns"""
    
    cursor = conn.cursor()
    
    # Get all products
    cursor.execute("SELECT sku, category FROM products")
    products = cursor.fetchall()
    
    # Define velocity patterns by category
    #velocity_patterns = {
    #    'Running': {'base_rate': 0.15, 'variation': 0.1, 'seasonal': 1.2},  # Fast moving
    #    'Casual': {'base_rate': 0.10, 'variation': 0.08, 'seasonal': 1.0},   # Moderate
    #    'Basketball': {'base_rate': 0.12, 'variation': 0.15, 'seasonal': 1.3}, # Seasonal peaks
    #    'Trail': {'base_rate': 0.08, 'variation': 0.06, 'seasonal': 0.8},     # Slow
    #    'Formal': {'base_rate': 0.05, 'variation': 0.03, 'seasonal': 0.9},    # Very slow
    #    'Boots': {'base_rate': 0.07, 'variation': 0.05, 'seasonal': 1.1},     # Moderate
    #    'Sandals': {'base_rate': 0.13, 'variation': 0.12, 'seasonal': 1.4}    # Seasonal
    #}

    velocity_patterns = {
        'Running': {'base_rate': 0.1, 'variation': 0.68, 'seasonal': 0.9},  # Fast moving
        'Casual': {'base_rate': 0.67, 'variation': 0.05, 'seasonal': 0.67},   # Moderate
        'Basketball': {'base_rate': 0.08, 'variation': 0.1, 'seasonal': 0.8}, # Seasonal peaks
        'Trail': {'base_rate': 0.05, 'variation': 0.04, 'seasonal': 0.5},     # Slow
        'Formal': {'base_rate': 0.35, 'variation': 0.02, 'seasonal': 0.6},    # Very slow
        'Boots': {'base_rate': 0.045, 'variation': 0.035, 'seasonal': 0.9},     # Moderate
        'Sandals': {'base_rate': 0.09, 'variation': 0.08, 'seasonal': 1.0}    # Seasonal
    }
    
    # Generate sales with realistic patterns
    sales_data = []
    
    for sku, category in products:
        pattern = velocity_patterns.get(category, {'base_rate': 0.67, 'variation': 0.035, 'seasonal': 0.67 })
        
        # Get current stock
        cursor.execute("SELECT store_id, quantity FROM stock_levels WHERE sku = ?", (sku,))
        stock_records = cursor.fetchall()
        
        for store_id, stock_qty in stock_records:
            if stock_qty <= 0:
                continue
            
            # Base sales rate for this SKU
            base_rate = pattern['base_rate'] * random.uniform(0.8, 1.2)
            
            for day in range(days):
                date = datetime.now() - timedelta(days=days - day)
                
                # Seasonal factor
                month = date.month
                seasonal_factor = pattern['seasonal']
                if category == 'Sandals' and month in [11, 12, 1, 2]:  # Summer
                    seasonal_factor *= 1.5
                elif category == 'Boots' and month in [6, 7, 8]:  # Winter
                    seasonal_factor *= 1.3
                
                # Calculate daily probability of sale
                daily_prob = (base_rate * seasonal_factor) / 30  # Convert monthly to daily
                
                # Generate sales based on probability
                if random.random() < daily_prob:
                    # Quantity sold (usually 1, sometimes more)
                    quantity = 1 if random.random() > 0.1 else random.randint(2, 3)
                    
                    # Get retail price
                    cursor.execute("SELECT retail_price FROM products WHERE sku = ?", (sku,))
                    price_result = cursor.fetchone()
                    retail_price = price_result[0] if price_result else 100.0
                    
                    sales_data.append({
                        'store_id': store_id,
                        'sku': sku,
                        'sale_date': date.strftime('%Y-%m-%d'),
                        'quantity': quantity,
                        'revenue': quantity * retail_price
                    })
    
    # Insert sales
    if sales_data:
        sales_df = pd.DataFrame(sales_data)
        sales_df.to_sql('sales', conn, if_exists='append', index=False)
    
    return len(sales_data)

def generate_quick_test_data(conn):
    """Generate a smaller test dataset quickly"""
    print("Generating quick test dataset...")
    
    # Generate products
    products_df = generate_large_product_catalog(50)
    products_df.to_sql('products', conn, if_exists='replace', index=False)
    
    # Generate stock
    generate_high_volume_stock(conn, min_stock_records=2000)
    
    # Generate sales
    generate_high_volume_sales(conn, min_sales=5000, days=30)
    
    return True