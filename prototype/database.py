# database.py
import sqlite3
import streamlit as st

def init_database():
    """Initialize SQLite database with required tables"""
    try:
        conn = sqlite3.connect('inventory_v3.db', check_same_thread=False)
        cursor = conn.cursor()
        
        # Enable foreign keys
        cursor.execute("PRAGMA foreign_keys = ON")
        
        # Stores table
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS stores (
            store_id TEXT PRIMARY KEY,
            store_name TEXT NOT NULL,
            store_type TEXT CHECK(store_type IN ('physical', 'online')),
            location TEXT,
            is_active BOOLEAN DEFAULT 1
        )
        ''')
        
        # Products table (SKUs)
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS products (
            sku TEXT PRIMARY KEY,
            style_code TEXT NOT NULL,
            style_name TEXT NOT NULL,
            description TEXT,
            category TEXT,
            size REAL,
            gender TEXT,
            color TEXT,
            cost_price REAL,
            retail_price REAL,
            supplier TEXT,
            brand TEXT,
            product_type TEXT,
            season TEXT,
            create_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        ''')
        
        # Stock levels table
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS stock_levels (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            store_id TEXT,
            sku TEXT,
            quantity INTEGER,
            last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (store_id) REFERENCES stores(store_id),
            FOREIGN KEY (sku) REFERENCES products(sku),
            UNIQUE(store_id, sku)
        )
        ''')
        
        # Sales transactions table
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS sales (
            sale_id TEXT,
            store_id TEXT,
            sku TEXT,
            sale_date DATETIME,
            quantity INTEGER,
            revenue REAL,
            FOREIGN KEY (store_id) REFERENCES stores(store_id),
            FOREIGN KEY (sku) REFERENCES products(sku)
        )
        ''')
        
        # Transfer recommendations table
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS transfer_recommendations (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            recommendation_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            from_store_id TEXT,
            to_store_id TEXT,
            sku TEXT,
            quantity INTEGER,
            priority TEXT DEFAULT 'medium',
            status TEXT DEFAULT 'pending',
            reason TEXT,
            FOREIGN KEY (from_store_id) REFERENCES stores(store_id),
            FOREIGN KEY (to_store_id) REFERENCES stores(store_id)
        )
        ''')
        
        # Insert default stores if they don't exist
        #stores = [
        #    ('ONLINE', 'Online Store', 'online', 'Australia', 1),
        #    ('STORE01', 'Sydney CBD', 'physical', 'Sydney', 1),
        #    ('STORE02', 'Melbourne Central', 'physical', 'Melbourne', 1),
        #    ('STORE03', 'Brisbane Queen St', 'physical', 'Brisbane', 1),
        #    ('STORE04', 'Perth City', 'physical', 'Perth', 1),
        #    ('STORE05', 'Adelaide Rundle', 'physical', 'Adelaide', 1),
        #    ('STORE06', 'Canberra Centre', 'physical', 'Canberra', 1),
        #    ('STORE07', 'Gold Coast', 'physical', 'Gold Coast', 1)
        #]
        
        #cursor.executemany(
        #    "INSERT OR IGNORE INTO stores (store_id, store_name, store_type, location, is_active) VALUES (?, ?, ?, ?, ?)",
        #    stores
        #)
        
        conn.commit()
        return conn
        
    except sqlite3.Error as e:
        st.error(f"Database error: {e}")
        return None