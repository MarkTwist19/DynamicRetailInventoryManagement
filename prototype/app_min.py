# minimal_app.py - Works with Python 3.13 (no external dependencies needed)
import streamlit as st
import sqlite3
import csv
import io
from datetime import datetime, timedelta
import random

# Set page config
st.set_page_config(
    page_title="Retail Stock Balancer",
    page_icon="👟",
    layout="wide"
)

# Initialize database
def init_database():
    conn = sqlite3.connect(':memory:')  # Use in-memory database
    cursor = conn.cursor()
    
    # Create tables
    cursor.execute('''
    CREATE TABLE stores (
        store_id TEXT PRIMARY KEY,
        store_name TEXT,
        store_type TEXT,
        location TEXT
    )
    ''')
    
    cursor.execute('''
    CREATE TABLE stock_levels (
        store_id TEXT,
        sku TEXT,
        quantity INTEGER,
        PRIMARY KEY (store_id, sku)
    )
    ''')
    
    cursor.execute('''
    CREATE TABLE sales (
        store_id TEXT,
        sku TEXT,
        sale_date TEXT,
        quantity INTEGER
    )
    ''')
    
    # Insert default stores
    stores = [
        ('ONLINE', 'Online Store', 'online', 'Australia'),
        ('STORE01', 'Sydney CBD', 'physical', 'Sydney'),
        ('STORE02', 'Melbourne Central', 'physical', 'Melbourne'),
        ('STORE03', 'Brisbane Queen St', 'physical', 'Brisbane'),
        ('STORE04', 'Perth City', 'physical', 'Perth'),
        ('STORE05', 'Adelaide Rundle', 'physical', 'Adelaide'),
        ('STORE06', 'Canberra Centre', 'physical', 'Canberra'),
        ('STORE07', 'Gold Coast', 'physical', 'Gold Coast')
    ]
    
    cursor.executemany("INSERT INTO stores VALUES (?, ?, ?, ?)", stores)
    
    # Generate sample products
    products = []
    for style in ['RUN001', 'RUN002', 'CAS001', 'FOR001']:
        for size in [7, 8, 9, 10, 11]:
            products.append((f"{style}-{size}", f"{style} Shoes", "Footwear", size))
    
    # Generate sample stock
    stock = []
    for store_id, _, _, _ in stores:
        for sku, _, _, _ in products:
            if random.random() > 0.3:  # 70% chance store has this SKU
                quantity = random.randint(0, 10) if store_id != 'ONLINE' else random.randint(5, 20)
                stock.append((store_id, sku, quantity))
    
    cursor.executemany("INSERT INTO stock_levels VALUES (?, ?, ?)", stock)
    
    # Generate sample sales
    sales = []
    for _ in range(100):
        store = random.choice(stores)[0]
        product = random.choice(products)[0]
        days_ago = random.randint(0, 30)
        date = (datetime.now() - timedelta(days=days_ago)).strftime('%Y-%m-%d')
        quantity = random.randint(1, 3)
        sales.append((store, product, date, quantity))
    
    cursor.executemany("INSERT INTO sales VALUES (?, ?, ?, ?)", sales)
    
    conn.commit()
    return conn

# Initialize the app
conn = init_database()

# Simple UI
st.title("👟 Retail Stock Balancer")
st.write("Minimal version for Python 3.13")

# Sidebar controls
with st.sidebar:
    st.header("Settings")
    low_stock = st.slider("Low stock threshold", 1, 5, 2)
    days_lookback = st.slider("Days to look back", 7, 90, 14)
    
    if st.button("Generate New Sample Data"):
        conn.close()
        conn = init_database()
        st.success("New data generated!")

# Main analysis
st.header("Store Analysis")

# Get all stores
cursor = conn.cursor()
cursor.execute("SELECT store_id, store_name FROM stores ORDER BY store_type DESC, store_name")
stores = cursor.fetchall()

for store_id, store_name in stores:
    with st.expander(f"**{store_name}** ({store_id})"):
        # Get stock needs
        cursor.execute('''
        SELECT sl.sku, sl.quantity, COALESCE(SUM(s.quantity), 0) as sales_count
        FROM stock_levels sl
        LEFT JOIN sales s ON sl.store_id = s.store_id AND sl.sku = s.sku 
            AND s.sale_date >= date('now', ? || ' days')
        WHERE sl.store_id = ?
        GROUP BY sl.sku, sl.quantity
        ''', (f"-{days_lookback}", store_id))
        
        rows = cursor.fetchall()
        
        if rows:
            needs = []
            excess = []
            
            for sku, quantity, sales_count in rows:
                if sales_count > 0 and quantity <= low_stock:
                    needs.append((sku, quantity, sales_count))
                elif sales_count == 0 and quantity >= 5:
                    excess.append((sku, quantity, sales_count))
            
            col1, col2 = st.columns(2)
            
            with col1:
                if needs:
                    st.subheader(f"🚨 Needs Stock ({len(needs)} items)")
                    for sku, qty, sales in needs:
                        st.write(f"- {sku}: {qty} left, sold {sales} in last {days_lookback} days")
                else:
                    st.success("✅ No urgent stock needs")
            
            with col2:
                if excess:
                    st.subheader(f"📦 Excess Stock ({len(excess)} items)")
                    for sku, qty, sales in excess:
                        st.write(f"- {sku}: {qty} in stock, no sales in {days_lookback} days")
                else:
                    st.info("No excess stock identified")
        else:
            st.write("No stock data for this store")

# Overall stats
st.header("📊 Overall Statistics")

col1, col2, col3 = st.columns(3)

with col1:
    cursor.execute("SELECT COUNT(DISTINCT sku) FROM stock_levels")
    total_skus = cursor.fetchone()[0]
    st.metric("Total SKUs", total_skus)

with col2:
    cursor.execute("SELECT COUNT(*) FROM sales WHERE sale_date >= date('now', ? || ' days')", 
                   (f"-{days_lookback}",))
    recent_sales = cursor.fetchone()[0]
    st.metric(f"Sales ({days_lookback}d)", recent_sales)

with col3:
    cursor.execute("SELECT SUM(quantity) FROM stock_levels")
    total_stock = cursor.fetchone()[0] or 0
    st.metric("Total Stock", total_stock)

# Export functionality
st.header("📤 Export Data")

if st.button("Export Stock Report"):
    cursor.execute("SELECT * FROM stock_levels ORDER BY store_id, sku")
    stock_data = cursor.fetchall()
    
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(['Store ID', 'SKU', 'Quantity'])
    writer.writerows(stock_data)
    
    st.download_button(
        label="Download CSV",
        data=output.getvalue(),
        file_name=f"stock_report_{datetime.now().strftime('%Y%m%d')}.csv",
        mime="text/csv"
    )

# Close connection when done
conn.close()