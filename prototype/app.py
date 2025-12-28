# app.py
import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import numpy as np
from datetime import datetime, timedelta
import sqlite3
import database as db
import data_generator as dg
import io

# Page config
st.set_page_config(
    page_title="Retail Stock Balancer",
    page_icon="👟",
    layout="wide"
)

# Initialize database
conn = db.init_database()

# Sidebar
with st.sidebar:
    st.title("⚙️ Configuration")
    
    # Analysis Settings - sliders and inputs
    st.subheader("Analysis Settings")
    
    # Days lookback
    days_lookback = st.slider("Sales lookback period (days)", 1, 90, 7)
    
    # Need Stock Thresholds
    st.markdown("---")
    st.subheader("🚨 Stock Needs Settings")
    
    col1, col2 = st.columns(2)
    with col1:
        low_stock_threshold = st.number_input(
            "Low stock level", 
            min_value=1, 
            max_value=10, 
            value=2,
            help="Stock below this number is considered critically low"
        )
    
    with col2:
        need_ratio_threshold = st.slider(
            "Sales velocity ratio", 
            min_value=1, 
            max_value=5, 
            value=2,
            help="Ratio of sales to stock (e.g., 1:3 = selling 1 item per 3 in stock)"
        )
    
    st.markdown("---")
    st.subheader("📦 Excess Stock Settings")
    
    col3, col4 = st.columns(2)
    with col3:
        excess_stock_threshold = st.number_input(
            "Minimum stock for excess", 
            min_value=5, 
            max_value=20, 
            value=8,
            help="Only consider items with at least this many in stock"
        )
    
    with col4:
        excess_ratio_threshold = st.slider(
            "Slow-moving ratio", 
            min_value=1, 
            max_value=10, 
            value=7,
            help="Stock-to-sales ratio for excess (e.g., 1:15 = 1 sale per 15 in stock)"
        )
    
    # Explanation of ratios
    with st.expander("📊 Ratio Explanation"):
        st.markdown("""
        **Stock Needs Logic:**
        - If `(units_sold / current_stock) > 1/{need_ratio_threshold}`
        - Example: 1:3 ratio means if you sold 2 items in period and have 5 in stock: `2/5 = 0.4` vs `1/3 = 0.33` → 0.4 > 0.33 → Needs stock!
        
        **Excess Stock Logic:**
        - If `(current_stock / units_sold) > {excess_ratio_threshold}` AND stock > minimum
        - Example: 1:15 ratio means if you have 20 items and sold 1: `20/1 = 20` > 15 → Excess!
        """)
    
    st.title("⚙️ Data Management")

    st.subheader("Upload CSV Data")
    # Upload products CSV
    products_file = st.file_uploader("Upload Products CSV", type=['csv'], key="products")
    if products_file is not None:
        products_df = pd.read_csv(products_file)
        # Ensure required columns exist
        required_cols = ['sku', 'style_code', 'style_name']
        if all(col in products_df.columns for col in required_cols):
            products_df.to_sql('products', conn, if_exists='replace', index=False)
            st.success(f"Loaded {len(products_df)} products")
        else:
            st.error(f"CSV must contain columns: {required_cols}")
    
    # Upload stock CSV
    stock_file = st.file_uploader("Upload Stock Levels CSV", type=['csv'], key="stock")
    if stock_file is not None:
        stock_df = pd.read_csv(stock_file)
        required_cols = ['store_id', 'sku', 'quantity']
        if all(col in stock_df.columns for col in required_cols):
            # Clear existing stock
            cursor = conn.cursor()
            cursor.execute("DELETE FROM stock_levels")
            
            # Insert new stock
            stock_df.to_sql('stock_levels', conn, if_exists='append', index=False)
            st.success(f"Loaded {len(stock_df)} stock records")
        else:
            st.error(f"Stock CSV must contain columns: {required_cols}")
    
    # Upload sales CSV
    sales_file = st.file_uploader("Upload Sales CSV", type=['csv'], key="sales")
    if sales_file is not None:
        sales_df = pd.read_csv(sales_file)
        required_cols = ['store_id', 'sku', 'sale_date', 'quantity']
        if all(col in sales_df.columns for col in required_cols):
            # Add revenue column if not present
            if 'revenue' not in sales_df.columns:
                # Try to get retail price from products
                products = pd.read_sql("SELECT sku, retail_price FROM products", conn)
                if not products.empty:
                    sales_df = sales_df.merge(products, on='sku', how='left')
                    sales_df['revenue'] = sales_df['quantity'] * sales_df['retail_price'].fillna(0)
                    sales_df = sales_df[['store_id', 'sku', 'sale_date', 'quantity', 'revenue']]
            
            sales_df.to_sql('sales', conn, if_exists='append', index=False)
            st.success(f"Loaded {len(sales_df)} sales records")
        else:
            st.error(f"Sales CSV must contain columns: {required_cols}")
    
    st.subheader("Export Data")
    if st.button("📥 Download Sample CSV Templates"):
        # Create sample templates
        sample_products = pd.DataFrame({
            'sku': ['RUN001-9', 'RUN001-10', 'CAS001-8'],
            'style_code': ['RUN001', 'RUN001', 'CAS001'],
            'style_name': ['Running Shoes Pro', 'Running Shoes Pro', 'Casual Sneakers'],
            'category': ['Running', 'Running', 'Casual'],
            'size': [9, 10, 8],
            'gender': ['M', 'M', 'U'],
            'cost_price': [50.0, 50.0, 45.0],
            'retail_price': [100.0, 100.0, 90.0]
        })
        
        sample_stock = pd.DataFrame({
            'store_id': ['STORE01', 'STORE01', 'STORE02'],
            'sku': ['RUN001-9', 'RUN001-10', 'RUN001-9'],
            'quantity': [1, 5, 10]
        })
        
        sample_sales = pd.DataFrame({
            'store_id': ['STORE01', 'STORE01', 'STORE02'],
            'sku': ['RUN001-9', 'RUN001-10', 'RUN001-9'],
            'sale_date': ['2024-01-15', '2024-01-15', '2024-01-14'],
            'quantity': [1, 2, 1]
        })
        
        # Create a ZIP file with all templates
        import zipfile
        from io import BytesIO
        
        zip_buffer = BytesIO()
        with zipfile.ZipFile(zip_buffer, 'w') as zip_file:
            zip_file.writestr('products_template.csv', sample_products.to_csv(index=False))
            zip_file.writestr('stock_template.csv', sample_stock.to_csv(index=False))
            zip_file.writestr('sales_template.csv', sample_sales.to_csv(index=False))
        
        zip_buffer.seek(0)
        st.download_button(
            label="Download Templates ZIP",
            data=zip_buffer,
            file_name="stock_balancer_templates.zip",
            mime="application/zip"
        )

    st.subheader("Data Generation")
    if st.button("🔄 Generate Test Data"):
        with st.spinner("Generating test data..."):
            # Generate products
            products_df = dg.generate_large_product_catalog()
            products_df.to_sql('products', conn, if_exists='replace', index=False)
            
            # Generate initial stock
            dg.generate_high_volume_stock(conn)
            
            # Generate sales data
            dg.generate_high_volume_sales(conn, days=30)
            
            st.success("Test data generated successfully!")

# Main Dashboard
st.title("👟 Footwear Retail Stock Balancer")
st.markdown("### Multi-Store Inventory Optimization Dashboard")

# Quick stats
col1, col2, col3, col4 = st.columns(4)

try:
    with col1:
        total_stock = pd.read_sql("SELECT SUM(quantity) FROM stock_levels", conn).iloc[0,0] or 0
        st.metric("Total Stock", f"{total_stock:,}")
    
    with col2:
        stores_count = pd.read_sql("SELECT COUNT(*) FROM stores WHERE is_active = 1", conn).iloc[0,0]
        st.metric("Active Stores", stores_count)
    
    with col3:
        sku_count = pd.read_sql("SELECT COUNT(DISTINCT sku) FROM stock_levels", conn).iloc[0,0]
        st.metric("Unique SKUs", sku_count)
    
    with col4:
        recent_sales = pd.read_sql("""
            SELECT COUNT(*) as count FROM sales 
            WHERE sale_date >= date('now', ? || ' days')
        """, conn, params=(f"-{days_lookback}",)).iloc[0,0]
        st.metric(f"Sales ({days_lookback}d)", recent_sales)

except Exception as e:
    st.warning("No data available. Please generate test data or upload CSV files.")

try:
    # Calculate stock needs and excess using sales-to-stock ratios
    query = """
    WITH sales_last_n_days AS (
        SELECT 
            store_id,
            sku,
            SUM(quantity) as units_sold,
            COUNT(DISTINCT sale_date) as days_sold
        FROM sales
        WHERE sale_date >= date('now', ? || ' days')
        GROUP BY store_id, sku
    ),
    stock_info AS (
        SELECT 
            sl.store_id,
            s.store_name,
            s.store_type,
            sl.sku,
            p.style_name,
            p.size,
            p.category,
            p.retail_price,
            sl.quantity as current_stock,
            COALESCE(sd.units_sold, 0) as units_sold,
            COALESCE(sd.days_sold, 0) as days_sold,
            CASE 
                WHEN COALESCE(sd.units_sold, 0) = 0 THEN 0
                ELSE CAST(COALESCE(sd.units_sold, 0) AS FLOAT) / sl.quantity
            END as sales_to_stock_ratio,
            CASE 
                WHEN COALESCE(sd.units_sold, 0) = 0 THEN 999  -- High number for no sales
                ELSE CAST(sl.quantity AS FLOAT) / COALESCE(sd.units_sold, 1)
            END as stock_to_sales_ratio
        FROM stock_levels sl
        JOIN stores s ON sl.store_id = s.store_id
        JOIN products p ON sl.sku = p.sku
        LEFT JOIN sales_last_n_days sd ON sl.store_id = sd.store_id AND sl.sku = sd.sku
        WHERE s.is_active = 1
    )
    SELECT 
        *,
        CASE 
            -- Need stock: High sales velocity relative to stock
            WHEN units_sold > 0 
                 AND current_stock <= ?  -- Below critical threshold
                 OR (sales_to_stock_ratio > (1.0 / ?) AND current_stock > 0) -- Fast mover
            THEN 'NEEDS_STOCK'
            
            -- Excess stock: Slow sales relative to high stock
            WHEN current_stock >= ?  -- Above minimum stock threshold
                 AND (units_sold = 0 OR stock_to_sales_ratio > ?)  -- Slow moving
            THEN 'EXCESS_STOCK'
            
            ELSE 'OK'
        END as status,
        
        -- Calculate imbalance quantity
        CASE 
            -- How many more needed for a healthy buffer
            WHEN units_sold > 0 AND current_stock <= ? THEN ? - current_stock
            WHEN units_sold > 0 AND sales_to_stock_ratio > (1.0 / ?) THEN 
                CAST(units_sold * ? AS INTEGER) - current_stock  -- Target stock = sales * ratio
            -- How much excess to remove
            WHEN current_stock >= ? AND (units_sold = 0 OR stock_to_sales_ratio > ?) THEN 
                current_stock - CAST(units_sold * ? AS INTEGER)  -- Target stock = sales * ratio
            ELSE 0
        END as imbalance_qty,
        
        -- Additional metrics
        CASE 
            WHEN units_sold = 0 THEN 'No sales'
            WHEN sales_to_stock_ratio > 1 THEN 'Very Fast (1:' || ROUND(1/sales_to_stock_ratio, 1) || ')'
            WHEN sales_to_stock_ratio > 0.5 THEN 'Fast (1:' || ROUND(1/sales_to_stock_ratio, 1) || ')'
            WHEN sales_to_stock_ratio > 0.2 THEN 'Moderate (1:' || ROUND(1/sales_to_stock_ratio, 1) || ')'
            WHEN sales_to_stock_ratio > 0.1 THEN 'Slow (1:' || ROUND(1/sales_to_stock_ratio, 1) || ')'
            ELSE 'Very Slow (1:' || ROUND(1/sales_to_stock_ratio, 1) || ')'
        END as velocity_category
    FROM stock_info
    ORDER BY store_type DESC, store_name, 
             CASE WHEN status = 'NEEDS_STOCK' THEN 1 
                  WHEN status = 'EXCESS_STOCK' THEN 2 
                  ELSE 3 END,
             sales_to_stock_ratio DESC, current_stock
    """
    
    # Execute query with all parameters
    df = pd.read_sql(query, conn, params=(
        f"-{days_lookback}",           # days lookback
        low_stock_threshold,           # low stock threshold
        need_ratio_threshold,          # need ratio (1:X)
        excess_stock_threshold,        # excess min stock
        excess_ratio_threshold,        # excess ratio (1:X)
        low_stock_threshold,           # low stock threshold (imbalance)
        low_stock_threshold,           # low stock threshold (imbalance)
        need_ratio_threshold,          # need ratio (1:X) for target calc
        need_ratio_threshold,          # need ratio (1:X) for target calc
        excess_stock_threshold,        # excess min stock (imbalance)
        excess_ratio_threshold,        # excess ratio (1:X) for target calc
        excess_ratio_threshold         # excess ratio (1:X) for target calc
    ))
    
    # Rest of your analysis code continues...
    
    if df.empty:
        st.info("No stock data available. Please generate test data or upload CSV files.")
    else:
       
        # Visualizations
        st.markdown("---")
        st.subheader("📊 Inventory Overview")
        
        col1, col2 = st.columns(2)
        
        with col1:
            # Stock by store
            stock_by_store = pd.read_sql("""
                SELECT s.store_name, SUM(sl.quantity) as total_stock
                FROM stock_levels sl
                JOIN stores s ON sl.store_id = s.store_id
                GROUP BY s.store_name
                ORDER BY s.store_type DESC, total_stock DESC
            """, conn)
            
            if not stock_by_store.empty:
                fig1 = px.bar(stock_by_store, x='store_name', y='total_stock',
                              title='Total Stock by Store',
                              color='total_stock',
                              color_continuous_scale='viridis')
                st.plotly_chart(fig1, use_container_width=True)
        
        with col2:
            # Stock status breakdown
            status_counts = df['status'].value_counts().reset_index()
            status_counts.columns = ['Status', 'Count']
            
            if not status_counts.empty:
                fig2 = px.pie(status_counts, values='Count', names='Status',
                              title='Stock Status Distribution',
                              color='Status',
                              color_discrete_map={'NEEDS_STOCK': 'red', 
                                                 'EXCESS_STOCK': 'orange', 
                                                 'OK': 'green'})
                st.plotly_chart(fig2, use_container_width=True)

        # Store Dashboard Section
        # Separate online and physical stores
        online_stores = df[df['store_type'] == 'online']
        physical_stores = df[df['store_type'] == 'physical']

        st.markdown("---")
        st.subheader("🏬 Store Stock Analysis")

        # Display Online Store First
        if not online_stores.empty:
            st.markdown("### 🌐 Online Store")
            
            for store_id in online_stores['store_id'].unique():
                store_data = online_stores[online_stores['store_id'] == store_id]
                store_name = store_data['store_name'].iloc[0]
                
                needs = store_data[store_data['status'] == 'NEEDS_STOCK']
                excess = store_data[store_data['status'] == 'EXCESS_STOCK']
                
                with st.expander(f"**{store_name}** | Needs: {len(needs)} SKUs | Excess: {len(excess)} SKUs", expanded=False):
                    tab1, tab2 = st.tabs(["Stock Needs", "Excess Stock"])
                    
                    with tab1:
                        if not needs.empty:
                            needs_display = needs[['sku', 'style_name', 'size', 'current_stock', 'units_sold', 'days_sold']]
                            needs_display = needs_display.sort_values('units_sold', ascending=False)
                            st.dataframe(
                                needs_display.rename(columns={
                                    'sku': 'SKU',
                                    'style_name': 'Style',
                                    'size': 'Size',
                                    'current_stock': 'Current Stock',
                                    'units_sold': f'Sold ({days_lookback}d)',
                                    'days_sold': 'Days Sold'
                                }),
                                use_container_width=True
                            )
                        else:
                            st.success("✅ No urgent stock needs")
                    
                    with tab2:
                        if not excess.empty:
                            excess_display = excess[['sku', 'style_name', 'size', 'current_stock', 'units_sold', 'days_sold']]
                            excess_display = excess_display.sort_values('current_stock', ascending=False)
                            st.dataframe(
                                excess_display.rename(columns={
                                    'sku': 'SKU',
                                    'style_name': 'Style',
                                    'size': 'Size',
                                    'current_stock': 'Current Stock',
                                    'units_sold': f'Sold ({days_lookback}d)',
                                    'days_sold': 'Days Sold'
                                }),
                                use_container_width=True
                            )
                        else:
                            st.info("No excess stock identified")
        
        # Display Physical Stores
        if not physical_stores.empty:
            st.markdown("### 🏪 Physical Stores")
            
            for store_id in physical_stores['store_id'].unique():
                store_data = physical_stores[physical_stores['store_id'] == store_id]
                store_name = store_data['store_name'].iloc[0]
                
                needs = store_data[store_data['status'] == 'NEEDS_STOCK']
                excess = store_data[store_data['status'] == 'EXCESS_STOCK']
                
                with st.expander(f"**{store_name}** | Needs: {len(needs)} SKUs | Excess: {len(excess)} SKUs"):
                    col1, col2 = st.columns(2)
                    
                    with col1:
                        if not needs.empty:
                            st.subheader("🚨 Needs Stock")
                            needs_display = needs[['sku', 'style_name', 'size', 'current_stock', 'units_sold']]
                            needs_display = needs_display.sort_values('units_sold', ascending=False)
                            st.dataframe(
                                needs_display.head(10).rename(columns={
                                    'sku': 'SKU',
                                    'style_name': 'Style',
                                    'size': 'Size',
                                    'current_stock': 'Current',
                                    'units_sold': f'Sold ({days_lookback}d)'
                                }),
                                use_container_width=True
                            )
                        else:
                            st.success("✅ Stock levels OK")
                    
                    with col2:
                        if not excess.empty:
                            st.subheader("📦 Excess Stock")
                            excess_display = excess[['sku', 'style_name', 'size', 'current_stock', 'units_sold']]
                            excess_display = excess_display.sort_values('current_stock', ascending=False)
                            st.dataframe(
                                excess_display.head(10).rename(columns={
                                    'sku': 'SKU',
                                    'style_name': 'Style',
                                    'size': 'Size',
                                    'current_stock': 'Current',
                                    'units_sold': f'Sold ({days_lookback}d)'
                                }),
                                use_container_width=True
                            )
                        else:
                            st.info("No excess stock")
        
    # Transfer Recommendations
    st.markdown("---")
    st.subheader("🔄 Transfer Recommendations")

    try:
        # Advanced transfer logic using ratios
        transfer_query = """
        WITH sales_data AS (
            SELECT 
                store_id,
                sku,
                SUM(quantity) as units_sold
            FROM sales
            WHERE sale_date >= date('now', ? || ' days')
            GROUP BY store_id, sku
        ),
        store_performance AS (
            SELECT 
                sl.store_id,
                s.store_name,
                sl.sku,
                p.style_name,
                p.size,
                sl.quantity as current_stock,
                COALESCE(sd.units_sold, 0) as units_sold,
                -- Sales velocity ratio
                CASE 
                    WHEN COALESCE(sd.units_sold, 0) = 0 THEN 0
                    ELSE CAST(COALESCE(sd.units_sold, 0) AS FLOAT) / sl.quantity
                END as sales_velocity,
                -- Stock coverage ratio
                CASE 
                    WHEN COALESCE(sd.units_sold, 0) = 0 THEN 999
                    ELSE CAST(sl.quantity AS FLOAT) / COALESCE(sd.units_sold, 1)
                END as stock_coverage
            FROM stock_levels sl
            JOIN stores s ON sl.store_id = s.store_id
            JOIN products p ON sl.sku = p.sku
            LEFT JOIN sales_data sd ON sl.store_id = sd.store_id AND sl.sku = sd.sku
            WHERE s.is_active = 1
        ),
        -- Stores needing stock (high velocity relative to stock)
        needs_stores AS (
            SELECT *
            FROM store_performance
            WHERE units_sold > 0 
              AND (
                current_stock <= ?  -- Critically low
                OR sales_velocity > (1.0 / ?)  -- Fast mover (e.g., 1:3 ratio)
              )
        ),
        -- Stores with excess stock (slow moving relative to stock)
        excess_stores AS (
            SELECT *
            FROM store_performance
            WHERE current_stock >= ?  -- Minimum stock threshold
              AND (
                units_sold = 0  -- No sales
                OR stock_coverage > ?  -- Slow moving (e.g., 1:15 ratio)
              )
        )
        SELECT 
            n.store_name as needs_store,
            n.store_id as needs_store_id,
            n.sku,
            n.style_name,
            n.size,
            n.current_stock as needs_qty,
            n.units_sold as needs_sales,
            n.sales_velocity as needs_velocity,
            e.store_name as has_stock_store,
            e.store_id as has_store_id,
            e.current_stock as has_qty,
            e.units_sold as has_sales,
            e.stock_coverage as has_coverage,
            -- Calculate optimal transfer quantity
            CASE 
                -- If critically low, transfer enough to reach safety stock
                WHEN n.current_stock <= ? THEN ? - n.current_stock
                -- If fast mover, transfer based on sales velocity
                ELSE CAST(n.units_sold * ? - n.current_stock AS INTEGER)
            END as transfer_qty_needed,
            -- How much can be spared from source
            CASE 
                WHEN e.units_sold = 0 THEN e.current_stock - ?  -- Remove all excess
                ELSE e.current_stock - CAST(e.units_sold * ? AS INTEGER)  -- Keep target ratio
            END as transfer_qty_available,
            -- Final transfer quantity (min of needed and available)
            MIN(
                CASE 
                    WHEN n.current_stock <= ? THEN ? - n.current_stock
                    ELSE CAST(n.units_sold * ? - n.current_stock AS INTEGER)
                END,
                CASE 
                    WHEN e.units_sold = 0 THEN e.current_stock - ?
                    ELSE e.current_stock - CAST(e.units_sold * ? AS INTEGER)
                END
            ) as transfer_qty,
            -- Priority score (higher = more urgent)
            (n.sales_velocity * 10) + (CASE WHEN n.current_stock <= ? THEN 5 ELSE 0 END) as priority_score
        FROM needs_stores n
        JOIN excess_stores e ON n.sku = e.sku
        WHERE n.store_id != e.store_id
          AND n.current_stock < ?  -- Only if actually needs stock
          AND e.current_stock > ?  -- Only if actually has excess
        ORDER BY priority_score DESC, n.units_sold DESC
        LIMIT 25
        """
    
        transfers = pd.read_sql(transfer_query, conn, params=(
            f"-{days_lookback}",           # days lookback
            low_stock_threshold,           # low stock threshold
            need_ratio_threshold,          # need ratio (1:X)
            excess_stock_threshold,        # excess min stock
            excess_ratio_threshold,        # excess ratio (1:X)
            low_stock_threshold,           # low stock threshold (for transfer calc)
            low_stock_threshold,           # low stock threshold (for transfer calc)
            need_ratio_threshold,          # need ratio (for target stock)
            excess_stock_threshold,        # excess min stock (for transfer calc)
            excess_ratio_threshold,        # excess ratio (for target stock)
            low_stock_threshold,           # low stock threshold (for min calc)
            low_stock_threshold,           # low stock threshold (for min calc)
            need_ratio_threshold,          # need ratio (for target stock)
            excess_stock_threshold,        # excess min stock (for min calc)
            excess_ratio_threshold,        # excess ratio (for target stock)
            low_stock_threshold,           # priority bonus for critical
            low_stock_threshold,           # needs stock threshold
            excess_stock_threshold         # has excess threshold
        ))
    
        if not transfers.empty:
            # Filter out negative or zero transfer quantities
            transfers = transfers[transfers['transfer_qty'] > 0]
        
            if not transfers.empty:
                st.write(f"Found {len(transfers)} transfer opportunities")
            
                # Group by priority
                high_priority = transfers[transfers['priority_score'] >= 8]
                medium_priority = transfers[(transfers['priority_score'] >= 4) & (transfers['priority_score'] < 8)]
                low_priority = transfers[transfers['priority_score'] < 4]
            
                # Display by priority
                for priority_group, group_name, color in [
                    (high_priority, "🔥 High Priority", "red"),
                    (medium_priority, "⚠️ Medium Priority", "orange"),
                    (low_priority, "ℹ️ Low Priority", "blue")
                ]:
                    if not priority_group.empty:
                        st.markdown(f"### {group_name} ({len(priority_group)} transfers)")
                    
                        for idx, row in priority_group.iterrows():
                            with st.container():
                                cols = st.columns([3, 1, 3, 2])
                            
                                with cols[0]:
                                    st.markdown(f"**{row['needs_store']}** needs {row['transfer_qty']} of")
                                    st.markdown(f"`{row['sku']}` - {row['style_name']} (Size {row['size']})")
                                
                                    # Show velocity info
                                    if row['needs_velocity'] > 1:
                                        st.caption(f"🔥 Very fast mover: {row['needs_velocity']:.1f} sales per item")
                                    elif row['needs_velocity'] > 0.33:  # 1:3
                                        st.caption(f"⚡ Fast mover: {row['needs_velocity']:.1f} sales per item")
                                
                                    st.caption(f"Current: {row['needs_qty']}, Sold: {row['needs_sales']} in {days_lookback}d")
                            
                                with cols[1]:
                                    st.markdown("<h2 style='text-align: center; color: {};'>➡️</h2>".format(color), 
                                              unsafe_allow_html=True)
                            
                                with cols[2]:
                                    st.markdown(f"**{row['has_stock_store']}** has {row['has_qty']} available")
                                
                                    # Show coverage info
                                    if row['has_coverage'] > 20:
                                        st.caption(f"🐌 Very slow: 1 sale per {row['has_coverage']:.0f} items")
                                    elif row['has_coverage'] > 10:
                                        st.caption(f"🐢 Slow: 1 sale per {row['has_coverage']:.0f} items")
                                
                                    st.caption(f"Sold: {row['has_sales']} in {days_lookback}d")
                            
                                with cols[3]:
                                    # Create a unique key
                                    transfer_key = f"transfer_{row['needs_store_id']}_{row['has_store_id']}_{row['sku']}"
                                
                                    if st.button("📋 Create Transfer", key=transfer_key):
                                        cursor = conn.cursor()
                                        cursor.execute("""
                                            INSERT INTO transfer_recommendations 
                                            (from_store_id, to_store_id, sku, quantity, priority, reason)
                                            VALUES (?, ?, ?, ?, ?, ?)
                                        """, (
                                            row['has_store_id'],
                                            row['needs_store_id'],
                                            row['sku'],
                                            int(row['transfer_qty']),
                                            'high' if row['priority_score'] >= 8 else 
                                            'medium' if row['priority_score'] >= 4 else 'low',
                                            f"Velocity: {row['needs_velocity']:.2f} sales/item (needs) vs {row['has_coverage']:.1f} items/sale (has)"
                                        ))
                                        conn.commit()
                                        st.success(f"Transfer order created for {row['sku']}!")
                                        st.rerun()
            else:
                st.info("No transfer opportunities found with current thresholds")
        else:
            st.info("No transfer recommendations found with current thresholds")
        
    except Exception as transfer_error:
        st.warning(f"Could not generate transfer recommendations: {transfer_error}")
        import traceback
        st.code(traceback.format_exc())
                
    # Advanced Analytics Section
    st.markdown("---")
    st.subheader("📈 Advanced Sales Velocity Analytics")

    col1, col2 = st.columns(2)

    with col1:
        # Sales Velocity Distribution
        if 'sales_to_stock_ratio' in df.columns:
            velocity_df = df[df['units_sold'] > 0].copy()
            if not velocity_df.empty:
                velocity_df['velocity_ratio'] = 1 / velocity_df['sales_to_stock_ratio'].clip(lower=0.01)
                fig1 = px.histogram(
                    velocity_df, 
                    x='velocity_ratio',
                    nbins=30,
                    title='Sales-to-Stock Ratio Distribution',
                    labels={'velocity_ratio': 'Stock per Sale (1:X ratio)'},
                    color_discrete_sequence=['#636EFA']
                )
                fig1.add_vline(x=need_ratio_threshold, line_dash="dash", line_color="red", 
                              annotation_text=f"Need threshold (1:{need_ratio_threshold})")
                fig1.add_vline(x=excess_ratio_threshold, line_dash="dash", line_color="orange", 
                              annotation_text=f"Excess threshold (1:{excess_ratio_threshold})")
                st.plotly_chart(fig1, use_container_width=True)

    with col2:
        # Velocity vs Stock Scatter Plot
        if 'sales_to_stock_ratio' in df.columns and 'current_stock' in df.columns:
            scatter_df = df[df['units_sold'] > 0].copy()
            if not scatter_df.empty:
                scatter_df['log_velocity'] = np.log10(scatter_df['sales_to_stock_ratio'] + 0.01)
            
                fig2 = px.scatter(
                    scatter_df,
                    x='current_stock',
                    y='sales_to_stock_ratio',
                    color='velocity_category',
                    size='units_sold',
                    hover_data=['sku', 'style_name', 'size', 'store_name'],
                    title='Stock vs Sales Velocity',
                    labels={
                        'current_stock': 'Current Stock',
                        'sales_to_stock_ratio': 'Sales per Item',
                        'velocity_category': 'Velocity'
                    },
                    log_y=True
                )
            
                # Add threshold lines
                need_threshold_line = 1.0 / need_ratio_threshold
                excess_threshold_line = 1.0 / excess_ratio_threshold
            
                fig2.add_hline(y=need_threshold_line, line_dash="dash", line_color="green",
                              annotation_text="Fast mover threshold")
                fig2.add_hline(y=excess_threshold_line, line_dash="dash", line_color="red",
                              annotation_text="Slow mover threshold")
            
                st.plotly_chart(fig2, use_container_width=True)

    # Summary Metrics with Ratios
    st.markdown("---")
    st.subheader("📊 Velocity Summary")

    col1, col2, col3, col4 = st.columns(4)

    with col1:
        fast_movers = len(df[(df['sales_to_stock_ratio'] > (1.0 / need_ratio_threshold)) & (df['units_sold'] > 0)])
        st.metric("Fast Movers", fast_movers, 
                  help=f"Items selling faster than 1:{need_ratio_threshold} ratio")

    with col2:
        slow_movers = len(df[(df['stock_to_sales_ratio'] > excess_ratio_threshold) & (df['current_stock'] >= excess_stock_threshold)])
        st.metric("Slow Movers", slow_movers,
                  help=f"Items moving slower than 1:{excess_ratio_threshold} ratio")

    with col3:
        if 'sales_to_stock_ratio' in df.columns:
            avg_velocity = df[df['units_sold'] > 0]['sales_to_stock_ratio'].mean()
            if not pd.isna(avg_velocity):
                st.metric("Avg Sales per Item", f"{avg_velocity:.2f}",
                         help="Average sales per unit of stock")

    with col4:
        if 'stock_to_sales_ratio' in df.columns:
            avg_coverage = df[df['units_sold'] > 0]['stock_to_sales_ratio'].mean()
            if not pd.isna(avg_coverage):
                st.metric("Avg Stock Coverage", f"1:{avg_coverage:.1f}",
                         help="Average stock per sale")

except Exception as e:
    st.error(f"Error analyzing data: {e}")
    import traceback
    st.code(traceback.format_exc())
    st.info("Try generating test data first or check your CSV uploads")

# Footer
st.markdown("---")
st.caption(f"Last updated: {datetime.now().strftime('%Y-%m-%d %H:%M')} | Data refreshes on page reload")