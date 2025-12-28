# app.py
import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
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
    
    st.subheader("Data Management")
    if st.button("🔄 Generate Test Data"):
        with st.spinner("Generating test data..."):
            # Generate products
            products_df = dg.generate_test_data()
            products_df.to_sql('products', conn, if_exists='replace', index=False)
            
            # Generate initial stock
            dg.clear_and_regenerate_data(conn)
            
            # Generate sales data
            dg.generate_sales_data(conn, days=30)
            
            st.success("Test data generated successfully!")
    
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
    
    st.subheader("Analysis Settings")
    days_lookback = st.slider("Sales lookback (days)", 7, 90, 14)
    low_stock_threshold = st.number_input("Low stock threshold", 1, 10, 2, help="Stock below this is considered low")
    excess_stock_threshold = st.number_input("Excess stock threshold", 5, 30, 8, help="Stock above this with no sales is excess")
    
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

# Store Dashboard Section
st.markdown("---")
st.subheader("🏬 Store Stock Analysis")

try:
    # Calculate stock needs and excess
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
            sl.quantity as current_stock,
            COALESCE(sd.units_sold, 0) as units_sold,
            COALESCE(sd.days_sold, 0) as days_sold
        FROM stock_levels sl
        JOIN stores s ON sl.store_id = s.store_id
        JOIN products p ON sl.sku = p.sku
        LEFT JOIN sales_last_n_days sd ON sl.store_id = sd.store_id AND sl.sku = sd.sku
        WHERE s.is_active = 1
    )
    SELECT 
        *,
        CASE 
            WHEN units_sold > 0 AND current_stock <= ? THEN 'NEEDS_STOCK'
            WHEN units_sold = 0 AND current_stock >= ? THEN 'EXCESS_STOCK'
            ELSE 'OK'
        END as status,
        CASE 
            WHEN units_sold > 0 AND current_stock <= ? THEN ? - current_stock
            WHEN units_sold = 0 AND current_stock >= ? THEN current_stock - ?
            ELSE 0
        END as imbalance_qty
    FROM stock_info
    ORDER BY store_type DESC, store_name, style_name, size
    """
    
    # Execute query
    df = pd.read_sql(query, conn, params=(
        f"-{days_lookback}", 
        low_stock_threshold, 
        excess_stock_threshold,
        low_stock_threshold, low_stock_threshold,
        excess_stock_threshold, excess_stock_threshold
    ))
    
    if df.empty:
        st.info("No stock data available. Please generate test data or upload CSV files.")
    else:
        # Separate online and physical stores
        online_stores = df[df['store_type'] == 'online']
        physical_stores = df[df['store_type'] == 'physical']
        
        # Display Online Store First
        if not online_stores.empty:
            st.markdown("### 🌐 Online Store")
            
            for store_id in online_stores['store_id'].unique():
                store_data = online_stores[online_stores['store_id'] == store_id]
                store_name = store_data['store_name'].iloc[0]
                
                needs = store_data[store_data['status'] == 'NEEDS_STOCK']
                excess = store_data[store_data['status'] == 'EXCESS_STOCK']
                
                with st.expander(f"**{store_name}** | Needs: {len(needs)} SKUs | Excess: {len(excess)} SKUs", expanded=True):
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

        # Transfer Recommendations (optional - you can comment this out if not needed yet)
        st.markdown("---")
        st.subheader("🔄 Transfer Recommendations")

        try:
            # Direct query without relying on CTE
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
            needs_stores AS (
                SELECT 
                    sl.store_id,
                    s.store_name,
                    sl.sku,
                    p.style_name,
                    p.size,
                    sl.quantity as current_stock,
                    COALESCE(sd.units_sold, 0) as units_sold
                FROM stock_levels sl
                JOIN stores s ON sl.store_id = s.store_id
                JOIN products p ON sl.sku = p.sku
                LEFT JOIN sales_data sd ON sl.store_id = sd.store_id AND sl.sku = sd.sku
                WHERE COALESCE(sd.units_sold, 0) > 0 
                    AND sl.quantity <= ?
                    AND s.is_active = 1
            ),
            excess_stores AS (
                SELECT 
                    sl.store_id,
                    s.store_name,
                    sl.sku,
                    p.style_name,
                    p.size,
                    sl.quantity as current_stock,
                    COALESCE(sd.units_sold, 0) as units_sold
                FROM stock_levels sl
                JOIN stores s ON sl.store_id = s.store_id
                JOIN products p ON sl.sku = p.sku
                LEFT JOIN sales_data sd ON sl.store_id = sd.store_id AND sl.sku = sd.sku
                WHERE COALESCE(sd.units_sold, 0) = 0 
                    AND sl.quantity >= ?
                    AND s.is_active = 1
            )
            SELECT 
                n.store_name as needs_store,
                n.sku,
                n.style_name,
                n.size,
                n.current_stock as needs_qty,
                n.units_sold as needs_sales,
                e.store_name as has_stock_store,
                e.current_stock as has_qty,
                e.units_sold as has_sales,
                MIN(? - n.current_stock, e.current_stock - ?) as transfer_qty
            FROM needs_stores n
            JOIN excess_stores e ON n.sku = e.sku
            WHERE n.store_id != e.store_id
            ORDER BY n.units_sold DESC, transfer_qty DESC
            LIMIT 20
            """
    
            transfers = pd.read_sql(transfer_query, conn, params=(
                f"-{days_lookback}",
                low_stock_threshold,
                excess_stock_threshold,
                low_stock_threshold, excess_stock_threshold
            ))
    
            if not transfers.empty:
                st.write(f"Found {len(transfers)} transfer opportunities")
        
                for idx, row in transfers.iterrows():
                    with st.container():
                        cols = st.columns([3, 1, 3, 2])
                        with cols[0]:
                            st.write(f"**{row['needs_store']}** needs {row['transfer_qty']} of")
                            st.write(f"`{row['sku']}` - {row['style_name']} (Size {row['size']})")
                            st.caption(f"Current: {row['needs_qty']}, Sold: {row['needs_sales']}")
                
                        with cols[1]:
                            st.markdown("<h2 style='text-align: center;'>➡️</h2>", unsafe_allow_html=True)
                
                        with cols[2]:
                            st.write(f"**{row['has_stock_store']}** has {row['has_qty']} available")
                            st.caption(f"Sold: {row['has_sales']}")
                
                        with cols[3]:
                            if st.button(f"Create Transfer", key=f"transfer_{idx}"):
                                # Extract store IDs from names (assuming format "Store Name" and we need "STORE01")
                                cursor = conn.cursor()
                        
                                # Get store IDs from names
                                cursor.execute("SELECT store_id FROM stores WHERE store_name = ?", (row['needs_store'],))
                                needs_store_id = cursor.fetchone()[0] if cursor.fetchone() else row['needs_store']
                        
                                cursor.execute("SELECT store_id FROM stores WHERE store_name = ?", (row['has_stock_store'],))
                                has_store_id = cursor.fetchone()[0] if cursor.fetchone() else row['has_stock_store']
                        
                                cursor.execute("""
                                    INSERT INTO transfer_recommendations 
                                    (from_store_id, to_store_id, sku, quantity, priority, reason)
                                    VALUES (?, ?, ?, ?, ?, ?)
                                """, (
                                    has_store_id,
                                    needs_store_id,
                                    row['sku'],
                                    int(row['transfer_qty']),
                                    'high' if row['needs_sales'] > 3 else 'medium',
                                    f"Store selling but low stock (sold {row['needs_sales']} units in {days_lookback} days)"
                                ))
                                conn.commit()
                                st.success("Transfer order created!")
            else:
                st.info("No transfer recommendations found with current thresholds")
        
        except Exception as transfer_error:
            st.warning(f"Could not generate transfer recommendations: {transfer_error}")
            st.info("You might need more data or adjust thresholds")
                
except Exception as e:
    st.error(f"Error analyzing data: {e}")
    import traceback
    st.code(traceback.format_exc())
    st.info("Try generating test data first or check your CSV uploads")

# Footer
st.markdown("---")
st.caption(f"Last updated: {datetime.now().strftime('%Y-%m-%d %H:%M')} | Data refreshes on page reload")