# app.py
import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
import database as db
import data_generator as dg

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
        with st.spinner("Generating 90 days of test data..."):
            sales_df = dg.generate_sales_data(conn)
            st.success(f"Generated {len(sales_df):,} sales records")
    
    st.subheader("Data Upload")
    sales_file = st.file_uploader("Upload Sales CSV", type=['csv'])
    stock_file = st.file_uploader("Upload Stock CSV", type=['csv'])
    
    if sales_file:
        sales_df = pd.read_csv(sales_file)
        sales_df.to_sql('sales', conn, if_exists='append', index=False)
        st.success(f"Added {len(sales_df)} sales records")
    
    if stock_file:
        stock_df = pd.read_csv(stock_file)
        stock_df.to_sql('stock_levels', conn, if_exists='replace', index=False)
        st.success("Stock levels updated")
    
    st.subheader("Settings")
    days_lookback = st.slider("Sales lookback (days)", 7, 90, 14)
    low_stock_threshold = st.number_input("Low stock threshold", 1, 10, 2)
    excess_stock_threshold = st.number_input("Excess stock threshold", 5, 50, 5)

# Main Dashboard
st.title("👟 Footwear Retail Stock Balancer")
st.markdown("### Multi-Store Inventory Optimization Dashboard")

# Metrics row
col1, col2, col3, col4 = st.columns(4)

with col1:
    total_stock = pd.read_sql("SELECT SUM(quantity) FROM stock_levels", conn).iloc[0,0]
    st.metric("Total Stock", f"{total_stock:,}")

with col2:
    stores_with_low = pd.read_sql("""
        SELECT COUNT(DISTINCT store_id) 
        FROM stock_levels sl
        WHERE quantity <= ?
    """, conn, params=(low_stock_threshold,)).iloc[0,0]
    st.metric("Stores with Low Stock", stores_with_low)

with col3:
    potential_transfers = pd.read_sql("""
        SELECT COUNT(*) as count FROM (
            SELECT s1.store_id as needs_store, s2.store_id as has_stock
            FROM stock_levels s1
            JOIN stock_levels s2 ON s1.sku = s2.sku 
                AND s1.store_id != s2.store_id
                AND s1.quantity <= ?
                AND s2.quantity >= ?
        )
    """, conn, params=(low_stock_threshold, excess_stock_threshold)).iloc[0,0]
    st.metric("Potential Transfers", potential_transfers)

with col4:
    lost_sales = pd.read_sql("""
        SELECT COUNT(DISTINCT sku) 
        FROM sales 
        WHERE sale_date >= date('now', ? || ' days')
    """, conn, params=(f"-{days_lookback}",)).iloc[0,0]
    st.metric("Active SKUs (14 days)", lost_sales)

# Store Dashboard Section
st.markdown("---")
st.subheader("🏬 Store Stock Analysis")

# Get store data with calculations
query = """
WITH sales_summary AS (
    SELECT 
        store_id,
        sku,
        SUM(quantity) as units_sold,
        COUNT(DISTINCT sale_date) as days_sold
    FROM sales
    WHERE sale_date >= date('now', ? || ' days')
    GROUP BY store_id, sku
),
stock_current AS (
    SELECT 
        store_id,
        sku,
        quantity as current_stock,
        last_updated
    FROM stock_levels
),
combined AS (
    SELECT 
        s.store_id,
        st.store_name,
        st.store_type,
        s.sku,
        p.style_name,
        p.size,
        p.category,
        COALESCE(sm.units_sold, 0) as units_sold_14d,
        COALESCE(sm.days_sold, 0) as days_sold_14d,
        sc.current_stock,
        sc.last_updated,
        CASE 
            WHEN COALESCE(sm.units_sold, 0) > 0 AND sc.current_stock <= ? 
            THEN 'NEEDS_STOCK'
            WHEN COALESCE(sm.units_sold, 0) = 0 AND sc.current_stock >= ?
            THEN 'EXCESS_STOCK'
            ELSE 'OK'
        END as status,
        CASE 
            WHEN COALESCE(sm.units_sold, 0) > 0 AND sc.current_stock <= ?
            THEN sc.current_stock - ?  -- negative number showing deficit
            WHEN COALESCE(sm.units_sold, 0) = 0 AND sc.current_stock >= ?
            THEN sc.current_stock - ?  -- positive number showing excess
            ELSE 0
        END as imbalance_qty
    FROM stores st
    CROSS JOIN (SELECT DISTINCT sku FROM stock_levels) s
    LEFT JOIN products p ON s.sku = p.sku
    LEFT JOIN sales_summary sm ON s.store_id = sm.store_id AND s.sku = sm.sku
    LEFT JOIN stock_current sc ON s.store_id = sc.store_id AND s.sku = sc.sku
    WHERE sc.current_stock IS NOT NULL
    ORDER BY st.store_type DESC, st.store_name, p.style_name, p.size
)
SELECT * FROM combined;
"""

df = pd.read_sql(query, conn, params=(
    f"-{days_lookback}", 
    low_stock_threshold, 
    excess_stock_threshold,
    low_stock_threshold, low_stock_threshold,
    excess_stock_threshold, excess_stock_threshold
))

# Separate online and physical stores
online_stores = df[df['store_type'] == 'online']
physical_stores = df[df['store_type'] == 'physical']

# Display Online Store First
st.markdown("### 🌐 Online Store")

if not online_stores.empty:
    online_store_id = online_stores['store_id'].iloc[0]
    online_store_name = online_stores['store_name'].iloc[0]
    
    with st.expander(f"**{online_store_name}** - Summary", expanded=True):
        col1, col2 = st.columns(2)
        
        with col1:
            needs_stock = online_stores[online_stores['status'] == 'NEEDS_STOCK']
            if not needs_stock.empty:
                st.warning(f"**Needs Stock: {len(needs_stock)} SKUs**")
                needs_display = needs_stock[['sku', 'style_name', 'size', 'current_stock', 'units_sold_14d']]
                needs_display.columns = ['SKU', 'Style', 'Size', 'Current Stock', 'Sold (14d)']
                st.dataframe(needs_display, use_container_width=True)
            else:
                st.success("✅ No urgent stock needs")
        
        with col2:
            excess_stock = online_stores[online_stores['status'] == 'EXCESS_STOCK']
            if not excess_stock.empty:
                st.info(f"**Excess Stock: {len(excess_stock)} SKUs**")
                excess_display = excess_stock[['sku', 'style_name', 'size', 'current_stock', 'units_sold_14d']]
                excess_display.columns = ['SKU', 'Style', 'Size', 'Current Stock', 'Sold (14d)']
                st.dataframe(excess_display, use_container_width=True)
            else:
                st.success("✅ No excess stock identified")

# Display Physical Stores
st.markdown("### 🏪 Physical Stores")

for store_id in physical_stores['store_id'].unique():
    store_data = physical_stores[physical_stores['store_id'] == store_id]
    store_name = store_data['store_name'].iloc[0]
    
    needs_count = len(store_data[store_data['status'] == 'NEEDS_STOCK'])
    excess_count = len(store_data[store_data['status'] == 'EXCESS_STOCK'])
    
    with st.expander(f"**{store_name}** | Needs: {needs_count} SKUs | Excess: {excess_count} SKUs", expanded=True):
        tab1, tab2, tab3 = st.tabs(["Stock Needs", "Excess Stock", "Transfer Suggestions"])
        
        with tab1:
            needs = store_data[store_data['status'] == 'NEEDS_STOCK']
            if not needs.empty:
                needs_display = needs.sort_values('units_sold_14d', ascending=False)
                needs_display = needs_display[['sku', 'style_name', 'size', 'current_stock', 'units_sold_14d', 'days_sold_14d']]
                needs_display.columns = ['SKU', 'Style', 'Size', 'Current Stock', 'Units Sold (14d)', 'Days Sold']
                st.dataframe(needs_display, use_container_width=True)
                
                # Find potential sources for these SKUs
                for _, row in needs_display.iterrows():
                    sku = row['SKU']
                    sources = df[(df['sku'] == sku) & 
                                 (df['status'] == 'EXCESS_STOCK') & 
                                 (df['store_id'] != store_id)]
                    if not sources.empty:
                        st.caption(f"**Potential sources for {sku}:**")
                        source_display = sources[['store_name', 'current_stock']]
                        st.dataframe(source_display, hide_index=True)
            else:
                st.success("No urgent stock needs identified")
        
        with tab2:
            excess = store_data[store_data['status'] == 'EXCESS_STOCK']
            if not excess.empty:
                excess_display = excess.sort_values('current_stock', ascending=False)
                excess_display = excess_display[['sku', 'style_name', 'size', 'current_stock', 'units_sold_14d', 'days_sold_14d']]
                excess_display.columns = ['SKU', 'Style', 'Size', 'Current Stock', 'Units Sold (14d)', 'Days Sold']
                st.dataframe(excess_display, use_container_width=True)
            else:
                st.info("No excess stock identified")
        
        with tab3:
            # Generate transfer recommendations
            transfer_query = """
            SELECT 
                needs.store_name as needs_store,
                excess.store_name as has_stock_store,
                needs.sku,
                needs.style_name,
                needs.size,
                needs.current_stock as needs_qty,
                excess.current_stock as excess_qty,
                MIN(needs.low_stock_threshold - needs.current_stock, 
                    excess.current_stock - excess.excess_threshold) as transfer_qty
            FROM store_analysis needs
            JOIN store_analysis excess ON needs.sku = excess.sku 
                AND needs.store_id != excess.store_id
                AND needs.status = 'NEEDS_STOCK'
                AND excess.status = 'EXCESS_STOCK'
            WHERE needs.store_id = ?
            ORDER BY needs.units_sold_14d DESC
            LIMIT 10
            """
            
            transfers = pd.read_sql(transfer_query, conn, params=(store_id,))
            
            if not transfers.empty:
                st.write("**Recommended Transfers:**")
                for idx, row in transfers.iterrows():
                    st.write(f"➡️ Transfer **{row['transfer_qty']}** of {row['sku']} ({row['style_name']} - Size {row['size']})")
                    st.write(f"   From: {row['has_stock_store']} (has {row['excess_qty']})")
                    st.write(f"   To: {row['needs_store']} (has {row['needs_qty']})")
                    if st.button(f"Create Transfer Order {idx}", key=f"transfer_{store_id}_{idx}"):
                        st.success(f"Transfer order created for {row['sku']}")
            else:
                st.info("No transfer recommendations for this store")

# Visualization Section
st.markdown("---")
st.subheader("📊 Inventory Overview")

col1, col2 = st.columns(2)

with col1:
    # Stock distribution by store
    stock_by_store = pd.read_sql("""
        SELECT s.store_name, SUM(sl.quantity) as total_stock
        FROM stock_levels sl
        JOIN stores s ON sl.store_id = s.store_id
        GROUP BY s.store_name
        ORDER BY s.store_type DESC, total_stock DESC
    """, conn)
    
    fig1 = px.bar(stock_by_store, x='store_name', y='total_stock',
                  title='Total Stock by Store',
                  color='total_stock',
                  color_continuous_scale='Blues')
    st.plotly_chart(fig1, use_container_width=True)

with col2:
    # SKU velocity heatmap
    velocity_data = pd.read_sql("""
        SELECT 
            p.category,
            s.store_name,
            AVG(sm.units_sold) as avg_daily_sales
        FROM sales_summary sm
        JOIN products p ON sm.sku = p.sku
        JOIN stores s ON sm.store_id = s.store_id
        GROUP BY p.category, s.store_name
    """, conn)
    
    if not velocity_data.empty:
        fig2 = px.density_heatmap(velocity_data, x='store_name', y='category', 
                                  z='avg_daily_sales', title='Sales Velocity by Category & Store')
        st.plotly_chart(fig2, use_container_width=True)

# Export Functionality
st.markdown("---")
st.subheader("📤 Export Reports")

col1, col2, col3 = st.columns(3)

with col1:
    if st.button("📋 Export Stock Needs Report"):
        needs_report = df[df['status'] == 'NEEDS_STOCK']
        csv = needs_report.to_csv(index=False)
        st.download_button(
            label="Download CSV",
            data=csv,
            file_name=f"stock_needs_{datetime.now().strftime('%Y%m%d')}.csv",
            mime="text/csv"
        )

with col2:
    if st.button("📊 Export Transfer Recommendations"):
        # Generate comprehensive transfer recommendations
        transfer_recs = pd.read_sql("""
            -- Advanced transfer logic considering sales velocity and distance
            SELECT 
                n.store_name as to_store,
                e.store_name as from_store,
                n.sku,
                n.style_name,
                n.size,
                n.current_stock as to_current,
                e.current_stock as from_current,
                n.units_sold_14d as to_demand,
                e.units_sold_14d as from_demand,
                CASE 
                    WHEN n.units_sold_14d > 0 THEN 'HIGH_PRIORITY'
                    ELSE 'LOW_PRIORITY'
                END as priority,
                MIN(
                    ? - n.current_stock,  -- how many needed
                    e.current_stock - ?   -- how many can be spared
                ) as recommended_qty
            FROM combined n
            JOIN combined e ON n.sku = e.sku 
                AND n.store_id != e.store_id
                AND n.status = 'NEEDS_STOCK'
                AND e.status = 'EXCESS_STOCK'
            WHERE n.current_stock < ? 
                AND e.current_stock > ?
            ORDER BY priority DESC, n.units_sold_14d DESC
        """, conn, params=(low_stock_threshold, excess_stock_threshold, 
                          low_stock_threshold, excess_stock_threshold))
        
        csv = transfer_recs.to_csv(index=False)
        st.download_button(
            label="Download Transfer CSV",
            data=csv,
            file_name=f"transfer_recommendations_{datetime.now().strftime('%Y%m%d')}.csv",
            mime="text/csv"
        )

with col3:
    if st.button("🔄 Simulate Week of Sales"):
        # Simulate one week of sales to see how recommendations change
        with st.spinner("Simulating sales..."):
            # This would update stock levels based on sales patterns
            st.info("Feature: Sales simulation would update stock levels here")

# Footer
st.markdown("---")
st.caption(f"Last updated: {datetime.now().strftime('%Y-%m-%d %H:%M')} | Data refreshes automatically")