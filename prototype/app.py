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
import zipfile
from io import BytesIO
from fpdf import FPDF
import tempfile
import os
from typing import Set, Dict, List, Optional, Any, Union, cast

# Page config
st.set_page_config(
    page_title="Retail Stock Balancer",
    page_icon="👟",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Add responsive CSS
st.markdown("""
<style>
    /* Force columns to stack on smaller screens */
    @media (max-width: 1200px) {
        .stColumns {
            flex-direction: column !important;
        }
        .stColumn {
            width: 100% !important;
            min-width: 100% !important;
            margin-bottom: 20px;
        }
    }
    
    /* Make tables scrollable horizontally on small screens */
    @media (max-width: 768px) {
        .stDataFrame {
            overflow-x: auto;
        }
        .stDataFrame div[data-testid="stDataFrameResizable"] {
            max-width: 100%;
            overflow-x: auto;
        }
    }

    /* Responsive design for mobile */
    @media (max-width: 640px) {
        /* Stack columns on mobile */
        .stColumns {
            flex-direction: column !important;
        }
        
        /* Make sure columns take full width */
        .stColumn {
            width: 100% !important;
            min-width: 100% !important;
            margin-bottom: 10px;
        }
        
        /* Adjust font sizes */
        .stMarkdown, .stText, .stDataFrame {
            font-size: 14px;
        }
        
        /* Make buttons easier to tap */
        .stButton button {
            min-height: 44px;
            font-size: 16px;
        }
        
        /* Ensure tables are scrollable horizontally */
        .stDataFrame {
            overflow-x: auto;
        }
        
        /* Adjust metric displays */
        [data-testid="stMetricValue"] {
            font-size: 1.2rem !important;
        }
        
        [data-testid="stMetricLabel"] {
            font-size: 0.8rem !important;
        }
    }
    
    /* Tablet adjustments */
    @media (min-width: 641px) and (max-width: 1024px) {
        .stColumns {
            flex-wrap: wrap;
        }
        
        .stColumn {
            min-width: 200px;
        }
    }
</style>
""", unsafe_allow_html=True)

# Initialize session state
if 'selected_transfers' not in st.session_state:
    st.session_state.selected_transfers = set()
if 'show_qty_dialog' not in st.session_state:
    st.session_state.show_qty_dialog = False
if 'show_pdf_export' not in st.session_state:
    st.session_state.show_pdf_export = False
if 'show_complete_confirmation' not in st.session_state:
    st.session_state.show_complete_confirmation = False
if 'last_render_key' not in st.session_state:
    st.session_state.last_render_key = 0
if 'needs_rerun' not in st.session_state:
    st.session_state.needs_rerun = False
if 'active_tab' not in st.session_state:
    st.session_state.active_tab = "📊 Dashboard"
if 'active_subtab' not in st.session_state:
    st.session_state.active_subtab = "📊 Detailed"  # For the transfer tabs
if 'last_selected_tab' not in st.session_state:
    st.session_state.last_selected_tab = None
if 'previous_tab' not in st.session_state:
    st.session_state.previous_tab = "📊 Dashboard"

# Default settings
if 'default_days' not in st.session_state:
    st.session_state.default_days = 7
if 'default_low_stock' not in st.session_state:
    st.session_state.default_low_stock = 2
if 'default_need_ratio' not in st.session_state:
    st.session_state.default_need_ratio = 2
if 'default_excess_min' not in st.session_state:
    st.session_state.default_excess_min = 2
if 'default_slow_ratio' not in st.session_state:
    st.session_state.default_slow_ratio = 6

# Initialize database
conn = db.init_database()

# =========================================================
# HELPER FUNCTIONS
# =========================================================

def center_text(text):
    """Return HTML centered text"""
    return f"<div style='text-align: center'>{text}</div>"

def right_text(text):
    """Return HTML right-aligned text"""
    return f"<div style='text-align: right'>{text}</div>"

def set_table_height(num_rows: int, max_height: int) -> int:
    return min(35 * num_rows + 40, max_height)  # Estimate height based on number of rows
    
def safe_table_operation(conn: sqlite3.Connection, operation: Any, df: Optional[pd.DataFrame] = None, 
                        table_name: Optional[str] = None) -> Any:
    """Execute database operations with foreign keys temporarily disabled"""
    cursor = conn.cursor()
    cursor.execute("PRAGMA foreign_keys = OFF")
    try:
        result = operation(cursor)
        conn.commit()
        return result
    except Exception as e:
        conn.rollback()
        if df is not None and table_name is not None:
            st.error(f"Error in {table_name}: {e}")
        raise e
    finally:
        cursor.execute("PRAGMA foreign_keys = ON")
        conn.commit()

def bytes_to_str(b: Any) -> str:
    """Safely convert bytes to string"""
    if isinstance(b, bytes):
        return b.decode('utf-8', errors='ignore')
    return str(b)

def safe_int(value: Any) -> int:
    """Safely convert value to int"""
    try:
        if isinstance(value, (int, float)):
            return int(value)
        elif isinstance(value, str):
            return int(float(value)) if '.' in value else int(value)
        elif isinstance(value, bytes):
            return int(value.decode('utf-8', errors='ignore'))
        else:
            return 0
    except (ValueError, TypeError):
        return 0

def color_status(val):
    if '📦' in str(val):  # Pending transfer
        return 'background-color: #e3f2fd; color: #0d47a1'  # Light blue
    elif val == 'NEEDS_STOCK':
        return 'background-color: #ffcccc; color: #990000'
    elif val == 'EXCESS_STOCK':
        return 'background-color: #fff3cd; color: #856404'
    return ''                

def clean_for_pdf(df: pd.DataFrame) -> pd.DataFrame:
    """Remove or replace Unicode characters that cause encoding issues"""
    df_clean = df.copy()
    
    emoji_map = {
        '🔍': '', '🔴': '(High)', '🟡': '(Med)', '🟢': '(Low)',
        '✅': '[OK]', '❌': '[ERR]', '⚠️': '[WARN]', '📊': '', '📝': '',
        '📄': '', '📥': '', '📋': '', '↩️': '<-', '→': '->', '•': '-',
        '🔥': '[HOT]', '⚪': '( )', 'ℹ️': '(i)'
    }
    
    for col in df_clean.select_dtypes(include=['object']).columns:
        df_clean[col] = df_clean[col].astype(str).replace(emoji_map, regex=True)
        df_clean[col] = df_clean[col].apply(lambda x: bytes_to_str(x))
    
    return df_clean

def export_filtered_transfers_to_pdf(transfers_df: pd.DataFrame, source_filter: str, dest_filter: str) -> str:
    """Export filtered transfers to PDF"""
    pdf = FPDF()
    pdf.add_page()
    
    source_clean = source_filter.encode('ascii', 'ignore').decode('ascii') if source_filter != "All" else "All"
    dest_clean = dest_filter.encode('ascii', 'ignore').decode('ascii') if dest_filter != "All" else "All"
    
    pdf.set_font('Arial', 'B', 16)
    
    if source_filter != "All" and dest_filter != "All":
        title = f'TRANSFERS: {source_clean} to {dest_clean}'
    elif source_filter != "All":
        title = f'TRANSFERS FROM: {source_clean}'
    elif dest_filter != "All":
        title = f'TRANSFERS TO: {dest_clean}'
    else:
        title = 'ALL PENDING TRANSFERS'
    
    title = title.encode('ascii', 'ignore').decode('ascii')
    pdf.cell(190, 15, title, ln=True, align='C')
    pdf.ln(5)
    
    date_str = datetime.now().strftime("%Y-%m-%d %H:%M")
    pdf.set_font('Arial', '', 10)
    pdf.cell(190, 5, f'Generated: {date_str}', ln=True, align='R')
    pdf.ln(10)
    
    pdf.set_font('Arial', 'B', 12)
    pdf.set_fill_color(200, 200, 200)
    pdf.cell(190, 8, 'SUMMARY', ln=True, fill=True)
    pdf.ln(2)
    
    total_transfers = len(transfers_df)
    total_units = safe_int(transfers_df["quantity"].sum()) if "quantity" in transfers_df.columns else 0
    
    pdf.set_font('Arial', '', 10)
    pdf.cell(60, 6, 'Total Transfers:', 0)
    pdf.set_font('Arial', 'B', 10)
    pdf.cell(40, 6, str(total_transfers), 0)
    pdf.ln(6)
    
    pdf.cell(60, 6, 'Total Units:', 0)
    pdf.set_font('Arial', 'B', 10)
    pdf.cell(40, 6, str(total_units), 0)
    pdf.ln(6)
    
    if source_filter != "All" or dest_filter != "All":
        pdf.ln(5)
        pdf.set_font('Arial', 'B', 10)
        pdf.set_fill_color(240, 240, 240)
        pdf.cell(190, 6, 'FILTERS APPLIED:', ln=True, fill=True)
        pdf.set_font('Arial', '', 10)
        
        if source_filter != "All":
            pdf.cell(60, 6, f'Source: {source_clean}', ln=True)
        if dest_filter != "All":
            pdf.cell(60, 6, f'Destination: {dest_clean}', ln=True)
        pdf.ln(5)
    
    pdf.set_font('Arial', 'B', 8)
    pdf.set_fill_color(200, 200, 200)
    
    col_widths = [25, 25, 35, 35, 35, 10, 15, 15]
    headers = ['From', 'To', 'SKU', 'Supplier', 'Description', 'Size', 'color', 'Qty']
    
    for i, header in enumerate(headers):
        pdf.cell(col_widths[i], 8, header, border=1, fill=True, align='C')
    pdf.ln()
    
    pdf.set_font('Arial', '', 7)
    
    for idx, row in transfers_df.iterrows():
        if idx % 2 == 0:
            pdf.set_fill_color(255, 255, 255)
        else:
            pdf.set_fill_color(245, 245, 245)
        
        from_store = bytes_to_str(row.get('from_store', ''))[:15]
        to_store = bytes_to_str(row.get('to_store', ''))[:15]
        sku = bytes_to_str(row.get('sku', ''))
        supplier = bytes_to_str(row.get('supplier'))[:20]
        description = bytes_to_str(row.get('description', row.get('style_name', '')))[:25]
        color = bytes_to_str(row.get('color', ''))[:8]
        quantity = safe_int(row.get('quantity', 0))
        
        pdf.cell(col_widths[0], 6, from_store, border=1, fill=True)
        pdf.cell(col_widths[1], 6, to_store, border=1, fill=True)
        
        if len(sku) > 15:
            pdf.set_font('Arial', '', 6)
            pdf.cell(col_widths[2], 6, sku, border=1, fill=True)
            pdf.set_font('Arial', '', 7)
        else:
            pdf.cell(col_widths[2], 6, sku, border=1, fill=True)
        
        pdf.cell(col_widths[3], 6, supplier, border=1, fill=True)
        pdf.cell(col_widths[4], 6, description, border=1, fill=True)
        pdf.cell(col_widths[5], 6, str(row.get('size', '')), border=1, fill=True, align='C')
        pdf.cell(col_widths[6], 6, color, border=1, fill=True, align='C') 
        pdf.cell(col_widths[7], 6, str(quantity), border=1, fill=True, align='C')
        pdf.ln()
    
    pdf.set_y(-30)
    pdf.set_font('Arial', 'I', 8)
    pdf.cell(190, 5, f'Page {pdf.page_no()}', 0, 0, 'C')
    
    temp_file = tempfile.NamedTemporaryFile(delete=False, suffix='.pdf')
    pdf.output(temp_file.name)
    return temp_file.name

# =========================================================
# Data Mangetement Helpers - Creation & Upload
# =========================================================
def normalize_store_id(store_value: Any) -> str:
    """
    Normalize store identifier by:
    - Converting to string
    - Converting to uppercase
    - Removing all spaces, underscores, and special characters
    - Ensuring consistency across all data sources
    """
    if pd.isna(store_value):
        return ""
    
    # Convert to string and uppercase
    store_str = str(store_value).upper().strip()
    
    # Replace all special characters with nothing (remove them entirely)
    # This handles spaces, underscores, hyphens, slashes, etc.
    import re
    store_str = re.sub(r'[^A-Z0-9]', '', store_str)
    
    return store_str

def normalize_all_store_ids(stores_df: pd.DataFrame, stock_df: pd.DataFrame, sales_df: pd.DataFrame) -> tuple:
    """
    Normalize store IDs across all dataframes to ensure consistency
    Returns normalized versions of all dataframes
    """
    
    # Create a mapping dictionary to track original -> normalized
    store_id_map = {}
    
    # First, normalize stores_df
    if not stores_df.empty:
        stores_df['original_store_id'] = stores_df['store_id']  # Keep original for reference
        stores_df['store_id'] = stores_df['store_id'].apply(normalize_store_id)
        
        # Build mapping from normalized to original (for reference)
        for _, row in stores_df.iterrows():
            store_id_map[row['store_id']] = row.get('original_store_id', row['store_id'])
    
    # Normalize stock_df if it exists
    if not stock_df.empty and 'store_id' in stock_df.columns:
        stock_df['original_store_id'] = stock_df['store_id']
        stock_df['store_id'] = stock_df['store_id'].apply(normalize_store_id)
    
    # Normalize sales_df if it exists
    if not sales_df.empty and 'store_id' in sales_df.columns:
        sales_df['original_store_id'] = sales_df['store_id']
        sales_df['store_id'] = sales_df['store_id'].apply(normalize_store_id)
    
    return stores_df, stock_df, sales_df, store_id_map

def normalize_store_id(store_name: str) -> str:
    """Normalize store name to a consistent store_id format"""
    if pd.isna(store_name):
        return ""
    # Convert to string, uppercase, replace spaces and special chars with underscore
    store_id = str(store_name).upper().strip()
    # Replace spaces and special characters with underscore
    for char in [' ', '-', '/', '\\', '&', '.', ',']:
        store_id = store_id.replace(char, '_')
    # Remove any double underscores
    while '__' in store_id:
        store_id = store_id.replace('__', '_')
    # Remove trailing/leading underscores
    store_id = store_id.strip('_')
    return store_id

def extract_gender(text: str) -> str:
    """Extract gender from text"""
    if pd.isna(text):
        return 'U'
    text = str(text).upper()
    
    if any(word in text for word in ['MENS', 'MEN ', 'MALE', 'BOY', 'GENT']):
        return 'M'
    elif any(word in text for word in ['WOMENS', 'WOMEN ', 'FEMALE', 'LADIES', 'GIRL', 'LADY']):
        return 'F'
    elif any(word in text for word in ['KIDS', 'CHILD', 'INFANT', 'TODDLER', 'YOUTH', 'JUNIOR']):
        return 'C'
    else:
        return 'U'

def extract_category(text: str) -> str:
    """Extract main category from text"""
    if pd.isna(text):
        return 'General'
    text = str(text).upper()
    
    categories = [
        ('RUNNING', 'Running'),
        ('CASUAL', 'Casual'),
        ('FORMAL', 'Formal'),
        ('BOOTS', 'Boots'),
        ('SANDAL', 'Sandals'),
        ('SLIDE', 'Slides'),
        ('THONG', 'Thongs'),
        ('SNEAKER', 'Sneakers'),
        ('WALKING', 'Walking'),
        ('TRAIL', 'Trail'),
        ('BASKETBALL', 'Basketball'),
        ('TRAINER', 'Trainers'),
        ('CROSS', 'Cross Trainer'),
        ('FLIP', 'Flip Flops'),
        ('CLOG', 'Clogs'),
        ('LOAFER', 'Loafers'),
        ('DERBY', 'Derby'),
        ('OXFORD', 'Oxford'),
        ('CHELSEA', 'Chelsea Boots'),
        ('HIKE', 'Hiking'),
        ('SPORT', 'Sports'),
        ('DRESS', 'Dress Shoes'),
        ('WORK', 'Work Boots'),
        ('SAFETY', 'Safety Boots')
    ]
    
    for keyword, category in categories:
        if keyword in text:
            return category
    
    return 'General'

def extract_style_code(sku: str) -> str:
    """Extract style code from SKU"""
    if pd.isna(sku):
        return ''
    sku_str = str(sku)
    
    if '/' in sku_str:
        parts = sku_str.split('/')
        if len(parts) >= 2:
            return parts[1]
    elif '-' in sku_str and not sku_str.replace('-', '').isdigit():
        parts = sku_str.split('-')
        if len(parts) >= 2:
            return parts[0]
    
    return sku_str[:20]

def infer_location(store_name: str) -> str:
    """Infer location from store name"""
    store_upper = str(store_name).upper()
    if 'BENDIGO' in store_upper:
        return 'Bendigo, VIC'
    elif 'WAGGA' in store_upper:
        return 'Wagga Wagga, NSW'
    elif 'WANGARATTA' in store_upper:
        return 'Wangaratta, VIC'
    else:
        return ''

def display_column_mapping_ui(preview_df: pd.DataFrame, file_type: str, mapping_dict: dict):
    """Display mapping UI with all database tables and their columns"""
    
    # Define all database tables and their columns with special handling
    db_schema = {
        "stores": {
            "columns": [
                ("store_id", "🏬 Store ID", "column", True, "Select the column containing store identifiers (e.g., 'BENDIGO', 'WAGGA_WAGGA')"),
                ("store_name", "🏪 Store Name", "column", True, "Select the column containing store display names"),
                ("store_type", "📋 Store Type", "dropdown", False, "Select store type", 
                 {"options": ["physical", "online"], "default": "physical"}),
                ("location", "📍 Location", "column", False, "Select the column containing location info (city, state)"),
                ("is_active", "✅ Is Active", "dropdown", False, "Is this store active?", 
                 {"options": ["Yes", "No", "True", "False", "1", "0"], "default": "Yes"})
            ],
            "description": "Store information"
        },
        "products": {
            "columns": [
                ("sku", "🔖 SKU", "column", True, "Select the column containing SKU numbers (e.g., '12345', 'ABC-123')"),
                ("style_code", "🔑 Style Code", "column", False, "Select the column containing style codes"),
                ("style_name", "📝 Style Name", "column", False, "Select the column containing style names"),
                ("description", "📄 Description", "column", False, "Select the column containing product descriptions"),
                ("category", "🏷️ Category", "column", False, "Select the column containing product categories (e.g., 'Running', 'Casual')"),
                ("size", "📏 Size", "column", False, "Select the column containing sizes (e.g., 9, 10.5)"),
                ("gender", "⚥ Gender", "column", False, "Select the column containing gender info (M, F, U)"),
                ("color", "🎨 Colour", "column", False, "Select the column containing colours"),
                ("cost_price", "💰 Cost Price", "column", False, "Select the column containing cost prices"),
                ("retail_price", "💵 Retail Price", "column", False, "Select the column containing retail prices"),
                ("supplier", "🏭 Supplier", "column", False, "Select the column containing supplier names"),
                ("brand", "⭐ Brand", "column", False, "Select the column containing brand names"),
                ("product_type", "📦 Product Type", "column", False, "Select the column containing product types"),
                ("season", "🍂 Season", "column", False, "Select the column containing season info")
            ],
            "description": "Product catalog"
        },
        "stock_levels": {
            "columns": [
                ("store_id", "🏬 Store ID", "column", True, "Select the column containing store identifiers"),
                ("sku", "🔖 SKU", "column", True, "Select the column containing SKU numbers"),
                ("quantity", "🔢 Quantity", "column", True, "Select the column containing stock quantities"),
                ("last_updated", "🕒 Last Updated", "column", False, "Select the column containing last updated dates")
            ],
            "description": "Current stock levels by store"
        },
        "sales": {
            "columns": [
                ("sale_id", "🧾 Sale ID", "column", False, "Select the column containing sale/transaction identifiers"),
                ("store_id", "🏬 Store ID", "column", True, "Select the column containing store identifiers"),
                ("sku", "🔖 SKU", "column", True, "Select the column containing SKU numbers"),
                ("sale_date", "📅 Sale Date", "column", True, "Select the column containing sale dates"),
                ("quantity", "🔢 Quantity", "column", True, "Select the column containing quantities sold"),
                ("revenue", "💰 Revenue", "column", False, "Select the column containing revenue amounts")
            ],
            "description": "Sales transactions"
        }
    }
    
    # Get list of columns from the preview
    file_columns = ["--- Skip ---"] + list(preview_df.columns)
    
    st.markdown("##### Map Excel Columns to Database Tables")
    st.caption("For each database field, select the corresponding column from your Excel file or set a fixed value")
    
    # Create tabs for each database table
    table_tabs = st.tabs(list(db_schema.keys()))
    
    for i, (table_name, table_info) in enumerate(db_schema.items()):
        with table_tabs[i]:
            st.markdown(f"**{table_name}** - {table_info['description']}")
            st.markdown("---")
            
            # Create a container for the mapping table
            with st.container():
                # Header
                col1, col2, col3 = st.columns([1.5, 2, 0.5])
                with col1:
                    st.markdown("**Database Field**")
                with col2:
                    st.markdown("**Source**")
                with col3:
                    st.markdown("**Status**")
                
                st.markdown("---")
                
                # Create a row for each column in this table
                for field_info in table_info["columns"]:
                    field_name = field_info[0]
                    field_label = field_info[1]
                    field_type = field_info[2]
                    required = field_info[3]
                    help_text = field_info[4] if len(field_info) > 4 else ""
                    
                    # Create a unique key for this field across all tables
                    unique_field_key = f"{table_name}.{field_name}"
                    
                    col1, col2, col3 = st.columns([1.5, 2, 0.5])
                    
                    with col1:
                        # Show field name with required indicator
                        if required:
                            st.markdown(f"**{field_label}** <span style='color:red'>*</span>", unsafe_allow_html=True)
                        else:
                            st.markdown(f"**{field_label}**")
                        if help_text:
                            st.caption(help_text)
                    
                    with col2:
                        if field_type == "column":
                            # Column mapping dropdown
                            current_value = mapping_dict.get(unique_field_key, "--- Skip ---")
                            
                            # Suggest a match if not already set
                            if current_value == "--- Skip ---" and unique_field_key not in mapping_dict:
                                suggestion = suggest_match(field_name, field_label, preview_df)
                                if suggestion != "--- Skip ---":
                                    current_value = suggestion
                            
                            dropdown_key = f"{file_type}_map_{unique_field_key}_{hash(unique_field_key) % 10000}"
                            
                            # Find index of current value
                            if current_value in file_columns:
                                index = file_columns.index(current_value)
                            else:
                                index = 0
                            
                            selected = st.selectbox(
                                label=f"Map for {field_label}",
                                options=file_columns,
                                index=index,
                                key=dropdown_key,
                                label_visibility="collapsed",
                                help=f"Select the Excel column that contains the {field_label.lower()}"
                            )
                            
                            if selected != "--- Skip ---":
                                mapping_dict[unique_field_key] = selected
                            else:
                                if unique_field_key in mapping_dict:
                                    del mapping_dict[unique_field_key]
                        
                        elif field_type == "dropdown":
                            # Dropdown for predefined values
                            dropdown_config = field_info[5]
                            options = dropdown_config["options"]
                            default = dropdown_config["default"]
                            
                            dropdown_key = f"dropdown_{unique_field_key}"
                            current_value = mapping_dict.get(unique_field_key, default)
                            
                            selected = st.selectbox(
                                label=f"Set {field_label}",
                                options=options,
                                index=options.index(current_value) if current_value in options else 0,
                                key=f"{file_type}_{dropdown_key}_{hash(unique_field_key) % 10000}",
                                label_visibility="collapsed",
                                help=f"Select a value for {field_label}"
                            )
                            
                            mapping_dict[unique_field_key] = selected
                    
                    with col3:
                        # Show mapping status
                        if unique_field_key in mapping_dict:
                            if mapping_dict[unique_field_key] in preview_df.columns:
                                st.markdown("✅", help="Mapped to column")
                            else:
                                st.markdown("⚙️", help="Fixed value")
                        elif required:
                            st.markdown("❌", help="Required - not mapped")
                        else:
                            st.markdown("⬜", help="Optional - not mapped")
                    
                    st.markdown("---")
    
    # ============================================================================
    # PREVIEW MAPPED DATA
    # ============================================================================
    st.markdown("---")
    st.markdown("### 🔍 Preview Mapped Data")
    st.caption("This shows how your data will look after mapping (first 5 rows)")
    
    # Create preview of mapped data
    preview_mapped_data(preview_df, mapping_dict, file_type)

def preview_mapped_data(preview_df: pd.DataFrame, mapping_dict: dict, file_type: str):
    """Preview how the data will look after mapping"""
    
    # Group mappings by table
    table_mappings = {}
    for key, value in mapping_dict.items():
        if '.' in key:
            table, field = key.split('.', 1)
            if table not in table_mappings:
                table_mappings[table] = {}
            table_mappings[table][field] = value
    
    if not table_mappings:
        st.info("No mappings defined yet. Select columns above to see a preview.")
        return
    
    # Create tabs for each table that has mappings
    preview_tabs = st.tabs([f"📊 {table.title()}" for table in table_mappings.keys()])
    
    for i, (table_name, mappings) in enumerate(table_mappings.items()):
        with preview_tabs[i]:
            # Build preview data
            preview_data = []
            
            for idx in range(min(5, len(preview_df))):  # Show first 5 rows
                row = preview_df.iloc[idx]
                mapped_row = {}
                
                for field, source in mappings.items():
                    if source in preview_df.columns:  # It's a column
                        value = row[source] if pd.notna(row[source]) else ''
                        # Format based on field type
                        if field in ['quantity', 'revenue', 'cost_price', 'retail_price']:
                            try:
                                value = float(value) if value else 0
                            except:
                                value = 0
                        elif field in ['sale_date', 'last_updated']:
                            try:
                                if isinstance(value, datetime):
                                    value = value.strftime('%Y-%m-%d')
                                else:
                                    value = str(value)[:10]
                            except:
                                pass
                    else:  # It's a fixed value
                        value = source
                        # Convert boolean-like values
                        if field == 'is_active':
                            value = 1 if source in ['Yes', 'True', '1'] else 0
                    
                    mapped_row[field] = value
                
                preview_data.append(mapped_row)
            
            if preview_data:
                preview_df_mapped = pd.DataFrame(preview_data)
                
                # Apply formatting
                styled_preview = preview_df_mapped.style.format({
                    col: '{:.2f}' for col in preview_df_mapped.select_dtypes(include=['float']).columns
                })
                
                st.dataframe(
                    styled_preview,
                    use_container_width=True,
                    hide_index=True
                )
                
                # Show summary of mappings
                with st.expander("📋 Mapping Summary"):
                    col1, col2 = st.columns(2)
                    with col1:
                        st.markdown("**Mapped Columns:**")
                        for field, source in mappings.items():
                            if source in preview_df.columns:
                                st.markdown(f"• `{field}` ← **{source}**")
                    
                    with col2:
                        st.markdown("**Fixed Values:**")
                        for field, source in mappings.items():
                            if source not in preview_df.columns:
                                st.markdown(f"• `{field}` = **{source}**")
            else:
                st.info("No data to preview")

def suggest_match(field_name, field_label, preview_df):
    """Intelligently suggest column matches based on field name and content"""
    field_lower = field_name.lower()
    field_label_lower = field_label.lower()
    
    # Define priority patterns for each field type
    patterns = {
        'sku': {
            'primary': ['sku', 'stock code', 'product code', 'item code', 'article code'],
            'secondary': ['code', 'product', 'item', 'article', 'style'],
            'exclude': ['country', 'region', 'area', 'postcode', 'zip', 'tax', 'gst']
        },
        'store_id': {
            'primary': ['store id', 'store code', 'outlet id', 'location code', 'branch code'],
            'secondary': ['store', 'outlet', 'location', 'branch', 'shop'],
            'exclude': ['name', 'address', 'city', 'region', 'suburb']
        },
        'store_name': {
            'primary': ['store name', 'outlet name', 'location name', 'branch name', 'shop name'],
            'secondary': ['store', 'outlet', 'location', 'branch', 'shop'],
            'exclude': ['id', 'code', 'number', 'address']
        },
        'sale_date': {
            'primary': ['sale date', 'transaction date', 'invoice date', 'order date', 'date sold'],
            'secondary': ['date', 'created', 'timestamp', 'datetime', 'time'],
            'exclude': ['birth', 'updated', 'modified', 'delivery', 'ship']
        },
        'quantity': {
            'primary': ['quantity', 'qty', 'units', 'count', 'number sold', 'sold qty'],
            'secondary': ['sold', 'items', 'pieces', 'total'],
            'exclude': ['price', 'value', 'amount', 'cost']
        },
        'revenue': {
            'primary': ['revenue', 'sales', 'amount', 'total', 'value', 'price', 'sale amount'],
            'secondary': ['sale', 'invoice', 'transaction', 'total'],
            'exclude': ['cost', 'quantity', 'qty', 'tax']
        },
        'description': {
            'primary': ['description', 'product description', 'item description', 'short description'],
            'secondary': ['desc', 'details', 'name', 'title', 'product name'],
            'exclude': ['code', 'id', 'sku', 'number']
        },
        'size': {
            'primary': ['size', 'product size', 'item size', 'shoe size'],
            'secondary': ['sz'],
            'exclude': ['quantity', 'amount', 'length', 'width', 'height']
        },
        'color': {
            'primary': ['colour', 'color', 'product color', 'shoe color'],
            'secondary': ['col'],
            'exclude': ['code', 'family', 'group']
        },
        'brand': {
            'primary': ['brand', 'manufacturer', 'make', 'brand name'],
            'secondary': ['br'],
            'exclude': ['name', 'label']
        },
        'category': {
            'primary': ['category', 'product category', 'type', 'class', 'classification'],
            'secondary': ['cat', 'group', 'family'],
            'exclude': ['id', 'code', 'name']
        },
        'supplier': {
            'primary': ['supplier', 'vendor', 'provider', 'supplier name'],
            'secondary': ['sup'],
            'exclude': ['code', 'id']
        },
        'product_type': {
            'primary': ['product type', 'item type', 'style type', 'type'],
            'secondary': ['type', 'style'],
            'exclude': ['category', 'group']
        },
        'style_code': {
            'primary': ['style code', 'style', 'model code', 'model number'],
            'secondary': ['model', 'pattern', 'design'],
            'exclude': ['sku', 'product', 'item']
        }
    }
    
    # Get pattern config for this field, or use generic
    config = patterns.get(field_name, {
        'primary': [field_lower, field_label_lower],
        'secondary': [],
        'exclude': []
    })
    
    # Score each column
    scores = {}
    for col in preview_df.columns:
        col_str = str(col).lower().strip()
        score = 0
        
        # Check excluded terms first - if found, heavily penalize
        for term in config.get('exclude', []):
            if term in col_str:
                score -= 100
                break  # One exclusion is enough to disqualify
        
        # Primary matches (highest priority)
        for term in config.get('primary', []):
            if term in col_str:
                # Exact match gets highest score
                if col_str == term:
                    score += 200
                # Starts with or ends with term
                elif col_str.startswith(term + ' ') or col_str.endswith(' ' + term):
                    score += 150
                # Contains as whole word
                elif f' {term} ' in f' {col_str} ':
                    score += 120
                # Contains as substring
                else:
                    score += 100
                break
        
        # Secondary matches (if no primary match)
        if score == 0:
            for term in config.get('secondary', []):
                if term in col_str:
                    if col_str == term:
                        score += 80
                    elif col_str.startswith(term + ' ') or col_str.endswith(' ' + term):
                        score += 60
                    else:
                        score += 40
                    break
        
        # Check sample data for additional clues (only if score is positive or we want to boost)
        if score > 0:
            try:
                sample = preview_df[col].dropna().iloc[0] if not preview_df[col].dropna().empty else None
                if sample is not None:
                    sample_str = str(sample).lower()
                    
                    # For numeric fields
                    if field_name in ['quantity', 'revenue', 'cost_price', 'retail_price', 'size']:
                        if isinstance(sample, (int, float)) or sample_str.replace('.', '').replace('-', '').isdigit():
                            score += 30
                    # For date fields
                    elif field_name in ['sale_date', 'last_updated']:
                        try:
                            pd.to_datetime(sample)
                            score += 30
                        except:
                            pass
                    # For ID/SKU fields - check if it looks like a code (mixed letters/numbers)
                    elif field_name in ['sku', 'store_id', 'style_code']:
                        if any(c.isalpha() for c in sample_str) and any(c.isdigit() for c in sample_str):
                            score += 20
                        if len(sample_str) > 5:  # Codes are usually longer
                            score += 10
                    # For text fields
                    elif field_name in ['description', 'store_name', 'brand', 'supplier']:
                        if len(sample_str) > 3:  # Has meaningful content
                            score += 10
            except:
                pass
        
        scores[col] = score
    
    # Get the column with the highest score, if any
    if scores:
        best_match = max(scores.items(), key=lambda x: x[1])
        if best_match[1] > 50:  # Threshold for accepting match
            return best_match[0]
    
    return "--- Skip ---"

def check_mapping_complete(mapping_dict: dict, file_type: str) -> tuple:
    """Check if all required fields are mapped and return (is_complete, missing_fields)"""
    
    # Define required fields for each table
    required_fields = [
        "stores.store_id",
        "stores.store_name",
        "products.sku",
        "stock_levels.store_id",
        "stock_levels.sku",
        "stock_levels.quantity",
        "sales.store_id",
        "sales.sku",
        "sales.sale_date",
        "sales.quantity"
    ]
    
    mapped_fields = list(mapping_dict.keys())
    missing = [f for f in required_fields if f not in mapped_fields]
    
    # Special handling for dropdown fields - they're always considered mapped
    # since they have default values
    
    return len(missing) == 0, missing

def process_raw_files(sales_file, sales_sheet, sales_mapping, 
                     soh_file, soh_sheet, soh_mapping, conn):
    """Process raw Excel files and load into database following the CSV creation logic"""
    
    # Read the full data
    sales_df = pd.read_excel(sales_file, sheet_name=sales_sheet)
    soh_df = pd.read_excel(soh_file, sheet_name=soh_sheet)
    
    # sales_mapping is now {"table.column": value} where value can be:
    # - Excel column name (for column mappings)
    # - Fixed value (for dropdown fields)
    
    # Log warnings for duplicate records
    warnings = []

    # Debug: Show sales file info
    st.info(f"Sales file: {len(sales_df)} rows, {len(sales_df.columns)} columns")
    # st.write("Sales file columns:", list(sales_df.columns))

    # Check if the mapped columns exist
    for field, col in sales_mapping.items():
        if field.startswith('sales.') and col in sales_df.columns:
            st.write(f"✓ Field '{field}' mapped to column '{col}' - found")
        elif field.startswith('sales.'):
            st.warning(f"✗ Field '{field}' mapped to column '{col}' - NOT FOUND in sales file")
    
    # ============================================================================
    # STEP 1: Create STORES table with normalized IDs
    # ============================================================================

    # Extract raw store identifiers from both files
    raw_store_identifiers = []

    # Helper function to extract store identifiers from a dataframe based on mappings
    def extract_store_ids_from_df(df, mappings, source_name):
        identifiers = []
    
        # Check for store_id column mapping
        store_id_key = 'stores.store_id'
        if store_id_key in mappings:
            source = mappings[store_id_key]
            if source in df.columns:  # It's a column
                identifiers.extend(df[source].dropna().unique())
    
        # Check for store_name column mapping (might be used as ID)
        store_name_key = 'stores.store_name'
        if store_name_key in mappings:
            source = mappings[store_name_key]
            if source in df.columns:  # It's a column
                identifiers.extend(df[source].dropna().unique())
    
        # Check for outlet field (common in raw data)
        outlet_key = 'stores.outlet'
        if outlet_key in mappings:
            source = mappings[outlet_key]
            if source in df.columns:  # It's a column
                identifiers.extend(df[source].dropna().unique())
    
        return identifiers

    # Extract from sales file
    raw_store_identifiers.extend(extract_store_ids_from_df(sales_df, sales_mapping, 'sales'))

    # Extract from stock file
    raw_store_identifiers.extend(extract_store_ids_from_df(soh_df, soh_mapping, 'soh'))

    # Remove duplicates and None, and normalize
    normalized_store_map = {}  # normalized -> list of original values
    store_records = {}  # normalized -> store record

    for raw_id in raw_store_identifiers:
        if pd.isna(raw_id) or str(raw_id).strip() == '':
            continue
    
        raw_str = str(raw_id).strip()
        normalized = normalize_store_id(raw_str)
    
        if not normalized:
            continue
    
        # Track original values for this normalized ID
        if normalized not in normalized_store_map:
            normalized_store_map[normalized] = []
        if raw_str not in normalized_store_map[normalized]:
            normalized_store_map[normalized].append(raw_str)

    # Create store records for each normalized ID
    for normalized_id, original_values in normalized_store_map.items():
        # Use the most common original value as the store name
        from collections import Counter
        most_common_original = Counter(original_values).most_common(1)[0][0]
    
        # Get store_type from mappings
        store_type = "physical"  # Default
        if 'stores.store_type' in soh_mapping:
            store_type = soh_mapping['stores.store_type']
        elif 'stores.store_type' in sales_mapping:
            store_type = sales_mapping['stores.store_type']
    
        # Get is_active from mappings
        is_active = 1  # Default
        if 'stores.is_active' in soh_mapping:
            active_val = soh_mapping['stores.is_active']
            is_active = 1 if active_val in ['Yes', 'True', '1'] else 0
        elif 'stores.is_active' in sales_mapping:
            active_val = sales_mapping['stores.is_active']
            is_active = 1 if active_val in ['Yes', 'True', '1'] else 0
    
        # Infer location
        location = infer_location(most_common_original)
    
        store_records[normalized_id] = {
            'store_id': normalized_id,
            'store_name': most_common_original,
            'store_type': store_type,
            'location': location,
            'is_active': is_active
        }
    
        # Log warning if multiple different original values normalized to same ID
        if len(original_values) > 1:
            original_list = ", ".join(original_values)
            warnings.append(f"Store names '{original_list}' all normalized to the same ID: '{normalized_id}'")

    # Create stores dataframe
    stores_data = list(store_records.values())
    stores_df = pd.DataFrame(stores_data)

    # If no stores found from identifiers, check for fixed values in mappings
    if stores_df.empty:
        # Check if store_id is a fixed value
        if 'stores.store_id' in soh_mapping and soh_mapping['stores.store_id'] not in soh_df.columns:
            # It's a fixed value
            fixed_store_id = soh_mapping['stores.store_id']
            normalized_id = normalize_store_id(fixed_store_id)
        
            store_type = soh_mapping.get('stores.store_type', 'physical')
            is_active_val = soh_mapping.get('stores.is_active', 'Yes')
            is_active = 1 if is_active_val in ['Yes', 'True', '1'] else 0
        
            stores_data.append({
                'store_id': normalized_id,
                'store_name': fixed_store_id,
                'store_type': store_type,
                'location': infer_location(fixed_store_id),
                'is_active': is_active
            })
            stores_df = pd.DataFrame(stores_data)

    st.info(f"✅ Created {len(stores_df)} unique stores from {len(raw_store_identifiers)} raw identifiers")
    
    # ============================================================================
    # STEP 2: Create PRODUCTS table
    # ============================================================================
    
    # Build product catalog from both files
    products_data = []
    product_skus = set()
    
    # Track missing data for warnings
    missing_desc = 0
    missing_product_type = 0
    missing_size = 0
    missing_color = 0
    
    # Helper function to extract product from a dataframe row
    def extract_product_from_row(row, source_df, source_name, source_mapping):
        nonlocal missing_desc, missing_product_type, missing_size, missing_color
        
        # Get SKU - required
        sku = None
        if f'products.sku' in source_mapping:
            sku_source = source_mapping['products.sku']
            if sku_source in row.index and pd.notna(row[sku_source]):
                sku = str(row[sku_source]).strip()
        
        if not sku or sku in product_skus:
            return None
        
        # Helper to get value from either column or fixed value
        def get_field_value(field_name):
            field_key = f'products.{field_name}'
            if field_key in source_mapping:
                source = source_mapping[field_key]
                if source in row.index:  # It's a column
                    return row[source] if pd.notna(row[source]) else ''
                else:  # It's a fixed value
                    return source
            return ''
        
        # Get all fields
        style_code = get_field_value('style_code')
        style_name = get_field_value('style_name')
        description = get_field_value('description')
        category = get_field_value('category')
        
        # Handle size (numeric)
        size_val = get_field_value('size')
        try:
            size = float(size_val) if size_val else 0.0
        except:
            size = 0.0
        if size == 0.0:
            missing_size += 1
        
        gender = get_field_value('gender')
        color = get_field_value('color')
        if not color:
            missing_color += 1
        
        # Handle prices
        cost_price_val = get_field_value('cost_price')
        try:
            cost_price = float(cost_price_val) if cost_price_val else 0.0
        except:
            cost_price = 0.0
        
        retail_price_val = get_field_value('retail_price')
        try:
            retail_price = float(retail_price_val) if retail_price_val else 0.0
        except:
            retail_price = 0.0
        
        supplier = get_field_value('supplier')
        brand = get_field_value('brand')
        product_type = get_field_value('product_type')
        if not product_type:
            missing_product_type += 1
        
        season = get_field_value('season')
        
        # Auto-detect missing values
        if not description:
            missing_desc += 1
        
        if not category:
            category = extract_category(description + ' ' + product_type)
        
        if not gender:
            gender = extract_gender(description + ' ' + product_type)
        
        # If style_name is empty, try to generate from other fields
        if not style_name:
            if style_code:
                style_name = style_code
            elif product_type:
                style_name = product_type
            else:
                style_name = description[:50] if description else sku
        
        return {
            'sku': sku,
            'style_code': style_code,
            'style_name': style_name,
            'description': description,
            'category': category,
            'size': size,
            'gender': gender,
            'color': color,
            'cost_price': cost_price,
            'retail_price': retail_price,
            'supplier': supplier,
            'brand': brand,
            'product_type': product_type,
            'season': season
        }
    
    # Process SOH data for products
    if 'products.sku' in soh_mapping:
        sku_source = soh_mapping['products.sku']
        if sku_source in soh_df.columns:  # It's a column
            unique_soh_skus = soh_df[sku_source].dropna().unique()
            
            for sku in unique_soh_skus:
                if pd.isna(sku):
                    continue
                    
                sku_str = str(sku).strip()
                if not sku_str:
                    continue
                    
                # Get the first occurrence of this SKU for attributes
                sku_rows = soh_df[soh_df[sku_source] == sku]
                if sku_rows.empty:
                    continue
                
                product = extract_product_from_row(sku_rows.iloc[0], soh_df, 'soh', soh_mapping)
                if product:
                    products_data.append(product)
                    product_skus.add(sku_str)
    
    # Process sales data for additional products
    if 'products.sku' in sales_mapping:
        sku_source = sales_mapping['products.sku']
        if sku_source in sales_df.columns:  # It's a column
            sales_skus = set(sales_df[sku_source].dropna().astype(str).str.strip())
            missing_from_soh = sales_skus - product_skus
            
            if missing_from_soh:
                warnings.append(f"Found {len(missing_from_soh)} SKUs in sales not in SOH - adding to products")
                
                for sku in missing_from_soh:
                    if not sku:
                        continue
                    
                    # Get first occurrence of this SKU in sales
                    sku_rows = sales_df[sales_df[sku_source].astype(str).str.strip() == sku]
                    if sku_rows.empty:
                        continue
                    
                    product = extract_product_from_row(sku_rows.iloc[0], sales_df, 'sales', sales_mapping)
                    if product:
                        products_data.append(product)
    
    products_df = pd.DataFrame(products_data).drop_duplicates(subset=['sku'])
    
    # ============================================================================
    # STEP 3: Create STOCK_LEVELS table
    # ============================================================================
    
    stock_levels = []
    invalid_stock = 0
    
    if all(f in soh_mapping for f in ['stock_levels.store_id', 'stock_levels.sku', 'stock_levels.quantity']):
        store_id_source = soh_mapping['stock_levels.store_id']
        sku_source = soh_mapping['stock_levels.sku']
        qty_source = soh_mapping['stock_levels.quantity']
    
        # Optional fields
        last_updated_source = soh_mapping.get('stock_levels.last_updated')
    
        # Check if sources are columns
        required_cols = [store_id_source, sku_source, qty_source]
        if all(col in soh_df.columns for col in required_cols):
            # Create a set of valid normalized store IDs for quick lookup
            valid_store_ids = set(stores_df['store_id'].values)
        
            for _, row in soh_df.iterrows():
                # Get raw store identifier
                store_val = row[store_id_source]
                if pd.isna(store_val):
                    invalid_stock += 1
                    continue
            
                # Normalize the store identifier
                normalized_store_id = normalize_store_id(store_val)
            
                # Check if it exists in our stores
                if normalized_store_id not in valid_store_ids:
                    warnings.append(f"Stock record has store '{store_val}' (normalized to '{normalized_store_id}') not found in stores table")
                    invalid_stock += 1
                    continue
            
                # Get SKU
                sku_val = row[sku_source]
                if pd.isna(sku_val):
                    invalid_stock += 1
                    continue
                sku = str(sku_val).strip()
            
                # Get quantity
                try:
                    quantity = int(float(row[qty_source])) if pd.notna(row[qty_source]) else 0
                except:
                    quantity = 0
            
                if quantity <= 0:
                    invalid_stock += 1
                    continue
            
                # Get last_updated if mapped (optional)
                last_updated = None
                if last_updated_source and last_updated_source in soh_df.columns:
                    if pd.notna(row[last_updated_source]):
                        try:
                            if isinstance(row[last_updated_source], datetime):
                                last_updated = row[last_updated_source]
                            else:
                                last_updated = pd.to_datetime(row[last_updated_source])
                        except:
                            pass
            
                stock_levels.append({
                    'store_id': normalized_store_id,  # Use the normalized store ID
                    'sku': sku,
                    'quantity': quantity,
                    'last_updated': last_updated
                })
    
    # Aggregate stock levels
    if stock_levels:
        stock_df = pd.DataFrame(stock_levels)
        stock_df = stock_df.groupby(['store_id', 'sku'], as_index=False)['quantity'].sum()
    else:
        stock_df = pd.DataFrame(columns=['store_id', 'sku', 'quantity'])
    
    # ============================================================================
    # STEP 4: Create SALES table with better error handling
    # ============================================================================

    sales_records = []
    invalid_sales = 0
    sales_errors = {
        'missing_store': 0,
        'missing_sku': 0,
        'missing_date': 0,
        'missing_quantity': 0,
        'invalid_quantity': 0,
        'store_not_found': 0
    }

    st.info(f"Processing sales data...")  # Debug info

    if all(f in sales_mapping for f in ['sales.store_id', 'sales.sku', 'sales.sale_date', 'sales.quantity']):
        store_id_source = sales_mapping['sales.store_id']
        sku_source = sales_mapping['sales.sku']
        date_source = sales_mapping['sales.sale_date']
        qty_source = sales_mapping['sales.quantity']
    
        # Optional fields
        sale_id_source = sales_mapping.get('sales.sale_id')
        revenue_source = sales_mapping.get('sales.revenue')
    
        st.write(f"Debug - Column mappings:")  # Debug info
        st.write(f"  Store ID column: {store_id_source}")
        st.write(f"  SKU column: {sku_source}")
        st.write(f"  Date column: {date_source}")
        st.write(f"  Quantity column: {qty_source}")
    
        # Check if sources are columns in the dataframe
        missing_cols = []
        for col in [store_id_source, sku_source, date_source, qty_source]:
            if col not in sales_df.columns:
                missing_cols.append(col)
    
        if missing_cols:
            st.error(f"Missing columns in sales data: {missing_cols}")
            st.write("Available columns:", list(sales_df.columns))
        else:
            # Create a set of valid normalized store IDs for quick lookup
            valid_store_ids = set(stores_df['store_id'].values)
            st.write(f"Debug - Valid store IDs: {valid_store_ids}")  # Debug info
        
            # Sample first few rows to debug
            st.write("Debug - First 5 rows of sales data:")
            st.dataframe(sales_df.head(5))
        
            total_rows = len(sales_df)
            st.write(f"Debug - Total rows in sales file: {total_rows}")
        
            for idx, row in sales_df.iterrows():
                # if idx % 100 == 0 and idx > 0:  # Progress indicator
                    # st.write(f"Processed {idx}/{total_rows} rows...")
            
                # Get raw store identifier
                store_val = row[store_id_source]
                if pd.isna(store_val):
                    sales_errors['missing_store'] += 1
                    invalid_sales += 1
                    continue
            
                # Normalize the store identifier
                normalized_store_id = normalize_store_id(store_val)
            
                # Check if it exists in our stores
                if normalized_store_id not in valid_store_ids:
                    sales_errors['store_not_found'] += 1
                    invalid_sales += 1
                    if invalid_sales < 10:  # Show first few errors
                        st.warning(f"Row {idx}: Store '{store_val}' (normalized to '{normalized_store_id}') not found in stores table")
                    continue
            
                # Get SKU
                sku_val = row[sku_source]
                if pd.isna(sku_val):
                    sales_errors['missing_sku'] += 1
                    invalid_sales += 1
                    continue
                sku = str(sku_val).strip()
            
                # Get date
                date_val = row[date_source]
                if pd.isna(date_val):
                    sales_errors['missing_date'] += 1
                    invalid_sales += 1
                    continue
            
                try:
                    if isinstance(date_val, datetime):
                        date_str = date_val.strftime('%Y-%m-%d')
                    else:
                        # Try to parse the date
                        parsed_date = pd.to_datetime(date_val, errors='coerce')
                        if pd.isna(parsed_date):
                            raise ValueError(f"Could not parse date: {date_val}")
                        date_str = parsed_date.strftime('%Y-%m-%d')
                except Exception as e:
                    sales_errors['missing_date'] += 1
                    invalid_sales += 1
                    if invalid_sales < 10:
                        st.warning(f"Row {idx}: Invalid date format: '{date_val}' - {str(e)}")
                    continue
            
                # Get quantity
                try:
                    qty_val = row[qty_source]
                    if pd.isna(qty_val):
                        sales_errors['missing_quantity'] += 1
                        invalid_sales += 1
                        continue
                
                    quantity = int(float(qty_val))
                    
                    # if quantity <= 0:
                    #     sales_errors['invalid_quantity'] += 1
                    #     invalid_sales += 1
                    #     continue
                    
                except Exception as e:
                    sales_errors['invalid_quantity'] += 1
                    invalid_sales += 1
                    if invalid_sales < 10:
                        st.warning(f"Row {idx}: Invalid quantity: '{qty_val}' - {str(e)}")
                    continue
            
                # Get sale_id if mapped
                sale_id = ''
                if sale_id_source and sale_id_source in sales_df.columns:
                    if pd.notna(row[sale_id_source]):
                        sale_id = str(row[sale_id_source]).strip()
            
                # Get revenue if mapped
                revenue = quantity * 100.0  # Default
                if revenue_source and revenue_source in sales_df.columns:
                    if pd.notna(row[revenue_source]):
                        try:
                            revenue = float(row[revenue_source])
                        except:
                            pass
            
                sales_records.append({
                    'sale_id': sale_id,
                    'store_id': normalized_store_id,
                    'sku': sku,
                    'sale_date': date_str,
                    'quantity': quantity,
                    'revenue': round(revenue, 2)
                })
    else:
        st.error("Missing required sales mappings. Required fields: store_id, sku, sale_date, quantity")
        st.write("Current sales mappings:", sales_mapping)

    # After processing, show summary
    st.write(f"Debug - Sales processing complete:")
    st.write(f"  Valid records: {len(sales_records)}")
    st.write(f"  Invalid records: {invalid_sales}")
    st.write(f"  Error breakdown: {sales_errors}")

    # Create sales dataframe
    if sales_records:
        sales_clean_df = pd.DataFrame(sales_records)
        st.write(f"Debug - Created sales dataframe with {len(sales_clean_df)} rows")
        st.write("Debug - First 5 sales records:")
        st.dataframe(sales_clean_df.head())
    else:
        sales_clean_df = pd.DataFrame(columns=['sale_id', 'store_id', 'sku', 'sale_date', 'quantity', 'revenue'])
        st.warning("No valid sales records were created!")
    
        # Show sample of problematic rows
        st.write("Debug - Sample of raw sales data (first 10 rows):")
        st.dataframe(sales_df.head(10))
    # ============================================================================
    # Load into database
    # ============================================================================
    
    cursor = conn.cursor()
    cursor.execute("PRAGMA foreign_keys = OFF")
    
    try:
        # Clear existing data
        cursor.execute("DELETE FROM sales")
        cursor.execute("DELETE FROM stock_levels")
        cursor.execute("DELETE FROM products")
        cursor.execute("DELETE FROM stores")
        
        # Load stores
        stores_df.to_sql('stores', conn, if_exists='append', index=False)
        
        # Load products
        if not products_df.empty:
            products_df.to_sql('products', conn, if_exists='append', index=False)
        
        # Load stock levels
        if not stock_df.empty:
            stock_df.to_sql('stock_levels', conn, if_exists='append', index=False)
        
        # Load sales
        if not sales_clean_df.empty:
            sales_clean_df.to_sql('sales', conn, if_exists='append', index=False)
        
        conn.commit()
        
        # Prepare summary with warnings
        summary = {
            'stores': len(stores_df),
            'products': len(products_df),
            'stock_records': len(stock_df),
            'sales_records': len(sales_clean_df),
            'warnings': warnings,
            'invalid_stock': invalid_stock,
            'invalid_sales': invalid_sales,
            'missing_desc': missing_desc,
            'missing_product_type': missing_product_type,
            'missing_size': missing_size,
            'missing_color': missing_color,
            'raw_store_identifiers': len(raw_store_identifiers),
            'normalized_stores': len(stores_df)
        }
        
        return summary
        
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        cursor.execute("PRAGMA foreign_keys = ON")
        conn.commit()

def extract_stores(sales_df: pd.DataFrame, soh_df: pd.DataFrame) -> pd.DataFrame:
    """Extract unique stores from both dataframes"""
    
    # Get store IDs from both sources
    sales_stores = set()
    if 'store_id' in sales_df.columns:
        sales_stores = set(sales_df['store_id'].dropna().unique())
    
    soh_stores = set()
    if 'store_id' in soh_df.columns:
        soh_stores = set(soh_df['store_id'].dropna().unique())
    
    all_stores = sales_stores.union(soh_stores)
    
    # Create store records
    stores = []
    for store_id in all_stores:
        stores.append({
            'store_id': str(store_id).strip(),
            'store_name': str(store_id).strip(),  # Default to ID as name
            'store_type': 'physical',
            'location': '',
            'is_active': 1
        })
    
    return pd.DataFrame(stores)

def extract_products(sales_df: pd.DataFrame, soh_df: pd.DataFrame) -> pd.DataFrame:
    """Extract unique products from both dataframes"""
    
    # Get SKUs from both sources
    sales_skus = set()
    if 'sku' in sales_df.columns:
        sales_skus = set(sales_df['sku'].dropna().unique())
    
    soh_skus = set()
    if 'sku' in soh_df.columns:
        soh_skus = set(soh_df['sku'].dropna().unique())
    
    all_skus = sales_skus.union(soh_skus)
    
    # Create product records with minimal info
    products = []
    for sku in all_skus:
        products.append({
            'sku': str(sku).strip(),
            'style_code': '',
            'style_name': '',
            'description': '',
            'category': '',
            'size': 0,
            'gender': 'U',
            'color': '',
            'brand': '',
            'supplier': '',
            'retail_price': 0.0
        })
    
    return pd.DataFrame(products)

def prepare_stock_levels(soh_df: pd.DataFrame) -> pd.DataFrame:
    """Prepare stock levels data"""
    
    required_cols = ['store_id', 'sku', 'quantity']
    
    # Ensure all required columns exist
    for col in required_cols:
        if col not in soh_df.columns:
            raise ValueError(f"Required column '{col}' not found in stock data after mapping")
    
    # Clean and prepare
    stock_df = soh_df[required_cols].copy()
    
    # Convert to appropriate types
    stock_df['store_id'] = stock_df['store_id'].astype(str).str.strip()
    stock_df['sku'] = stock_df['sku'].astype(str).str.strip()
    stock_df['quantity'] = pd.to_numeric(stock_df['quantity'], errors='coerce').fillna(0).astype(int)
    
    # Remove rows with invalid data
    stock_df = stock_df[stock_df['store_id'] != '']
    stock_df = stock_df[stock_df['sku'] != '']
    stock_df = stock_df[stock_df['quantity'] > 0]
    
    return stock_df

def prepare_sales_data(sales_df: pd.DataFrame) -> pd.DataFrame:
    """Prepare sales data"""
    
    required_cols = ['store_id', 'sku', 'sale_date', 'quantity']
    optional_cols = ['revenue']
    
    # Ensure all required columns exist
    for col in required_cols:
        if col not in sales_df.columns:
            raise ValueError(f"Required column '{col}' not found in sales data after mapping")
    
    # Select available columns
    available_cols = required_cols.copy()
    for col in optional_cols:
        if col in sales_df.columns:
            available_cols.append(col)
    
    sales_clean = sales_df[available_cols].copy()
    
    # Clean data
    sales_clean['store_id'] = sales_clean['store_id'].astype(str).str.strip()
    sales_clean['sku'] = sales_clean['sku'].astype(str).str.strip()
    
    # Handle dates
    sales_clean['sale_date'] = pd.to_datetime(sales_clean['sale_date'], errors='coerce')
    
    # Handle quantity
    sales_clean['quantity'] = pd.to_numeric(sales_clean['quantity'], errors='coerce').fillna(0).astype(int)
    
    # Handle revenue if available
    if 'revenue' in sales_clean.columns:
        sales_clean['revenue'] = pd.to_numeric(sales_clean['revenue'], errors='coerce').fillna(0)
    
    # Remove invalid rows
    sales_clean = sales_clean.dropna(subset=['sale_date'])
    sales_clean = sales_clean[sales_clean['store_id'] != '']
    sales_clean = sales_clean[sales_clean['sku'] != '']
    sales_clean = sales_clean[sales_clean['quantity'] > 0]
    
    return sales_clean


# =========================================================
# SIDEBAR (Common across all tabs) - Simplified Settings
# =========================================================
with st.sidebar:
    st.image("https://img.icons8.com/fluency/96/null/sneakers.png", width=80)
    st.title("⚙️ Inventory Balancer")
    
    # Navigation
    st.markdown("---")
    st.subheader("📋 Navigation")

    selected = st.selectbox(
        "Go to",
        options=["📊 Dashboard", "🔄 Transfers", "📈 Reports", "📁 Data Management"],
        index=["📊 Dashboard", "🔄 Transfers", "📈 Reports", "📁 Data Management"].index(st.session_state.active_tab)
    )

    if selected != st.session_state.active_tab:
        st.session_state.active_tab = selected
        st.rerun()

    # =========================================================
    # ANALYSIS SETTINGS - All in one place
    # =========================================================
    st.markdown("---")
    st.subheader("📊 Analysis Settings")
    
    with st.expander("📅 Sales Lookback", expanded=True):
        st.session_state.default_days = st.slider(
            "Lookback period (days)", 
            7, 90, st.session_state.default_days,
            help="Number of days to analyze for sales velocity"
        )
    
    with st.expander("🚨 Stock Needs", expanded=True):
        col1, col2 = st.columns(2)
        with col1:
            st.session_state.default_low_stock = st.number_input(
                "Critical level", 
                1, 10, st.session_state.default_low_stock,
                help="Stock below this is critically low"
            )
        with col2:
            st.session_state.default_need_ratio = st.slider(
                "Velocity ratio (1:X)", 
                1, 5, st.session_state.default_need_ratio,
                help="Items selling faster than 1:X ratio need stock"
            )
    
    with st.expander("📦 Excess Stock", expanded=True):
        col1, col2 = st.columns(2)
        with col1:
            st.session_state.default_excess_min = st.number_input(
                "Minimum stock", 
                0, 20, st.session_state.default_excess_min,
                help="Only consider items with at least this many units"
            )
        with col2:
            st.session_state.default_slow_ratio = st.slider(
                "Slow ratio (1:X)", 
                1, 10, st.session_state.default_slow_ratio,
                help="Items selling slower than 1:X ratio are excess"
            )
    
    st.markdown("---")
    st.caption("Settings apply to all analysis")

# =========================================================
# MAIN CONTENT - TABBED INTERFACE
# =========================================================

# Dashboard Tab
if st.session_state.active_tab == "📊 Dashboard":
    st.title("👟 Retail Stock Balancer Dashboard")
    st.markdown("### Multi-Store Inventory Optimization")
    
    # Check if data exists
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM stock_levels")
    stock_count = cursor.fetchone()[0] or 0
    cursor.execute("SELECT COUNT(*) FROM stores")
    store_count = cursor.fetchone()[0] or 0
    
    if stock_count == 0 or store_count == 0:
        st.warning("⚠️ No data loaded. Please upload your CSV or Excel files in the **Data Management** tab first.")
        
        col1, col2 = st.columns(2)
        with col1:
            if st.button("📤 Go to Data Management", width='content'):
                st.session_state.active_tab = "📁 Data Management"
                st.rerun()
        with col2:
            if st.button("🎲 Load Test Data", width='content'):
                with st.spinner("Generating test data..."):
                    products_df = dg.generate_large_product_catalog()
                    products_df.to_sql('products', conn, if_exists='replace', index=False)
                    dg.generate_high_volume_stock(conn)
                    dg.generate_high_volume_sales(conn, days=30)
                    st.success("Test data loaded!")
                    st.rerun()
    else:
        # Quick metrics row - MOVED FROM SIDEBAR TO DASHBOARD
        try:
            col1, col2, col3, col4 = st.columns(4)
            
            with col1:
                result = pd.read_sql("SELECT COUNT(*) FROM stores", conn)
                store_count = result.iloc[0, 0] if not result.empty and result.iloc[0, 0] is not None else 0
                st.metric("Total Stores", f"{int(store_count):,}")
            
            with col2:
                result = pd.read_sql("SELECT COUNT(*) FROM products", conn)
                product_count = result.iloc[0, 0] if not result.empty and result.iloc[0, 0] is not None else 0
                st.metric("Total Products", f"{int(product_count):,}")
            
            with col3:
                result = pd.read_sql("SELECT SUM(quantity) FROM stock_levels", conn)
                total_stock = result.iloc[0, 0] if not result.empty and result.iloc[0, 0] is not None else 0
                st.metric("Total Stock", f"{int(total_stock):,}")
            
            with col4:
                result = pd.read_sql("SELECT COUNT(*) FROM transfer_recommendations WHERE status = 'pending'", conn)
                pending = result.iloc[0, 0] if not result.empty and result.iloc[0, 0] is not None else 0
                st.metric("Pending Transfers", f"{int(pending):,}")
        except Exception as e:
            st.error(f"Error loading metrics: {e}")
        
        # Main analysis query using settings from sidebar
        try:
            query = """
            WITH sales_last_n_days AS (
                SELECT store_id, sku, SUM(quantity) as units_sold, COUNT(DISTINCT sale_date) as days_sold
                FROM sales WHERE sale_date >= date('now', ? || ' days')
                GROUP BY store_id, sku
            ),
            stock_info AS (
                SELECT sl.store_id, s.store_name, s.store_type, sl.sku, p.style_name, p.description,
                       p.size, p.category, p.color, p.brand, p.retail_price, sl.quantity as current_stock,
                       COALESCE(sd.units_sold, 0) as units_sold, COALESCE(sd.days_sold, 0) as days_sold
                FROM stock_levels sl
                JOIN stores s ON sl.store_id = s.store_id
                JOIN products p ON sl.sku = p.sku
                LEFT JOIN sales_last_n_days sd ON sl.store_id = sd.store_id AND sl.sku = sd.sku
                WHERE s.is_active = 1
            )
            SELECT *,
                CASE 
                    WHEN units_sold > 0 AND current_stock <= ? THEN 'NEEDS_STOCK'
                    WHEN units_sold > 0 AND (CAST(units_sold AS FLOAT) / NULLIF(current_stock, 0)) > (1.0 / ?) THEN 'NEEDS_STOCK'
                    WHEN current_stock >= ? AND units_sold = 0 THEN 'EXCESS_STOCK'
                    WHEN current_stock >= ? AND (CAST(current_stock AS FLOAT) / NULLIF(units_sold, 1)) > ? THEN 'EXCESS_STOCK'
                    ELSE 'OK'
                END as status
            FROM stock_info
            ORDER BY store_type DESC, store_name, 
                     CASE WHEN status = 'NEEDS_STOCK' THEN 1 WHEN status = 'EXCESS_STOCK' THEN 2 ELSE 3 END,
                     units_sold DESC
            """
            
            df = pd.read_sql(query, conn, params=(
                f"-{st.session_state.default_days}", 
                st.session_state.default_low_stock, 
                st.session_state.default_need_ratio,
                st.session_state.default_excess_min, 
                st.session_state.default_excess_min, 
                st.session_state.default_slow_ratio
            ))
            
            if df.empty:
                st.info("No stock data available. Upload files in Data Management tab.")
            else:
                # Summary charts
                col1, col2 = st.columns(2)
                
                with col1:
                    stock_by_store = df.groupby('store_name')['current_stock'].sum().reset_index()
                    fig = px.bar(stock_by_store, x='store_name', y='current_stock', 
                                title='Stock by Store', color='current_stock',
                                color_continuous_scale='viridis')
                    st.plotly_chart(fig, width='content')
                
                with col2:
                    status_counts = df['status'].value_counts().reset_index()
                    status_counts.columns = ['Status', 'Count']
                    fig = px.pie(status_counts, values='Count', names='Status', title='Stock Status',
                                color='Status',
                                color_discrete_map={'NEEDS_STOCK': 'red', 'EXCESS_STOCK': 'orange', 'OK': 'green'})
                    st.plotly_chart(fig, width='content')

            # =========================================================
            # GET PENDING TRANSFERS FOR FLAGGING
            # =========================================================
            cursor = conn.cursor()
            cursor.execute("""
                SELECT from_store_id, to_store_id, sku, quantity 
                FROM transfer_recommendations 
                WHERE status = 'pending'
            """)
            pending_transfers = {}
            for row in cursor.fetchall():
                key = f"{row[0]}_{row[2]}"  # store_id_sku combination
                if key not in pending_transfers:
                    pending_transfers[key] = []
                pending_transfers[key].append({
                    'from': row[0],
                    'to': row[1],
                    'qty': row[3]
                })

            # Add pending transfer info to the main dataframe
            def get_pending_status(row):
                store_sku_key = f"{row['store_id']}_{row['sku']}"
                if store_sku_key in pending_transfers:
                    transfers = pending_transfers[store_sku_key]
                    # Format as: "📦 X to Y" or multiple lines
                    if len(transfers) == 1:
                        return f"📦 → {transfers[0]['to']} ({transfers[0]['qty']})"
                    else:
                        return f"📦 {len(transfers)} pending"
                return row['status']  # Keep original status if no pending

            df['display_status'] = df.apply(get_pending_status, axis=1)

            # Store analysis
            st.markdown("---")
            st.subheader("🏬 Store Analysis")

            # Get unique stores
            stores = df['store_name'].unique()

            if len(stores) == 0:
                st.warning("No stores found in data")
            else:
                # Initialize session state for view modes if not exists
                for store in stores:
                    if f'view_mode_{store}' not in st.session_state:
                        st.session_state[f'view_mode_{store}'] = 'summary'  # 'summary' or 'all'
    
                # Create tabs for each store
                store_tabs = st.tabs([f"🏪 {store}" for store in stores])
    
                for i, store in enumerate(stores):
                    with store_tabs[i]:
                        store_data = df[df['store_name'] == store].copy()
                        needs = store_data[store_data['status'] == 'NEEDS_STOCK']
                        excess = store_data[store_data['status'] == 'EXCESS_STOCK']
        
                        # Store header with counts and view toggle
                        col1, col2, col3, col4 = st.columns([2, 1, 1, 1])
                        with col1:
                            st.markdown(f"### {store}")
                        with col2:
                            st.markdown(f"🚨 Needs: **{len(needs)}**")
                        with col3:
                            st.markdown(f"📦 Excess: **{len(excess)}**")
                        with col4:
                            # View mode toggle button
                            view_mode_key = f'view_mode_{store}'
                            if view_mode_key not in st.session_state:
                                st.session_state[view_mode_key] = 'summary'
                
                            if st.session_state[view_mode_key] == 'summary':
                                if st.button("📋 Show All Products", key=f"show_all_btn_{store}"):
                                    st.session_state[view_mode_key] = 'all'
                                    st.rerun()
                            else:
                                if st.button("📊 Show Summary", key=f"show_summary_btn_{store}"):
                                    st.session_state[view_mode_key] = 'summary'
                                    st.rerun()
        
                        # ===== SEARCH SECTION - OUTSIDE ANY PRODUCT LOOP =====
                        with st.expander("🔍 Search Products in this Store", expanded=False):
                            search_col1, search_col2 = st.columns([3, 1])
            
                            with search_col1:
                                # This is now created only once per store
                                search_term = st.text_input(
                                    "Search by SKU, Description, Style, or Brand",
                                    placeholder="e.g., running, nike, black, 93333...",
                                    key=f"search_{store}"
                                ).lower().strip()
                
                            with search_col2:
                                search_type = st.selectbox(
                                    "Search in",
                                    options=["All Fields", "SKU", "Description", "Style", "Brand", "Category", "Colour"],
                                    key=f"search_type_{store}"
                                )
        
                        # ===== DETERMINE WHAT TO DISPLAY =====
                        view_mode = st.session_state.get(f'view_mode_{store}', 'summary')
        
                        # Initialize display_data
                        display_data = pd.DataFrame()
        
                        if search_term:
                            # We're in search mode - search across all store data
                            mask = pd.Series(False, index=store_data.index)
            
                            if search_type in ["All Fields", "SKU"]:
                                mask |= store_data['sku'].astype(str).str.lower().str.contains(search_term, na=False)
                            if search_type in ["All Fields", "Description"]:
                                if 'description' in store_data.columns:
                                    mask |= store_data['description'].astype(str).str.lower().str.contains(search_term, na=False)
                                mask |= store_data['style_name'].astype(str).str.lower().str.contains(search_term, na=False)
                            if search_type in ["All Fields", "Style"]:
                                mask |= store_data['style_name'].astype(str).str.lower().str.contains(search_term, na=False)
                            if search_type in ["All Fields", "Brand"]:
                                if 'brand' in store_data.columns:
                                    mask |= store_data['brand'].astype(str).str.lower().str.contains(search_term, na=False)
                            if search_type in ["All Fields", "Category"]:
                                if 'category' in store_data.columns:
                                    mask |= store_data['category'].astype(str).str.lower().str.contains(search_term, na=False)
                            if search_type in ["All Fields", "Colour"]:
                                if 'color' in store_data.columns:
                                    mask |= store_data['color'].astype(str).str.lower().str.contains(search_term, na=False)
            
                            display_data = store_data[mask].copy()
            
                            if display_data.empty:
                                st.warning(f"No products found matching '{search_term}'")
                            else:
                                st.info(f"Found {len(display_data)} products matching '{search_term}'")
                
                        elif view_mode == 'all':
                            # Show all products
                            display_data = store_data.copy()
                            st.info(f"Showing all {len(store_data)} products")
        
                        # ===== DISPLAY TABLE IF WE HAVE DATA =====
                        if not display_data.empty:
                            # Select columns to display
                            display_cols = ['sku', 'brand', 'description', 'style_name', 'category', 'size', 'color', 
                                           'current_stock', 'units_sold', 'display_status']
                            if 'retail_price' in display_data.columns:
                                display_cols.insert(7, 'retail_price')
            
                            available_cols = [col for col in display_cols if col in display_data.columns]
                            display_df = display_data[available_cols].copy()
            
                            # Truncate long descriptions
                            if 'description' in display_df.columns:
                                display_df['description'] = display_df['description'].astype(str).str[:60] + '...'
            
                            # Define column config
                            column_config = {
                                'sku': st.column_config.TextColumn('SKU', width='small'),
                                'brand': st.column_config.TextColumn('Brand', width='small'),
                                'description': st.column_config.TextColumn('Description', width='medium'),
                                'style_name': st.column_config.TextColumn('Style', width='small'),
                                'category': st.column_config.TextColumn('Category', width='small'),
                                'size': st.column_config.NumberColumn('Size', format="%.1f", width='small'),
                                'color': st.column_config.TextColumn('Colour', width='small'),
                                'retail_price': st.column_config.NumberColumn('Price', format="$%.2f", width='small'),
                                'current_stock': st.column_config.NumberColumn('Stock', width='small'),
                                'units_sold': st.column_config.NumberColumn(f'Sold ({st.session_state.default_days}d)', width='small'),
                                'display_status': st.column_config.TextColumn('Status', width='small')
                            }
            
                            # Apply styling
                            styled_df = display_df.style.map(color_status, subset=['display_status'] if 'display_status' in display_df.columns else [])
            
                            st.dataframe(
                                styled_df,
                                width='stretch',
                                hide_index=True,
                                height=set_table_height(len(display_df), 600),
                                column_config=column_config
                            )
        
                        # ===== SUMMARY VIEW (NEEDS & EXCESS) - ONLY SHOW IF NOT SEARCHING AND NOT IN ALL PRODUCTS VIEW =====
                        if not search_term and view_mode != 'all':
                            with st.container():
                                col1, col2 = st.columns(2)
                
                                with col1:
                                    if not needs.empty:
                                        st.markdown("**🚨 Top Needs**")
                                        top_needs = needs.nlargest(10, 'units_sold')[['sku', 'brand', 'description', 'size', 'current_stock', 'units_sold']]
                                        st.dataframe(
                                            top_needs, 
                                            width='stretch',
                                            hide_index=True,
                                            column_config={
                                                "sku": st.column_config.TextColumn("SKU", width="small"),
                                                "brand": st.column_config.TextColumn("Brand", width="small"),
                                                "description": st.column_config.TextColumn("Description", width="medium"),
                                                "size": st.column_config.NumberColumn("Size", width="small"),
                                                "current_stock": st.column_config.NumberColumn("Stock", width="small"),
                                                "units_sold": st.column_config.NumberColumn("Sold", width="small")
                                            }
                                        )
                                    else:
                                        st.success("✅ No urgent stock needs")
                
                                with col2:
                                    if not excess.empty:
                                        st.markdown("**📦 Top Excess**")
                                        top_excess = excess.nlargest(10, 'current_stock')[['sku', 'brand', 'description', 'size', 'current_stock', 'units_sold']]
                                        st.dataframe(
                                            top_excess, 
                                            width='stretch',
                                            hide_index=True,
                                            column_config={
                                                "sku": st.column_config.TextColumn("SKU", width="small"),
                                                "brand": st.column_config.TextColumn("Brand", width="small"),
                                                "description": st.column_config.TextColumn("Description", width="medium"),
                                                "size": st.column_config.NumberColumn("Size", width="small"),
                                                "current_stock": st.column_config.NumberColumn("Stock", width="small"),
                                                "units_sold": st.column_config.NumberColumn("Sold", width="small")
                                            }
                                        )
                                    else:
                                        st.info("No excess stock identified")
        
                        # Summary stats at the bottom
                        st.markdown("---")
                        col1, col2, col3, col4 = st.columns(4)
                        with col1:
                            st.metric("Total Products", len(store_data))
                        with col2:
                            total_stock = store_data['current_stock'].sum()
                            st.metric("Total Stock", f"{int(total_stock)}")
                        with col3:
                            total_sales = store_data['units_sold'].sum()
                            st.metric("Total Sales", f"{int(total_sales)}")
                        with col4:
                            needs_count = len(store_data[store_data['status'] == 'NEEDS_STOCK'])
                            st.metric("Needs Stock", needs_count)
                
        except Exception as e:
            st.error(f"Error analyzing data: {e}")
            import traceback
            st.code(traceback.format_exc())

# Transfers Tab
elif st.session_state.active_tab == "🔄 Transfers":
    st.title("🔄 Transfer Recommendations")
    
    # Track tab changes
    if 'last_main_tab' not in st.session_state:
        st.session_state.last_main_tab = "📊 Dashboard"
    if 'last_subtab' not in st.session_state:
        st.session_state.last_subtab = "📊 Detailed"
    
    # Check if main tab changed
    if st.session_state.last_main_tab != "🔄 Transfers":
        # Clear all selections when entering from another main tab
        selection_keys = [
            "available_detail", "created_detail", 
            "available_compact", "created_compact"
        ]
        for key in selection_keys:
            if key in st.session_state:
                del st.session_state[key]
        st.session_state.show_create_popup = False
        st.session_state.selected_transfer = None
        st.session_state.last_main_tab = "🔄 Transfers"

    try:
        # Use the main settings from sidebar
        days_lookback = st.session_state.default_days
        low_threshold = st.session_state.default_low_stock
        need_ratio = st.session_state.default_need_ratio
        excess_min = st.session_state.default_excess_min
        
        # Use excess_min as the buffer (keep that much in source stores)
        buffer = excess_min
        
        query = """
        WITH sales_data AS (
            SELECT store_id, sku, SUM(quantity) as units_sold
            FROM sales WHERE sale_date >= date('now', ? || ' days')
            GROUP BY store_id, sku
        ),
        store_perf AS (
            SELECT sl.store_id, s.store_name, sl.sku, p.style_name, p.description, p.size, p.color, p.brand, p.supplier,
                   sl.quantity as current_stock, COALESCE(sd.units_sold, 0) as units_sold
            FROM stock_levels sl
            JOIN stores s ON sl.store_id = s.store_id
            JOIN products p ON sl.sku = p.sku
            LEFT JOIN sales_data sd ON sl.store_id = sd.store_id AND sl.sku = sd.sku
            WHERE s.is_active = 1
        ),
        needs AS (
            SELECT * FROM store_perf
            WHERE units_sold > 0 
              AND (current_stock <= ? 
                   OR (CAST(units_sold AS FLOAT) / NULLIF(current_stock, 0)) > (1.0 / ?))
        ),
        excess AS (
            SELECT * FROM store_perf
            WHERE current_stock >= ? 
              AND units_sold = 0
        )
        SELECT 
            n.sku,
            n.description,
            n.style_name,
            n.size,
            n.color,
            n.brand,
            n.supplier,
            n.store_name as needs_store,
            n.store_id as needs_store_id,
            n.current_stock as needs_qty,
            n.units_sold as sales,
            e.store_name as source_store,
            e.store_id as source_store_id,
            e.current_stock as source_qty,
            CAST(n.units_sold * 2 AS INTEGER) as target_qty,
            CAST(n.units_sold * 2 - n.current_stock AS INTEGER) as needed,
            e.current_stock - ? as available,
            CASE 
                WHEN (CAST(n.units_sold AS FLOAT) / NULLIF(n.current_stock, 0)) > 1 THEN '🔥 Very Fast'
                WHEN (CAST(n.units_sold AS FLOAT) / NULLIF(n.current_stock, 0)) > 0.5 THEN '⚡ Fast'
                ELSE '📈 Normal'
            END as velocity
        FROM needs n
        JOIN excess e ON n.sku = e.sku
        WHERE n.store_id != e.store_id  -- This prevents self-transfers
          AND n.current_stock < CAST(n.units_sold * 2 AS INTEGER)
          AND e.current_stock > ?
        ORDER BY (n.units_sold * 2 - n.current_stock) DESC, n.units_sold DESC
        """
        
        transfers = pd.read_sql(query, conn, params=(
            f"-{days_lookback}", 
            low_threshold, 
            need_ratio,
            excess_min, 
            buffer, 
            excess_min
        ))
        
        if not transfers.empty:
            # Calculate max transfer
            transfers['max_transfer'] = transfers.apply(
                lambda row: min(row['needed'], row['available']), 
                axis=1
            )
            
            # =========================================================
            # FILTERS
            # =========================================================
            st.markdown("### 🔍 Filter Transfers")
            
            filter_cols = st.columns([1, 1])
            
            # In the filters section, add validation
            with filter_cols[0]:
                sources = ["All Sources"] + sorted(transfers['source_store'].unique().tolist())
                selected_source = st.selectbox("📍 Source Store", sources, key="transfer_source_filter")

            with filter_cols[1]:
                # Filter destinations to exclude the selected source (if not "All Sources")
                if selected_source != "All Sources":
                    available_dests = ["All Destinations"] + sorted(
                        [d for d in transfers['needs_store'].unique().tolist() if d != selected_source]
                    )
                else:
                    available_dests = ["All Destinations"] + sorted(transfers['needs_store'].unique().tolist())
    
                selected_dest = st.selectbox("🎯 Destination Store", available_dests, key="transfer_dest_filter")

            # Show warning if same store selected in both (shouldn't happen now, but just in case)
            if selected_source != "All Sources" and selected_dest == selected_source:
                st.warning("⚠️ Source and destination stores cannot be the same. Please select different stores.")
            
            # Apply filters
            filtered = transfers.copy()
            if selected_source != "All Sources":
                filtered = filtered[filtered['source_store'] == selected_source]
            if selected_dest != "All Destinations":
                filtered = filtered[filtered['needs_store'] == selected_dest]
            
            st.info(f"📊 Showing {len(filtered)} transfer opportunities")
            
            # =========================================================
            # CHECK EXISTING PENDING TRANSFERS
            # =========================================================
            cursor = conn.cursor()
            cursor.execute("""
                SELECT from_store_id, to_store_id, sku, quantity 
                FROM transfer_recommendations 
                WHERE status = 'pending'
            """)
            existing_transfers = {}
            for row in cursor.fetchall():
                key = f"{row[0]}_{row[1]}_{row[2]}"
                existing_transfers[key] = row[3]
            
            # =========================================================
            # BATCH ACTION BUTTONS
            # =========================================================
            available_count = 0
            pending_count = 0
            
            for idx, row in filtered.iterrows():
                transfer_key = f"{row['source_store_id']}_{row['needs_store_id']}_{row['sku']}"
                if transfer_key in existing_transfers:
                    pending_count += 1
                else:
                    available_count += 1
            
            batch_cols = st.columns([1, 1])
            
            with batch_cols[0]:
                if available_count > 0:
                    if st.button(f"✅ Create All ({available_count})", use_container_width=True, type="primary"):
                        created_count = 0
                        for idx, row in filtered.iterrows():
                            transfer_key = f"{row['source_store_id']}_{row['needs_store_id']}_{row['sku']}"
                            if transfer_key not in existing_transfers:
                                max_allowed = int(min(row['needed'], row['available'], row['max_transfer']))
                                needed_val = int(row['needed'])
                                
                                cursor.execute(
                                    """
                                    INSERT INTO transfer_recommendations 
                                    (from_store_id, to_store_id, sku, quantity, priority, reason)
                                    VALUES (?, ?, ?, ?, ?, ?)
                                    """,
                                    (
                                        row['source_store_id'],
                                        row['needs_store_id'],
                                        row['sku'],
                                        max_allowed,
                                        'high' if needed_val > 10 else 'medium' if needed_val > 5 else 'low',
                                        f"Need {needed_val} units to reach target"
                                    )
                                )
                                created_count += 1
                        
                        conn.commit()
                        st.success(f"✅ {created_count} transfers created!")
                        st.rerun()
            
            with batch_cols[1]:
                if pending_count > 0:
                    if st.button(f"↩️ Undo All ({pending_count})", use_container_width=True, type="secondary"):
                        deleted_count = 0
                        for idx, row in filtered.iterrows():
                            transfer_key = f"{row['source_store_id']}_{row['needs_store_id']}_{row['sku']}"
                            if transfer_key in existing_transfers:
                                cursor.execute(
                                    "DELETE FROM transfer_recommendations WHERE from_store_id = ? AND to_store_id = ? AND sku = ? AND status = 'pending'",
                                    (row['source_store_id'], row['needs_store_id'], row['sku'])
                                )
                                deleted_count += 1
                        
                        conn.commit()
                        st.success(f"⏪ {deleted_count} transfers undone!")
                        st.rerun()
            
            st.markdown("---")
            
            # =========================================================
            # RESPONSIVE TRANSFER TABLE
            # =========================================================
            st.markdown("### 📋 Transfer Opportunities")

            # Create simplified data for table
            table_data = []
            for idx, row in filtered.iterrows():
                transfer_key = f"{row['source_store_id']}_{row['needs_store_id']}_{row['sku']}"
                is_created = transfer_key in existing_transfers
    
                max_allowed = int(min(row['available'], row['max_transfer']))
                max_allowed = max(1, max_allowed)
    
                short_desc = row['description'][:25] + '...' if len(row['description']) > 25 else row['description']
    
                table_data.append({
                    'From': row['source_store'],
                    'To': row['needs_store'],
                    'SKU': row['sku'],
                    'Brand': row['supplier'],
                    'Style': row['style_name'],
                    'Product': short_desc,
                    'Size': f"{row['size']:.1f}",
                    'Colour': row['color'],
                    'Need': row['needed'],
                    'Avail': row['available'],
                    'Status': '✅' if is_created else '⚪',
                    'Qty': existing_transfers.get(transfer_key, max_allowed) if is_created else max_allowed,
                    'transfer_key': transfer_key,
                    'source_id': row['source_store_id'],
                    'dest_id': row['needs_store_id'],
                    'sku_code': row['sku'],
                    'is_created': is_created,  # Make sure this is always included
                    'idx': idx
                })

            # Create DataFrame
            table_df = pd.DataFrame(table_data)

            # Verify the column exists (optional debugging)
            if 'is_created' not in table_df.columns:
                st.error("Critical error: 'is_created' column missing from table_df")
                st.write("Columns:", table_df.columns.tolist())
            
            # =========================================================
            # TABBED VIEW FOR DIFFERENT SCREEN SIZES
            # =========================================================
            view_tab1, view_tab2, view_tab3 = st.tabs(["📊 Detailed", "📋 Compact", "🎯 Individual"])
            
            # Determine active subtab
            active_subtab = "📊 Detailed"
            with view_tab1:
                active_subtab = "📊 Detailed"
            with view_tab2:
                active_subtab = "📋 Compact"
            with view_tab3:
                active_subtab = "🎯 Individual"

            # Check if subtab changed
            if st.session_state.last_subtab != active_subtab:
                # Clear selections when switching subtabs
                selection_keys = [
                    "available_detail", "created_detail", 
                    "available_compact", "created_compact"
                ]
                for key in selection_keys:
                    if key in st.session_state:
                        del st.session_state[key]
                st.session_state.show_create_popup = False
                st.session_state.selected_transfer = None
                st.session_state.last_subtab = active_subtab

            with view_tab1:
                # Separate dataframes for created and not created
                created_df = table_df[table_df['is_created']].copy()
                not_created_df = table_df[~table_df['is_created']].copy()
    
                # Display not created transfers first (available to create)
                if not not_created_df.empty:
                    st.markdown("**📦 Available Transfers**")
            
                    # DETAILED VIEW - All columns
                    detailed_df = not_created_df[[
                        'From', 'To', 'SKU', 'Brand', 'Style', 'Product', 'Size', 'Colour', 
                        'Need', 'Avail'
                    ]].copy()

                    # Add selection with on_select
                    selection = st.dataframe(
                        detailed_df,
                        width='stretch',
                        hide_index=True,
                        on_select="rerun",
                        selection_mode="single-row",
                        key="available_transfers_detail"
                    )
                    st.info(f"📊 Showing {len(not_created_df)} available transfer opportunities")

                    # Handle selection
                    if selection and selection.selection and selection.selection.rows:
                        selected_idx = selection.selection.rows[0]
                        selected_row = not_created_df.iloc[selected_idx]
        
                        # Show selection info
                        col1, col2 = st.columns([3, 1])
                        with col1:
                            st.info(f"Selected: {selected_row['From']} → {selected_row['To']} | {selected_row['SKU']}")
                        with col2:
                            # Open popup button
                            if st.button("📋 Review Transfer", key=f"review_{selected_idx}", use_container_width=True):
                                st.session_state.show_create_popup = True
                                st.session_state.selected_transfer = selected_row.to_dict()
                                st.rerun()
        
                        # =========================================================
                        # CREATE TRANSFER POPUP
                        # =========================================================
                        if st.session_state.get('show_create_popup', False):
                            with st.popover("✏️ Create Transfer", use_container_width=True):
                                transfer = st.session_state.selected_transfer
                
                                st.markdown(f"### Transfer Details")

                                col1, col2 = st.columns(2)
                                with col1:
                                    st.markdown(f"**From:** {transfer['From']}")
                                    st.markdown(f"**SKU:** {transfer['SKU']}")
                                    st.markdown(f"**Brand:** {transfer['Brand']}")
                                    st.markdown(f"**Colour:** {transfer['Colour']}")
                                    
                                with col2:
                                    st.markdown(f"**To:** {transfer['To']}")
                                    st.markdown(f"**Style:** {transfer['Style']}")
                                    st.markdown(f"**Product:** {transfer['Product']}")
                                    st.markdown(f"**Size:** {transfer['Size']}")
                                
                                col1, col2 = st.columns(2)
                                with col1:
                                    st.metric("Need", transfer['Need'])
                                with col2:
                                    st.metric("Available", transfer['Avail'])
                
                                # Editable quantity
                                max_qty = transfer['Avail']
                                rec_qty = min(transfer['Need'], transfer['Avail'])
                                qty = st.number_input(
                                    "Quantity to transfer",
                                    min_value=1,
                                    max_value=max_qty,
                                    value=rec_qty,
                                    key="detail_popup_qty"
                                )
                
                                # Optional reason/notes
                                reason = st.text_input("Reason (optional)", placeholder="e.g., Stock transfer", key="create_reason_input")

                                col1, col2 = st.columns(2)
                                with col1:
                                    if st.button("✅ Confirm Transfer", use_container_width=True, type="primary"):
                                        # Get the actual row data from table_df using the stored indices
                                        source_id = transfer['source_id']
                                        dest_id = transfer['dest_id']
                                        sku_code = transfer['sku_code']
                        
                                        # In the individual transfer creation section, add a check
                                        if source_id == dest_id:
                                            st.warning(f"Cannot create transfer: Source and destination are the same store")
                                        else:
                                            cursor.execute(
                                                """
                                                INSERT INTO transfer_recommendations 
                                                (from_store_id, to_store_id, sku, quantity, priority, reason)
                                                VALUES (?, ?, ?, ?, ?, ?)
                                                """,
                                                (
                                                    source_id,
                                                    dest_id,
                                                    sku_code,
                                                    qty,
                                                    'medium',
                                                    reason if reason else f"Need {transfer['Need']} units"
                                                )
                                            )
                                            conn.commit()
                        
                                            # Clear popup state
                                            st.session_state.show_create_popup = False
                                            st.session_state.selected_transfer = None
                        
                                            st.success("✅ Transfer created!")
                                            st.rerun()
                
                                with col2:
                                    if st.button("❌ Cancel", use_container_width=True):
                                        st.session_state.show_create_popup = False
                                        st.session_state.selected_transfer = None
                                        st.rerun()
    
                # Display created transfers
                if not created_df.empty:
                    st.markdown("**✅ Created Transfers**")
        
                    # DETAILED VIEW - All columns
                    detailed_df = created_df[[
                                        'From', 'To', 'SKU', 'Brand', 'Style', 'Product', 'Size', 'Colour', 
                                        'Avail', 'Qty'
                                    ]].copy()
                    detailed_df = detailed_df.rename(columns={'Qty': 'Trns'})

                    # Add selection with on_select
                    selection = st.dataframe(
                        detailed_df,
                        width='stretch',
                        hide_index=True,
                        on_select="rerun",
                        selection_mode="single-row",
                        key="created_transfers_detail"
                    )
                    st.success(f"📊 Showing {len(created_df)} created transfers")

                    # Handle selection
                    if selection.selection.rows:
                        selected_idx = selection.selection.rows[0]
                        original_idx = created_df.iloc[selected_idx].name
                        selected_row = table_df.loc[original_idx]
            
                        col1, col2 = st.columns([3, 1])
                        with col1:
                            st.info(f"Selected: {selected_row['From']} → {selected_row['To']} | {selected_row['SKU']}")
                        with col2:
                            if st.button("↩️ Undo Selected", key="undo_selected", use_container_width=True):
                                cursor.execute(
                                    "DELETE FROM transfer_recommendations WHERE from_store_id = ? AND to_store_id = ? AND sku = ? AND status = 'pending'",
                                    (selected_row['source_id'], selected_row['dest_id'], selected_row['sku_code'])
                                )
                                conn.commit()
                                st.success("⏪ Transfer undone!")
                                st.rerun()

            with view_tab2:
                # Separate dataframes for created and not created
                # When using is_created, add a check to prevent KeyError
                if 'is_created' in table_df.columns:
                    created_df = table_df[table_df['is_created']].copy() if not table_df[table_df['is_created']].empty else pd.DataFrame()
                    not_created_df = table_df[~table_df['is_created']].copy() if not table_df[~table_df['is_created']].empty else pd.DataFrame()
                else:
                    st.error("Data structure error: Please refresh the page")
                    created_df = pd.DataFrame()
                    not_created_df = pd.DataFrame()
    
                # Display not created transfers first (available to create)
                if not not_created_df.empty:
                    st.markdown("**📦 Available Transfers**")
        
                    # Prepare display dataframe without action buttons
                    display_df = not_created_df[['From', 'To', 'SKU', 'Product', 'Avail', 'Need']].copy()
        
                    # Add selection with on_select
                    selection = st.dataframe(
                        display_df,
                        width='stretch',
                        hide_index=True,
                        on_select="rerun",
                        selection_mode="single-row",
                        key="available_transfers_compact"
                    )
                    st.info(f"📊 Showing {len(not_created_df)} available transfer opportunities")

                   # Handle selection
                    if selection and selection.selection and selection.selection.rows:
                        selected_idx = selection.selection.rows[0]
                        selected_row = not_created_df.iloc[selected_idx]
        
                        # Show selection info
                        col1, col2 = st.columns([3, 1])
                        with col1:
                            st.info(f"Selected: {selected_row['From']} → {selected_row['To']} | {selected_row['SKU']}")
                        with col2:
                            # Open popup button
                            if st.button("📋 Review Transfer", key=f"review_{selected_idx}", use_container_width=True):
                                st.session_state.show_create_popup = True
                                st.session_state.selected_transfer = selected_row.to_dict()
                                st.rerun()
        
                        # =========================================================
                        # CREATE TRANSFER POPUP
                        # =========================================================
                        if st.session_state.get('show_create_popup', False):
                            with st.popover("✏️ Create Transfer", use_container_width=True):
                                transfer = st.session_state.selected_transfer
                
                                st.markdown(f"### Transfer Details")

                                col1, col2 = st.columns(2)
                                with col1:
                                    st.markdown(f"**From:** {transfer['From']}")
                                    st.markdown(f"**SKU:** {transfer['SKU']}")
                                    st.markdown(f"**Brand:** {transfer['Brand']}")
                                    st.markdown(f"**Colour:** {transfer['Colour']}")
                                    
                                with col2:
                                    st.markdown(f"**To:** {transfer['To']}")
                                    st.markdown(f"**Style:** {transfer['Style']}")
                                    st.markdown(f"**Product:** {transfer['Product']}")
                                    st.markdown(f"**Size:** {transfer['Size']}")
                                
                                col1, col2 = st.columns(2)
                                with col1:
                                    st.metric("Need", transfer['Need'])
                                with col2:
                                    st.metric("Available", transfer['Avail'])
                
                                # Editable quantity
                                max_qty = transfer['Avail']
                                rec_qty = min(transfer['Need'], transfer['Avail'])
                                qty = st.number_input(
                                    "Quantity to transfer",
                                    min_value=1,
                                    max_value=max_qty,
                                    value=rec_qty,
                                    key="compact_popup_qty"
                                )
                
                                # Optional reason/notes
                                reason = st.text_input("Reason (optional)", placeholder="e.g., Stock transfer", key="create_reason_input")
                
                                col1, col2 = st.columns(2)
                                with col1:
                                    if st.button("✅ Confirm Transfer", use_container_width=True, type="primary"):
                                        # Get the actual row data from table_df using the stored indices
                                        source_id = transfer['source_id']
                                        dest_id = transfer['dest_id']
                                        sku_code = transfer['sku_code']
                        
                                       # In the individual transfer creation section, add a check
                                        if source_id == dest_id:
                                            st.warning(f"Cannot create transfer: Source and destination are the same store")
                                        else:
                                            cursor.execute(
                                                """
                                                INSERT INTO transfer_recommendations 
                                                (from_store_id, to_store_id, sku, quantity, priority, reason)
                                                VALUES (?, ?, ?, ?, ?, ?)
                                                """,
                                                (
                                                    source_id,
                                                    dest_id,
                                                    sku_code,
                                                    qty,
                                                    'medium',
                                                    reason if reason else f"Need {transfer['Need']} units"
                                                )
                                            )
                                            conn.commit()
                        
                                            # Clear popup state
                                            st.session_state.show_create_popup = False
                                            st.session_state.selected_transfer = None
                        
                                            st.success("✅ Transfer created!")
                                            st.rerun()
                
                                with col2:
                                    if st.button("❌ Cancel", use_container_width=True):
                                        st.session_state.show_create_popup = False
                                        st.session_state.selected_transfer = None
                                        st.rerun()
    
                # Display created transfers
                if not created_df.empty:
                    st.markdown("**✅ Created Transfers**")
        
                    # Prepare display dataframe
                    display_df = created_df[['From', 'To', 'SKU', 'Product', 'Avail', 'Qty']].copy()
                    display_df = display_df.rename(columns={'Qty': 'Trns'})
        
                    # Add selection with on_select
                    selection = st.dataframe(
                        display_df,
                        width='stretch',
                        hide_index=True,
                        on_select="rerun",
                        selection_mode="single-row",
                        key="created_transfers_compact"
                    )
                    st.success(f"📊 Showing {len(created_df)} created transfers")

                    # Handle selection
                    if selection.selection.rows:
                        selected_idx = selection.selection.rows[0]
                        original_idx = created_df.iloc[selected_idx].name
                        selected_row = table_df.loc[original_idx]
            
                        col1, col2 = st.columns([3, 1])
                        with col1:
                            st.info(f"Selected: {selected_row['From']} → {selected_row['To']} | {selected_row['SKU']}")
                        with col2:
                            if st.button("↩️ Undo Selected", key="undo_selected", use_container_width=True):
                                cursor.execute(
                                    "DELETE FROM transfer_recommendations WHERE from_store_id = ? AND to_store_id = ? AND sku = ? AND status = 'pending'",
                                    (selected_row['source_id'], selected_row['dest_id'], selected_row['sku_code'])
                                )
                                conn.commit()
                                st.success("⏪ Transfer undone!")
                                st.rerun()

            with view_tab3:
                # Use columns to show multiple cards per row
                num_cols = 2 if len(table_df) > 4 else 1
                card_cols = st.columns(num_cols)
                
                for idx, row in table_df.iterrows():
                    with card_cols[idx % num_cols]:
                        with st.container():
                            # Card header
                            st.markdown(f"**{row['From']} → {row['To']}**")
                            st.caption(f"{row['SKU']} - {row['Product']}")
                            
                            # Details in expander
                            with st.expander("Details", expanded=True):
                                det_col1, det_col2 = st.columns(2)
                                with det_col1:
                                    st.write(f"Brand: {row['Brand']}")
                                    st.write(f"Size: {row['Size']}")
                                    st.write(f"Need: {row['Need']}")
                                with det_col2:
                                    st.write(f"Style: {row['Style']}")
                                    st.write(f"color: {row['Colour']}")
                                    st.write(f"Avail: {row['Avail']}")
                            
                            # Action controls
                            if row['is_created']:
                                st.success(f"✅ Created: {row['Qty']} units")
                                if st.button("↩️ Undo", key=f"undo_{idx}"):
                                    cursor.execute(
                                        "DELETE FROM transfer_recommendations WHERE from_store_id = ? AND to_store_id = ? AND sku = ? AND status = 'pending'",
                                        (row['source_id'], row['dest_id'], row['sku_code'])
                                    )
                                    conn.commit()
                                    st.success("⏪ Undone!")
                                    st.rerun()
                            else:
                                # Simple - just limit by available
                                qty = st.number_input(
                                    "Qty",
                                    min_value=1,
                                    max_value=row['Avail'],  # Only limited by available stock
                                    value=min(row['Need'], row['Avail']),  # Default to need if enough, otherwise max available
                                    key=f"qty_{idx}",
                                    label_visibility="collapsed"
                                )
                                if st.button("📋 Create", key=f"create_{idx}"):
                                    cursor.execute(
                                        """
                                        INSERT INTO transfer_recommendations 
                                        (from_store_id, to_store_id, sku, quantity, priority, reason)
                                        VALUES (?, ?, ?, ?, ?, ?)
                                        """,
                                        (
                                            row['source_id'],
                                            row['dest_id'],
                                            row['sku_code'],
                                            qty,
                                            'medium',
                                            f"Available: {row['Avail']} units"
                                        )
                                    )
                                    conn.commit()
                                    st.success("✅ Created!")
                                    st.rerun()
                            
                            st.markdown("---")
            
            # =========================================================
            # SUMMARY STATS
            # =========================================================
            st.markdown("---")
            st.subheader("📊 Summary")
            
            sum_cols = st.columns([1, 1, 1, 1, 1])
            
            with sum_cols[0]:
                st.metric("Total", len(filtered))
            with sum_cols[1]:
                total_units = int(filtered['max_transfer'].sum()) if not filtered['max_transfer'].isna().all() else 0
                st.metric("Units", f"{total_units:,}")
            with sum_cols[2]:
                st.metric("Pending", pending_count)
            with sum_cols[3]:
                unique_skus = filtered['sku'].nunique()
                st.metric("SKUs", unique_skus)
            with sum_cols[4]:
                avg_val = filtered['max_transfer'].mean()
                avg_transfer = 0 if pd.isna(avg_val) else int(avg_val)
                st.metric("Avg", avg_transfer)
        
        else:
            st.info("No transfer recommendations found with current settings")
            
    except Exception as e:
        st.error(f"Error generating transfers: {e}")
        import traceback
        st.code(traceback.format_exc())


# Reports Tab
elif st.session_state.active_tab == "📈 Reports":
    st.title("📈 Transfer Reports")
    
    tab1, tab2, tab3 = st.tabs(["📊 Summary", "📝 Pending", "✅ Completed"])
    
    with tab1:
        st.subheader("Transfer Summary")
        
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM transfer_recommendations")
        transfer_count = cursor.fetchone()[0] or 0
        
        if transfer_count == 0:
            st.info("No transfers have been created yet. Go to the Transfers tab to create some!")
        else:
            cursor.execute("""
                SELECT 
                    COUNT(*) as total,
                    SUM(CASE WHEN status = 'pending' THEN 1 ELSE 0 END) as pending,
                    SUM(CASE WHEN status = 'completed' THEN 1 ELSE 0 END) as completed,
                    SUM(quantity) as total_units
                FROM transfer_recommendations
            """)
            stats = cursor.fetchone()
            
            col1, col2, col3, col4 = st.columns(4)
            with col1:
                st.metric("Total Transfers", stats[0] or 0)
            with col2:
                st.metric("Pending", stats[1] or 0)
            with col3:
                st.metric("Completed", stats[2] or 0)
            with col4:
                st.metric("Total Units", stats[3] or 0)
            
            st.markdown("---")
            st.subheader("🏬 Transfers by Store")
            
            col1, col2 = st.columns(2)
            
            with col1:
                outgoing = pd.read_sql("""
                    SELECT s.store_name, COUNT(*) as transfers, SUM(t.quantity) as units
                    FROM transfer_recommendations t
                    JOIN stores s ON t.from_store_id = s.store_id
                    GROUP BY s.store_name
                    ORDER BY units DESC
                """, conn)
                
                if not outgoing.empty:
                    st.markdown("**📤 Outgoing (Sending)**")
                    st.dataframe(
                                outgoing, 
                                width='content', 
                                height = set_table_height(len(outgoing), 400),
                                hide_index=True,
                                column_config={
                                    "store_name": st.column_config.TextColumn("Store Name", width="medium"),
                                    "transfers": st.column_config.NumberColumn("# Transfers", width="small", format="%d"),
                                    "units": st.column_config.NumberColumn("Units", width="small", format="%d")
                                }
                                
                    )
            
            with col2:
                incoming = pd.read_sql("""
                    SELECT s.store_name, COUNT(*) as transfers, SUM(t.quantity) as units
                    FROM transfer_recommendations t
                    JOIN stores s ON t.to_store_id = s.store_id
                    GROUP BY s.store_name
                    ORDER BY units DESC
                """, conn)
                
                if not incoming.empty:
                    st.markdown("**📥 Incoming (Receiving)**")
                    st.dataframe(
                                incoming, 
                                width='content', 
                                height = set_table_height(len(incoming), 400),
                                hide_index=True,
                                column_config={
                                    "store_name": st.column_config.TextColumn("Store Name", width="medium"),
                                    "transfers": st.column_config.NumberColumn("# Transfers", width="small", format="%d"),
                                    "units": st.column_config.NumberColumn("Units", width="small", format="%d")
                                }
                    )
            
            st.markdown("---")
            st.subheader("🔥 Top Transferred SKUs")
            
            top_skus = pd.read_sql("""
                SELECT 
                    t.sku, 
                    p.description,
                    p.style_name,
                    COUNT(*) as transfers, 
                    SUM(t.quantity) as units
                FROM transfer_recommendations t
                JOIN products p ON t.sku = p.sku
                GROUP BY t.sku, p.description, p.style_name
                ORDER BY units DESC
                LIMIT 10
            """, conn)
            
            if not top_skus.empty:
                st.dataframe(
                            top_skus,
                            width='content',
                            height = set_table_height(len(top_skus), 400),                                
                            hide_index=True,
                            column_config={
                                'sku': 'SKU',
                                'description': 'Description',
                                'style_name': 'Style',
                                'transfers': st.column_config.NumberColumn('# Transfers', format="%d"),
                                'units': st.column_config.NumberColumn('Total Units', format="%d")
                            }
                )
            
            st.markdown("---")
            st.subheader("📊 Priority Breakdown")
            
            priority_stats = pd.read_sql("""
                SELECT 
                    priority,
                    COUNT(*) as transfers,
                    SUM(quantity) as units
                FROM transfer_recommendations
                GROUP BY priority
                ORDER BY 
                    CASE priority 
                        WHEN 'high' THEN 1 
                        WHEN 'medium' THEN 2 
                        WHEN 'low' THEN 3 
                    END
            """, conn)
            
            if not priority_stats.empty:
                col1, col2 = st.columns(2)
                with col1:
                    fig = px.pie(priority_stats, values='transfers', names='priority',
                                title='Transfers by Priority',
                                color='priority',
                                color_discrete_map={'high': 'red', 'medium': 'orange', 'low': 'green'})
                    st.plotly_chart(fig, width='content')
                
                with col2:
                    st.dataframe(priority_stats, 
                                 width='content', 
                                 height = set_table_height(len(priority_stats), 400),
                                 hide_index=True,
                                 column_config={
                                        "priority": st.column_config.TextColumn("Priority", width="small"),
                                        "transfers": st.column_config.NumberColumn("# Transfers", width="small", format="%d"),
                                        "units": st.column_config.NumberColumn("Units", width="small", format="%d")
                                    }
                                 )
    
    with tab2:
        st.subheader("📝 Pending Transfers")
        
        cursor.execute("SELECT COUNT(*) FROM transfer_recommendations WHERE status = 'pending'")
        pending_count = cursor.fetchone()[0] or 0
        
        if pending_count == 0:
            st.info("No pending transfers")
        else:
            filter_df = pd.read_sql("""
                SELECT DISTINCT 
                    s_from.store_name as from_store,
                    s_to.store_name as to_store,
                    t.priority
                FROM transfer_recommendations t
                JOIN stores s_from ON t.from_store_id = s_from.store_id
                JOIN stores s_to ON t.to_store_id = s_to.store_id
                WHERE t.status = 'pending'
                ORDER BY s_from.store_name, s_to.store_name
            """, conn)
            
            with st.expander("🔍 Filter Transfers", expanded=True):
                col1, col2, col3, col4 = st.columns(4)
                
                with col1:
                    all_sources = ["All Sources"] + sorted(filter_df['from_store'].unique().tolist())
                    selected_source = st.selectbox("📍 Source Store", options=all_sources, index=0)
                
                with col2:
                    all_dests = ["All Destinations"] + sorted(filter_df['to_store'].unique().tolist())
                    selected_dest = st.selectbox("🎯 Destination Store", options=all_dests, index=0)
                
                with col3:
                    all_priorities = ["All Priorities", "high", "medium", "low"]
                    selected_priority = st.selectbox("⚡ Priority", options=all_priorities, index=0)
                
                with col4:
                    date_options = ["All time", "Last 7 days", "Last 30 days", "Last 90 days"]
                    selected_date_range = st.selectbox("📅 Date Range", options=date_options, index=0)
            
            where_clauses = ["t.status = 'pending'"]
            params = []
            
            if selected_source != "All Sources":
                where_clauses.append("s_from.store_name = ?")
                params.append(selected_source)
            
            if selected_dest != "All Destinations":
                where_clauses.append("s_to.store_name = ?")
                params.append(selected_dest)
            
            if selected_priority != "All Priorities":
                where_clauses.append("t.priority = ?")
                params.append(selected_priority)
            
            if selected_date_range != "All time":
                days_map = {"Last 7 days": 7, "Last 30 days": 30, "Last 90 days": 90}
                days = days_map.get(selected_date_range, 0)
                if days > 0:
                    where_clauses.append(f"t.recommendation_date >= datetime('now', '-{days} days')")
            
            where_sql = " AND ".join(where_clauses)
            
            query = f"""
                SELECT 
                    datetime(t.recommendation_date, 'localtime') as created,
                    s_from.store_name as from_store,
                    s_to.store_name as to_store,
                    t.sku,
                    p.supplier,
                    p.description,
                    p.style_name,
                    p.size,
                    p.color,
                    t.quantity,
                    t.priority,
                    t.reason
                FROM transfer_recommendations t
                JOIN stores s_from ON t.from_store_id = s_from.store_id
                JOIN stores s_to ON t.to_store_id = s_to.store_id
                JOIN products p ON t.sku = p.sku
                WHERE {where_sql}
                ORDER BY 
                    CASE t.priority 
                        WHEN 'high' THEN 1 
                        WHEN 'medium' THEN 2 
                        WHEN 'low' THEN 3 
                    END,
                    t.recommendation_date DESC
            """
            
            pending = pd.read_sql(query, conn, params=params)
            
            st.info(f"📊 Showing {len(pending)} of {pending_count} pending transfers")
            
            if not pending.empty:
                pending['priority_display'] = pending['priority'].map({
                    'high': '🔴 HIGH',
                    'medium': '🟡 MEDIUM',
                    'low': '🟢 LOW'
                }).fillna('⚪ UNKNOWN')
                
                st.dataframe(
                    pending[['created', 'from_store', 'to_store', 'sku', 'supplier', 'description', 'style_name',
                            'size', 'color', 'quantity', 'priority_display', 'reason']],
                    width='content',
                    hide_index=True,
                    column_config={
                        'created': st.column_config.DatetimeColumn('Date', format="YYYY-MM-DD HH:mm"),
                        'from_store': 'From',
                        'to_store': 'To',
                        'sku': 'SKU',
                        'supplier': 'Supplier',
                        'description': 'Description',
                        'style_name': 'Style',
                        'size': st.column_config.NumberColumn('Size', format="%.1f"),
                        'color': 'Colour',
                        'quantity': st.column_config.NumberColumn('Qty', format="%d"),
                        'priority_display': 'Priority',
                        'reason': 'Reason'
                    }
                )
                
                st.markdown("---")
                st.subheader("📄 Export Options")
                
                col1, col2 = st.columns(2)
                
                with col1:
                    if st.button("📥 Export Filtered as CSV", width='content'):
                        csv = pending.to_csv(index=False)
                        
                        if selected_source != "All Sources" and selected_dest != "All Destinations":
                            filename = f"pending_{selected_source}_to_{selected_dest}_{datetime.now().strftime('%Y%m%d_%H%M')}.csv"
                        elif selected_source != "All Sources":
                            filename = f"pending_from_{selected_source}_{datetime.now().strftime('%Y%m%d_%H%M')}.csv"
                        elif selected_dest != "All Destinations":
                            filename = f"pending_to_{selected_dest}_{datetime.now().strftime('%Y%m%d_%H%M')}.csv"
                        elif selected_priority != "All Priorities":
                            filename = f"pending_{selected_priority}_{datetime.now().strftime('%Y%m%d_%H%M')}.csv"
                        else:
                            filename = f"all_pending_{datetime.now().strftime('%Y%m%d_%H%M')}.csv"
                        
                        st.download_button("Download CSV", csv, filename, mime="text/csv")
                
                with col2:
                    if st.button("📄 Export Filtered as PDF", width='content'):
                        with st.spinner("Generating PDF report..."):
                            pdf_df = clean_for_pdf(pending)
                            
                            if selected_source != "All Sources" and selected_dest != "All Destinations":
                                filter_context = f"{selected_source}_to_{selected_dest}"
                                title_source = selected_source
                                title_dest = selected_dest
                            elif selected_source != "All Sources":
                                filter_context = f"from_{selected_source}"
                                title_source = selected_source
                                title_dest = "All"
                            elif selected_dest != "All Destinations":
                                filter_context = f"to_{selected_dest}"
                                title_source = "All"
                                title_dest = selected_dest
                            elif selected_priority != "All Priorities":
                                filter_context = f"{selected_priority}_priority"
                                title_source = "All"
                                title_dest = "All"
                            else:
                                filter_context = "all_pending"
                                title_source = "All"
                                title_dest = "All"
                            
                            pdf_file = export_filtered_transfers_to_pdf(pdf_df, title_source, title_dest)
                            
                            with open(pdf_file, 'rb') as f:
                                pdf_data = f.read()
                            
                            os.unlink(pdf_file)
                            
                            st.download_button(
                                "Download PDF",
                                data=pdf_data,
                                file_name=f"pending_{filter_context}_{datetime.now().strftime('%Y%m%d_%H%M')}.pdf",
                                mime="application/pdf"
                            )
                
                st.markdown("---")
                st.subheader("📊 Summary by Store Pair")
                pair_summary = pending.groupby(['from_store', 'to_store']).agg({
                    'sku': 'count',
                    'quantity': 'sum'
                }).reset_index()
                pair_summary.columns = ['From', 'To', '# Transfers', 'Total Units']
                pair_summary = pair_summary.sort_values('Total Units', ascending=False)
                st.dataframe(
                            pair_summary, 
                            width='content',
                            height = set_table_height(len(pair_summary), 400),
                            hide_index=True
                )
                
                st.markdown("---")
                st.subheader("⚡ Batch Action")
                
                if st.button("✅ Mark All Filtered as Completed", width='content', type="primary"):
                    update_query = f"""
                        UPDATE transfer_recommendations 
                        SET status = 'completed' 
                        WHERE id IN (
                            SELECT t.id
                            FROM transfer_recommendations t
                            JOIN stores s_from ON t.from_store_id = s_from.store_id
                            JOIN stores s_to ON t.to_store_id = s_to.store_id
                            WHERE {where_sql}
                        )
                    """
                    cursor.execute(update_query, params)
                    conn.commit()
                    st.success(f"✅ All {len(pending)} filtered transfers marked as completed!")
                    st.rerun()
            
            else:
                st.warning("No transfers match the selected filters")
    
    with tab3:
        st.subheader("✅ Completed Transfers")
        
        cursor.execute("SELECT COUNT(*) FROM transfer_recommendations WHERE status = 'completed'")
        completed_count = cursor.fetchone()[0] or 0
        
        if completed_count == 0:
            st.info("No completed transfers yet")
        else:
            days_options = [7, 30, 60, 90, 365]
            days = st.selectbox("Show transfers from last", days_options, index=1, format_func=lambda x: f"{x} days")
            
            completed = pd.read_sql(f"""
                SELECT 
                    datetime(t.recommendation_date, 'localtime') as completed_date,
                    s_from.store_name as from_store,
                    s_to.store_name as to_store,
                    t.sku,
                    p.description,
                    p.style_name,
                    p.size,
                    p.color,
                    t.quantity,
                    t.priority
                FROM transfer_recommendations t
                JOIN stores s_from ON t.from_store_id = s_from.store_id
                JOIN stores s_to ON t.to_store_id = s_to.store_id
                JOIN products p ON t.sku = p.sku
                WHERE t.status = 'completed'
                  AND t.recommendation_date >= datetime('now', ?)
                ORDER BY t.recommendation_date DESC
            """, conn, params=[f'-{days} days'])
            
            if not completed.empty:
                st.info(f"Showing {len(completed)} completed transfers from the last {days} days")
                
                st.dataframe(
                    completed,
                    width='content',
                    column_config={
                        'completed_date': 'Date Completed',
                        'from_store': 'From',
                        'to_store': 'To',
                        'sku': 'SKU',
                        'description': 'Description',
                        'style_name': 'Style',
                        'size': 'Size',
                        'color': 'Colour',
                        'quantity': 'Qty',
                        'priority': 'Priority'
                    }
                )
                
                col1, col2, col3 = st.columns(3)
                
                with col1:
                    total_units = int(completed['quantity'].sum())
                    st.metric("Total Units Moved", f"{total_units:,}")
                
                with col2:
                    unique_skus = completed['sku'].nunique()
                    st.metric("Unique SKUs", unique_skus)
                
                with col3:
                    avg_per_transfer = total_units / len(completed) if len(completed) > 0 else 0
                    st.metric("Avg Units/Transfer", f"{avg_per_transfer:.1f}")
                
                csv = completed.to_csv(index=False)
                st.download_button(
                    "📥 Download Completed Transfers CSV",
                    data=csv,
                    file_name=f"completed_transfers_{datetime.now().strftime('%Y%m%d_%H%M')}.csv",
                    mime="text/csv",
                    width='content'
                )
            else:
                st.info(f"No completed transfers in the last {days} days")

# Data Management Tab

elif st.session_state.active_tab == "📁 Data Management":
    st.title("📁 Data Management")
    
    tab1, tab2, tab3 = st.tabs(["📤 Upload Files", "🔄 Raw Data Processing", "📥 Download Templates"])
    
    with tab1:
        st.subheader("Upload Processed CSV Files")
        st.caption("Upload files in this order: stores.csv → products.csv → stock_levels.csv → sales.csv")
        
        col1, col2 = st.columns(2)
        
        with col1:
            stores_file = st.file_uploader("stores.csv", type=['csv'], key="stores_upload")
            if stores_file:
                try:
                    df = pd.read_csv(stores_file)
                    required_cols = ['store_id', 'store_name']
                    if all(col in df.columns for col in required_cols):
                        df.to_sql('stores', conn, if_exists='replace', index=False)
                        st.success(f"Loaded {len(df)} stores")
                    else:
                        st.error(f"Missing required columns. Need: {required_cols}")
                except Exception as e:
                    st.error(f"Error loading stores: {e}")
            
            products_file = st.file_uploader("products.csv", type=['csv'], key="products_upload")
            if products_file:
                df = pd.read_csv(products_file)
                df.to_sql('products', conn, if_exists='replace', index=False)
                st.success(f"Loaded {len(df)} products")
        
        with col2:
            stock_file = st.file_uploader("stock_levels.csv", type=['csv'], key="stock_upload")
            if stock_file:
                df = pd.read_csv(stock_file)
                cursor = conn.cursor()
                cursor.execute("DELETE FROM stock_levels")
                df.to_sql('stock_levels', conn, if_exists='append', index=False)
                st.success(f"Loaded {len(df)} stock records")
            
            sales_file = st.file_uploader("sales.csv", type=['csv'], key="sales_upload")
            if sales_file:
                df = pd.read_csv(sales_file)
                df.to_sql('sales', conn, if_exists='append', index=False)
                st.success(f"Loaded {len(df)} sales records")
    
    with tab2:
        st.subheader("Process Raw POS Files")
        st.caption("Upload your raw Excel files and map columns to import into the database")
    
        # Initialize session state for file processing
        if 'sales_file_uploaded' not in st.session_state:
            st.session_state.sales_file_uploaded = False
        if 'soh_file_uploaded' not in st.session_state:
            st.session_state.soh_file_uploaded = False
        if 'sales_column_mapping' not in st.session_state:
            st.session_state.sales_column_mapping = {}
        if 'soh_column_mapping' not in st.session_state:
            st.session_state.soh_column_mapping = {}
        if 'sales_sheets' not in st.session_state:
            st.session_state.sales_sheets = []
        if 'soh_sheets' not in st.session_state:
            st.session_state.soh_sheets = []
    
        # Create two columns for file uploads
        col1, col2 = st.columns(2)
    
        with col1:
            st.markdown("### 📊 Sales Data")
            sales_file = st.file_uploader(
                "Upload Sales Excel File", 
                type=['xlsx', 'xls'], 
                key="sales_raw_upload",
                help="Select the Excel file containing sales transaction data"
            )
        
            if sales_file:
                st.session_state.sales_file_uploaded = True
                # Read Excel file to get sheet names
                try:
                    xl = pd.ExcelFile(sales_file)
                    st.session_state.sales_sheets = xl.sheet_names
                
                    # Sheet selector
                    selected_sales_sheet = st.selectbox(
                        "Select Sales Sheet",
                        options=st.session_state.sales_sheets,
                        key="sales_sheet_selector"
                    )
                
                    # Preview data from selected sheet
                    preview_df = pd.read_excel(sales_file, sheet_name=selected_sales_sheet, nrows=5)
                
                    st.markdown("**Column Mapping**")
                    st.caption("Map database fields to columns in your Excel file")
                
                    # Column mapping interface
                    mapping_container = st.container()
                    with mapping_container:
                        display_column_mapping_ui(
                            preview_df, 
                            "sales", 
                            st.session_state.sales_column_mapping
                        )
                
                    # Show data preview
                    with st.expander("🔍 Preview Excel Data"):
                        st.dataframe(preview_df, use_container_width=True)
                    
                except Exception as e:
                    st.error(f"Error reading Excel file: {e}")
                    st.session_state.sales_file_uploaded = False
    
        with col2:
            st.markdown("### 📦 Stock on Hand Data")
            soh_file = st.file_uploader(
                "Upload Stock on Hand Excel File", 
                type=['xlsx', 'xls'], 
                key="soh_raw_upload",
                help="Select the Excel file containing current stock levels"
            )
        
            if soh_file:
                st.session_state.soh_file_uploaded = True
                try:
                    xl = pd.ExcelFile(soh_file)
                    st.session_state.soh_sheets = xl.sheet_names
                
                    selected_soh_sheet = st.selectbox(
                        "Select Stock Sheet",
                        options=st.session_state.soh_sheets,
                        key="soh_sheet_selector"
                    )
                
                    preview_df = pd.read_excel(soh_file, sheet_name=selected_soh_sheet, nrows=5)
                
                    st.markdown("**Column Mapping**")
                    st.caption("Map database fields to columns in your Excel file")
                
                    mapping_container = st.container()
                    with mapping_container:
                        display_column_mapping_ui(
                            preview_df, 
                            "soh", 
                            st.session_state.soh_column_mapping
                        )
                
                    with st.expander("🔍 Preview Excel Data"):
                        st.dataframe(preview_df, use_container_width=True)
                    
                except Exception as e:
                    st.error(f"Error reading Excel file: {e}")
                    st.session_state.soh_file_uploaded = False
    
        # Process button
        st.markdown("---")
    
        # Check if both files are uploaded and required columns mapped
        if st.session_state.sales_file_uploaded and st.session_state.soh_file_uploaded:
            sales_complete, sales_missing = check_mapping_complete(st.session_state.sales_column_mapping, "sales")
            soh_complete, soh_missing = check_mapping_complete(st.session_state.soh_column_mapping, "soh")
        
            if sales_complete and soh_complete:
                if st.button("🚀 Process Files", type="primary", use_container_width=True):
                    with st.spinner("Processing files and loading into database..."):
                        try:
                            # Process both files
                            result = process_raw_files(
                                sales_file, 
                                selected_sales_sheet,
                                st.session_state.sales_column_mapping,
                                soh_file,
                                selected_soh_sheet,
                                st.session_state.soh_column_mapping,
                                conn
                            )
                        
                            # Show success message with summary
                            st.success("✅ Files processed successfully!")
                        
                            # Display store normalization info
                            if result.get('raw_store_identifiers', 0) > result.get('stores', 0):
                                st.info(f"🏬 Normalized {result['raw_store_identifiers']} raw store identifiers to {result['stores']} unique stores")

                            # Display warnings if any
                            if result['warnings']:
                                with st.expander("⚠️ Processing Warnings"):
                                    for warning in result['warnings']:
                                        st.warning(warning)
                        
                            # Display summary
                            col1, col2, col3, col4 = st.columns(4)
                            with col1:
                                st.metric("Stores", result['stores'])
                            with col2:
                                st.metric("Products", result['products'])
                            with col3:
                                st.metric("Stock Records", result['stock_records'])
                            with col4:
                                st.metric("Sales Records", result['sales_records'])
                        
                            # Show data quality metrics
                            with st.expander("📊 Data Quality Metrics"):
                                col1, col2 = st.columns(2)
                                with col1:
                                    st.metric("Invalid Stock Rows", result['invalid_stock'])
                                    st.metric("Products missing Size", result['missing_size'])
                                    st.metric("Products missing Description", result['missing_desc'])
                                with col2:
                                    st.metric("Invalid Sales Rows", result['invalid_sales'])
                                    st.metric("Products missing Product Type", result['missing_product_type'])
                                    st.metric("Products missing Size/Colour", 
                                            result['missing_size'] + result['missing_color'])
                        
                            # Reset session state for next upload
                            st.session_state.sales_file_uploaded = False
                            st.session_state.soh_file_uploaded = False
                            st.session_state.sales_column_mapping = {}
                            st.session_state.soh_column_mapping = {}
                        
                        except Exception as e:
                            st.error(f"Error processing files: {e}")
                            import traceback
                            st.code(traceback.format_exc())
            else:
                missing = []
                if not sales_complete:
                    missing.append(f"Sales: {', '.join(sales_missing)}")
                if not soh_complete:
                    missing.append(f"Stock: {', '.join(soh_missing)}")
                st.warning(f"Please map all required fields: {'; '.join(missing)}")
        else:
            st.info("👆 Please upload both Sales and Stock files to begin")
    
    with tab3:
        st.subheader("Download Templates")
        
        col1, col2, col3, col4 = st.columns(4)
        
        sample_stores = pd.DataFrame({
            'store_id': ['BENDIGO', 'WAGGA_WAGGA'],
            'store_name': ['Bendigo', 'Wagga Wagga'],
            'store_type': ['physical', 'physical'],
            'location': ['VIC', 'NSW'],
            'is_active': [1, 1]
        })
        
        sample_products = pd.DataFrame({
            'sku': ['9333333385287', '9333333369089'],
            'style_code': ['TEST', 'TEST'],
            'style_name': ['Test Product 1', 'Test Product 2'],
            'description': ['Test Description 1', 'Test Description 2'],
            'category': ['General', 'General'],
            'size': [9, 10],
            'gender': ['U', 'U'],
            'color': ['Black', 'White'],
            'cost_price': ['29.00', '39.50'],
            'retail_price': ['49.99', '69.99'],
            'supplier': ['ON OCEANIA PTY LTD', 'AUSTRALIAN FOOTWEAR GROUP PTY LTD'],
            'color': ['Black', 'White'],
            'product_type': ['MERRELL', 'ZIERA'],
            'season': ['S25', 'W25']
        })
        
        with col1:
            csv = sample_stores.to_csv(index=False)
            st.download_button("stores.csv", csv, "stores_template.csv")
        
        with col2:
            csv = sample_products.to_csv(index=False)
            st.download_button("products.csv", csv, "products_template.csv")
        
        with col3:
            sample_stock = pd.DataFrame({
                'store_id': ['BENDIGO', 'BENDIGO'],
                'sku': ['TEST001', 'TEST002'],
                'quantity': [10, 5]
            })
            csv = sample_stock.to_csv(index=False)
            st.download_button("stock_levels.csv", csv, "stock_template.csv")
        
        with col4:
            sample_sales = pd.DataFrame({
                'store_id': ['BENDIGO', 'BENDIGO'],
                'sku': ['9333333385287', '9333333369089'],
                'sale_date': ['2026-01-27', '2026-01-27'],
                'quantity': [1, 2],
                'revenue': [49.99, 139.98]
            })
            csv = sample_sales.to_csv(index=False)
            st.download_button("sales.csv", csv, "sales_template.csv")

# Footer
st.markdown("---")
st.caption(f"Last updated: {datetime.now().strftime('%Y-%m-%d %H:%M')}")
