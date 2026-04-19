import streamlit as st
import pandas as pd
from db import engine
from etl import create_tables, run_etl

st.set_page_config(page_title="Amazon Data Warehouse", layout="wide")

st.title("Amazon Data Warehouse (OLAP Dashboard)")

# ---------------- SESSION ----------------
if "initialized" not in st.session_state:
    st.session_state.initialized = False

# ---------------- INIT BUTTON ----------------
if st.sidebar.button("Initialize Data Warehouse"):
    with st.spinner("Running ETL..."):
        try:
            create_tables()
            run_etl()
            st.session_state.initialized = True
            st.success("Warehouse Ready")
        except Exception as e:
            st.error(f"Error: {e}")

# ---------------- CHECK ----------------
def table_exists(name):
    q = f"""
    SELECT EXISTS (
        SELECT FROM information_schema.tables 
        WHERE table_name = '{name}'
    );
    """
    return pd.read_sql(q, engine).iloc[0, 0]

if not st.session_state.initialized:
    if table_exists("sales_fact"):
        st.session_state.initialized = True
    else:
        st.warning("Click 'Initialize Data Warehouse'")
        st.stop()

# ---------------- QUERY ----------------
def run_query(q):
    return pd.read_sql(q, engine)

# ---------------- KPI ----------------
kpi = run_query("""
SELECT 
    COUNT(*) as orders, 
    COALESCE(SUM(final_price), 0) as revenue
FROM sales_fact
""")

orders = int(kpi['orders'][0]) if not kpi.empty else 0
revenue = float(kpi['revenue'][0]) if not kpi.empty else 0

col1, col2 = st.columns(2)
col1.metric("Total Orders", orders)
col2.metric("Total Revenue", round(revenue, 2))

# ---------------- CATEGORY ----------------
st.subheader("Sales by Category")

df = run_query("""
SELECT category, SUM(final_price) as revenue
FROM sales_fact sf
JOIN product_dim pd ON sf.product_id = pd.product_id
GROUP BY category
ORDER BY revenue DESC
""")

if not df.empty:
    st.bar_chart(df.set_index("category"))

# ---------------- MONTHLY ----------------
st.subheader("Monthly Revenue")

df = run_query("""
SELECT month, SUM(final_price) as revenue
FROM sales_fact sf
JOIN time_dim td ON sf.time_id = td.time_id
GROUP BY month
ORDER BY month
""")

if not df.empty:
    st.line_chart(df.set_index("month"))

# ---------------- RETURNS ----------------
st.subheader("Returns")

df = run_query("""
SELECT category, COUNT(*) as returns
FROM sales_fact sf
JOIN product_dim pd ON sf.product_id = pd.product_id
WHERE is_returned = TRUE
GROUP BY category
""")

if not df.empty:
    st.bar_chart(df.set_index("category"))

