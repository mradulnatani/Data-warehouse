import pandas as pd
from db import engine
from sqlalchemy import text

# ---------------- CREATE TABLES ----------------
def create_tables():
    with engine.begin() as conn:

        conn.execute(text("""
        DROP TABLE IF EXISTS 
        sales_fact, user_dim, product_dim, seller_dim, 
        time_dim, payment_dim, delivery_dim 
        CASCADE;
        """))

        conn.execute(text("""
        CREATE TABLE user_dim (
            user_id TEXT PRIMARY KEY,
            location TEXT,
            device TEXT
        );
        """))

        conn.execute(text("""
        CREATE TABLE product_dim (
            product_id TEXT PRIMARY KEY,
            category TEXT,
            subcategory TEXT,
            brand TEXT
        );
        """))

        conn.execute(text("""
        CREATE TABLE seller_dim (
            seller_id TEXT PRIMARY KEY,
            seller_rating NUMERIC
        );
        """))

        conn.execute(text("""
        CREATE TABLE time_dim (
            time_id SERIAL PRIMARY KEY,
            purchase_date DATE,
            day INT,
            month INT,
            year INT
        );
        """))

        conn.execute(text("""
        CREATE TABLE payment_dim (
            payment_id SERIAL PRIMARY KEY,
            payment_method TEXT UNIQUE
        );
        """))

        conn.execute(text("""
        CREATE TABLE delivery_dim (
            delivery_id SERIAL PRIMARY KEY,
            delivery_status TEXT UNIQUE
        );
        """))

        conn.execute(text("""
        CREATE TABLE sales_fact (
            sale_id SERIAL PRIMARY KEY,
            user_id TEXT,
            product_id TEXT,
            seller_id TEXT,
            time_id INT,
            payment_id INT,
            delivery_id INT,
            price NUMERIC,
            discount NUMERIC,
            final_price NUMERIC,
            shipping_time_days INT,
            is_returned BOOLEAN
        );
        """))

# ---------------- ETL ----------------
def run_etl():
    df = pd.read_csv("data/amazon_ecommerce_1M.csv")

    df['user_id'] = df['user_id'].astype(str)
    df['product_id'] = df['product_id'].astype(str)
    df['seller_id'] = df['seller_id'].astype(str)

    df['purchase_date'] = pd.to_datetime(df['purchase_date']).dt.date

    df['payment_method'] = df['payment_method'].astype(str).str.strip().str.lower()
    df['delivery_status'] = df['delivery_status'].astype(str).str.strip().str.lower()

    # ---------------- DIMENSIONS ----------------

    # USER DIM (FIXED)
    user_dim = df.sort_values('purchase_date') \
                 .drop_duplicates(subset=['user_id'], keep='last') \
                 [['user_id', 'location', 'device']]

    # PRODUCT DIM (FIXED)
    product_dim = df.drop_duplicates(subset=['product_id'])[
        ['product_id', 'category', 'subcategory', 'brand']
    ]

    # SELLER DIM (FIXED)
    seller_dim = df.drop_duplicates(subset=['seller_id'])[
        ['seller_id', 'seller_rating']
    ]

    # TIME DIM
    time_dim = df[['purchase_date']].drop_duplicates().reset_index(drop=True)
    time_dim['day'] = pd.to_datetime(time_dim['purchase_date']).dt.day
    time_dim['month'] = pd.to_datetime(time_dim['purchase_date']).dt.month
    time_dim['year'] = pd.to_datetime(time_dim['purchase_date']).dt.year

    # PAYMENT DIM
    payment_dim = df[['payment_method']].drop_duplicates().reset_index(drop=True)
    payment_dim['payment_id'] = payment_dim.index + 1

    # DELIVERY DIM
    delivery_dim = df[['delivery_status']].drop_duplicates().reset_index(drop=True)
    delivery_dim['delivery_id'] = delivery_dim.index + 1

    # ---------------- LOAD DIMENSIONS ----------------
    user_dim.to_sql("user_dim", engine, if_exists="append", index=False)
    product_dim.to_sql("product_dim", engine, if_exists="append", index=False)
    seller_dim.to_sql("seller_dim", engine, if_exists="append", index=False)
    time_dim.to_sql("time_dim", engine, if_exists="append", index=False)
    payment_dim.to_sql("payment_dim", engine, if_exists="append", index=False)
    delivery_dim.to_sql("delivery_dim", engine, if_exists="append", index=False)

    # ---------------- LOAD BACK WITH IDs ----------------
    time_dim_db = pd.read_sql("SELECT * FROM time_dim", engine)
    payment_dim_db = pd.read_sql("SELECT * FROM payment_dim", engine)
    delivery_dim_db = pd.read_sql("SELECT * FROM delivery_dim", engine)

    # Normalize again
    time_dim_db['purchase_date'] = pd.to_datetime(time_dim_db['purchase_date']).dt.date

    # ---------------- MERGE ----------------
    df = df.merge(time_dim_db, on='purchase_date')
    df = df.merge(payment_dim_db, on='payment_method')
    df = df.merge(delivery_dim_db, on='delivery_status')

    print("Rows after merge:", len(df))

    # ---------------- FACT TABLE ----------------
    sales_fact = df[[
        'user_id', 'product_id', 'seller_id',
        'time_id', 'payment_id', 'delivery_id',
        'price', 'discount', 'final_price',
        'shipping_time_days', 'is_returned'
    ]]

    print("Fact rows:", len(sales_fact))

    sales_fact.to_sql("sales_fact", engine, if_exists="append", index=False)

