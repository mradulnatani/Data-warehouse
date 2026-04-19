#  Amazon Data Warehouse (OLAP Dashboard)

A complete **Data Warehouse + OLAP analytics system** built using:

* **PostgreSQL** → Data Warehouse
* **Python (Pandas + SQLAlchemy)** → ETL Pipeline
* **Streamlit** → Interactive Browser Dashboard

This project demonstrates how to design a **Star Schema**, perform **ETL (Extract-Transform-Load)**, and run **OLAP queries** on real-world e-commerce data.

---

#  Features

*  Real dataset (Amazon E-commerce from Kaggle)
*  Star schema (Fact + Dimension tables)
*  ETL pipeline using Python
*  OLAP queries (aggregation, slicing, grouping)
*  Interactive dashboard (browser-based)
*  KPIs + Visual analytics

---

#  Dataset

Dataset used:
  Amazon E-commerce Dataset

Download it from:
https://www.kaggle.com/datasets/sharmajicoder/amazon-e-commerce

---

#  Setup Instructions

##  Clone the Repository

```bash
git clone <your-repo-url>
cd Data-warehouse
```

---

##  Create Virtual Environment

```bash
python3 -m venv venv
source venv/bin/activate
```

---

##  Install Dependencies

```bash
pip install -r requirements.txt
```

If `requirements.txt` is not present:

```bash
pip install streamlit pandas sqlalchemy psycopg2-binary
```

---

##  Setup PostgreSQL

Create database:

```sql
CREATE DATABASE amazon_dwh;
```

Update `db.py`:

```python
engine = create_engine(
    "postgresql://postgres:password@localhost:5432/amazon_dwh"
)
```

---

#  Download Dataset from Kaggle

## Option 1: Manual Download

1. Open dataset link
2. Download ZIP
3. Extract it
4. Place CSV inside:

```
data/amazon_ecommerce_1M.csv
```

---

## Option 2: Kaggle CLI (Recommended)

### Install Kaggle CLI

```bash
pip install kaggle
```

### Setup API Key

1. Go to Kaggle → Account
2. Generate API Token
3. Move file:

```bash
mkdir ~/.kaggle
mv kaggle.json ~/.kaggle/
chmod 600 ~/.kaggle/kaggle.json
```

### Download Dataset

```bash
kaggle datasets download -d sharmajicoder/amazon-e-commerce
unzip amazon-e-commerce.zip
```

Move file:

```bash
mkdir data
mv *.csv data/amazon_ecommerce_1M.csv
```

---

#  How to Run the Application

```bash
streamlit run app.py
```

Open in browser:

```
http://localhost:8501
```

---

#  How It Works

##  Architecture Overview

```
CSV Dataset (Kaggle)
        ↓
   ETL Pipeline (Python)
        ↓
PostgreSQL Data Warehouse
        ↓
   OLAP Queries
        ↓
 Streamlit Dashboard
```

---

## Data Warehouse Design

###  Star Schema

### Fact Table:

* `sales_fact`

### Dimension Tables:

* `user_dim`
* `product_dim`
* `seller_dim`
* `time_dim`
* `payment_dim`
* `delivery_dim`

---

##  ETL Pipeline

Implemented in `etl.py`

###  Extract

* Load CSV using Pandas

###  Transform

* Data cleaning
* Type conversion
* Duplicate removal
* Date normalization
* Dimension table preparation

###  Load

* Insert into PostgreSQL tables
* Generate surrogate keys (time, payment, delivery)

---

##  OLAP Operations

The system supports:

###  Aggregation

```sql
SUM(final_price)
```

###  Grouping

```sql
GROUP BY category
```

###  Time Analysis

```sql
GROUP BY month
```

###  Filtering

```sql
WHERE is_returned = TRUE
```

---

#  Dashboard Features

##  KPIs

* Total Orders
* Total Revenue

##  Charts

### 1. Sales by Category

* Category-wise revenue distribution

### 2. Monthly Revenue

* Time-based trend analysis

### 3. Returns Analysis

* Return patterns across categories

---

# How to Initialize Warehouse

Inside UI:

  Click **"Initialize Data Warehouse"**

This will:

1. Drop old tables
2. Create schema
3. Run ETL
4. Load data into warehouse

---

# Sample Output

* ~1,000,000 records processed
* Real-time dashboard updates
* Interactive analytics

---

# Important Notes

* Dataset is **not included in repo** (large size)
* Always place dataset in:

```
data/amazon_ecommerce_1M.csv
```

---

# Technologies Used

* Python
* PostgreSQL
* Pandas
* SQLAlchemy
* Streamlit

---

# Learning Outcomes

This project demonstrates:

* Data Warehouse Design
* Star Schema Modeling
* ETL Pipeline Development
* OLAP Querying
* Data Visualization

---


