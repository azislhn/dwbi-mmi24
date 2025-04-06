"""
Pemeriksaan kualitas data: memeriksa nilai NULL dan jumlah baris pada tabel dim_customers.
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import duckdb

def check_null_customer_id():
    conn = duckdb.connect('brazilian_ecommerce.db')
    df = conn.execute("SELECT * FROM dim_customers").fetchdf()
    conn.close()

    if df['customer_id'].isnull().sum() > 0:
        raise ValueError("Null values ditemukan pada kolom 'customer_id'.")
    print("customer_id NULL check passed.")

def check_row_dim_customers():
    conn = duckdb.connect('brazilian_ecommerce.db')
    count = conn.execute("SELECT COUNT(*) FROM dim_customers").fetchone()[0]
    conn.close()

    if count < 100:
        raise ValueError(f"Jumlah baris dim_customers hanya {count}. Ambang minimal adalah 100.")
    print("dim_customers row check passed.")

def document_metrics():
    conn = duckdb.connect('brazilian_ecommerce.db')
    df = conn.execute("SELECT * FROM dim_customers").fetchdf()
    conn.close()

    total_rows = len(df)
    nulls = df['customer_id'].isnull().sum()

    with open('/tmp/data_quality_report.txt', 'w') as f:
        f.write(f"Total rows: {total_rows}\n")
        f.write(f"Nulls in customer_id: {nulls}\n")
        f.write("Thresholds: min rows = 100, nulls allowed = 0\n")

# Default args
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 4, 1),
    'retries': 2,
    'retry_delay': timedelta(minutes=2),
}

with DAG(
    dag_id='data_quality_monitoring',
    default_args=default_args,
    description='Memantau kualitas data dan dokumentasi metrik',
    schedule_interval='@daily',
    catchup=False,
    tags=['etl', 'ecommerce', 'duckdb']
) as dag:

    t1 = PythonOperator(
        task_id='check_null_customer_id',
        python_callable=check_null_customer_id
    )

    t2 = PythonOperator(
        task_id='check_row_dim_customers',
        python_callable=check_row_dim_customers
    )

    t3 = PythonOperator(
        task_id='document_metrics',
        python_callable=document_metrics
    )

    [t1, t2] >> t3