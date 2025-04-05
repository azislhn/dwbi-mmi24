from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import duckdb

def check_nulls():
    """
    Memeriksa apakah ada nilai null di kolom penting.
    Ambang batas: Tidak boleh ada null.
    """
    conn = duckdb.connect('brazilian_ecommerce.db')
    df = conn.execute("SELECT * FROM invoices").fetchdf()
    conn.close()

    if df['customer_id'].isnull().sum() > 0:
        raise ValueError("Null values ditemukan pada kolom 'customer_id'.")

def check_row_volume():
    """
    Memeriksa apakah jumlah baris memenuhi ambang batas minimum.
    Ambang batas: minimal 100 baris.
    """
    conn = duckdb.connect('brazilian_ecommerce.db')
    count = conn.execute("SELECT COUNT(*) FROM invoices").fetchone()[0]
    conn.close()

    if count < 100:
        raise ValueError(f"Jumlah baris hanya {count}. Ambang minimal adalah 100.")

def document_metrics():
    """
    Dokumentasi metrik kualitas data.
    - Jumlah baris total
    - Jumlah nilai null di kolom customer_id
    """
    conn = duckdb.connect('brazilian_ecommerce.db')
    df = conn.execute("SELECT * FROM invoices").fetchdf()
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
    'data_quality_monitoring_dag',
    default_args=default_args,
    description='Memantau kualitas data dan dokumentasi metrik',
    schedule_interval='@daily',
    catchup=False
) as dag:

    t1 = PythonOperator(
        task_id='check_nulls',
        python_callable=check_nulls
    )

    t2 = PythonOperator(
        task_id='check_row_volume',
        python_callable=check_row_volume
    )

    t3 = PythonOperator(
        task_id='document_metrics',
        python_callable=document_metrics
    )

    [t1, t2] >> t3
