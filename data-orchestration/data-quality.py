from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import duckdb

def check_null_values():
    """Check for null values in important columns."""
    conn = duckdb.connect('/path/to/duckdb.db')  # Sesuaikan path
    df = conn.execute("SELECT * FROM transformed_data").fetchdf()
    conn.close()
    
    if df['new_column'].isnull().any():
        raise ValueError("Null values found in new_column!")

def check_row_count():
    """Check if transformed data is empty."""
    conn = duckdb.connect('/path/to/duckdb.db')  # Sesuaikan path
    row_count = conn.execute("SELECT COUNT(*) FROM transformed_data").fetchone()[0]
    conn.close()
    
    if row_count == 0:
        raise ValueError("Transformed data is empty!")

# Default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 4, 1),
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

# Define DAG
with DAG(
    'data_quality_dag',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False
) as dag:

    check_null_task = PythonOperator(
        task_id='check_null_values',
        python_callable=check_null_values
    )

    check_row_count_task = PythonOperator(
        task_id='check_row_count',
        python_callable=check_row_count
    )

    check_null_task >> check_row_count_task
