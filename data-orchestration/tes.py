from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import duckdb

def extract():
    """Extract data from CSV file."""
    df = pd.read_csv('/path/to/source.csv')  # Sesuaikan path
    df.to_pickle('/tmp/extracted.pkl')

def transform():
    """Transform data using Pandas."""
    df = pd.read_pickle('/tmp/extracted.pkl')
    df['new_column'] = df['existing_column'] * 2  # Contoh transformasi
    df.to_pickle('/tmp/transformed.pkl')

def load():
    """Load transformed data to DuckDB."""
    df = pd.read_pickle('/tmp/transformed.pkl')
    conn = duckdb.connect('/path/to/duckdb.db')  # Sesuaikan path
    conn.execute("CREATE TABLE IF NOT EXISTS transformed_data AS SELECT * FROM df")
    conn.close()

# Default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 4, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define DAG
with DAG(
    'etl_pandas_dag',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False
) as dag:

    extract_task = PythonOperator(
        task_id='extract',
        python_callable=extract
    )

    transform_task = PythonOperator(
        task_id='transform',
        python_callable=transform
    )

    load_task = PythonOperator(
        task_id='load',
        python_callable=load
    )

    extract_task >> transform_task >> load_task
