from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.email import EmailOperator
from airflow.utils.email import send_email
from datetime import datetime, timedelta
import pandas as pd
import duckdb

def extract():
    """Extract data from CSV file."""
    try:
        df = pd.read_csv('/path/to/source.csv')  # Sesuaikan path
        df.to_pickle('/tmp/extracted.pkl')
    except Exception as e:
        raise ValueError(f"Extract failed: {str(e)}")

def transform():
    """Transform data using Pandas."""
    try:
        df = pd.read_pickle('/tmp/extracted.pkl')
        df['new_column'] = df['existing_column'] * 2  # Contoh transformasi
        df.to_pickle('/tmp/transformed.pkl')
    except Exception as e:
        raise ValueError(f"Transform failed: {str(e)}")

def load():
    """Load transformed data to DuckDB."""
    try:
        df = pd.read_pickle('/tmp/transformed.pkl')
        conn = duckdb.connect('/path/to/duckdb.db')  # Sesuaikan path
        conn.execute("CREATE TABLE IF NOT EXISTS transformed_data AS SELECT * FROM df")
        conn.close()
    except Exception as e:
        raise ValueError(f"Load failed: {str(e)}")

# Default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 4, 1),
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'email_on_failure': True,
    'email_on_retry': True,
    'email': ['your_email@example.com'],  # Ganti dengan email yang sesuai
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

    notify_success = EmailOperator(
        task_id='notify_success',
        to='your_email@example.com',  # Ganti dengan email yang sesuai
        subject='ETL DAG Success',
        html_content='ETL process completed successfully!'
    )

    extract_task >> transform_task >> load_task >> notify_success
