from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.email import EmailOperator
from datetime import datetime, timedelta
import pandas as pd
import duckdb
import matplotlib.pyplot as plt

def extract_data():
    try:
        conn = duckdb.connect('brazilian-ecommerce.db')
        df = conn.execute("""
            SELECT invoice_id, total, customer_id, date(invoice_date) AS invoice_date
            FROM invoices
        """).fetchdf()
        df.to_pickle('/tmp/invoices.pkl')
        conn.close()
    except Exception as e:
        raise RuntimeError(f"Extract failed: {str(e)}")

def aggregate_data():
    try:
        df = pd.read_pickle('/tmp/invoices.pkl')
        agg_df = df.groupby('customer_id').agg(
            jumlah_invoice=('invoice_id', 'count'),
            total_pembayaran=('total', 'sum'),
            rata_rata=('total', 'mean')
        ).reset_index()
        agg_df.to_csv('/tmp/agg_customer.csv', index=False)
    except Exception as e:
        raise RuntimeError(f"Aggregation failed: {str(e)}")

def generate_plot():
    try:
        df = pd.read_csv('/tmp/agg_customer.csv')
        df = df.sort_values('total_pembayaran', ascending=False).head(10)
        plt.figure(figsize=(10, 6))
        plt.bar(df['customer_id'].astype(str), df['total_pembayaran'])
        plt.title('Top 10 Customer by Total Payment')
        plt.xticks(rotation=45)
        plt.tight_layout()
        plt.savefig('/tmp/top10_customers.png')
    except Exception as e:
        raise RuntimeError(f"Plot generation failed: {str(e)}")

# Default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 4, 1),
    'retries': 2,
    'retry_delay': timedelta(minutes=3),
    'email_on_failure': True,
    'email_on_retry': True,
    'email': ['furqanst@mail.ugm.ac.id'], 
}

with DAG(
    dag_id='test_pipeline_dag',
    default_args=default_args,
    description='DAG based on test_pipeline.ipynb with error handling and notifications',
    schedule_interval='@daily',
    catchup=False
) as dag:

    extract_task = PythonOperator(
        task_id='extract_data',
        python_callable=extract_data
    )

    aggregate_task = PythonOperator(
        task_id='aggregate_data',
        python_callable=aggregate_data
    )

    plot_task = PythonOperator(
        task_id='generate_plot',
        python_callable=generate_plot
    )

    notify_success = EmailOperator(
        task_id='notify_success',
        to='furqanst@mail.ugm.ac.id',
        subject='DAG Success - test_pipeline_dag',
        html_content='All tasks in test_pipeline_dag completed successfully!'
    )

    extract_task >> aggregate_task >> plot_task >> notify_success
