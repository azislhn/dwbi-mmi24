""" 
DAG Pipeline ETL
- Mengimplementasikan dependensi tugas menggunakan operator tradisional >> dan <<
- DAG mencakup komponen tugas ekstraksi transformasi dan loading data, serta satu sensor untuk memeriksa ketersediaan data (wait_for_dataset)
- Mengiplementasikan fitur Airflow: branching logic based on conditions, error handling and retry mechanisms, dan email notifications for success/failure
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.email import EmailOperator
from airflow.sensors.python import PythonSensor
from datetime import datetime, timedelta
import os
import pickle
import duckdb
import pandas as pd
import kagglehub as kh

def open_connection():
    return duckdb.connect('brazilian_ecommerce.db')

def close_connection(conn):
    conn.close()

def _get_dataset_dict():
    if not os.path.exists('dataset_dict.pkl'):
        path = kh.dataset_download("olistbr/brazilian-ecommerce")

        print("Path to dataset files:", path)

        customers_csv = pd.read_csv(os.path.join(path,'olist_customers_dataset.csv'))
        geo_csv = pd.read_csv(os.path.join(path,'olist_geolocation_dataset.csv'))
        items_csv = pd.read_csv(os.path.join(path,'olist_order_items_dataset.csv'))
        payments_csv = pd.read_csv(os.path.join(path,'olist_order_payments_dataset.csv'))
        reviews_csv = pd.read_csv(os.path.join(path,'olist_order_reviews_dataset.csv'))
        orders_csv = pd.read_csv(os.path.join(path,'olist_orders_dataset.csv'))
        products_csv = pd.read_csv(os.path.join(path,'olist_products_dataset.csv'))
        sellers_csv = pd.read_csv(os.path.join(path,'olist_sellers_dataset.csv'))
        category_csv = pd.read_csv(os.path.join(path,'product_category_name_translation.csv'))

        dataset_dict = {
        'customers': customers_csv,
        'geo': geo_csv,
        'items': items_csv,
        'payments': payments_csv,
        'reviews': reviews_csv,
        'orders': orders_csv,
        'products': products_csv,
        'sellers': sellers_csv,
        'category': category_csv
        }

        # Save the dataset_dict to a pickle file
        with open('dataset_dict.pkl', 'wb') as file:
            pickle.dump(dataset_dict, file)
        
    return os.path.exists('dataset_dict.pkl')

def initialize_warehouse_schema(conn):
    try:
        conn.execute("DROP TABLE IF EXISTS dim_payments")
        conn.execute("DROP TABLE IF EXISTS fact_order_items")
        conn.execute("DROP TABLE IF EXISTS fact_orders")
        conn.execute("DROP TABLE IF EXISTS dim_products")
        conn.execute("DROP TABLE IF EXISTS dim_sellers")
        conn.execute("DROP TABLE IF EXISTS dim_customers")
        conn.execute("DROP TABLE IF EXISTS dim_date")
        conn.execute("DROP TABLE IF EXISTS dim_category")

        conn.execute("""
        CREATE OR REPLACE TABLE dim_date AS
        WITH date_range AS (
            SELECT unnest(generate_series('2016-09-01'::DATE, '2020-05-01'::DATE, INTERVAL '1 day')) as date
        )
        SELECT
            (EXTRACT(YEAR FROM date) * 10000 + EXTRACT(MONTH FROM date) * 100 + EXTRACT(DAY FROM date))::INTEGER AS date_id,
            date as date_value,
            EXTRACT(DAY FROM date) AS day,
            EXTRACT(MONTH FROM date) AS month,
            strftime(date, '%B') AS month_name,
            EXTRACT(YEAR FROM date) AS year,
            strftime(date, '%A') AS day_name,
            EXTRACT(DOW FROM date) AS day_of_week,
            EXTRACT(QUARTER FROM date) AS quarter
        FROM date_range;
        """)

        # Add primary key to dim_date table
        conn.execute("ALTER TABLE dim_date ADD PRIMARY KEY (date_id);")

        conn.execute("""
        -- Tabel Dimensi
        CREATE OR REPLACE TABLE dim_customers (
            customer_id VARCHAR(50) PRIMARY KEY,
            customer_unique_id VARCHAR(50) NOT NULL,
            customer_city VARCHAR(100),
            customer_state VARCHAR(50),
            customer_zip_code_prefix VARCHAR(20)
        );

        CREATE OR REPLACE TABLE dim_sellers (
            seller_id VARCHAR(50) PRIMARY KEY,
            seller_zip_code_prefix VARCHAR(20),
            seller_city VARCHAR(100),
            seller_state VARCHAR(50)
        );

        -- scd type 2
        CREATE OR REPLACE TABLE dim_products (
            produk_key INT PRIMARY KEY,
            product_id VARCHAR(50) UNIQUE NOT NULL,
            product_category_name VARCHAR(100),
            product_category_name_english VARCHAR(100),
            product_weight_g DECIMAL(10,2),
            product_length_cm DECIMAL(10,2),
            product_height_cm DECIMAL(10,2),
            product_width_cm DECIMAL(10,2),
            effective_date DATE NOT NULL,
            expiration_date DATE,
            current_flag BOOLEAN DEFAULT TRUE
        );

        -- Tabel Fakta Pemesanan
        CREATE OR REPLACE TABLE fact_orders (
            order_id VARCHAR(50) PRIMARY KEY,
            customer_id VARCHAR(50) NOT NULL,
            date_id INT NOT NULL,
            order_status VARCHAR(50),
            order_purchase_timestamp DATETIME NOT NULL,
            order_approved_at DATETIME,
            order_delivered_customer_date DATETIME,
            order_estimated_delivery_date DATETIME,
            sales_amount DECIMAL(10,2),
            FOREIGN KEY (customer_id) REFERENCES dim_customers(customer_id),
            FOREIGN KEY (date_id) REFERENCES dim_date(date_id)
        );

        -- Tabel Fakta Item Pemesanan
        CREATE OR REPLACE TABLE fact_order_items (
            order_id VARCHAR(50) NOT NULL,
            order_item_id VARCHAR(50) NOT NULL,
            product_id VARCHAR(50) NOT NULL,
            seller_id VARCHAR(50) NOT NULL,
            date_id INT NOT NULL,
            shipping_limit_date DATETIME,
            price DECIMAL(10,2),
            freight_value DECIMAL(10,2),
            PRIMARY KEY (order_id, order_item_id),
            FOREIGN KEY (order_id) REFERENCES fact_orders(order_id),
            FOREIGN KEY (seller_id) REFERENCES dim_sellers(seller_id),
            FOREIGN KEY (product_id) REFERENCES dim_products(product_id),
            FOREIGN KEY (date_id) REFERENCES dim_date(date_id)
        );

        -- DuckDB belum mendukung ALTER TABLE ADD FOREIGN KEY
        -- Sehingga dimensi payments harus setelah fact_orders karena foreign key
        CREATE OR REPLACE TABLE dim_payments (
            payment_id INT PRIMARY KEY,
            order_id VARCHAR(50) NOT NULL,
            payment_sequential INT,
            payment_type VARCHAR(50),
            payment_installments INT,
            payment_value DECIMAL(10,2),
            FOREIGN KEY (order_id) REFERENCES fact_orders(order_id)
        );
        """)

    except Exception as e:
        print(f"[ERROR] Gagal saat membuat skema: {e}")
        raise

def extract_data(conn):
    try:
        with open('dataset_dict.pkl', 'rb') as file:
            dataset_dict = pickle.load(file)
        
        customers_df = dataset_dict['customers']
        sellers_df = dataset_dict['sellers']
        products_df = dataset_dict['products']
        category_df = dataset_dict['category']
        payments_df = dataset_dict['payments']
        orders_df = dataset_dict['orders']
        items_df = dataset_dict['items']

        # Extracting data from packle file
        conn.execute("""
            CREATE OR REPLACE TABLE stg_customers AS SELECT * FROM customers_df;
            CREATE OR REPLACE TABLE stg_sellers AS SELECT * FROM sellers_df;
            CREATE OR REPLACE TABLE stg_products AS SELECT * FROM products_df;
            CREATE OR REPLACE TABLE stg_payments AS SELECT * FROM payments_df;
            CREATE OR REPLACE TABLE stg_orders AS SELECT * FROM orders_df;
            CREATE OR REPLACE TABLE stg_items AS SELECT * FROM items_df;
            CREATE OR REPLACE TABLE stg_category AS SELECT * FROM category_df;
        """)
        return True
    except FileNotFoundError as fnf_error:
        print(f"[ERROR] File tidak ditemukan: {fnf_error}")
        raise
    except Exception as e:
        print(f"[ERROR] Gagal saat extract data: {e}")
        raise

def transform_data(conn):
    try:
        # Dimensi Customers
        stg_customers = conn.execute("SELECT * FROM stg_customers").fetchdf()
        stg_customers = stg_customers.drop_duplicates(subset=['customer_id'])
        stg_customers_sorted = stg_customers[['customer_id', 'customer_unique_id', 'customer_city', 'customer_state', 'customer_zip_code_prefix']]
        conn.execute("CREATE OR REPLACE TABLE stg_dim_customers AS SELECT * FROM stg_customers_sorted")

        # Dimensi Sellers
        stg_sellers = conn.execute("SELECT * FROM stg_sellers").fetchdf()
        stg_sellers = stg_sellers.drop_duplicates(subset=['seller_id'])
        stg_sellers_sorted = stg_sellers[['seller_id', 'seller_city', 'seller_state', 'seller_zip_code_prefix']]
        conn.execute("CREATE OR REPLACE TABLE stg_dim_sellers AS SELECT * FROM stg_sellers_sorted")

        # Dimensi Products
        stg_products = conn.execute("SELECT * FROM stg_products").fetchdf()
        stg_products = stg_products.drop_duplicates(subset=['product_id'])

        stg_products = stg_products.drop('product_name_lenght', axis=1)
        stg_products = stg_products.drop('product_description_lenght', axis=1)
        stg_products = stg_products.drop('product_photos_qty', axis=1)

        for column in ['product_weight_g', 'product_length_cm', 'product_height_cm', 'product_width_cm']:
            mean_value = stg_products[column].mean()
            stg_products[column] = stg_products[column].fillna(mean_value)

        stg_products['product_key'] = range(1, len(stg_products) + 1)
        stg_products['effective_date'] = datetime.now().date()
        stg_products['expiration_date'] = None
        stg_products['current_flag'] = True

        stg_category = conn.execute("SELECT * FROM stg_category").fetchdf()
        for i in range(len(stg_category)):
            ctg = stg_category['product_category_name'][i]
            eng_ctg = stg_category['product_category_name_english'][i]
            if (ctg in stg_products['product_category_name'].unique()):
                stg_products.loc[stg_products['product_category_name'] == ctg, 'product_category_name_english'] = eng_ctg

        stg_products_sorted = stg_products[['product_key', 'product_id',
                                            'product_category_name', 'product_category_name_english',
                                            'product_weight_g', 'product_length_cm', 'product_height_cm', 'product_width_cm',
                                            'effective_date', 'expiration_date', 'current_flag']]
        conn.execute("CREATE OR REPLACE TABLE stg_dim_products AS SELECT * FROM stg_products_sorted")

        # Dimensi Payments
        stg_payments = conn.execute("SELECT * FROM stg_payments").fetchdf()
        stg_payments['payment_id'] = range(1, len(stg_payments) + 1)
        stg_payments_sorted = stg_payments[['payment_id', 'order_id', 'payment_sequential', 'payment_type', 'payment_installments', 'payment_value']]
        conn.execute("CREATE OR REPLACE TABLE stg_dim_payments AS SELECT * FROM stg_payments_sorted")

        # Fakta Orders
        stg_orders = conn.execute("SELECT * FROM stg_orders").fetchdf()
        stg_orders = stg_orders.drop('order_delivered_carrier_date', axis=1)

        stg_orders['date_id'] = pd.to_datetime(stg_orders['order_purchase_timestamp']).dt.strftime('%Y%m%d').astype(int)

        orders_cols = list(stg_orders.columns)
        customer_id_index = orders_cols.index('customer_id')
        orders_cols.insert(customer_id_index + 1, orders_cols.pop(orders_cols.index('date_id')))
        stg_orders_sorted = stg_orders[orders_cols]

        stg_items = conn.execute("SELECT * FROM stg_items").fetchdf()
        sales_amount_df = stg_items.groupby('order_id')['price'].sum().reset_index()
        sales_amount_df = sales_amount_df.rename(columns={'price': 'sales_amount'})
        stg_orders_sorted = pd.merge(stg_orders_sorted, sales_amount_df, on='order_id', how='left')
        conn.execute("CREATE OR REPLACE TABLE stg_fact_orders AS SELECT * FROM stg_orders_sorted")

        # Fakta Order Items
        stg_order_items = conn.execute("SELECT * FROM stg_items").fetchdf()
        stg_order_items['date_id'] = pd.to_datetime(stg_order_items['shipping_limit_date']).dt.strftime('%Y%m%d').astype(int)

        items_cols = list(stg_order_items.columns)
        product_id_index = items_cols.index('seller_id')
        items_cols.insert(product_id_index + 1, items_cols.pop(items_cols.index('date_id')))
        stg_order_items_sorted = stg_order_items[items_cols]
        conn.execute("CREATE OR REPLACE TABLE stg_fact_order_items AS SELECT * FROM stg_order_items_sorted")

        return True

    except Exception as e:
        print(f"[ERROR] Gagal saat transform data: {e}")
        raise

def load_data(conn):
    # Loading data into DuckDB
    try:
        stg_dim_customers = conn.execute("SELECT * FROM stg_dim_customers").fetchdf()
        stg_dim_sellers = conn.execute("SELECT * FROM stg_dim_sellers").fetchdf()
        stg_dim_products = conn.execute("SELECT * FROM stg_dim_products").fetchdf()
        stg_dim_payments = conn.execute("SELECT * FROM stg_dim_payments").fetchdf()
        stg_fact_orders = conn.execute("SELECT * FROM stg_fact_orders").fetchdf()
        stg_fact_order_items = conn.execute("SELECT * FROM stg_fact_order_items").fetchdf()

        conn.execute("INSERT INTO dim_customers SELECT * FROM stg_dim_customers")
        conn.execute("INSERT INTO dim_sellers SELECT * FROM stg_dim_sellers")
        conn.execute("INSERT INTO dim_products SELECT * FROM stg_dim_products")
        conn.execute("INSERT INTO fact_orders SELECT * FROM stg_fact_orders")
        conn.execute("INSERT INTO fact_order_items SELECT * FROM stg_fact_order_items")
        conn.execute("INSERT INTO dim_payments SELECT * FROM stg_dim_payments")

        return True

    except Exception as e:
        print(f"[ERROR] Gagal saat load data: {e}")
        raise

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 4, 1),
    'email': ['azizsolihin@mail.ugm.ac.id'],
    'email_on_failure': True,
    'email_on_retry': False,
    'email_on_success': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

# Inisialisasi DAG
with DAG(
    dag_id='etl_pipeline_duckdb',
    default_args=default_args,
    description='ETL pipeline untuk Brazilian E-Commerce ke DuckDB',
    schedule_interval='@daily',
    catchup=False,
    tags=['etl', 'ecommerce', 'duckdb']

) as dag:

    def initialize():
        conn = open_connection()
        initialize_warehouse_schema(conn)
        close_connection(conn)

    def extract():
        conn = open_connection()
        extract_data(conn)
        close_connection(conn)

    def transform():
        conn = open_connection()
        transform_data(conn)
        close_connection(conn)

    def load():
        conn = open_connection()
        load_data(conn)
        close_connection(conn)

    wait_for_dataset = PythonSensor(
        task_id='wait_for_dataset_dict',
        python_callable=_get_dataset_dict,
        mode='reschedule',
        poke_interval=30,   # Cek setiap 30 detik
        timeout=60 * 60 * 2,  # Maks waktu tunggu: 2 jam
    )

    # Define task dengan PythonOperator
    initialize_schema = PythonOperator(
        task_id='initialize_schema',
        python_callable=initialize
    )

    extract_task = PythonOperator(
        task_id='extract_data',
        python_callable=extract
    )

    transform_task = PythonOperator(
        task_id='transform_data',
        python_callable=transform
    )

    load_task = PythonOperator(
        task_id='load_data',
        python_callable=load
    )

    notify_success = EmailOperator(
        task_id='notify_success',
        to='furqanst@mail.ugm.ac.id',
        subject='DAG Success - etl_pipeline_duckdb',
        html_content='All tasks in etl_pipeline_duckdb completed successfully!'
    )

    # Set urutan dependensi
    initialize_schema >> wait_for_dataset >> extract_task >> transform_task >> load_task >> notify_success