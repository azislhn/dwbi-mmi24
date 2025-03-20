-- DDL Script untuk Fact Constellation Schema - Brazilian E-Commerce Public Dataset

-- Tabel Dimensi
CREATE OR REPLACE TABLE dim_location (
    zip_code_prefix INTEGER PRIMARY KEY,
    city_name VARCHAR NOT NULL,
    state_abbr VARCHAR NOT NULL
);

CREATE OR REPLACE TABLE dim_customers (
    customer_key INTEGER PRIMARY KEY,
    customer_id VARCHAR UNIQUE NOT NULL,
    customer_unique_id VARCHAR NOT NULL,
    customer_zip_code_prefix INTEGER,
    FOREIGN KEY (customer_zip_code_prefix) REFERENCES dim_location(zip_code_prefix)
);

CREATE OR REPLACE TABLE dim_sellers (
    seller_key INTEGER PRIMARY KEY,
    seller_id VARCHAR UNIQUE NOT NULL,
    seller_zip_code_prefix INTEGER,
    FOREIGN KEY (seller_zip_code_prefix) REFERENCES dim_location(zip_code_prefix)
);

CREATE OR REPLACE TABLE dim_time (
    time_key INTEGER PRIMARY KEY,
    time_date DATE UNIQUE NOT NULL,
    time_year INTEGER NOT NULL,
    time_month INTEGER NOT NULL,
    time_day_of_week VARCHAR NOT NULL
);

-- SCD Type 2 untuk Dimensi Products
CREATE OR REPLACE TABLE dim_products (
    product_key INTEGER PRIMARY KEY,
    product_id VARCHAR NOT NULL,
    product_category_name VARCHAR NOT NULL,
    product_weight_g FLOAT NOT NULL,
    product_length_cm FLOAT NOT NULL,
    start_date TIMESTAMP NOT NULL,
    end_date TIMESTAMP,
    is_current BOOLEAN DEFAULT TRUE
);

-- Tabel Fakta
CREATE OR REPLACE TABLE fact_orders (
    order_id VARCHAR PRIMARY KEY,
    customer_key INTEGER NOT NULL,
    order_status VARCHAR NOT NULL,
    order_purchase_timestamp TIMESTAMP NOT NULL,
    order_estimated_delivery_date DATE NOT NULL,
    FOREIGN KEY (customer_key) REFERENCES dim_customers(customer_key)
);

CREATE OR REPLACE TABLE fact_order_items (
    order_id VARCHAR NOT NULL,
    order_item_id INTEGER NOT NULL,
    product_key INTEGER NOT NULL,
    seller_key INTEGER NOT NULL,
    price FLOAT NOT NULL,
    freight_value FLOAT NOT NULL,
    PRIMARY KEY (order_id, order_item_id),
    FOREIGN KEY (order_id) REFERENCES fact_orders(order_id),
    FOREIGN KEY (product_key) REFERENCES dim_products(product_key),
    FOREIGN KEY (seller_key) REFERENCES dim_sellers(seller_key)
);

CREATE OR REPLACE TABLE fact_payments (
    order_id VARCHAR NOT NULL,
    payment_sequential INTEGER NOT NULL,
    payment_type VARCHAR NOT NULL,
    payment_value FLOAT NOT NULL,
    PRIMARY KEY (order_id, payment_sequential),
    FOREIGN KEY (order_id) REFERENCES fact_orders(order_id)
);

CREATE OR REPLACE TABLE fact_reviews (
    review_id VARCHAR PRIMARY KEY,
    order_id VARCHAR NOT NULL,
    review_score INTEGER NOT NULL,
    review_creation_date DATE NOT NULL,
    FOREIGN KEY (order_id) REFERENCES fact_orders(order_id)
);
