-- DDL Script untuk Fact Constellation Schema - Brazilian E-Commerce Public Dataset

-- Tabel Dimensi
CREATE OR REPLACE TABLE dim_geo (
    geo_sk INT PRIMARY KEY,
    zip_code_prefix STRING UNIQUE NOT NULL,
    city STRING NOT NULL,
    state STRING NOT NULL
);

CREATE OR REPLACE TABLE dim_customers (
    customer_sk INT PRIMARY KEY,
    customer_id STRING UNIQUE NOT NULL,
    customer_unique_id STRING NOT NULL,
    geo_sk INT NOT NULL,
    FOREIGN KEY (geo_sk) REFERENCES dim_geo(geo_sk)
);

CREATE OR REPLACE TABLE dim_sellers (
    seller_sk INT PRIMARY KEY,
    seller_id STRING UNIQUE NOT NULL,
    geo_sk INT NOT NULL,
    FOREIGN KEY (geo_sk) REFERENCES dim_geo(geo_sk)
);

CREATE OR REPLACE TABLE dim_time (
    time_sk INT PRIMARY KEY,
    date DATE UNIQUE NOT NULL,
    year INT NOT NULL,
    month INT NOT NULL,
    day_of_week STRING NOT NULL
);

-- SCD Type 2 untuk Dimensi Products
CREATE OR REPLACE TABLE dim_products (
    product_sk INT PRIMARY KEY,
    product_id STRING NOT NULL,
    product_category_name STRING NOT NULL,
    product_weight_g FLOAT NOT NULL,
    product_length_cm FLOAT NOT NULL,
    start_date TIMESTAMP NOT NULL,
    end_date TIMESTAMP,
    is_current BOOLEAN DEFAULT TRUE
);

-- Tabel Fakta
CREATE OR REPLACE TABLE fact_orders (
    order_id STRING PRIMARY KEY,
    customer_sk INT NOT NULL,
    order_status STRING NOT NULL,
    order_purchase_timestamp TIMESTAMP NOT NULL,
    order_estimated_delivery_date DATE NOT NULL,
    FOREIGN KEY (customer_sk) REFERENCES dim_customers(customer_sk)
);

CREATE OR REPLACE TABLE fact_order_items (
    order_id STRING NOT NULL,
    order_item_id INT NOT NULL,
    product_sk INT NOT NULL,
    seller_sk INT NOT NULL,
    price FLOAT NOT NULL,
    freight_value FLOAT NOT NULL,
    PRIMARY KEY (order_id, order_item_id),
    FOREIGN KEY (order_id) REFERENCES fact_orders(order_id),
    FOREIGN KEY (product_sk) REFERENCES dim_products_scd(product_sk),
    FOREIGN KEY (seller_sk) REFERENCES dim_sellers(seller_sk)
);

CREATE OR REPLACE TABLE fact_payments (
    order_id STRING NOT NULL,
    payment_sequential INT NOT NULL,
    payment_type STRING NOT NULL,
    payment_value FLOAT NOT NULL,
    PRIMARY KEY (order_id, payment_sequential),
    FOREIGN KEY (order_id) REFERENCES fact_orders(order_id)
);

CREATE OR REPLACE TABLE fact_reviews (
    review_id STRING PRIMARY KEY,
    order_id STRING NOT NULL,
    review_score INT NOT NULL,
    review_creation_date DATE NOT NULL,
    FOREIGN KEY (order_id) REFERENCES fact_orders(order_id)
);
