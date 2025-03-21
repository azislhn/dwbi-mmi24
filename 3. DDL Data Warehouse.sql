-- DDL Script untuk Fact Constellation Schema - Brazilian E-Commerce Public Dataset
-- Dengan Fakta Pemesanan dan Fakta Item Pemesanan

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

-- 
CREATE OR REPLACE TABLE dim_products (
    produk_key INT PRIMARY KEY,
    product_id VARCHAR(50) NOT NULL,
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

CREATE OR REPLACE TABLE dim_date (
    date_id INT PRIMARY KEY,  -- Format YYYYMMDD
    date_value DATE NOT NULL,
    day INT NOT NULL,
    month INT NOT NULL,
    month_name VARCHAR(20),
    year INT NOT NULL,
    day_of_week INT,
    day_name VARCHAR(20),
    quarter INT NOT NULL
);

CREATE OR REPLACE TABLE dim_payments (
    payment_id INT PRIMARY KEY,
    order_id VARCHAR(50) NOT NULL,
    payment_sequential INT,
    payment_type VARCHAR(50),
    payment_installments INT,
    payment_value DECIMAL(10,2),
    FOREIGN KEY (order_id) REFERENCES fact_orders(order_id)
);

-- Tabel Fakta
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

CREATE OR REPLACE TABLE fact_order_items (
    order_id VARCHAR(50) NOT NULL,
    order_item_id INT NOT NULL,
    seller_id VARCHAR(50) NOT NULL,
    product_id VARCHAR(50) NOT NULL,
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
