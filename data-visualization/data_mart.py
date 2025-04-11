import duckdb
import sqlite3

duck_connect = duckdb.connect(database='brazilian_ecommerce.db', read_only=False)

# Create the data mart tables
duck_connect.execute("""
CREATE OR REPLACE TABLE dm_penjualan_bulanan AS
SELECT
    d.year,
    d.month,
    d.month_name,
    dp.product_category_name_english AS kategori,
    dc.customer_city AS kota,
    COUNT(DISTINCT fo.order_id) AS jumlah_order,
    SUM(fo.sales_amount) AS total_penjualan,
    AVG(fo.sales_amount) AS rata_rata_penjualan
FROM fact_orders fo
JOIN dim_date d ON fo.date_id = d.date_id
JOIN dim_customers dc ON fo.customer_id = dc.customer_id
JOIN fact_order_items foi ON fo.order_id = foi.order_id
JOIN dim_products dp ON foi.product_id = dp.product_id
WHERE fo.order_status = 'delivered'
GROUP BY 1, 2, 3, 4, 5;
""")
dm_penjualan_bulanan = duck_connect.execute("SELECT * FROM dm_penjualan_bulanan").fetchdf()

duck_connect.execute("""
CREATE OR REPLACE TABLE dm_operasional_harian AS
SELECT
    d.date_value AS tanggal,
    d.day_name AS nama_hari,
    COUNT(DISTINCT fo.order_id) AS jumlah_order,
    SUM(foi.freight_value) AS total_biaya_pengiriman,
    AVG(DATE_DIFF('day', fo.order_purchase_timestamp, fo.order_delivered_customer_date)) AS rata_rata_waktu_pengiriman,
    COUNT(DISTINCT dp.payment_type) AS jumlah_metode_pembayaran
FROM fact_orders fo
JOIN dim_date d ON fo.date_id = d.date_id
JOIN fact_order_items foi ON fo.order_id = foi.order_id
LEFT JOIN dim_payments dp ON fo.order_id = dp.order_id
WHERE fo.order_status = 'delivered'
GROUP BY 1, 2;
""")
dm_operasional_harian = duck_connect.execute("SELECT * FROM dm_operasional_harian").fetchdf()

# Create the data mart views
duck_connect.execute("""
CREATE OR REPLACE VIEW vw_tren_pendapatan AS
SELECT 
    year,
    month,
    month_name,
    SUM(total_penjualan) AS pendapatan_bulanan
FROM dm_penjualan_bulanan
GROUP BY year, month, month_name
ORDER BY year, month;
""")
vw_tren_pendapatan = duck_connect.execute("SELECT * FROM vw_tren_pendapatan").fetchdf()

duck_connect.execute("""
CREATE OR REPLACE VIEW vw_kpi_operasional_mingguan AS
SELECT *
FROM dm_operasional_harian
WHERE tanggal >= CURRENT_DATE - INTERVAL 7 DAY
ORDER BY tanggal DESC;
""")
vw_kpi_operasional_mingguan = duck_connect.execute("SELECT * FROM vw_kpi_operasional_mingguan").fetchdf()

sqlite_connect = sqlite3.connect('data_mart.sqlite')
dm_penjualan_bulanan.to_sql('dm_penjualan_bulanan', sqlite_connect, if_exists='replace', index=False)
dm_operasional_harian.to_sql('dm_operasional_harian', sqlite_connect, if_exists='replace', index=False)
vw_tren_pendapatan.to_sql('vw_tren_pendapatan', sqlite_connect, if_exists='replace', index=False)
vw_kpi_operasional_mingguan.to_sql('vw_kpi_operasional_mingguan', sqlite_connect, if_exists='replace', index=False)

duck_connect.close()
sqlite_connect.close()