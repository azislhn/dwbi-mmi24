# Brazilian E-Commerce Data Mart Builder

This project builds a simple **Data Mart** from a Brazilian e-commerce dataset using **DuckDB** and **SQLite**. It creates two main data marts (monthly sales and daily operations), stores them in SQLite, and provides views for analytical purposes.

## 📦 What It Does

- **Creates data mart tables**:
  - `dm_penjualan_bulanan`: Monthly sales summary by product category and customer city.
  - `dm_operasional_harian`: Daily operational KPIs including freight costs, delivery time, and payment methods.
  
- **Exports to SQLite** for further analysis or dashboard usage.

- **Creates analytical views**:
  - `vw_tren_pendapatan`: Revenue trend per month.
  - `vw_kpi_operasional_mingguan`: Weekly operational KPIs.

## 🧰 Dependencies

Make sure to install the required libraries:

```bash
pip install duckdb pandas

## 🗃️ Input Assumptions

This script assumes you already have the following dimension and fact tables in a DuckDB database named `brazilian_ecommerce.db`:

- `fact_orders`
- `fact_order_items`
- `dim_date`
- `dim_customers`
- `dim_products`
- `dim_payments`

## 📊 Output Tables

### 1. dm_penjualan_bulanan
| year | month | month_name | kategori | kota | jumlah_order | total_penjualan | rata_rata_penjualan |
|------|-------|------------|----------|------|---------------|------------------|----------------------|

### 2. dm_operasional_harian
| tanggal | nama_hari | jumlah_order | total_biaya_pengiriman | rata_rata_waktu_pengiriman | jumlah_metode_pembayaran |
|---------|-----------|---------------|--------------------------|-----------------------------|---------------------------|

### 3. vw_tren_pendapatan
| year | month | month_name | pendapatan_bulanan |
|------|-------|------------|---------------------|

### 4. vw_kpi_operasional_mingguan
| tahun | minggu_ke | awal_minggu | akhir_minggu | total_order | total_biaya_pengiriman | rata2_waktu_pengiriman | rata2_metode_pembayaran |
|-------|-----------|--------------|---------------|--------------|-------------------------|-------------------------|--------------------------|
