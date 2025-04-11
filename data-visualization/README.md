# **Data Mart E-Commerce Brasil**  

## **Gambaran Umum**  
Skrip ini membangun *data mart* untuk analisis data e-commerce Brasil menggunakan **DuckDB** (pemrosesan) dan **SQLite** (penyimpanan). Dibuat dua tabel fakta dan dua *view* analitis untuk melacak tren penjualan dan KPI operasional.  

## **Fitur**  
### **1. Tabel Data Mart**  
- **`dm_penjualan_bulanan`** (Penjualan Bulanan):  
  - Melacak penjualan berdasarkan tahun, bulan, kategori produk, dan kota pelanggan.  
  - Metrik: jumlah pesanan, total penjualan, rata-rata penjualan.  
- **`dm_operasional_harian`** (Operasional Harian):  
  - Memantau volume pesanan harian, biaya pengiriman, waktu pengiriman, dan metode pembayaran.  

### **2. View Analitis**  
- **`vw_tren_pendapatan`** (Tren Pendapatan):  
  - Menampilkan pendapatan bulanan untuk analisis tren.  
- **`vw_kpi_operasional_mingguan`** (KPI Operasional Mingguan):  
  - Menyajikan metrik operasional 7 hari terakhir.  

## **Penyiapan & Eksekusi**  
1. **Dependensi**:  
   ```bash
   pip install duckdb sqlite3 pandas
   ```  
2. **Jalankan skrip**:  
   ```python
   python data_mart_builder.py
   ```  
3. **Output**:  
   - Data diproses dalam **`brazilian_ecommerce.db`** (DuckDB).  
   - *Data mart* akhir disimpan di **`data_mart.db`** (SQLite).  

## **Cara Penggunaan**  
Akses database SQLite untuk analisis:  
```sql
-- Contoh: Lihat tren pendapatan bulanan
SELECT * FROM vw_tren_pendapatan;
```  

## **Detail Skema**  
- **Tabel Sumber**:  
  - `fact_orders`, `dim_date`, `dim_customers`, `fact_order_items`, `dim_products`, `dim_payments`  
- **Filter**: Hanya pesanan **terkirim** (`order_status = 'delivered'`).  
