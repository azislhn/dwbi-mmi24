## 📁 1. Repositori Kode

Implementasi Data Orchestration menggunakan Apache Airflow tersedia di repositori berikut:  
🔗 [https://github.com/azislhn/dwbi-mmi24/tree/main/data-orchestration](https://github.com/azislhn/dwbi-mmi24/tree/main/data-orchestration)

---

## 📚 2. Dokumentasi

### 🗺️ Diagram DAG

#### ETL Pipeline
<img src="etl_pipeline_diagram.jpeg" alt="Diagram DAG ETL" width="auto"/>

#### Data Quality Check
<img src="data_quality_diagram.jpeg" alt="Diagram DAG Data Quality" width="auto"/>

---

### ⏱️ Strategi Penjadwalan

Penjadwalan DAG dilakukan menggunakan parameter `schedule_interval='@daily'`, yang berarti pipeline akan dieksekusi satu kali setiap hari. Alasan pemilihan strategi ini:

- Data transaksi (seperti invoice) bersifat harian, sehingga pembersihan dan analisis cukup dilakukan setiap akhir hari.
- Frekuensi harian cukup untuk kebutuhan monitoring tanpa membebani resource sistem.
- Digunakan bersama `catchup=False` untuk menghindari eksekusi backlog saat DAG pertama kali diaktifkan.

---

### 📅 Strategi Partisi

Meskipun belum menerapkan partisi fisik pada penyimpanan, pipeline telah menggunakan strategi partisi logis berbasis tanggal, seperti `invoice_date`. Strategi ini bermanfaat untuk:

- Memudahkan pemrosesan data dalam rentang waktu tertentu (harian/bulanan).
- Mengoptimalkan performa query dan agregasi data.
- Mempermudah pelacakan anomali berdasarkan waktu.

Jika ke depannya data disimpan di data warehouse atau data lake (misalnya BigQuery, Hive, atau Delta Lake), maka partisi fisik akan diterapkan untuk efisiensi lebih lanjut.

---

### ⚙️ Alasan Pemilihan Fitur Airflow

#### ✅ Error Handling & Retry Mechanism

**Mengapa dipilih?**  
Agar pipeline lebih andal dan tahan terhadap gangguan sementara (misalnya, koneksi database terputus sesaat).

**Implementasi:**  
- Penggunaan parameter `retries` dan `retry_delay` untuk otomatisasi percobaan ulang.
- Task dibungkus dengan blok `try/except` untuk menangkap dan menjelaskan error.

**Manfaat:**  
- Mengurangi kemungkinan kegagalan total pipeline karena error minor.
- Memberikan log error yang informatif untuk proses debugging.

---

#### 📧 Email Notifikasi (Sukses/Gagal)

**Mengapa dipilih?**  
Untuk memberikan visibilitas kepada tim data engineering terkait status eksekusi pipeline.

**Implementasi:**  
- Aktivasi fitur `email_on_failure` dan `email_on_retry` di `default_args`.
- Penambahan task `EmailOperator` untuk notifikasi saat DAG berhasil dijalankan.

**Manfaat:**  
- Memungkinkan respons cepat terhadap kegagalan pipeline.
- Menyediakan log rutin bahwa pipeline berjalan sebagaimana mestinya.

---

## 📸 3. Bukti Eksekusi DAG

#### ETL DAG - Eksekusi Berhasil
<img src="etl_dag_success.jpeg" alt="DAG ETL Success" width="auto"/>

#### Data Quality DAG - Eksekusi Berhasil
<img src="data_quality_dag_success.jpeg" alt="DAG Data Quality Success" width="auto"/>

---

## 📊 4. Rencana Visualisasi
### 🧩 Ringkasan
Pipeline ETL telah berhasil dijalankan menggunakan Apache Airflow, dan data telah dimuat ke dalam Data Warehouse dengan skema Snowflake. Struktur terdiri dari satu tabel fakta utama (`fact_orders`) dan beberapa tabel dimensi seperti `dim_customers`, `dim_products`, `dim_sellers`, `dim_date`, dan `dim_payments`. Data kini telah terstruktur dan siap digunakan untuk keperluan analisis dan visualisasi.

### 🛠️ Alat Visualisasi yang Direkomendasikan: Tableau
Kami merekomendasikan penggunaan Tableau karena:
- Mudah digunakan dengan antarmuka drag-and-drop untuk eksplorasi visual.
- Mendukung koneksi langsung ke berbagai sumber data.
- Mendukung pembuatan visualisasi kompleks seperti tren waktu, heatmap geografis, dan analisis multi-dimensi.
- Cocok untuk membuat dashboard interaktif.

### 📌 Contoh Query untuk Visualisasi

#### 1. Total Penjualan per Bulan
```sql
SELECT  
    d.year,  
    d.month_name,  
    SUM(f.sales_amount) AS total_sales  
FROM fact_orders f  
JOIN dim_date d ON f.date_id = d.date_id  
GROUP BY d.year, d.month_name  
ORDER BY d.year, d.month;
```
#### 2. Top 5 Produk Terlaris
```sql
SELECT  
    p.product_category_name,  
    COUNT(*) AS total_ordered  
FROM fact_order_items foi  
JOIN dim_products p ON foi.product_id = p.product_id  
GROUP BY p.product_category_name  
ORDER BY total_ordered DESC  
LIMIT 5;
```
#### 3. Distribusi Metode Pembayaran
```sql
SELECT  
    dp.payment_type,  
    COUNT(*) AS total_transactions,  
    SUM(dp.payment_value) AS total_payment  
FROM dim_payments dp  
GROUP BY dp.payment_type;
```
#### 4. Rata-Rata Waktu Pengiriman
```sql
SELECT  
    AVG(DATEDIFF(day, order_approved_at, order_delivered_customer_date)) AS avg_delivery_days  
FROM fact_orders  
WHERE order_delivered_customer_date IS NOT NULL;
```