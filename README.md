# Nama dan Sumber Dataset:
Nama: Brazilian E-Commerce Public Dataset by Olist <br>
Sumber: Dataset ini disediakan oleh Olist, sebuah perusahaan e-commerce asal Brasil, dan diunggah ke Kaggle oleh tim Olist. Dataset ini menyajikan data transaksi e-commerce di Brasil dari tahun 2016 hingga 2018. <br><br>
<b>Struktur Data (Skema dan Format):</b> <br>
Dataset terdiri dari 9 file CSV yang saling terhubung, menggambarkan aktivitas lengkap dalam ekosistem e-commerce: <br>

| Nama File                            | Deskripsi                                                         | Jumlah Kolom |
|-------------------------------------|-------------------------------------------------------------------|--------------|
| olist_orders_dataset.csv            | Informasi pesanan (order ID, status, waktu pemesanan, dll.)       | 8 kolom      |
| olist_customers_dataset.csv         | Data pelanggan (ID, lokasi, dll.)                                 | 5 kolom      |
| olist_order_items_dataset.csv       | Detail produk dalam setiap pesanan                                | 7 kolom      |
| olist_products_dataset.csv          | Informasi produk (kategori, berat, ukuran, dll.)                  | 9 kolom      |
| olist_sellers_dataset.csv           | Data penjual (ID, lokasi, dll.)                                   | 4 kolom      |
| olist_order_reviews_dataset.csv     | Ulasan pelanggan terhadap pesanan                                 | 7 kolom      |
| olist_order_payments_dataset.csv    | Detail pembayaran (jumlah cicilan, metode bayar, nilai pembayaran)| 5 kolom      |
| product_category_name_translation.csv | Terjemahan kategori produk dari Portugis ke Inggris              | 2 kolom      |
| olist_geolocation_dataset.csv       | Data lokasi berdasarkan koordinat (zipcode, lat/lon)              | 5 kolom      |
<br>
<b>Volume Data (Jumlah Record):</b><br>

| Nama File                            | Jumlah Record (Baris) |
|-------------------------------------|------------------------|
| olist_orders_dataset.csv            | 99.441                |
| olist_customers_dataset.csv         | 99.441                |
| olist_order_items_dataset.csv       | 112.650               |
| olist_products_dataset.csv          | 32.951                |
| olist_sellers_dataset.csv           | 3.099                 |
| olist_order_reviews_dataset.csv     | 100.000               |
| olist_order_payments_dataset.csv    | 103.886               |
| product_category_name_translation.csv | 71                  |
| olist_geolocation_dataset.csv       | 1.000.016             |
<br>
<img src="data-relation.png" alt="Relasi data" width="auto"/>

# Script ETL Python/pandas
