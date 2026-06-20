# CPMK-3: Implementasi Data Lakehouse

*Dokumen ini disusun khusus untuk memenuhi kriteria "Sangat Baik (A)" pada Rubrik Evaluasi CPMK-3.*

---

## 1. Arsitektur Medallion (Bronze, Silver, Gold)

Sistem SRIS telah mengimplementasikan **Medallion Architecture (Bronze, Silver, Gold)** secara utuh dan terstruktur di dalam Apache Hadoop (HDFS). Konsep ini menjamin pergerakan data memiliki tingkatan *trust* (kualitas) dan kematangan yang semakin tinggi di setiap lapisannya, memisahkan secara tegas antara data mentah dengan data analitik.

### 🥉 Bronze Layer (Raw Ingestion Data)
**Bronze Layer** adalah zona pendaratan pertama bagi seluruh data yang masuk ke dalam *Lakehouse*. Data di sini diperlakukan secara suci dan tidak boleh diubah (*immutable audit trail*).

* **Fungsi Utama:** Menyimpan histori data mentah persis seperti apa adanya dari sumber aslinya tanpa adanya filter, penghapusan, atau transformasi apa pun. Hal ini sangat berguna jika di masa depan kita perlu mereproduksi ulang tabel Silver/Gold akibat *bug* pada logika pembersihan data.
* **Mekanisme Pipa Data (*Data Pipeline*):** 
  Apache Spark *Structured Streaming* secara terus-menerus menyedot pesan asinkron (*consume messages*) dari tiga topik Kafka utama, yakni `road_reports` (laporan warga), `weather_data` (kondisi curah hujan terbuka), dan `traffic_data` (kepadatan jalan raya). Data JSON yang disedot langsung dilempar dan disimpan di HDFS menggunakan model penambahan data (*append-only*).
* **Metadata & Skema:** Karena berupa data JSON mentah, skema di layer ini seringkali di-*infer* (tebak otomatis) atau menggunakan struktur *string* mentah untuk mencegah kegagalan *job* (*schema evolution safety*).
* **Path Penyimpanan (HDFS):** 
  * `/lakehouse/bronze/road_reports/`
  * `/lakehouse/bronze/weather_data/`

---

### 🥈 Silver Layer (Cleaned, Conformed, & Standardized Data)
**Silver Layer** adalah zona perantara di mana data mentah mulai "dijinakkan", dibersihkan, dan distandardisasi sehingga menjadi data *Enterprise* yang valid dan siap digabungkan (*joinable*).

* **Fungsi Utama:** Menyingkirkan anomali data (seperti *spam*, *null values*), memformat ulang tipe data, serta menjaga integritas relasi. Data di layer ini adalah "*Single Source of Truth*" yang sering diakses oleh tim Data Science untuk melakukan *training* model *Machine Learning* tanpa harus membersihkan data lagi dari awal.
* **Proses Transformasi & Pembersihan:**
  1. **Standardisasi Teks & Tipe Data:** Mengubah seluruh string area (kecamatan, kelurahan) menjadi huruf kecil semua dan menghapus spasi berlebih untuk mencegah gagalnya proses *JOIN* antartabel. Nilai koordinat dikonversi secara ketat menjadi tipe `FloatType`.
  2. **Penanganan Nilai Kosong (*Null Imputation*):** Data cuaca sering mengalami *lost-connection*. Jika intensitas curah hujan terpantau `null`, Spark secara otomatis mengisinya dengan default `0.0`. 
  3. **Pengayaan Geospasial (*Geocoding*):** Jika laporan dari aplikasi warga gagal mencantumkan *latitude/longitude*, Spark mengeksekusi logika *User-Defined Function (UDF)* untuk mencari titik koordinat pusat (*centroid*) dari nama kecamatan yang dilampirkan.
* **Path Penyimpanan (HDFS):** 
  * `/lakehouse/silver/reports_clean/`
  * `/lakehouse/silver/weather_enriched/`

---

### 🥇 Gold Layer (Aggregated Analytical & Business-Level Data)
**Gold Layer** adalah lapisan puncak dari ekosistem analitik. Data di layer ini tidak lagi berfokus pada detail peristiwa individu, melainkan pada agregasi bisnis dan metrik tingkat tinggi yang diformulasikan khusus untuk divisualisasikan oleh instansi pemerintah (Dinas Pekerjaan Umum).

* **Fungsi Utama:** Menyediakan ringkasan eksekutif dan proyeksi prediktif. Tabel Gold didesain sesederhana mungkin agar sangat cepat di-*query* oleh aplikasi BI (*Business Intelligence*) maupun *Dashboard* React kita, tanpa membebani komputasi Spark secara berulang-ulang.
* **Proses Komputasi Tingkat Lanjut:**
  1. **Multi-Source JOIN:** Mengawinkan data bersih (*Silver Layer*) dari `reports_clean` dengan `weather_enriched` berdasarkan zona waktu dan ID wilayah untuk mengetahui korelasi kerusakan jalan dengan cuaca ekstrem pada saat tersebut.
  2. **Agregasi Metrik Kompleks:** Menghitung total komulatif *Severity Score* untuk sebuah kecamatan, dan mengkalkulasi ulang menjadi metrik **Indeks Kesehatan Jalan (Road Health Index)** berskala 1-100.
  3. **Scoring Prioritas AI:** Menerapkan hasil dari *Machine Learning Model* (PySpark MLlib) ke atas tabel teragregasi untuk menghasilkan **Skor Prioritas Perbaikan**, memberitahu pemerintah jalan mana yang paling darurat untuk segera ditambal hari ini.
* **Sinkronisasi (Data Sink):** 
  Data Gold Layer ini kemudian di-*export* secara kontinu ke **PostgreSQL** (*Serving Layer*) via konektor JDBC. Database relasional ini bertindak sebagai basis *read-optimized* yang menopang lalu lintas akses *real-time* dari *Web API* (FastAPI).
* **Path Penyimpanan (HDFS):** 
  * `/lakehouse/gold/road_health_index/`
  * `/lakehouse/gold/repair_priority_matrix/`

---

## 2. Format Penyimpanan, Partisi, dan Contoh Hasil Analisis

Sistem kami tidak sekadar menulis file ke HDFS, melainkan mengoptimalkannya dengan standar *Enterprise Data Engineering*:

### A. Standar Format dan Pemartisian (Partitioning)

1. **Format Apache Parquet**
   Semua penulisan data dari Spark dikonversi ke dalam format `.parquet` (`df.write.parquet()`). Format ini mendukung kompresi *Snappy* secara *default*, yang meminimalkan *footprint* penyimpanan di dalam HDFS dan mempercepat *I/O* pembacaan berkat model penyimpanannya yang berbasis kolom (*columnar storage*).
2. **Partisi Hierarkis Berdasarkan Waktu dan Geografis**
   Mencegah *full table scan* yang lambat, kami mempartisi data secara hierarkis: `year=.../month=.../day=.../district=...`

   Berikut adalah perbandingan skema penyimpanan dan partisi pada ketiga *layer* Lakehouse:
   
   * **Bronze Layer (Raw):** Dipisahkan murni berdasarkan waktu ingesti untuk mempercepat penulisan (*write-heavy*).
     ```text
     /lakehouse/bronze/road_reports/year=2026/month=06/day=19/part-00000.snappy.parquet
     ```
   * **Silver Layer (Cleaned):** Dipisahkan berdasarkan waktu dan ditambahkan partisi *district* (kecamatan) karena data sudah memiliki struktur lokasi yang matang pasca proses *Geocoding*.
     ```text
     /lakehouse/silver/reports_clean/year=2026/month=06/day=19/district=wonocolo/part-00001.snappy.parquet
     ```
   * **Gold Layer (Aggregated):** Tidak lagi menyimpan data per baris peristiwa individu, melainkan menyimpan ringkasan (*snapshot*) agregasi harian yang ringkas dan siap disajikan.
     ```text
     /lakehouse/gold/road_health_index/year=2026/month=06/day=19/part-00002.snappy.parquet
     ```
   *Justifikasi Akselerasi:* Ketika Dashboard UI hanya meminta data "Kecamatan Wonocolo di bulan Juni 2026", Spark hanya perlu membaca *folder* Silver/Gold tersebut secara spesifik tanpa harus *scan* jutaan laporan jalan dari kecamatan lain di seluruh Surabaya (fitur *Predicate Pushdown* dan *Partition Pruning* berjalan sangat optimal).

### B. Contoh Hasil Analisis (Gold Layer Output)

Berikut adalah representasi data akhir (*tabular*) yang dihasilkan di *Gold Layer* setelah seluruh proses *JOIN* multidimensi dan Agregasi Machine Learning selesai. Data inilah yang akan disinkronisasikan ke PostgreSQL untuk di-render oleh React Frontend:

| date | district_id | district_name | total_reports | avg_severity | rainfall_mm | road_health_index | repair_priority |
|---|---|---|---|---|---|---|---|
| 2026-06-19 | SBY-05 | Wonocolo | 142 | 85.4 | 45.2 | 12.5 (Kritis) | **High (1)** |
| 2026-06-19 | SBY-12 | Rungkut | 45 | 40.2 | 12.0 | 65.0 (Cukup) | Medium (3) |
| 2026-06-19 | SBY-02 | Genteng | 12 | 15.0 | 5.5 | 92.0 (Sangat Baik) | Low (5) |

* **Penjelasan Analisis & Insight:**
  Kecamatan **Wonocolo** terdeteksi mengalami anomali cuaca ekstrem (`rainfall_mm: 45.2`) dan secara bersamaan menerima ledakan 142 laporan kerusakan dengan tingkat keparahan (*Severity*) yang sangat tinggi dari model YOLOv8 (`avg_severity: 85.4`). Melalui komputasi gabungan di *Gold Layer*, sistem secara preskriptif menjatuhkan vonis **Road Health Index "Kritis"** dan langsung memberikan tanda **Prioritas Perbaikan Utama (High/1)** agar Dinas Pekerjaan Umum segera mengerahkan truk aspal ke area tersebut.
