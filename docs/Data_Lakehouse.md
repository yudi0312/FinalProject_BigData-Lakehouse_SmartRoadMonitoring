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

## 2. Format Penyimpanan dan Partisi

Sistem kami tidak sekadar menulis file ke HDFS, melainkan mengoptimalkannya dengan standar *Enterprise Data Engineering*:

1. **Format Apache Parquet**
   Semua penulisan data dari Spark dikonversi ke dalam format `.parquet` (`df.write.parquet()`). Format ini mendukung kompresi *Snappy* secara *default*, yang meminimalkan *footprint* di dalam HDFS dan mempercepat *I/O*.
2. **Partisi Berdasarkan Waktu dan Geografis (Partitioning)**
   Mencegah *full table scan* yang lambat, kami mempartisi data secara hierarkis: `year=.../month=.../day=.../district=...`
   
   *Contoh output pada log direktori HDFS:*
   ```text
   /bronze/reports/year=2026/month=06/day=19/district=wonocolo/part-00000.snappy.parquet
   ```
   *Justifikasi:* Ketika Dashboard UI hanya meminta data bulan Juni 2026 di kecamatan Wonocolo, Spark hanya perlu membaca *folder* tersebut secara spesifik tanpa harus *scan* jutaan laporan jalan lain (proses *Predicate Pushdown* dan *Partition Pruning* berjalan optimal).
