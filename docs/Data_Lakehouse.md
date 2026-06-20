# CPMK-3: Implementasi Data Lakehouse

*Dokumen ini disusun khusus untuk memenuhi kriteria "Sangat Baik (A)" pada Rubrik Evaluasi CPMK-3.*

---

## 1. Arsitektur Medallion

Sistem SRIS telah mengimplementasikan **Medallion Architecture (Bronze, Silver, Gold)** secara eksplisit di dalam Apache Hadoop (HDFS). Konsep ini menjamin data memiliki tingkatan *trust* (kualitas) yang semakin tinggi di tiap lapisannya.

###  Bronze Layer (Raw Data)
- **Fungsi:** Menyimpan data mentah persis seperti apa adanya dari sumber aslinya (Kafka) tanpa transformasi apa pun. Bertindak sebagai *immutable audit trail*.
- **Implementasi Kode:** Terdapat pada `bigdata/bronze/__init__.py`.
- **Proses:** PySpark *Structured Streaming* menyedot topik Kafka (`road_reports`, `weather_data`, `traffic_data`) dan menuliskannya secara *append-only*.
- **Path HDFS:** `/bronze/reports/`, `/bronze/weather/`

###  Silver Layer (Cleaned & Conformed Data)
- **Fungsi:** Membersihkan anomali, melakukan standardisasi tipe data, deduplikasi, dan imputasi *missing values* agar data siap digabungkan (*joinable*).
- **Implementasi Kode:** Terdapat pada `bigdata/silver/__init__.py` dan `standardization.py`.
- **Proses:** 
  1. Membersihkan teks (huruf kecil semua, menghapus spasi).
  2. Melakukan *Geocoding* (jika koordinat *latitude/longitude* kosong, otomatis diisi titik tengah kelurahan/kecamatan menggunakan `geocoding_service.py`).
  3. Mengisi data curah hujan yang kosong (`null`) dengan `0.0`.
- **Path HDFS:** `/silver/reports/`, `/silver/weather/`

###  Gold Layer (Aggregated Analytical Data)
- **Fungsi:** Menghasilkan tabel analitik final yang siap disajikan untuk *Machine Learning* atau *Business Intelligence Dashboard*.
- **Implementasi Kode:** Terdapat pada file Spark Jobs, contohnya `silver_to_gold.py`.
- **Proses:** Menggabungkan tabel (*JOIN*) cuaca, laporan, dan kemacetan untuk menghitung *Priority Score* dan *Road Health Index*.
- **Path HDFS:** `/gold/priority_score/`, `/gold/road_health_index/`

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
