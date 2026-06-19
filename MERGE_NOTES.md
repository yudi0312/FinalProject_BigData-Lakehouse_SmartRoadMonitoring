# Catatan Penggabungan (Merge Notes)

Dokumen ini menjelaskan bagaimana isi `mergesendiri.zip` (lakehouse baru) dan
`FinalProject_BigData-Lakehouse_SmartRoadMonitoring-main.zip` (project lama,
berisi kode bronze/silver/gold/optimize sebelumnya) digabungkan.

## Dasar Penggabungan

- **Base/struktur utama** = `mergesendiri.zip`, karena ini adalah lakehouse
  terbaru: punya folder `bigdata/lakehouse/{bronze,silver,gold}` dan pipeline
  batch baru `kafka_to_bronze.py → bronze_to_silver.py → silver_to_gold.py`.
- `backend/`, `frontend/`, `database/`, `docker-compose*.yml`, `README.md`,
  `BIGDATA_SETUP.md` **identik** di kedua zip (sudah dicek dengan diff), jadi
  tidak ada konflik di bagian itu — langsung dipakai apa adanya (file di
  `backend/uploads` dan `backend/models` dari mergesendiri dipertahankan karena
  lebih baru/lengkap).

## File yang Ditambahkan dari Project Lama (tidak ada konflik nama)

File-file ini **tidak ada** di `mergesendiri.zip`, jadi langsung disalin masuk:

| File | Fungsi |
|---|---|
| `bigdata/config.py` | Konfigurasi terpusat (Kafka topics, HDFS paths, Postgres) untuk layer Bronze/Silver/Gold |
| `bigdata/bronze/__init__.py` | Class `BronzeLayer` — ingestion Kafka→HDFS versi class-based |
| `bigdata/silver/__init__.py` | Class `SilverLayer` — cleaning/standardization/geocoding versi class-based |
| `bigdata/silver/standardization.py` | Service standardisasi data (UDF) |
| `bigdata/silver/geocoding_service.py` | Service geocoding untuk data laporan jalan |
| `bigdata/scripts/verify_lakehouse.sh` | Script verifikasi isi lakehouse di HDFS |
| `bigdata/kafka/producers/accident_producer.py` | Producer Kafka untuk topic `accident_data` (tidak ada di mergesendiri) |
| `CHANGES_SUMMARY.md` | Riwayat perubahan dari project lama |

⚠️ **Perlu disambungkan secara manual oleh kamu**: `bronze/__init__.py` dan
`silver/__init__.py` melakukan `from config import ...` dan
`from silver.geocoding_service import ...` — ini mengasumsikan working
directory `bigdata/` ada di `sys.path` (akan otomatis benar kalau dijalankan
dengan `spark-submit` dari dalam folder `bigdata/`, atau set `PYTHONPATH`).
Kalau mau dipanggil dari pipeline `silver_to_gold.py` yang baru, perlu
ditambahkan import eksplisit — saat ini ketiga script baru
(`kafka_to_bronze.py`, `bronze_to_silver.py`, `silver_to_gold.py`) berdiri
sendiri (standalone) dan **belum** memanggil `BronzeLayer`/`SilverLayer` ini.

## ⚠️ Konflik yang Ditemukan: Dua Arsitektur Gold Layer Berbeda

Ini bagian paling penting untuk kamu putuskan. Ternyata project lama dan
`mergesendiri.zip` punya **dua pipeline gold yang beda total** dan **menulis
ke tabel Postgres dengan nama sama tapi skema kolom berbeda**:

### 1. Pipeline BARU (dipakai sebagai default di hasil gabungan ini)
File: `bigdata/spark/jobs/{kafka_to_bronze,bronze_to_silver,silver_to_gold}.py`
- Model: **batch**, baca dari `lakehouse/silver/*`, hitung semua metrik gold
  sekaligus dalam satu file (`silver_to_gold.py`): Road Health Index, Priority
  Score, Damage Prediction, Accident Prediction, **+ Hotspot Analysis**.
- Skema tabel mengikuti `bigdata/sql/create_lakehouse_tables.sql` (kolom:
  `road_health_index(severity, rainfall, traffic, road_age_score)`,
  `priority_score(traffic_score, accident_score, complaint_score, news_score)`,
  `damage_prediction(predicted_risk_level)`,
  `accident_prediction(predicted_accident_probability, risk_level)`,
  plus tabel baru `hotspot_analysis`).

### 2. Pipeline LAMA (dipindah ke `bigdata/spark/jobs/streaming_legacy/`)
File: `road_health_index.py`, `damage_prediction.py`, `priority_score.py`,
`accident_prediction.py` di dalam folder `streaming_legacy/`.
- Model: **real-time streaming** langsung dari Kafka (`spark.readStream`),
  dengan partisi `year/month/day/country/province/city` saat ditulis ke
  Parquet, dan feature engineering lebih detail (mis. `accident_prediction.py`
  join weather+traffic+road health secara real-time).
- Skema tabel beda (lihat `streaming_legacy/create_lakehouse_tables_streaming.sql`):
  `road_health_index(report_count, average_severity, pothole_count, crack_count)`,
  `priority_score` & `damage_prediction` punya kolom `confidence`, `status`, dst,
  plus tabel tambahan `data_quality_metrics` dan `etl_logs` yang tidak ada di
  pipeline baru.

**Karena nama tabel Postgres-nya sama tapi struktur kolom berbeda, kedua
pipeline ini TIDAK BISA dijalankan bersamaan menulis ke database yang sama
tanpa konflik skema.** Saya tidak mengubah/menggabungkan paksa skemanya
karena itu keputusan desain yang sebaiknya kamu yang tentukan. Yang saya
lakukan:
- Pipeline baru (`mergesendiri`) saya jadikan default karena kamu bilang
  "ini lakehousenya".
- Pipeline lama saya **simpan utuh** di `streaming_legacy/` (tidak dihapus)
  supaya tidak ada kode yang hilang, dan kamu bisa pilih salah satu, atau
  gabungkan manual (mis. ganti nama tabel jadi `road_health_index_streaming`,
  dst, kalau memang mau jalan dua-duanya).

## Spark Jobs yang Identik / Tidak Berubah
- `bigdata/kafka/producers/{news,report,traffic,weather}_producer.py` — identik di kedua zip.
- `bigdata/spark/sql/create_bigdata_tables.sql` (tabel bronze) — identik di kedua zip.
- `bigdata/hdfs/config/*.xml`, `bigdata/docker/` — identik.

## Cek yang Sudah Dilakukan
- Semua file `.py` di hasil gabungan ini sudah dicek **valid secara syntax**
  (`ast.parse` + `py_compile`, tidak ada error).
- Script `bigdata/scripts/verify_lakehouse.sh` sudah dicek valid secara syntax bash (`bash -n`).
- Tidak menjalankan job Spark secara aktual (butuh cluster Kafka/HDFS/Postgres
  yang tidak tersedia di sandbox ini), jadi validasi runtime tetap perlu kamu
  lakukan di environment Docker kamu sendiri.
