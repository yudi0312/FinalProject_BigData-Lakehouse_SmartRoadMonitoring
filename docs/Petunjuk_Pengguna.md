# Petunjuk Pengguna: Smart Road Intelligence System (SRIS)

Selamat datang di panduan teknis **Smart Road Intelligence System (SRIS)**. Sistem ini merupakan *crowdsourcing module* cerdas berbasis *Big Data Lakehouse* untuk pelaporan jalan rusak di Surabaya. Dokumen ini menjelaskan tata cara penyiapan, *deployment*, dan pengujian sistem.

## Alur Kerja Keseluruhan (Flow of the System)

1. **Persiapan Model AI:** Anda harus memiliki model Computer Vision YOLOv8 (`best.pt`) yang sudah dilatih untuk mendeteksi *pothole* dan *crack*. Model ini ditempatkan di dalam folder backend.
2. **Build Infrastruktur:** Sistem berjalan di atas sekumpulan kontainer Docker. Perintah *docker compose* akan secara otomatis membangun *database* PostgreSQL, *backend* FastAPI, dan *frontend* React.
3. **Pelaporan oleh Pengguna:** Masyarakat membuka website (Frontend) untuk mengisi formulir dan mengunggah foto jalan rusak.
4. **Validasi & Ekstraksi AI:** Backend menerima *request*, menyimpan foto secara lokal, dan langsung melakukan inferensi AI menggunakan model YOLOv8 untuk mendapatkan `severity_score` dan jenis kerusakan.
5. **Penyimpanan Operasional:** Data hasil validasi AI disimpan di *database* relasional PostgreSQL agar bisa dibaca langsung oleh fitur pemetaan (*Leaflet*) di UI.
6. **Integrasi Big Data (Background):** Data tersebut juga akan didorong ke *Apache Kafka*, diserap oleh *Apache Spark*, dan dimurnikan di dalam HDFS (Hadoop) hingga menghasilkan agregasi prediksi *Machine Learning* yang finalnya bisa dianalisis via *Dashboard Analytics*.

---

## 1. Persiapan Environment & Struktur Folder

Pastikan struktur *folder* aplikasi Anda seperti ini sebelum memulai:

```text
sris-crowdsourcing/
├── backend/
│   ├── app/
│   │   ├── services/
│   │   │   └── yolo_service.py
│   │   ├── database.py
│   │   ├── main.py
│   │   ├── models.py
│   │   └── schemas.py
│   ├── models/
│   │   └── best.pt            <-- [PENTING] Letakkan model YOLOv8 di sini
│   ├── uploads/               <-- Tempat foto laporan tersimpan otomatis
│   ├── Dockerfile
│   └── requirements.txt
├── database/
│   └── schema.sql             <-- Skema awal PostgreSQL
├── frontend/
│   ├── src/
│   ├── Dockerfile
│   ├── package.json
│   └── vite.config.js
└── docker-compose.yml
```

### Konfigurasi Variabel
Secara bawaan (*default*), sistem sudah terkonfigurasi. Namun jika ingin melakukan modifikasi, berikut adalah parameter pentingnya:

**Backend Environment:**
```env
DATABASE_URL=postgresql+psycopg2://sris:sris_password@db:5432/sris_db
UPLOAD_DIR=/app/uploads
```

**Frontend Environment:**
```env
VITE_API_BASE_URL=http://localhost:8000
```

---

## 2. Menjalankan Sistem dengan Docker Compose

Untuk menyalakan *frontend*, *backend*, dan *database* sekaligus dalam satu perintah:

```bash
docker compose up -d --build
```

Tunggu hingga proses instalasi (NPM dan PIP) selesai. Setelah semua *container* berjalan dengan status `Up`, Anda dapat mengakses layanan melalui *browser*:

- **Antarmuka Website (Frontend):** [http://localhost:5173](http://localhost:5173)
- **API Server (Backend):** [http://localhost:8000](http://localhost:8000)
- **Dokumentasi API Interaktif (Swagger):** [http://localhost:8000/docs](http://localhost:8000/docs)
- **PostgreSQL Database:** `localhost:5432`

---

## 3. Integrasi & Perilaku Model YOLOv8

Model **Computer Vision** akan dimuat (*load*) secara *singleton* satu kali saja saat *server* FastAPI menyala melalui file `backend/app/services/yolo_service.py` untuk menghemat RAM.

Setiap kali ada laporan baru masuk:
1. *Endpoint* akan menyimpan salinan foto fisik ke `backend/uploads/`.
2. YOLOv8 memindai foto dan memilih tingkat *confidence* tertinggi.
3. Jenis kerusakan dipetakan ke tingkat keparahan (*severity score*):
   - `D40_Pothole` -> **100** (Paling Parah)
   - `D20_Alligator_Crack` -> **75**
   - `D10_Transverse_Crack` -> **50**
   - `D00_Longitudinal_Crack` -> **40**
   - `D50_Other_Damage` -> **30**
4. Jika tidak ditemukan kerusakan (misal warga memfoto langit), AI mengembalikan:
   `{"damage_type": "Unknown", "confidence": 0, "severity_score": 0}`

*(Catatan: Jika file `best.pt` tidak ada di folder, sistem tidak akan crash, melainkan mengembalikan error HTTP `503 Service Unavailable` khusus untuk deteksi AI).*

---

## 4. Dokumentasi Endpoint API Utama

Jika Anda ingin melakukan pengujian tanpa melalui UI React, Anda bisa menggunakan `curl` atau *Postman*:

### `POST /reports` (Pengiriman Laporan)
Digunakan untuk mengirim data kerusakan. Membutuhkan format *multipart/form-data*.
**Parameter:**
- `reporter_name` (Text)
- `road_name` (Text)
- `district` (Text)
- `village` (Text)
- `description` (Text)
- `latitude` (Float)
- `longitude` (Float)
- `photos` (File gambar, maks 5)

**Response Sukses:**
```json
{
  "report_id": "R001",
  "damage_type": "D40_Pothole",
  "confidence": 0.89,
  "severity_score": 100
}
```

### `GET /reports` (Tabel Peta / Operasional)
Menarik semua histori laporan mentah untuk disajikan pada *Leaflet Maps*.

### `GET /stats` (Dashboard Metrik)
Merekap jumlah total kerusakan, statistik ringan untuk visualisasi bar atas *dashboard*.

### `POST /predict` (Test-bed AI)
Hanya untuk mengetes model YOLO tanpa memasukkannya ke dalam *database* PostgreSQL.

---

## 5. Best Practices untuk Tahap Production (Server Nyata)

Jika sistem ini akan dibawa ke tahap produksi berskala kota besar:
1. **Pemisahan GPU/Inference Server:** Menjalankan model PyTorch di kontainer Uvicorn yang sama dengan web API bisa memakan banyak memori (*memory leak*). Pisahkan *service* inferensi menggunakan antrean asinkron (misal Celery/Redis) pada mesin dengan *GPU dedicated*.
2. **Persistent Storage AWS/MinIO:** Jangan simpan foto secara statis di `backend/uploads/`. *Mounting volume* lokal di Docker rentan korup. Integrasikan ke layanan *Object Storage* (S3).
3. **Kafka Partitioning:** Ketika laporan mencapai ribuan per detik, atur tingkat partisi pada topik Kafka menjadi 3 atau 5 agar klaster *Spark worker* dapat mengunyah data pelaporan secara serentak.
