# Smart Road Intelligence System (SRIS) - Crowdsourcing Module

Website modern untuk mengumpulkan laporan jalan rusak dari masyarakat Kota Surabaya.

## Tech Stack

- Frontend: React, Vite, Tailwind CSS, Leaflet
- Backend: FastAPI, SQLAlchemy, PostgreSQL
- Storage: local upload folder untuk foto laporan
- Deployment: Docker Compose

## Struktur Folder

```text
sris-crowdsourcing/
├── backend/
│   ├── app/
│   │   ├── database.py
│   │   ├── main.py
│   │   ├── models.py
│   │   └── schemas.py
│   ├── uploads/
│   ├── Dockerfile
│   └── requirements.txt
├── database/
│   └── schema.sql
├── frontend/
│   ├── src/
│   │   ├── components/
│   │   ├── pages/
│   │   ├── App.jsx
│   │   ├── main.jsx
│   │   └── index.css
│   ├── Dockerfile
│   ├── index.html
│   ├── package.json
│   ├── postcss.config.js
│   ├── tailwind.config.js
│   └── vite.config.js
├── docker-compose.yml
└── README.md
```

## Menjalankan dengan Docker

```bash
docker compose up --build
```

Layanan:

- Frontend: http://localhost:5173
- Backend API: http://localhost:8000
- Swagger Docs: http://localhost:8000/docs
- PostgreSQL: localhost:5432

## Integrasi YOLOv8

Letakkan model road damage detection pada:

```text
backend/models/best.pt
```

Model di-load sekali saat FastAPI startup melalui `backend/app/services/yolo_service.py`. Setiap laporan yang masuk akan:

1. Menyimpan foto ke `backend/uploads`.
2. Menjalankan YOLOv8 untuk semua foto yang diupload.
3. Memilih prediksi dengan confidence tertinggi.
4. Menghitung `severity_score` dari mapping class.
5. Menyimpan `damage_type`, `confidence`, dan `severity_score` ke PostgreSQL.
6. Menampilkan hasilnya pada dashboard admin dan popup peta.

Class model:

```python
{
    0: "D00_Longitudinal_Crack",
    1: "D10_Transverse_Crack",
    2: "D20_Alligator_Crack",
    3: "D40_Pothole",
    4: "D50_Other_Damage"
}
```

Severity mapping:

```python
{
    "D00_Longitudinal_Crack": 40,
    "D10_Transverse_Crack": 50,
    "D20_Alligator_Crack": 75,
    "D40_Pothole": 100,
    "D50_Other_Damage": 30
}
```

Jika tidak ada deteksi, sistem menyimpan:

```json
{
  "damage_type": "Unknown",
  "confidence": 0,
  "severity_score": 0
}
```

Jika file model tidak ditemukan atau inferensi gagal, API mengembalikan HTTP `503` dengan detail error.

## Endpoint API

### `POST /reports`

Menerima multipart form:

- `reporter_name`
- `email` opsional
- `road_name`
- `district`
- `village`
- `description`
- `latitude`
- `longitude`
- `photos` maksimal 5 file

Contoh response:

```json
{
  "report_id": "R001",
  "damage_type": "D40_Pothole",
  "confidence": 0.89,
  "severity_score": 100
}
```

### `GET /reports`

Mengambil seluruh laporan untuk dashboard admin.

### `GET /stats`

Mengambil statistik dashboard.

### `POST /predict`

Endpoint AI Road Damage Detection menggunakan YOLOv8.

Response:

```json
{
  "damage_type": "pothole",
  "severity_score": 82,
  "confidence": 0.91
}
```

## Konfigurasi Environment

Backend membaca `DATABASE_URL`.

Default Docker:

```env
postgresql+psycopg2://sris:sris_password@db:5432/sris_db
```

Frontend membaca:

```env
VITE_API_BASE_URL=http://localhost:8000
```

Opsional:

```env
UPLOAD_DIR=/app/uploads
```

## Migration Database

Untuk database yang sudah ada, jalankan:

```bash
psql -U sris -d sris_db -f database/migration_001_add_ai_detection_columns.sql
```

Docker Compose database baru otomatis membaca `database/schema.sql`.

## Best Practice Production

- Simpan `best.pt` di `backend/models/best.pt` sebelum build container backend.
- Jangan load YOLO di setiap request; service `yolo_service.py` memakai singleton dan `load_model()` dipanggil saat startup.
- Gunakan volume persistent untuk `backend/uploads`.
- Untuk traffic tinggi, jalankan inference worker/GPU service terpisah atau batasi jumlah worker Uvicorn agar memori model tidak digandakan berlebihan.
