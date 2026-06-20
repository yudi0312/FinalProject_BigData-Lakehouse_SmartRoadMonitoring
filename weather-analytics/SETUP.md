# Setup Weather Analytics Module

## Requirement
- Python 3.10+
- Docker (Postgres & Kafka harus jalan dari `docker compose up -d`)
- Tidak butuh API key — Open-Meteo gratis

---

## Instalasi (Windows)

### Step 1 — Masuk ke folder
```bash
cd weather-analytics
```

### Step 2 — Install dependencies
```bash
pip install -r requirements.txt
```

### Step 3 — Setup environment
```bash
cp .env.example .env
```
Isi `.env` sudah diset default sesuai `docker-compose.yml` project ini, tidak perlu diubah.

### Step 4 — Jalankan migration database (cukup sekali)
```powershell
Get-Content ../database/migration_003_add_weather_data_table.sql | docker exec -i sris_postgres psql -U sris -d sris_db
```

---

## Instalasi (Linux/Mac)
```bash
cd weather-analytics
pip install -r requirements.txt
cp .env.example .env
psql -U sris -d sris_db -f ../database/migration_003_add_weather_data_table.sql
```

---

## Menjalankan

### Default — loop otomatis setiap 2 jam (recommended)
```bash
python main.py
```

### Sekali jalan saja
```bash
python main.py --no-loop
```

### Skip Kafka kalau belum ready
```bash
python main.py --skip-kafka
```

### Ambil lebih banyak data historis (2 tahun)
```bash
python main.py --days 730 --no-loop
```

---

## Cek Data di Database

```powershell
# Total data cuaca
docker exec -it sris_postgres psql -U sris -d sris_db -c "SELECT COUNT(*) FROM weather_data;"

# Sample data terbaru
docker exec -it sris_postgres psql -U sris -d sris_db -c "SELECT date, precipitation_sum, rainfall_7d, rainfall_30d, rainfall_90d FROM weather_data ORDER BY date DESC LIMIT 10;"

# Cuaca hari ini
docker exec -it sris_postgres psql -U sris -d sris_db -c "SELECT * FROM latest_weather;"
```

## Cek Data di Kafka

```powershell
docker exec -it sris_kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic weather_data --from-beginning --max-messages 3
```

---

## Struktur Output

Tabel `weather_data` di PostgreSQL (`sris_db`):

| Kolom | Keterangan |
|---|---|
| date | Tanggal (unique) |
| precipitation_sum | Total curah hujan hari itu (mm) |
| rain_sum | Curah hujan murni (mm) |
| precipitation_hours | Jam hujan per hari |
| rainfall_7d | Rolling sum 7 hari — **dipakai di RHI & Feature Engineering** |
| rainfall_30d | Rolling sum 30 hari |
| rainfall_90d | Rolling sum 90 hari |

Kafka Topic: `weather_data`

---

## Troubleshooting

### Error: password authentication failed
Ada PostgreSQL lain yang jalan di port 5432. Stop dulu (PowerShell as Administrator):
```powershell
Stop-Service -Name "postgresql-x64-*"
```

### Error: kafka module not found
```bash
pip uninstall kafka-python -y
pip install kafka-python-ng
```

### Error: SQLAlchemy TypeError
```bash
pip install "SQLAlchemy>=2.0.36" --upgrade
```