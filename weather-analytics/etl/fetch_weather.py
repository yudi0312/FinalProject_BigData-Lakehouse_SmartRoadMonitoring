"""
ETL Weather Analytics — Phase 4

Fetch data curah hujan dari Open-Meteo API (gratis, tidak butuh API key)
untuk Surabaya, lalu hitung rolling rainfall features:
- rainfall_7d  : total curah hujan 7 hari terakhir
- rainfall_30d : total curah hujan 30 hari terakhir
- rainfall_90d : total curah hujan 90 hari terakhir

Data disimpan ke PostgreSQL tabel weather_data.
"""

import os
import json
import requests
from datetime import date, datetime, timedelta
from dotenv import load_dotenv

load_dotenv()

LAT = float(os.getenv("LATITUDE", "-7.2575"))
LON = float(os.getenv("LONGITUDE", "112.7521"))
TZ = os.getenv("TIMEZONE", "Asia/Jakarta")

OPEN_METEO_URL = "https://api.open-meteo.com/v1/forecast"
OPEN_METEO_ARCHIVE_URL = "https://archive-api.open-meteo.com/v1/archive"


def fetch_recent_weather(past_days: int = 90) -> list[dict]:
    """
    Ambil data cuaca 90 hari terakhir + hari ini dari Open-Meteo Forecast API.
    Ini sudah mencakup data historis sampai 90 hari ke belakang.
    """
    params = {
        "latitude": LAT,
        "longitude": LON,
        "daily": "precipitation_sum,rain_sum,precipitation_hours",
        "timezone": TZ,
        "past_days": past_days,
        "forecast_days": 1,
    }

    resp = requests.get(OPEN_METEO_URL, params=params, timeout=30)
    resp.raise_for_status()
    data = resp.json()

    daily = data.get("daily", {})
    dates = daily.get("time", [])
    precip = daily.get("precipitation_sum", [])
    rain = daily.get("rain_sum", [])
    hours = daily.get("precipitation_hours", [])

    records = []
    for i, d in enumerate(dates):
        records.append({
            "date": d,
            "precipitation_sum": precip[i] if i < len(precip) else None,
            "rain_sum": rain[i] if i < len(rain) else None,
            "precipitation_hours": hours[i] if i < len(hours) else None,
        })

    return records


def fetch_historical_weather(start_date: str, end_date: str) -> list[dict]:
    """
    Ambil data historis dari Open-Meteo Archive API.
    Dipakai kalau butuh data lebih dari 90 hari ke belakang.
    Format date: YYYY-MM-DD
    """
    params = {
        "latitude": LAT,
        "longitude": LON,
        "daily": "precipitation_sum,rain_sum,precipitation_hours",
        "timezone": TZ,
        "start_date": start_date,
        "end_date": end_date,
    }

    resp = requests.get(OPEN_METEO_ARCHIVE_URL, params=params, timeout=30)
    resp.raise_for_status()
    data = resp.json()

    daily = data.get("daily", {})
    dates = daily.get("time", [])
    precip = daily.get("precipitation_sum", [])
    rain = daily.get("rain_sum", [])
    hours = daily.get("precipitation_hours", [])

    records = []
    for i, d in enumerate(dates):
        records.append({
            "date": d,
            "precipitation_sum": precip[i] if i < len(precip) else None,
            "rain_sum": rain[i] if i < len(rain) else None,
            "precipitation_hours": hours[i] if i < len(hours) else None,
        })

    return records


def calculate_rolling_features(records: list[dict]) -> list[dict]:
    """
    Hitung rolling sum curah hujan untuk setiap tanggal.
    rainfall_7d, rainfall_30d, rainfall_90d dihitung dari data sebelumnya.
    """
    # Sort by date ascending
    records = sorted(records, key=lambda x: x["date"])

    # Buat dict untuk lookup cepat
    precip_by_date = {
        r["date"]: (r["precipitation_sum"] or 0.0)
        for r in records
    }

    for rec in records:
        current_date = datetime.strptime(rec["date"], "%Y-%m-%d").date()

        # Hitung rolling sum
        for days, key in [(7, "rainfall_7d"), (30, "rainfall_30d"), (90, "rainfall_90d")]:
            total = 0.0
            for delta in range(days):
                check_date = str(current_date - timedelta(days=delta))
                total += precip_by_date.get(check_date, 0.0)
            rec[key] = round(total, 2)

    return records


def save_to_db(records: list[dict]) -> dict:
    """Simpan records ke PostgreSQL, skip duplikat berdasarkan date."""
    from db.models import WeatherData, SessionLocal, ensure_tables
    from sqlalchemy.exc import IntegrityError

    ensure_tables()
    db = SessionLocal()
    saved, skipped = 0, 0

    try:
        for rec in records:
            existing = db.query(WeatherData).filter(
                WeatherData.date == rec["date"]
            ).first()

            if existing:
                # Update rolling features kalau sudah ada
                existing.rainfall_7d = rec.get("rainfall_7d")
                existing.rainfall_30d = rec.get("rainfall_30d")
                existing.rainfall_90d = rec.get("rainfall_90d")
                skipped += 1
            else:
                weather = WeatherData(
                    date=rec["date"],
                    precipitation_sum=rec.get("precipitation_sum"),
                    rain_sum=rec.get("rain_sum"),
                    precipitation_hours=rec.get("precipitation_hours"),
                    rainfall_7d=rec.get("rainfall_7d"),
                    rainfall_30d=rec.get("rainfall_30d"),
                    rainfall_90d=rec.get("rainfall_90d"),
                )
                db.add(weather)
                saved += 1

        db.commit()
    finally:
        db.close()

    return {"saved": saved, "updated": skipped}


def run_etl(past_days: int = 365) -> list[dict]:
    """
    Jalankan ETL lengkap:
    1. Fetch dari Open-Meteo
    2. Hitung rolling features
    3. Simpan ke DB
    Return list records untuk dipakai producer Kafka/HDFS.
    """
    print(f"[weather] Fetching {past_days} hari data cuaca Surabaya...")

    if past_days <= 90:
        records = fetch_recent_weather(past_days=past_days)
    else:
        # Ambil lebih dari 90 hari pakai Archive API
        end_date = date.today().strftime("%Y-%m-%d")
        start_date = (date.today() - timedelta(days=past_days)).strftime("%Y-%m-%d")
        records = fetch_historical_weather(start_date, end_date)

    print(f"[weather] Didapat {len(records)} records dari API")

    records = calculate_rolling_features(records)
    result = save_to_db(records)
    print(f"[weather] DB: saved={result['saved']}, updated={result['updated']}")

    return records
