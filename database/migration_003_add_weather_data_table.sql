-- Migration 003: tabel untuk Weather Analytics (Phase 4)
-- Jalankan dengan:
--   Get-Content database/migration_003_add_weather_data_table.sql | docker exec -i sris_postgres psql -U sris -d sris_db

CREATE TABLE IF NOT EXISTS weather_data (
    id SERIAL PRIMARY KEY,
    date DATE NOT NULL UNIQUE,
    precipitation_sum FLOAT,        -- total curah hujan hari itu (mm)
    rain_sum FLOAT,                 -- curah hujan murni (mm), tanpa salju
    precipitation_hours FLOAT,     -- jam hujan per hari
    rainfall_7d FLOAT,             -- rolling sum 7 hari terakhir
    rainfall_30d FLOAT,            -- rolling sum 30 hari terakhir
    rainfall_90d FLOAT,            -- rolling sum 90 hari terakhir
    created_at TIMESTAMP DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_weather_date ON weather_data(date);

-- View: ambil feature cuaca terbaru (untuk join di Feature Engineering Phase 7)
CREATE OR REPLACE VIEW latest_weather AS
SELECT *
FROM weather_data
WHERE date = (SELECT MAX(date) FROM weather_data);
