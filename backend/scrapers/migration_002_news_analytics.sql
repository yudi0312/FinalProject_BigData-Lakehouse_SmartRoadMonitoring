-- Migration: Tambah tabel news_analytics untuk Phase 2 News Analytics
-- Jalankan: psql -U sris -d sris_db -f migration_002_news_analytics.sql

CREATE TABLE IF NOT EXISTS news_analytics (
    id              SERIAL PRIMARY KEY,
    source          VARCHAR(50)     NOT NULL,           -- detik / kompas / surabayatoday
    title           TEXT            NOT NULL,
    url             TEXT            UNIQUE NOT NULL,
    published_at    TIMESTAMP,
    road_name       VARCHAR(200),                       -- nama jalan hasil NLP
    district        VARCHAR(100),                       -- kecamatan hasil NLP
    damage_type     VARCHAR(100),                       -- pothole / crack / subsidence / dll
    severity_level  VARCHAR(20),                        -- low / medium / high / critical
    complaint_count INTEGER,                            -- estimasi jumlah keluhan dari teks
    sentiment       VARCHAR(20),                        -- positive / negative / neutral
    scraped_at      TIMESTAMP DEFAULT NOW()
);

-- Index untuk query cepat per jalan / kecamatan
CREATE INDEX IF NOT EXISTS idx_news_road       ON news_analytics(road_name);
CREATE INDEX IF NOT EXISTS idx_news_district   ON news_analytics(district);
CREATE INDEX IF NOT EXISTS idx_news_source     ON news_analytics(source);
CREATE INDEX IF NOT EXISTS idx_news_published  ON news_analytics(published_at DESC);
CREATE INDEX IF NOT EXISTS idx_news_sentiment  ON news_analytics(sentiment);

-- View agregasi per jalan (berguna untuk Phase 8 RHI & Phase 9 RPS)
CREATE OR REPLACE VIEW v_road_news_summary AS
SELECT
    road_name,
    district,
    COUNT(*)                                        AS news_count,
    SUM(CASE WHEN sentiment = 'negative' THEN 1 ELSE 0 END) AS negative_count,
    SUM(CASE WHEN sentiment = 'positive' THEN 1 ELSE 0 END) AS positive_count,
    COALESCE(SUM(complaint_count), 0)               AS total_complaints,
    MAX(published_at)                               AS latest_news,
    -- Severity score numerik untuk feature engineering
    AVG(CASE
        WHEN severity_level = 'critical' THEN 4
        WHEN severity_level = 'high'     THEN 3
        WHEN severity_level = 'medium'   THEN 2
        WHEN severity_level = 'low'      THEN 1
        ELSE 0
    END)                                            AS avg_severity_score,
    -- Dominan damage type
    MODE() WITHIN GROUP (ORDER BY damage_type)      AS dominant_damage_type
FROM news_analytics
WHERE road_name IS NOT NULL
GROUP BY road_name, district;

COMMENT ON TABLE news_analytics IS 'Phase 2 — berita jalan rusak Surabaya hasil scraping';
COMMENT ON VIEW  v_road_news_summary IS 'Agregasi berita per jalan untuk feature engineering';
