-- ================================================
-- SILVER LAYER TABLES
-- Cleaned and standardized data from Bronze Layer
-- ================================================

CREATE TABLE IF NOT EXISTS silver_reports (
    id BIGSERIAL PRIMARY KEY,
    event_id VARCHAR(100),
    event_time TIMESTAMP,
    report_id VARCHAR(32) UNIQUE,
    road_name VARCHAR(180),
    district VARCHAR(120),
    village VARCHAR(120),
    latitude DOUBLE PRECISION,
    longitude DOUBLE PRECISION,
    damage_type VARCHAR(100),
    severity_score INTEGER,
    confidence FLOAT,
    status VARCHAR(40),
    ingestion_timestamp TIMESTAMP,
    source VARCHAR(100),
    processed_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    data_quality_score FLOAT
);

CREATE INDEX IF NOT EXISTS idx_silver_reports_district ON silver_reports (district);
CREATE INDEX IF NOT EXISTS idx_silver_reports_event_time ON silver_reports (event_time DESC);
CREATE INDEX IF NOT EXISTS idx_silver_reports_damage_type ON silver_reports (damage_type);

CREATE TABLE IF NOT EXISTS silver_weather (
    id BIGSERIAL PRIMARY KEY,
    event_id VARCHAR(100),
    event_time TIMESTAMP,
    city VARCHAR(100),
    district VARCHAR(120),
    rainfall_mm FLOAT,
    temperature_c FLOAT,
    humidity FLOAT,
    condition VARCHAR(100),
    ingestion_timestamp TIMESTAMP,
    source VARCHAR(100),
    processed_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_silver_weather_district ON silver_weather (district);
CREATE INDEX IF NOT EXISTS idx_silver_weather_event_time ON silver_weather (event_time DESC);

CREATE TABLE IF NOT EXISTS silver_traffic (
    id BIGSERIAL PRIMARY KEY,
    event_id VARCHAR(100),
    event_time TIMESTAMP,
    road_name VARCHAR(180),
    district VARCHAR(120),
    average_speed_kmh FLOAT,
    congestion_level VARCHAR(50),
    vehicle_count INTEGER,
    ingestion_timestamp TIMESTAMP,
    source VARCHAR(100),
    processed_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_silver_traffic_district ON silver_traffic (district);
CREATE INDEX IF NOT EXISTS idx_silver_traffic_event_time ON silver_traffic (event_time DESC);

CREATE TABLE IF NOT EXISTS silver_news (
    id BIGSERIAL PRIMARY KEY,
    event_id VARCHAR(100) UNIQUE,
    event_time TIMESTAMP,
    source VARCHAR(200),
    city VARCHAR(100),
    district VARCHAR(120),
    title TEXT,
    sentiment_score FLOAT,
    keyword VARCHAR(200),
    ingestion_timestamp TIMESTAMP,
    processed_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_silver_news_district ON silver_news (district);
CREATE INDEX IF NOT EXISTS idx_silver_news_event_time ON silver_news (event_time DESC);

CREATE TABLE IF NOT EXISTS silver_accidents (
    id BIGSERIAL PRIMARY KEY,
    event_id VARCHAR(100),
    event_time TIMESTAMP,
    accident_id VARCHAR(100) UNIQUE,
    road_name VARCHAR(180),
    district VARCHAR(120),
    latitude DOUBLE PRECISION,
    longitude DOUBLE PRECISION,
    severity VARCHAR(50),
    vehicle_count INTEGER,
    casualties INTEGER,
    ingestion_timestamp TIMESTAMP,
    source VARCHAR(100),
    processed_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_silver_accidents_district ON silver_accidents (district);
CREATE INDEX IF NOT EXISTS idx_silver_accidents_event_time ON silver_accidents (event_time DESC);

-- ================================================
-- GOLD LAYER TABLES
-- Analytical datasets for dashboards and ML
-- ================================================

CREATE TABLE IF NOT EXISTS road_health_index (
    id BIGSERIAL PRIMARY KEY,
    district VARCHAR(120) NOT NULL,
    report_count INTEGER NOT NULL,
    average_severity FLOAT NOT NULL,
    pothole_count INTEGER NOT NULL,
    crack_count INTEGER NOT NULL,
    road_health_index FLOAT NOT NULL,
    batch_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_road_health_district ON road_health_index (district);
CREATE INDEX IF NOT EXISTS idx_road_health_batch_time ON road_health_index (batch_time DESC);

CREATE TABLE IF NOT EXISTS priority_score (
    id BIGSERIAL PRIMARY KEY,
    report_id VARCHAR(32),
    road_name VARCHAR(180),
    district VARCHAR(120),
    damage_type VARCHAR(100),
    severity_score INTEGER,
    confidence FLOAT,
    priority_score FLOAT NOT NULL,
    batch_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_priority_district ON priority_score (district);
CREATE INDEX IF NOT EXISTS idx_priority_score ON priority_score (priority_score DESC);
CREATE INDEX IF NOT EXISTS idx_priority_batch_time ON priority_score (batch_time DESC);

CREATE TABLE IF NOT EXISTS damage_prediction (
    id BIGSERIAL PRIMARY KEY,
    report_id VARCHAR(32),
    road_name VARCHAR(180),
    district VARCHAR(120),
    damage_type VARCHAR(100),
    severity_score INTEGER,
    confidence FLOAT,
    status VARCHAR(40),
    event_time TIMESTAMP,
    batch_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_damage_district ON damage_prediction (district);
CREATE INDEX IF NOT EXISTS idx_damage_event_time ON damage_prediction (event_time DESC);
CREATE INDEX IF NOT EXISTS idx_damage_severity ON damage_prediction (severity_score DESC);

CREATE TABLE IF NOT EXISTS accident_prediction (
    id BIGSERIAL PRIMARY KEY,
    accident_id VARCHAR(100),
    district VARCHAR(120),
    road_name VARCHAR(180),
    event_time TIMESTAMP,
    accident_severity VARCHAR(50),
    vehicles_involved INTEGER,
    casualties INTEGER,
    damage_report_count INTEGER,
    avg_severity FLOAT,
    max_severity FLOAT,
    avg_speed FLOAT,
    avg_vehicle_count FLOAT,
    traffic_event_count INTEGER,
    rainfall_mm FLOAT,
    temperature_c FLOAT,
    humidity FLOAT,
    weather_condition VARCHAR(100),
    processed_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    batch_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_accident_district ON accident_prediction (district);
CREATE INDEX IF NOT EXISTS idx_accident_event_time ON accident_prediction (event_time DESC);
CREATE INDEX IF NOT EXISTS idx_accident_severity ON accident_prediction (accident_severity);

-- ================================================
-- METADATA TABLES
-- ================================================

CREATE TABLE IF NOT EXISTS data_quality_metrics (
    id BIGSERIAL PRIMARY KEY,
    layer VARCHAR(50),  -- bronze, silver, gold
    dataset_name VARCHAR(100),
    record_count BIGINT,
    null_count BIGINT,
    duplicate_count BIGINT,
    quality_score FLOAT,
    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS etl_logs (
    id BIGSERIAL PRIMARY KEY,
    job_name VARCHAR(100),
    layer VARCHAR(50),
    status VARCHAR(20),  -- success, failed, running
    start_time TIMESTAMP,
    end_time TIMESTAMP,
    record_processed BIGINT,
    error_message TEXT,
    checkpoint_path VARCHAR(500),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_etl_logs_job_name ON etl_logs (job_name);
CREATE INDEX IF NOT EXISTS idx_etl_logs_status ON etl_logs (status);
CREATE INDEX IF NOT EXISTS idx_etl_logs_created_at ON etl_logs (created_at DESC);
