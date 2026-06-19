-- SRIS Data Lakehouse Gold Layer PostgreSQL Sink Tables
-- Fulfills CPMK-3 and CPMK-4 Spatial Analytics requirements

-- 1. Road Health Index Table
CREATE TABLE IF NOT EXISTS road_health_index (
    id BIGSERIAL PRIMARY KEY,
    road_name VARCHAR(180) NOT NULL,
    district VARCHAR(120) NOT NULL,
    severity FLOAT NOT NULL,
    rainfall FLOAT NOT NULL,
    traffic FLOAT NOT NULL,
    road_age_score FLOAT NOT NULL,
    road_health_index FLOAT NOT NULL,
    batch_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- 2. Priority Score Table
CREATE TABLE IF NOT EXISTS priority_score (
    id BIGSERIAL PRIMARY KEY,
    report_id VARCHAR(32) NOT NULL,
    road_name VARCHAR(180) NOT NULL,
    district VARCHAR(120) NOT NULL,
    severity_score INTEGER NOT NULL,
    traffic_score FLOAT NOT NULL,
    accident_score FLOAT NOT NULL,
    complaint_score FLOAT NOT NULL,
    news_score FLOAT NOT NULL,
    priority_score FLOAT NOT NULL,
    batch_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- 3. Damage Prediction Table (Rule-Based Baseline)
CREATE TABLE IF NOT EXISTS damage_prediction (
    id BIGSERIAL PRIMARY KEY,
    report_id VARCHAR(32) NOT NULL,
    road_name VARCHAR(180) NOT NULL,
    district VARCHAR(120) NOT NULL,
    severity_score INTEGER NOT NULL,
    predicted_risk_level VARCHAR(40) NOT NULL,
    batch_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- 4. Accident Prediction Table (Rule-Based Baseline)
CREATE TABLE IF NOT EXISTS accident_prediction (
    id BIGSERIAL PRIMARY KEY,
    road_name VARCHAR(180) NOT NULL,
    district VARCHAR(120) NOT NULL,
    predicted_accident_probability FLOAT NOT NULL,
    risk_level VARCHAR(40) NOT NULL,
    prediction_date TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    batch_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- 5. Hotspot Analysis Table (GIS & Spatial Analytics Dashboard)
CREATE TABLE IF NOT EXISTS hotspot_analysis (
    id BIGSERIAL PRIMARY KEY,
    road_id VARCHAR(180) NOT NULL,
    road_name VARCHAR(180) NOT NULL,
    report_count INTEGER NOT NULL,
    avg_severity FLOAT NOT NULL,
    hotspot_score FLOAT NOT NULL,
    batch_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);
