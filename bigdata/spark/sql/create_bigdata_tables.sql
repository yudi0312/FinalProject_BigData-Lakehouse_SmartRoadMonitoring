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
