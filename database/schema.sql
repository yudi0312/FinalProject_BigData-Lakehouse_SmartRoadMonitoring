CREATE TABLE IF NOT EXISTS reports (
    report_id VARCHAR(12) PRIMARY KEY,
    report_date TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    reporter_name VARCHAR(120) NOT NULL,
    email VARCHAR(160),
    road_name VARCHAR(180) NOT NULL,
    district VARCHAR(120) NOT NULL,
    village VARCHAR(120) NOT NULL,
    description TEXT NOT NULL,
    latitude DOUBLE PRECISION NOT NULL,
    longitude DOUBLE PRECISION NOT NULL,
    image_path TEXT,
    damage_type VARCHAR(100),
    severity_score INTEGER,
    confidence FLOAT,
    status VARCHAR(40) NOT NULL DEFAULT 'Pending'
);

CREATE INDEX IF NOT EXISTS idx_reports_date ON reports (report_date DESC);
CREATE INDEX IF NOT EXISTS idx_reports_district ON reports (district);
CREATE INDEX IF NOT EXISTS idx_reports_status ON reports (status);
CREATE INDEX IF NOT EXISTS idx_reports_damage_type ON reports (damage_type);
