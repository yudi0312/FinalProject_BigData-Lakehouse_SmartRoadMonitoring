ALTER TABLE reports
    ADD COLUMN IF NOT EXISTS damage_type VARCHAR(100),
    ADD COLUMN IF NOT EXISTS confidence FLOAT,
    ADD COLUMN IF NOT EXISTS severity_score INTEGER;

CREATE INDEX IF NOT EXISTS idx_reports_damage_type ON reports (damage_type);
