CREATE TABLE IF NOT EXISTS vehicles (
    id BIGSERIAL PRIMARY KEY,
    vehicle_id VARCHAR(50) NOT NULL UNIQUE,
    name VARCHAR(100),
    owner VARCHAR(100),
    active BOOLEAN NOT NULL DEFAULT TRUE,
    registered_at TIMESTAMP
);

CREATE TABLE IF NOT EXISTS anomaly_alerts (
    id BIGSERIAL PRIMARY KEY,
    vehicle_id VARCHAR(50) NOT NULL,
    anomaly_type VARCHAR(255) NOT NULL,
    field VARCHAR(50),
    value DOUBLE PRECISION,
    threshold VARCHAR(200),
    severity VARCHAR(10),
    detector VARCHAR(10),
    vehicle_timestamp TIMESTAMPTZ,
    detected_at TIMESTAMPTZ NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_anomaly_vehicle_id ON anomaly_alerts (vehicle_id);
CREATE INDEX IF NOT EXISTS idx_anomaly_detected_at ON anomaly_alerts (detected_at);
