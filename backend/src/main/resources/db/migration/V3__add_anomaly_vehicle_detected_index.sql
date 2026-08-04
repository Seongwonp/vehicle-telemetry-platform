CREATE INDEX IF NOT EXISTS idx_anomaly_vehicle_detected_at
    ON anomaly_alerts (vehicle_id, detected_at DESC);
