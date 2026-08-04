ALTER TABLE anomaly_alerts ADD COLUMN IF NOT EXISTS event_id VARCHAR(64);

UPDATE anomaly_alerts
SET event_id = md5(id::text || ':' || vehicle_id || ':' || detected_at::text)
WHERE event_id IS NULL;

ALTER TABLE anomaly_alerts ALTER COLUMN event_id SET NOT NULL;
CREATE UNIQUE INDEX IF NOT EXISTS uk_anomaly_event_id ON anomaly_alerts (event_id);
