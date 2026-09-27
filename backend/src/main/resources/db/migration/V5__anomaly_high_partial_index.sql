-- 차량 목록의 "HIGH 이상 건수"는 차량마다 count(*)를 돌린다. 2026-09-27에 200만 행에서 재보니
-- idx_anomaly_vehicle_id로 4,000블록 heap을 읽어 24.9ms — 차량 500대 목록이면 12초다.
-- severity='HIGH'만 담는 부분 인덱스로 Index Only Scan 0.06ms(3.5MB, 복합 인덱스는 14MB).
-- docs/verification/2026-09-27-postgres-explain.md
CREATE INDEX IF NOT EXISTS idx_anomaly_vehicle_high
    ON anomaly_alerts (vehicle_id) WHERE severity = 'HIGH';
