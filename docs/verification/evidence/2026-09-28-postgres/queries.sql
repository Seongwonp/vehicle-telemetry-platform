\pset pager off
SELECT version();
SELECT count(*) AS total, count(*) FILTER(WHERE severity='HIGH') AS high FROM anomaly_alerts;
SELECT min(n), max(n), count(*) FROM (SELECT vehicle_id, count(*) FILTER(WHERE severity='HIGH') n FROM anomaly_alerts GROUP BY vehicle_id) s;
SET max_parallel_workers_per_gather=0;
EXPLAIN (ANALYZE, BUFFERS) SELECT count(*) FROM anomaly_alerts WHERE vehicle_id='V-0123' AND severity='HIGH' AND detected_at BETWEEN now()-interval '30 day' AND now();
EXPLAIN (ANALYZE, BUFFERS) SELECT vehicle_id, count(*) FROM anomaly_alerts WHERE severity='HIGH' AND vehicle_id IN ('V-0001','V-0002','V-0003','V-0004','V-0005','V-0006','V-0007','V-0008','V-0009','V-0010','V-0011','V-0012','V-0013','V-0014','V-0015','V-0016','V-0017','V-0018','V-0019','V-0020') GROUP BY vehicle_id;
EXPLAIN (ANALYZE, BUFFERS) SELECT vehicle_id, count(id) FROM anomaly_alerts WHERE severity='HIGH' AND vehicle_id IN ('V-0001','V-0002','V-0003','V-0004','V-0005','V-0006','V-0007','V-0008','V-0009','V-0010','V-0011','V-0012','V-0013','V-0014','V-0015','V-0016','V-0017','V-0018','V-0019','V-0020') GROUP BY vehicle_id;