# 검증 — PostgreSQL 조회 경로 실행계획, 200만 행 (2026-09-27)

| 항목 | 값 |
| --- | --- |
| **검증 상태** | **1회 관찰** — 같은 조건 반복 없음. 인덱스 선택의 근거로만 쓴다 |
| 대상 | `anomaly_alerts` 조회 5종, `vehicles` 소유자 조회 1종 (실제 서비스 코드의 쿼리 모양) |
| 데이터 | 사용자 50 · 차량 500(사용자당 10, 5%는 비활성) · 이상 알림 **2,000,000**(차량당 4,000, 90일 균등, HIGH 25%) — `generate_series`로 생성, 테이블 650MB |
| 환경 | 이 PC Docker Desktop의 `telemetry-postgres`(postgres:16-alpine) 안에 별도 DB `telemetry_bench`. V1~V4 적용 후 `ANALYZE` |
| 코드 상태 | V4(users·owner FK) 적용 직후, V5 적용 전. 이 문서의 결론이 V5가 됐다 |
| 도구 | `EXPLAIN (ANALYZE, BUFFERS)` |

## 왜 쟀나

사용자·소유권을 RDB로 옮기면서(ADR-027) "RDB 설계"라고 말하려면 인덱스가 실제 쿼리를 어떻게 타는지
봐야 했다. 서비스 코드에서 PostgreSQL을 치는 쿼리는 여섯 모양뿐이라 전부 쟀다.

## 결과

| # | 쿼리 (서비스 위치) | 계획 | 실행 시간 | 판정 |
| --- | --- | --- | --- | --- |
| Q1 | 차량 최근 20건 (`AnomalyService.getRecent`) | Index Scan `idx_anomaly_vehicle_detected_at`, 23 블록 | **0.58 ms** | 좋음 |
| Q2 | 차량 + severity + 30일 + OFFSET 40 (`search`) | 같은 인덱스로 기간까지 좁힘, severity는 Filter(1,361행 제거) | 6.7 ms | 허용. severity를 인덱스에 넣으면 줄지만 20건 페이지에 6ms는 병목이 아니다 |
| Q2c | 위 페이지의 count (`Page.getTotalElements`) | Bitmap Heap Scan 1,361 블록 | 2.9 ms | 허용 |
| **Q3** | **차량 HIGH 건수** (`VehicleService.findAllVisibleTo` — 차량마다 1회) | Bitmap Heap Scan **4,000 블록**, Filter로 HIGH 골라냄 | **24.9 ms** | **문제.** 목록의 차량 수만큼 반복(N+1) — 차량 500대면 약 12초 |
| Q4 | 내 활성 차량 (`findAllByOwner_UsernameAndActiveTrue`) | Nested Loop, `idx_vehicles_owner_active`(V4 부분 인덱스) | 0.11 ms | 좋음. users는 50행이라 Seq Scan이 맞다 |
| Q5 | 깊은 페이지 OFFSET 3000 | Index Scan으로 3,020행 훑고 버림 | 6.1 ms | 허용. 차량당 4,000건 규모에서 keyset 페이지네이션은 아직 필요 없다 |

원본 계획 전문은 아래 "부록".

## Q3를 고쳤다 — 두 가지를 같이

**인덱스**: HIGH만 담는 부분 인덱스 vs `(vehicle_id, severity)` 복합 인덱스를 같은 데이터에서 비교했다.

| 인덱스 | Q3 계획 | Q3 시간 | 크기 |
| --- | --- | --- | --- |
| 없음(기존 `idx_anomaly_vehicle_id`) | Bitmap Heap Scan 4,000 블록 | 24.9 ms | 16 MB |
| `(vehicle_id, severity)` 복합 | Index Only Scan | 0.18 ms | 14 MB |
| **`(vehicle_id) WHERE severity='HIGH'` 부분** | **Index Only Scan** | **0.06 ms** | **3.5 MB** |

부분 인덱스를 골랐다(V5). 이 쿼리는 severity가 항상 `'HIGH'` 상수라 조건을 인덱스 정의에 넣을 수 있고,
크기가 1/4이다. 대가: `severity`별 다른 값의 count에는 안 쓰인다 — 그런 쿼리는 지금 없다.

**호출 수**: 인덱스로 25ms → 0.06ms가 돼도 차량마다 왕복하는 구조는 그대로다. `GROUP BY vehicle_id`
한 번으로 바꿨다(`countHighByVehicleIds`). 차량 20대: **1.7 ms, 왕복 1회**.

## 한계

- 1회 관찰이고 캐시가 따뜻한 상태(`shared hit`)가 섞여 있다. 절대 시간이 아니라 **계획의 모양과 블록 수**를 근거로 썼다.
- Index Only Scan의 `Heap Fetches: 0`은 방금 `ANALYZE`/VACUUM된 테이블 기준이다. 갱신이 잦으면 visibility map이 낡아 heap을 다시 읽는다. 이 테이블은 INSERT만 있어 영향이 작다.
- Docker Desktop(WSL2) 디스크라 `read` 블록의 비용은 운영과 다르다.
- `users`가 50행이라 username 조회가 Seq Scan이다. UNIQUE 인덱스가 있으므로 커지면 알아서 바뀐다 — 확인 안 함.
- 파티셔닝은 하지 않았다. 2백만 행·650MB에서 모든 경로가 인덱스를 타므로 근거가 없다. 90일 보존 삭제(`docs/data-retention.md`)가 필요해지는 시점에 `detected_at` 범위 파티션을 다시 검토한다.

## 재현

```bash
# telemetry-postgres 컨테이너, .env의 POSTGRES_USER
docker exec -i telemetry-postgres psql -U <user> -d telemetry -c "CREATE DATABASE telemetry_bench"
for f in backend/src/main/resources/db/migration/V*.sql; do
  sed 's/\${admin_username}/admin/g' "$f" | docker exec -i telemetry-postgres psql -U <user> -d telemetry_bench
done
```

```sql
INSERT INTO users(username, password_hash, role) SELECT 'user'||g, 'x', 'USER' FROM generate_series(1,50) g;
INSERT INTO vehicles(vehicle_id, name, owner_id, active)
SELECT 'V-'||lpad(g::text,4,'0'), 'car '||g, (SELECT id FROM users WHERE username='user'||((g-1)%50+1)), g%20<>0
FROM generate_series(1,500) g;
INSERT INTO anomaly_alerts(event_id, vehicle_id, anomaly_type, field, value, threshold, severity, detector, vehicle_timestamp, detected_at)
SELECT md5(g::text), 'V-'||lpad(((g-1)%500+1)::text,4,'0'), CASE WHEN g%3=0 THEN 'RPM 과부하' ELSE '엔진 과열' END,
       'engine_temp', 100+random()*20, '>105', CASE WHEN g%4=0 THEN 'HIGH' ELSE 'MEDIUM' END, 'RULE',
       now() - (random()*90) * interval '1 day', now() - (random()*90) * interval '1 day'
FROM generate_series(1,2000000) g;
ANALYZE users; ANALYZE vehicles; ANALYZE anomaly_alerts;
```

## 부록 — 계획 원문 (V5 적용 전)

```
Q1  Limit (actual time=0.085..0.183 rows=20)  Buffers: shared hit=3 read=20
      -> Index Scan using idx_anomaly_vehicle_detected_at  Index Cond: (vehicle_id = 'V-0123')
    Execution Time: 0.575 ms

Q2  Limit (actual time=6.661..6.667 rows=0)  Buffers: shared hit=39 read=1330
      -> Index Scan using idx_anomaly_vehicle_detected_at
         Index Cond: (vehicle_id = 'V-0123' AND detected_at >= now()-30d AND detected_at <= now())
         Filter: (severity = 'HIGH')   Rows Removed by Filter: 1361
    Execution Time: 6.732 ms

Q2c Aggregate (actual time=2.844..2.849)  Buffers: shared hit=1369
      -> Bitmap Heap Scan  Heap Blocks: exact=1361  Filter: severity='HIGH'  Rows Removed by Filter: 1361
         -> Bitmap Index Scan on idx_anomaly_vehicle_detected_at (rows=1361)
    Execution Time: 2.927 ms

Q3  Aggregate (actual time=24.797..24.806)  Buffers: shared hit=1402 read=2604
      -> Bitmap Heap Scan  Recheck Cond: vehicle_id='V-0123'  Filter: severity='HIGH'
         Rows Removed by Filter: 4000  Heap Blocks: exact=4000
         -> Bitmap Index Scan on idx_anomaly_vehicle_id (rows=4000)
    Execution Time: 24.904 ms

Q3' (부분 인덱스 후)
    Aggregate (actual time=0.032..0.033)
      -> Index Only Scan using idx_anomaly_vehicle_high  Index Cond: (vehicle_id = 'V-0123')  Heap Fetches: 0
    Execution Time: 0.060 ms

Q3'' (GROUP BY, 차량 20대)
    GroupAggregate (actual time=0.357..1.690 rows=5)
      -> Index Only Scan using idx_anomaly_vehicle_high  Index Cond: (vehicle_id = ANY ('{V-0001,...,V-0020}'))  rows=20000  Heap Fetches: 0
    Execution Time: 1.730 ms

Q4  Nested Loop (actual time=0.040..0.052 rows=10)
      -> Seq Scan on users  Filter: username='user7'  Rows Removed by Filter: 50
      -> Bitmap Heap Scan on vehicles  Recheck Cond: (owner_id = u.id AND active)
         -> Bitmap Index Scan on idx_vehicles_owner_active
    Execution Time: 0.110 ms

Q5  Limit (actual time=6.044..6.078 rows=20)  Buffers: shared hit=3032
      -> Index Scan using idx_anomaly_vehicle_detected_at  rows=3020
    Execution Time: 6.112 ms

인덱스 크기(200만 행): pkey 43MB · idx_anomaly_detected_at 56MB · idx_anomaly_vehicle_detected_at 77MB
· idx_anomaly_vehicle_id 16MB · uk_anomaly_event_id 146MB · idx_anomaly_vehicle_high 3.5MB · (비교용) idx_anomaly_vehicle_severity 14MB
```
