# 검증 — PostgreSQL 조회 경로 실행계획, 200만 행 (2026-09-27, 같은 날 재측정)

| 항목 | 값 |
| --- | --- |
| **검증 상태** | **1회 관찰** — 같은 조건 반복 없음. 인덱스 선택의 근거로만 쓴다 |
| 대상 | `anomaly_alerts` 조회 5종, `vehicles` 소유자 조회 1종 (실제 서비스 코드의 쿼리 모양) |
| 데이터 | 사용자 50 · 차량 500(사용자당 10, 5%는 비활성) · 이상 알림 **2,000,000**(차량당 4,000, 90일 균등) · **HIGH는 `random() < 0.25`로 차량과 독립**(차량당 935~1,067건, V-0123은 1,063건) — `generate_series`, 테이블 650MB |
| 환경 | 이 PC Docker Desktop의 `telemetry-postgres`(postgres:16-alpine) 안에 별도 DB `telemetry_bench`. V1~V4 적용 → 측정 → V5 적용 → 측정. `VACUUM ANALYZE` 후, 병렬 워커 0 |
| 도구 | `EXPLAIN (ANALYZE, BUFFERS)` |

## 첫 측정은 무효였다 — 그대로 남긴다

같은 날 오전 측정은 severity를 `g % 4 = 0`, 차량을 `(g-1) % 500 + 1`로 만들었다. **500이 4의 배수라 같은 차량은 severity가
항상 같았다** — 125대는 4,000건 전부 HIGH, 375대는 0건, 측정 차량 V-0123은 **0건**. 그때 얻은 "24.9ms → 0.06ms"는
빈 결과를 찾는 쿼리의 개선이었고 "차량 500대면 12초"라는 확대도 근거가 없었다. 제3자 리뷰(Codex)가 생성식에서 잡았다.
아래는 severity를 차량과 독립인 `random()`으로 다시 만든 결과다. **인덱스와 GROUP BY를 고른 결론은 같지만 배수는 훨씬 작다.**

## 왜 쟀나

사용자·소유권을 RDB로 옮기면서(ADR-027) "RDB 설계"라고 말하려면 인덱스가 실제 쿼리를 어떻게 타는지 봐야 했다.
서비스 코드에서 PostgreSQL을 치는 쿼리는 여섯 모양뿐이라 전부 쟀다.

## 결과 (V4까지 적용, V5 전)

| # | 쿼리 (서비스 위치) | 계획 | 실행 시간 | 판정 |
| --- | --- | --- | --- | --- |
| Q1 | 차량 최근 20건 (`AnomalyService.getRecent`) | Index Scan `idx_anomaly_vehicle_detected_at`, 23 블록 | 0.58 ms | 좋음 |
| Q2 | 차량 + severity + 30일 + OFFSET 40 (`search`) | 같은 인덱스로 기간까지 좁힘, severity는 Filter(164행 제거) | 1.3 ms | 좋음 |
| Q2c | 위 페이지의 count | Bitmap Heap Scan | 2.9 ms | 허용 |
| **Q3** | **차량 HIGH 건수** (`VehicleService.findAllVisibleTo` — 차량마다 1회) | Bitmap Heap Scan **4,000 블록**, Filter로 HIGH 1,063건 남김 | **7.1 ms** (첫 실행·캐시 없음 11~31 ms) | 개선 대상. 목록 차량 수만큼 반복(N+1) |
| Q4 | 내 활성 차량 (`findAllByOwner_UsernameAndActiveTrue`) | Nested Loop, `idx_vehicles_owner_active`(V4 부분 인덱스) | 0.11 ms | 좋음. users는 50행이라 Seq Scan이 맞다 |
| Q5 | 깊은 페이지 OFFSET 3000 | Index Scan으로 3,020행 훑고 버림 | 6.1 ms | 허용. 차량당 4,000건 규모에서 keyset 페이지네이션은 아직 필요 없다 |

Q1·Q2c·Q4·Q5는 오전 측정값이다(severity 분포와 무관한 쿼리). Q2·Q3은 재측정값.

## Q3 — 인덱스와 호출 수를 따로 재고, 둘 다 바꿨다

목록 20대 기준, 같은 캐시 상태(VACUUM 후, `shared hit`):

| | 차량마다 count (N+1, 20회) | `GROUP BY` 1회 |
| --- | --- | --- |
| 인덱스 없음 | **147 ms** | 21 ms |
| `idx_anomaly_vehicle_high` (V5) | 2.6 ms | **2.9 ms** |

- **부분 인덱스**(`(vehicle_id) WHERE severity='HIGH'`, 3.5MB): Q3 단건 7.1 → **0.96 ms**, Index Only Scan(`Heap Fetches: 0`). 20대 N+1 147 → 2.6 ms.
  복합 `(vehicle_id, severity)`(14MB)도 Index Only Scan이 되지만 이 쿼리는 severity가 항상 상수라 부분 인덱스가 1/4 크기로 같은 일을 한다.
- **GROUP BY 1회**(`countHighByVehicleIds`): 인덱스가 있으면 DB 시간은 N+1과 비슷하다(2.6 vs 2.9 ms). **이득은 DB 시간이 아니라 왕복 수다** — 차량 N대에 N회 → 1회. 인덱스가 없을 때는 DB 시간도 7배 준다.
- 정직하게 쓰면: **인덱스가 시간을, GROUP BY가 왕복을 줄였다.** "400배"·"12초" 같은 표현은 무효 측정에서 나온 것이라 쓰지 않는다.

## 한계

- 1회 관찰. 캐시가 따뜻한 상태라 절대 시간이 아니라 **계획의 모양·블록 수·상대 배수**를 근거로 썼다.
- **2026-09-28 재실행 원문**: [`evidence/2026-09-28-postgres/`](evidence/2026-09-28-postgres/)(`queries.sql`·`plans.txt`).
  계획 모양은 같았다(20대 GROUP BY = Index Only Scan, `Heap Fetches: 0`, `count(id)`로 heap을 강제하면 Bitmap Heap Scan 4,097블록).
  그러나 시간은 **19.7 ms**로, 위 표의 2.9 ms와 7배 차이가 났다 — 시뮬레이터·백엔드·감지기 컨테이너가 20시간째 도는
  상태에서 쟀고 `read=18`이 섞였다. 절대 시간을 근거로 쓰지 않는 이유가 이것이다.
- **Index Only Scan은 visibility map에 의존한다.** 대량 INSERT 직후 `ANALYZE`만 한 상태에서는 같은 인덱스가 Bitmap Heap Scan으로 떨어져 Q3 5.4 ms, 20대 GROUP BY 47 ms였다. `VACUUM` 뒤에 위 수치가 나온다. 이 테이블은 INSERT만 있어 autovacuum(PG13+ insert 트리거)이 처리한다 — 운영에서 확인한 것은 아니다.
- Docker Desktop(WSL2) 디스크라 `read` 블록 비용은 운영과 다르다.
- 파티셔닝은 하지 않았다. 200만 행·650MB에서 모든 경로가 인덱스를 타므로 근거가 없다. 90일 보존 삭제(`docs/data-retention.md`)가 필요해지는 시점에 `detected_at` 범위 파티션을 다시 검토한다.

## 재현

```bash
# telemetry-postgres 컨테이너, .env의 POSTGRES_USER. V4까지만 먼저 적용해야 "V5 전" 기준선이 나온다.
docker exec -i telemetry-postgres psql -U <user> -d telemetry -c "CREATE DATABASE telemetry_bench"
for f in backend/src/main/resources/db/migration/V[1-4]*.sql; do
  sed 's/\${admin_username}/admin/g' "$f" | docker exec -i telemetry-postgres psql -U <user> -d telemetry_bench
done
```

```sql
SELECT setseed(0.42);
INSERT INTO users(username, password_hash, role) SELECT 'user'||g, 'x', 'USER' FROM generate_series(1,50) g;
INSERT INTO vehicles(vehicle_id, name, owner_id, active)
SELECT 'V-'||lpad(g::text,4,'0'), 'car '||g, (SELECT id FROM users WHERE username='user'||((g-1)%50+1)), g%20<>0
FROM generate_series(1,500) g;
-- severity는 차량과 독립이어야 한다 (g%4는 g%500과 결합된다 — 위 "첫 측정은 무효였다")
INSERT INTO anomaly_alerts(event_id, vehicle_id, anomaly_type, field, value, threshold, severity, detector, vehicle_timestamp, detected_at)
SELECT md5(g::text), 'V-'||lpad(((g-1)%500+1)::text,4,'0'), CASE WHEN g%3=0 THEN 'RPM 과부하' ELSE '엔진 과열' END,
       'engine_temp', 100+random()*20, '>105', CASE WHEN random() < 0.25 THEN 'HIGH' ELSE 'MEDIUM' END, 'RULE',
       now() - (random()*90) * interval '1 day', now() - (random()*90) * interval '1 day'
FROM generate_series(1,2000000) g;
VACUUM (ANALYZE, PARALLEL 0) anomaly_alerts;   -- 컨테이너 /dev/shm 64MB라 병렬 워커는 끈다
SET max_parallel_workers_per_gather = 0;
-- 측정 → 그 뒤 V5 적용 → VACUUM ANALYZE → 재측정
```

## 부록 — 계획 원문

```
Q3 (V5 전, VACUUM 후)
 Aggregate (actual time=6.980..6.987)  Heap Blocks: exact=4000
   -> Bitmap Heap Scan  Recheck: vehicle_id='V-0123'  Filter: severity='HIGH'  Rows Removed by Filter: 2937  rows=1063
      -> Bitmap Index Scan on idx_anomaly_vehicle_id  rows=4000
 Execution Time: 7.076 ms        (같은 쿼리 첫 실행: 11.257 ms / 다른 차량 첫 실행: 30.871 ms)

Q3 (V5 후, VACUUM 후)
 Aggregate (actual time=0.850..0.858)
   -> Index Only Scan using idx_anomaly_vehicle_high  Index Cond: (vehicle_id = 'V-0123')  rows=1063  Heap Fetches: 0
 Execution Time: 0.964 ms
 (VACUUM 전에는 Bitmap Heap Scan, Buffers: shared hit=1063, 5.420 ms)

20대 GROUP BY (V5 후, VACUUM 후)
 GroupAggregate (actual time=0.181..2.831 rows=20)
   -> Index Only Scan using idx_anomaly_vehicle_high  Index Cond: (vehicle_id = ANY(...))  rows=20012  Heap Fetches: 0
 Execution Time: 2.878 ms       (VACUUM 전: HashAggregate + Bitmap Heap Scan, 46.981 ms)

20대 개별 count ×20 (unnest 서브쿼리로 흉내)   V5 후 2.572 ms / V5 전 147.043 ms
20대 GROUP BY, V5 전                               20.946 ms

Q2 (재측정)  Limit (actual time=0.743..1.204 rows=20)  Index Scan idx_anomaly_vehicle_detected_at  Rows Removed by Filter: 164  1.270 ms

인덱스 크기(200만 행): pkey 43MB · idx_anomaly_detected_at 56MB · idx_anomaly_vehicle_detected_at 77MB
· idx_anomaly_vehicle_id 16MB · uk_anomaly_event_id 146MB · idx_anomaly_vehicle_high 3.5MB · (비교용) idx_anomaly_vehicle_severity 14MB
```
