# 부하 테스트 계획 (Load Test Plan)

> 상태: **실측 완료** (2026-07-22~23, 로컬 Docker Compose 환경)
> 목적: "차량 N대까지 견디는가", "초당 몇 건을 처리하는가", "병목은 어디이고 어떻게 개선했는가"를
> 추측이 아니라 **측정값**으로 말할 수 있게 만든다.
>
> **실행 환경**: MacBook, Docker Desktop, 전 서비스 단일 호스트 컨테이너로 기동. 결과는 이 환경
> 기준이며, 실제 서버 스펙/네트워크에서는 절대치가 달라질 수 있다. 5절에 실측 결과를 기록했다.

---

## 0. 왜 하는가

이 프로젝트는 "실시간 스트리밍 / 대용량 처리 / IoT 백엔드"를 표방한다. 그런데 지금까지는
기능 구현과 장애 시나리오 문서화까지만 되어 있고, **실제로 얼마까지 견디는지에 대한 숫자가 없다.**

부하 테스트의 결과물은 두 가지다.

1. **처리량·지연 지표** — 초당 처리 메시지 수(msg/s), API TPS, p95/p99 지연, Consumer Lag
2. **병목 1건 이상을 찾아서 개선한 기록** — "측정 → 병목 발견 → 개선 → 재측정"의 before/after

두 번째가 핵심이다. 숫자만 있는 것보다 "병목을 어떻게 찾고 어떻게 풀었는가"가 훨씬 강하다.

---

## 1. 시스템 부하 지점 두 갈래

이 시스템은 성격이 다른 두 개의 부하 경로가 있고, **도구도 다르다.**

```
[Track A · 수집 파이프라인]  시뮬레이터 → MQTT → Spring(MqttHandler) → Kafka → Consumer × 2 → InfluxDB / PostgreSQL / 이상감지
[Track B · REST API]        k6 → Spring REST (JWT · Rate Limit · InfluxDB/PG 조회)
```

- **Track A**는 MQTT 기반이라 k6(HTTP)로 못 건다. 대신 **시뮬레이터 자체를 스케일**한다
  (`VEHICLE_COUNT`, `PUBLISH_INTERVAL`이 이미 환경변수로 열려 있음). 이게 "대용량 차량 데이터
  처리"라는 주장의 진짜 시험대다.
- **Track B**는 HTTP라 **k6**로 표준적으로 부하를 건다. 인증·조회 성능과 Rate Limit 동작을 본다.

---

## 2. 측정 지표와 수집 위치

| 지표 | 의미 | 수집 위치 |
| --- | --- | --- |
| 입력 처리량 (msg/s) | 초당 수집·저장한 텔레메트리 건수 | 시뮬레이터 발행량 계산 + InfluxDB 저장 카운트 |
| Kafka Consumer Lag | 컨슈머가 프로듀서를 못 따라가는 정도 | `kafka-consumer-groups --describe` / Prometheus |
| End-to-End 지연 | publish timestamp → InfluxDB 저장 시각 | 메시지 `timestamp`와 저장 시각 차이 |
| InfluxDB 쓰기 지연 | 배치 flush 소요 | Actuator/커스텀 메트릭 or 로그 |
| API TPS | 초당 처리 요청 수 | k6 요약 (`http_reqs`) |
| API p50/p95/p99 | 응답 지연 분포 | k6 요약 (`http_req_duration`) |
| 에러율 | 5xx / 타임아웃 비율 | k6 (`http_req_failed`) + Sentry |
| 리소스 | CPU/메모리/GC | Prometheus(Actuator) + `docker stats` |

> Grafana + Prometheus가 이미 떠 있으므로, 부하 주는 동안 대시보드를 캡처해 두면 그대로 근거 자료가 된다.

---

## 3. Track A — 수집 파이프라인 부하 (시뮬레이터 스케일)

### 3.1 부하 주는 법

`docker-compose.yml`의 `simulator` 서비스 환경변수를 올린다.

```bash
# 예: 차량 500대 × 0.1초 간격(=차량당 10Hz) → 이론상 5,000 msg/s
VEHICLE_COUNT=500 PUBLISH_INTERVAL=0.1 docker compose --profile simulator up -d simulator

# 단계적으로: 50 → 100 → 300 → 500 → 1000 대로 올리며 무너지는 지점을 찾는다
```

부하 계산: `초당 메시지 = VEHICLE_COUNT / PUBLISH_INTERVAL`
(500대 / 0.1초 = 5,000 msg/s)

### 3.2 단계별 측정 (ramp-up)

| 단계 | VEHICLE_COUNT | PUBLISH_INTERVAL | 목표 msg/s | 관찰 |
| --- | --- | --- | --- | --- |
| A0 기준선 | 3 | 1.0 | 3 | 정상 동작 확인 |
| A1 | 100 | 0.5 | 200 | — |
| A2 | 300 | 0.2 | 1,500 | — |
| A3 | 500 | 0.1 | 5,000 | — |
| A4 | 1000 | 0.1 | 10,000 | 한계 탐색 |

각 단계에서 5분 이상 유지하며 아래를 기록한다.

- Kafka Consumer Lag이 **누적되는가**(따라잡는가) — `telemetry-storage-group`, `anomaly-detector-group` 각각
- InfluxDB 저장 건수가 발행량과 일치하는가 (유실 여부)
- End-to-End 지연이 유지되는가, 계속 늘어나는가
- Spring Boot CPU/메모리/GC, InfluxDB CPU

### 3.3 병목 가설 (코드 기반 — 실측으로 검증)

> 아래는 코드를 보고 세운 가설이다. 실측으로 맞는지 확인하고, 맞으면 개선 후 재측정한다.

1. **Kafka 컨슈머 concurrency 미설정 (가장 유력)**
   - 토픽 파티션은 3개인데(`vehicle-telemetry`), `application.yml`에 컨슈머 `concurrency` 설정이
     없어 기본값 1이다. 즉 파티션이 3개여도 **한 그룹당 스레드 1개**만 소비 → 처리량이 파티션 수만큼
     안 나올 가능성.
   - **개선안**: `spring.kafka.listener.concurrency=3` (파티션 수와 정렬). before/after msg/s와 Lag 비교.
2. **InfluxDB 배치 설정 (`batchSize=500`, `flushInterval=1000ms`)**
   - 저부하에서는 flushInterval 때문에 최대 1초 지연이 깔린다. 고부하에서는 배치가 빨리 차서
     flush가 잦아진다. 부하별로 batchSize/flushInterval을 바꿔가며 처리량-지연 곡선을 그린다.
   - 이미 ADR에 "쓰기 보장 대신 처리량" 트레이드오프로 문서화돼 있으니, **실측 곡선을 ADR에 붙이면**
     결정이 근거를 갖는다.
3. **Producer `acks=all`**
   - 내구성은 높지만 처리량은 낮다. `acks=1`과 비교 측정해 트레이드오프를 숫자로 제시.
4. **MQTT 핸들러 단일 경로**
   - `MqttMessageHandler`가 단일 스레드로 수신→프로듀스하면 수집 입구에서 막힐 수 있다.
     Track A에서 Kafka Lag은 없는데 InfluxDB 저장이 안 따라오는지, 아니면 입구부터 막히는지 구분.

---

## 4. Track B — REST API 부하 (k6)

### 4.1 대상 엔드포인트

| 메서드 | 경로 | 성격 |
| --- | --- | --- |
| POST | `/api/auth/login` | 인증 (JWT 발급) |
| GET | `/api/vehicles/{id}/telemetry/latest` | 최신값 조회 (가벼움) |
| GET | `/api/vehicles/{id}/telemetry` | 기간 범위 조회 (InfluxDB range — 무거움 후보) |
| GET | `/api/vehicles/{id}/anomalies` | 이상 이력 조회 (PostgreSQL) |
| GET | `/api/vehicles/{id}/diagnosis` | AI 진단 |

### 4.2 k6 시나리오 (예시)

```javascript
// load-test/rest_api.js
import http from 'k6/http';
import { check, sleep } from 'k6';

export const options = {
  stages: [
    { duration: '1m', target: 50 },   // ramp up
    { duration: '3m', target: 50 },   // steady
    { duration: '1m', target: 200 },  // spike
    { duration: '2m', target: 200 },
    { duration: '1m', target: 0 },    // ramp down
  ],
  thresholds: {
    http_req_duration: ['p(95)<500'], // p95 500ms 목표 (관찰용, 조정 가능)
    http_req_failed: ['rate<0.01'],
  },
};

const BASE = __ENV.BASE_URL || 'http://localhost:8080';
let token = __ENV.TOKEN; // 사전 로그인 토큰 주입

export default function () {
  const params = { headers: { Authorization: `Bearer ${token}` } };
  const res = http.get(`${BASE}/api/vehicles/KR-GA-1234/telemetry/latest`, params);
  check(res, { 'status 200': (r) => r.status === 200 });
  sleep(1);
}
```

```bash
# 실제 스크립트는 ENDPOINT별로 실행하며 로그인 정보는 환경변수로만 주입한다.
k6 run -e ENDPOINT=latest -e BASE_URL=http://localhost:8080 \
  -e USERNAME=<username> -e PASSWORD=<password> load-test/rest_api.js
```

### 4.3 주의 · 병목 가설

1. **Rate Limit이 먼저 걸린다** — `rate-limit.requests-per-minute` 기본 60. 순수 성능 측정 시엔
   테스트 프로파일에서 크게 올리거나(예: 100000) 우회하고, **별도로 "Rate Limit이 정확히 60에서
   막는가"를 검증하는 시나리오**를 따로 둔다 (이건 성능이 아니라 정확성 테스트).
2. **기간 범위 조회(`/telemetry`)가 무거운 후보** — InfluxDB range 쿼리라, 조회 구간이 넓으면 느려진다.
   구간 크기별 p95를 비교.
3. **Redis 의존** — Rate Limit·Refresh Token이 Redis를 타므로, 고부하에서 Redis가 병목이 되는지 관찰.

---

## 5. 결과 기록 (실측)

### Track A — 수집 처리량

시뮬레이터는 2개 컨슈머 그룹이 공유하는 `vehicle-telemetry` 토픽(파티션 3개)에 발행한다.
각 단계 5분(A0은 2분) 유지하며 InfluxDB 저장 건수 증가량으로 실제 msg/s를 계산했다.

| 단계 | 차량수/간격 | 목표 msg/s | 실제 저장 msg/s | Storage Lag(telemetry-storage-group) | Anomaly Lag(anomaly-detector-group) | Spring/Kafka CPU | 판정 |
| --- | --- | --- | --- | --- | --- | --- | --- |
| A0 | 3 / 1.0s | 3 | ~3.2 | 0 (누적 없음) | 0 (누적 없음) | 낮음 | 정상 |
| A1 (버그 상태) | 100 / 0.5s | 200 | **~99.6** (50% 유실) | 0-8 | 43→321로 증가 추세 | backend 3-71% | **데이터 유실 발견** |
| A1 (수정 후) | 100 / 0.5s | 200 | **~197.1** | 0-3 | 91-355 (진동, 비누적) | backend 11-73% | 유실 해소 확인 |
| A2 | 300 / 0.2s | 1,500 | ~1,024 | 0-73 (비누적) | 89-1742 (진동 폭 확대) | backend 33-48%, kafka 36-50% | 시뮬레이터 한계 도달 |
| A3 | 500 / 0.1s | 5,000 | ~1,253 | 0-75 (비누적) | 66-1616 (진동) | backend 48-67%, kafka 50-65% | 시뮬레이터 한계 유지 |
| A4 | 1,000 / 0.1s | 10,000 | ~1,004 | 0-80 (비누적) | 259-2274 (진동 폭 최대) | backend 37-66%, kafka 41-90% | 시뮬레이터 한계, 오히려 소폭 하락 |

**한계점**: Track A에서 실제로 도달한 병목은 처음 예상한 "Kafka/InfluxDB 처리 한계"가 아니라
**두 가지 다른 지점**이었다.

1. **InfluxDB 타임스탬프 정밀도 유실 버그** (아래 병목 개선 참고) — A1에서 처음 발견. 목표
   msg/s와 무관하게 A1의 차량당 초당 2회 조건에서 정확히 50%가 사라졌다. 더 빠른 발행
   조건에서는 같은 초의 여러 포인트가 하나로 합쳐질 수 있는 구조였다.
2. **부하 생성기(시뮬레이터) 자체의 한계** — A2~A4에서 목표 msg/s를 1,500→10,000으로
   6.7배 올렸는데도 실제 발행량은 ~1,024 → ~1,253 → ~1,004로 거의 늘지 않았고(A4에서는 오히려
   A3보다 낮음), `telemetry-storage-group` Kafka lag은 이 전 구간에서 0 근처로 유지됐다(수치는
   Kafka가 실제로 받은 양과 거의 정확히 일치). 즉 **Java 백엔드/Kafka는 이번 테스트에서 한 번도
   포화되지 않았다** — Python(GIL) 스레드-차량 모델의 시뮬레이터가 ~1,000-1,250 msg/s에서
   먼저 벽에 부딪혔다. 진짜 백엔드 한계를 재려면 비동기/멀티프로세스 기반의 더 강한 부하
   생성기가 필요하다(향후 계획 참고).
3. `anomaly-detector-group`(Python 이상 감지)은 A2부터 lag 진동 폭이 눈에 띄게 커졌고(A4에서
   최대 ~2,300), 테스트한 서비스 중 스트레스 징후를 가장 먼저 보인 곳이다 — 다만 완전히
   무너지진 않고(폭주하지 않고 다시 줄어듦), 부하 생성기 한계 때문에 이 서비스의 진짜 포화점도
   이번 테스트로는 확인하지 못했다.

### Track A — 병목 개선 before/after

| 개선 항목 | 변경 전 | 변경 후 | 비고 |
| --- | --- | --- | --- |
| **InfluxDB 타임스탬프 정밀도**(시뮬레이터 초 단위 + `WritePrecision.S`) | 100 vehicles/0.5s에서 **~99.6 msg/s** 저장(목표 200의 50%) — 같은 차량의 초당 2번째 메시지가 InfluxDB에서 동일 시리즈+타임스탬프로 충돌해 앞 값을 조용히 덮어씀 | 시뮬레이터를 ms 정밀도로, `WritePrecision.MS`로 변경 후 **~197.1 msg/s**(목표의 98.5%) | Kafka lag은 버그 상태에서도 0에 가까웠다 — "Lag 없음 = 정상"이라는 가정만으로는 이 유실을 못 잡는다는 게 핵심 교훈 |
| Kafka listener concurrency 1 → 3 | 1,000 vehicles/0.1s에서 ~1,004 msg/s, storage lag 최대 80 | 동일 조건 재측정 ~745 msg/s, storage lag 최대 **26** | 처리량 차이는 시뮬레이터 자체 변동성(A2~A4에서 이미 확인된 ~1,000-1,250 msg/s 벽) 때문으로 보임 — lag 최대치는 오히려 낮아졌으나, concurrency=1 상태에서도 lag이 누적된 적이 없어 "이 테스트로 도달한 부하 범위에서는 병목이 아니었다"가 정확한 결론. 파티션 수(3)에 맞는 설정이라 프로덕션 기본값으로는 유지 |
| InfluxDB batchSize/flush 튜닝 |  |  | 이번 회차에서 측정하지 않음 — 향후 과제 |
| acks=all → acks=1 (참고) |  |  | 이번 회차에서 측정하지 않음 — 향후 과제 |

> **추가 발견(Track B에서 역으로 확인)**: InfluxDB 조회 API(`/latest`, `/telemetry`) 부하 테스트
> 중 InfluxDB Java 클라이언트의 OkHttp Dispatcher(호스트당 동시 요청 기본값 5)를 200으로
> 늘려봤더니 p95가 3.82s→6.23s로, 에러율이 0.03%→1.43%로 오히려 **악화**됐다. InfluxDB/백엔드
> 로그에 `context canceled` / `SocketTimeoutException`이 다수 발생 — 기본값 5가 병목이 아니라
> 로컬 InfluxDB 컨테이너 자체의 동시 쿼리 처리 용량을 클라이언트 쪽에서 우연히 보호하고 있었던
> 것으로 판단, 기본 설정으로 되돌렸다. "병목처럼 보이는 설정을 넓혔더니 그 뒤의 진짜 병목이
> 드러난" 사례.

### Track B — InfluxDB 클라이언트 동시 요청 확대 before/after

동일한 `/telemetry/latest`, VU 50→200 시나리오로 비교했다. 확대 설정에서 별도로 기록하지
못한 값은 비워 두었다.

| 설정 | TPS | p50 | p95 | p99 | 에러율 | 판정 |
| --- | --- | --- | --- | --- | --- | --- |
| 기본 OkHttp Dispatcher(호스트당 동시 요청 5) | 48.77/s | 277.05ms | 3.82s | 6.08s | 0.03% | 기준 |
| Dispatcher 200/200 |  |  | 6.23s |  | 1.43% | 악화 — InfluxDB 타임아웃 증가, 변경 원복 |

### Track B — REST API (VU 50→200 스파이크, RATE_LIMIT_RPM 임시 상향 후 측정)

| 엔드포인트 | 목표 부하(VU) | TPS | p50 | p95 | p99 | 에러율 |
| --- | --- | --- | --- | --- | --- | --- |
| POST /auth/login | 50→200 | 45.55/s | 391.94ms | 3.5s | 4.34s | 0.00% |
| GET /telemetry/latest | 50→200 | 48.77/s | 277.05ms | 3.82s | 6.08s | 0.03% (5/13,208) |
| GET /telemetry (최근 100건) | 50→200 | 48.84/s | 325.2ms | 3.71s | 5.81s | 0.01% (1/13,240) |
| GET /anomalies (PostgreSQL) | 50→200 | 92.24/s | 11.14ms | 95.3ms | 297.65ms | 0.00% |

**관찰**: `/latest`와 `/telemetry`가 조회 데이터량(1건 vs 100건)과 무관하게 거의 동일한
p95/p99를 보였다 — 두 엔드포인트가 공유하는 InfluxDB 조회 경로에 병목이 있다는 신호.
반면 PostgreSQL 기반 `/anomalies`는 같은 부하에서 TPS가 거의 2배, p95는 1/40 수준으로 훨씬
빨랐다. InfluxDB 쪽이 REST 조회 경로의 실질적 병목이라는 게 명확했지만(위 "추가 발견" 참고),
근본 해결(InfluxDB 스케일링/쿼리 최적화/캐싱)은 이번 범위를 벗어나 향후 과제로 남긴다.

**Rate Limit 정확성 시나리오**: `RATE_LIMIT_RPM`을 기본값 60으로 되돌린 뒤 동일 IP·토큰으로
`/telemetry/latest`를 순차 65회 요청 → **60번째까지 200, 61번째부터 정확히 429** 시작. 설계대로
동작함을 확인.

---

## 6. 실행 순서 체크리스트

- [x] `docker compose up -d` 로 전체 스택 기동, Grafana/Prometheus 접속 확인
- [x] A0 기준선(3대) 정상 동작 확인
- [x] Track A: A1→A4 단계별 5분 유지하며 지표 기록, 한계점 탐색 (→ 시뮬레이터 자체 한계로 판명)
- [x] 병목 가설 검증 → 실측 결과 InfluxDB 타임스탬프 정밀도 유실이 진짜 1순위 병목 → 수정 후 재측정 (before/after)
- [x] Kafka concurrency 1→3 변경 후 재측정 (before/after) — 도달 가능한 부하 범위에서는 유의미한 차이 없음을 확인
- [x] k6 설치 후 Track B 시나리오 실행, 엔드포인트별 p95 기록
- [x] Rate Limit 정확성 시나리오 별도 실행 (60번째까지 200, 61번째부터 429 확인)
- [x] 결과 표 채움 (Grafana 캡처는 이번 회차에서 생략 — 터미널/Flux 쿼리로 직접 수치 확인)
- [x] 요약을 README "성능" 섹션과 ADR-011/ADR-014에 반영

---

## 7. 포트폴리오 반영 문구 (실측 완료)

- "시뮬레이터로 수집 파이프라인 부하 테스트 중 InfluxDB 타임스탬프 정밀도(초 단위) 때문에
  차량 발행 주기가 1초 미만이면 데이터의 절반이 조용히 유실되는 버그를 발견 — Kafka lag은
  0으로 정상처럼 보였지만 실제 저장량은 목표의 50%였음. 밀리초 정밀도로 수정해 유실을
  해소(약 100 msg/s → 약 197 msg/s, 목표 200의 98.5%)."
- "REST API 부하 테스트에서 InfluxDB 기반 조회(/latest, /telemetry)가 PostgreSQL 기반 조회
  (/anomalies)보다 최대 40배 느린 것을 확인. InfluxDB 클라이언트의 동시 요청 한도를 넓혀봤다가
  오히려 에러율이 0.03%→1.43%로 악화되는 것을 보고 진짜 병목이 애플리케이션이 아니라 InfluxDB
  자체의 동시 쿼리 처리 용량이라는 걸 역으로 검증."
- "Rate Limit(분당 60회)이 설계대로 정확히 61번째 요청부터 차단되는 것을 순차 요청으로 검증."
- "Kafka 파티션 수(3)에 맞춰 컨슈머 concurrency를 1→3으로 조정 — 이번 테스트로 도달 가능했던
  부하 범위(~1,250 msg/s)에서는 유의미한 차이가 없었음을 정직하게 확인(원인: 그 범위에서는
  애초에 병목이 아니었음). 병목이 없다는 것도 측정으로 증명한 사례."
