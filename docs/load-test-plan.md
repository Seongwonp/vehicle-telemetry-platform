# 부하 테스트 계획 (Load Test Plan)

> 상태: 계획 수립 완료 · 실측 대기 (숫자는 실행 후 채운다)
> 목적: "차량 N대까지 견디는가", "초당 몇 건을 처리하는가", "병목은 어디이고 어떻게 개선했는가"를
> 추측이 아니라 **측정값**으로 말할 수 있게 만든다.

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
# 실행
k6 run -e BASE_URL=http://localhost:8080 -e TOKEN=<jwt> load-test/rest_api.js
```

### 4.3 주의 · 병목 가설

1. **Rate Limit이 먼저 걸린다** — `rate-limit.requests-per-minute` 기본 60. 순수 성능 측정 시엔
   테스트 프로파일에서 크게 올리거나(예: 100000) 우회하고, **별도로 "Rate Limit이 정확히 60에서
   막는가"를 검증하는 시나리오**를 따로 둔다 (이건 성능이 아니라 정확성 테스트).
2. **기간 범위 조회(`/telemetry`)가 무거운 후보** — InfluxDB range 쿼리라, 조회 구간이 넓으면 느려진다.
   구간 크기별 p95를 비교.
3. **Redis 의존** — Rate Limit·Refresh Token이 Redis를 타므로, 고부하에서 Redis가 병목이 되는지 관찰.

---

## 5. 결과 기록 템플릿 (실행 후 채운다)

### Track A — 수집 처리량

| 단계 | 목표 msg/s | 실제 저장 msg/s | Storage Lag | Anomaly Lag | E2E p95 지연 | Spring CPU | 판정 |
| --- | --- | --- | --- | --- | --- | --- | --- |
| A1 | 200 | | | | | | |
| A2 | 1,500 | | | | | | |
| A3 | 5,000 | | | | | | |
| A4 | 10,000 | | | | | | |

**한계점**: 약 ____ msg/s 부터 Lag 누적 / 유실 시작.

### Track A — 병목 개선 before/after

| 개선 항목 | 변경 전 | 변경 후 |
| --- | --- | --- |
| Kafka concurrency 1 → 3 | ___ msg/s | ___ msg/s |
| InfluxDB batchSize/flush 튜닝 | ___ | ___ |
| acks=all → acks=1 (참고) | ___ | ___ |

### Track B — REST API

| 엔드포인트 | 목표 부하(VU) | TPS | p50 | p95 | p99 | 에러율 |
| --- | --- | --- | --- | --- | --- | --- |
| /auth/login | | | | | | |
| /telemetry/latest | | | | | | |
| /telemetry (range) | | | | | | |
| /anomalies | | | | | | |

---

## 6. 실행 순서 체크리스트

- [ ] `docker compose up -d` 로 전체 스택 기동, Grafana/Prometheus 접속 확인
- [ ] A0 기준선(3대) 정상 동작 확인
- [ ] Track A: A1→A4 단계별 5분 유지하며 지표 기록, 한계점 탐색
- [ ] 병목 가설 검증 → Kafka concurrency 등 개선 → 재측정 (before/after)
- [ ] k6 설치 후 Track B 시나리오 실행, 엔드포인트별 p95 기록
- [ ] Rate Limit 정확성 시나리오 별도 실행
- [ ] 결과 표 채우고, Grafana 캡처 첨부
- [ ] 요약을 README "성능" 섹션과 ADR(InfluxDB 배치 결정)에 반영

---

## 7. 포트폴리오 반영 문구 (실측 후)

> 실측 전에는 쓰지 않는다. 아래는 숫자를 채운 뒤 이력서/README에 넣을 문장의 형태다.

- "시뮬레이터를 N대까지 스케일해 수집 파이프라인을 부하 테스트, 약 ____ msg/s까지 무손실 처리 확인"
- "파티션 3개 대비 Kafka 컨슈머 concurrency가 1로 설정돼 처리량이 제한되는 병목을 발견,
  concurrency를 파티션 수에 맞춰 조정해 처리량을 ___ → ___ msg/s로 개선"
- "REST API p95 ___ms를 목표로 부하 테스트, 기간 범위 조회의 InfluxDB 병목을 확인하고 ____ 개선"
