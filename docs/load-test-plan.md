# 부하 테스트 계획 (Load Test Plan)

> 상태: **실측 완료** (2026-07-22~27, 로컬 Docker Compose 환경 — 7/22~23 1차, 7/26~27 멀티프로세스 부하 생성기로 2차 추가 측정)
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
| Kafka listener concurrency 1 → 3 (단일 프로세스 부하, 1차) | 1,000 vehicles/0.1s에서 ~1,004 msg/s, storage lag 최대 80 | 동일 조건 재측정 ~745 msg/s, storage lag 최대 **26** | 처리량 차이는 시뮬레이터 자체 변동성(A2~A4에서 이미 확인된 ~1,000-1,250 msg/s 벽) 때문으로 보임 — lag 최대치는 오히려 낮아졌으나, concurrency=1 상태에서도 lag이 누적된 적이 없어 "이 테스트로 도달한 부하 범위에서는 병목이 아니었다"가 정확한 결론 |
| Kafka listener concurrency 1 → 3 (멀티프로세스 부하로 재검증, 2차 — 아래 절 참고) | ~2,500 msg/s 실부하에서 concurrency=1: storage lag 295-829 | 동일 실부하에서 concurrency=3: storage lag 156-1182 | 진짜 배압(anomaly-detector-group lag 200만+)이 걸리는 부하에서 재검증해도 두 설정 모두 storage lag이 같은 자릿수(수백)에 머묾 — `telemetry-storage-group`은 메시지당 처리가 가벼워(JSON 파싱 + InfluxDB WriteApi 큐잉) concurrency 1과 3 사이에 유의미한 차이가 없다는 걸 실부하로도 확정. 파티션 수(3)에 맞춘 설정이라 프로덕션 기본값으로 유지 |
| InfluxDB batchSize/flush 튜닝 |  |  | 이번 회차에서 측정하지 않음 — 향후 과제 |
| acks=all → acks=1 (참고) |  |  | 이번 회차에서 측정하지 않음 — 향후 과제 |

> **추가 발견(Track B에서 역으로 확인)**: InfluxDB 조회 API(`/latest`, `/telemetry`) 부하 테스트
> 중 InfluxDB Java 클라이언트의 OkHttp Dispatcher(호스트당 동시 요청 기본값 5)를 200으로
> 늘려봤더니 p95가 3.82s→6.23s로, 에러율이 0.03%→1.43%로 오히려 **악화**됐다. InfluxDB/백엔드
> 로그에 `context canceled` / `SocketTimeoutException`이 다수 발생 — 기본값 5가 병목이 아니라
> 로컬 InfluxDB 컨테이너 자체의 동시 쿼리 처리 용량을 클라이언트 쪽에서 우연히 보호하고 있었던
> 것으로 판단, 기본 설정으로 되돌렸다. "병목처럼 보이는 설정을 넓혔더니 그 뒤의 진짜 병목이
> 드러난" 사례.

### Track A — 진짜 백엔드 한계치 (멀티프로세스 부하 생성기, 2차 측정 7/26~27)

1차 측정(A0~A4)에서는 시뮬레이터 자체가 ~1,000-1,250 msg/s에서 벽에 부딪혀 Java 백엔드/Kafka의
진짜 한계를 못 쟀다. 원인을 뜯어보니 시뮬레이터는 차량마다 스레드 하나 + 그 안에서 paho MQTT
클라이언트가 또 내부 루프 스레드를 하나씩 띄우는 구조라, 차량 1,000대 = OS 스레드 약 2,000개가
파이썬 GIL 하나를 두고 경쟁한다 — CPU 사용률은 40% 안팎으로 낮은데도 처리량이 안 늘던 이유다.

**해결**: `VEHICLE_ID_OFFSET` 환경변수를 추가해 시뮬레이터를 여러 **프로세스**(컨테이너)로
동시에 띄웠다 — 프로세스마다 별도 GIL이라 진짜 병렬성이 생긴다.
`docker compose run -d --rm --name telemetry-sim-N -e VEHICLE_ID_OFFSET=$((N*250)) ...`
형태로 200대/0.05초 프로세스를 6개 동시 실행(이론상 24,000 msg/s, 프로세스당 실측 ~400-600).

| 구성 | 실제 저장 msg/s | Storage Lag | Anomaly Lag | Backend/Kafka CPU | 판정 |
| --- | --- | --- | --- | --- | --- |
| 프로세스 6개 (약 24시간 지속) | ~2,500-2,700 | 156-1,182 (비누적, 수백대 유지) | **1,264 → 2,000,000+** (계속 증가, 회복 안 됨) | backend 82-135%(멀티코어), kafka 35-119% | **진짜 병목 발견** |
| 프로세스 10개 | 6개와 비슷하거나 소폭 낮음 | 334-405 | 계속 증가 | backend 73%, kafka 35-55% | 호스트 8코어 공유 한계(백엔드+Kafka+InfluxDB+시뮬레이터 프로세스가 전부 같은 호스트) — 6→10개는 유의미한 증가 없음 |

**결론 — 이번에 처음으로 진짜 포화점을 봤다**:

- `telemetry-storage-group`(Java → InfluxDB)은 ~24시간 동안 ~2,500 msg/s를 유지하는 내내
  lag이 수백 단위에서만 움직였다(수천, 수만으로 번지지 않음) — **이 테스트로 도달한 부하
  범위에서는 여전히 포화되지 않았다.**
- `anomaly-detector-group`(Python 이상 감지, 단일 인스턴스)은 같은 기간 lag이 **1,264에서
  200만 건 이상으로 폭주**했다 — 완전히 다른 이야기다. 회복 신호 없이 계속 쌓였고, 부하를
  끈 뒤에도(측정 종료 시점 기준 파티션 합계 약 390만) 자연 배수 중이다.
- Kafka Consumer Group을 저장 경로와 이상 감지 경로로 분리해둔 설계(ADR-002)가 정확히
  의도대로 동작했다 — 이상 감지가 완전히 밀려도 InfluxDB 저장은 전혀 영향받지 않았다.
- 즉 이 시스템의 진짜 첫 번째 확장 병목은 Kafka도 InfluxDB도 아니라 **단일 인스턴스로 도는
  Python 이상 감지 서비스**다. 다음 확장 단계는 이 서비스를 파티션 수에 맞춰 다중 인스턴스로
  늘리거나(Consumer Group 내 여러 프로세스), 룰 기반과 ML 기반을 분리해 무거운 ML 추론만
  비동기 큐로 빼는 방향이 유력하다 (향후 과제).
- 호스트가 8코어 하나뿐이라 시뮬레이터 프로세스 6→10개에서는 추가 이득이 없었다 — 더 큰
  절대치를 재려면 여러 호스트로 부하 생성기를 분산해야 한다(로컬 테스트의 한계, 향후 과제).

### Track A — 이상 감지 서비스 다중화 (3차 측정 8/2, 코덱스 리뷰 반영해 재작성)

> **이 절은 외부 리뷰(코덱스)에서 방법론적 결함을 지적받고 표현을 다시 정직하게 고쳤다.**
> 원본 원시 로그는 `load-test/anomaly-detector-scale/`에 커밋해뒀다(이전엔 세션 스크래치패드에만
> 있어 재검증이 불가능했다 — 그 자체가 리뷰에서 지적된 문제였다).

위에서 찾은 병목(`anomaly-detector-group` 단일 인스턴스, lag 1,264 → 200만+)을 실제로
고치기 전에, 원인을 먼저 들여다봤다. 다만 아래 두 단계 모두 **부하량과 호스트 경합을
동시에 바꿨기 때문에, "순수 알고리즘 한계"와 "호스트 경합"을 깔끔하게 분리하지는 못했다** —
이 점은 처음 작성 때 과장했던 부분이라 바로잡는다.

**0단계 — 원인 분리 시도(단일 인스턴스, 부하를 낮추고 호스트도 여유롭게)**: 시뮬레이터
2프로세스만(200대/0.25초씩, 실측 ~1,200-1,250 msg/s) 띄워 12분 관찰
(원본: `load-test/anomaly-detector-scale/S0_isolation_single-instance-1200msgs.txt`).

| 시간대(샘플) | Anomaly Lag 합계(3파티션) | 판정 |
| --- | --- | --- |
| 초반 | 3,844 → 5,669 | 상승 |
| 중반 | 2,197 → 3,431 → 4,952 → 1,594 → 5,096 → 977 | 진동(수천대에서 오르내림, 누적 아님) |
| 종반 | 4,560 → 2,848 → 6,151 → 3,216 → 3,875 → **231** | 마지막엔 거의 다 따라잡음 |

Storage Lag은 전 구간 2-145로 건강. **여기서 실제로 확인한 것은 딱 이것뿐이다**: 부하를
~1,200 msg/s로 낮추고 호스트 경합도 같이 줄인 조건에서는, 단일 인스턴스가 12분 동안
평균적으로 따라잡았다(진동은 있지만 발산 없음). **이것만으로는 "단일 인스턴스 알고리즘
자체의 한계"와 "호스트 경합" 중 무엇이 원래 24시간 테스트 폭주의 주원인이었는지 분리해서
말할 수 없다** — 부하도 낮아졌고 경합도 줄었으니 둘 중 하나만으로도, 혹은 둘의 조합으로도
설명 가능하다. 이 부분을 "원인 분리 완료"라고 썼던 건 과장이었다.

**1단계 — 다중화**: `docker-compose.yml`의 `anomaly-detector` 서비스에서 `container_name`
고정을 제거(스케일과 고정 이름은 공존 불가)하고 `deploy.replicas: 3`을 선언해, `--scale` 옵션
없이 평범한 `docker compose up -d`만으로도 3개가 유지되게 했다(고정 안 하면 `docker compose
down` 후 재기동 시 1개로 돌아가는 문제가 있었다 — 이것도 리뷰에서 지적받아 고쳤다).
`kafka-consumer-groups --describe`로 파티션 3개가 서로 다른 컨테이너 3개(IP 다름)에 정확히
1개씩 자동 배분되는 것을 확인.

**2단계 — 재측정(원래 폭주를 냈던 것과 같은 설정으로)**: 시뮬레이터 6프로세스(원래 테스트와
동일 설정)로 ~2,324 msg/s를 11분 지속
(원본: `load-test/anomaly-detector-scale/S2_multiplex3_3instances-2300msgs.txt`).

| 샘플 시각 | Anomaly Lag 합계(3파티션, 3인스턴스) | Storage Lag |
| --- | --- | --- |
| 04:40:59 | 16,438 | 155-216 |
| 04:43:50 | 20,690 | 384-495 |
| 04:46:28 | 4,437 | 589-706 |
| 04:48:27 | 4,613 | 654-900 |
| 04:49:56 | 15,182 | 347-580 |
| 04:50:59 | 2,879 | 477-747 |
| 04:52:08 | 7,368 | 721 |

**정직한 요약(과장 없이)**:

| 구성 | 부하 | 관찰 시간 | Anomaly Lag 추이 | 판정 |
| --- | --- | --- | --- | --- |
| 단일 인스턴스 | ~2,500-2,700 msg/s | 24시간 | 1,264 → 2,000,000+ | 장기적으로 발산 확인됨 |
| 3인스턴스 | ~2,324 msg/s | 11분 | 16,438 → 20,690 → 4,437 → 4,613 → 15,182 → **2,879 → 7,368** | 11분 구간에서 순발산은 없었고 backlog가 순감소했으나, **마지막 두 샘플은 오히려 상승**(2,879→7,368) — 노이즈 폭이 커서 이 정도 샘플 수로는 "완전히 안정" 여부를 통계적으로 확정하기 어렵다 |

**확실히 말할 수 있는 것**:
- 단일 인스턴스는 기존 공유 호스트의 ~2,500-2,700 msg/s 장기(24시간) 부하에서 발산했다.
- 3인스턴스는 ~2,324 msg/s를 11분 처리하는 동안 backlog가 순감소했다(시작 대비 끝 값이 낮음).
- `container_name` 제거 + Kafka Consumer Group 기반 수평 확장은 구조적으로 올바른 방향이다
  (파티션 수에 맞춰 자동으로 1:1 배분되는 것을 확인).

**아직 확정할 수 없는 것** (과장하지 않기 위해 명시):
- "3인스턴스면 병목이 완전히 해소된다"는 아직 증명되지 않았다 — before/after가 **동일 조건**이
  아니었다(부하 수준 다름: 2,500-2,700 vs 2,324 msg/s / 관찰 시간 다름: 24시간 vs 11분 /
  재빌드로 `kafka-python` 버전도 3.0.7→3.0.9로 바뀜). 동일 이미지·동일 부하·동일 관찰
  시간으로 1인스턴스와 3인스턴스를 나란히 비교해야 결론을 확정할 수 있다.
- 11분 관찰은 장기 안정성을 보장하지 않는다 — 최소 1시간 비교 + 3인스턴스 12~24시간
  soak test가 필요하다.
- "이상 감지 알고리즘 자체"가 병목이라는 표현도 좁혀야 한다 — 실제 처리 경로엔 룰 계산 외에
  JSON 역직렬화, 경고 로그, Kafka `producer.flush()`(이상 감지 시마다 동기 호출), 동기 웹훅
  호출(`notifier.send_webhook`)이 섞여 있고, 어느 단계가 실제 병목인지 프로파일링하지 않았다.

**참고**: 재빌드 과정에서 `anomaly-detector/requirements.txt`가 `kafka-python>=2.0.2`로
버전을 고정하지 않아, 1차 측정 때는 3.0.7, 이번엔 3.0.9가 설치됐다(재현성에 실제로 영향을
줄 수 있는 변수 — 향후 버전 고정 필요). 인스턴스 시작 로그에 `kafka.net.selector`가 이벤트
루프 블로킹 경고를 1회 출력했지만 그룹 조인 시점 1회성이라 지속적인 처리 지연의 원인은
아닌 것으로 판단.

**향후 제대로 검증하려면** (`load-test/anomaly-detector-scale/README.md`에도 기록):
1. 동일 이미지·동일 부하(정확히 같은 msg/s)·동일 시작 lag으로 1인스턴스 vs 3인스턴스를
   각각 최소 1시간 나란히 비교
2. 3인스턴스로 12~24시간 soak test
3. 호스트 경합만 분리하려면 부하 생성기를 다른 호스트로 옮기거나 컨테이너 CPU quota를
   고정한 채 프로세스 수만 바꿔 비교
4. `process()` 함수 내 `producer.flush()`/동기 웹훅 호출이 실제 지연에 기여하는지 프로파일링

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
- [x] Track A: A1→A4 단계별 5분 유지하며 지표 기록, 한계점 탐색 (→ 1차는 시뮬레이터 자체 한계로 판명)
- [x] 병목 가설 검증 → 실측 결과 InfluxDB 타임스탬프 정밀도 유실이 진짜 1순위 병목 → 수정 후 재측정 (before/after)
- [x] Kafka concurrency 1→3 변경 후 재측정 (before/after) — 1차(약한 부하)·2차(멀티프로세스 실부하) 둘 다 유의미한 차이 없음을 확인
- [x] 멀티프로세스 부하 생성기(VEHICLE_ID_OFFSET)로 시뮬레이터 GIL 한계 극복 → 진짜 병목(anomaly-detector-group, lag 200만+) 발견
- [x] k6 설치 후 Track B 시나리오 실행, 엔드포인트별 p95 기록
- [x] Rate Limit 정확성 시나리오 별도 실행 (60번째까지 200, 61번째부터 429 확인)
- [x] 이상 감지 서비스 다중화(container_name 제거 + `deploy.replicas=3`) → 원래 폭주하던 부하로 11분 재측정 — 순발산은 없었으나 동일 조건 A/B가 아니라 "완전 해소"는 미확정(코덱스 리뷰로 표현 정정, `load-test/anomaly-detector-scale/` 원본 로그 추가)
- [x] docker-compose 환경변수 전달 보완 (`ADMIN_USERNAME`/`ADMIN_PASSWORD`/`CORS_ALLOWED_ORIGINS` 추가, `RATE_LIMIT_RPM`은 이전에 완료)
- [x] 결과 표 채움 (Grafana 캡처는 이번 회차에서 생략 — 터미널/Flux 쿼리로 직접 수치 확인)
- [x] 요약을 README "성능" 섹션과 ADR-011/ADR-014/ADR-016에 반영

---

## 7. 포트폴리오 반영 문구 (실측 완료)

- "시뮬레이터로 수집 파이프라인 부하 테스트 중 InfluxDB 타임스탬프 정밀도(초 단위) 때문에
  차량 발행 주기가 1초 미만이면 데이터의 절반이 조용히 유실되는 버그를 발견 — Kafka lag은
  0으로 정상처럼 보였지만 실제 저장량은 목표의 50%였음. 밀리초 정밀도로 수정해 유실을
  해소(약 100 msg/s → 약 197 msg/s, 목표 200의 98.5%)."
- "단일 프로세스 부하 생성기가 파이썬 GIL 때문에 ~1,250 msg/s에서 먼저 한계에 부딪히는 것을
  발견하고, 프로세스를 여러 개 병렬 실행하는 방식으로 부하 생성기 자체를 개선해 시스템의
  진짜 병목을 찾아냄 — 약 24시간 동안 ~2,500 msg/s를 지속했더니 Kafka Consumer Group으로
  분리해둔 두 경로(저장 vs 이상 감지) 중 이상 감지(Python, 단일 인스턴스) 쪽만 lag이 1,264에서
  200만 건 이상으로 폭주, 저장 경로(Java/InfluxDB)는 수백 단위에서 그대로 버팀. Kafka Consumer
  Group 분리 설계가 실제로 장애 전파를 막는 것을 실측으로 검증."
- "REST API 부하 테스트에서 InfluxDB 기반 조회(/latest, /telemetry)가 PostgreSQL 기반 조회
  (/anomalies)보다 최대 40배 느린 것을 확인. InfluxDB 클라이언트의 동시 요청 한도를 넓혀봤다가
  오히려 에러율이 0.03%→1.43%로 악화되는 것을 보고 진짜 병목이 애플리케이션이 아니라 InfluxDB
  자체의 동시 쿼리 처리 용량이라는 걸 역으로 검증."
- "Rate Limit(분당 60회)이 설계대로 정확히 61번째 요청부터 차단되는 것을 순차 요청으로 검증."
- "Kafka 파티션 수(3)에 맞춰 컨슈머 concurrency를 1→3으로 조정 — 약한 부하와 실제 배압이
  걸리는 부하(~2,500 msg/s) 양쪽에서 재검증했지만 둘 다 유의미한 차이가 없었음을 정직하게
  확인(원인: 이 컨슈머는 메시지당 처리가 가벼워 애초에 병목이 아니었음). 병목이 없다는 것도
  두 번의 실측으로 증명한 사례."
- "찾아낸 병목(이상 감지 서비스 단일 인스턴스)에 Kafka Consumer Group의 다중 인스턴스
  리밸런싱을 적용해봄 — 원래 lag이 200만 건 이상으로 폭주했던 것과 같은 수준의 부하로
  11분 재측정했더니 3인스턴스는 순발산 없이 backlog가 줄었다. 다만 동일 조건 A/B가
  아니었다는 걸(부하·관찰시간·라이브러리 버전 차이) 외부 리뷰로 지적받고 결론을
  '완전 해소 확정'이 아니라 '유망하지만 동일 조건 재검증 필요'로 정정 — 성능 주장은
  검증 압박을 견뎌야 의미 있다는 걸 보여준 사례."
