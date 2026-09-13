# 이벤트 상관관계 — 현행 식별자 조사와 설계안

> 상태: **조사·설계만. 구현하지 않았다** (2026-09-13, roadmap 4번).
> 근거는 전부 코드 읽기다 — **이 문서의 어떤 경로도 실측하지 않았다.** 파일:줄은 조사 시점 기준.

## 0. 풀려는 문제

"차량 KR-GA-1234가 12:00:03.120에 보낸 한 건"을 MQTT 수신 → Kafka → InfluxDB 저장 / 이상 감지 → 알림 저장
→ WebSocket까지 **같은 키로 찾을 수 있는가.** 지금 HTTP에는 `traceId`(MDC)가 있지만 **요청 단위**라
파이프라인 메시지와 무관하다(`RequestLoggingFilter.java:41`).

## 1. 현행 식별자와 전달 경로

| 단계 | 코드 | 식별에 쓰이는 것 | Kafka key / header | 비고 |
| --- | --- | --- | --- | --- |
| MQTT 수신 | `MqttMessageHandler.java:56-88` | topic `vehicle/telemetry/{vehicle_id}`, payload의 `vehicle_id`·`timestamp` | — | **메시지 ID 없음.** MQTT packet ID는 세션 안에서만 유일하고 앱에 노출되지 않는다 |
| MQTT 거부 | `MqttMessageHandler.java:99`, `MqttInvalidMessagePublisher.java:24-35` | 로그: `payloadLength`·`payloadSha256` | `vehicle-telemetry-mqtt-dlq`, key=**MQTT topic**, value=`{mqtt_topic, reason, payload}`, header 없음 | 깨진 JSON·ID 누락도 여기로 온다 |
| Kafka 발행 | `TelemetryProducer.java:104-125,168` | — | `vehicle-telemetry`, key=`vehicle_id`, **header 없음** | **value는 원본이 아니라 `VehicleTelemetry` 재직렬화본**이다 |
| spool(Kafka 장애) | `TelemetrySpool.java:28-35` | 파일명 `밀리초-시퀀스-uuid.json` | 드레인 시 새 offset | **재직렬화 payload만 저장.** uuid는 파일명에만 있고 Kafka로 가지 않는다 |
| 저장 | `TelemetryRepository.java:83-123` | InfluxDB identity = `vehicle_telemetry` + tag `vehicle_id` + time(**ms**) | — | 같은 `(vehicle_id, timestamp)`는 **덮어쓴다** — 재전달엔 멱등, 같은 ms의 다른 메시지는 충돌 |
| 저장 DLQ | `TelemetryConsumer.java:240-262` | `x-dlq-origin-topic/partition/offset` | key·value 원본 유지 | **원본 header는 `x-dlq-replay-count`만 복사** |
| 이상 감지 | `anomaly_detector.py:206-248` | **`event_id = sha256(vehicle_id\|timestamp\|anomaly_type\|field\|detector)`** | `vehicle-anomaly-alerts`, key=`vehicle_id` | `detected_at`은 키에 없다 — 재처리해도 같다 |
| 감지 DLQ | `anomaly_detector.py:346-405` | `x-dlq-origin-*`, `x-dlq-source-path` | key·value 원본 유지 | **원본 header를 하나도 복사하지 않는다** (§6-1) |
| 알림 저장 | `AnomalyService.java:108-116,183-200` | `event_id` — payload 값이 64hex면 사용, 아니면 **같은 공식으로 재계산** | — | `ON CONFLICT (event_id) DO NOTHING`, `UNIQUE` 인덱스(V2) |
| 방송 | `TelemetryConsumer.java:196-221` | `/topic/vehicle/{id}/telemetry`, `/anomalies` | — | 알림 방송은 **insert 성공 시에만**(ADR-020) |
| DLQ 재주입 | `dlq.py:319-329` | — | **key·value·header 그대로** + `x-dlq-replay-count` 증가 | 새 offset이 붙는다 |

**요약:** 원본 메시지에는 **전용 식별자가 없다.** 대신 `(vehicle_id, timestamp)`가 **사실상의 대리 키**로
세 곳에서 이미 쓰인다 — InfluxDB identity, `event_id` 해시의 앞 두 칸, 시뮬레이터 정답 로그(`vehicle_simulator.py:238`).

## 2. 원본 식별자와 기존 `event_id`는 역할이 다르다

| | 기존 anomaly `event_id` | 필요한 원본 식별자 |
| --- | --- | --- |
| 무엇을 가리키나 | **알림 하나**(결과) | **텔레메트리 메시지 하나**(입력) |
| 원본과의 관계 | 원본 1건 → 알림 **N건**(유형·필드·탐지기별) | 1:1 |
| 목적 | **저장 멱등성**(`UNIQUE`) | **추적**(로그·Kafka·DB·WebSocket 조인) |
| 만드는 곳 | 감지기(Python), 백엔드가 재계산 가능 | 미정(§4) |

**같은 이름을 쓰면 안 된다.** 원본 ID를 `event_id`로 부르면 DB 컬럼·`UNIQUE` 인덱스·ADR-020의 뜻과 섞인다.
설계안은 원본 쪽을 **`message_id`**로 부르고, 알림에는 **`source_message_id`**로 참조를 싣는다(§8 결정 1).

## 3. 대안

| | A. producer(차량)가 생성 | B. 백엔드 MQTT 입구가 생성 | C. 내용 기반 결정적 키 |
| --- | --- | --- | --- |
| 형태 | payload 필드 `message_id`(UUIDv7 등) | Kafka header `x-message-id`(UUID) | `sha256(vehicle_id\|timestamp)` 또는 두 값 자체 |
| 추적 시작점 | **차량부터** | 입구부터 | 입구부터(차량 로그와도 조인 가능 — 시뮬레이터가 이미 두 값을 남긴다) |
| QoS1 중복 수신 | **같은 ID**(중복 판별 가능) | **새 ID**(같은 메시지인지 모름) | **같은 키** |
| 계약 영향 | **있다** — 지금은 `UNKNOWN_FIELD`로 거부된다 | 없음 | 없음 |
| 구현 비용 | 계약 2언어 + DTO + 시뮬레이터 + 전 경로 | 입구 + header 전파 + DLQ·spool 수정 | **거의 없음**(로그 필드 표준화) |
| 약점 | 구버전 producer는 ID 없음, 차량 신뢰 문제 | 재전달마다 달라 "같은 메시지인가"에 답 못 함 | 같은 ms의 **다른** 메시지를 한 키로 접는다(InfluxDB와 같은 한계), 깨진 JSON에서 계산 불가 |

## 4. 재전달·spool·DLQ 재주입 때 ID가 유지되나

| 사건 | A (payload) | B (header) | C (내용) |
| --- | --- | --- | --- |
| Kafka consumer 재전달 | 유지 | 유지 | 유지 |
| MQTT QoS1 중복 수신 | 유지 | **새로 생성** | 유지 |
| spool 드레인 | 유지 — **단, 재직렬화에 필드가 있어야** | **소실** — spool이 header를 저장하지 않는다 | 유지 |
| 저장 DLQ → `dlq.py` 재주입 | 유지(value 원본) | **소실** — DLQ가 replay-count만 복사 | 유지 |
| 감지 DLQ → 재주입 | 유지 | **소실** — header를 전혀 복사하지 않는다 | 유지 |
| 알림으로 전파 | 감지기가 payload에서 복사해야 | 감지기가 header를 읽어 복사해야 | 알림에 `vehicle_id`·`timestamp`가 이미 있다 |

**B는 현 구조에서 세 곳이 끊긴다.** B를 고르면 spool 파일 형식과 두 DLQ의 header 복사를 함께 바꿔야 한다.

**정책(모든 안 공통):** 재전달·재주입은 **ID를 새로 만들지 않는다.** ID가 바뀌면 추적 키가 아니라
"처리 시도" 키가 된다. 재처리 횟수는 ID가 아니라 `x-dlq-replay-count`가 담는다.

## 5. 입력이 온전하지 않을 때

| 입력 | 현재 동작 | 상관관계에서 가능한 것 |
| --- | --- | --- |
| 깨진 JSON | MQTT DLQ(`MALFORMED_JSON`), 로그에 `payloadSha256` | **어떤 안이든 원본 ID를 읽을 수 없다.** 추적 키는 `(mqtt topic, payloadSha256, 수신 시각)`까지 |
| `vehicle_id`·`timestamp` 누락/형식 오류 | 계약 거부 → DLQ | C 계산 불가. A도 JSON이 온전하면 읽을 수는 있으나 **계약 위반 메시지의 ID를 신뢰할 근거가 없다** |
| 구버전 producer(A 도입 후 ID 없음) | — | **거부하지 않는다.** C 키로 대체 추적. `message_id` 누락률을 지표로 본다 |
| topic과 payload의 `vehicle_id` 불일치 | `TOPIC_VEHICLE_MISMATCH` 거부 | 로그에 둘 다 남겨야 조인 가능 |

`payloadSha256`은 **거부 메시지 추적 보조**로만 쓴다 — 고유 ID가 아니다. P0-2b에서 정한 것과 같다:
밀리초 충돌로 정상 메시지를 접을 수 있고, MQTT 거부 입력의 고유 건수는 셀 수 없다.

## 6. 영향

### 6-1. 부수 발견 — 감지 DLQ가 `x-dlq-replay-count`를 복사하지 않았다 (**2026-09-13 재현·수정 완료**)

> 아래는 발견 당시 기록이다. 실제 Kafka 재현과 수정은 `load-test/dlq-replay-count/RESULT_20260913_replay_count.md`.
> 수정은 replay-count **하나만** 이어받는다 — 이 문서 §4의 "DLQ가 원본 header를 복사하지 않는다"는 여전히 참이라,
> header 기반 `message_id`(안 B)는 이 수정으로도 DLQ 왕복을 살아남지 못한다.

`TelemetryConsumer.sendToDlq`는 replay-count를 이어받는다 — 주석에 "안 이으면 재처리마다 2→4→8→16건으로
증식하는 걸 확인하고 고쳤다"고 적혀 있다. **`anomaly_detector.dlq_headers`는 새 header 목록만 만든다.**
감지 DLQ를 원본 토픽으로 재주입했는데 다시 실패하면 카운트가 0으로 돌아가 `--max-replays`가 동작하지 않는다.
`dlq.py`가 기본으로 영구·불명 실패를 건너뛰어 발생 조건은 좁지만, **일시적 실패가 반복되는 경우는 막지 못한다.**
**이번 범위에서 고치지 않았다** — 별도 항목으로 등록할 것.

### 6-2. 설계가 건드리는 것

| 영역 | 영향 |
| --- | --- |
| 저장 identity | **바꾸지 않는다.** InfluxDB point identity(`vehicle_id`+ms)를 `message_id` tag로 바꾸면 같은 ms 덮어쓰기는 사라지지만 **시계열 카디널리티가 메시지 수만큼 폭증**한다. `message_id`를 넣는다면 **field**로 |
| 중복 방지 | 알림 `UNIQUE(event_id)`는 **유지.** `event_id` 공식에 `message_id`를 넣으면 A 도입 전후로 같은 원본의 `event_id`가 달라져 **전환 구간 중복 행**이 생긴다 → 공식은 바꾸지 않는다 |
| 로그 | 모든 파이프라인 로그에 같은 필드명(`vehicle`, `ts`, 도입 후 `msg`)을 남긴다. 지금은 단계마다 `vehicle=`·`offset=`·`partition=` 조합이 다르다 |
| 개인정보 | `vehicle_id`는 차량 식별자라 **이미 로그에 있다.** `message_id`는 추가 개인정보가 아니지만, **로그 보존 기간이 곧 추적 가능 기간**이 된다(`docs/data-retention.md`) |
| metric cardinality | **`message_id`·`vehicle_id`를 라벨에 넣지 않는다**(`ContractMetricsTest.라벨_경계`가 이미 막는다). 추적은 로그·Kafka·DB에서 하고 지표는 집계만 한다 |
| WebSocket | 알림 응답에 `source_message_id`를 추가하면 앱 DTO 변경 — 앱 저장소 영향, **optional로** |

## 7. 권장 설계 — 단계로 나눈다

**1단계 (계약 불변, 권장 즉시안): C를 표준화한다.**
- 파이프라인 로그 전 구간에 `vehicle`·`ts`(원본 `timestamp`) 필드를 같은 이름으로 남긴다.
- 거부 경로는 `payloadSha256`까지.
- "한 건 찾기" 절차를 Runbook에 쿼리로 적는다(InfluxDB `vehicle_id`+time, PostgreSQL `vehicle_id`+`vehicle_timestamp`, Kafka key).
- 비용이 거의 없고 재전달·spool·DLQ에서 **자동으로 유지**된다. 계약·저장·DTO 변경이 없다.

**2단계 (필요가 확인되면): A를 optional로 추가한다.** 순서가 계약 때문에 강제된다 —
1. **수신 측 먼저**: Java `VehicleTelemetry`·Python `contract.KNOWN_FIELDS`·공유 fixture에 **optional** `message_id` 추가
   (형식 검증, 누락 허용). 이 단계를 건너뛰면 새 producer의 메시지가 **`UNKNOWN_FIELD`로 전부 DLQ에 간다.**
2. 재직렬화(`TelemetryProducer`)가 필드를 보존하는지 테스트로 고정 — spool 경로 포함.
3. 감지기가 알림 payload에 `source_message_id`로 복사, 백엔드 알림 저장에 nullable 컬럼 추가. **`event_id` 공식은 불변.**
4. **그 다음에** 시뮬레이터(producer)가 전송 시작.
5. 누락률 지표(라벨 없는 카운터) — 구버전 producer 비율 확인.

**B는 권장하지 않는다.** 재전달마다 ID가 바뀌어 "같은 메시지인가"에 답하지 못하고, 현 구조에서 spool·두 DLQ
세 곳에서 끊긴다. 수신 인스턴스 추적이 필요해지면 **보조 `x-ingest-id`**로만 검토한다.

## 8. 결정이 필요한 것

1. **원본 식별자 이름** — `message_id`/`source_message_id`(권장) vs `event_id` 재사용.
2. **2단계를 할 것인가** — 1단계로 "한 건 찾기"가 충분한지 먼저 실제 추적 예시로 확인할지.
3. **A의 형식** — UUIDv7(시간 정렬) vs ULID vs 임의 UUID. 차량이 만든 ID를 **신뢰 경계 밖 입력**으로 볼지(형식만 검증).
4. **같은 `(vehicle_id, timestamp)`의 다른 payload**를 계속 덮어쓸지 — 지금은 조용히 한 건이 사라진다.
5. **감지 DLQ replay-count 누락(§6-1)**을 별도 작업으로 먼저 고칠지.

## 9. 구현 완료 조건 (도입 시)

- **1단계**: 한 건을 MQTT 수신 로그 → Kafka(key·offset) → InfluxDB → 감지 로그 → PostgreSQL 알림 → WebSocket
  메시지까지 **같은 `(vehicle_id, timestamp)`로 찾는 절차**를 Runbook에 적고, 실제 스택에서 **1건을 끝까지 추적한 기록**을 evidence로 남긴다.
- 재전달·spool 드레인·DLQ 재주입 각각 후에도 **같은 키로 찾아지는지** 시나리오별 1회.
- **2단계**: 공유 fixture에 `message_id` 있음·없음·형식 오류 칸 추가, Java·Python **같은 판정**.
  구버전 payload(필드 없음)가 **거부되지 않는** 회귀 테스트. 재직렬화·spool 보존 테스트.
  `event_id` 불변 테스트(같은 원본 → A 도입 전후 같은 `event_id`).
- 지표 라벨에 `message_id`·`vehicle_id`가 없음(기존 라벨 경계 테스트 통과).
- 각 결과는 1회 관측으로 표현하고 안정성 주장은 하지 않는다.
