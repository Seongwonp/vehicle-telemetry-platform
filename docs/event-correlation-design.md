# 이벤트 상관관계 — 현행 식별자 조사와 설계안

> 상태: **조사·설계만. 구현하지 않았다** (2026-09-13, roadmap 4번). 2026-09-14에 §10(전달표·충돌 정책·배포 순서·변경 파일·결정)을 보탰다.
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

## 10. 구체화 (2026-09-14) — 2단계(안 A, optional `message_id`)를 한다면

> **여전히 설계만이다. 구현하지 않았다.** 아래 파일:함수는 2026-09-14 코드 읽기 기준이며 실측하지 않았다.
> 1단계(§7, `(vehicle_id, timestamp)` 표준화)는 이 절과 무관하게 먼저 할 수 있다.

### 10-1. ID 전달표 — 어디에 실려 가고, 어디서 끊길 수 있나

| # | 구간 | 코드 | `message_id`가 사는 곳 | 유지 조건 / 끊기는 지점 | 바꿀 것 |
| --- | --- | --- | --- | --- | --- |
| 1 | producer | `simulator/vehicle_simulator.py` `_to_payload()` (실제 차량도 같은 계약) | payload 필드 | **메시지를 만들 때 한 번** 정한다. QoS1 재전송은 paho가 같은 바이트를 다시 보내므로 같은 값 | 필드 추가, 재전송 동일성 테스트 |
| 2 | MQTT 수신 | `MqttMessageHandler` → `TelemetryDecoder.decode` | payload | **계약이 필드를 알아야 한다** — 지금은 `KNOWN_FIELDS`에 없어 `UNKNOWN_FIELD`로 거부 | `TelemetryDecoder.KNOWN_FIELDS`, `VehicleTelemetry` |
| 3 | MQTT 거부 | `MqttInvalidMessagePublisher` | envelope의 원본 `payload` 문자열 안 | 원본 문자열을 그대로 싣는다 → 유지. 깨진 JSON이면 **읽을 수 없다** | 없음(로그에 읽을 수 있으면 `msg`) |
| 4 | Kafka 발행 | `TelemetryProducer.send` — `objectMapper.writeValueAsString(telemetry)` | 재직렬화 payload | **`VehicleTelemetry`에 필드가 없으면 여기서 조용히 사라진다.** 원본이 아니라 도메인 객체를 다시 쓰기 때문 | `VehicleTelemetry` 필드 + **`@JsonInclude(NON_NULL)`**(10-5) |
| 5 | spool | `TelemetrySpool.store(payload)` | 4의 문자열을 파일에 그대로 | 4를 따른다. 파일명 uuid는 spool 내부 이름일 뿐 ID가 아니다 | 없음(왕복 테스트만) |
| 6 | 저장 | `TelemetryConsumer` → `TelemetryRepository.toPoint` | InfluxDB **field** | **tag로 넣지 않는다** — 시계열 카디널리티 폭증(§6-2). point identity(`vehicle_id`+ms)는 그대로 | `toPoint` field 추가 |
| 7 | 저장 DLQ | `TelemetryConsumer.sendToDlq` | value 원본 | value를 그대로 두므로 유지 | 없음 |
| 8 | 감지 | `contract.validate` → `anomaly_detector.process` | payload dict | **Python 계약도 필드를 알아야 한다**(`contract.KNOWN_FIELDS`). 알림 payload로 복사해야 전달된다 | `contract.py`, `process()`에서 `source_message_id` 복사 |
| 9 | 감지 DLQ | `anomaly_detector.dlq_headers` | value 원본 | value 원본 유지 → 유지(header는 여전히 replay-count만 이어받는다) | 없음 |
| 10 | 알림 저장 | `AnomalyService.saveOne` → `AnomalyAlertRepository.insertIfAbsent` | PostgreSQL `source_message_id`(nullable) | **`event_id` 공식은 바꾸지 않는다.** payload의 모르는 키는 지금도 무시된다(`payload.get(...)`로 필요한 것만 읽음) | `AnomalyAlert`, `insertIfAbsent`, `V4__add_anomaly_source_message_id.sql` |
| 11 | 응답·WebSocket | `AnomalyResponse` | optional 필드 | 앱 `Anomaly.fromJson`은 필요한 키만 읽어 **모르는 키를 무시한다** — 앱 변경 없이 호환 | `AnomalyResponse`(선택) |
| 12 | DLQ 재주입 | `dlq-tools/dlq.py replay` | key·value·header 그대로 | 유지. **재주입이 ID를 새로 만들지 않는다**(§4 정책) | 없음(보존 테스트) |

**끊길 수 있는 곳은 두 군데뿐이다 — #2·#8(계약이 필드를 모름)과 #4(재직렬화 DTO에 필드 없음).** 나머지는 원본 value를 그대로 운반한다.

### 10-2. 원본 메시지 ID와 기존 `event_id` — 섞지 않는 규칙

| | `message_id` (새로, 원본) | `event_id` (기존, 알림) |
| --- | --- | --- |
| 가리키는 것 | 텔레메트리 **입력 한 건** | 이상 **알림 한 건** |
| 개수 관계 | 1 | 입력 1건 → 알림 0~N건 |
| 만드는 곳 | producer | 감지기(백엔드가 같은 공식으로 재계산 가능) |
| 공식 | 생성값(UUID 등) — 내용에서 계산하지 않는다 | `sha256(vehicle_id\|timestamp\|anomaly_type\|field\|detector)` — **불변** |
| 유일성 제약 | **없다**(10-4) | PostgreSQL `UNIQUE` — 저장 멱등성 |
| 알림에서의 이름 | `source_message_id` | `event_id` |

- 알림 행의 `event_id`에 `message_id`를 넣거나 공식에 섞지 않는다 — 도입 전후로 같은 원본의 `event_id`가 달라져 **전환 구간에 중복 알림 행**이 생긴다.
- 로그 필드명: 원본 `msg`, 알림 `event`. 한 로그 줄에 둘이 같이 나오면 `msg`가 원인, `event`가 결과다.

### 10-3. 입력이 온전하지 않을 때

| 입력 | 처리 | 추적 키 | 지표 |
| --- | --- | --- | --- |
| `message_id` 없음(구버전 producer) | **수용** — 필드는 optional | `(vehicle_id, timestamp)` | 라벨 없는 `telemetry.message_id.missing` 카운터 |
| `message_id: null` | 없음과 같게 수용 | 같음 | 같음 |
| 형식 오류(문자열 아님·UUID 형식 아님·길이 초과) | **결정 필요(10-7 ③)** — 추천: 기존 사유 코드(`TYPE_MISMATCH`/`PAYLOAD_VALIDATION_FAILED`)로 **거부** | DLQ의 원본 문자열 | 기존 거부 지표 |
| 깨진 JSON | 지금과 같다 — MQTT DLQ `MALFORMED_JSON` | ID를 **읽을 수 없다.** `(mqtt topic, payloadSha256, 수신 시각)`까지 | 기존 |
| `vehicle_id`·`timestamp` 누락, topic 불일치 | 지금과 같다 — 계약 거부 | 파싱 가능하면 로그에 `msg`를 남기되 **신뢰하지 않는다**(계약 위반 메시지의 필드) | 기존 |

### 10-4. 같은 `message_id`에 다른 payload가 오면

| 경우 | 의미 | 지금 구조에서 일어나는 일 |
| --- | --- | --- |
| 같은 ID + 같은 payload | QoS1 재전송·Kafka 재전달·DLQ 재주입 | 저장은 `(vehicle_id, ms)` 덮어쓰기로, 알림은 `event_id` `UNIQUE`로 이미 멱등 |
| 같은 ID + **다른** payload | producer 버그(ID 재사용)·위조 | 아무것도 막지 않는다 — timestamp가 다르면 둘 다 저장, 같은 ms면 뒤가 덮어쓴다(ID와 무관한 기존 규칙) |

| 선택지 | 비용 | 판단 |
| --- | --- | --- |
| **① 막지 않고 식별 가능하게** — 수신 로그에 `msg`+`payloadSha256`, 사후 조회로 판별 | 로그 필드만 | **추천.** ID는 추적 키지 유일성 키가 아니다 |
| ② 수신 입구에서 최근 N분 `ID→hash` 캐시로 탐지해 DLQ | 인스턴스가 여럿이면 **공유 저장소(Redis) 필요** — 입구가 Redis 장애에 묶인다(Redis 정책 §9와 충돌) | 비추천(신규 의존) |
| ③ DB `UNIQUE(message_id)` | InfluxDB엔 제약이 없고, PostgreSQL 알림은 1:N이라 걸 수 없다 | 불가 |

`(vehicle_id, timestamp)` 같은 다른 payload를 덮어쓸지(§8-4)는 **이 결정과 별개**로 남는다.

### 10-5. unknown field 거부 계약과 호환되는 배포 순서

**핵심 위험:** 받는 쪽이 필드를 모르면 `UNKNOWN_FIELD`로 **메시지 전체**를 거부한다. 백엔드는 Kafka로 재직렬화본을 보내므로,
**백엔드가 필드를 싣기 시작하는 순간 구버전 감지기가 모든 메시지를 DLQ로 보낼 수 있다.**

| 순서 | 배포 | 이 시점에 필드가 흐르면 | 방어 |
| --- | --- | --- | --- |
| 0 | (현재) | Java·Python 모두 거부 | — |
| 1 | 공유 fixture + Java `TelemetryDecoder`/`VehicleTelemetry` + Python `contract.py`를 **한 변경으로**(배포 전) | — | `test_shared_fixtures`·`BothEntrancesSameContractTest`가 같은 판정 강제 |
| 2 | **감지기 먼저** | 감지기는 수용 | 백엔드는 아직 필드를 모름 |
| 3 | 백엔드(수신·Kafka 발행·저장·V4 마이그레이션) | 백엔드가 수용·운반 | **`NON_NULL`** — ID 없는 메시지는 키 자체가 없어, 순서를 실수해도 구 감지기가 전부 거부하지 않는다 |
| 4 | 감지기가 알림에 `source_message_id` 싣기 | 백엔드 알림 소비자는 모르는 키를 무시 → 순서 무관 | — |
| 5 | **producer(시뮬레이터·차량) 마지막** | 전 구간 수용 | — |
| 6 | 누락률 지표 확인 | — | — |

**롤백은 역순이며 두 곳을 먼저 비운다:** producer를 먼저 끄고, 백엔드를 되돌리기 전에 **spool 깊이 0**, 감지기를 되돌리기 전에
**`vehicle-telemetry` lag 0**을 확인한다. 필드가 실린 메시지가 남아 있으면 구버전이 `UNKNOWN_FIELD`로 DLQ에 보낸다
(그때는 DLQ 재주입이 아니라 새 버전 재배포가 복구다).

### 10-6. 변경 파일·공유 fixture·완료 조건

| 영역 | 파일 |
| --- | --- |
| 공유 fixture | `contract-fixtures/cases.json`(+`README.md`) — `message_id` 있음/없음/`null`/숫자 타입/형식 오류/길이 초과 칸, storage·detector 두 칸 모두 |
| 백엔드 계약 | `domain/TelemetryDecoder.java`(`KNOWN_FIELDS`), `domain/VehicleTelemetry.java`(필드 + `@JsonInclude(NON_NULL)` + 형식 검증) |
| 백엔드 운반·저장 | `influxdb/TelemetryRepository.java`(`toPoint` field), `mqtt/MqttMessageHandler.java`(로그 `msg`) — `kafka/TelemetryProducer.java`·`TelemetrySpool.java`는 **변경 없이 보존 테스트만** |
| 백엔드 알림 | `entity/AnomalyAlert.java`, `repository/AnomalyAlertRepository.java`(`insertIfAbsent`), `service/AnomalyService.java`, `dto/response/AnomalyResponse.java`, `db/migration/V4__add_anomaly_source_message_id.sql`(nullable, 인덱스는 추적 조회가 필요할 때만) |
| 지표 | 라벨 없는 누락 카운터 — `ContractMetricsTest` 라벨 경계 유지 |
| 감지기 | `anomaly-detector/contract.py`(`KNOWN_FIELDS` + 형식), `anomaly_detector.py`(`process` 복사), `tests/` |
| producer | `simulator/vehicle_simulator.py`(`_to_payload`) |
| 변경 없음(확인만) | `dlq-tools/dlq.py`, 앱 `lib/core/models/anomaly.dart` |
| 문서 | `docs/telemetry-schema-decision-table.md`, `docs/anomaly-path-contract.md`, Runbook "한 건 찾기", ADR |

**완료 조건(§9에 더해):**
- `NON_NULL` 테스트 — ID 없는 메시지의 재직렬화 결과에 `message_id` 키가 **없다**.
- **혼합 버전 테스트** — 새 백엔드가 만든 Kafka value(ID 있음·없음)를 **구 계약**으로 검증했을 때 "없음"은 수용, "있음"은 거부(= 배포 순서가 필요한 이유를 테스트로 고정).
- `event_id` 불변 — 같은 원본에서 도입 전후 같은 `event_id`.
- spool 왕복 — `store`→`read`→발행 value에 ID 보존.
- V4 마이그레이션 Testcontainers — 기존 행 `NULL`, 새 알림에 값.
- 실제 스택 1건 추적(ID 있음) + 재전달·spool 드레인·저장 DLQ 재주입·감지 DLQ 재주입 각 1회에서 **같은 ID** — 각 1회 관측으로만 표현.

### 10-7. 결정이 필요한 것 (§8을 대체)

| # | 결정 | 선택지 | 추천 |
| --- | --- | --- | --- |
| ① | 이름 | `message_id`/`source_message_id` · `event_id` 재사용 | **`message_id`/`source_message_id`** — 10-2 |
| ② | 형식 | UUIDv7 문자열 · ULID · 임의 UUIDv4 | **UUIDv7**(시간 정렬, 표준 문자열 36자) |
| ③ | 형식 오류 | 계약 위반으로 거부 · ID만 버리고 수용 | **거부** — "계약 밖 값은 거부" 원칙과 같다. 대가: producer의 ID 생성 버그가 데이터 유실(DLQ 격리)로 이어진다 |
| ④ | 같은 ID 다른 payload | 막지 않고 식별 · 입구 캐시 탐지 | **막지 않고 식별** — 10-4 |
| ⑤ | 2단계 착수 | 1단계 추적 기록을 먼저 남기고 판단 · 바로 착수 | **1단계 먼저** — `(vehicle_id, timestamp)`로 부족한 사례가 실제로 나오면 착수 |

§8의 5번(감지 DLQ replay-count)은 2026-09-13에 고쳤다(§6-1).
