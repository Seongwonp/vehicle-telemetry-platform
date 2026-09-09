# 거부 사유별 관측성 (P0-2b) — 카운터 실사·설계·구현

> 상태: **구현 완료 (2026-09-09).** 알림 임계는 미정(정상 구간 미측정).
>
> 세 입구(MQTT·Kafka 저장·감지기)가 **같은 이름·같은 라벨**로 사유별 거부를 올린다.

## 0. 왜 실사부터 했나

"거부가 늘었다"를 보려고 지표를 추가하기 전에, **이미 있는 카운터가 무엇을 세는지**를
정확히 알아야 한다. 그러지 않으면 새 지표가 기존 것과 뜻이 겹치거나, 더 나쁘게는
**둘을 빼서 만든 수가 아무 뜻도 아닌** 상태가 된다.

**초안에서 실제로 그 실수를 했다.** `invalid − published = 미격리 메시지 수`라고 썼는데
성립하지 않는다 — 두 지표는 **처리 단계가 다르고, 집계 대상이 다르고, 재시도 횟수도
다를 수 있다.** 프로세스가 재시작하면 각자 0부터 다시 세는데 그 시점도 서로 다르다.
**빼서 유도한 수를 유실이나 미격리 건수로 읽으면 안 된다.**

## 1. 실사 — 지금 있는 카운터 전수

### 1-1. 저장 경로 (Java)

| 지표 | 라벨 | **정확히 무엇을 세나** | 상태 |
| --- | --- | --- | --- |
| `telemetry_mqtt_messages_received_total` | 없음 | MQTT 수신 시도 횟수 | 기존 |
| `telemetry_mqtt_messages_invalid_total` | 없음 | MQTT 입구의 **모든 거부 판정** 횟수(사유 무관) | 기존 |
| `telemetry_kafka_dlq_published_total` | `topic` | DLQ 발행 **성공을 확인한** 횟수 | 기존 |
| `telemetry_kafka_dlq_publish_failures_total` | `topic` | DLQ 발행 **실패·timeout을 관찰한** 횟수 | 기존 |
| `telemetry_influx_write_failures_total` | 없음 | InfluxDB 쓰기 실패(배치 단위) | 기존 |
| `telemetry_influx_points_written_total` | 없음 | 저장 성공 포인트 수 | 기존 |
| `telemetry_anomaly_stored_total` | `result=new\|duplicate` | 알림 저장 시도 결과 | 기존 |

### 1-2. 감지 경로 (Python)

| 지표 | 라벨 | 무엇을 세나 |
| --- | --- | --- |
| `telemetry_anomaly_processed_total` | 없음 | 계약 통과 + 룰 판정 완료 횟수 |
| `telemetry_anomaly_processing_failed_total` | 없음 | 계약 통과 후 처리 실패 횟수 |

### 1-3. 실사에서 드러난 것

- **저장 경로에는 사유별 구분이 아예 없었다.** MQTT는 하나로 뭉치고 Kafka 저장 입구는
  전용 거부 카운터가 없었다.
- **"발행 시도"를 세는 지표가 없었다.** 성공과 실패만 있었다.
- **timeout이 확정적 실패와 같이 묶여 있었다.** timeout은 브로커가 받았는지 모른다.

## 2. 네 단계를 구분한다 — 빼서 유도하지 않는다

| 단계 | 지표 | 라벨 |
| --- | --- | --- |
| ① 계약 거부 **판정** | `telemetry_contract_rejected_attempts_total` | `entrance`, `reason` |
| ② DLQ 발행 **시도** | `telemetry_kafka_dlq_publish_attempts_total` | `topic` |
| ③ 발행 **성공 확인** | `telemetry_kafka_dlq_published_total` (기존) | `topic` |
| ④ 발행 **실패·timeout 관찰** | `telemetry_kafka_dlq_publish_failures_total` (기존) | `topic` |
| ④-a 그중 **발행 여부 불명** | `telemetry_kafka_dlq_publish_indeterminate_total` | `topic` |

**각 단계는 자기 이름으로만 읽는다.** ①−③ 같은 뺄셈은 뜻이 없다 —
①은 판정 단계이고 ③은 발행 단계이며, 그 사이에 재시도가 끼거나 프로세스가 재시작하면
두 수의 기준 시점 자체가 달라진다.

### 2-1. timeout은 "발행되지 않았다"가 아니다 — 다만 적용 범위가 좁다

`send(...).get(10s)`가 timeout이면 **브로커가 받았는지 알 수 없다.**
그래서 ④-a를 따로 센다 — ④의 **부분집합**이고, 0이 아니면
**"실패했다"가 아니라 "모른다"**로 읽어야 한다.

**이 카운터가 세는 것은 "관찰한 timeout 중 발행 여부가 불명인 것"뿐이다.**

| 구현 | 알아보는 예외 |
| --- | --- |
| `ContractMetrics.isIndeterminate()` | `java.util.concurrent.TimeoutException`, `org.apache.kafka.common.errors.TimeoutException` |
| `_is_indeterminate()` (Python) | `KafkaTimeoutError`, `TimeoutError`, `RequestTimedOutError` |

**값이 0이라고 모든 발행 결과가 확정됐다는 뜻이 아니다.** 우리가 분류하지 못한 예외
중에도 실제로는 브로커에 닿았을 수 있는 것이 있다. 이 지표가 주장하는 것은
**"이만큼은 확실히 불명이다"**이지 "나머지는 확실하다"가 아니다.

**분류를 지금 넓히지 않는다.** 실제로 다른 예외를 관찰한 뒤에 근거를 갖고 추가한다 —
목록에 없는 예외를 상상해서 넣으면 그것도 근거 없는 주장이 된다.

크기 초과·직렬화 실패처럼 **클라이언트에서 확정적으로 실패한 것**은 여기 해당하지 않는다.

### 2-2. 기존 지표는 이름도 의미도 그대로 둔다

`alerts.yml`이 `telemetry_kafka_dlq_published_total`과
`telemetry_kafka_dlq_publish_failures_total`을 본다. **이름을 바꾸면 알림이 조용히 죽는다.**
`ContractMetricsTest.기존_이름_유지`가 이걸 고정한다.

`telemetry_mqtt_messages_invalid_total`도 그대로다 — MQTT 입구의 **모든** 거부를 센다.
새 `rejected_attempts`는 **계약 사유일 때만** 오른다. `TOPIC_VEHICLE_MISMATCH`는
MQTT 고유 검사라 계약 사유가 아니고, 섞으면 세 입구를 나란히 놓을 수 없다.
그래서 두 지표는 MQTT에서 **의도적으로 값이 다를 수 있다** — 중복이 아니다.

## 3. 고유 건수 — **식별 범위가 입구마다 다르다**

**모든 거부·DLQ 카운터는 "시도·관찰 횟수"이지 "고유 메시지 수"가 아니다.**
at-least-once라 같은 메시지가 여러 번 판정될 수 있고, 재전달 경로를 이미 여럿 측정했다.

| 경로 | 근거 |
| --- | --- |
| 배치 저장 실패 후 재시도 → 이미 DLQ로 보낸 레코드 재발행 | `TelemetryConsumer` 주석(의도된 트레이드오프) |
| DLQ 발행 실패 → 재시작 시 배치 전체 재전달 | `load-test/anomaly-contract-kafka/` s2→s3 |
| 리밸런싱 재전달 | `load-test/rebalance-redelivery/` (40건 관찰) |
| 강제 종료 후 재시작 | `load-test/storage-integrity/RESULT_20260904_kill_redelivery.md` |
| MQTT QoS 1 재전달 | 설계상 at-least-once |

### 3-1. Kafka 원본에서 오는 것 — 고유 키가 있다

DLQ 레코드의 `(x-dlq-origin-topic, x-dlq-origin-partition, x-dlq-origin-offset)` 조합이
**그 원본 레코드의 고유 키**다. 재전달돼도 같은 키라 중복을 구분할 수 있다.

```bash
docker exec telemetry-kafka kafka-console-consumer \
  --bootstrap-server localhost:29092 --topic vehicle-telemetry-dlq \
  --from-beginning --timeout-ms 15000 \
  --property print.headers=true --property print.value=false 2>/dev/null \
  | grep -o 'x-dlq-origin-topic:[^,]*,x-dlq-origin-partition:[^,]*,x-dlq-origin-offset:[^,]*' \
  | sort -u | wc -l
```

**단, DLQ를 원본 토픽으로 재주입하면 새 offset이 생긴다.** 그러면 같은 내용이라도
**다른 키**가 되므로 위 수는 "원본 레코드 수"가 아니라 "서로 다른 원본 위치 수"다.
재처리 이력은 `x-dlq-replay-count` 헤더로 따로 본다.

### 3-2. MQTT 입구에서 거부된 것 — **고유 건수를 셀 수 없다**

MQTT 입구에서 거부된 메시지는 **Kafka에 들어가기 전에 걸린 것**이라
`origin-topic/partition/offset`이 **아예 없다.** `vehicle-telemetry-mqtt-dlq`의 envelope에는
`mqtt_topic`·`reason`·`payload`만 있다.

**현재 식별 체계로는 MQTT 거부의 고유 건수를 셀 수 없다. 이건 한계이지 버그가 아니다.**

**payload 해시나 MQTT packet ID를 고유 이벤트 ID로 쓰지 않는다.**
- payload 해시는 **정상적으로 동일한 두 메시지**(같은 차량이 같은 밀리초에 같은 값)를
  하나로 접는다. 밀리초 충돌은 이 프로젝트에서 실제로 측정한 현상이다
  (`load-test/storage-integrity/RESULT_20260905_ms_collision.md`).
- MQTT packet ID는 **세션 안에서만** 유일하고 재연결하면 재사용된다.
  `cleanSession=false`에 재연결이 실제로 일어난다(2026-09-05 브로커 장애 측정).

둘 다 **"고유해 보이지만 아닌" 식별자**다. 그런 걸 고유 키로 쓰면 조용히 틀린 수가 나온다.
필요해지면 **발행 측이 메시지에 식별자를 넣는 것**이 맞는 방향이고, 그건 별도 설계다
(로드맵의 이벤트 상관관계 항목과 겹친다).

## 4. 시계열 수 — "전체 12개"가 아니다

`entrance`(3) × `reason`(4) = **12는 `rejected_attempts` 한 지표의 논리적 라벨 조합 수**다.

**실제 시계열은 그보다 많다.**
- 지표가 여럿이다(거부 1종 + DLQ 4종).
- DLQ 지표는 `topic`으로 갈린다(3종).
- **인스턴스마다 따로 잡힌다.** 감지기는 `replicas: 3`이고 backend도 `scale` 프로파일에서
  늘어난다. Prometheus는 `instance`/`job` 라벨을 자동으로 붙인다.
- Micrometer는 `application` 같은 공통 태그를 더 붙일 수 있다.

**낮은 카디널리티 원칙은 유지한다** — 우리가 붙이는 라벨은 위 표의 것뿐이고,
차량 수·payload 내용처럼 무한히 늘어나는 축은 넣지 않는다.
다만 **"전체 12개"라고 쓰지 않는다.** 인스턴스가 늘면 시계열도 는다.

집계할 때는 항상 `sum by (...)`로 인스턴스를 접고, 재시작으로 카운터가 0이 되는 것을
고려해 `rate()`/`increase()`를 쓴다.

## 5. 라벨에 넣지 않는 것

**차량 ID·payload·예외 메시지·필드 경로.** 카디널리티도 문제지만 Prometheus 라벨은
보존 기간 내내 남아 `docs/data-retention.md`의 정책 밖으로 개인정보가 새는 경로가 된다.
`TelemetryDecoder.describe()`가 사유에서 값을 빼는 것과 같은 이유다.

**어느 필드가 문제인지는 지표가 아니라 DLQ에서 본다** (`docs/runbook/dlq-reprocessing.md` 2-1절).

`ContractMetricsTest.라벨_경계`가 허용 키 집합(`entrance`/`reason`/`topic`)을 단언한다.

## 6. 검증 — 카운터 **증가 위치**를 본다

값의 크기가 아니라 어느 단계에서 오르는지가 계약이다.

| 확인 | 어디 |
| --- | --- |
| 계약 위반 1건 정상 격리 → 판정·시도·성공 각 1, 실패 0 | `TelemetryConsumerTest.지표_정상격리`, `test_지표_정상격리` |
| DLQ 발행 실패 시 **성공 카운터 미증가** | `TelemetryConsumerTest.지표_발행실패`, `test_지표_발행실패시_성공카운터는_안_오른다` |
| timeout은 실패이면서 **동시에 발행 여부 불명** | `ContractMetricsTest.timeout은_불명으로도_센다`, `test_지표_timeout은_발행여부_불명으로도_센다` |
| 확정적 실패는 불명이 아니다 | `ContractMetricsTest.확정적_실패는_불명이_아니다` |
| **같은 Kafka 원본 재전달 시 판정·시도가 다시 오른다** | `TelemetryConsumerTest.지표_재전달시_다시_오른다`, `test_지표_재전달시_다시_오른다` |
| 정상 입력·저장 장애를 거부로 잘못 세지 않는다 | `TelemetryConsumerTest.지표_오분류_없음`, `test_지표_정상입력과_알림실패는_거부로_안_센다` |
| MQTT 고유 사유는 계약 사유에 안 섞인다 | `MqttMessageHandlerTest.토픽불일치는_계약사유가_아니다` |
| 라벨 경계 / 기존 이름 유지 | `ContractMetricsTest` |
| **세 입구를 실제 Prometheus에서 구분 조회** | `load-test/schema-contract/evidence/20260909-P0-2b-metrics/` |

실제 Prometheus 결과:

```
sum by (entrance, reason) (telemetry_contract_rejected_attempts_total)
  anomaly-detector   MALFORMED_JSON              1
  anomaly-detector   PAYLOAD_VALIDATION_FAILED   1
  kafka-storage      MALFORMED_JSON              1
  kafka-storage      PAYLOAD_VALIDATION_FAILED   1
  mqtt               PAYLOAD_VALIDATION_FAILED   1
  mqtt               UNKNOWN_FIELD               1
```

Kafka 토픽은 저장 입구와 감지기가 **둘 다** 읽으므로 한 번 주입에 두 입구에 잡힌다 —
그게 정상이고, 그래서 두 입구를 구분해서 봐야 한다.

## 7. 대시보드

`monitoring/grafana/dashboards/pipeline-funnel.json`에 패널 4개를 추가했다.

- **읽는 법** 텍스트 패널 — "시도 횟수이지 고유 메시지 수가 아니다", 단계별 지표 표,
  `indeterminate`는 "실패"가 아니라 "모른다", 고유 건수는 DLQ에서 세되 **MQTT는 셀 수 없다**.
- 계약 거부 판정 — 입구·사유별
- DLQ 격리 단계 — **시도/성공/실패/불명을 나란히**(비율을 만들지 않는다)
- 입구별 DLQ 격리 — `topic`이 입구를 가른다

## 8. 알림 — 아직 정하지 않았다

`dlq_publish_failures`가 0보다 크면 즉시 봐야 한다는 것은 분명하다(기존 알림이 이미 있다).
사유 분포 변화 알림(`UNKNOWN_FIELD` 급증 = 발행 측 스펙이 앞서갔다는 신호)은
**정상 구간을 측정한 뒤에** 임계를 정한다. 지금 정하면 근거 없는 숫자가 된다.

## 9. 한계

- **알림 임계 미정.** 정상 구간 분포를 안 쟀다.
- **부하 중에 재지 않았다.** 지표 추가가 처리량에 주는 영향은 미측정이다.
- **재전달 이중 계수는 단위 테스트로만 확인했다.** 실제 리밸런싱·재시작에서 카운터가
  어떻게 움직이는지는 안 봤다.
- **MQTT 거부의 고유 건수는 여전히 셀 수 없다**(§3-2). 발행 측 식별자가 필요하다.
- **`x-dlq-replay-count`를 지표로 노출하지 않았다.** 재처리 이력은 DLQ 헤더로만 본다.
- 실제 Prometheus 확인은 **1회**, 4건 주입이다.
