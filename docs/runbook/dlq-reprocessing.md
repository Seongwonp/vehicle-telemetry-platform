# Runbook — DLQ 재처리

DLQ에 메시지가 쌓였을 때 무엇을 확인하고 어떻게 되돌릴지. 도구는 `dlq-tools/dlq.py`.

> 이 문서 이전까지 DLQ는 **격리해서 유실을 막는 데까지만** 구현돼 있었다(ADR-017).
> "재처리 컨슈머는 아직 없음"이라는 주석이 코드 여러 곳에 있었고, 실제로 없었다.

## 0. DLQ 토픽 지도

| 토픽 | 무엇이 들어오나 | 발행 주체 |
| --- | --- | --- |
| `vehicle-telemetry-dlq` | 텔레메트리 저장 실패 (**입력 계약 위반**, JSON 파싱, 타임스탬프 변환, 재시도 소진) | backend `TelemetryConsumer` |
| `vehicle-anomaly-alerts-dlq` | 이상 알림 저장 실패 | backend `TelemetryConsumer` |
| `vehicle-telemetry-anomaly-dlq` | 이상 감지 경로의 **입력 계약 위반**·처리 실패 | Python `anomaly_detector.py` |
| `vehicle-telemetry-mqtt-dlq` | MQTT 수신 단계의 **입력 계약 위반**·타임스탬프·토픽 불일치 | backend `MqttInvalidMessagePublisher` |

`vehicle-telemetry-dlq`에는 **성질이 다른 두 경로**가 섞여 들어온다.

- **레코드 단위 격리** (`sendToDlq`) — 그 메시지 하나가 처리 불가능한 경우.
  `x-dlq-failure-class` 등 `x-dlq-*` 헤더가 붙는다.
- **재시도 소진** (`DeadLetterPublishingRecoverer`) — InfluxDB 장애 등으로 배치 전체가
  재시도를 다 쓴 경우. Spring이 `kafka_dlt-*` 헤더를 붙인다.

도구는 두 헤더를 모두 읽는다. 재시도 소진 경로에서는 **`kafka_dlt-exception-cause-fqcn`을
먼저 봐야 한다** — Spring이 리스너 예외를 `ListenerExecutionFailedException`으로 감싸므로
`kafka_dlt-exception-fqcn`만 보면 InfluxDB 장애든 JSON 파싱 실패든 전부 같은 wrapper
이름으로 보인다. 실제로 InfluxDB 장애를 주입했더니 DLQ 76,878건이 전부 `unknown`으로
분류돼 자동 재처리 대상이 하나도 안 나왔고(진짜 원인은 `InfluxException`), 그때 고쳤다.

## 1. 알림이 떴다 — 먼저 무엇을 보나

`monitoring/prometheus/alerts.yml`의 DLQ 알림은 `telemetry.kafka.dlq.published`를 본다.
알림이 떴다는 건 "격리가 일어났다"이지 "무엇이 잘못됐다"가 아니다. 원인을 먼저 분류한다.

```bash
docker run --rm --network vehicle-telemetry-platform_telemetry-net \
  -v "$PWD/dlq-tools:/w" -w /w vehicle-telemetry-platform-anomaly-detector \
  python dlq.py --topic vehicle-telemetry-dlq inspect --show-samples
```

출력의 **판정** 절이 결정을 좌우한다.

| 판정 | 뜻 | 조치 |
| --- | --- | --- |
| `transient` | 외부 의존성 장애·타임아웃 — 다시 넣으면 성공한다 | 원인 복구 후 재처리 |
| `permanent` | 메시지 자체가 처리 불가능 — 몇 번을 넣어도 실패한다 | **재처리 금지.** 발행 측을 고쳐야 한다 |
| `unknown` | 헤더가 없는 옛 레코드이거나 분류 목록에 없는 예외 | 사람이 표본을 보고 판단 |

`unknown`을 자동으로 재처리하지 않는 것은 의도된 설계다. 모르는 것을 일시적이라고
가정하면 영구 실패를 루프에 태우게 된다.

## 2. `permanent`가 대부분이라면 — 재처리가 답이 아니다

깨진 JSON을 원본 토픽에 다시 넣으면 **원본 → 컨슈머 실패 → DLQ**로 되돌아온다.
무한 루프이고, 그동안 정상 트래픽의 처리량까지 갉아먹는다.

할 일은 **발행 측을 고치는 것**이다. `inspect --show-samples`로 payload를 보고:

- 시뮬레이터/동글이 스펙과 다른 형식을 보내는가 → 발행 측 수정
- 우리 파서가 유효한 형식을 못 받아들이는가 → `VehicleTelemetry` / `toPoint()` 수정

**코드를 고쳐서 이제 처리 가능해졌다면** 그때 비로소 `--include-permanent`로 되돌린다.
이 플래그를 쓸 때는 왜 안전해졌는지가 설명돼야 한다.

## 2-1. `TelemetryContractException` — 입력 계약 위반 (2026-09-09 추가)

**지금 `vehicle-telemetry-dlq`에서 가장 흔히 볼 타입이다.** 2026-09-09 P0-2에서 두 입구
(MQTT·Kafka 직접)가 공통 decoder(`TelemetryDecoder`)를 쓰게 하면서 생겼다. 그 전까지
Kafka 직접 주입에는 검증이 아예 없어서, **지금 DLQ로 오는 것 중 상당수는 예전에는 조용히
저장되던 것**이다. DLQ가 늘었다고 해서 새 장애가 난 것이 아닐 수 있다 — 안 보이던 것이
보이기 시작한 것이다.

**분류는 항상 `permanent`다.** payload가 그대로인 한 몇 번을 되돌려도 같은 자리에서 실패한다.
`dlq-tools/dlq.py`의 `PERMANENT_MARKERS`에 들어 있고, 회귀는 `dlq-tools/test_dlq.py`가 막는다.

### 사유 코드 — `x-dlq-failure-message`의 **맨 앞**에 있다

형식은 `<사유 코드>: <상세>`다. 상세에는 **값이 들어가지 않는다** — 거부 사유는 로그와
DLQ 헤더에 남고 그 보존 기간 동안 좌표·식별자가 같이 남기 때문이다
(`docs/data-retention.md`).

| 사유 코드 | 언제 | 상세에 담기는 것 | 무엇을 고치나 |
| --- | --- | --- | --- |
| `MALFORMED_JSON` | JSON 자체가 안 읽힌다 | 파서 메시지 | 발행 측 직렬화·인코딩·잘린 전송 |
| `UNKNOWN_FIELD` | 계약에 없는 필드가 있다 | 그 필드명 | 오타(`sped`)거나 **발행 측이 우리보다 새 스펙**이다 |
| `TYPE_MISMATCH` | 타입이 안 맞는다 | 필드 경로 | 발행 측 타입. 최상위가 `null`이면 `(최상위 null)` |
| `PAYLOAD_VALIDATION_FAILED` | 형식은 맞고 **값이 계약 밖**이다 | `필드 사유` 목록 | 누락·범위 초과·DTC 형식 |

**`UNKNOWN_FIELD`는 발행 측 잘못이 아닐 수 있는 유일한 칸이다.** 동글이나 시뮬레이터에
필드가 추가됐는데 우리 스키마가 안 따라간 경우, 고칠 곳은 발행 측이 아니라
`VehicleTelemetry`와 `docs/telemetry-schema-decision-table.md`다.

### 사유 코드별로 세어보기

`inspect`는 타입까지만 가른다. 어느 사유가 몇 건인지는 헤더를 직접 센다.

```bash
docker exec telemetry-kafka kafka-console-consumer   --bootstrap-server localhost:29092 --topic vehicle-telemetry-dlq   --from-beginning --timeout-ms 15000   --property print.headers=true --property print.value=false 2>/dev/null   | grep -o 'x-dlq-failure-message:[A-Z_]*' | sort | uniq -c | sort -rn
```

MQTT 쪽은 헤더가 아니라 **envelope JSON**이라 읽는 법이 다르다. 그리고 **사유 코드만 있고
상세가 없다** — 어느 필드가 문제인지는 `payload`를 직접 봐야 한다(2026-09-09 E2E에서 확인).

```bash
docker exec telemetry-kafka kafka-console-consumer   --bootstrap-server localhost:29092 --topic vehicle-telemetry-mqtt-dlq   --from-beginning --timeout-ms 15000 2>/dev/null   | python -c 'import sys,json,collections; c=collections.Counter(json.loads(l)["reason"] for l in sys.stdin if l.strip()); print(c)'
```

### 고치고 되돌리는 절차

**재주입만으로는 다시 실패한다.** 계약 위반은 payload의 성질이지 환경의 상태가 아니다.
`--include-permanent`를 먼저 쓰면 같은 레코드가 DLQ로 되돌아오고 `x-dlq-replay-count`만 는다.

1. **무엇이 왜 거부됐는지 확정한다.** 위 사유 코드 집계 + `inspect --show-samples`.
   사유가 한 가지로 몰리지 않으면 원인이 여럿이다 — 한 번에 고치려 하지 마라.
2. **고칠 쪽을 정한다.** 발행 측(시뮬레이터·동글·부하 도구)인가, 우리 계약인가.
   계약을 넓히는 쪽이면 `docs/telemetry-schema-decision-table.md`에 근거를 먼저 적는다 —
   그 문서가 숫자의 단일 기준이고, 코드만 고치면 다음 사람이 근거 없이 되돌린다.
3. **고쳤는지 코드로 확인한다.** 표본 payload를 `TelemetryContractTest`에 fixture로
   넣고 통과하는지 본다. **DLQ를 되돌려서 확인하지 마라** — 실패하면 두 번 실패한다.
   ```bash
   cd backend && ./gradlew test --tests '*TelemetryContractTest' --tests '*BothEntrancesSameContractTest'
   ```
4. **배포한다.** 옛 이미지가 뜨면 검증이 통째로 무효다.
   ```bash
   docker compose -f docker-compose.yml -f docker-compose.dev.yml build backend
   docker compose -f docker-compose.yml -f docker-compose.dev.yml up -d backend
   ```
5. **그 다음에** 되돌린다. dry-run으로 건수를 먼저 본다.
   ```bash
   docker run --rm --network vehicle-telemetry-platform_telemetry-net      -v "$PWD/dlq-tools:/w" -w /w vehicle-telemetry-platform-anomaly-detector      python dlq.py --topic vehicle-telemetry-dlq replay --include-permanent
   # 건수가 맞으면 --execute 추가
   ```
6. **DLQ가 다시 느는지 본다.** 늘면 3번이 틀린 것이다. 즉시 멈춰라.

### `vehicle-telemetry-mqtt-dlq`는 이 절차로 되돌릴 수 없다

payload가 원본이 아니라 envelope(`{"mqtt_topic":…,"reason":…,"payload":…}`)이라
재처리가 미지원이다. 고친 뒤 원본을 다시 흘려보내려면 envelope의 `payload`를 꺼내
직접 발행해야 한다. **여전히 자동화하지 않았다**(§5).

## 2-2. 감지 경로의 계약 위반 — `ContractViolation` (2026-09-09 추가)

P0-2a에서 **저장 경로와 같은 입력 계약**을 감지 경로에도 적용했다. 그래서
`vehicle-telemetry-anomaly-dlq`에 `ContractViolation`이 들어온다.

**저장 경로와 사유 코드가 같다**(§2-1의 표를 그대로 쓴다). 다른 것은 두 가지다.

| | 저장 경로 | 감지 경로 |
| --- | --- | --- |
| 토픽 | `vehicle-telemetry-dlq` | `vehicle-telemetry-anomaly-dlq` |
| 예외 이름 | `TelemetryContractException` | `ContractViolation` |
| 사유 코드 위치 | `x-dlq-failure-message` 앞부분 | **`x-dlq-contract-reason` 헤더에 따로** |
| 경로 표시 | 없음 | `x-dlq-source-path: anomaly-detector` |

**같은 payload는 양쪽에서 같은 사유로 거부된다.** 한쪽에만 있으면 둘 중 하나가
틀린 것이다 — `contract-fixtures/cases.json`을 양쪽 테스트가 읽어 막고 있다.

### 사유별 집계는 지표로 본다 — **세 입구 모두**

```
sum by (entrance, reason) (telemetry_contract_rejected_attempts_total)
```

`entrance`는 `mqtt` / `kafka-storage` / `anomaly-detector`. DLQ 단계는 `topic`이 입구를 가른다.

**이 수는 판정 횟수이지 고유 메시지 수가 아니다.** 재전달되면 다시 오른다 —
Kafka 원본의 고유 건수는 DLQ 헤더 `(origin-topic, partition, offset)` 조합으로 세고,
**MQTT 거부는 그 식별자가 없어 고유 건수를 셀 수 없다.**
자세한 것은 `docs/rejection-metrics-design.md` 3절.

**라벨은 사유 코드뿐이다.** 어느 차량·어느 필드인지는 지표에 없다 — DLQ를 봐야 한다.
그렇게 만든 이유는 Prometheus 라벨이 보존 기간 내내 남아 개인정보가 새기 때문이다.

헤더로 세려면:

```bash
docker exec telemetry-kafka kafka-console-consumer   --bootstrap-server localhost:29092 --topic vehicle-telemetry-anomaly-dlq   --from-beginning --timeout-ms 15000   --property print.headers=true --property print.value=false 2>/dev/null   | grep -o 'x-dlq-contract-reason:[A-Z_]*' | sort | uniq -c | sort -rn
```

### 분류에서 조심할 것 — 이름만 보고 영구로 단정하지 않는다

| 예외 | 분류 | 뜻 |
| --- | --- | --- |
| `ContractViolation` | `permanent` | 검증기가 만든 것. **재주입만으로는 다시 실패한다** |
| `TypeError`/`KeyError`/`ValueError`/`AttributeError` | `unknown` | 계약을 통과한 뒤 나온 것이라 **구현 버그일 수 있다** |
| `KafkaTimeoutError`/`NoBrokersAvailable` | `unknown` | **발생 위치에 따라 다르다**(아래) |

`unknown`이 보이면 표본을 열어 스택을 봐라. **구현 버그면 고친 뒤 재처리가 성공한다** —
영구로 분류해두면 고친 다음에도 자동 재처리에서 빠진다.

**Kafka 타임아웃이 특히 미묘하다.** 알림 발행 중 타임아웃이면 브로커가 **이미 받았을 수
있어** 재처리가 중복 알림을 만든다 — 다만 `UNIQUE(event_id)` + `ON CONFLICT DO NOTHING`이
막아 행은 늘지 않는다(2026-09-05 확인). DLQ 발행 중 타임아웃이면 애초에 DLQ에 레코드가
안 남는다(원본 offset 미커밋 → 재전달).

### 격리 실패는 조용히 넘어가지 않는다

DLQ 발행이 실패하면 감지기는 예외를 밖으로 던지고 **아무 offset도 커밋하지 않는다.**
그 배치 전체가 재전달된다 — 중복 처리는 생기지만 격리 못 한 것을 완료로 치지는 않는다.
실측: `anomaly-detector/tests/test_consume_loop.py`.

## 3. `transient`라면 — 원인부터 복구하고 재처리

순서를 지켜야 한다. 원인이 살아있지 않은 상태에서 재처리하면 그대로 다시 실패해
`x-dlq-replay-count`만 소모한다(3회를 넘기면 도구가 건너뛴다).

```bash
# (1) 원인 복구 확인 — 예: InfluxDB
curl -s localhost:8086/health
curl -s localhost:8080/actuator/prometheus | grep telemetry_influx_write_failures_total

# (2) dry-run — 몇 건이 대상인지부터 본다 (--execute가 없으면 아무것도 발행하지 않는다)
python dlq.py --topic vehicle-telemetry-dlq replay --target vehicle-telemetry

# (3) 실제 재처리
python dlq.py --topic vehicle-telemetry-dlq replay --target vehicle-telemetry --execute
```

재처리 후 확인:

```bash
# 원본 토픽 소비가 정상으로 도는지
docker exec telemetry-kafka kafka-consumer-groups --bootstrap-server localhost:29092 \
  --describe --group telemetry-storage-group

# DLQ가 다시 늘지 않는지 (늘면 분류가 틀렸거나 원인이 안 고쳐졌다)
python dlq.py --topic vehicle-telemetry-dlq inspect
```

## 4. 반드시 알고 있어야 할 제약

- **원본 DLQ 레코드는 지워지지 않는다.** Kafka는 임의 레코드 삭제를 지원하지 않는다.
  재처리에 성공해도 DLQ에는 그대로 남아 있고, retention이 만료돼야 사라진다.
  그래서 `replay`는 **커서**(consumer group `dlq-replay-<topic>`의 커밋 offset)를 써서
  이미 되돌린 지점 이후만 본다. 커서가 없으면 실행할 때마다 DLQ 전체를 다시 되돌리고
  그것들이 또 실패해 쌓이므로 **레코드가 배로 늘어난다** — 실측으로 확인했다:

  | 재처리 시도 | 커서 없음 | 커서 있음 |
  | ---: | ---: | ---: |
  | 1 | 2건 | 2건 |
  | 2 | 4건 | 2건 |
  | 3 | 8건 | 2건 |
  | 4 | 16건 | **0건** (횟수 초과로 차단) |
  | 5 | 30건 | 0건 |

  커서를 넣으면 총량이 `원본 × (1 + max_replays)`로 상한이 잡힌다(위 실측에서 8건).
  `--max-replays`만으로는 막지 못한다 — 원본 레코드의 카운트는 늘 0이라 매번 대상이 된다.
- **커서는 건너뛴 레코드도 지나친다.** 분류 때문에 건너뛴 레코드를 나중에 다시 보려면
  `--group`에 새 이름을 줘서 처음부터 읽어야 한다. `inspect`는 커서를 쓰지 않으므로
  언제든 전체를 볼 수 있다.
- **`x-dlq-replay-count`는 컨슈머가 이어받아야 동작한다.** 되돌린 메시지가 다시 실패해
  DLQ로 갈 때 이 헤더를 승계하지 않으면 카운터가 매번 0으로 리셋된다(Java·Python 양쪽에
  구현돼 있고 회귀 테스트로 고정했다).
- **재처리는 중복을 만든다 — 하지만 InfluxDB에서는 흡수된다.** 포인트 identity가
  (measurement, `vehicle_id`, ms 타임스탬프)라 같은 메시지를 다시 써도 덮어써진다.
  실측으로 확인했다(`load-test/storage-integrity/RESULT_20260904_kill_redelivery.md`:
  재전달 68건, 행 증가 0).
- **PostgreSQL로 가는 이상 알림도 중복되지 않는다 — 실측했다.**
  `anomaly_alerts.event_id`(= `vehicle_id|timestamp|anomaly_type|field|detector`의 SHA-256)에
  UNIQUE 인덱스가 있고 저장이 `ON CONFLICT DO NOTHING`이다.
  같은 DLQ 레코드를 **커서를 바꿔 두 번** 되돌려도 행이 늘지 않았고, 행 수가 토픽의
  고유 event_id 수와 정확히 일치했다(16,636)
  — `load-test/anomaly-dlq-idempotency/RESULT_20260905_alert_replay.md`.

  **재처리는 이미 저장된 알림을 반드시 다시 넣는다.** 그 실측에서 DLQ 9건 중 3건이
  이미 저장돼 있었다 — 서버 커밋은 끝났는데 연결이 끊겨 클라이언트만 실패로 본
  경우(`Unable to commit`)다. 그러니 "재처리 건수 = 복구된 건수"가 아니다.
  실제로 몇 건이 새로 들어갔는지는 지표로 본다:

  ```bash
  curl -s localhost:8080/actuator/prometheus | grep telemetry_anomaly_stored
  # result="new"       … 실제로 저장된 건수
  # result="duplicate" … 이미 있어서 건너뛴 건수
  ```

  로그로도 `[이상 저장]`(신규)과 `[이상 중복]`(건너뜀)이 나뉜다. 예전에는 둘 다
  `[이상 저장]`으로 찍혀서 재처리 후 로그를 세면 저장 건수가 부풀려졌고,
  **중복분까지 WebSocket 알림이 다시 나가고 있었다**(같은 문서에서 고쳤다).
- **분류 목록은 완전하지 않다.** `dlq.py`의 `TRANSIENT_MARKERS`/`PERMANENT_MARKERS`는
  지금까지 본 예외만 담고 있다. 새 예외는 `unknown`으로 떨어지므로, `inspect`에서
  `unknown` 비중이 크면 목록을 늘려야 한다.
- **`vehicle-telemetry-mqtt-dlq`는 형태가 다르다.** payload가 원본 바이트가 아니라
  `mqtt_topic`을 포함한 envelope이라, 원본 토픽으로 그대로 되돌릴 수 없다.
  이 토픽의 재처리는 아직 지원하지 않는다.

## 4-1. 알아둘 것 — 짧은 장애도 DLQ를 주 경로로 만든다

InfluxDB를 **90초** 정지시키자 그 구간 트래픽의 대부분인 **76,878건(전체의 47.6%)이
DLQ로 갔다**(`load-test/fault-injection/RESULT_20260904_fault_injection.md`).

원인은 재시도 예산이다. `KafkaConfig`의 `FixedBackOff(1000L, 2L)`는 **3회 시도 / 약 2초**라,
2초를 넘기는 장애에서는 사실상 모든 메시지가 DLQ로 간다. 현실의 의존성 장애는 거의 항상
2초보다 길다 — **DLQ가 예외 경로가 아니라 주 경로가 된다.**

유실은 아니다. 재처리로 **완전히 복구된다는 것을 실측했다**(InfluxDB 행 84,615 → 161,356,
Kafka 토픽 수와 정확히 일치). 하지만 그 복구는 **사람이 이 Runbook을 보고 수동으로**
돌려야 하고, 장애 때마다 수만 건을 되돌려야 한다는 뜻이다.

따라서 운영 관점에서는 **재시도 예산을 늘리는 편이 낫다**. 재시도는 멱등하고
(`load-test/storage-integrity/`에서 확인), 재시도 중 쌓이는 lag은 이미 알림으로 드러난다.
그러면 DLQ에는 진짜 처리 불가능한 메시지만 남는다.

**이 변경은 그 뒤에 했다.** `FixedBackOff(1000L, 2L)`를 `ExponentialBackOff` +
재시도 예산 180초(`telemetry.kafka.retry.budget-ms`)로 바꿨고, 같은 InfluxDB
90초 장애에서 **DLQ 76,878건 → 0건**이 됐다(InfluxDB 행이 토픽 수와 정확히 일치,
리밸런싱·백오프 소진 로그 0건). 즉 위 수치는 **바뀌기 전의 기록**이다.

## 4-2. 이상 알림 경로도 이제 재시도 예산을 탄다 (2026-09-05 변경)

**예전에는** `consumeAnomalyAlerts`가 저장 실패를 직접 잡아 재시도 없이 바로 DLQ로
보냈다. 알림 저장을 배치화하면서(ADR-022) 이 동작을 바꿨다 — 배치에서 그러면 수천
건이 한꺼번에 DLQ로 가기 때문이다. 이제는 offset을 커밋하지 않고 예외를 던져
**180초 재시도 예산**에 맡기고, 소진되면 `DeadLetterPublishingRecoverer`가 처리한다.
텔레메트리 경로와 같은 정책이다.

역직렬화·변환 실패(깨진 JSON, 잘못된 타임스탬프)는 여전히 **레코드 단위로 격리**해
그 한 건만 DLQ로 보낸다. 배치 전체를 재시도시킬 이유가 없는 영구 실패이기 때문이다.

**바뀐 뒤 같은 장애를 다시 주입해 확인했다.** PostgreSQL 60초 장애에서
**유실이 6건 → 0건**이 됐다. 예전에는 그 6건이 사람이 재처리해야만 복구됐고,
이제는 재시도 예산 안에 DB가 살아나면 자동으로 저장된다. DLQ에 남은 6건은
**전부 이미 저장돼 있는 것**이었다(in-doubt 커밋) — 즉 **DLQ에 복구가 필요한 알림이
하나도 없다.** (`load-test/anomaly-dlq-idempotency/RESULT_20260905_alert_replay.md`)

그래도 PostgreSQL 장애에서는 DLQ 자체가 거의 쌓이지 않는다. 60초 정지를 주입했더니
DLQ로 간 알림은 **9건뿐**이었고, 나머지는 전부 lag으로 쌓였다. 컨슈머가 실패로
빠르게 떨어지는 게 아니라 HikariCP `connectionTimeout`(기본 30초)만큼 **붙잡혀 있기**
때문이다(파티션 3개 × 30초에 1건 = 초당 0.1건).

운영상 함의: **PostgreSQL 장애 때는 DLQ가 아니라 lag을 봐야 한다.**

### 300초 장애로도 재봤다 — DLQ 0건, 리밸런싱 0건, 유실 0건

`max.poll.interval.ms`(300초)를 넘기는 장애가 어떻게 되는지 미측정으로 남아 있었다.
300초로 늘려 돌린 결과는 위와 같다(행 43,347 = 토픽 고유 event_id 43,347).

**"180초 예산"인데 300초를 견디는 이유**: `ExponentialBackOff.maxElapsedTime`이 세는 것은
**백오프로 쉰 시간의 합**이지 벽시계 시간이 아니다. 시도마다 HikariCP
`connectionTimeout`(30초)을 기다리느라 실패 자체에 30초씩 쓰이고, 백오프 합
(1+2+4+8+16+30…)은 그 사이 180초에 도달하지 못한다. 기본값 기준 실효 내성은
대략 **8분** 수준이다.

같은 이유로 "예산 &lt; `max.poll.interval.ms`"라는 관계도 그대로 성립하지 않는다 —
컨슈머가 쫓겨나는 기준은 **poll 사이의 벽시계 시간**이다. 300초에서는 0건이었지만
**더 긴 장애에서 어디서 도는지는 재지 않았다.**

> 이 300초 측정에서 **버그를 하나 찾아 고쳤다.** `AnomalyService`의 클래스 레벨
> `@Transactional(readOnly = true)` 때문에 DB를 안 건드리는 `toEntity()`까지
> 트랜잭션을 열고 있었고, 그래서 DB 장애 중 **변환 단계에서** 실패해 레코드가
> 하나씩 DLQ로 갔다(DLQ 19건 = 고유 알림 8건). 고친 뒤 같은 장애에서 DLQ 0건.
> 자세한 내용은 `load-test/anomaly-dlq-idempotency/RESULT_20260905_alert_replay.md`.

## 5. 아직 안 한 것

- **자동 재처리 컨슈머는 없고, 만들 계획도 아직 없다.** 위 절차는 사람이 판단해서
  돌리는 수동 절차다. 자동화하려면 "언제 원인이 복구됐다고 볼 것인가"를 기계가
  판정해야 하는데, 그 판정을 틀리면 루프가 된다.
- **재처리 결과의 정합성 대조**는 이 도구 범위 밖이다. 되돌린 뒤 실제로 저장됐는지는
  `load-test/storage-integrity/measure_integrity.py`로 따로 확인한다.
- **PostgreSQL 장애가 5분을 넘을 때 리밸런싱이 도는지 미측정.** 컨슈머가 건당 30초씩
  붙잡히므로 `max.poll.interval.ms`(300초) 안에 10건밖에 처리하지 못한다.
- ~~HikariCP `connectionTimeout` 30초를 줄일지~~ — **줄이지 않기로 했다(2026-09-05).**
  원래 근거는 "줄이면 장애 중 DLQ가 정상적으로 쌓여 이 재처리 절차가 의미를 갖는다"였다.
  그 전제가 사라졌다 — 알림 경로가 재시도 예산을 타게 된 뒤로 **PostgreSQL 300초 장애에서
  DLQ 0건, 유실 0건**이다. 사람이 재처리할 일이 애초에 안 생긴다.

  게다가 30초라는 값이 그 결과를 **만들어내는** 요소다. 시도마다 30초를 기다리는 덕분에
  백오프 합(= 재시도 예산)이 천천히 차서 실효 내성이 8분까지 늘어난다. 줄이면
  재시도가 빨리 소진돼 **DLQ로 더 빨리 가고 사람 손이 더 자주 필요해진다.**
  느린 DB에서 정상 요청이 실패로 떨어지는 위험도 그대로다. 바꿀 이유가 없다.
