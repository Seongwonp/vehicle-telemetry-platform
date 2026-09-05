# Runbook — DLQ 재처리

DLQ에 메시지가 쌓였을 때 무엇을 확인하고 어떻게 되돌릴지. 도구는 `dlq-tools/dlq.py`.

> 이 문서 이전까지 DLQ는 **격리해서 유실을 막는 데까지만** 구현돼 있었다(ADR-017).
> "재처리 컨슈머는 아직 없음"이라는 주석이 코드 여러 곳에 있었고, 실제로 없었다.

## 0. DLQ 토픽 지도

| 토픽 | 무엇이 들어오나 | 발행 주체 |
| --- | --- | --- |
| `vehicle-telemetry-dlq` | 텔레메트리 저장 실패 (JSON 파싱, 타임스탬프 변환, 재시도 소진) | backend `TelemetryConsumer` |
| `vehicle-anomaly-alerts-dlq` | 이상 알림 저장 실패 | backend `TelemetryConsumer` |
| `vehicle-telemetry-anomaly-dlq` | 이상 감지 처리 실패 | Python `anomaly_detector.py` |
| `vehicle-telemetry-mqtt-dlq` | MQTT 수신 단계에서 형식이 틀린 메시지 | backend `MqttInvalidMessagePublisher` |

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
총 경과 시간 예산 180초(`telemetry.kafka.retry.budget-ms`)로 바꿨고, 같은 InfluxDB
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
이번 60초 장애에서는 리밸런싱이 없었지만(`generation 1` 유지),
`max.poll.interval.ms`가 300초라 장애가 길어지면 도는지는 **재지 않았다.**

## 5. 아직 안 한 것

- **자동 재처리 컨슈머는 없고, 만들 계획도 아직 없다.** 위 절차는 사람이 판단해서
  돌리는 수동 절차다. 자동화하려면 "언제 원인이 복구됐다고 볼 것인가"를 기계가
  판정해야 하는데, 그 판정을 틀리면 루프가 된다.
- **재처리 결과의 정합성 대조**는 이 도구 범위 밖이다. 되돌린 뒤 실제로 저장됐는지는
  `load-test/storage-integrity/measure_integrity.py`로 따로 확인한다.
- **PostgreSQL 장애가 5분을 넘을 때 리밸런싱이 도는지 미측정.** 컨슈머가 건당 30초씩
  붙잡히므로 `max.poll.interval.ms`(300초) 안에 10건밖에 처리하지 못한다.
- **HikariCP `connectionTimeout` 30초를 줄일지 정하지 않았다.** 줄이면 장애 중 DLQ가
  정상적으로 쌓여 위 재처리 절차가 실제로 의미를 갖지만, 느린 DB에서 정상 요청이
  실패로 떨어질 수 있다. 트레이드오프를 재지 않았다.
