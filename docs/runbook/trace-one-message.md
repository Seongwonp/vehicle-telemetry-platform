# Runbook — 텔레메트리 한 건을 끝까지 찾기 (`vehicle`, `ts`)

"차량 SIM-001이 `2026-09-28T09:40:12.417Z`에 보낸 한 건이 어디까지 갔나"에 답하는 절차다.
원본 메시지에는 전용 ID가 없다(ADR-028, 이벤트 상관관계 1단계). 대신 **`(vehicle_id, timestamp)`가 전 구간의 대리 키**이고,
이 runbook은 그 두 값으로 로그·Kafka·InfluxDB·PostgreSQL·WebSocket을 차례로 조회한다.

| 구간 | 키 | 로그의 필드 이름 |
| --- | --- | --- |
| 백엔드·감지기 로그 | `vehicle=`, `ts=`(원본 `timestamp` 문자열 그대로) | 정상 경로·거부 경로 공통 |
| 거부(MQTT DLQ, 저장 DLQ, 감지 DLQ) | `payloadSha256=` — 원본 바이트의 SHA-256 | MQTT 거부·Kafka 저장 DLQ·감지 DLQ가 **같은 함수**로 같은 값 |
| Kafka | key = `vehicle_id`, `(partition, offset)` | `partition=`, `offset=` |
| InfluxDB | tag `vehicle_id` + `_time`(밀리초) | — |
| PostgreSQL 알림 | `vehicle_id` + `vehicle_timestamp`, 알림 자체는 `event_id` | `event=` |

> **한계를 먼저**: 같은 차량이 **같은 밀리초**에 두 건을 보내면 InfluxDB는 뒤가 앞을 덮어쓰고 이 키로는 둘을 구분할 수 없다.
> 깨진 JSON은 `vehicle`·`ts`를 읽을 수 없어 `payloadSha256`과 수신 시각으로만 찾는다. 이것이 2단계(optional `message_id`)를
> 여는 조건이다 — `docs/event-correlation-design.md` §7.

아래 `V`·`T`를 채운다. `T`는 payload의 `timestamp` **문자열 그대로**(정규화하지 않는다 — 로그는 원본을 남긴다).

```bash
V=SIM-001
T=2026-09-28T09:40:12.417Z
```

## 1. 수신됐나 — 백엔드 로그 (MQTT 입구)

정상 수신은 DEBUG다(`logging.level.com.telemetry=INFO`에서는 안 보인다). 거부는 WARN이라 항상 보인다.

```bash
docker compose logs --no-log-prefix backend | grep -F "vehicle=$V" | grep -F "ts=$T"
```

| 보이는 줄 | 뜻 | 다음 |
| --- | --- | --- |
| `[MQTT→Kafka] vehicle=… ts=…` (DEBUG) | 계약 통과, Kafka로 넘김 | 2 |
| `[MQTT] 메시지 거부 — … reason=… vehicle=… ts=… payloadSha256=…` | 거부. `vehicle`·`ts`는 계약을 통과한 뒤 MQTT 검사(`INVALID_TIMESTAMP`·`TOPIC_VEHICLE_MISMATCH`)에서 걸린 경우에만 채워지고 계약 위반이면 `-`다 — 그때는 topic의 차량 ID와 `payloadSha256`으로 찾는다 | `vehicle-telemetry-mqtt-dlq`에서 `payloadSha256`으로 찾는다(6) |
| 아무것도 없음 | 백엔드에 안 왔거나 DEBUG가 꺼져 있다 | 브로커 `$SYS` 수신 수(`telemetry_mqtt_broker_messages_received`)와 `telemetry_mqtt_messages_received_total`을 대조 — `docs/runbook/pipeline-observability.md` |

DEBUG를 켜지 않았다면 1을 건너뛰고 2로 간다 — Kafka에 있으면 수신된 것이다.

## 2. Kafka에 있나 — key와 payload로

`vehicle-telemetry` 토픽은 key가 `vehicle_id`다. 파티션은 key 해시로 정해지므로 세 파티션 중 하나에만 있다.

**좌표를 아는 경우(권장)** — 1단계의 `[Kafka] 전송 완료 — … partition=N offset=M` 줄이 있으면 그 한 건만 읽는다:

```bash
docker exec telemetry-kafka kafka-console-consumer --bootstrap-server kafka:29092   --topic vehicle-telemetry --partition N --offset M --max-messages 1 --timeout-ms 15000   --property print.key=true --property print.partition=true --property print.offset=true   --property key.separator='|' 2>/dev/null
```

**좌표를 모르는 경우** — 전체를 훑어 `timestamp` 문자열로 거른다. **비싸다**: 이 노트북에서 약 107k 메시지를 `--from-beginning`으로
읽는 데 13분이 넘어 중단했다(2026-09-28). 먼저 `kafka-get-offsets`로 끝 offset을 보고 `--partition N --offset <근처>`로 좁힌다.
파티션은 key 해시라 같은 차량은 항상 같은 파티션이다 — 그 차량의 다른 건 좌표를 알면 파티션은 재사용된다.

```bash
docker exec telemetry-kafka kafka-console-consumer --bootstrap-server kafka:29092   --topic vehicle-telemetry --from-beginning --timeout-ms 15000   --property print.key=true --property print.partition=true --property print.offset=true   --property key.separator='|' 2>/dev/null | grep -F "\"timestamp\":\"$T\"" | grep -F "$V"
```

출력 한 줄이 `Partition:N|Offset:M|SIM-001|{…}`다. **`(N, M)`을 적어 둔다** — 이후 DLQ 헤더의 `x-dlq-origin-partition/offset`과 대조한다.

백엔드가 Kafka 발행 실패로 spool에 넣었다면 로그에 `[Kafka] 브로커 전송 실패 — spool에 보관 vehicle=… ts=…`가 있고,
드레인되면 `[Kafka] spool 드레인 완료 — vehicle=… ts=… partition=… offset=…`(DEBUG)에 **새 좌표**가 찍힌다.
spool은 producer가 `delivery.timeout.ms`(기본 120초)를 넘긴 뒤에야 쓰인다 — 브로커가 그 안에 돌아오면 버퍼에서 그대로 나가고 spool 로그는 없다(2026-09-29 실측, 25초 정지에서 0건).

## 3. 저장됐나 — InfluxDB

```bash
docker exec telemetry-influxdb influx query --org "$INFLUXDB_ORG" --token "$INFLUXDB_TOKEN" \
  "from(bucket:\"$INFLUXDB_BUCKET\") |> range(start: time(v: \"$T\"), stop: time(v: \"$T\")) \
   |> filter(fn:(r)=>r._measurement==\"vehicle_telemetry\" and r.vehicle_id==\"$V\") \
   |> pivot(rowKey:[\"_time\"], columnKey:[\"_field\"], valueColumn:\"_value\")"
```

`range`의 `stop`은 배타라 정확히 그 시각 한 점을 잡으려면 `stop`을 1ms 뒤로 둔다(`|> range(start: T, stop: T+1ms)`) —
`influx`가 산술을 안 받으면 `stop`에 밀리초를 손으로 더한 문자열을 쓴다.

| 결과 | 뜻 | 다음 |
| --- | --- | --- |
| 1행 | 저장됨 | 4 |
| 0행, Kafka에는 있음 | 저장 경로에서 거부·실패 | 백엔드 로그 `[Kafka→InfluxDB] … partition=N offset=M payloadSha256=…` → `vehicle-telemetry-dlq`(6). 저장 **실패**(InfluxDB 장애)는 DLQ가 아니라 재시도라 lag이 오른다 |

## 4. 감지기가 봤나 — 감지기 로그

감지는 저장과 **다른 consumer group**이라 3과 독립이다. 이상이 없으면 감지기는 정상 메시지에 대해 아무 줄도 남기지 않는다 —
"봤다"의 근거는 `anomaly-detector-group`의 committed offset이 `M`을 지났는가다.

```bash
docker compose logs --no-log-prefix anomaly-detector-1 anomaly-detector-2 anomaly-detector-3 | grep -F "vehicle=$V" | grep -F "ts=$T"
docker exec telemetry-kafka kafka-consumer-groups --bootstrap-server kafka:29092 --describe --group anomaly-detector-group
```

| 보이는 줄 | 뜻 |
| --- | --- |
| `[이상 감지] vehicle=… ts=… event=<64hex> type=…` | 알림 발행. `event`가 5단계의 키다 |
| `[계약 위반] … partition=N offset=M payloadSha256=…` | 감지 경로에서 거부 → `vehicle-telemetry-anomaly-dlq` |
| 없음 + committed offset > M | 봤고 정상 판정 |

## 5. 알림이 저장·방송됐나 — PostgreSQL

```bash
docker exec telemetry-postgres sh -c "psql -U \"\$POSTGRES_USER\" -d \"\$POSTGRES_DB\" -c \
  \"SELECT event_id, anomaly_type, severity, detected_at FROM anomaly_alerts WHERE vehicle_id='$V' AND vehicle_timestamp='$T';\""
```

백엔드 로그의 `[이상 저장] vehicle=… ts=… event=…`가 같은 `event_id`를 가리킨다. `[이상 중복]`이면 재처리에서 온 같은 알림이다 —
행은 하나뿐이고 방송도 처음 한 번만(ADR-020). WebSocket 방송은 서버가 로그를 남기지 않는다 — 구독 클라이언트(앱)의
`/topic/vehicle/{V}/anomalies` 프레임에서 `eventId`로 확인한다.

## 6. 거부됐다면 — DLQ에서 `payloadSha256`으로

세 DLQ 모두 value에 **원본 바이트**를 그대로 싣는다(MQTT DLQ는 `{reason, mqtt_topic, payload}` envelope 안의 `payload` 문자열 —
해시는 그 문자열에 대해 계산한다). 로그의 `payloadSha256`과 맞춰 본다. MQTT DLQ는 key가 MQTT topic이고 헤더가 없다.

```bash
python dlq-tools/dlq.py inspect --show-samples            # 원인별 분류와 표본
docker exec telemetry-kafka kafka-console-consumer --bootstrap-server kafka:29092 \
  --topic vehicle-telemetry-dlq --from-beginning --timeout-ms 15000 \
  --property print.headers=true --property print.key=true 2>/dev/null | grep -F "$V"
```

헤더 `x-dlq-origin-partition`·`x-dlq-origin-offset`이 2단계의 `(N, M)`과 같아야 같은 건이다. `x-dlq-replay-count`가 0보다 크면
이미 재주입된 적이 있다 — 재주입은 **새 offset**을 받으므로 2단계에서 두 좌표가 나온다(`docs/runbook/dlq-reprocessing.md`).

## 7. 시뮬레이터 정답과 맞추기 (실험 중일 때)

시뮬레이터는 주입한 이상마다 `[GT] vehicle=… ts=… label=… kind=…`를 남긴다 — 같은 두 키다.

```bash
docker compose logs --no-log-prefix simulator | grep -F "vehicle=$V" | grep -F "ts=$T"
```

## 기록 예

실제 스택에서 한 건을 이 절차로 끝까지 따라간 기록: `docs/verification/2026-09-28-trace-one-message.md`.
