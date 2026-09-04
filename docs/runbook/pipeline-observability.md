# Runbook — 파이프라인 단계별 대조로 무음 유실 잡기

대시보드: Grafana → **Telemetry Pipeline — 단계별 대조** (`monitoring/grafana/dashboards/pipeline-funnel.json`)

## 왜 필요한가

이 프로젝트에서 가장 비싸게 배운 사고가 이것이다. 12시간 soak test에서 InfluxDB 저장이
**시작 26초 만에 멈췄는데**, `telemetry-storage-group`의 Kafka lag은 12시간 내내 낮게
유지됐다(실패해도 offset이 커밋되고 있었다). 지표를 하나씩 보면 전부 정상이었고,
알림은 하나도 뜨지 않았다.

**`lag = 0`은 "잘 처리되고 있다"가 아니라 "offset이 전진했다"일 뿐이다.**
두 문장을 가르는 유일한 방법이 **단계 간 대조**다.

## 단계와 지표

| # | 단계 | 지표 |
| --- | --- | --- |
| 1 | 브로커가 받은 PUBLISH | `telemetry_mqtt_broker_messages_received` (mosquitto `$SYS`) |
| 2 | 백엔드가 받은 MQTT | `telemetry_mqtt_messages_received_total` |
| 3 | Kafka에서 소비 | `kafka_consumer_fetch_manager_records_consumed_total{topic="vehicle-telemetry"}` |
| 4 | InfluxDB 저장 **성공** | `telemetry_influx_points_written_total` |

4번은 성공한 것만 센다. `telemetry_influx_write_batch_size_sum`은 `saveAll` 진입 시점에
기록돼서 **실패해도 올라간다** — 저장 시도이지 성공이 아니라 이 대조에는 쓸 수 없다.

정상 부하에서 네 값이 거의 겹친다(실측: 325.7 / 332.7 / 333.6 / 333.7, 오차 약 2%).

## 갈라졌을 때 어디를 보나

| 갈라지는 지점 | 뜻 | 확인할 것 |
| --- | --- | --- |
| 1 → 2 | 브로커 큐 오버플로, 백엔드가 유입을 못 따라감 | `telemetry_mqtt_broker_messages_dropped` |
| 2 → 3 | Kafka 프로듀서 실패 | `telemetry_spool_pending` (0이 아니면 spool에 보관 중) |
| 3 → 4 | 저장 실패 | `telemetry_kafka_dlq_published_total`, `docs/runbook/dlq-reprocessing.md` |

**셋 다 0인데 단계가 갈라졌다면 계측이 부족한 것이다** — 어디로 샜는지 모른다는 뜻이므로,
그 구간에 지표를 먼저 추가해야 한다.

## 알림

| 알림 | 조건 | 탐지 시간 |
| --- | --- | --- |
| `TelemetryStorageStalled` (critical) | 유입은 있는데 저장 성공이 0 | 약 4분 |
| `TelemetryStorageRatioLow` (warning) | (저장 + DLQ) / 수신 < 95% | 10분 |
| `TelemetrySpoolNotDraining` (warning) | spool이 10분간 안 줄어듦 | 10분 |

`TelemetryStorageStalled`는 위 12시간 soak 사고를 재현해 **실제로 발동하는 것을 확인했다.**
그때 `kafka_consumer_records_lag_max`는 값 자체가 없어서, 기존 `KafkaConsumerLagHigh`로는
**원천적으로 잡을 수 없는** 상황이었다.

쓰기 실패 카운터로도 못 잡는다 — 조용히 커밋하던 그 버그에서는 실패 카운터조차 안 올랐다.
그래서 "성공한 저장이 0인가"를 직접 본다.

## 주의

- **`$SYS/broker/messages/received`가 아니라 `publish/messages/received`를 봐야 한다.**
  전자는 PUBLISH뿐 아니라 PUBACK·PINGREQ·SUBSCRIBE 등 모든 MQTT 패킷을 세서,
  PUBLISH만 센 백엔드 수신량과 비교하면 정상 부하에서도 정확히 2배가 나온다
  (실측 139,009 vs 69,242). 이 대시보드를 만들다 `MqttIngestFallingBehind` 알림이
  **정상 상태에서 계속 울리고 있던 것**을 발견하고 고쳤다.
- 단계 1은 브로커 전체 수치라 다른 클라이언트가 붙으면 함께 오른다.
  차량 트래픽만 보려면 단계 2부터 대조하는 게 정확하다.
- `telemetry_influx_points_written_total`은 **포인트 수**이고 단계 2·3은 **메시지 수**다.
  메시지 1건 = 포인트 1건인 현재 구조에서만 직접 비교가 성립한다.
