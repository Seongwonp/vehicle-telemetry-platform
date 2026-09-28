# 검증 — 텔레메트리 한 건을 `(vehicle_id, timestamp)`로 끝까지 찾기 (2026-09-28)

| 항목 | 값 |
| --- | --- |
| **검증 상태** | **정상 경로 1건 · 거부 경로 1건, 각 1회 관찰** — 안정성 주장 없음. 재전달·spool 드레인·DLQ 재주입 뒤의 추적은 **미검증** |
| 대상 | ADR-028 1단계 — 새 ID 없이 로그 키 `vehicle`·`ts`·`payloadSha256`만으로 [runbook](../runbook/trace-one-message.md) 절차가 실제 스택에서 닫히는가 |
| 코드 상태 | `88cf7f1` + 1단계 변경(이 문서와 같이 커밋). 백엔드 이미지 `0d6030172418`, 감지기 이미지도 같은 빌드 |
| 환경 | 이 노트북 Docker Desktop, dev(평문) 프로파일, 시뮬레이터 SIM-001~003, 무부하. **추적을 위해 백엔드만 `LOGGING_LEVEL_COM_TELEMETRY=DEBUG`**(compose 밖 override, 끝난 뒤 되돌림) |
| 원본 | [`evidence/2026-09-28-trace/`](evidence/2026-09-28-trace/) — 지점마다 파일 하나 |

## 1. 정상 경로 — SIM-003, `ts=2026-09-28T10:07:04.309Z` (시뮬레이터가 주입한 저전압)

감지기 로그의 최신 `[이상 감지]` 한 줄을 골라 그 키로 **역방향·순방향 전부**를 조회했다. 키를 고른 뒤에는 어떤 값도 손으로 넣지 않았다.

| # | 지점 | 조회 | 결과 | 원본 |
| --- | --- | --- | --- | --- |
| 1 | 백엔드 MQTT 수신 | `logs backend \| grep vehicle= \| grep ts=` | `[MQTT→Kafka] vehicle=SIM-003 ts=… speed=108.7 engine_temp=92.2 battery_voltage=10.41` | `01_backend_log.txt` |
| 2 | Kafka 발행 | 같은 grep | `[Kafka] 전송 완료 — vehicle=SIM-003 ts=… partition=1 offset=69705` | 〃 |
| 3 | Kafka 레코드 | `--partition 1 --offset 69705 --max-messages 1` | key `SIM-003`, value의 `"timestamp":"…04.309Z"`, `battery_voltage` 10.41 | `02_kafka.txt` |
| 4 | InfluxDB | `range(start: T, stop: T+1ms)`, `vehicle_id==SIM-003`, pivot | **1행**, `_time=…04.309000000Z`, engine_temp 92.2, battery_voltage 10.41 | `03_influx.txt` |
| 5 | 감지기 | `logs anomaly-detector \| grep` | `[이상 감지] vehicle=SIM-003 ts=… event=3ee127a2… type=배터리 저전압 severity=MEDIUM` | `04_detector_log.txt` |
| 6 | 감지 그룹 offset | `kafka-consumer-groups --describe` | 파티션 1 current 70156 > 69705 (봤다) | `04b_detector_group_offsets.txt` |
| 7 | 알림 저장(백엔드) | 같은 grep | `[이상 저장] vehicle=SIM-003 ts=… event=3ee127a2…` | `01_backend_log.txt` |
| 8 | PostgreSQL | `WHERE vehicle_id AND vehicle_timestamp` | **1행**, `event_id=3ee127a2…`(5·7과 같은 값), `vehicle_timestamp=10:07:04.309+00` | `05_postgres.txt` |
| 9 | 시뮬레이터 정답 | `logs simulator \| grep` | `[GT] vehicle=SIM-003 ts=… label=low_battery kind=rule` | `07_simulator_log.txt` |

**9개 지점이 같은 두 값으로 이어졌고, 알림 키 `event`는 감지기 로그·백엔드 로그·DB 행에서 같았다.** WebSocket 방송은 서버 로그가 없어
이 실행에서는 보지 않았다(앱 구독이 필요하다 — 한계).

## 2. 거부 경로 — `TRACE-01`, `speed=300` (계약 범위 0~255 밖)

`mosquitto_pub`로 한 건을 넣었다(`06_reject_payload.json`). 발행 전에 로컬에서 SHA-256을 계산해 뒀다.

| # | 지점 | 결과 | 원본 |
| --- | --- | --- | --- |
| 1 | 로컬 `sha256sum` | `ae82ae10…4f77` | `06_reject_local_sha256.txt` |
| 2 | 백엔드 거부 로그 | `[MQTT] 메시지 거부 — topic=vehicle/telemetry/TRACE-01 reason=PAYLOAD_VALIDATION_FAILED vehicle=- ts=- payloadLength=212 payloadSha256=ae82ae10…4f77` | `06_reject_backend_log.txt` |
| 3 | `vehicle-telemetry-mqtt-dlq` | key=`vehicle/telemetry/TRACE-01`, 헤더 없음, value=`{reason, mqtt_topic, payload}` → **envelope의 `payload` 문자열 SHA-256 = `ae82ae10…4f77`** | `06_reject_mqtt_dlq.txt` |

셋이 같았다. 계약 위반이라 `vehicle=- ts=-`다 — 설계대로 **거부된 메시지의 필드는 키로 쓰지 않는다**(§10-3). 찾는 키는 topic의 차량 ID와 해시다.

## 3. 절차에서 고친 것 (이 실행이 runbook을 바꿨다)

- **`--from-beginning` 전체 스캔은 못 쓴다.** 약 107k 메시지를 `docker exec` 파이프로 읽는 데 **13분이 넘어 중단**했다. 1단계 DEBUG 줄의
  `partition=`·`offset=`으로 **한 건만 읽는** 명령을 runbook의 기본으로 올렸다. DEBUG가 꺼진 운영에서는 이 좌표가 없다 — 그때는 파티션을
  좁혀 스캔해야 하고, 그 비용이 2단계(`message_id`)의 근거 중 하나가 된다.
- 시뮬레이터 정답 접두사는 `[GROUND_TRUTH]`가 아니라 `[GT]`였다.
- MQTT DLQ의 해시 대상은 value 전체가 아니라 envelope 안 `payload` 문자열이다.

## 4. 한계

- 각 1회, 무부하. **재전달·spool 드레인·DLQ 재주입 뒤에도 같은 키로 찾아지는지는 안 봤다**(§9의 나머지 완료 조건) — 코드상 원본 value를
  그대로 나르므로 유지되지만, 실측이 아니다.
- 정상 경로 1·2·7은 DEBUG에서만 보인다. INFO 운영에서 "수신됐다"의 근거는 Kafka 레코드(3)다.
- 같은 차량·같은 밀리초 두 건은 이 키로 구분할 수 없다(ADR-028 한계).
