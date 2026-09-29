# 검증 — 재전달·spool 드레인·DLQ 재주입·WebSocket 뒤에도 `(vehicle_id, timestamp)`로 찾아지는가 (2026-09-29)

| 항목 | 값 |
| --- | --- |
| **검증 상태** | **재전달·spool 드레인·DLQ 재주입(거부 1건 + 합성 성공 1건)·WebSocket 방송 각 1회 관찰** — 안정성 주장 없음. 발견한 추적 구멍은 §3·§4 |
| 대상 | ADR-028 1단계의 남은 완료 조건(`docs/event-correlation-design.md` §9 둘째 줄) — 09-28 검증(정상·거부 각 1건)의 후속. §1·§2는 `6cf6331`, §3·§4는 HEAD `6f1c0a0`에서 실행 |
| 코드 상태 | `6cf6331`(clean, 작업 트리에는 이 evidence 디렉터리만 untracked). 백엔드 이미지 `sha256:57989d0d…`, 감지기 `1f5d3d8b…`, 시뮬레이터 `e2728fe3…` — `00_metadata.txt` |
| 환경 | **데스크탑**(노트북 아님) Docker Desktop 29.7.2 / compose 5.4.0, dev(평문) 프로파일 + simulator(3대, 1초 간격), 무부하. 백엔드만 `LOGGING_LEVEL_COM_TELEMETRY=DEBUG`(저장소 밖 override) |
| 원본 | [`evidence/2026-09-29-trace-redelivery/`](evidence/2026-09-29-trace-redelivery/), [`evidence/2026-09-29-trace-spool/`](evidence/2026-09-29-trace-spool/), [`evidence/2026-09-29-trace-dlq-replay/`](evidence/2026-09-29-trace-dlq-replay/), [`evidence/2026-09-29-trace-websocket/`](evidence/2026-09-29-trace-websocket/) |
| 실행 | §1·§2는 서브에이전트가 수행하다 **사용 한도로 중단**돼 남은 원본으로만 썼다. §3·§4는 후속 서브에이전트가 실행했다. **백엔드 이미지(`sha256:57989d0d…`)는 `6cf6331` 기준 빌드라 HEAD의 WebSocket 방송 try/catch(`6f1c0a0`)가 들어 있지 않다** — §4는 그 이전 코드의 방송 경로를 본 것이다(정상 방송에는 차이 없음) |

## 1. 재전달 — SIM-001, `ts=2026-09-28T22:22:28.084Z`

저장 consumer(백엔드 컨테이너)를 kill한 뒤 재시작해, **이미 저장된 한 건이 다시 전달돼도** 같은 키로 찾아지고 InfluxDB에 두 번 쌓이지 않는지 봤다.

| # | 지점 | 조회 | 결과 | 원본 |
| --- | --- | --- | --- | --- |
| 1 | kill 전 그룹 offset | `kafka-consumer-groups --describe` | 파티션 0/1/2 = 14 / 9551 / 4787, lag 0 | `01_group_before_kill.txt` |
| 2 | 대상 건 | DEBUG 로그 | `[MQTT→Kafka] vehicle=SIM-001 ts=…28.084Z` → `[Kafka] 전송 완료 … partition=2 offset=4768` | `06_backend_log_prekill.txt` |
| 3 | Kafka 레코드 | `--partition 2 --offset 4768 --max-messages 1` | key `SIM-001`, `"timestamp":"…22:22:28.084Z"`, speed 62.4 | `05_target_kafka_record.txt` |
| 4 | kill | `kill_at_utc=22:22:47.486Z` | kill 직후 그룹 offset 14 / 9553 / 4788, lag 0 — **kill 시점에 미커밋 건이 없었다** | `02_kill_time.txt`, `03_group_after_kill.txt`, `04_end_offsets_after_kill.txt` |
| 5 | 재전달 유도 | `kafka-consumer-groups --reset-offsets --to-offset 4768`(파티션 2) | NEW-OFFSET 4768 | `09_offset_reset.txt` |
| 6 | InfluxDB (재시작 전) | `range(T, T+1ms)`, `vehicle_id==SIM-001` | **1행**, `_time=…28.084Z`, engine_temp 91.9, battery 13.86 | `07_influx_before_restart.txt` |
| 7 | InfluxDB 창 건수 (전) | `22:22:28~22:22:48`, `speed` count | **20** | `08_influx_window_count_before.txt` |
| 8 | 재시작 | `restart_at_utc=22:23:43.716Z` | 파티션 2 current 4875(재전달 구간을 지나감), 새 consumer id | `10_group_after_restart.txt` |
| 9 | 재전달 배치 | DEBUG 로그 | `[Kafka→InfluxDB] 배치 저장 완료 — 수신 20건 중 20건 저장` 뒤 1·1·3·1·15·16·37건 | `13_backend_log_after_restart_batch.txt` |
| 10 | InfluxDB (재시작 후) | 6과 같은 조회 | **1행**, 값 동일 | `11_influx_after_restart.txt` |
| 11 | InfluxDB 창 건수 (후) | 7과 같은 조회 | **20** — 재전달 20건이 새 행을 만들지 않았다 | `12_influx_window_count_after.txt` |

**결과**: 재전달 뒤에도 같은 `(vehicle_id, timestamp)`로 로그(2)·Kafka(3)·InfluxDB(10)에서 찾아졌고, InfluxDB는 `vehicle_id`+밀리초가 point identity라 **같은 키 재저장이 덮어쓰기로 흡수**됐다(창 건수 20 → 20).

**한계**: kill 시점에 lag이 0이라(4) 자연 재전달이 아니라 **offset을 되감아 만든 재전달**이다(5). 배치 저장 로그(9)에는 `vehicle=`·`ts=`가 없어 "그 20건 중 대상 건이 있었다"는 offset 범위(4768 ≤ … < 4788)로만 알 수 있다 — 배치 로그에 키가 없는 것은 §9 기준으로 **추적 구멍**이며 코드 변경 없이 기록만 남긴다.

## 2. spool 드레인 — SIM-001, `ts=2026-09-28T22:27:11.212Z`

| 시도 | Kafka 정지 | 결과 | 원본 |
| --- | --- | --- | --- |
| 1 | 약 25초(22:25:36~22:26:01Z) | **spool에 들어가지 않았다** — `telemetry_spool_pending` 0, 드레인 0. 정지 구간에 MQTT로 받은 78건은 producer 버퍼에 있다가 복구 뒤 `[Kafka] 전송 완료`로 나갔다(22:26:26에 ts 22:25:40~44 표본) | `01_attempt1_20s_timeline.txt`, `02_attempt1_result.txt` |
| 2 | 약 150초(22:27:10~22:29:40Z) — `delivery.timeout.ms` 기본 120초를 넘긴다 | spool 보관 로그 195줄, 드레인 로그 2880줄(반복 시도 포함), 드레인 완료 22:43:51Z | `03_attempt2_145s_timeline.txt`, `04_spool_store_log_head.txt` |

시도 2의 대상 한 건:

| # | 지점 | 결과 | 원본 |
| --- | --- | --- | --- |
| 1 | MQTT 수신 | `22:27:11 [MQTT→Kafka] vehicle=SIM-001 ts=…27:11.212Z speed=108.1` | `05_target_backend_log.txt` |
| 2 | spool 보관 | `22:29:11 [Kafka] 브로커 전송 실패 — spool에 보관 vehicle=SIM-001 ts=…27:11.212Z` — 수신 2분 뒤(delivery timeout) | 〃 |
| 3 | 드레인 | `22:30:11 [Kafka] spool 드레인 완료 — vehicle=SIM-001 ts=…27:11.212Z partition=2 offset=5106` | 〃 |
| 4 | Kafka 레코드 | `--partition 2 --offset 5106`: key `SIM-001`, `"timestamp":"…27:11.212Z"`, speed 108.1 | `06_kafka_record_after_drain.txt` |
| 5 | InfluxDB | `range(T, T+1ms)`: **1행**, `_time=…27:11.212Z`, speed 108.1, battery 13.78 | `07_influx_after_drain.txt` |

**결과**: spool을 거친 건도 세 로그 줄(수신·보관·드레인)이 같은 `vehicle=`·`ts=`를 갖고, 드레인 로그가 Kafka 좌표를 주므로 runbook 절차가 그대로 닫힌다.

**발견**: "Kafka 20초 정지"로는 spool이 켜지지 않는다 — producer가 `delivery.timeout.ms`(120초) 동안 버퍼에 들고 있다가 브로커가 돌아오면 그대로 보낸다. spool은 그 시간을 넘긴 뒤에야 쓰인다. §9의 시나리오 설명과 runbook의 spool 절은 이 조건을 적어야 한다.

## 3. DLQ 재주입 — `RPL-01`, 저장 DLQ(`vehicle-telemetry-dlq`)

**어느 DLQ로 가는지부터 판단했다.** `speed=300`은 계약 위반(255 초과)이다. MQTT로 넣으면 **MQTT 거부 DLQ**(`vehicle-telemetry-mqtt-dlq`)로 가는데, 그 DLQ는 envelope라 `dlq.py replay`로 되돌릴 수 없다(`dlq-reprocessing.md` §2-1). 그래서 같은 payload를 **Kafka `vehicle-telemetry`에 직접**(key `RPL-01`) 넣어 저장 DLQ로 보냈다. 이 경로는 MQTT 수신 로그(`[MQTT→Kafka]`)가 없다.

| # | 지점 | 조회 | 결과 | 원본 |
| --- | --- | --- | --- | --- |
| 1 | 원본 발행 | `kafka-console-producer`(key `RPL-01`) | `vehicle-telemetry` partition 2 offset **16498** | `07_kafka_original_record.txt` |
| 2 | 백엔드 로그(거부) | `grep RPL-01` | `WARN 계약 위반 PAYLOAD_VALIDATION_FAILED — DLQ로 이동 vehicle=RPL-01 partition=2 offset=16498 payloadSha256=e2453abd…` — **`ts=`가 없다**. `sha256sum`으로 계산한 payload 해시와 일치 | `05_backend_log_rejected.txt` |
| 3 | 저장 DLQ 레코드 | `--partition 0 --offset 16` | key `RPL-01`, `x-dlq-origin-partition:2`, `x-dlq-origin-offset:16498`, `x-dlq-failure-class:…TelemetryContractException`, 사유 `speed must be less than or equal to 255`, **`x-dlq-replay-count` 없음** | `06_dlq_record_before_replay.txt` |
| 4 | InfluxDB (재주입 전) | `RPL-01`, 하루 범위 | **0행** | `08_influx_before_replay.txt` |
| 5 | 재주입 | `dlq.py replay --target vehicle-telemetry --include-permanent --execute` (분류 `permanent`라 플래그 필요) | 1건 발행 성공, 커서 전진 | `10_replay_dryrun.txt`, `12_replay_execute.txt` |
| 6 | 재주입된 Kafka 레코드 | partition 2 offset **16499** | **새 offset**, key·value 동일, 원본 `x-dlq-*` 헤더 유지 + **`x-dlq-replay-count:1`** | `16_kafka_replayed_record.txt` |
| 7 | 재실패 로그 | `grep RPL-01` | 같은 WARN, `offset=16499`, **`payloadSha256`이 #2와 동일** | `15_backend_log_after_replay.txt` |
| 8 | 저장 DLQ 두 번째 레코드 | offset 17 | `x-dlq-origin-offset:16499`(새 좌표), 사유 동일, **`x-dlq-replay-count:1` 승계** | `17_dlq_record_after_replay.txt` |
| 9 | InfluxDB (재주입 후) | 같은 조회 | **0행** | `18_influx_after_replay.txt` |

조회 도구는 같은 스크립트로 시뮬레이터 차량 한 점이 나오는 것을 확인했다(`19_influx_query_sanity_SIM-002.txt`). 처음 확인 대상으로 고른 `SIM-001`은 아래 §4의 이유로 그 시각에 발행 중이 아니라 빈 결과였고, 그 빈 파일은 지웠다.

**결과(거부 건)**: 재주입 전후 모두 **`payloadSha256`이 같아** 같은 원본으로 이어졌고, 좌표는 재주입 때 `(2,16498)` → `(2,16499)`로 **새 offset**이 붙었다(runbook 6절 기술과 일치). `x-dlq-replay-count`는 0(없음) → 1로 이어졌다. `(vehicle_id, timestamp)`로는 Kafka 값에서 두 레코드를 찾을 수 있지만 **로그에는 `ts=`가 없어** 로그 조회는 `vehicle=`+`payloadSha256`으로 해야 한다.

**추가 관찰 — 재주입이 성공하는 경로(합성)**: 위는 계약 위반이라 재주입이 다시 거부돼 "InfluxDB에 들어간 뒤"는 못 봤다. 그래서 `RPL-02`(정상 payload, `ts=2026-09-29T09:01:00.000Z`)를 **`x-dlq-failure-class:…InfluxException` 헤더를 손으로 붙여** 저장 DLQ에 직접 넣었다(`21_synthetic_dlq_record.txt`, `x-dlq-failure-message`에 `SYNTHETIC`). 실제 InfluxDB 장애로 만든 DLQ 레코드가 **아니다** — 분류기가 `transient`로 읽도록 만든 합성이다.

| # | 지점 | 결과 | 원본 |
| --- | --- | --- | --- |
| 1 | `dlq.py replay`(`--include-permanent` 없이) | 2건 읽음(offset 17 = 위 RPL-01 재실패분, 18 = RPL-02) → **1건 재처리, 1건 건너뜀(영구 실패)** | `23_synthetic_replay_execute.txt` |
| 2 | Kafka | partition 1 offset **35362**, key `RPL-02`, `x-dlq-replay-count:1` | `28_kafka_replayed_synthetic_record.txt` |
| 3 | InfluxDB | `range(09:01:00.000, 09:01:00.001)` **1행**, engine_temp 90, battery 12.5 | `26_synthetic_influx.txt` |
| 4 | 백엔드 로그 | `grep RPL-02` → **0줄** — 저장 성공 경로는 배치 요약(`수신 N건 중 N건 저장`)만 남기고 키가 없다(§1 한계와 같은 구멍) | `24_synthetic_backend_log.txt` |

**결과(합성 성공 건)**: 재주입된 레코드가 같은 `(vehicle_id, timestamp)`로 Kafka(#2)와 InfluxDB(#3)에서 찾아졌고 `x-dlq-replay-count`가 붙었다. 로그(#4)로는 찾을 수 없다.

**한계**: 각 1회. RPL-02의 DLQ 진입은 합성이라 "실제 InfluxDB 장애 → DLQ → 재주입 → 저장" 전체를 본 것이 아니다. `x-dlq-replay-count` 증가·승계만 봤고 `--max-replays` 차단은 이 실행에서 다시 보지 않았다(`load-test/dlq-replay-count/`가 별도로 다뤘다). 검증용 DLQ 레코드(RPL-01 2건, RPL-02 1건)와 커서 그룹 `dlq-replay-trace-20260929`는 **Kafka에 남아 있다** — 삭제할 수 없고 삭제하지 않았다. 기존 DLQ 16건이 함께 재주입되지 않도록 새 그룹의 커서를 offset 16으로 미리 맞췄다(`09_cursor_setup.txt`).

## 4. WebSocket 방송 — `SIM-001`, `ts=2026-09-29T09:00:02.123Z`

| # | 지점 | 조회 | 결과 | 원본 |
| --- | --- | --- | --- | --- |
| 1 | 로그인 | `POST /api/auth/login`(`.env`의 관리자 계정, 값 미기록) | HTTP 200, 토큰은 저장하지 않음 | `01_probe_log.txt` |
| 2 | STOMP CONNECT | `Authorization: Bearer …`, `ws://localhost:8080/ws` | `CONNECTED version:1.2 user-name:admin` | `02_connected_frame.txt` |
| 3 | 구독 | `/topic/vehicle/SIM-001/telemetry` | 성공 | `01_probe_log.txt` |
| 4 | 발행(정상 1건) | `mosquitto_pub -q 1`, 토픽 `vehicle/telemetry/SIM-001`, speed 61.5 | 09:00:02Z | `05_published_payload.json` |
| 5 | **수신 프레임** | 첫 `MESSAGE` 프레임 | `vehicleId=SIM-001`, `timestamp=2026-09-29T09:00:02.123Z`, speed 61.5 — **수신 09:00:08.4Z**(발행 6초 뒤) | `03_message_frame.txt`, `04_message_body.json` |
| 6 | 백엔드 로그 | `vehicle=SIM-001` + `ts=` | `09:00:03 [MQTT→Kafka] …` → `09:00:08 [Kafka] spool 드레인 완료 — … partition=2 offset=16500` | `06_backend_log.txt` |
| 7 | Kafka 레코드 | partition 2 offset 16500 | key `SIM-001`, `"timestamp":"…09:00:02.123Z"`, speed 61.5 | `07_kafka_record.txt` |
| 8 | InfluxDB | `range(T, T+1ms)` | **1행**, engine_temp 88.5, battery 13.5, fuel 55 | `08_influx.txt` |

**결과**: 프레임의 `vehicleId`·`timestamp`(JSON 필드명 camelCase)로 로그·Kafka·InfluxDB의 같은 건이 찾아졌다. WebSocket 프레임에는 `(vehicle_id, timestamp)`가 **원본 문자열 그대로** 들어 있어 별도 변환 없이 조인된다. 서버가 방송 로그를 남기지 않는 것은 그대로다(runbook 5절) — 방송의 증거는 클라이언트가 받은 프레임뿐이다.

**첫 시도는 실패했다**(`evidence/2026-09-29-trace-websocket/failed_attempt1_SIM-002/`): 시뮬레이터 차량 `SIM-002`를 구독하니 STOMP `ERROR` 프레임이 왔다. `VehicleAccessService`는 **관리자도 등록된 활성 차량만** 통과시키는데(코드 확인) `vehicles` 테이블에는 `SIM-001` 한 행뿐이다(`SELECT`로 확인). 그래서 등록된 `SIM-001`을 구독했다. 이 결과는 방송 결함이 아니라 **미등록 차량은 구독 불가**라는 설계 동작이다.

**한계와 발견**:
- 각 1회, 무부하, 데스크탑. 프로브는 `evidence/…/ws_probe.py`(stdlib `urllib` + `websockets`, 관리자 자격증명은 `.env`에서 읽고 출력하지 않음)이며 **프레임 1개만** 받았다 — 재연결·stale·순서 뒤바뀜은 안 봤다.
- 이 실행에서 `SIM-001` 시뮬레이터 스레드는 돌고 있지 않았다(스택 기동 직후 브로커 미준비로 `MQTT 브로커에 연결할 수 없습니다` 후 종료 — 시뮬레이터 로그 08:34:13). 그래서 정상 1건을 **직접 발행**했다. 다른 프레임이 섞일 수 없는 차량이라 "첫 프레임 = 내 건"이다.
- **관찰(결함 후보, 미조사)**: 이 스택에서 백엔드는 실행 내내 `telemetry_spool_pending=10`이고 정상 메시지가 전부 `spool 드레인 완료`로 나갔다(드레인 로그 20분간 약 2,400줄). `TelemetryProducer`의 `backlog` 플래그는 5초 주기 스캔이 spool 비어 있음을 봐야 꺼지는데, 이미 켜져 있으면 새 메시지가 다시 spool로 가서 스캔이 계속 비어 있지 않을 수 있다(코드 읽기 기준 추정 — 재현·확정 안 함). 결과적으로 **정상 경로 지연이 5초 주기에 묶여**(위 5번의 6초) 발행→수신이 약 6초 걸렸다. 켜진 계기(기동 직후 spool 잔여 여부)와 자연 해제 여부는 확인하지 않았다. 코드는 변경하지 않았다.
- 정상 방송 경로만 본 것이다. HEAD `6f1c0a0`의 방송 실패 try/catch(`telemetry.websocket.broadcast.failures`)는 이 이미지에 없어 **관찰하지 않았고**, 실제 STOMP 브로커 예외 재현도 안 했다.

## 5. 미검증

- 감지 DLQ(`vehicle-telemetry-anomaly-dlq`) 재주입 후 추적, 알림 방송(`/topic/vehicle/{id}/anomalies`)의 `eventId` 조인.
- 실제 InfluxDB 장애로 생긴 DLQ 레코드의 재주입(RPL-02는 합성).
- 앱 실기기의 프레임 수신·재연결.
- 위 모든 항목은 각 1회, 무부하, 데스크탑이며 반복하지 않았다.
