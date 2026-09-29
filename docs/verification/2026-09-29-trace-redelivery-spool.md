# 검증 — 재전달·spool 드레인 뒤에도 `(vehicle_id, timestamp)`로 찾아지는가 (2026-09-29)

| 항목 | 값 |
| --- | --- |
| **검증 상태** | **재전달 1회 · spool 드레인 1회 관찰** — 안정성 주장 없음. **DLQ 재주입·WebSocket 방송은 미실행(미검증)** |
| 대상 | ADR-028 1단계의 남은 완료 조건(`docs/event-correlation-design.md` §9 둘째 줄) — 09-28 검증(정상·거부 각 1건)의 후속 |
| 코드 상태 | `6cf6331`(clean, 작업 트리에는 이 evidence 디렉터리만 untracked). 백엔드 이미지 `sha256:57989d0d…`, 감지기 `1f5d3d8b…`, 시뮬레이터 `e2728fe3…` — `00_metadata.txt` |
| 환경 | **데스크탑**(노트북 아님) Docker Desktop 29.7.2 / compose 5.4.0, dev(평문) 프로파일 + simulator(3대, 1초 간격), 무부하. 백엔드만 `LOGGING_LEVEL_COM_TELEMETRY=DEBUG`(저장소 밖 override) |
| 원본 | [`evidence/2026-09-29-trace-redelivery/`](evidence/2026-09-29-trace-redelivery/), [`evidence/2026-09-29-trace-spool/`](evidence/2026-09-29-trace-spool/) |
| 실행 | 서브에이전트가 수행하던 중 **사용 한도로 중단**됐다. 남은 원본 파일로만 이 문서를 썼다 — 파일에 없는 것은 적지 않았다. DLQ 재주입은 payload만 준비된 채(`evidence/2026-09-29-trace-dlq-replay/01_payload.json`) 실행되지 않았고, WebSocket 디렉터리는 비어 있다 |

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

## 3. 미검증

- **DLQ 재주입** — 준비만 됐다(`RPL-01`, `speed=300` payload). `dlq.py replay` 뒤 같은 키 추적과 `x-dlq-replay-count` 승계는 안 봤다.
- **WebSocket 방송** — 안 봤다(09-28과 같은 한계).
- 각 1회, 무부하, 데스크탑. 반복하지 않았다.
