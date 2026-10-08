# 실험 잔여 데이터 목록 — 2026-10-08

상태: **목록만 작성.** 아무것도 지우거나 바꾸지 않았다(사용자 결정 ④). 삭제 여부는 사용자가 이 목록을 보고 정한다.
이 문서는 **삭제를 권하지 않는다.** "삭제 영향" 열은 사실만 적는다.

| 항목 | 값 |
| --- | --- |
| 수집 시각 | 호스트 시계 2026-10-06 23:37~23:42 KST (14:37~14:42 UTC). 문서 파일명(10-07·10-08)과 호스트 시계가 하루 이상 어긋나 있다 — 아래 시각은 전부 **데이터 자체의 UTC 시각**이다 |
| 스택 | dev compose(평문 1883). 수집 중 다른 에이전트가 같은 스택에서 MQTT/Kafka 실험을 돌리고 있었다 — 값은 그 시점의 스냅샷이다 |
| 방법 | 읽기 전용 조회만. 컨테이너 재시작·새 consumer group·새 MQTT 연결 없음. 비밀값은 컨테이너 내부 환경변수로만 썼고 출력하지 않았다 |
| 원본 | [`docs/verification/evidence/2026-10-08-experiment-data-inventory/`](verification/evidence/2026-10-08-experiment-data-inventory/) (01~06, 각 파일 첫 줄에 명령) |
| 삭제 후보 dry-run | [§6](#6-삭제-후보-표--dry-run-삭제-실행-없음) — 같은 디렉터리 `dryrun_01`~`dryrun_07`. 삭제 명령은 **적지만 실행하지 않았고**, 같은 조건의 COUNT/SELECT/describe만 돌렸다 |

"삭제 영향" 값의 뜻:

- **none** — 이 데이터를 근거로 쓰는 주장이 없다(또는 이미 비어 있다).
- **evidence already in files** — 그 실험의 주장은 저장소의 evidence 파일(발행 payload, 전후 카운트, 로그)로 닫혀 있다. 라이브 데이터는 재계산에 쓰이지 않는다.
- **needed for re-verification** — 라이브 데이터를 다시 조회해야만 확인할 수 있는 주장이 있다.
- **unknown** — 판단 근거가 부족하다.

## 요약

| 저장소 | 실험 잔여물 | 상태 |
| --- | --- | --- |
| Mosquitto 영속 세션 | `telemetry-backend-g1`·`-g1-sys`·`-g2`·`-g2-sys`·`-h2`·`-h2-sys` (6개) | **g1·g2·g1-sys·g2-sys 4개는 사용자 승인으로 삭제했다(host 2026-10-07 13:03 UTC, §6-5).** 남은 것은 `-h2`·`-h2-sys`(M5·M6, 요청 범위 밖) |
| InfluxDB `telemetry` 버킷 | 실험 차량 28개 tag (총 8,222행) + 시뮬레이터 SIM-001~003 (63,581행) | 남아 있음. 버킷 보존 2160h(90일) |
| Kafka DLQ·운영 토픽 | 6개 토픽 **전부 비어 있음**(earliest = latest) | retention 1시간으로 만료. 09-29 문서의 "RPL DLQ 레코드가 남아 있다"는 **더 이상 사실이 아니다** |
| Kafka 테스트 토픽 | `itc-220850-*`·`itc-t1-*` (6개) | 토픽은 남음, 레코드는 만료(비어 있음) |
| Consumer group | `dlq-replay-trace-20260929` (Empty) | 남아 있음. 기본 `offsets.retention.minutes=10080`(7일)에 걸리는 시점이라 곧 자동 만료될 수 있다(시각 미확인). **→ dry-run 시점(15:57 UTC)에는 이미 없다(자동 만료, §6)** |
| PostgreSQL | 사용자 `e2e-pw-0929183104`, `qa-admin`(비활성), `qa-user`(비활성), 차량 SIM-002(소유자 qa-user), SIM-001 긴 이름, `SCHEMA145736-*` 알림 3행 | 남아 있음 |

## 1. Mosquitto 영속 세션

원본: [`01_mosquitto_db_summary.txt`](verification/evidence/2026-10-08-experiment-data-inventory/01_mosquitto_db_summary.txt), [`02_mosquitto_log_connections.txt`](verification/evidence/2026-10-08-experiment-data-inventory/02_mosquitto_log_connections.txt).

**어떻게 봤나**: `/mosquitto/data/mosquitto.db`(4,258,086 bytes, mtime **2026-10-06 05:16:45 UTC**)에 `strings`만 실행했다(파일 수정 없음).
**한계**: 이것은 **디스크 스냅샷**이지 브로커 메모리의 현재 상태가 아니다. 브로커는 14:32 UTC에 재기동됐는데 파일 mtime은 05:16이다(이유 미확인). DB 형식을 파싱하지 않았고 문자열 출현 횟수로 추정했다.
라이브 세션 목록은 **보지 못했다** — `mosquitto_ctrl`은 dynamic-security 플러그인이 필요한데 현재 설정(`mosquitto-dev.conf`)에는 없고, `$SYS` 구독은 새 MQTT 연결을 만들어 진행 중 실험의 브로커 지표(`clients/connected`)를 건드릴 수 있어 하지 않았다.

| client ID | DB에서 본 구독 | 출처 실험 | 시각(UTC) | 문서 | evidence | 삭제 영향 |
| --- | --- | --- | --- | --- | --- | --- |
| `telemetry-backend-g1` | `vehicle/telemetry/#` + 메시지 참조 **9,899회** | 실험 G(세션 없는 재시작) | 2026-10-05 13:07 연결 | [ack-boundary §실험 G](verification/2026-10-01-mqtt-ack-boundary.md), [HANDOFF_2026-10-05](HANDOFF_2026-10-05.md) | [`2026-10-05-mqtt-session-loss/`](verification/evidence/2026-10-05-mqtt-session-loss/) (`override_g1.yml`, `G1_start.txt`) | evidence already in files |
| `telemetry-backend-g1-sys` | `$SYS/broker/{publish/messages/received,dropped,clients/connected}` | 실험 G (백엔드가 client ID + `-sys`로 자동 생성) | 같은 시기 | **문서에 없음** — HANDOFF는 g1·g2만 적었다 | 같음 | evidence already in files |
| `telemetry-backend-g2` | `vehicle/telemetry/#` + 메시지 참조 **8,999회** | 실험 G | 2026-10-05 13:08:44 연결 | 같음 | 같음 (`G2_*`, `override_g2.yml`) | evidence already in files |
| `telemetry-backend-g2-sys` | `$SYS` 3종 | 실험 G | 같은 시기 | **문서에 없음** | 같음 | evidence already in files |
| `telemetry-backend-h2` | **구독 기록 없음** (SUBACK 0x80 거부 실험이라 정상) | 실험 H-2·H-2a | 2026-10-05 22:11 경 | [ack-boundary §실험 H](verification/2026-10-01-mqtt-ack-boundary.md), [HANDOFF_2026-10-06](HANDOFF_2026-10-06.md) | [`2026-10-06-timeout-alert/`](verification/evidence/2026-10-06-timeout-alert/) (`H2_*`, `H2a_*`) | evidence already in files |
| `telemetry-backend-h2-sys` | `$SYS` 3종 | 실험 H-2 | 같은 시기 | 같음 | 같음 (`H2_mosquitto_log.txt`) | evidence already in files |
| `telemetry-backend`, `telemetry-backend-sys` | `vehicle/telemetry/#`, `$SYS` 4종(uptime 포함) | **운영 기본 ID** — 실험 잔여물 아님 | — | `application.yml` | — | (대상 아님) |

**g1·g2에 관해 관찰한 것(추정 포함)**:

- DB에 남은 텔레메트리 메시지 본문은 약 10,617건이다(**정정(§6 dry-run 때 확인): 원본 §B의 합은 9,897건이다 — 10,617은 잘못 더한 값이다.** 9,897은 g1 참조 9,899와 거의 같다. 차량별 내역은 원본 §B — OUTAGE-E3·S·R·P·T·TR·U, RECON-*, SESSLOSS-G2, SIM-001~003, payload 시각 2026-10-05 13:08 ~ 2026-10-06 05:16 UTC).
  `telemetry-backend-g1` 9,899회와 `-g2` 8,999회의 차이가 정확히 900 = `SESSLOSS-G2` 발행 수(g2가 접속해 있던 동안 g1만 쌓았다)라서, **이 수치는 두 세션의 오프라인 큐 길이로 읽힌다.** DB를 파싱해 확인한 것은 아니다.
- 두 세션은 QoS 1 `vehicle/telemetry/#` 구독을 가진 채 오프라인이므로, Mosquitto 의미상 **새 텔레메트리가 들어올 때마다 큐가 늘어난다**(클라이언트당 상한 `max_queued_messages 100000`). 현재 큐 길이는 보지 않았다.
- 이 메시지들은 `telemetry-backend` 세션으로 이미 저장 경로를 탄 것과 같은 메시지다. 누군가 `MQTT_CLIENT_ID=telemetry-backend-g1`로 백엔드를 띄우면 쌓인 메시지가 재전달된다(실험 G의 "원래 ID 복귀 시 백로그 재전달"과 같은 기전).

## 2. InfluxDB 실험 차량 행

원본: [`03_influx_vehicle_rows.txt`](verification/evidence/2026-10-08-experiment-data-inventory/03_influx_vehicle_rows.txt). 버킷 `telemetry`(보존 2160h), measurement `vehicle_telemetry` 하나. 행 수 = `speed` 필드 포인트 수. vehicle_id tag 값 31개.
문서에 "0행"으로 적힌 `OUTAGE-H`·`OUTAGE-H2`는 실제로 tag가 없다(일치).

| vehicle_id | 행 | 시간 범위(UTC) | 실험 | 문서 | evidence | 삭제 영향 |
| --- | ---: | --- | --- | --- | --- | --- |
| `SCHEMA145736-K01`·`K02`·`K03`·`M01`·`M02`·`M03` | 각 1 | 2026-09-09 06:03:54~56 | 스키마 계약 E2E `20260909-145736` | [RESULT_20260909_contract_e2e](../load-test/schema-contract/RESULT_20260909_contract_e2e.md) | [`load-test/schema-contract/evidence/20260909-145736/`](../load-test/schema-contract/evidence/20260909-145736/) (checksums 있음) | evidence already in files |
| `SCHEMA145736-MIX` | 2 | 2026-09-09 06:03:56 | 같음(혼합 배치) | 같음 | 같음 | evidence already in files |
| `RPL-02` | 1 | 2026-09-29 09:01:00.000 | DLQ 재주입 추적(합성 건) | [2026-09-29-trace-redelivery-spool §3](verification/2026-09-29-trace-redelivery-spool.md) | [`2026-09-29-trace-dlq-replay/`](verification/evidence/2026-09-29-trace-dlq-replay/) (`26_synthetic_influx.txt`) | evidence already in files |
| `ACKTEST-A` / `ACKTEST-B` | 30 / 30 | 2026-10-05 12:13:58 / 12:15:14 | 실험 C(대조 / 강제 종료) | [ack-boundary §실험 C](verification/2026-10-01-mqtt-ack-boundary.md) | [`2026-10-05-mqtt-ack-kill-e2e/`](verification/evidence/2026-10-05-mqtt-ack-kill-e2e/) (`11_control_after.txt`, `27_exp_final.txt`) | evidence already in files |
| `OUTAGE-E` | 720 | 2026-10-05 12:29:05 ~ 12:33:04 | 실험 E | §실험 E | [`2026-10-05-mqtt-ack-outage-resubscribe/`](verification/evidence/2026-10-05-mqtt-ack-outage-resubscribe/) (`E_after_influx.txt`) | evidence already in files |
| `RESUB-F` / `RESUB-F2` | 300 / 10 | 2026-10-05 12:37:47 / 12:38:34 | 실험 F | §실험 F | 같은 디렉터리 (`F_after_influx.txt`) | evidence already in files |
| `OUTAGE-E2` | 720 | 2026-10-05 12:56:52 ~ 13:00:51 | 실험 E2 | §실험 E2 | [`2026-10-05-mqtt-ack-outage-E2/`](verification/evidence/2026-10-05-mqtt-ack-outage-E2/) | evidence already in files |
| `SESSLOSS-G1` | 10 | 2026-10-05 13:07:29 | 실험 G (g1) | §실험 G | [`2026-10-05-mqtt-session-loss/`](verification/evidence/2026-10-05-mqtt-session-loss/) | evidence already in files |
| `SESSLOSS-G2` | **900** | 2026-10-05 13:08:05.734 ~ 13:09:38.261 | 실험 G (g2) | §실험 G | 같음 (`G2_after_influx.txt` = **523**) | evidence already in files — 단, **라이브 값이 이미 문서 값과 다르다**(아래) |
| `OUTAGE-E3` / `OUTAGE-S` | 720 / 720 | 2026-10-05 21:47:03 ~ 21:51:02 / 21:53:13 ~ 21:57:12 | 실험 E3·S | §실험 E3·S | [`2026-10-06-timeout-alert/`](verification/evidence/2026-10-06-timeout-alert/) (`E3_influx.txt`, `S_influx.txt`) | evidence already in files |
| `OUTAGE-R` | 300 | 2026-10-05 22:17:19 ~ 22:18:58 | 실험 H 복구 확인 | §실험 H 복구 | 같은 디렉터리 (`R_*`) | evidence already in files |
| `OUTAGE-P` | 720 | 2026-10-05 22:44:28 ~ 22:48:27 | 실험 P | §실험 P | [`2026-10-06-pause-alert-timing/`](verification/evidence/2026-10-06-pause-alert-timing/) (`P_*`) | evidence already in files |
| `OUTAGE-T` / `OUTAGE-TR` | 767 / 5 | 2026-10-05 22:58:37 ~ 23:08:00 / 23:11:01~02 | 실험 T / 복구 확인 | §실험 T, §실험 P 한계 | 같은 디렉터리 (`T_*`, `R_pub.log`) | evidence already in files |
| `RECON-PRB` | 1 | 2026-10-06 03:52:16.001 | 실험 D2 사전 프로브 | §실험 D2, [HANDOFF_2026-10-07](HANDOFF_2026-10-07.md) | [`2026-10-07-recheck-2aba182/`](verification/evidence/2026-10-07-recheck-2aba182/) (`R_probe_*`) | evidence already in files |
| `RECON-D2` / `D2B` / `D2C` | 360 / 240 / 63 | 2026-10-06 03:52:33 ~ 04:01:48 | 실험 D2·D2b·D2c | §실험 D2 | 같은 디렉터리 (`D2*_after_*`, `D2*_kafka_records.txt`) | evidence already in files |
| `OUTAGE-U` | 1,592 | 2026-10-06 04:10:59 ~ 04:27:41 | 실험 U | §실험 U | 같은 디렉터리 (`U_*`) | evidence already in files |
| `RECON-ZR` | 5 | 2026-10-06 04:31:29~30 | 실험 U 뒤 복구 확인 | HANDOFF_2026-10-07 | 같은 디렉터리 (`Z_restore_*`) | evidence already in files |
| `SIM-001` / `SIM-002` / `SIM-003` | 20,037 / 21,772 / 21,772 | 2026-09-16 12:18:05 ~ 2026-10-06 05:16:37 | 시뮬레이터(특정 실험 아님). 09-29 추적(WebSocket `SIM-001 ts=…09:00:02.123Z`, `19_influx_query_sanity_SIM-002.txt`), 10-07 앱 확인 기간 포함 | HANDOFF_2026-10-07 §4, 2026-09-29-trace-redelivery-spool §4 | 09-29 추적 결과는 [`2026-09-29-trace-websocket/`](verification/evidence/2026-09-29-trace-websocket/)·`trace-dlq-replay/` 파일에 있음 | **unknown** — 앱·대시보드 확인에 쓰는 일상 데이터라 어떤 주장이 라이브 행에 기대는지 전수 확인하지 않았다 |

**`SESSLOSS-G2` 900 vs 523**: 실험 G 직후 측정(`G2_after_influx.txt`)은 523(구독 이후분)이었다. 지금은 900 = 발행 전량이다.
실험 G 문서가 적은 "원래 ID 복귀 기동 시 `telemetry-backend` 세션에 쌓인 실험 중 메시지 재전달"로 나머지 377건이 나중에 저장된 것으로 보인다(추정, 이번에 확인하지 않음).
즉 이 차량의 라이브 행은 이미 실험 G의 판정 시점 상태가 아니며, 그 판정의 근거는 evidence 파일뿐이다.

공통 참고: 위 evidence 디렉터리 중 체크섬 매니페스트가 있는 것은 `load-test/schema-contract/evidence/20260909-145736/`와 `docs/verification/evidence/2026-10-01-mqtt-ack/`뿐이다. 나머지(10-05~10-07)는 파일은 있으나 매니페스트가 없다.

## 3. Kafka DLQ와 토픽

원본: [`04_kafka_offsets_retention.txt`](verification/evidence/2026-10-08-experiment-data-inventory/04_kafka_offsets_retention.txt). `kafka-get-offsets --time -2/-1`(group 없음)과 `kafka-configs --describe`만 썼다.
DLQ 이름 출처: `vehicle-telemetry-dlq`(`dlq-tools/dlq.py`), `vehicle-telemetry-anomaly-dlq`(`anomaly-detector/anomaly_detector.py`), `vehicle-telemetry-mqtt-dlq`, `vehicle-anomaly-alerts-dlq`.

**모든 토픽에서 earliest offset = latest offset — 읽을 레코드가 하나도 없다.** 그래서 키·`x-dlq-*` 헤더 표본은 **읽을 수 없었다**(읽을 것이 없어 group 없는 `--partition --offset` 읽기도 시도하지 않았다).

| 토픽 | earliest = latest (파티션별) | retention | 관련 실험 | 문서 | 삭제 영향 |
| --- | --- | --- | --- | --- | --- |
| `vehicle-telemetry-dlq` | p0: 19 | 1h (토픽 설정) | 누적 19건 중 16건은 09-29 이전 기존분, RPL-01 2건·RPL-02 1건(09-29) | [2026-09-29-trace-redelivery-spool §3](verification/2026-09-29-trace-redelivery-spool.md) — "Kafka에 남아 있다"고 적혀 있으나 **이미 만료됨** | none (비어 있음). 레코드 내용은 `2026-09-29-trace-dlq-replay/06_…`, `17_…`, `21_…`에 있음 |
| `vehicle-telemetry-mqtt-dlq` | p0: 16 | 1h | 스키마 계약 E2E 등(개별 매핑 불가 — 레코드 만료) | — | none (비어 있음) |
| `vehicle-telemetry-anomaly-dlq` | p0: 16 | 1h | P0-2a 감지기 실험 등(개별 매핑 불가) | — | none (비어 있음) |
| `vehicle-anomaly-alerts-dlq` | p0: 0 | 1h | 쓰인 적 없음 | — | none |
| `vehicle-telemetry` | p0 3484 / p1 45788 / p2 23099 | 1h | p0 3484는 실험 U 문서의 "p0 1892 → 3484"와 같다 | §실험 U | none (비어 있음) |
| `vehicle-anomaly-alerts` | p0 1 / p1 894 / p2 407 | 1h | — | — | none (비어 있음) |
| `itc-220850-telemetry` / `-dlq` / `-alerts` | 6 / 2 / 0 | 브로커 기본 168h | 이름상 `load-test/anomaly-contract-kafka` 실행 `20260909-220850` 계열로 보이나, 현재 스크립트의 이름 형식(`itc-<run>-<slot>-*`)과 다르다 — **정확한 출처 미확인** | [`evidence/20260909-220850/`](../load-test/anomaly-contract-kafka/evidence/20260909-220850/) | none (레코드 만료, 빈 토픽) |
| `itc-t1-telemetry` / `-dlq` / `-alerts` | 3 / 1 / 0 | 168h | **출처 미확인** (저장소에서 `itc-t1` 문자열을 찾지 못함) | — | unknown (빈 토픽이지만 무엇이 만들었는지 모름) |

브로커: `log.retention.hours=168`(기본), `offsets.retention.minutes=10080`(기본), `KAFKA_AUTO_CREATE_TOPICS_ENABLE=false`.

## 4. Consumer group ("커서 그룹")

원본: [`05_kafka_consumer_groups.txt`](verification/evidence/2026-10-08-experiment-data-inventory/05_kafka_consumer_groups.txt). `--list`·`--describe --all-groups` 만 실행.

| 그룹 | 상태 | 커밋 위치 | 분류 | 문서 | 삭제 영향 |
| --- | --- | --- | --- | --- | --- |
| `telemetry-storage-group` | Stable, 3 members | vehicle-telemetry 전 파티션 lag 0 | 운영 | `application.yml` | (대상 아님) |
| `anomaly-detector-group` | Stable, 3 members | 같음, lag 0 | 운영 | — | (대상 아님) |
| `anomaly-storage-group` | Stable, 3 members | vehicle-anomaly-alerts lag 0 | 운영 | — | (대상 아님) |
| `dlq-replay-trace-20260929` | **Empty**, 0 members | `vehicle-telemetry-dlq` p0 = 19 (끝) | 실험(DLQ 재주입 커서) | [2026-09-29-trace-redelivery-spool §3](verification/2026-09-29-trace-redelivery-spool.md), [HANDOFF_2026-09-29](HANDOFF_2026-09-29.md) | evidence already in files (`09_cursor_setup.txt`, `12_…`, `23_…`). 커서가 이미 DLQ 끝이고 DLQ는 비어 있다. 7일 offsets 보존으로 **자동 만료될 수 있다**(마지막 커밋 시각을 보지 않아 시점 미확인) |

`itc-*-group`·`-replay` 같은 load-test 전용 그룹은 목록에 **없다**(스크립트가 지웠거나 만료됨).

## 5. PostgreSQL

원본: [`06_postgres_rows.txt`](verification/evidence/2026-10-08-experiment-data-inventory/06_postgres_rows.txt). SELECT만, `password_hash`는 조회하지 않았다. 테이블: `users`, `vehicles`, `anomaly_alerts`, `flyway_schema_history`.

| 대상 | 내용 | 생성 시각(UTC) | 출처 | 문서 | evidence | 삭제 영향 |
| --- | --- | --- | --- | --- | --- | --- |
| `users` id 3 `e2e-pw-0929183104` | USER, **활성** | 2026-09-29 09:31:05 | 비밀번호 E2E | [2026-09-29-password-e2e](verification/2026-09-29-password-e2e.md) | [`2026-09-29-password-e2e/`](verification/evidence/2026-09-29-password-e2e/) (`01_e2e_results.txt`) | evidence already in files. 소유 차량 없음 |
| `users` id 4 `qa-admin` | ADMIN, **비활성** | 2026-10-06 04:57:19 | 앱 역할 확인(10-07) | [qa-account-deactivation](verification/2026-10-08-qa-account-deactivation.md), 앱 저장소 `docs/verification/2026-10-07-android-emulator-auth-roles.md` | [`2026-10-08-qa-account-deactivation/`](verification/evidence/2026-10-08-qa-account-deactivation/) | evidence already in files |
| `users` id 5 `qa-user` | USER, **비활성** | 2026-10-06 04:58:09 | 같음 | 같음 | 같음 | evidence already in files. **단, `vehicles.owner_id` FK가 이 행을 참조한다**(SIM-002) — 사용자 행만 지우면 FK 위반 |
| `vehicles` id 2 `SIM-002` | 이름 "QA User Car", 활성, 소유자 `qa-user` | 2026-10-06 05:03:37 | 앱 역할 확인에서 qa-admin이 등록 | 같음 | 앱 저장소 스크린샷(`04-admin-register-sim002.png` 등) | evidence already in files. 시뮬레이터 SIM-002 발행 자체와는 무관(차량 등록 행일 뿐) |
| `vehicles` id 1 `SIM-001` | 이름 "IONIQ 5 Long Range AWD Calligraphy 2024 Sales Team 3 Shared", 소유자 `admin` | 2026-09-16 12:54:54 | 앱 에뮬레이터 긴 이름 확인 | 앱 저장소 `docs/verification/2026-09-16-android-emulator.md`, [HANDOFF_2026-09-29](HANDOFF_2026-09-29.md) | 앱 저장소 | unknown — 실험용 이름이지만 SIM-001은 관리자 화면·WebSocket 확인에 계속 쓰이는 유일한 admin 소유 차량이다 |
| `anomaly_alerts` `SCHEMA145736-K03`·`K08`·`M03` | 각 1행(엔진 과열 2, 과속 1), detector RULE | 2026-09-09 06:03:55~56 | 스키마 계약 E2E `20260909-145736`(106°C 양쪽 이상 알림 등) | [RESULT_20260909_contract_e2e](../load-test/schema-contract/RESULT_20260909_contract_e2e.md) | `load-test/schema-contract/evidence/20260909-145736/` | evidence already in files |
| `anomaly_alerts` `SIM-001`~`003` | 404 / 440 / 452행 (6종 유형) | vehicle_timestamp 2026-09-16 12:18 ~ 2026-10-06 05:16 | 시뮬레이터의 주입 이상값 | — | — | unknown (§2 SIM과 같은 이유) |

`users` id 2는 존재하지 않는다(과거에 지워졌거나 롤백 — 이번에 확인하지 않음).

## 6. 삭제 후보 표 — dry-run (삭제 실행 없음)

상태: **사용자 결정 "지금은 삭제하지 않음"에 따라 후보와 명령만 준비했다. 아무것도 지우지 않았다.**
수집: 호스트 시계 2026-10-07 00:52~01:01 KST = **2026-10-06 15:52~16:01 UTC**. dev 스택을 `up -d`로 띄워 읽기 전용으로 조회하고 `stop`했다(볼륨 유지, `down -v` 없음).
이번 기동 동안 브로커에 붙은 것은 백엔드(`telemetry-backend`, `-sys`)뿐이었다(시뮬레이터·발행 없음, `dryrun_01` 끝) — 그래서 아래 Mosquitto 수치는 기동 중 메모리 상태와도 같다고 본다(추정).

**후보 기준**: 특정 실험이 만들었고 **그 실험의 문서·evidence에 정확한 식별자가 적힌 것만** `삭제 후보`. 출처가 하나로 정해지지 않거나 사용자 결정이 있는 것은 `보류`/`보존`.
**dry-run 방식**: 삭제 명령을 그대로 적고(실행 안 함), **같은 조건(같은 predicate·WHERE·시간 범위·대상 이름)**으로 COUNT/SELECT/describe를 돌려 영향 건수를 남겼다.

| 원본 | 내용 |
| --- | --- |
| [`dryrun_01_mosquitto_sessions.txt`](verification/evidence/2026-10-08-experiment-data-inventory/dryrun_01_mosquitto_sessions.txt) | `mosquitto.db` 재집계(client ID 문자열, 차량별 본문 수), 이번 기동 중 연결 |
| [`dryrun_02_mosquitto_session_removal_NOT_RUN.txt`](verification/evidence/2026-10-08-experiment-data-inventory/dryrun_02_mosquitto_session_removal_NOT_RUN.txt) | 세션 제거 방법(실행 안 함)과 대안 비교 |
| [`dryrun_03_influx_delete_counts.txt`](verification/evidence/2026-10-08-experiment-data-inventory/dryrun_03_influx_delete_counts.txt) | 차량별 `influx delete` 명령 틀 + 같은 창·같은 predicate의 Flux 카운트, 전체 목록, 버킷·tag key |
| [`dryrun_04_kafka_group_and_topics.txt`](verification/evidence/2026-10-08-experiment-data-inventory/dryrun_04_kafka_group_and_topics.txt) | consumer group 목록·describe, `itc-*` 토픽 describe·offset |
| [`dryrun_05_postgres_delete_counts.txt`](verification/evidence/2026-10-08-experiment-data-inventory/dryrun_05_postgres_delete_counts.txt) | `BEGIN READ ONLY … ROLLBACK` 안의 SELECT. DELETE는 주석으로만 |
| [`dryrun_06_redis_keys.txt`](verification/evidence/2026-10-08-experiment-data-inventory/dryrun_06_redis_keys.txt) | 키 이름 접두사·TTL·refresh token 소유자 일치 여부(토큰·값은 출력 안 함) |
| [`dryrun_07_volumes_spool.txt`](verification/evidence/2026-10-08-experiment-data-inventory/dryrun_07_volumes_spool.txt) | compose 볼륨 목록, spool 볼륨 파일 |

### 6-1. 2026-10-06 수집 이후 바뀐 것

- **Mosquitto 큐가 자랐다(추정 확인)**: `telemetry-backend-g1` 9,899 → **10,447**, `-g2` 8,999 → **9,547**. 둘 다 **+548** = 그 사이 발행된 `RECON-D3` 540 + `RECON-D3Z` 5 + `OPTF-E2E` 3. DB 본문도 9,897 → **10,445**(차량별 내역이 정확히 그 548건만큼 늘었다). g1−g2 = 900(`SESSLOSS-G2`) 그대로. 파일 4,258,086 → 4,498,962 bytes(mtime 2026-10-06 15:16:58 UTC = 직전 정지 시각).
- **InfluxDB 실험 행**: 8,222 → **8,770**(같은 548건: `RECON-D3`·`RECON-D3Z`·`OPTF-E2E` 추가 — [실험 D3](verification/2026-10-01-mqtt-ack-boundary.md), [선택 필드 E2E](verification/2026-10-08-optional-fields-e2e.md)).
- **PostgreSQL**: `anomaly_alerts`에 `OPTF-E2E` 1행 추가.
- **Consumer group `dlq-replay-trace-20260929`는 이미 없다**(`does not exist`, 7일 offsets 보존으로 자동 만료). 지울 것이 없다.
- 새로 본 것: Redis 키 6개(§6-3), spool 볼륨의 0바이트 `.tmp` 1개(D3, §6-3).

### 6-2. 삭제 후보 (dry-run 건수 포함, 실행 안 함)

공통: **저장소에 백업이 없다**(`influx backup`·`pg_dump`·`mosquitto.db` 사본 없음 — 저장소 파일과 compose 볼륨 목록 기준). 아래 삭제는 모두 **되돌릴 수 없다**. "재생성"은 같은 종류의 데이터를 다시 만든다는 뜻이지 같은 상태로 되돌린다는 뜻이 아니다.

#### Mosquitto 영속 세션 (`mosquitto-data` 볼륨의 `mosquitto.db`)

> **갱신(host 2026-10-07 13:03 UTC): M1~M4는 사용자 승인으로 삭제했다. M5·M6은 남아 있다.** 삭제 직전 건수·백업·검증은 [§6-5](#6-5-m1m4-삭제-실행-기록-사용자-승인). 아래 표의 dry-run 수치는 그 이전 값이다.

삭제 방법(실행 안 함): 같은 client ID로 **clean_session=true** 접속 후 즉시 끊기 —
`docker run --rm --network vehicle-telemetry-platform_telemetry-net eclipse-mosquitto:2.0 mosquitto_sub -h mosquitto -p 1883 -i <CLIENT_ID> -t 'telemetrix/session-cleanup/noop' -q 0 -E`.
브로커는 clean session CONNECT를 받는 순간 그 ID의 구독과 큐를 **전달 없이 버린다** — **이 접속 자체가 삭제다.** 운영 ID `telemetry-backend`·`-sys`에는 절대 쓰지 않는다 — 실행 중 백엔드가 끊기는 데 그치지 않고, **그 영속 세션의 구독과 아직 PUBACK 안 된 QoS 1 큐가 버려진다.** ADR-029 ACK 경계가 재전달을 기대는 바로 그 메시지라 **실제 유실**이 된다. 스크립트로 만들 경우 위 6개 ID의 **정확한 허용 목록**만 받게 한다(2차 리뷰). `mosquitto.db` 파일 삭제는 운영 세션까지 지우므로 쓰지 않는다. `mosquitto_ctrl`/dynsec kick은 오프라인 세션을 못 지우고 dev 설정엔 dynsec도 없다. 사후 확인은 브로커 정지 뒤 스냅샷으로만 가능하다(`dryrun_02`).

| # | 식별자 | 생성한 실험 | 연관 데이터 | dry-run (같은 대상 읽기 전용) | 삭제 영향 | 복구 가능 여부 |
| --- | --- | --- | --- | --- | --- | --- |
| M1 **삭제됨** | `telemetry-backend-g1` | [실험 G](verification/2026-10-01-mqtt-ack-boundary.md) (`2026-10-05-mqtt-session-loss/override_g1.yml`) | `vehicle/telemetry/#` QoS1 구독 + **오프라인 큐 약 10,447건**(차량: OUTAGE-E3·S·R·T·TR·P·U, RECON-PRB·D2·D2B·D2C·ZR·D3·D3Z, SESSLOSS-G2, OPTF-E2E, SIM-001~003 각 1,168). 이 메시지들은 `telemetry-backend` 세션으로 이미 저장 경로를 탄 사본이다 | client ID 문자열 **10,447회**(`dryrun_01`) | 실험 G 주장은 evidence 파일로 닫혀 있다(evidence already in files). **남겨 두면 브로커 가동 중 텔레메트리가 발행될 때마다 큐가 자란다**(정지 중엔 아님, 상한 100,000 — §6-3-b). 누가 이 ID로 백엔드를 띄우면 약 1만 건이 재전달되어 **InfluxDB에서 지운 행이 되살아날 수 있다** — InfluxDB 행을 지운다면 이 세션을 먼저(또는 함께) 지워야 한다 | 큐 내용은 **복구 불가**(사본이라 손실은 없음). 세션 자체는 실험 G 절차(`MQTT_CLIENT_ID=telemetry-backend-g1`, c0)로 다시 만들 수 있으나 큐는 그 뒤 발행분만 쌓인다 |
| M2 **삭제됨** | `telemetry-backend-g2` | 실험 G (`override_g2.yml`) | 같은 구독 + 큐 약 **9,547건**(= g1 − SESSLOSS-G2 900) | **9,547회** | 같음. 본문은 g1과 참조를 나눠 가져 **g1·g2를 둘 다 지워야** 공유 본문이 풀린다 | 같음 |
| M3 **삭제됨** | `telemetry-backend-g1-sys` | 실험 G (백엔드가 `<clientId>-sys`로 자동 생성, `MqttConfig.java:183`; evidence `G2_mosq_before/after.txt`에 기록) | `$SYS` 구독 3개. `$SYS`는 QoS0이라 오프라인 큐 없음 | 문자열 4회(세션 1 + 구독 3) | 없음(큐 없음). 실험 G evidence 파일에 이미 있음 | 실험 G 재실행 시 자동 재생성 |
| M4 **삭제됨** | `telemetry-backend-g2-sys` | 실험 G | 같음 | 4회 | 같음 | 같음 |
| M5 남음 | `telemetry-backend-h2` | [실험 H-2·H-2a](verification/2026-10-01-mqtt-ack-boundary.md) (`2026-10-06-timeout-alert/H2_override-h2.yml`) | **구독 기록 없음**(SUBACK 거부 실험) → 큐 없음 | 1회 | 없음. evidence already in files | H-2 재실행으로 재생성 |
| M6 남음 | `telemetry-backend-h2-sys` | 실험 H-2 (`H2_mosquitto_log.txt`에 기록) | `$SYS` 구독 3개, 큐 없음 | 4회 | 없음 | 같음 |

큐 길이 한계: `mosquitto.db`를 파싱하지 않고 `strings` 출현 횟수로 셌다. g1 10,447 vs 본문 10,445(차이 2)처럼 ±몇 건의 오차가 있다.

#### InfluxDB `telemetry` 버킷 (measurement `vehicle_telemetry`, tag `vehicle_id` 하나)

명령 틀(실행 안 함, 차량마다 1회):
`influx delete --bucket telemetry --org "$DOCKER_INFLUXDB_INIT_ORG" --token "$DOCKER_INFLUXDB_INIT_ADMIN_TOKEN" --start <START> --stop <STOP> --predicate '_measurement="vehicle_telemetry" AND vehicle_id="<ID>"'`
(컨테이너 안 환경변수 이름으로만 참조). 창은 `START = 첫 점의 초 내림`, `STOP = 마지막 점의 초 내림 + 1초`로 잡았다 — 앞뒤 여유가 있어 `influx delete`와 Flux `range()`(stop 배타)의 경계 차이가 결과를 바꾸지 않는다. 각 차량의 START/STOP은 `dryrun_03` 표에 있다.

**predicate 한계(정직하게)**: InfluxDB 2.x delete predicate는 **`=`와 `AND`만** 된다 — **`OR` 없음, 정규식·접두사 없음, `!=` 없음, `_field` 지정 불가**(시리즈의 모든 필드가 지워진다). 그래서 `SCHEMA145736-*` 같은 묶음도 **tag 값마다 명령 하나**다(아래 31개 tag = 31개 명령). 삭제는 즉시 되돌릴 수 없고 compaction 전까지 디스크가 바로 줄지 않을 수 있다.
**삭제 전 확인(2차 리뷰)**: "evidence already in files"는 I2·I15·P1/P2만 파일을 명시했고 I3~I14는 행마다 근거 파일을 적지 않았다(이 표의 "생성한 실험" 링크 문서 기준). InfluxDB 삭제는 되돌릴 수 없으므로 **실행 전 행마다 건수 증거 파일 경로를 채운다.**

행(row) = `speed` 포인트 수, 포인트 = 모든 필드 합(보통 행당 8, `OPTF-E2E`는 선택 필드가 빠져 14/3).

| # | 식별자 (`vehicle_id`) | 생성한 실험 | dry-run: 행 / 포인트 | 연관 데이터 | 삭제 영향 | 복구 가능 여부 |
| --- | --- | --- | --- | --- | --- | --- |
| I1 | `SCHEMA145736-K01`·`K02`·`K03`·`M01`·`M02`·`M03`·`MIX` | [스키마 계약 E2E](../load-test/schema-contract/RESULT_20260909_contract_e2e.md) (`inputs.csv` prefix=`SCHEMA145736`) | 1·1·1·1·1·1·2 = **8 / 64** | PostgreSQL 알림 K03·M03(P1), K08(보류, §6-3) | evidence already in files(체크섬 매니페스트 있음). 버킷 보존 90일이라 **2026-12-08 전후 자동 만료** 예정 | `PREFIX=SCHEMA145736 ./run_e2e.sh`로 같은 ID 재생성 가능, 타임스탬프는 다름(payload 파일 없음) |
| I2 | `RPL-02` | [DLQ 재주입 추적 §3](verification/2026-09-29-trace-redelivery-spool.md) | **1 / 8** | Kafka DLQ 레코드는 이미 만료 | evidence already in files(`26_synthetic_influx.txt`) | `20_synthetic_payload_RPL-02.json`으로 같은 시점 재생성 가능(보존 기간 안) |
| I3 | `ACKTEST-A` / `ACKTEST-B` | 실험 C ([ack-boundary](verification/2026-10-01-mqtt-ack-boundary.md)) | 30 / 30 = **60 / 480** | — | evidence already in files | `10_control.payloads.jsonl`·`22_exp.payloads.jsonl` 재발행으로 같은 시점 재생성 가능 |
| I4 | `OUTAGE-E` | 실험 E | **720 / 5,760** | — | evidence already in files | `E_payloads.jsonl` |
| I5 | `RESUB-F` / `RESUB-F2` | 실험 F | 300 / 10 = **310 / 2,480** | — | evidence already in files | `F_pub300`·`F_pub10.payloads.jsonl` |
| I6 | `OUTAGE-E2` | 실험 E2 | **720 / 5,760** | — | evidence already in files | `E2_payloads.jsonl` |
| I7 | `SESSLOSS-G1` / `SESSLOSS-G2` | 실험 G | 10 / 900 = **910 / 7,280** | G2 900건은 g1 큐(M1)에도 있음 | evidence already in files. **G2 라이브 900 ≠ 판정 시점 523** — 라이브 행은 이미 판정 근거가 아니다 | `G1_pub10`·`G2_payloads.jsonl`. 단 523/900 같은 중간 상태는 재현 불가 |
| I8 | `OUTAGE-E3` / `OUTAGE-S` | 실험 E3·S | 720 / 720 = **1,440 / 11,520** | g1·g2 큐에 사본 | evidence already in files | `E3_`·`S_payloads.jsonl` |
| I9 | `OUTAGE-R` | 실험 H 복구 확인 | **300 / 2,400** | 큐에 사본 | evidence already in files | `R_payloads.jsonl`(timeout-alert) |
| I10 | `OUTAGE-P` | 실험 P | **720 / 5,760** | 큐에 사본 | evidence already in files | `P_payloads.jsonl` |
| I11 | `OUTAGE-T` / `OUTAGE-TR` | 실험 T / 복구 확인 | 767 / 5 = **772 / 6,176** | 큐에 사본 | evidence already in files | T: `T_payloads.jsonl`. TR: `R_pub.log`만 있고 payload 파일은 확인하지 못함 |
| I12 | `RECON-PRB` / `RECON-D2` / `D2B` / `D2C` | 실험 D2 프로브·D2·D2b·D2c | 1 / 360 / 240 / 63 = **664 / 5,312** | 큐에 사본 | evidence already in files | `R_probe_payload`·`D2*_payloads.jsonl` |
| I13 | `OUTAGE-U` / `RECON-ZR` | 실험 U / 복구 확인 | 1,592 / 5 = **1,597 / 12,776** | 큐에 사본 | evidence already in files | `U_payloads`·`Z_restore_payloads.jsonl`(recheck) |
| I14 | `RECON-D3` / `RECON-D3Z` | [실험 D3](verification/2026-10-01-mqtt-ack-boundary.md) / 복구 확인 (`2026-10-08-ack-wait-observability/00_metadata.txt` test_vehicles) | 540 / 5 = **545 / 4,360** | 큐에 사본, spool `.tmp`(보존, §6-3) | evidence already in files | `D3_payloads`·`Z_restore_payloads.jsonl`(ack-wait) |
| I15 | `OPTF-E2E` | [선택 필드 E2E](verification/2026-10-08-optional-fields-e2e.md) | **3 / 14** | 알림 P2, 큐에 사본 | evidence already in files(`influx_field_counts.txt`) | `payloads.jsonl` 재발행으로 같은 시점 재생성 가능 |
| | **합계 31개 tag** | | **8,770 행 / 70,150 포인트** | | | |

#### Kafka 토픽

| # | 식별자 | 생성한 실험 | 연관 데이터 | dry-run | 삭제 영향 | 복구 가능 여부 |
| --- | --- | --- | --- | --- | --- | --- |
| K1 | `itc-220850-telemetry` / `-dlq` / `-alerts` | `load-test/anomaly-contract-kafka` 실행 `20260909-220850` — **중단된 실행**([`INVALID.md`](../load-test/anomaly-contract-kafka/evidence/20260909-220850/INVALID.md)), 근거는 `INVALID.md`와 실행 시각이다 — `inputs.csv`의 `itc-220850-telemetry`는 스크립트의 고정 `evidence_input` 라벨(`run_integration.sh` 32행)이라 실제 만든 토픽 이름을 증명하지 못한다(다른 실행들도 같은 형식 라벨, 현재 스크립트는 `itc-<RUN>-<slot>-<t>`를 만든다 — 라벨이 낡은 작은 증거 결함). 이름 형식은 슬롯 도입 전 스크립트와 맞는다(추정). 중단돼서 스크립트의 정리 단계(`--delete --topic`)가 안 돌았다 | 전용 group `itc-220850-group`은 이미 없음 | `kafka-topics --describe`: 각 1 partition, RF 1, 토픽 설정 없음(보존 168h). earliest = latest = 6 / 2 / 0 → **레코드 0건** | none(빈 토픽, 무효 실행). 무효 사유는 evidence에 남아 있음 | 명령(실행 안 함): `docker exec telemetry-kafka kafka-topics --bootstrap-server localhost:29092 --delete --topic itc-220850-telemetry`(‑dlq·‑alerts 각각). 빈 토픽이라 잃는 레코드 없음. 같은 이름 재생성은 `kafka-topics --create`로 가능 |

#### PostgreSQL `anomaly_alerts`

| # | 식별자 | 생성한 실험 | 명령(실행 안 함) | dry-run (같은 WHERE, `BEGIN READ ONLY`) | 삭제 영향 | 복구 가능 여부 |
| --- | --- | --- | --- | --- | --- | --- |
| P1 | `SCHEMA145736-K03`·`M03` 알림(엔진 과열 106°C, id 2·1) | 스키마 계약 E2E (`counts.csv` `anomaly_alerts_K=1`·`_M=1`) | `DELETE FROM anomaly_alerts WHERE vehicle_id IN ('SCHEMA145736-K03','SCHEMA145736-M03') AND detected_at >= '2026-09-09 06:03:54+00' AND detected_at < '2026-09-09 06:03:58+00';` | **2행** | evidence already in files(건수 `counts.csv`, 체크섬 있음). FK 없음(다른 테이블이 참조 안 함) | 재실행으로 같은 종류 재생성. `event_id`는 원본 payload에서 결정되므로 같은 payload면 같은 키 |
| P2 | `OPTF-E2E` 저전압 알림(10.5V, id 1318) | 선택 필드 E2E | `DELETE FROM anomaly_alerts WHERE vehicle_id = 'OPTF-E2E' AND detected_at >= '2026-10-06 15:16:00+00' AND detected_at < '2026-10-06 15:17:00+00';` | **1행** | evidence already in files(`anomaly_alerts.txt`, `logs_grep.txt`에 event_id) | `payloads.jsonl` 재발행 시 같은 `event_id`(`e5c62138…`)로 재생성될 것으로 본다(미확인) |

### 6-3. 보류·보존

| 식별자 | 저장소 | 판정 | 읽기 전용 확인 | 이유 |
| --- | --- | --- | --- | --- |
| `qa-admin`(id 4), `qa-user`(id 5) | PostgreSQL `users` | **보존(사용자 결정)** | 둘 다 `active=f`, refresh token 0(Redis 값 대조) | 사용자 결정. `qa-user`는 `vehicles.owner_id`(FK `fk_vehicles_owner`)가 참조 |
| `SIM-002` (vehicles id 2, "QA User Car") | PostgreSQL `vehicles` | **보존(사용자 결정)** | 소유자 `qa-user` | 사용자 결정 |
| `telemetry-backend`, `telemetry-backend-sys` | Mosquitto | 보존(운영) | 이번 기동에서 접속 | 운영 기본 ID |
| `telemetry-storage-group`·`anomaly-detector-group`·`anomaly-storage-group` | Kafka group | 보존(운영) | 목록에 있음 | 운영 |
| 운영 토픽 6개(`vehicle-telemetry`, DLQ 4개, `vehicle-anomaly-alerts`) | Kafka | 보존(운영) | retention 1h로 이미 비어 있음(04) | 운영 토픽. 레코드 삭제가 필요 없다 |
| `dlq-replay-trace-20260929` | Kafka group | **대상 없음** | `does not exist`(dryrun_04) | 자동 만료됐다 |
| `itc-t1-telemetry` / `-dlq` / `-alerts` | Kafka 토픽 | 보류 | 레코드 0(3/1/0 = 3/1/0) | **출처 미확인** — 저장소 어디에도 `itc-t1` 문자열이 없다. 후보 기준(문서화된 식별자) 미달 |
| `SCHEMA145736-K08` 알림 1행(과속, speed 300) | PostgreSQL `anomaly_alerts` | 보류 | 1행(id 3, dryrun_05 H1) | 실험은 확실하지만 **evidence가 이 행을 담고 있지 않다** — `counts.csv`는 K03·M03 알림만 셌다. 저장은 거부(DLQ)되고 감지기는 알림을 낸, **P0-2a 이전 저장/감지 갈림의 이 실행 유일한 라이브 흔적**이다. 지우면 재확인 불가(self-contained 아님) |
| `e2e-pw-0929183104`(id 3) + Redis refresh token 1개 | PostgreSQL `users`, Redis | 보류 | 소유 차량 0, `active=t`. refresh token 1개(값이 이 사용자, TTL로 역산하면 2026-09-29 09:30 UTC 발급 = E2E 실행, 2026-10-13경 자동 만료) | 지시상 e2e 사용자는 보류. 문서가 "DB에 남아 있다"고 적은 의도적 잔여물이다. 지운다면 Redis 토큰도 함께(`RefreshTokenService.revokeAll` 방식) |
| `SIM-001`~`SIM-003` 행(20,037 / 21,772 / 21,772), 알림 404 / 440 / 452 | InfluxDB, PostgreSQL | 보류 | 전체 목록(dryrun_03 끝, dryrun_05) | 시뮬레이터 일상 데이터. 특정 실험이 만든 것이 아니고 앱·대시보드 확인과 09-29 추적이 기댄다 |
| `vehicles` id 1 `SIM-001`(긴 이름, 소유자 admin) | PostgreSQL | 보류 | 그대로 | admin 소유 유일 차량. 긴 이름 확인은 앱 저장소 문서 |
| `counter:__rand_int__`, `key:__rand_int__` | Redis | 보류 | 2키, TTL 없음 | redis-benchmark의 고정 키 이름이고 테스트 종류(SET/GET/INCR)가 `load-test/redis-outage/evidence/20260912-115730/redis-normal-latency.csv`와 맞는다. 하지만 **키 이름이 evidence에 적혀 있지 않다**(기준 미달). 지우는 영향은 없다 |
| admin refresh token 3개 | Redis | 보존 | 값이 `admin` | 운영 계정 |
| spool `1791298387712-…-11c1b58d-….tmp` (0바이트) | `backend-spool` 볼륨 | **보존(증거)** | 1개, 2026-10-06 14:53:07 UTC(dryrun_07) | 실험 D3 결함 후보의 현물 증거. 처리 방식은 HANDOFF_2026-10-08 §3-1 결정 대기 |
| compose 볼륨 12개 | Docker 볼륨 | 보존 | 목록(dryrun_07) | 볼륨 삭제·`down -v` 금지 |
| Grafana·Prometheus TSDB | 볼륨 | 범위 밖 | — | 이번에 보지 않음 |

### 6-3-b. g1·g2 큐 — 측정 시각과 증가 구간 (2026-10-07 보정)

큐 크기는 **브로커가 디스크에 쓴 `mosquitto.db` 스냅샷**을 `strings`로 센 추정치다(실시간 메모리가 아님).

| 측정 | 읽은 시각(UTC) | 스냅샷 시각 = `mosquitto.db` mtime(UTC) | g1 | g2 |
| --- | --- | --- | ---: | ---: |
| 1차 목록 | 2026-10-06 14:37:41 | 2026-10-06 05:16:45 | 약 9,899 | 약 8,999 |
| dry-run | 2026-10-06 15:52:08 | 2026-10-06 15:16:58 | 약 10,447 | 약 9,547 |
| 삭제 직전(§6-5) | 2026-10-07 13:02:40 | 2026-10-07 13:02:22 (브로커 정지로 갱신) | 약 11,719 | 약 10,819 |
| 삭제 뒤(§6-5) | 2026-10-07 13:04:12 | 2026-10-07 13:04:11 | **0** (세션 없음) | **0** (세션 없음) |

- **증가는 두 스냅샷 사이(10-06 05:16:45 → 15:16:58 UTC), 브로커가 가동 중이던 구간에서 일어났다.** 그 구간에 `vehicle/telemetry/#`로 발행된 실험 메시지(D3 540 + D3Z 5 + OPTF-E2E 3 = 548)와 증가분 +548이 정확히 같다.
- **스택이 정지한 동안에는 늘지 않는다**(브로커가 꺼져 있으면 받을 메시지가 없다). 앞선 보고의 "지금도 계속 증가"는 "브로커가 가동되고 누군가 `vehicle/telemetry/#`로 발행하는 동안 늘어난다"로 고친다.
- 현재 다른 발행자: dry-run 뒤 스택은 `stop` 상태이고, 시뮬레이터 profile은 dry-run 기동에 포함되지 않았다(그 에이전트 보고 기준). 다음에 시뮬레이터를 켜면 초당 발행량만큼 두 큐가 다시 늘어난다(상한 `max_queued_messages` 100,000).
- **clean session 접속 금지(재확인)**: 두 ID가 실험 전용임은 실험 G 문서·증거의 client ID와 일치하는 것까지만 확인했다. 사용자가 실험 전용임을 확정하기 전에는 어떤 정리 접속도 하지 않는다. **→ 사용자가 g1·g2(+`-sys`)를 확정·승인해 §6-5에서 삭제했다.**

### 6-3-c. 삭제 후보별 복구 가능한 백업 방법 (실행 안 함)

| 대상 | 백업 방법 | 복원 방법 | 주의 |
| --- | --- | --- | --- |
| Mosquitto 세션(M1~M6) | 브로커 **정지 후** 데이터 볼륨의 `mosquitto.db`를 통째로 복사(예: 임시 컨테이너로 볼륨을 읽어 호스트에 tar) | 같은 파일을 되돌리고 브로커 기동 | **파일 단위라 선택 복원이 안 된다** — 운영 세션(`telemetry-backend`·`-sys`)까지 그 시점으로 돌아간다. 가동 중 복사는 일관성 보장 없음 |
| InfluxDB 실험 차량 행(I1~I31) | (a) `influx backup`(버킷 전체) (b) 차량별 `influx query`로 annotated CSV 추출 | (a) `influx restore` (b) `influx write --format csv`로 같은 timestamp 재기록(같은 시리즈·시각은 덮어쓰기) | (a)는 전체 버킷이 그 시점으로 — 운영 데이터와 섞여 선택 복원이 어렵다. (b)가 차량 단위 복원에 맞다. 대부분은 실험 `payloads.jsonl` 재발행으로도 재생성 가능(SCHEMA145736·OUTAGE-TR은 payload 파일 없음) |
| Kafka 빈 토픽(K1) | `kafka-topics --describe` 출력 보관(파티션·설정) | `kafka-topics --create`로 같은 이름·설정 | 레코드 0건이라 데이터 백업 불필요 |
| PostgreSQL 알림 행(P1~P3) | `\copy (SELECT * FROM anomaly_alerts WHERE <같은 조건>) TO '<파일>' CSV HEADER` | `\copy anomaly_alerts FROM '<파일>' CSV HEADER` | `id`·`event_id` UNIQUE 충돌 확인, 시퀀스는 건드리지 않음 |
| 볼륨 전체(최후 수단) | 스택 정지 후 각 named volume을 tar로 보관 | 같은 볼륨에 풀기 | 크고 느리다. 선택 복원 불가. **볼륨 삭제는 하지 않는다** |

### 6-4. 실행한다면 순서 (참고, 결정 아님)

1. Mosquitto M1·M2(큐가 있는 것)를 먼저 — 남겨 둔 채 InfluxDB만 지우면, 이 ID로 누가 접속할 때 OUTAGE-E3 이후 실험 행과 SIM 행이 재전달로 다시 쓰인다.
2. InfluxDB I1~I15(tag마다 명령 1개), PostgreSQL P1·P2, Kafka K1.
3. 각 단계 뒤 이 절의 dry-run 조회를 그대로 다시 돌려 0이 되는지 확인. Mosquitto는 브로커 정지 뒤 스냅샷으로만 확인된다.

### 6-5. M1~M4 삭제 실행 기록 (사용자 승인)

상태: **실행 완료.** 범위는 사용자가 지정한 4개 ID뿐이다 — `telemetry-backend-g1`·`-g2`·`-g1-sys`·`-g2-sys`. `-h2`·`-h2-sys`(M5·M6)는 요청에 없어 **건드리지 않았고 남아 있다.** InfluxDB·PostgreSQL·Kafka는 지우지 않았다.
시각: 호스트 시계 2026-10-07 13:02~13:07 UTC(이 문서의 다른 시각과 같은 호스트 시계 어긋남 주의). HEAD `3d6ab28`. dev compose(평문 1883, `allow_anonymous true`, dev 리스너에 ACL 없음). 시뮬레이터는 시작 전에 이미 종료 상태였다.
원본: [`docs/verification/evidence/2026-10-09-session-cleanup/`](verification/evidence/2026-10-09-session-cleanup/) (`00_metadata.txt`에 타임라인).

**삭제 직전 기록** (`02_before_snapshot_fresh.txt` — 브로커를 정지해 13:02:22에 새로 쓰인 스냅샷을 읽기 전용 마운트로 `strings`. 정지 직전 디스크 스냅샷(mtime 12:58:04, `01`)과 수치 동일)

| ID | 소유·목적 | 구독 | 큐(스냅샷 문자열 수, 추정) |
| --- | --- | --- | ---: |
| `telemetry-backend-g1` | [실험 G](verification/2026-10-01-mqtt-ack-boundary.md#실험-g--세션-없는-재시작과-구독-2026-10-05) (`MQTT_CLIENT_ID` override, `2026-10-05-mqtt-session-loss/override_g1.yml`) | `vehicle/telemetry/#` (QoS1) | **11,719** (본문 11,717 — OUTAGE-E3·P·R·S·T·TR·U, RECON-D2·D2B·D2C·D3·D3Z·PRB·ZR, SESSLOSS-G2 900, OPTF-E2E 3, STUCK-CTL 720·CTLZ 5·WARM 1, SIM-001~003 각 1,350) |
| `telemetry-backend-g2` | 실험 G (`override_g2.yml`) | `vehicle/telemetry/#` (QoS1) | **10,819** (= g1 − SESSLOSS-G2 900) |
| `telemetry-backend-g1-sys` | 실험 G (백엔드가 `<clientId>-sys`로 자동 생성) | `$SYS/broker/{publish/messages/received, publish/messages/dropped, clients/connected}` | 0 (`$SYS` QoS0, 큐 없음) |
| `telemetry-backend-g2-sys` | 실험 G | 같은 `$SYS` 3개 | 0 |

dry-run(10,447 / 9,547) 이후 +1,272 = SIM 3대 × 182 + STUCK-* 726 — 그 사이 시뮬레이터와 다른 실험(STUCK)이 발행한 만큼 두 큐가 같이 자랐다.

**백업(§6-3-c 방식)**: 브로커 정지 상태에서 볼륨을 읽기 전용으로 마운트해 `mosquitto.db`를 저장소 **밖**으로 복사했다.
위치 `C:\Users\USER\AppData\Local\Temp\claude\D--vehicle-telemetry-platform\d456ac25-…\scratchpad\mosq-backup\mosquitto.db.20261007T1302Z-before-g1g2-cleanup`(전체 경로는 `00_metadata.txt`), → **보존용 사본: `D:ehicle-telemetry-backups\mosquitto.db.20261007T1302Z-before-g1g2-cleanup`**(저장소 밖, 같은 sha256 `72067d72…be652` 확인 — 세션 임시 폴더는 지워질 수 있다).
**5,049,590 bytes, sha256 `72067d72c5a39b8d917c22680bc07eb7363c02527c4ca4e7cfbd5c93101be652`**(복사 시점 볼륨 파일과 같은 해시).
**주의**: 세션 임시 디렉터리다 — 보관하려면 다른 곳으로 옮겨야 한다. 파일 단위 복원이라 되돌리면 운영 세션·h2 세션도 13:02:22 상태로 돌아간다.

**삭제 방법**: [`cleanup_sessions.sh`](verification/evidence/2026-10-09-session-cleanup/cleanup_sessions.sh) — 정확히 4개 ID만 받는 하드 허용 목록. 인자 중 하나라도 목록 밖이면 **접속 없이 전체 거부**(종료 코드 3). 각 ID로 일회용 `eclipse-mosquitto:2.0` 컨테이너(compose 네트워크)에서 `mosquitto_sub -h mosquitto -p 1883 -i <ID> -t telemetrix/session-cleanup/noop -q 0 -E` (‑c 없음 = clean session).
거부 시험(`03`): `telemetry-backend`, `telemetry-backend-sys`, `telemetry-backend-h2`, 허용 ID와 운영 ID 혼합 — 4건 모두 접속 없이 거부. 운영 ID로는 한 번도 접속하지 않았다.

**결과** (`04`, `05`)

| 확인 | 결과 |
| --- | --- |
| 4개 접속 | 13:03:38~53, 각각 CONNACK 0 → SUBACK → DISCONNECT, exit 0. 브로커 로그 `as telemetry-backend-g1 (p2, c1, k60)` 등 4줄 |
| 삭제 뒤 스냅샷(브로커 정지, mtime 13:04:11) | g1·g2·g1-sys·g2-sys 문자열 **0**, 저장된 메시지 본문 **0**, 파일 5,049,590 → 969 bytes |
| 남은 세션 | `telemetry-backend`(`vehicle/telemetry/#`), `telemetry-backend-sys`($SYS 4개), `telemetry-backend-h2`(구독 없음), `telemetry-backend-h2-sys`($SYS 3개) |
| 백엔드 재접속 | 브로커 재기동마다 `telemetry-backend`·`-sys` (c0) 재접속(마지막 13:04:26). 브로커 정지 2회(13:02:22~13:03:01, 13:04:10~13:04:19) 동안 백엔드 로그에 `Lost connection` — 의도한 짧은 중단 |
| 수집 확인(`06`, `07`) | `SESSCLN-1009` QoS1 5건 발행 → PUBACK 5 → InfluxDB 0 → **5** |

**이 작업이 새로 남긴 것**: InfluxDB `SESSCLN-1009` 5행(2026-10-07T13:04:32.001~.005Z, 확인용 발행).
**한계**: 큐 길이는 여전히 `strings` 추정(파싱 아님). 삭제 뒤 본문 0은 운영 세션 `telemetry-backend`가 그 시점 큐를 비워 둔 상태였다는 뜻이기도 하다(운영 세션 큐를 직접 본 것은 아님).

## 7. 실험 행이 앱·이력·집계에 섞이는지

상태: **읽기 전용 확인(코드 + SQL/Flux).** 삭제 없음. REST는 호출하지 않았다(로그인 없음) — REST 결론은 코드 근거다.
원본: 같은 디렉터리 `08_postgres_leak_check.txt`(`BEGIN READ ONLY … ROLLBACK`), `09_influx_vehicle_ids.txt`, `10_grafana_panel_series_30m.txt`, `11_leak_code_reading.txt`(파일·줄 참조).

**전제**: 수집 경로는 차량 등록을 확인하지 않는다. 계약을 통과한 `vehicle_id`는 무엇이든 InfluxDB에 저장되고, 감지기 알림도 PostgreSQL에 저장되며, WebSocket 프레임도 발행된다. 그래서 실험 행은 **저장소에는 있다.** 문제는 어디서 **보이느냐**다.

현재 InfluxDB tag 40개(`09`) 중 PostgreSQL `vehicles`에 있는 것은 `SIM-001`·`SIM-002` **둘뿐**이다(`08`). 문서 목록 이후 새로 생긴 tag: `STUCK-CTL` 720·`STUCK-CTLZ` 5·`STUCK-WARM` 1, `SIM-050` 1, `STOR1-CHK` 3, `SESSCLN-1009` 5(이번 확인용) — 출처는 아래 §7-1에 보충(2026-10-08).

| 경로 | 섞이는가 | 근거 |
| --- | --- | --- |
| (a) 앱 차량 목록 `GET /api/vehicles` | **섞이지 않는다** | ID 목록을 PostgreSQL `vehicles`(활성, 관리자는 전체·사용자는 소유)에서 만들고, InfluxDB 최신값·HIGH 건수는 **그 ID들로만** 조회한다(`VehicleService.findAllVisibleTo`, `getLatestByVehicleIds`, `countHighByVehicleIds`). 실험 ID는 `vehicles`에 0건 |
| (b) 이상 이력 REST `/api/vehicles/{id}/anomalies[/count\|/page]` | **보이지 않는다** | 모두 `vehicleId == {id}` 필터 + `VehicleAccessInterceptor` → `canAccess`: 관리자도 **등록된 활성 차량**이어야 통과. 실험 ID는 관리자에게도 거부 |
| (b) WebSocket `/topic/vehicle/{id}/(telemetry\|anomalies)` | **구독 불가** | 같은 `canAccess`. 서버는 실험 ID 프레임도 발행하지만 받을 수 있는 구독자가 없다 |
| (b) 전체/관리자용 이상 목록 | **없음** | 차량 단위 매핑만 있다 |
| (b) PostgreSQL에 저장된 미등록 차량 알림 | **저장은 됨, 노출 경로 없음** | `OPTF-E2E` 1, `SCHEMA145736-K03`·`K08`·`M03` 각 1, 그리고 **`SIM-003` 466**(시뮬레이터지만 미등록) = 470행. STUCK·OUTAGE·RECON 등은 알림 0 |
| (b) Webhook 알림(`notifier.py`) | **설정되면 섞인다(현재 꺼짐)** | 차량 필터 없이 모든 알림을 보낸다. 실행 중 감지기 3개 모두 `WEBHOOK_URL` 미설정(존재 여부만 확인) |
| (c) 백엔드 집계·fleet 요약 | **섞이지 않는다** | `TelemetryQueryService`의 Flux는 전부 `r.vehicle_id == <id>`. fleet 요약 필드는 목록 차량별 값 |
| (c) Grafana `vehicle-telemetry` 대시보드(5개 InfluxDB 패널) | **섞인다 — 별도 선으로** | `_measurement`·`_field`만 거르고 `vehicle_id` 필터·등록 차량 조인·변수가 없다. `group()`이 없어 차량별로 평균하므로 **SIM 값에 평균으로 섞이지는 않지만**, 시간 범위(기본 now-30m) 안에 실험 행이 있으면 **추가 선으로 나타난다.** 13:07 UTC에 범위가 돌려준 시리즈: SIM-001~003 외 `SESSCLN-1009`·`SIM-050`·`STUCK-CTLZ`(`10`) |
| (c) Grafana `pipeline-funnel`·`backend-metrics` | **실험 당시 트래픽이 합산돼 있다(분리 불가)** | Prometheus 카운터·rate, 차량 라벨 없음. 저장된 행이 아니라 발행 시점의 처리량이다 |
| (c) 감지기 ML(IsolationForest) | **최근 실험분은 학습 창에 섞인다(일시적)** | 파티션당 모델 1개(그 파티션의 모든 차량), 슬라이딩 창 2,000건, Redis에 상태 저장. 같은 파티션에 새 메시지 2,000건이 들어오면 빠진다(코드 추론, Redis 상태는 읽지 않음) |

**정리**: 앱(목록·상세·이력·WebSocket)에는 등록된 활성 차량만 보이므로 실험 행이 섞이지 않는다. 섞이는 곳은 **Grafana 텔레메트리 대시보드(별도 선)**, **설정 시 Webhook**, **감지기 ML 학습 창(일시적)**, 그리고 실험 당시의 **Prometheus 처리량**이다. `SIM-003`은 실험이 아니지만 미등록이라 앱에서 보이지 않고(알림 466행 포함) Grafana에만 보인다.
**확인하지 않은 것**: 실제 REST 호출(관리자 로그인 필요), Grafana 화면 렌더링, Redis ML 상태 내용, 앱 저장소 쪽 코드.

### 7-1. 새 tag 출처 보충 (2026-10-08, 삭제 없음)

시각은 InfluxDB의 `speed` 포인트 min/max `_time`(UTC, 읽기 전용 조회 — [`evidence/2026-10-08-experiment-data-inventory/11_new_tags_2026-10-08.txt`](verification/evidence/2026-10-08-experiment-data-inventory/11_new_tags_2026-10-08.txt)).

| tag | 행 | 시각(UTC) | 만든 실험 | 증거 |
| --- | ---: | --- | --- | --- |
| `STUCK-WARM` | 1 | 2026-10-07 12:31:30.1 | MqttAckWaitStuck 대조군 — override 실효값 확인용 워밍업 1건 | [`2026-10-09-ack-wait-stuck-control/`](verification/evidence/2026-10-09-ack-wait-stuck-control/) `A_*` |
| `STUCK-CTL` | 720 | 12:32:25.3 ~ 12:36:24.8 | MqttAckWaitStuck 대조군 본 실행(Kafka pause 150.9초) | 같은 폴더 `C_*`, ack-boundary 문서 D3 "대조군" |
| `STUCK-CTLZ` | 5 | 12:41:14.2 ~ 12:41:15.0 | 대조군 원복 확인 5건 | 같은 폴더 `Z_*` |
| `SIM-050` | 1 | 12:50:45.0 | CRL 폐기 검증 — 폐기되지 않은 차량 인증서 1회 발행 | [`2026-10-09-crl-revocation/04_sim050_and_nocert.txt`](verification/evidence/2026-10-09-crl-revocation/04_sim050_and_nocert.txt) |
| `STOR1-CHK` | 3 | 01:21:57.1 ~ 01:21:57.3 | 인증서 접근 범위 — storage-1 저장 경로 확인(Kafka 직접 주입) | [`2026-10-08-cert-access-scope/14_*`·`15_*`](verification/evidence/2026-10-08-cert-access-scope/) |
| `SESSCLN-1009` | 5 | 13:04:32.001 ~ .005 | 실험 G 세션 정리 뒤 수집 확인 | [`2026-10-09-session-cleanup/06_*`·`07_*`](verification/evidence/2026-10-09-session-cleanup/) |

모두 **실험이 만든 것이 확실**하다(식별자가 각 증거의 입력 파일에 있다). 사용자 결정(2026-10-08)에 따라 **지우지 않는다** — `-h2`·`-h2-sys` 세션과 PostgreSQL 미등록 차량 알림 470행도 그대로 둔다.

## 보지 않은 것

- Mosquitto **메모리상** 현재 세션 목록과 큐 길이(위 §1 한계).
- Redis 키(이번 범위 밖). qa 계정 refresh token은 10-08 문서에 따르면 이미 삭제됐다. **→ §6에서 키 이름·소유자만 확인했다(6개: admin refresh 3, e2e refresh 1, redis-benchmark 2; qa 0).**
- Grafana·Prometheus TSDB 안의 실험 기간 시계열(보존 기간 동안 남는다).
- 다른 에이전트가 수집 이후에 추가한 데이터.
