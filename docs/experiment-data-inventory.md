# 실험 잔여 데이터 목록 — 2026-10-08

상태: **목록만 작성.** 아무것도 지우거나 바꾸지 않았다(사용자 결정 ④). 삭제 여부는 사용자가 이 목록을 보고 정한다.
이 문서는 **삭제를 권하지 않는다.** "삭제 영향" 열은 사실만 적는다.

| 항목 | 값 |
| --- | --- |
| 수집 시각 | 호스트 시계 2026-10-06 23:37~23:42 KST (14:37~14:42 UTC). 문서 파일명(10-07·10-08)과 호스트 시계가 하루 이상 어긋나 있다 — 아래 시각은 전부 **데이터 자체의 UTC 시각**이다 |
| 스택 | dev compose(평문 1883). 수집 중 다른 에이전트가 같은 스택에서 MQTT/Kafka 실험을 돌리고 있었다 — 값은 그 시점의 스냅샷이다 |
| 방법 | 읽기 전용 조회만. 컨테이너 재시작·새 consumer group·새 MQTT 연결 없음. 비밀값은 컨테이너 내부 환경변수로만 썼고 출력하지 않았다 |
| 원본 | [`docs/verification/evidence/2026-10-08-experiment-data-inventory/`](verification/evidence/2026-10-08-experiment-data-inventory/) (01~06, 각 파일 첫 줄에 명령) |

"삭제 영향" 값의 뜻:

- **none** — 이 데이터를 근거로 쓰는 주장이 없다(또는 이미 비어 있다).
- **evidence already in files** — 그 실험의 주장은 저장소의 evidence 파일(발행 payload, 전후 카운트, 로그)로 닫혀 있다. 라이브 데이터는 재계산에 쓰이지 않는다.
- **needed for re-verification** — 라이브 데이터를 다시 조회해야만 확인할 수 있는 주장이 있다.
- **unknown** — 판단 근거가 부족하다.

## 요약

| 저장소 | 실험 잔여물 | 상태 |
| --- | --- | --- |
| Mosquitto 영속 세션 | `telemetry-backend-g1`·`-g1-sys`·`-g2`·`-g2-sys`·`-h2`·`-h2-sys` (6개) | 남아 있음. **g1·g2는 `vehicle/telemetry/#` 구독을 가진 채 오프라인이라 메시지가 쌓이고 있다**(추정, §1) |
| InfluxDB `telemetry` 버킷 | 실험 차량 28개 tag (총 8,222행) + 시뮬레이터 SIM-001~003 (63,581행) | 남아 있음. 버킷 보존 2160h(90일) |
| Kafka DLQ·운영 토픽 | 6개 토픽 **전부 비어 있음**(earliest = latest) | retention 1시간으로 만료. 09-29 문서의 "RPL DLQ 레코드가 남아 있다"는 **더 이상 사실이 아니다** |
| Kafka 테스트 토픽 | `itc-220850-*`·`itc-t1-*` (6개) | 토픽은 남음, 레코드는 만료(비어 있음) |
| Consumer group | `dlq-replay-trace-20260929` (Empty) | 남아 있음. 기본 `offsets.retention.minutes=10080`(7일)에 걸리는 시점이라 곧 자동 만료될 수 있다(시각 미확인) |
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

- DB에 남은 텔레메트리 메시지 본문은 약 10,617건이다(차량별 내역은 원본 §B — OUTAGE-E3·S·R·P·T·TR·U, RECON-*, SESSLOSS-G2, SIM-001~003, payload 시각 2026-10-05 13:08 ~ 2026-10-06 05:16 UTC).
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

## 보지 않은 것

- Mosquitto **메모리상** 현재 세션 목록과 큐 길이(위 §1 한계).
- Redis 키(이번 범위 밖). qa 계정 refresh token은 10-08 문서에 따르면 이미 삭제됐다.
- Grafana·Prometheus TSDB 안의 실험 기간 시계열(보존 기간 동안 남는다).
- 다른 에이전트가 수집 이후에 추가한 데이터.
