# MQTT ACK 경계 — 2026-10-01

상태: **부분 검증 / dirty 작업 트리 관찰**. 전체 스택·실차·장기 부하 검증 완료가 아니다.

## 사전 기준과 구현

[계획](../plans/2026-10-01-mqtt-ack-boundary.md), [ADR-029](../architecture-decisions.md).
동일 MQTT client ID와 살아 있는 브로커에서, Kafka 완료 전 수신 프로세스를 강제 종료하면 미확인 메시지가 재전달되어야 한다. Kafka 또는 spool 실패를 성공 ACK로 처리하지 않는다.

현재 구현은 단일 Paho 콜백에서 저장 확인을 기다린다. Kafka 콜백 완료 순서대로 PUBACK을 보내지 않는다. 이 단순화에는 처리 동시성 감소라는 대가가 있다.

## 실험 A — 강제 종료 경계

- 실제 Mosquitto + Spring Integration 어댑터 + 실제 MqttMessageHandler/TelemetryProducer + 별도 Java 프로세스.
- **KafkaTemplate 전송 완료만 테스트용 pending future로 통제했다. 실제 Kafka 브로커 장애 재현이 아니다.**
- 부모 프로세스가 차량측 publish 완료, 수신측 producer 진입을 확인한다. 자동 ACK 대조군은 브로커 로그의 수신측 PUBACK도 확인한 뒤 종료한다.
- Windows `Process.destroyForcibly()` 사용. Linux SIGKILL 실측이라고 표현하지 않는다.
- 종료 뒤 같은 client ID·cleanSession=false로 연결해 3초 동안 재전달을 관측한다. 브로커는 계속 살아 있다.

| 조건 | 반복 | Kafka 완료 | 재전달 |
| --- | ---: | ---: | ---: |
| 기존 자동 ACK | 3 | 0 | 0/3 |
| manual ACK 보류 | 3 | 0 | 3/3 |

수정 전 어댑터 설정 대조 실험은 [before.xml](evidence/2026-10-01-mqtt-ack/before.xml), 수정된 handler로 같은 비교를 실행한 결과는 [after.xml](evidence/2026-10-01-mqtt-ack/after.xml).
현재 테스트는 Kafka 미완료 구간을 통제한 경계 테스트이며, 전체 Spring Boot JVM·실제 Kafka·InfluxDB를 동시에 연결한 프로세스 강제 종료 E2E를 대신하지 않는다.

## 실험 B — 실제 Kafka/MQTT 정상 전달과 실패 spool

실제 Testcontainers Kafka(3 partition, RF=1)와 Mosquitto. 생산자 ACK=all, idempotence=true. 테스트 시간을 제한하기 위해 delivery.timeout.ms=3000, request.timeout.ms=1000, max.block.ms=3000으로 설정했다. **운영 기본 재시도 시간이나 장기 장애 실험이 아니다.**

- 정상 전달: 자동/수동/수동/자동 순서로 각 100건. 고유 timestamp별 Kafka 레코드 400건 확인.
- Kafka pause: 추가 1건이 spool에 남는지 확인한 뒤 unpause. retryPending 이후 고유 레코드 총 401건, spool 0 확인.
- handler 단위 테스트: 저장 완료 전 ACK 없음, 실패 receipt ACK 없음, DLQ 성공 후 ACK 및 DLQ 실패 ACK 없음.

[원본 Kafka 계약 테스트](evidence/2026-10-01-mqtt-ack/kafka.xml).

### 소규모 소요 시간 — 성능 결론 보류

| 모드 | 100건 발행 시작부터 Kafka 관측까지 |
| --- | ---: |
| 자동 1 | 3884ms |
| 수동 1 | 908ms |
| 수동 2 | 1416ms |
| 자동 2 | 602ms |

warm-up·consumer group 할당·폴링이 포함됐고, 발행자는 publish 완료를 순차 대기한다. 이는 포화 부하 측정이 아니다. 편차가 커 개선률·최대 처리량을 계산하지 않는다. 기존 9,600 msg/s와 비교할 수 없다. 높은 유입에서 브로커 큐가 쌓이는지, p95/p99와 실제 ACK 창을 측정하는 작업은 남아 있다.

## 실험 C — 전체 스택 강제 종료 (2026-10-05)

상태: **각 1회 관찰.** 반복·안정성 주장 없음. 원본: [`evidence/2026-10-05-mqtt-ack-kill-e2e/`](evidence/2026-10-05-mqtt-ack-kill-e2e/) (`00_metadata.txt`).

**가설(실행 전)**: manual ACK 경계가 맞다면, Kafka가 완료를 주지 못하는 동안 받은 메시지는 백엔드 프로세스가 SIGKILL 되어도 브로커가 같은 client ID 세션(cleanSession=false)으로 재전달하고, 재시작 뒤 최종 InfluxDB의 고유 timestamp 수가 발행 수와 같다.
**성공 기준(실행 전)**: (1) 발행자가 PUBACK을 받은 수 = N. (2) 재시작 뒤 InfluxDB의 해당 차량 고유 시점 수 = N. (3) 재시작한 백엔드가 N건을 수신(재전달 관찰). (4) 새로 생긴 DLQ 레코드 0. (5) Kafka 레코드 수는 N 이상이며 중복은 허용(at-least-once)하되 고유 timestamp는 N. **대조군**: 같은 절차에서 Kafka 정상, kill 없음.
**실패 기준**: InfluxDB 고유 수 < N, 또는 재전달 수 < 미확인 메시지 수.

### 환경

- 코드: HEAD `c5347c8`. 이미지 `sha256:4a004892…`(`vehicle-telemetry-platform-backend`, 이 실행에서 `build backend`). 빌드가 약 6분 걸렸고 그 사이 **다른 쪽이 작업 트리를 수정해 `690f50e`로 커밋했다.** 그래서 이미지 jar를 꺼내 확인했다 — `UnsendableTelemetryException` 클래스와 `ack.callback.missing` 문자열이 **없어** 이미지는 `c5347c8` 코드다(확인함). 이 실험은 `690f50e`를 검증하지 않는다.
- Docker Desktop 29.7.2 / compose 5.4.0, dev(평문 1883) 스택. 시뮬레이터·감지기는 켜지 않았다. 기존 볼륨 유지(삭제 없음). 백엔드 DEBUG는 저장소 밖 override(삭제함).
- **브로커 세션 유지 조건(확인함)**: `MqttConfig`의 `setCleanSession(false)`, client ID `${MQTT_CLIENT_ID:telemetry-backend}`(`application.yml`, compose에서 override 없음 → 고정), Mosquitto `persistence true` + `mosquitto-data` 볼륨. 브로커 로그 `New client connected … as telemetry-backend (p2, c0, k60)`의 `c0`이 cleanSession=false다 — `31_mosquitto_backend_session_lines.txt`. 브로커는 실험 내내 살아 있었다.
- 발행: `eclipse-mosquitto:2.0` 컨테이너의 `mosquitto_pub -d -q 1 -l`, 토픽 `vehicle/telemetry/ACKTEST-x`, 밀리초 단위 고유 timestamp 30개. 발행 수는 **로그의 `received PUBACK`(RC:0) 수**로 셌다. 이 PUBACK은 브로커→발행자이며 백엔드의 PUBACK이 아니다.
- 종료: `docker kill`(SIGKILL, 컨테이너 exit 137, OOM 아님). 자동 재시작은 `docker update --restart=no`로 막았다(종료 후 `unless-stopped`로 되돌림).
- Kafka 정지는 `docker pause`(프로세스 동결, 연결 유지)다. `stop`이 아니다.

### 결과

| 항목 | 대조군(ACKTEST-A, Kafka 정상, kill 없음) | 실험(ACKTEST-B, Kafka pause → 백엔드 kill → 복구 → 재시작) |
| --- | ---: | ---: |
| 발행자 PUBACK(RC:0) | 30 | 30 |
| 백엔드 수신(handler) | 30 | kill 전 **1**, 재시작 뒤 **30** |
| `vehicle-telemetry` end offset 증가 | +30 | **+31** |
| 그중 고유 timestamp(`kafka-console-consumer`, 차량 필터) | 30 | **30**(중복 1) |
| DLQ(`-dlq`, `-mqtt-dlq`) 증가 | 0 / 0 | 0 / 0 |
| InfluxDB 고유 시점 수 | **30** | **30** |
| spool 보관·드레인 | 0 | 0 (`telemetry_spool_*`) |

실험 시간표(UTC, `21_exp_marks.txt`): Kafka pause 12:15:03 → 발행 시작·완료(약 12:15:14) → 백엔드 kill 12:15:31 → Kafka unpause 12:15:42 → 백엔드 시작 12:16:02 → 12:17:17 최종 집계.

- kill 전: handler가 **첫 1건만** 받아 Kafka 완료를 기다리며 멈췄다(`23_…`). 나머지 29건은 브로커 쪽에 남았다고 본다(브로커 큐 상태는 직접 보지 않았다 — 재시작 뒤 수신 수 30으로 추정 뒷받침). spool 보관 로그는 없었다(delivery.timeout 120초 전에 종료).
- **Kafka unpause 직후, 백엔드가 죽어 있는데 p1 end offset이 +1 됐다**(`25_…`). 죽은 프로세스가 pause 전에 소켓으로 보낸 첫 레코드를 브로커가 풀린 뒤 기록한 것으로 보인다(추정 — 레코드 timestamp 일치만 확인). 이 건은 ACK된 적이 없어 재전달되었고 Kafka에 **같은 timestamp가 2건**이다. 즉 이 관찰에서 중복은 실제로 발생했고, 저장소(`(vehicle_id, ms)`가 point identity)가 흡수했다.
- 재시작한 프로세스의 `telemetry.mqtt.messages.received` = 30, invalid 0, `전송 완료` 로그 30(`26_…`, `28_…`).

판정: 성공 기준 (1)~(5) **충족(각 1회)**. 실패 기준 해당 없음.

### 발견한 것 — 재시작 직후 구독 오류 로그 1건 (원인 미확정)

재시작 직후 `Error subscribing to [vehicle/telemetry/#] … MqttException: Timed out waiting for a response from the server`(`connectComplete` → `subscribe`, 12:16:25)가 1회 기록됐고(`30_…`), 바로 뒤에 재전달 메시지 처리가 시작됐다. 이 실행에서는 세션이 브로커에 남아 있어 구독이 유지돼 메시지가 모두 도착했다. 같은 로그는 최초 기동(재전달할 메시지 없음)에는 없었다. 원인은 **확정하지 못했다.** 가설: 연결 직후 Paho 콜백 스레드가 재전달 메시지와 SUBACK을 같은 스레드에서 처리하며 `completionTimeout`(5초)을 소모. 새 세션(cleanSession 효과)에서도 같은지, 수동 ACK 변경 전에도 있었는지는 **미확인**이다. 영향 범위도 미확인이므로 후속 확인 항목으로 둔다.

### 한계

- **각 1회, N=30, 단일 차량 토픽, 무부하.** 재전달 건수가 큰 경우, 브로커 inflight(20) 초과 대기분, 장시간 정지는 보지 않았다.
- kill 시점은 handler가 1건에서 멈춘 상태 하나다. 임의 시점 반복이 아니다. 완료 직전·직후 경계(Kafka 완료 → ACK 호출 사이)는 이 실험이 아니라 실험 A의 통제 future 테스트가 다룬다.
- Linux SIGKILL이 컨테이너 안 JVM에 전달된 것이다. 호스트 전원 차단·브로커 강제 종료는 범위 밖이다(ADR-029 그대로).
- 이 결과를 "전체 at-least-once 완료"로 읽지 않는다. 수정 전(자동 ACK) 이미지로 같은 절차를 돌린 대조는 **하지 않았다.** 수정 전 동작은 실험 A의 자동 ACK 0/3(통제 조건)만 근거다.
- Kafka 레코드 고유 수는 차량 필터로 센 것이고, 다른 차량 데이터는 대조하지 않았다.
- 남긴 테스트 데이터: InfluxDB `ACKTEST-A`·`ACKTEST-B` 각 30행, Kafka 같은 키 레코드(retention 1시간). 삭제하지 않았다.

## 환경·실행

- 기준 commit: `21cf99cc0bf9314f54ce206d078135078bf4a78d`, 미커밋 변경 포함.
- Windows, Docker Desktop Linux containers, Java 17.0.20, Gradle 8.7, Paho 1.2.5, Spring Integration MQTT 6.2.4.
- Mosquitto 2.0.22, 이미지 ID `sha256:199ea8ef2e35ec2b1b37e59cfd1dbae538ed4dfa4a2251a121a52215a6248a21`.
- 실행 명령(backend): `./gradlew.bat test --tests '*MqttCrashBoundaryContractTest' --no-daemon --offline --console=plain` (수정 전), `./gradlew.bat test --tests '*Mqtt*' --tests '*TelemetryProducerTest' --no-daemon --offline --console=plain` (수정 후).
- 수정 후 관련 테스트 37건 통과. 이어서 전체 Java **373건 통과, 실패 0, skip 0**, 계약 테스트 클래스 7개 실행, 7분 36초. [전체 실행 요약](evidence/2026-10-01-mqtt-ack/test-summary.txt)에 보존했다. Python·Flutter는 이번 변경 대상이 아니므로 재실행하지 않았다.
- 기존 Telemetrix 스택 및 다른 프로젝트 DB·Redis는 시작·중지·데이터 변경하지 않았다. Testcontainers의 임시 리소스만 사용했다.

## 남은 검증

- ~~실제 Kafka 완료 경계에서 전체 백엔드 프로세스 강제 종료, 재시작 후 최종 InfluxDB 대조.~~ **1회 관찰 완료(2026-10-05, 실험 C)** — 30/30 저장, 중복 1건은 저장소가 흡수. 반복·임의 시점·대량 재전달은 미검증. 재시작 직후 구독 timeout 로그 1건의 원인 확인이 새로 남았다.
- 네트워크 단절·재접속과 저장 지연이 겹치는 경우, 디스크 용량 부족 실스택 재현.
- 수동 ACK 변경 후 목표 부하에서 처리량·지연·큐 포화 비교. 현재는 저부하 계약만 확인했다.
- 브로커 강제 종료/호스트 전원 차단은 이번 보호 범위 밖이다.
- ELM327 실측은 연결 가능한 장비·차량 정보가 없어 미실행. [절차](../runbook/elm327-measurement.md)만 준비했다.
