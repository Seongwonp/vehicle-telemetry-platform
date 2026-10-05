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

## 실험 E — Kafka 장기 정지 중 MQTT 연결 유지 (2026-10-05)

상태: **1회 관찰.** 반복·안정성 주장 없음. 원본: [`evidence/2026-10-05-mqtt-ack-outage-resubscribe/`](evidence/2026-10-05-mqtt-ack-outage-resubscribe/) (`00_metadata.txt`, `E_*`). 아래 가설·기준은 실행 전에 먼저 적었다.

**가설(실행 전, ADR-029 "남은 위험"에서 가져옴)**: Kafka가 `docker stop`으로 150초 내려가면 handler가 첫 메시지의 Kafka 완료를 기다리며(producer 기본 `max.block.ms`/`delivery.timeout.ms` 60·120초) Paho 콜백 스레드를 막고, keepAlive 60초 안에 PINGRESP를 처리하지 못해 MQTT 연결이 끊길 수 있다. 끊기면 미ACK 메시지가 재전달되어 중복이 생기지만 유실은 없어야 한다. 반대로 연결이 유지될 수도 있다고 보았다(PINGREQ/PINGRESP가 콜백 스레드와 별개일 가능성).
**조작**: 고유 밀리초 timestamp 720건(차량 `OUTAGE-E`)을 호스트 루프 `sleep 0.333`으로 공급 → 시작 30초 뒤 `docker stop telemetry-kafka`(pause 아님) → 150초 뒤 `docker start`. 시뮬레이터·감지기 없음. 스크립트 `expE.sh`.
**성공 기준(실행 전)**: (1) 발행자 PUBACK(RC:0) = N. (2) Kafka 고유 timestamp = N, InfluxDB 고유 시점 = N. (3) DLQ(`-dlq`, `-mqtt-dlq`) 증가 0. (4) 중복 수 기록(허용). (5) 연결 끊김/재연결 여부·시각 기록(끊김 자체는 실패가 아니라 관찰 대상). **실패 기준**: 고유 수 < N, DLQ 증가, 복구 후 수신 미재개.

### 환경

HEAD `7d3aafd`(작업 트리 변경 없음, 증거 폴더만 untracked). 이미지 `sha256:4eefa21d…`(이 실행에서 `build backend`). 이미지 jar에 `UnsendableTelemetryException` 클래스와 `ack.callback.missing` 문자열이 **있음을 확인**했다(HEAD 코드). dev(평문 1883) 스택, 기존 볼륨 유지, 백엔드 DEBUG는 저장소 밖 override(삭제함). Docker Desktop 29.7.2. producer/MQTT 설정은 코드 기본값 그대로다(`max.block.ms`·`delivery.timeout.ms` 미지정 → 기본, `keepAlive=60`, `cleanSession=false`, 자동 재연결 상한 5초).

### 결과

| 항목 | 값 |
| --- | ---: |
| 발행 수 / 발행자 PUBACK(RC:0) | 720 / **720** |
| `vehicle-telemetry` end offset 증가 | +721 |
| 그중 고유 timestamp (`kafka-console-consumer`, 차량 필터) | **720**(중복 1) |
| InfluxDB 고유 시점 수 | **720** |
| DLQ(`-dlq`, `-mqtt-dlq`) 증가 | 0 / 0 |
| `telemetry.mqtt.messages.received` / invalid / `ack.callback.missing` | 721 / 0 / 0 |
| 브로커 `dropped` | 0 |
| spool 보관·드레인 | 1 / 1, 최종 pending 0 |

실제 공급 속도는 약 2.6건/초였고 720건 발행에 약 328초가 걸렸다(호스트 `sleep` 루프 오버헤드. 목표 3건/초·240초와 다르다). 발행 시작 12:29:06Z, Kafka stop 12:29:36~42, start 12:32:12~15, 발행 종료 12:34:33 (UTC, `E_marks.txt`). 성공 기준 (1)~(4) **충족(1회)**.

**시간표(백엔드·브로커 로그, UTC)**

| 시각 | 사건 | 출처 |
| --- | --- | --- |
| 12:29:37.6 | Kafka stop 직후 들어온 71번째 메시지의 전송 대기 시작(전송 완료 로그 없음) | `E_backend_full_log.txt` |
| 약 12:29:42 | 마지막 inbound 활동(`lastInboundActivity`로 역산) | 같은 파일, ClientState 로그 |
| 12:30:37 | PINGREQ 송신(`lastPing`) — PINGRESP 처리 흔적 없음 | 같은 로그 |
| **12:31:37.16** | **`ClientState: Timed out as no activity, keepAlive=60s` → `Lost connection`**. 브로커 로그는 `Client telemetry-backend closed its connection`(keepalive 초과 문구 없음) | 백엔드·브로커 로그 |
| **12:31:37.71** | **`Expiring 1 record(s) … 120000 ms`** → spool 보관. 첫 메시지가 콜백을 **약 120초** 막았다(= `delivery.timeout.ms` 기본값) | 백엔드 로그 |
| 12:31:38.18 | 브로커에 새 TCP 연결, CONNECT 없음 → 12:33:08.54 브로커 `Client <unknown> has exceeded timeout` | 브로커 로그 |
| 12:32:14 | Kafka 시작(host mark) | `E_marks.txt` |
| 12:32:38.8 | spool 드레인 완료(offset 18984) | 백엔드 로그 |
| 12:33:08.84 | 재연결 성공(`p1`, c0) | 브로커 로그 |
| **12:33:13.84** | **`Error subscribing … Timed out waiting for a response`(연결 5.0초 뒤)**, 직후 백로그 도착(483건/10초) | 백엔드 로그 |

- **연결은 유지되지 않았다 — 가설이 맞았다(1회 관찰).** keepalive 60초 안에 PINGRESP가 처리되지 않아 클라이언트가 스스로 연결을 끊었다. `lastInboundActivity`가 약 12:29:42에서 멈춘 것은 콜백 스레드가 막힌 뒤 수신 큐가 찬 시점과 맞는다(2.6건/초 × 약 4~5초 ≈ 10건). **이 메커니즘(수신 큐 포화 → receiver 스레드 정지 → PINGRESP 미처리)은 Paho 소스를 읽어 확인한 것이 아니라 로그와 일치하는 추정이다.**
- **ACK 경계가 중복을 만든 건은 이 건이다.** 71번째 메시지(`ts=…12:29:28.643Z`)는 12:31:37.71에 spool에 보관돼 future가 완료되며 ACK를 시도했지만, 연결은 0.5초 전에 끊긴 뒤였다. 이 메시지는 spool 드레인으로 offset 18984에 기록됐고, 재연결 뒤 브로커가 같은 메시지를 재전달해 12:33:13에 다시 처리되어 offset 18985에 한 번 더 들어갔다(Kafka 같은 timestamp 2건, `uniq -d`로 확인). ADR-029가 적은 "연결이 끊긴 사이 대기를 마친 콜백이 옛 메시지를 ACK할 수 있다"의 실스택 사례다. 옛 연결에서의 ACK가 실패했는지 무시됐는지는 로그에 없다. 저장소가 `(vehicle_id, ms)` 동일 시점을 하나로 흡수해 InfluxDB는 720이다.
- **유실은 없었다.** 연결이 끊겨 있던 12:29:42~12:33:13 사이 들어온 약 560건은 브로커가 세션 큐에 보관했다가 재접속 뒤 전달했다(브로커 `dropped`=0). 브로커 큐 상한(100,000)과 한참 거리가 있는 저부하다.
- **ADR-029의 "수신 큐가 차면 PINGRESP를 못 읽어 끊길 수 있다"는 추정이 이 조건에서 실제로 일어났다.** 끊김은 Kafka 정지 150초 중 **약 120초 지점**(delivery timeout 도달 직전)에서 발생했고, 끊김 이후의 spool 경로는 의도대로 동작했다.

### 발견한 것 / 설명 못 한 것

- **재접속이 약 90초 늦었다(원인 미확정).** Kafka가 돌아오기 전인 12:31:38에 TCP 연결은 열렸으나 브로커는 CONNECT를 받지 못했고(`<unknown>`), 90초 뒤 브로커가 그 소켓을 닫았다. 실제 MQTT 재연결은 12:33:08이다. 이 시점에 콜백 스레드는 12:31:37.7에 풀려 있었다. 자동 재연결 상한은 5초로 설정돼 있는데도 이 지연이 생겼다. 원인은 모른다(확인하지 않음). 결과적으로 **수집 중단 시간이 약 96초(12:31:37~12:33:13) 늘어났다.** 반복하지 않았으므로 상수로 쓰지 않는다. 이 지연 때문에 "Kafka 정지 150초 → 수집 정상화까지"는 정지 시작 후 약 217초였다.
- 같은 날 실험 C의 구독 timeout 오류가 **여기서도 재현됐다**(12:33:13) — 실험 F 참고.

### 한계

각 1회, 단일 차량, 저부하(약 2.6건/초). Kafka 정지 길이 150초 하나만 시험했다(60·120초 경계 근처는 보지 않았다). 호스트 `sleep` 기반 공급이라 속도가 일정하지 않다. 브로커 쪽 큐 길이는 직접 측정하지 않고 메시지 수와 `dropped`로 추정했다. 실제 차량 속도·복수 차량·고부하에서는 수신 큐가 더 빨리 차거나 브로커 큐 상한에 닿을 수 있다. spool 용량·디스크는 건드리지 않았다.
남긴 테스트 데이터: InfluxDB `OUTAGE-E` 720행, Kafka 721건(retention 1시간).

## 실험 F — 재시작 직후 구독 timeout 재현 (2026-10-05)

상태: **1회 관찰(재현됨).** 반복 없음. 원본: 위와 같은 폴더(`F_*`). 가설·기준은 실행 전에 적었다.

**가설(실행 전)**: 실험 C의 `Error subscribing … Timed out waiting for a response` 1회는 재접속 직후 브로커가 offline 큐의 밀린 메시지를 먼저 쏟아내 SUBACK 처리가 completionTimeout 안에 끝나지 않았기 때문일 수 있다(추정). 그렇다면 백엔드를 `docker stop`한 채 300건을 쌓았다 `start`할 때 같은 로그가 재현되어야 한다. 로그가 나도 세션 구독이 브로커에 남아 있어 수신은 계속될 것이라고 예상했다(추정).
**조작**: 백엔드 `docker stop` → `RESUB-F` 300건 발행(PUBACK을 센다) → `docker start` → 로그 관찰 → 새 10건(`RESUB-F2`) 발행.
**성공 기준(실행 전)**: (1) PUBACK 300 → Kafka·InfluxDB 고유 300. (2) 구독 오류 로그 발생 여부와 이후 수신 재개 기록. (3) 새 10건 도착. (4) DLQ 증가 0. **실패 기준**: 300건 누락 또는 새 10건 미도착.

### 결과

| 항목 | 값 |
| --- | ---: |
| 발행자 PUBACK(RC:0) (백엔드 정지 중) | 300 / 300 |
| 재시작 뒤 구독 오류 로그 | **1회 발생**(브로커 연결 12:38:13.95 → 오류 12:38:18.96, **5.0초**) |
| 백엔드 수신 `RESUB-F` / 전송 완료 | 300 / 300 |
| 구독 오류 뒤 첫 메시지 처리 | 12:38:19.10(오류 0.14초 뒤), 300건 완료 12:38:20.6 |
| 새 10건(`RESUB-F2`) PUBACK / 수신 / Kafka / InfluxDB | 10 / 10 / 10 / 10 |
| Kafka end offset 증가 / 고유 timestamp | +310 / 310 (중복 0) |
| InfluxDB 고유 시점 | `RESUB-F` 300, `RESUB-F2` 10 |
| DLQ 증가 | 0 / 0 |
| `messages.received` / invalid | 310 / 0 |

판정: 성공 기준 (1)~(4) **충족(1회)**. 이 로그는 실험 C·E·F에서 각 1회, **백로그가 있는 재접속 3건 모두**에서 났고, 백로그가 없던 최초 기동에서는 없었다(실험 C 기록). 같은 조건 3회 반복이 아니라 서로 다른 시나리오 3건이라 안정성 표현은 쓰지 않는다.

### 관찰에서 읽은 것

- **세 건 모두 오류 시각이 연결 직후 5.0초이고, 첫 메시지는 오류 직후(0.1~0.2초)에 처리됐다.** 연결~오류 사이 5초 동안 백엔드는 메시지를 한 건도 처리하지 않았다(F: 12:38:13.95~19.10, E: 12:33:08.84~13.84). 스택트레이스에서 `connectComplete → subscribe → waitForCompletion`이 **Paho `CommsCallback.run` 스레드**에서 실행됨이 확인된다(`F_backend_log.txt` 781행~). 메시지 전달도 같은 콜백 스레드라는 점(Paho 설계, 소스로 확인하지 않음)을 합치면, **콜백 스레드가 SUBACK을 기다리며 막혀 있고 그동안 브로커가 보낸 백로그가 수신 큐에 쌓여 SUBACK이 그 뒤에서 읽히지 못한다**는 설명이 로그와 맞는다. 이것은 **추정**이다(Paho 내부 큐·스레드 동작을 소스로 확인하지 않았고, 브로커가 SUBACK을 메시지 뒤에 보냈는지도 확인하지 않았다). 5초 값은 관찰값이며 Spring Integration의 기본 completion timeout 상수는 확인하지 못했다.
- **오류 뒤 구독은 살아 있었다.** 새 10건이 모두 도착했다. 다만 `cleanSession=false`라 브로커가 이전 세션의 구독을 유지하고 있었으므로, "재구독 요청이 성공했다"와 "이전 구독이 유지됐다"는 이 실험으로 구분되지 않는다. 세션이 사라진 경우(브로커 볼륨 손실, client ID 변경)에 이 timeout이 구독 상실로 이어지는지는 **미검증**이다.
- **Spring Integration 6.2.4의 처리(외부 소스 근거)**: `MqttPahoMessageDrivenChannelAdapter.subscribe()`는 `MqttException`을 `catch`해 `MqttConnectionFailedEvent`를 발행하고 `logger.error("Error subscribing to …")`를 남길 뿐, **재시도·재던지기·연결 종료 없이 연결을 유지한다.** 우리 로그의 메시지·스택(`subscribe` 302행 → `connectComplete` 425행, 이 줄 번호는 로그의 것)과 일치한다. 출처: [`MqttPahoMessageDrivenChannelAdapter.java` v6.2.4](https://github.com/spring-projects/spring-integration/blob/v6.2.4/spring-integration-mqtt/src/main/java/org/springframework/integration/mqtt/inbound/MqttPahoMessageDrivenChannelAdapter.java). 소스는 웹 조회 도구의 요약으로 확인했으므로 라인 단위 인용이 아니다(이 저장소에서 소스를 직접 열어 보지 않았다). **백엔드 코드에는 `MqttConnectionFailedEvent`·`MqttSubscribedEvent` 리스너가 없다**(`backend/src/main` 검색, 0건). 구독 실패가 로그 한 줄 외의 신호로 남지 않는다는 뜻이며, 세션 소실 경로에서 구독을 잃으면 감지할 수단이 없을 수 있다(결함 후보, 재현하지 않음).

### 한계

각 1회, N=300, 단일 차량 토픽, 무부하. 백엔드 정지 시간은 약 8초(발행 300건 동안)로 짧다. 브로커 큐에 더 많이(수천 건 이상) 쌓였을 때 timeout 횟수·지속 시간은 보지 않았다. 세션 소실 경로 미검증. 첫 발행 시도 한 번은 스크립트의 상대경로 오류로 0건 발행(브로커에 아무것도 가지 않음)했고 절대경로로 다시 실행했다(`F_marks.txt`에 `pub_done` 줄이 둘인 이유). 남긴 테스트 데이터: InfluxDB `RESUB-F` 300행·`RESUB-F2` 10행, Kafka 310건.

## 실험 E2 — 재접속 수정 후 재실행 (2026-10-05)

상태: **1회 관찰.** 반복·안정성 주장 없음. 원본: [`evidence/2026-10-05-mqtt-ack-outage-E2/`](evidence/2026-10-05-mqtt-ack-outage-E2/) (`00_metadata.txt`, `E2_*`, `expE2.sh`). 실험 E와 같은 절차·같은 스크립트(차량 ID만 `OUTAGE-E2`)다. 질문: 커밋 `c41ff49`(연결 끊김 시 저장 확인 대기를 깨움, 실험 D)가 실험 E의 재접속 약 90초 지연과 중복에 영향을 주는가.

**실행 전 기대(추정)**: 수정이 콜백 스레드의 대기를 풀어 주므로 재접속이 빨라질 수 있다. 다만 실험 E에서 끊김(12:31:37.16)과 대기 만료(12:31:37.71)가 0.5초 차이였으므로, 이 조건에서는 수정 효과가 작을 수도 있다고 보았다. 성공 기준은 실험 E와 같다(PUBACK=N, Kafka·InfluxDB 고유=N, DLQ 증가 0, 중복·끊김 기록).

### 환경

HEAD `c41ff49`(작업 트리 변경 없음, 증거 폴더만 untracked). 이미지 `sha256:8e0b82d3…`(이 실행 직전 `build backend`). 이미지 jar의 `MqttMessageHandler.class`에 `onConnectionLost`·`MqttConnectionFailedEvent`가 **있음을 확인**했다(소스는 `MqttMessageHandler.java:163`). dev(평문 1883) 스택, 기존 볼륨 유지, 시뮬레이터·감지기 없음, DEBUG는 저장소 밖 override(삭제함). 스택은 실험 전에 전부 정지 상태였고 필요한 서비스만 올렸다(Kafka가 healthy가 되기까지 첫 `up`이 timeout으로 한 번 실패해 healthy 확인 뒤 `kafka-init`을 다시 실행했다).

### 결과 — 실험 E와 나란히 (각 1회)

| 항목 | 실험 E (`7d3aafd`) | 실험 E2 (`c41ff49`) |
| --- | ---: | ---: |
| 발행 / PUBACK(RC:0) | 720 / 720 | 720 / 720 |
| 발행 소요 | 약 328초 | 약 270초 (호스트 `sleep` 오버헤드 편차) |
| Kafka stop → start (host) | 12:29:36~42 → 12:32:12~15 | 12:57:22~27 → 12:59:57~13:00:00 |
| keepAlive 끊김(`Timed out as no activity`/`Lost connection`) | 12:31:37 (stop 후 약 120초) | 12:59:22 (stop 후 약 120초) |
| `연결 끊김 — 저장 확인 대기 중단` 로그 | 없음(코드 이전) | **1회**, 끊김과 같은 초(12:59:22) |
| 대기 만료(`Expiring 1 record(s)… 120000 ms`) | 12:31:37 (끊김 +0.5초) | 12:59:23 (끊김 +1초, 로그 초 단위) |
| 브로커가 본 재접속(`New client connected`) | 12:33:08 | **12:59:24** |
| 끊김 → 재접속 | 약 91초 (백엔드 기준 끊김→수신 재개 약 96초) | **약 2초** (끊김→첫 메시지 처리 재개 약 7초) |
| 브로커 `<unknown> has exceeded timeout` | 1회 (12:33:08) | **이번 실행 구간에서 0회** |
| 끊김 횟수 / `Lost connection` | 1 / 1 | 1 / 1 |
| 재접속 뒤 `Error subscribing … Timed out` | 있음(연결 +5.0초) | 있음(12:59:24 → 12:59:29, +5초) |
| `vehicle-telemetry` end offset 증가 | +721 | +721 |
| Kafka 고유 timestamp / 중복 | 720 / 1 | 720 / 1 |
| InfluxDB 고유 시점 | 720 | 720 |
| DLQ(`-dlq`, `-mqtt-dlq`) 증가 | 0 / 0 | 0 / 0 |
| `messages.received` / invalid / `ack.callback.missing` | 721 / 0 / 0 | 721 / 0 / 0 |
| 브로커 `dropped` | 0 | 0 |
| spool 드레인(`telemetry.spool.drained`) / 최종 pending | 1 / 0 | **491** / 0 |
| spool 드레인 완료 | Kafka start +24초 | Kafka start +약 25초 (13:00:25) |

성공 기준 (1)~(4) **충족(1회)**, (5) 끊김 1회 기록.

### 관찰에서 읽은 것

- **재접속 지연은 이 실행에서 나타나지 않았다.** 끊김 2초 뒤 브로커가 재접속을 받았고 `<unknown>` 타임아웃도 없었다. 이 실행에서는 수신 재개까지 약 7초(재접속 후 구독 timeout 5초 포함)였다. 실험 E에서는 약 96초였다. **각 1회 관찰이라 수정의 효과라고 단정하지 않는다.** 실험 E의 지연 원인은 미확정이었고 이번에도 원인을 확인하지 않았다. 수정(옛 콜백 스레드 인터럽트)이 지연을 없앴을 수 있다는 것은 실험 D의 소스 수준 판단과 일관된 **추정**일 뿐이며, 수정 없이 같은 지연이 다시 나타나는지(E의 재현율)도 모른다.
- **수정의 몫이 크지 않았을 수 있다.** 이번에도 끊김과 대기 만료가 1초 이내로 붙어 있었고(둘 다 120초 delivery timeout 근처), 대기 스레드가 풀린 시점의 차이는 로그 초 단위로는 분리되지 않는다. 대기가 훨씬 길게 남은 조건(예: `delivery.timeout.ms`가 더 긴 경우)에서의 효과는 보지 않았다.
- **중복 1건은 그대로 남았다.** 71번째에 해당하는 첫 메시지(`ts=…12:57:19.639Z`)가 대기 만료로 spool에 보관됐고(12:59:23, 로그 1건), 재접속 뒤 브로커가 같은 메시지를 재전달해(12:59:29) 한 번 더 spool에 들어가 Kafka에 같은 timestamp 2건(offset 106·107, 둘 다 spool 드레인)이 생겼다. 중복은 InfluxDB의 `(vehicle_id, ms)` 흡수로 720이다. 대기 중단 수정은 "ACK 없이 재전달 대기"를 만들어 중복 허용 설계와 맞지만 중복 자체를 없애지는 않는다.
- **spool 경로가 훨씬 많이 쓰였다(새 관찰).** 재접속이 Kafka 복구(12:59:57)보다 약 33초 먼저 일어나 그 사이와 이후 드레인 동안 들어온 메시지가 디스크 spool로 들어갔다(491건, E는 1건). 유실 0·최종 pending 0이었지만, 재접속이 빨라지면 Kafka 정지 구간의 수집량이 spool 디스크로 옮겨 간다는 뜻이다(spool 용량·디스크 한계는 이번에도 건드리지 않았다). 첫 실패 외 메시지는 `backlog` 플래그로 로그 없이 spool로 갔다(`TelemetryProducer` 주석·로그 1건으로 확인, 개별 메시지의 경로는 로그로 대조하지 않았다).
- 재접속 직후 구독 timeout 로그는 이번에도 났고(연결 +5초) 이후 메시지는 정상 수신됐다. 실험 F의 관찰과 같은 양상이며 새로 확인한 것은 없다.

### 한계

각 1회, 단일 차량, 저부하(약 2.7건/초), Kafka 정지 150초 하나. E와 E2는 같은 절차지만 호스트 공급 속도(약 328초 vs 270초)와 실행 시각이 달라 완전한 대조군이 아니다. 로그 시각은 초 단위라 끊김과 대기 만료의 선후는 정밀하게 분리되지 않는다. 수정 전 이미지로 E를 다시 돌려 지연 재현율을 보는 대조는 하지 않았다. 남긴 테스트 데이터: InfluxDB `OUTAGE-E2` 720행, Kafka 721건(retention 1시간).

## 실험 G — 세션 없는 재시작과 구독 (2026-10-05)

상태: **1회 관찰.** 반복·안정성 주장 없음. 원본: [`evidence/2026-10-05-mqtt-session-loss/`](evidence/2026-10-05-mqtt-session-loss/) (`G1_*`, `G2_*`, `R_*`, `00_metadata.txt`). 코드 변경 없음(HEAD `4124bb0`, `backend/src/main`은 `c41ff49` 이후 변경 0, 이미지 재사용).

**질문**: 브로커에 해당 client ID의 세션이 없는 상태로 백엔드를 시작하면 구독 timeout이 나는가, 나면 구독이 실제로 성립해 있는가.
**조작**: 브로커 볼륨은 지우지 않고 `MQTT_CLIENT_ID`만 바꿨다(저장소 밖 compose override, 종료 뒤 삭제). 브로커에는 SUBSCRIBE 기록용으로 `log_type subscribe/unsubscribe`를 더한 설정을 저장소 밖에서 마운트했다. (1) `-g1`로 기동해 새 세션·구독 확인, 10건 수신. (2) `-g1` 정지 뒤 `-g2`(브로커에 세션 전혀 없음)로 바꾸고, **기동 전부터** 다른 차량 토픽(`SESSLOSS-G2`)에 100ms 간격으로 QoS 1 발행을 계속한 채(13:08:05~13:09:38) 백엔드를 기동했다.
**성공 기준(실행 전)**: 구독 오류 로그 유무, 브로커 SUBSCRIBE 기록, 구독 성립 이후 새 메시지 도착.

### 결과

| 항목 | 값 |
| --- | --- |
| G1: 새 ID 연결·구독(브로커 로그) | `telemetry-backend-g1 1 vehicle/telemetry/#` (13:07:15), 10/10 InfluxDB 도착, 구독 오류 0 |
| G2: 브로커 로그 | 연결·구독 `telemetry-backend-g2 1 vehicle/telemetry/#`가 같은 초(13:08:44)에 기록 |
| G2: 구독 오류·`Timed out` 로그 | **0회** |
| G2: 저장된 첫 행 | 13:08:44.206 (구독 기록과 같은 초). 그 전에 발행된 약 377건은 도착하지 않음 |
| G2: 구독 이후 발행분 | payload 파일 기준 523건 = InfluxDB 고유 시점 523 = `messages.received` 523 = Kafka 저장 경로 +523. 마지막 저장 행 시각이 마지막 payload 시각(13:09:38.261)과 같음 |
| DLQ·invalid 증가 | 0 / 0 |

판정: **세션이 없는 시작에서는 구독 timeout이 나지 않았고, 구독은 성립해 이후 메시지가 전부 도착했다(1회).** 구독 실패로 데이터가 영영 안 들어오는 상태는 **재현하지 못했다.**

### 읽을 수 있는 것과 없는 것

- 구독 전에 발행된 메시지가 도착하지 않은 것은 세션도 구독도 없던 때의 정상 동작이다(브로커는 구독자가 없으면 큐에 쌓지 않는다. 발행자 PUBACK은 구독자 유무와 무관). 유실로 세지 않는다. 다만 이것이 "cleanSession=false인데 세션이 사라진 동안의 메시지는 복구되지 않는다"는 뜻이기도 하다.
- timeout이 안 난 것은 **재구독이 막힐 백로그(브로커가 쏟아낼 offline 큐)가 없었기 때문**이라는 것이 실험 F의 설명(추정)과 일치한다. 이 실험은 그 추정의 대조군에 가까우나, 백로그 없이 SUBACK이 정상적으로 처리된 것을 확인한 것일 뿐 **원인을 입증한 것은 아니다.**
- **"세션 소실 + 백로그가 동시에 있어 timeout이 나는" 경우는 만들지 못했다.** 세션이 없으면 백로그가 없으므로 이 둘은 한 번의 재시작에서 동시에 일어나기 어렵다(추정). 따라서 "timeout이 났는데 구독이 브로커에 없는" 조합은 여전히 **미검증**이다. 구독 실패 이벤트를 감지할 리스너가 없다는 점(실험 F)도 그대로다.
- 부수 관찰: 실험 뒤 **원래 ID(`telemetry-backend`)로 되돌려 기동**하자 그 세션에는 이 실험 동안의 메시지가 쌓여 있었고, 구독 timeout 로그가 **다시 났다**(13:11:08, `R_restore_backend_log.txt`). 실험 C·E·F에 이은 같은 양상이며 백로그 있는 재접속의 4번째 관찰이다. 이후 수신 확인은 이 복원 기동에서는 하지 않았다.

### 한계

1회, 단일 차량 토픽, 약 90초 발행, 100ms 간격(무부하). client ID 변경은 "브로커 볼륨 손실"과 같은 결과(세션 없음)를 만들지만 브로커 측 persistence 손상 경로 자체는 아니다. 발행자 로그의 `sending PUBLISH` 줄 수는 895로 payload 파일(900줄)과 달랐으나 마지막 저장 행이 마지막 payload와 같아 로그 집계상 차이로 보며 조사하지 않았다. 브로커 로그 설정을 바꿔(SUBSCRIBE 기록) 재생성했으므로 브로커는 실험 중 한 번 재시작됐다. 남긴 테스트 데이터: InfluxDB `SESSLOSS-G1` 10·`SESSLOSS-G2` 523행, Kafka 약 533건, 브로커에 `telemetry-backend-g1`/`-g2` 영속 세션 2개(볼륨에 남음). 종료 시 스택은 stop(down 아님) 상태이며 mosquitto 컨테이너는 임시 마운트로 생성된 채라, 다음 `up --force-recreate mosquitto` 전까지 SUBSCRIBE 로그가 켜져 있다.

## 환경·실행

- 기준 commit: `21cf99cc0bf9314f54ce206d078135078bf4a78d`, 미커밋 변경 포함.
- Windows, Docker Desktop Linux containers, Java 17.0.20, Gradle 8.7, Paho 1.2.5, Spring Integration MQTT 6.2.4.
- Mosquitto 2.0.22, 이미지 ID `sha256:199ea8ef2e35ec2b1b37e59cfd1dbae538ed4dfa4a2251a121a52215a6248a21`.
- 실행 명령(backend): `./gradlew.bat test --tests '*MqttCrashBoundaryContractTest' --no-daemon --offline --console=plain` (수정 전), `./gradlew.bat test --tests '*Mqtt*' --tests '*TelemetryProducerTest' --no-daemon --offline --console=plain` (수정 후).
- 수정 후 관련 테스트 37건 통과. 이어서 전체 Java **373건 통과, 실패 0, skip 0**, 계약 테스트 클래스 7개 실행, 7분 36초. [전체 실행 요약](evidence/2026-10-01-mqtt-ack/test-summary.txt)에 보존했다. Python·Flutter는 이번 변경 대상이 아니므로 재실행하지 않았다.
- 기존 Telemetrix 스택 및 다른 프로젝트 DB·Redis는 시작·중지·데이터 변경하지 않았다. Testcontainers의 임시 리소스만 사용했다.

## 남은 검증

- ~~실제 Kafka 완료 경계에서 전체 백엔드 프로세스 강제 종료, 재시작 후 최종 InfluxDB 대조.~~ **1회 관찰 완료(2026-10-05, 실험 C)** — 30/30 저장, 중복 1건은 저장소가 흡수. 반복·임의 시점·대량 재전달은 미검증. 재시작 직후 구독 timeout 로그 1건의 원인 확인이 새로 남았다.
- **Kafka 장기 정지 중 MQTT 연결 유지: 1회 관찰 완료(2026-10-05, 실험 E)** — 연결은 유지되지 않았고(keepalive 타임아웃, 약 120초 지점), 유실 0·중복 1(spool 보관 뒤 재전달). 반복·다른 정지 길이·고부하·복수 차량은 미검증. 재접속이 약 90초 늦은 원인은 미확정. **실험 E2(수정 후 재실행, 1회)에서는 끊김 약 2초 뒤 재접속됐고 `<unknown>` 타임아웃이 없었다 — 수정 효과인지는 단정 못 함, 원인은 여전히 미확정.**
- **재시작 직후 구독 timeout: 재현됨(실험 F, 1회)** — 백로그가 있는 재접속에서 연결 5초 뒤 발생하고 구독은 유지됐다. 세션 소실 경로에서의 구독 상실 여부, 구독 실패 이벤트 감지 수단 부재는 미검증/결함 후보.
- 네트워크 단절·재접속과 저장 지연이 겹치는 경우, 디스크 용량 부족 실스택 재현.
- 수동 ACK 변경 후 목표 부하에서 처리량·지연·큐 포화 비교. 현재는 저부하 계약만 확인했다.
- 브로커 강제 종료/호스트 전원 차단은 이번 보호 범위 밖이다.
- ELM327 실측은 연결 가능한 장비·차량 정보가 없어 미실행. [절차](../runbook/elm327-measurement.md)만 준비했다.

## 실험 D — 재접속 경계

질문: 저장 확인을 기다리는 동안 MQTT TCP 연결이 끊겼다 이어지면, 옛 연결 기준 ACK가 새 연결을 오염시키거나 메시지를 잃게 하는가.

### 소스 수준 판단 (Paho 1.2.5, Spring Integration MQTT 6.2.4)

- `MqttPahoMessageDrivenChannelAdapter$AcknowledgmentImpl.acknowledge()`는 연결 세대를 확인하지 않고 `client.messageArrivedComplete(id, qos)`를 호출한다(6.2.4 jar을 javap로 확인: `ackClient`가 null이 아니면 그대로 호출).
- 그래서 늦은 ACK는 새 연결에서 같은 packet ID의 PUBACK이 될 수 있다. 다만 Mosquitto 2.0.22는 cleanSession=false 세션의 미확인 메시지를 같은 mid로 재전송한다(브로커 로그 `m1..m3`, 재전달 헤더 `mqtt_id=1`). 늦은 ACK가 닿으면 같은 메시지의 재전송분이 ACK되는 것이지 다른 메시지가 아니다.
- 실제로 먼저 걸린 것은 ACK 오염이 아니라 **재연결 지연**이었다. Paho의 재연결(`ClientComms$ConnectBG`)은 `CommsCallback.start()`에서 옛 콜백 스레드가 끝나기를 기다린다. 핸들러가 그 콜백 스레드에서 `receipt.get(150초)`로 막혀 있으면 브로커가 살아 있어도 재연결이 멈춘다.

### 방법

`MqttReconnectAckContractTest`. 실제 Mosquitto 2.0 + 실제 Spring 어댑터(manualAcks) + 실제 `MqttMessageHandler`. 어댑터와 브로커 사이에 테스트 안의 TCP 중계기를 두고 연결만 닫아 단절을 만들었다(컨테이너 네트워크 단절·pause 대신 — 포트와 소켓 종료 시점을 통제하려고). Kafka 전송 완료만 통제한 future이고 실제 Kafka 장애가 아니다. 순서: 메시지 3건 발행 → 1번의 저장 확인이 대기 중일 때 연결 단절 → 재접속 대기(30초 상한) → 저장 완료 → 같은 client ID로 다시 붙어 재전달 3초 관찰.

### 결과

- **수정 전(결함)**: 재접속이 30초 안에 일어나지 않았다. 스레드 덤프에서 `MQTT Call`이 `MqttMessageHandler.acknowledge`의 `receipt.get`에, `MQTT Con`(재접속)이 `CommsCallback.start`의 sleep에 있었다. 대기 상한(150초)이 지나서야 재접속이 이어졌고, 그동안 2·3번 메시지는 처리되지 않았다(유실은 아님, 브로커가 보관). 같은 실패를 3회 관찰했다(상한 30초와 200초 대기 각각 포함, 테스트를 고치는 중이라 정식 반복은 아님).
- **수정**: 연결 끊김 이벤트(`MqttConnectionFailedEvent`)를 `MqttMessageHandler`가 받아, 저장 확인을 기다리는 콜백 스레드만 인터럽트한다. 그 스레드는 ACK 없이 예외로 빠져나오고 Paho가 재접속을 이어간다. `$SYS` 어댑터 이벤트는 무시한다.
- **수정 후**: 같은 테스트 1회 통과. 단절 약 4ms 뒤 대기 중단, 약 1.4초 뒤 브로커가 재접속 확인. 1번은 재전달되어 전송 시도 2회(중복), 2·3번은 각 1회, 재접속 뒤 같은 client ID로 붙었을 때 재전달 0건(저장 확인된 것은 전부 ACK됨). 단위 테스트 `MqttAcknowledgmentTest`가 대기 스레드 중단·ACK 없음·`$SYS` 무시·인터럽트 플래그 비누수를 고정한다.
- (a) 예외·크래시 없이 처리: 핸들러 예외는 Paho가 연결을 다시 끊고 재연결해 재전달하는 기존 경로로 흡수된다. (b) 유실 없음, 중복은 관찰. (c) 다른 메시지를 잘못 ACK한 흔적은 없다 — 다만 이는 "재전달 0건·2·3번 각 1회 처리"라는 관찰 범위이며, packet ID 단위 증명은 아니다.

### 한계

- 수정 후 전체 Java 375건 통과, 실패 0, skip 0(XML 67개 합, 9분). 실험 D 자체는 1회 관찰이다. 반복 3회 이상·dirty 트리라 `검증 완료`가 아니다.
- TCP 중계기로 만든 깨끗한 단절만 다뤘다. 반쯤 열린 연결(keepalive 만료까지 감지 못 함), 브로커 재시작, TLS 경로는 미검증.
- 이벤트는 어댑터가 연결 끊김을 알아챈 뒤에야 온다. 감지 전의 지연은 줄이지 못한다.
- 인터럽트 직후 레이스(저장 확인이 완료된 직후 이벤트 도착)는 중복 재전달로만 귀결되도록 설계했으나 부하에서 재현 시험은 하지 않았다.
- 실제 Kafka 지연·고부하와 겹친 경우는 미검증.