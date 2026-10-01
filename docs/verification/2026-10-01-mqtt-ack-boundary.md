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

## 환경·실행

- 기준 commit: `21cf99cc0bf9314f54ce206d078135078bf4a78d`, 미커밋 변경 포함.
- Windows, Docker Desktop Linux containers, Java 17.0.20, Gradle 8.7, Paho 1.2.5, Spring Integration MQTT 6.2.4.
- Mosquitto 2.0.22, 이미지 ID `sha256:199ea8ef2e35ec2b1b37e59cfd1dbae538ed4dfa4a2251a121a52215a6248a21`.
- 실행 명령(backend): `./gradlew.bat test --tests '*MqttCrashBoundaryContractTest' --no-daemon --offline --console=plain` (수정 전), `./gradlew.bat test --tests '*Mqtt*' --tests '*TelemetryProducerTest' --no-daemon --offline --console=plain` (수정 후).
- 수정 후 관련 테스트 37건 통과. 이어서 전체 Java **373건 통과, 실패 0, skip 0**, 계약 테스트 클래스 7개 실행, 7분 36초. [전체 실행 요약](evidence/2026-10-01-mqtt-ack/test-summary.txt)에 보존했다. Python·Flutter는 이번 변경 대상이 아니므로 재실행하지 않았다.
- 기존 Telemetrix 스택 및 다른 프로젝트 DB·Redis는 시작·중지·데이터 변경하지 않았다. Testcontainers의 임시 리소스만 사용했다.

## 남은 검증

- 실제 Kafka 완료 경계에서 전체 백엔드 프로세스 강제 종료, 재시작 후 최종 InfluxDB 대조.
- 네트워크 단절·재접속과 저장 지연이 겹치는 경우, 디스크 용량 부족 실스택 재현.
- 수동 ACK 변경 후 목표 부하에서 처리량·지연·큐 포화 비교. 현재는 저부하 계약만 확인했다.
- 브로커 강제 종료/호스트 전원 차단은 이번 보호 범위 밖이다.
- ELM327 실측은 연결 가능한 장비·차량 정보가 없어 미실행. [절차](../runbook/elm327-measurement.md)만 준비했다.
