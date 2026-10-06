# MqttKafkaAckContractTest CI flaky — 2026-10-07

상태: **코드로 확인한 대기 결함은 고쳤다 / CI 실패 원인은 증명하지 못했다.** 로컬 재현 0회.

- 기준 커밋: `bc3751ee6dc961ff38d395f83c65aa04be2261a4` (수정은 이 위의 미커밋 작업 트리)
- 환경: Windows 11 Pro 10.0.26200, Docker Desktop(Engine 29.7.2, WSL2 커널 6.18.33.2), JDK 17, Gradle 8.7
- 라이브러리: spring-integration-mqtt 6.2.4, Paho mqttv3 1.2.5, eclipse-mosquitto:2.0, cp-kafka 7.6.1

## 관찰된 것

CI run `37387388419` attempt 1(커밋 `3eda5bf`)에서 한 번 실패했고 attempt 2는 통과했다.

```
MqttKafkaAckContractTest > compareNormalDeliveryAndRecoverKafkaOutageThroughSpool() FAILED
    org.eclipse.paho.client.mqttv3.MqttException at MqttKafkaAckContractTest.java:90
```

90행은 정상 전달 반복문의 `publisher.publish(...).waitForCompletion(10000)`이다.
**reason code는 모른다.** Gradle 콘솔은 예외 클래스와 위치만 찍고, CI는 test-results XML을
artifact로 올리지 않는다. 몇 번째 반복, 몇 번째 메시지에서 실패했는지도 남아 있지 않다.

## 코드 분석

### 1. 2~4회차의 구독 대기는 이미 참이었다 — 확인

```java
adapter.start();
await().atMost(10s).until(() -> BROKER.getLogs().contains("Received SUBSCRIBE from normal-ack"));
```

`getLogs()`는 컨테이너 기동부터 지금까지의 전체 로그다. 1회차에서 그 줄이 한 번 찍히면
2~4회차의 조건은 `start()` 직후 첫 평가에서 바로 참이 된다. **현재 반복의 구독을 기다리지 않는다.**
반복문 뒤 Kafka pause 구간의 `adapter.start()`에는 대기가 아예 없었다.

### 2. stop()/start() 사이에 일어나는 일 (6.2.4 바이트코드 확인)

- `doStop()`: `cleanSession=false`이면 **unsubscribe하지 않고** `disconnectForcibly`만 한다.
- `doStart()`: 새 클라이언트로 `connect()`하고 CONNACK을 기다린다. 구독은 대개 Paho 콜백
  스레드의 `connectComplete()`에서 `subscribe()`로 따로 나간다. 따라서 **`start()`가 돌아온 시점에
  SUBSCRIBE가 아직 브로커에 도착하지 않았을 수 있다.** 즉 대기 없이 publish가 먼저 나갈 수 있다.
- 다만 브로커 쪽 세션이 살아 있다. 진단 실행 1회의 브로커 로그에서 2회차 이후 재접속 4번이
  모두 `Sending CONNACK to normal-ack (1, 0)`(session present)였다.

### 3. 이 경합만으로 발행자 MqttException이 나는가 — 아니라고 판단한다

- 발행자(`pub-normal`)의 PUBACK은 QoS 1에서 **브로커가** 보낸다. 구독자가 구독 전이든,
  접속 전이든 상관없다. 세션이 살아 있으므로 메시지는 `normal-ack` 큐에 쌓였다가 전달된다.
- 발행자 옵션은 Paho 기본값이다(maxInflight 10, keepAlive 60초). 발행은 한 건씩
  `waitForCompletion`으로 순차 대기하므로 inflight는 최대 1이다. `32202`(inflight 초과)는 나올 수 없다.
- `mosquitto.conf`의 `max_inflight_messages 20`은 브로커→구독자 방향이다. 발행자 PUBACK과 관계없다.
- 남는 후보: `32000`(10초 안에 PUBACK 없음) 또는 `32109`(연결 끊김). 이는 호스트 부하나 Docker
  포트 프록시 쪽 원인과 맞지만 **증거가 없다.**

결론: **대기가 현재 반복에 묶여 있지 않은 것은 코드로 확인된 테스트 결함이다. 하지만 그것이
CI의 90행 예외를 일으켰다는 경로는 찾지 못했다.** 이 문서는 둘을 섞지 않는다.

## 재현 시도 (수정 전)

```
./gradlew test --no-daemon --tests com.telemetry.contract.MqttKafkaAckContractTest --rerun-tasks
```

**10회 중 0회 실패.** 실행마다 XML을 보관했다(`before-run01~10.xml`).

| 회차 | 결과 | 자동1 / 수동1 / 수동2 / 자동2 (ms) |
| ---: | --- | --- |
| 1 | 통과 | 1237 / 527 / 511 / 245 |
| 2 | 통과 | 857 / 533 / 497 / 222 |
| 3 | 통과 | 2167 / 817 / 491 / 214 |
| 4 | 통과 | 1088 / 512 / 510 / 222 |
| 5 | 통과 | 1077 / 539 / 417 / 257 |
| 6 | 통과 | 1124 / 533 / 558 / 237 |
| 7 | 통과 | 1123 / 543 / 575 / 260 |
| 8 | 통과 | 929 / 463 / 525 / 279 |
| 9 | 통과 | 1023 / 498 / 378 / 199 |
| 10 | 통과 | 868 / 527 / 527 / 222 |

시간 열은 참고용이다. 성능 결론을 내리지 않는다.

진단 실행 1회(`probe-run.xml`)는 테스트 끝에 브로커 로그를 임시로 출력했다. 이 출력은 되돌렸다.
이 1회에서는 5번의 접속 모두 `Received SUBSCRIBE` → `Sending SUBACK`이 해당 회차 첫
`Received PUBLISH from pub-normal`보다 앞에 기록됐다. 즉 이 환경에서는 경합이 실제로 벌어지지 않았다.
(mosquitto 로그는 초 단위라 시간 간격은 재지 못했다. 순서만 본다.)

## 수정

`backend/src/test/java/com/telemetry/contract/MqttKafkaAckContractTest.java`만 바꿨다.
**운영 코드는 바꾸지 않았다** — 운영 결함의 증거가 없다.

- `startAndAwaitSubscription(adapter)`: `start()` 전에 `Sending SUBACK to normal-ack` 개수를 세고,
  그 개수가 늘어날 때까지 기다린다. 기다리는 대상을 `Received SUBSCRIBE`에서 SUBACK으로 바꿨다 —
  브로커가 구독을 설치하고 응답한 뒤다.
- 반복문 4회와 Kafka pause 직전 `start()` 모두 이 함수를 쓴다.
- 발행 건수, 모드 순서, 기대 값(400 / 401건, spool 0)은 그대로다.

## 수정 후

- 해당 클래스 단독: **5회 중 0회 실패**(`after-run01~05.xml`), 각 skipped 0.
- 전체 `./gradlew test --no-daemon` 1회: XML 67개, **tests 382, failures 0, errors 0, skipped 0**.
  contract 패키지 결과 파일 **8개, skipped 0**(CI 조건과 같다).

원본: [evidence/2026-10-07-mqtt-ack-contract-flake/](evidence/2026-10-07-mqtt-ack-contract-flake/)

## 한계

- **CI 실패의 원인은 증명되지 않았다.** reason code가 없고 로컬에서 재현되지 않았다.
  이번 수정은 코드로 확인한 대기 결함을 닫은 것이지 CI 실패를 고쳤다는 증거가 아니다.
- 같은 실패가 다시 나도 지금 CI 설정으로는 reason code를 볼 수 없다. 테스트 실패 시
  test-results XML을 artifact로 올리거나 `testLogging.exceptionFormat = 'full'`로 바꾸는 것은
  이번 범위 밖이다(파일을 건드리지 않았다).
- 로컬은 Windows + Docker Desktop(WSL2) 한 대다. GitHub 러너(Linux, 2 vCPU 수준)의 부하·타이밍과 다르다.
- 수정 전 10회, 수정 후 5회는 "이 환경에서 드물다"는 것 이상을 말하지 못한다.
