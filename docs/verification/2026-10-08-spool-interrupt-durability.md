# spool 쓰기 중 연결 끊김 인터럽트 — 2026-10-08

> 출발점: 실험 D3([`2026-10-01-mqtt-ack-boundary.md`](2026-10-01-mqtt-ack-boundary.md) §실험 D3)의 결함 후보.
> 백로그 경로에서 Paho 콜백 스레드가 spool 파일을 쓰는 중에 연결 끊김 인터럽트가 닿아 `ClosedByInterruptException` →
> 그 메시지는 ACK 안 됨, spool 볼륨에 0바이트 `.tmp` 1개 잔존(`evidence/2026-10-08-ack-wait-observability/D3_spool_dir_after.txt`).

## 조건

| 항목 | 값 |
| --- | --- |
| 기준 커밋 | `d276d84e6638153aef9cd73423d60642f5b1467b` (main) |
| 작업 전부터 dirty(이 작업과 무관, 건드리지 않음) | `docs/verification/2026-10-08-qa-account-deactivation.md`(M), `docs/verification/2026-10-08-secret-scope-mqtt-tls-store.md`(??) |
| 이 작업의 미커밋 변경 | 아래 "변경 파일" |
| 환경 | Windows 11 Pro, JDK 17.0.15, Docker 29.7.2, Testcontainers `eclipse-mosquitto:2.0` |
| 재현 위치 | JUnit `@TempDir`와 Testcontainers 브로커만. **compose 스택·`backend-spool` 볼륨은 건드리지 않았다**(볼륨의 0바이트 `.tmp`는 수정 전 증거로 그대로 있다) |
| 원본 | [`evidence/2026-10-08-spool-interrupt-durability/`](evidence/2026-10-08-spool-interrupt-durability/) — `before-fix/`(수정 전 실패 XML), `after-fix/`(수정 후 통과 XML) |

D3 백엔드 로그에서 확인한 실제 순서(`D3_backend_full_log.txt` 1201~1253행): `Lost connection` 14:53:07.712924 →
대기 중단 WARN .712948 → **다음 메시지(`…24.638Z`)의 처리 로그 .712951**(이미 대기 구간 안) → `spool 저장까지 실패` .714767,
원인 `ClosedByInterruptException` at `FileChannelImpl.write` ← `TelemetrySpool.store`(43행 = `channel.write`) ←
`TelemetryProducer.storeForRetry` ← `send`(131행 = backlog 분기) ← `MqttMessageHandler.handle`(123행). 즉 인터럽트는
`enterWait()` 뒤, `FileChannel.open` 전후에 이미 걸려 있었고 첫 `write`에서 터졌다 — 0바이트 `.tmp`와 일치한다.

## 1. 인터럽트 시점 × spool 단계 × ACK

`TelemetrySpool.store`의 단계: `createDirectories` → `FileChannel.open(CREATE_NEW)`(= 0바이트 `.tmp` 생성) → `write` 반복 →
`force(true)` → `close` → `Files.move(ATOMIC_MOVE)`(= `.json`). JDK의 `FileChannel`은 `AbstractInterruptibleChannel`이라
**`write`·`force`만** 인터럽트에 반응한다(진입 때 이미 걸려 있거나 도중에 오면 채널을 닫고 `ClosedByInterruptException`,
바이트가 이미 써졌어도 던진다). `open`·`close`·`Files.move`·`deleteIfExists`는 인터럽트로 끊기지 않는다.

| 인터럽트가 닿는 창 | store() 결과 | 남는 파일 (수정 전) | 남는 파일 (수정 후) | 핸들러 | ACK |
| --- | --- | --- | --- | --- | --- |
| `enterWait` ~ `open` 전, 또는 `open` ~ 첫 `write` (D3) | 던짐(`ClosedByInterruptException`) | **0바이트 `.tmp`** | 없음 | `get` → `ExecutionException` → `failed` | **안 나감** |
| `write` 도중 | 던짐 | 일부 또는 전체 내용 `.tmp`(rename 안 됨) | 없음 | 같음 | **안 나감** |
| `force` 도중 | 던짐 | 전체 내용 `.tmp`(fsync 미확인, rename 안 됨) | 없음 | 같음 | **안 나감** |
| `force` 뒤(`close`·`move`·반환 사이) | **정상 반환** | 완전한 `.json` | 완전한 `.json` | 이미 완료된 future라 `get`이 즉시 반환, `exitWait`가 플래그를 지움 | **영속 뒤 나감**(계약 안) |
| 동기 `send()` 중(max.block) — 백로그 아님 | `kafka.send()`가 `InterruptException`(플래그 재설정) → `sendDirect` catch → 같은 스레드에서 store → 첫 `write`에서 던짐(`ClosedByInterruptException`) | **0바이트 `.tmp`**(D3 창과 같은 store 경로 — 수정 전 이 경로로는 재지 않음) | 없음 | `failed` | **안 나감** |
| 대기 구간 밖 | — | — | — | `waitLock`으로 인터럽트 자체가 안 감(ADR-029 2026-10-06) | — |

- 실패 창에서 ACK가 나가는 경로는 없다: `storeForRetry`가 `failedFuture`를 돌려주고 `handle`은 `failure != null`이면 던진다.
  수정 후에도 실패를 그대로 던진다(삼키지 않는다).
- **Kafka 완료 대기 중**에 닿은 인터럽트(D3의 첫 메시지)는 이 표와 다르다 — spool 쓰기가 Kafka I/O 스레드에서 일어나므로
  인터럽트 대상(콜백 스레드)이 아니고, 쓰기는 끝나며 재전달분이 중복이 된다(D3에서 3중 기록).
- **동기 `send()` 중(두 번째 경로, 리뷰 지적)**: 백로그가 아니어도 Kafka가 멈춰 있으면 콜백 스레드는 `kafka.send()`의 동기 구간
  (메타데이터 대기·버퍼 할당, 상한 `max.block.ms` 10초)에서 막힌다. 여기에 연결 끊김 인터럽트가 닿으면:
  kafka-clients **3.6.2** `KafkaProducer.doSend`가 `InterruptedException`을 `org.apache.kafka.common.errors.InterruptException`으로
  바꿔 던지고, 그 생성자 3종이 모두 `Thread.currentThread().interrupt()`로 **플래그를 다시 세운다**(둘 다 `javap` 바이트코드로 확인).
  spring-kafka **3.1.4** `KafkaTemplate.doSend`는 `producer.send` 호출을 감싸지 않으므로(예외 테이블은 이미 완료된 future의 `get()`에만
  있다) 그대로 `TelemetryProducer.sendDirect`의 `catch (Exception)` → `storeForRetry`로 간다 → 플래그가 선 같은 스레드에서 store →
  `ClosedByInterruptException` → 영속 안 됨, future 실패, ACK 안 함, 회복은 브로커 재전달뿐. 이번 `.tmp` 정리가 이 경로에도 적용된다.
  **검증 범위**: 실제 `KafkaTemplate`·`TelemetryProducer`·`TelemetrySpool`·`MqttMessageHandler` + 가짜 `Producer`(메타데이터 대기 대신
  10초 래치 대기, 인터럽트되면 `new InterruptException(e)` — 즉 **KafkaProducer의 변환은 바이트코드로 확인했고 테스트는 그것을 흉내 냈다**).
  실제 KafkaProducer의 메타데이터 대기·실제 브로커 정지·실제 Mosquitto 재전달은 이 경로로 돌리지 않았다. 수정 전 코드로 이 경로를
  재지 않았다(같은 `store()` 경로라 D3 창과 같은 0바이트 `.tmp`가 남았을 것으로 보지만 추정이다).
  인터럽트가 `send()` 진입 **전**에 이미 서 있고 메타데이터가 있으면 `send()`는 막히지 않고 반환해 future 경로가 된다 — 그 경우는 재지 않았다.
- 관측: 이 경우 Timer는 `interrupted`가 아니라 `failed`로 센다(예외가 `InterruptedException`이 아님). 이번에 바꾸지 않았다.

**테스트 근거**

| 테스트 | 무엇을 고정하나 | 수정 전 | 수정 후 |
| --- | --- | --- | --- |
| `TelemetrySpoolInterruptTest#pendingInterruptFailsStoreWithoutJson_andLeavesNoTmp` | D3 창 — 인터럽트가 걸린 채 store: 던짐, `.json` 0, `.tmp` 0, 인터럽트 상태 유지, 다음 저장 정상 | **실패**(0바이트 `.tmp` 1개) | 통과 |
| `TelemetrySpoolInterruptTest#interruptAtRandomPoint_neverYieldsJsonOnFailure_norPartialJson_norLeftoverTmp` | 8MB payload, 0~30ms 무작위 인터럽트(4회에 1회는 지연 0) 최소 40회 — 회차마다 정상 반환이면 완전한 `.json` 1개, 던지면 `.json` 0, 끝에 `.tmp` 0. **던진 회차 ≥ 1을 단언**(없으면 최대 400회까지 더 돈다 — 빠른 디스크에서 정리 경로를 안 타고 통과하지 않게, 리뷰 후속) | **실패**. 분포(창 분류를 넣은 뒤 2회): `{returned:interruptAfterForce=3, returned:noInterruptSeen=25, threw=12, tmp:0byte=1, tmp:complete=11}`, `{1, 28, 11, 0byte 1, complete 10}`(그 전 2회는 분류 없는 판이고 `.tmp`를 누적 집계해 수치 생략) | 통과. 예: `{noInterruptSeen=9, threw=31}`, `.tmp` 0 |
| `MqttSpoolInterruptAckTest#interruptLandingBeforeSpoolWrite_failsWithoutAck_andLeavesNothing` | 실제 `TelemetryProducer`·`TelemetrySpool`, store 직전 `onConnectionLost` — ACK 없음, `.json` 0, `.tmp` 0, Timer `failed`=1, 플래그 비누수, 재전달 같은 메시지는 저장 뒤 ACK 1회 | **실패**(`.tmp` 1개, 다른 단언은 통과) | 통과 |
| `MqttSpoolInterruptAckTest#interruptLandingAfterSpoolRename_acksBecauseAlreadyPersisted` | store 반환 직후 `onConnectionLost` — ACK 시점에 `.json`이 이미 있음, 플래그 비누수 | 통과 | 통과 |
| `TelemetrySpoolInterruptTest#kafkaTemplateRethrowsInterruptExceptionFromBlockingSend_withFlagReSet` | 실제 `KafkaTemplate` + 막히는 가짜 Producer: 인터럽트 → `InterruptException`이 감싸지지 않고 나옴, 플래그 다시 섬 | (리뷰 후속, 수정 후에만 실행) | 통과 |
| `TelemetrySpoolInterruptTest#interruptDuringBlockingKafkaSend_fallsBackToSpoolOnFlaggedThread_failsWithoutJsonOrTmp` | 같은 구성 + 실제 `TelemetryProducer`(백로그 아님): `send()`는 던지지 않고 실패 future(원인 `ClosedByInterruptException`), 반환 뒤에도 플래그 섬, `.json` 0, `.tmp` 0 | (수정 후에만) | 통과 |
| `MqttSpoolInterruptAckTest#interruptWhileBlockedInSynchronousKafkaSend_spoolFallbackFails_noAck_noFiles` | 핸들러까지: send 동기 구간 진입 확인 뒤 `onConnectionLost` — ACK 없음, `.json`·`.tmp` 0, Timer `failed`=1, 다음 작업에 플래그 비누수 | (수정 후에만) | 통과 |

무작위 테스트에서 `tmp:partial`은 한 번도 나오지 않았다(일반 파일의 `write` 한 번이 전부 쓰므로). 부분 `.tmp`는 이론상 창이지
관찰하지 못했다. 분포는 Windows/NTFS 3회 값이고 Linux(CI)에서는 다를 수 있다 — 테스트는 분포가 아니라 불변식만 단언한다.

## 2. 실패한 메시지의 실제 재전달과 최종 저장

`MqttReconnectAckContractTest#interruptDuringSpoolWriteIsNotAckedThenRedeliveredAndSpooledOnce`(기존 계약 클래스에 메서드로 추가 —
계약 결과 파일 수 8 유지).

- 구성: **실제 Mosquitto 2.0**(Testcontainers) + 실제 Spring `MqttPahoMessageDrivenChannelAdapter`(manualAcks, cleanSession=false,
  QoS 1) + 실제 `MqttMessageHandler`·`TelemetryProducer`·`TelemetrySpool`(`@TempDir`). 단절은 실험 D와 같은 테스트 내 TCP 중계기.
  Kafka는 `send`가 동기적으로 던지는 mock — 모든 정상 메시지가 콜백 스레드에서 spool로 간다(D3의 백로그 경로와 같은 스레드).
- 인터럽트 시점 고정: 첫 `store()`가 중계기를 끊고, 연결 끊김 이벤트가 콜백 스레드에 인터럽트를 걸 때까지 `parkNanos`로
  기다린 뒤(플래그를 지우지 않음) 실제 쓰기를 한다. **자연 발생 타이밍이 아니라 훅으로 만든 창이다.** 이 계약 테스트가 덮는 것은
  **"쓰기 전에 플래그가 이미 선" 창 하나**다. `write`·`force` 도중 도착은 단위 무작위 테스트(§1)가, 동기 `send()` 중 경로는 §1의
  단위 테스트가 덮는다 — 둘 다 실제 Mosquitto 재전달까지는 돌리지 않았다.
- 관찰(수정 후, `after-fix/TEST-com.telemetry.contract.MqttReconnectAckContractTest.xml`):
  `821ms delivered seq=1 dup=false` → `846ms MqttConnectionFailedEvent` → `847ms interrupted=true` →
  `851ms first store failed: ClosedByInterruptException` → `854ms handler threw … 저장 확인 실패 — ACK하지 않음` →
  `2179ms delivered seq=1 dup=true` → `2199ms store ok` → `2200ms handler acked`.
  `deliveries=[false, true] json=1 tmp=0 pubackFromClient=1 redeliveredAfterResume=[]`.
- 단언: 첫 실패 원인 = `ClosedByInterruptException`; 전달 = [DUP 아님, DUP] — 브로커가 PUBACK을 받지 못해 재전달했다;
  최종 spool `.json` = 1건이고 내용이 seq 1; 같은 client ID로 다시 붙어
  3초 동안 재전달 0; `.tmp` 0. (단언 아님, 출력만: 브로커 로그의 이 client `Received PUBACK` = 1 — 재전달분 하나.)
- 수정 전: 같은 테스트 2회 실행, 둘 다 **`.tmp` 단언에서만 실패**(`tmp=1`), 나머지(재전달·PUBACK 1·`.json` 1·재전달 0)는 같았다.

**검증된 범위**: "깨끗한 TCP 단절로 연결 끊김 이벤트가 온 상태에서, 인터럽트가 동기 spool 쓰기에 닿으면 → 그 메시지는 PUBACK되지
않고 → 재접속 뒤 DUP로 재전달되어 → spool에 정확히 1건 영속되고 그 뒤 ACK된다"를 실제 Mosquitto에서 수정 전 2회·수정 후 단독 1회와
전체 실행 1회 관찰했다. **검증하지 않은 것**: 실제 Kafka(재전달분이 Kafka·InfluxDB까지 가는 것은 D3 1회 관찰뿐), 수신 큐가 찬
상태(D2식 인지 지연), 반쯤 열린 연결·브로커 재시작·TLS, 브로커 큐 상한 초과·세션 만료(재전달 자체가 없어지는 경우), 자연 타이밍.
"ACK가 안 나갔으니 유실 없음"은 일반 명제로 쓰지 않는다 — 인터럽트된 메시지는 영속되지 않았고 **브로커 세션이 메시지를 보관해
재전달하는 동안에만** 회복된다.

## 3. 재시작 시 남은 `.tmp` (0바이트·잘린 내용)

`TelemetrySpoolInterruptTest#leftoverZeroByteAndPartialTmp_areIgnoredByStartupAndDrain_andNotCountedNorDeleted` — 수정 전·후 모두 통과
(현재 동작의 고정).

- `pending()`·`depth()`가 `.json`만 거르므로 0바이트·잘린 `.tmp`는 **무시된다**: 예외 없음, depth에 안 셈, 드레인 대상 아님,
  `quarantine`(`.corrupt`) 대상도 아님, **지우지 않는다.**
- 같은 디렉터리의 정상 `.json`은 `initializeBacklog` → `retryPending`으로 Kafka에 보내지고 지워진다. `telemetry.spool.drained`=1,
  `telemetry.spool.corrupt`=0, `telemetry.spool.pending` 게이지 0, `.tmp` 두 개는 그대로.
- 수정 후에도 `.tmp`가 남을 수 있는 경우: `open`과 `move` 사이의 **프로세스 강제 종료·전원 차단**, 또는 `deleteIfExists` 자체의
  실패(실패 원인에 suppressed로 붙이고 WARN). 이들은 정리 주체가 없다 — 기동 시 정리(c안)는 하지 않았다(아래 트레이드오프).

## 4. 반복

`TelemetrySpoolInterruptTest#repeatedInterruptedStores_doNotAccumulateTmp_andNormalStoresStillWork` — 인터럽트된 저장 200회, 사이사이 정상 저장 4회.

- 수정 전: `failures=200 tmpLeft=200 json=4` — **실패 1회당 0바이트 `.tmp` 1개가 쌓였다.** 정상 저장·`depth()`(=4)·`pending()`(=4,
  `.json`만)은 영향이 없었다. 비용은 디렉터리 항목: `pending()`·`depth()`가 `Files.list`로 `.tmp`까지 전부 훑은 뒤 거르므로 `.tmp`
  하나가 스캔마다 항목 하나를 더한다(0바이트라 데이터 블록은 없고 inode·디렉터리 항목만). 스캔 시간 증가는 재지 않았다.
- 수정 후: `failures=200 tmpLeft=0 json=4`. 실패 수는 그대로다(인터럽트된 쓰기는 여전히 실패하고 ACK되지 않는다).
- 실스택에서의 빈도는 연결 끊김 이벤트 1회당 최대 1건이다(인터럽트는 대기 중인 스레드 하나에만 간다). 추정이며 재지 않았다.

## 수정

`backend/src/main/java/com/telemetry/kafka/TelemetrySpool.java` — `store()`에서 `.tmp`를 만든 뒤(`open` 성공 뒤)의
`write`·`force`·`move` 중 어느 것이 실패하든 **자기 `.tmp`만 `deleteIfExists`하고 원래 예외를 그대로 던진다.** `open` 자체가
실패하면 지우지 않는다(만든 적 없는 파일). 삭제 실패는 원래 예외에 suppressed로 붙이고 WARN. 성공 경로·`.json`·드레인은 바뀌지 않았다.
`MqttMessageHandler`는 `onConnectionLost` Javadoc 한 줄("저장 경로는 그대로 진행되므로 … 유실이 아니다")만 범위에 맞게 고쳤다(동작 변경 없음).

**택한 것과 이유 — (b) 실패 시 `.tmp` 삭제만**
- 확인된 결함은 "인터럽트로 쓰기가 실패한다"가 아니라 **"실패한 쓰기의 잔재를 아무도 치우지 않는다"**다. 실패 자체는 계약을
  지킨다(영속 안 됨 → ACK 안 함 → 재전달). (b)는 그 잔재를 인터럽트뿐 아니라 디스크 가득 참 등 모든 쓰기 실패에서 막는다.
- **계약 영향 없음**: ACK 시점·조건은 그대로다. `.json`은 손대지 않는다. 지우는 것은 `.json`으로 옮겨지지 않은 자기 `.tmp`뿐이다.

**택하지 않은 것**
- (a) 인터럽트 불가 쓰기: 플래그를 비웠다가 복원하는 방식은 쓰기 **도중** 도착하는 인터럽트를 못 막아 창을 좁힐 뿐이다.
  `FileOutputStream`+`getFD().sync()`(인터럽트 불가 I/O)는 창을 닫지만, 그러면 영속 → 옛 연결에 ACK 시도 → 재전달 → **중복**이 된다.
  지금은 같은 경우가 "영속 안 됨 → 재전달 → 1회 저장"이다. 둘 다 계약 안이고, 첫 시도 영속이 브로커 재전달 의존을 줄이는 장점은
  있지만 I/O 경로 교체라 "최소 변경"을 넘는다. 재전달이 사라지는 조건(세션 만료·큐 상한)이 중요해지면 다시 본다.
- (c) 기동 시 오래된 `.tmp` 정리: (b) 뒤에 남는 `.tmp`는 강제 종료·전원 차단 잔재뿐이고 드레인·depth에 영향이 없다(§3). 이
  저장소는 증거 파일을 지우지 않는 규칙이 있고, 실제 볼륨의 D3 `.tmp`가 바로 그 증거다. 필요해지면 `.tmp`만, 나이 기준으로, 개수 지표와 함께 별도로 검토한다.

**대가**: 인터럽트된 메시지는 여전히 첫 시도에 영속되지 않는다 — 회복은 브로커 재전달에 달려 있다. `.tmp`가 지워지므로 실패한
쓰기의 흔적은 디스크가 아니라 로그(`spool 저장까지 실패 — MQTT ACK 금지`)와 Timer `failed`에만 남는다.

## 전체 테스트

`cd backend && ./gradlew test --no-daemon` (수정 후 트리, BUILD SUCCESSFUL 4분 41초): `build/test-results/test/*.xml` **72개, tests 435, failures 0, errors 0, skipped 0** (기존 428 + 신규 7). 계약 패키지 결과 파일 **8개**(CI 강제값 그대로, `MqttReconnectAckContractTest` tests 2), 계약 skipped 0. 이 실행의 세 클래스 XML은 `after-fix/full-run/`. 무작위 테스트 분포(이 실행): `{returned:interruptAfterForce=1, returned:noInterruptSeen=18, threw=21}`, `.tmp` 0.
**리뷰 후속 뒤 다시 전체 실행**(신규 3건 + 무작위 테스트 보강 + Javadoc·문서, BUILD SUCCESSFUL 7분 48초): XML **72개, tests 438, failures 0,
errors 0, skipped 0**, 계약 패키지 결과 파일 **8개**·skipped 0. 무작위 테스트(이 실행): `iterations=40 {returned:interruptAfterForce=1,
returned:noInterruptSeen=17, threw:ClosedByInterruptException=22}`(단독 실행 1회: `{1, 15, 24}`). 세 클래스 XML은 `after-review-full-run/`.
아래 Javadoc 문단은 리뷰 후속에서 다시 고쳤고 이 실행에 포함됐다.
`MqttMessageHandler`의 Javadoc 수정은 전체 실행이 컴파일을 마친 뒤에 넣었다(그 실행의 `compileJava UP-TO-DATE`) — 주석만 바뀌었고 뒤에 `./gradlew compileJava` 통과를 따로 확인했다.

## 변경 파일 (미커밋)

- `backend/src/main/java/com/telemetry/kafka/TelemetrySpool.java` — 수정
- `backend/src/main/java/com/telemetry/mqtt/MqttMessageHandler.java` — Javadoc만
- `backend/src/test/java/com/telemetry/kafka/TelemetrySpoolInterruptTest.java` — 신규(6건, 리뷰 후속 2건 포함)
- `backend/src/test/java/com/telemetry/mqtt/MqttSpoolInterruptAckTest.java` — 신규(3건, 리뷰 후속 1건 포함)
- `backend/src/test/java/com/telemetry/contract/MqttReconnectAckContractTest.java` — 메서드 1건 추가(클래스 수 그대로)
- `docs/verification/2026-10-01-mqtt-ack-boundary.md` — D3 절 표현을 검증 범위로 좁힘, 후속 링크
- `docs/architecture-decisions.md` — ADR-029 2026-10-05 추가의 "유실은 없고" 정정, 2026-10-08 추가 두 번째 절
- `docs/verification/evidence/2026-10-08-spool-interrupt-durability/` — 수정 전·후 테스트 XML

## 남은 것

- 실스택(실제 Kafka pause + dynsec 끊김)에서 수정 후 재현은 하지 않았다. D3 조건에서 인터럽트가 spool 쓰기에 닿는 것은 자연 타이밍이라 재현이 보장되지 않는다.
- Timer가 이 경우를 `failed`로 세는 분류(`interrupted`가 아님).
- `docs/HANDOFF_2026-10-08.md` §3-1의 "재전달로 1회 저장(유실 없음)"은 이 문서의 범위로 읽어야 한다(인계 문서는 고치지 않았다).
- 반복·안정성 주장 없음: 계약 테스트는 수정 전 2회·수정 후 2회(단독·전체) 관찰이다.
