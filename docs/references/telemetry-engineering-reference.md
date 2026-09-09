# 참고 자료와 프로젝트 적용 결정

> 작성: 2026-09-09 · 확인 날짜는 항목마다 표기
>
> 이 문서는 **링크 모음이 아니다.** 각 자료마다
> **출처 → 이 프로젝트가 채택한 정책 → 코드·설정 → 검증 증거**를 잇는다.

## 이 문서를 읽는 법

**자료의 성격을 구분한다.** 같은 사실이라도 어디서 왔는지에 따라 신뢰 범위가 다르다.

| 표기 | 뜻 |
| --- | --- |
| `표준 원문` | SAE J1979 등 규격 문서 자체. **이 프로젝트는 아직 원문을 보지 못했다.** |
| `공식 제품 문서` | 그 소프트웨어를 만드는 주체가 낸 문서 |
| `제조사 설명` | 장비·도구 제조사의 기술 자료. 규격 원문이 아니다 |
| `오픈소스 구현` | 실제 구현체. 규격이 아니라 "누군가의 해석" |
| `프로젝트 선택` | 위 자료를 근거로 **우리가 정한 것**. 자료가 정해준 것이 아니다 |

**숫자와 정책의 단일 기준은 [`docs/telemetry-schema-decision-table.md`](../telemetry-schema-decision-table.md)다.**
이 문서에는 범위 숫자를 다시 적지 않는다 — 두 곳에 적으면 갈라진다.

## 확인한 버전

적용 시점에 실제로 쓰던 버전이다. **버전이 바뀌면 아래 결정을 다시 확인해야 한다.**

| 구성요소 | 버전 | 확인 방법 |
| --- | --- | --- |
| Spring Boot | 3.2.5 | `backend/build.gradle` |
| Spring Kafka | 3.1.4 | `gradlew dependencies` 해석 결과 |
| kafka-clients | 3.6.2 | 같음 |
| jackson-databind | 2.15.4 | 같음 |
| Hibernate Validator | 8.0.1.Final | 같음 |
| jakarta.validation-api | 3.0.2 | 같음 |
| influxdb-client-java | 7.0.0 | `backend/build.gradle` |
| Java | 17 | `build.gradle` toolchain |

---

## 1. 입력 계약(P0-2)에 직접 쓴 자료

### 1.1 OBD-II PID 표현 범위

| 항목 | 내용 |
| --- | --- |
| 자료 | CSS Electronics — OBD2 PID 표·변환식 (`제조사 설명`) |
| URL | https://www.csselectronics.com/pages/obd2-pid-table-on-board-diagnostics-j1979 |
| 자료 | OBD Solutions — ECUsim 5100 사용자 문서 (`제조사 설명`) |
| URL | https://www.scantool.net/scantool/downloads/64/ecusim_5100-ug.pdf |
| 확인 날짜 | 2026-09-09 (링크는 검토 과정에서 제공받았다) |
| 참고한 내용 | PID별 단위·범위·변환식. 속도(0D), RPM(0C), 냉각수 온도(05), 스로틀(11), 연료(2F), 제어 모듈 전압(42) |

**채택한 정책**: 각 필드의 허용 범위를 **해당 PID의 표현 범위**로 정했다.
숫자는 결정표에 있다.

**채택 이유**: 처음에는 "물리적으로 불가능한 값을 거부한다"는 기준으로 속도 500km/h 같은
값을 골랐는데, 그건 **이상 감지 임계값 위에 여유를 얹어 고른 것**이지 역산이 아니었다.
장치가 만들 수 있는 값의 범위를 계약으로 삼으면 근거가 명확하고, 실제 OBD-II 연동이라는
프로젝트 목표와도 맞는다.

**코드·검증**:
`backend/src/main/java/com/telemetry/domain/VehicleTelemetry.java`(제약),
`backend/src/test/java/com/telemetry/domain/TelemetryContractTest.java`(경계값·범위 밖).

**미확인·한계**:
- **SAE J1979 원문을 보지 못했다.** 위 둘은 제조사 설명이고, 서로 교차 확인한 수준이다.
- **이 범위는 물리적 한계가 아니다.** 표현 범위이고, 우리가 그것을 계약으로 채택한 것이다.
- 실제 장치가 어떤 PID를 어떤 해상도로 보내는지는 **장치를 붙여봐야 안다**.
- `battery_voltage`를 제어 모듈 전압(42)으로 정의했지만 실제 장치가 그 PID를 보낼지는 미확인.

### 1.2 ECU 전압과 동글 측정 전압의 구분

| 항목 | 내용 |
| --- | --- |
| 자료 | python-OBD — Command Tables (`오픈소스 구현`) |
| URL | https://github.com/brendan-w/python-OBD/blob/master/docs/Command%20Tables.md |
| 확인 날짜 | 2026-09-09 |
| 참고한 내용 | 센서 명령 매핑. **제어 모듈 전압과 어댑터 자체 측정 전압이 별도 명령**이다 |

**채택한 정책**: `battery_voltage`를 **제어 모듈 전압**으로 정의한다.
기존 이상 감지 룰 `11.5~15V`는 **12V 계통을 전제로 한 정책**이라고 명시한다.

**채택 이유**: 같은 이름으로 서로 다른 출처의 값이 섞이면 나중에 구분할 수 없다.
24V·48V 계통 차량에는 기존 임계값을 그대로 쓸 수 없다는 것도 같이 적어야 한다.

**코드**: `VehicleTelemetry.batteryVoltage` javadoc, 결정표.

**미확인**: 실제 장치 연동 시 어느 값을 보낼지. `프로젝트 선택`이지 자료가 정해준 것이 아니다.

### 1.3 Jackson 역직렬화 정책

| 항목 | 내용 |
| --- | --- |
| 자료 | Jackson — Deserialization Features (`공식 제품 문서`) |
| URL | https://github.com/FasterXML/jackson-databind/wiki/Deserialization-Features |
| 확인 버전 | jackson-databind **2.15.4** |
| 참고한 내용 | `FAIL_ON_UNKNOWN_PROPERTIES`, 소수→정수 변환, 스칼라 강제 변환 |

**채택한 정책**:
- telemetry 전용 decoder에서만 `FAIL_ON_UNKNOWN_PROPERTIES`를 **켠다**. 전역 Boot 매퍼는
  건드리지 않는다 — REST 응답·JWT 등 무관한 곳이 같은 매퍼를 쓴다.
- **숫자 문자열(`"87.3"`)은 허용한다.** 손실 없는 변환이라 값이 바뀌지 않는다.
- 빈 문자열은 null이 되므로 `@NotNull`이 잡는다.

**채택 이유**: 필드명 오타(`sped`)를 잡는 경로가 이것뿐이다. 다만 전역으로 켜면 이번 변경과
무관한 곳이 깨지므로 범위를 telemetry로 제한한다.

**검증**: `ValidationBehaviorProbeTest`에서 이 버전의 실제 동작을 쟀다
(숫자 문자열 → 변환, 빈 문자열 → null, boolean·배열·객체 → 거부, `NaN` 리터럴 → 파싱 실패).
**문서를 읽고 가정하지 않고 측정했다** — 처음에 `new ObjectMapper()`로 재다가
Boot 매퍼와 기본값이 달라 한 칸이 틀린 적이 있다.

### 1.4 `@Pattern`과 null

| 항목 | 내용 |
| --- | --- |
| 자료 | Jakarta Validation 3.0 — `@Pattern` (`공식 제품 문서`) |
| URL | https://jakarta.ee/specifications/bean-validation/3.0/apidocs/jakarta/validation/constraints/pattern |
| 확인 버전 | jakarta.validation-api **3.0.2** / Hibernate Validator **8.0.1.Final** |
| 참고한 내용 | `@Pattern`은 **null을 유효로 본다** |

**채택한 정책**: DTC 원소에 `@NotNull`과 `@Pattern`을 **함께** 건다.

**채택 이유**: `@Pattern`만 걸면 `[null]`이 통과한다. 그 값이 저장 단계에서
`String.join`으로 **문자열 `"null"`**이 되던 것이 이번에 막으려는 대상이다.

**검증**: `ValidationBehaviorProbeTest.pattern_은_null을_통과시킨다`(계약 자체를 실측),
`TelemetryContractTest.dtc_null_원소`(계약 적용 결과).

### 1.5 숫자 제약과 비유한 값

| 항목 | 내용 |
| --- | --- |
| 자료 | Hibernate Validator — Documentation (`공식 제품 문서`) |
| URL | https://hibernate.org/validator/documentation/ |
| 확인 버전 | **8.0.1.Final** |
| 참고한 내용 | 컨테이너 원소 제약, 중첩 검증, 숫자 제약 |

**채택한 정책**: 범위는 `@DecimalMin`/`@DecimalMax`로 건다. **검증 계층에 별도의 유한값
검사를 두지 않는다.**

**채택 이유**: 이 버전에서 **`@DecimalMin`/`@DecimalMax`가 NaN도 거부한다**는 것을 실측했다.
`±Infinity`는 범위 밖이라 당연히 걸린다. 처음에는 "NaN은 어떤 비교도 false라 안 걸릴
것"으로 예상하고 유한값 검사를 하나 더 넣으려 했는데, **재보니 필요 없었다.**

**검증**: `ValidationBehaviorProbeTest.범위제약과_비유한값`.
저장 단계의 `TelemetryRepository.finite()`는 그대로 둔다 — 검증을 거치지 않는 경로가
생기더라도 저장에서 한 번 더 막는 방어다.

### 1.6 Spring Kafka 배치 실패 처리

| 항목 | 내용 |
| --- | --- |
| 자료 | Spring Kafka — Handling Exceptions (`공식 제품 문서`) |
| URL | https://docs.spring.io/spring-kafka/reference/kafka/annotation-error-handling.html |
| 확인 버전 | spring-kafka **3.1.4** |
| 참고할 부분 | `BatchListenerFailedException`, `commitRecovered`, `setFailIfSendResultIsError` |

**채택한 정책**: **배치 전체를 먼저 검증하는 구조를 만들지 않는다.**
검증 실패는 기존 역직렬화 실패와 **같은 레코드별 try/catch**에서 DLQ로 보낸다.

**채택 이유**: 배치를 먼저 검증하다 중간에서 실패하면, 아직 저장하지 않은 앞쪽 정상
레코드가 처리 완료로 간주될 수 있다. 현재 구조는 레코드별로 DLQ 처리하고
`saveAll` 실패만 예외로 던져 배치 전체를 재시도하므로, **부분 커밋이 생기지 않는다.**

**코드**: `TelemetryConsumer.consumeForStorage`(주석에 이유를 남겼다).

**미확인**: `BatchListenerFailedException`·`commitRecovered`·`setFailIfSendResultIsError`를
**현재 설정과 한 줄씩 대조하지는 않았다.** 이번 변경이 그 경로를 건드리지 않았고
기존 계약 테스트가 통과하는 것까지만 확인했다.

### 1.7 계약 테스트 환경

| 항목 | 내용 |
| --- | --- |
| 자료 | Testcontainers — Kafka Module (`공식 제품 문서`) |
| URL | https://java.testcontainers.org/modules/kafka/ |
| 확인 날짜 | 2026-09-09 |

**채택한 정책**: 기존 Testcontainers 계약 테스트 5종을 그대로 쓴다. **새 컨테이너 제품을
늘리지 않는다.**

**검증**: 이번 변경 후에도 계약 테스트가 skip 없이 통과한다(전체 162개, skip 0).

---

## 2. 다음 고도화에서 볼 자료

**지금 구현하지 않는다.** 어떤 백로그와 이어지는지만 남긴다.

| 자료 | 성격 | 이어지는 작업 | 왜 지금이 아닌가 |
| --- | --- | --- | --- |
| [Mosquitto 설정 문서](https://mosquitto.org/man/mosquitto-conf-5.html) | `공식 제품 문서` | mTLS 인증서 identity와 ACL, persistence·queue·메시지 크기 제한 | 현재 mTLS·ACL·queue 100,000은 실측으로 정했다. 다음은 인증서 identity와 ACL을 코드 계약으로 잇는 작업이고 P0-2 범위 밖이다 |
| [InfluxDB v2 중복 point 처리](https://docs.influxdata.com/influxdb/v2/write-data/best-practices/duplicate-points/) | `공식 제품 문서` | timestamp·tag identity와 재전달·덮어쓰기 | 밀리초 충돌은 이미 실측·판단했다. 논리적 중복(다른 timestamp·같은 내용)은 아직 정의도 안 했다 |
| [Prometheus Instrumentation](https://prometheus.io/docs/practices/instrumentation/) | `공식 제품 문서` | 수신·거부·저장·DLQ 계측, label cardinality | **이번 변경으로 거부 사유가 4종으로 늘었는데 사유별 카운터가 없다.** 다음 작업 후보다 |
| [Redis GETDEL](https://redis.io/docs/latest/commands/getdel/) | `공식 제품 문서` | refresh token 회전, Redis 장애 정책(감사 P1) | **명령 문서가 fail-open/fail-closed를 정해주지 않는다.** 제품 동작과 우리가 고를 정책은 별개다 |

---

## 3. 이 문서의 한계

- **표준 원문(SAE J1979)을 읽지 않았다.** PID 범위는 제조사 설명 둘을 교차 확인한 것이다.
- 위 URL 중 다수는 **검토 과정에서 제공받은 것**이고, 이 프로젝트가 독립적으로 전문을
  읽고 대조한 것이 아니다. 대신 **동작에 영향을 주는 항목은 실측으로 확인**했다
  (`ValidationBehaviorProbeTest`) — 자료보다 이쪽이 이 프로젝트에서는 더 강한 근거다.
- 자료의 내용을 여기 옮겨 적지 않는다. 링크와 짧은 요약만 둔다.
- 버전이 올라가면 1.3~1.6의 결정을 다시 확인해야 한다. **이번 범위에 의존성 업그레이드는
  넣지 않았다.**
