# Architecture Decision Records (ADR)

기술 선택의 이유를 기록한다. "무엇을 썼냐"는 README에 있고, 여기는 **왜 이걸 골랐냐**에 집중한다.

---

## ADR-001: MQTT → Kafka 이중 파이프라인

**결정**: 차량 → MQTT 브로커 → Kafka → Spring Boot 구조로 연결

**이유**:
- MQTT는 IoT 기기에 최적화된 경량 프로토콜이지만, 메시지 영속성과 재처리가 없다.
- Kafka를 중간에 두면 InfluxDB 저장과 이상 감지가 같은 메시지를 **독립적으로** 소비할 수 있다.
- 브로커가 잠시 다운되어도 Kafka offset 덕분에 재시작 후 밀린 메시지를 처리할 수 있다.

**포기한 대안**:
- MQTT Consumer → 직접 InfluxDB 저장: 이상 감지 경로를 추가하려면 코드 수정이 필요, 확장성 낮음
- RabbitMQ: AMQP 기반으로 IoT 워크로드보다 엔터프라이즈 메시징에 적합, 시계열 파티셔닝 지원 부족

---

## ADR-002: Kafka Consumer Group 분리

**결정**: InfluxDB 저장(`telemetry-storage-group`)과 이상 감지(`anomaly-detector-group`)를 별도 Consumer Group으로 운영

**이유**:
- 같은 Group으로 묶으면 하나의 파티션을 단 하나의 Consumer만 읽는다. 즉, 저장 또는 이상 감지 중 하나가 메시지를 받지 못한다.
- Group을 분리하면 각 Group이 동일 토픽을 독립적으로 구독하므로 두 경로 모두 모든 메시지를 수신한다.
- Python anomaly-detector가 별도 프로세스임에도 메시지 유실 없이 연동 가능한 핵심 이유.

---

## ADR-003: vehicle_id를 Kafka 파티션 키로 사용

**결정**: `kafkaTemplate.send(TOPIC, vehicleId, payload)` — vehicle_id를 key로 전송

**이유**:
- 같은 차량의 메시지가 항상 동일 파티션에 쌓이므로, Consumer는 순서를 보장한 채로 처리한다.
- 시계열 이상 탐지(예: 엔진 온도가 3연속 상승)는 순서 보장이 필수다.
- 키 없이 라운드로빈으로 보내면 동일 차량의 메시지가 여러 파티션에 분산되어 Isolation Forest가 오동작한다.

---

## ADR-004: InfluxDB를 시계열 DB로 선택

**결정**: 센서 데이터 저장소로 PostgreSQL 대신 InfluxDB 사용

**이유**:
- PostgreSQL에 시계열 데이터를 쌓으면 행이 수억 건에 이를 때 시간 범위 쿼리가 느려진다.
- InfluxDB는 시간 기반 파티셔닝과 압축을 내장하며, Flux 쿼리로 이동평균, 집계를 간결하게 표현할 수 있다.
- `vehicle_id`를 tag로 설정하면 자동 인덱싱되어 "특정 차량의 최근 N건" 쿼리가 빠르다.

**포기한 대안**:
- TimescaleDB (PostgreSQL 확장): 기존 SQL 자산 재사용 가능하나 InfluxDB 대비 시계열 특화 기능 부족
- Elasticsearch: 검색엔진 기반으로 시계열 압축 효율이 낮고 운영 복잡도가 높음

---

## ADR-005: 이상 감지를 Python 별도 서비스로 분리

**결정**: Spring Boot 내부가 아닌 Python 독립 프로세스(`anomaly-detector`)가 이상 감지를 담당

**이유**:
- scikit-learn Isolation Forest는 Java 생태계에서 대응 라이브러리가 부족하다.
- 이상 감지 로직 변경(임계값 조정, 모델 교체)이 Spring Boot 재빌드/배포 없이 가능하다.
- Python 서비스는 Kafka를 통해 결과를 `vehicle-anomaly-alerts` 토픽으로 발행하고, Spring Boot는 이를 저장만 한다 — 관심사 분리가 명확하다.

---

## ADR-006: JWT + InMemory 사용자 (현재 단계)

**결정**: Spring Security + HMAC-SHA256 JWT, 사용자 정보는 InMemoryUserDetailsManager로 관리

**이유**:
- 포트폴리오 단계에서 DB 기반 사용자 관리는 과도한 복잡성이다.
- Stateless JWT는 Redis 세션 저장소 없이도 수평 확장이 가능하다.
- JJWT 0.12.5 라이브러리를 사용해 토큰 서명/검증/파싱을 일관성 있게 처리한다.

**알려진 한계**:
- 토큰 강제 무효화(로그아웃)를 위한 Redis 블랙리스트 필요 (현재 미구현)
- 다중 사용자 지원 시 DB 기반 UserDetailsService로 교체 필요

---

## ADR-007: Redis를 Rate Limiting과 BruteForce 감지에 사용

**결정**: IP별 카운터를 Redis에 저장

**이유**:
- 인메모리 카운터(ConcurrentHashMap)는 서버 재시작 시 초기화되고, 다중 인스턴스 배포 시 인스턴스별로 제한이 적용되어 의도한 제한이 동작하지 않는다.
- Redis의 `INCR`는 원자 연산이라 동시 요청이 쏟아져도 카운터가 정확하다.
- TTL 설정으로 만료를 Redis가 자동 관리하므로 별도 정리 스케줄러가 필요 없다.

---

## ADR-008: soft delete (active 플래그)

**결정**: `vehicles` 테이블에서 행을 물리 삭제하지 않고 `active = false`로 처리

**이유**:
- `vehicle_id`는 InfluxDB 텔레메트리 데이터의 tag로 사용된다. 행을 삭제하면 "KR-GA-1234의 과거 엔진 온도" 같은 이력 조회 시 차량 메타데이터를 찾지 못해 응답이 깨진다.
- 감사 추적(언제 등록됐는지, 비활성화됐는지)도 가능하다.

---

## ADR-009: Spring Integration MQTT (vs @KafkaListener 방식)

**결정**: MQTT 수신에 Spring Integration의 `MqttPahoMessageDrivenChannelAdapter` 사용

**이유**:
- Eclipse Paho MQTT 클라이언트를 직접 사용하면 재연결 로직, 스레드 관리, 채널 연결을 직접 구현해야 한다.
- Spring Integration이 자동 재연결, 채널 기반 메시지 라우팅을 처리하므로 `MqttMessageHandler`는 비즈니스 로직에만 집중할 수 있다.
- `@ServiceActivator(inputChannel = "mqttInputChannel")`로 채널과 핸들러를 선언적으로 연결한다.

---

## ADR-010: Refresh Token을 JWT가 아닌 Redis opaque token으로 저장

**결정**: Refresh Token은 서명된 JWT가 아니라 `UUID.randomUUID()` 문자열을 Redis에
`refresh_token:{token} → username` 형태로 저장한다 (`RefreshTokenService`).

**이유**:
- Access Token(Stateless JWT)은 발급 후 서버가 강제로 무효화할 수 없다는 게 ADR-006의 알려진 한계였다.
- Refresh Token까지 JWT로 만들면 같은 문제가 반복된다 — 서버가 값을 들고 있지 않으니 로그아웃해도 막을 방법이 없다.
- Redis에 저장하면 로그아웃 시 해당 키를 지우는 것만으로 재발급을 차단할 수 있다.
  Access Token은 여전히 만료시간까지 유효하지만, 최소한 "재발급 사슬"은 즉시 끊어진다.
- `BruteForceDetector`/`RateLimitInterceptor`와 동일하게 `StringRedisTemplate`을 재사용해 새 인프라 의존성을 추가하지 않았다.

**Rotation 적용 이유**: `rotate()`는 검증과 동시에 기존 토큰을 즉시 삭제한다.
탈취된 Refresh Token이 재사용되는 창구를 최소화하기 위함 — 정상 사용자가 먼저 재발급받으면
공격자가 들고 있던 토큰은 그 순간 무효가 된다.

---

## ADR-011: InfluxDB 비동기 배치 쓰기(WriteApi)로 전환

**결정**: `TelemetryRepository`가 `WriteApiBlocking`(단건 동기 쓰기) 대신
`WriteApi`(비동기 배치, `batchSize=500`, `flushInterval=1000ms`)를 사용하도록 변경.

**이유**:
- 단건 쓰기는 Kafka 메시지 하나마다 InfluxDB에 HTTP 요청을 하나씩 보낸다 — 차량 수가 늘어나면 InfluxDB 부하가 선형으로 증가한다.
- `WriteApi`는 내부 버퍼에 포인트를 모았다가 배치 크기 또는 시간 조건에 도달하면 한 번에 flush한다.

**트레이드오프**: 쓰기가 비동기이므로 `TelemetryRepository.save()`는 더 이상 전송 성공을 보장하지 않는다.
실제 쓰기 실패는 `InfluxDbConfig`에 등록한 `WriteErrorEvent` 리스너가 백그라운드에서 로깅하며,
`TelemetryConsumer`의 catch 블록은 더 이상 InfluxDB 네트워크 오류를 잡지 못한다(역직렬화/포인트 구성 오류만 잡음).
Kafka offset은 이미 auto-commit이 꺼져 있고 리스너가 정상 반환하면 커밋되므로, 이 부분은 배치 전환 이전과 동일한 "저장 성공을 보장하지 않는" 특성을 유지한다.

**2026-07 부하 테스트 실측 추가**: `batchSize=500` / `flushInterval=1000ms` 자체를 다른 값과
비교 측정하지는 못했다(향후 과제). 대신 부하 테스트 중 이 설정과는 무관한 별개의 심각한
버그를 발견했다 — `WritePrecision.S`(초 단위)와 시뮬레이터의 초 단위 타임스탬프가 겹쳐서,
차량 발행 주기가 1초 미만이면 같은 시리즈+타임스탬프에 여러 포인트가 충돌해 뒤 값이 앞
값을 조용히 덮어썼다(50% 데이터 유실, Kafka lag은 0으로 정상처럼 보임). ADR-014, 그리고
`docs/load-test-plan.md` 5절에 상세 기록. `WriteApi`의 비동기 특성과는 별개로, 정밀도
설정 자체가 배치 쓰기 이전부터 있던 잠재적 유실 지점이었다는 점에서 이 ADR과 관련이 깊어 여기에도 남긴다.

읽기 클라이언트도 같은 부하 테스트에서 검증했다. `/latest`의 p95가 3.82초까지 증가해
OkHttp Dispatcher의 호스트당 동시 요청 기본값 5를 200으로 확대했지만, p95 6.23초와
에러율 1.43%로 오히려 악화됐다. InfluxDB의 `context canceled`와 백엔드의
`SocketTimeoutException`을 확인해 클라이언트 대기열 뒤의 InfluxDB 동시 쿼리 처리 용량이
실제 제약임을 확인했고, 확대 설정은 원복했다. 읽기 경로 개선은 InfluxDB 스케일링·쿼리
최적화·캐싱을 별도 검증한 뒤 결정한다.

---

## ADR-012: Kafka DLQ로 저장 실패 메시지 격리

**결정**: `TelemetryConsumer`의 두 리스너가 역직렬화/저장 준비 단계에서 실패하면
원본 메시지를 각각 `vehicle-telemetry-dlq`, `vehicle-anomaly-alerts-dlq` 토픽으로 재발행한다.

**이유**:
- 기존에는 실패를 로그로만 남기고 메시지를 버렸다 — 장애 원인 파악 후에도 유실된 데이터를 복구할 방법이 없었다.
- DLQ에 원본 페이로드와 원래 key(`vehicle_id`)를 그대로 유지해 두면, 나중에 원인을 고쳐서 DLQ를 재처리하거나
  최소한 어떤 차량의 어느 데이터가 얼마나 유실됐는지 조회할 수 있다.
- 자동 재처리 컨슈머는 이번 범위에서 제외했다 — 재시도 정책(횟수 제한, 백오프)까지 설계하면 범위가 커지므로,
  우선 유실 방지와 가시성 확보까지만 구현했다.

---

## ADR-013: MQTT mTLS — 기본값은 평문 유지, 플래그로 전환 + PKCS12 변환

**결정**: `mqtt.tls.enabled`(기본 `false`)로 Spring Boot 백엔드의 MQTT 연결을 평문(`tcp://`)과
mTLS(`ssl://`) 사이에서 전환한다. `generate-certs.sh`는 Mosquitto/Python 시뮬레이터용 PEM 인증서에
더해 Spring Boot용 `client.p12`/`truststore.p12`도 함께 생성한다.

**기본값을 평문으로 유지하는 이유**:
- mTLS를 기본으로 켜면 매번 인증서를 미리 생성해둬야 `docker-compose up -d`가 성공한다 —
  포트폴리오 프로젝트에서 반복되는 로컬 개발 사이클에 불필요한 마찰을 추가한다.
- 데모/보안 점검 시에만 플래그를 켜는 구조가 "준비는 됐지만 강제하지는 않는다"는 현재 프로젝트의
  다른 보안 기능들(MQTT TLS 리스너 자체도 mosquitto.conf에서 기본 주석 처리)과 일관된다.

**PKCS12 변환이 필요한 이유**:
- `openssl req`가 만드는 개인키는 PKCS#1 PEM 형식인데, Java의 `KeyStore`/`KeyManagerFactory`는
  이를 직접 읽지 못한다(PKCS#8 또는 PKCS12만 지원).
- `openssl pkcs12 -export`로 인증서+키를 묶어 `client.p12`로 만들면 표준 `javax.net.ssl` API로 로드 가능하다.
- 트러스트스토어(`truststore.p12`, CA 인증서만 포함)는 `openssl pkcs12 -export -nokeys`로 만들면
  인증서가 `trustedCertEntry` 속성 없이 저장되어 Java `KeyStore`가 0개 항목으로 인식하는 문제가 있었다.
  `keytool -importcert`로 만들면 이 속성이 올바르게 채워진다 — 그래서 트러스트스토어만 keytool로 생성한다.

---

## ADR-014: 부하 테스트로 발견한 InfluxDB 타임스탬프 정밀도 유실 수정, Kafka concurrency 조정

**배경**: `docs/load-test-plan.md`에 따라 시뮬레이터를 스케일하며 수집 파이프라인 부하 테스트를
진행하던 중(2026-07), 원래 가설(Kafka 컨슈머 concurrency 미설정이 병목)과 다른, 훨씬 심각한
문제를 실측으로 발견했다.

**결정 1 — InfluxDB 타임스탬프를 초 단위(`WritePrecision.S`)에서 밀리초(`WritePrecision.MS`)로,
시뮬레이터 타임스탬프도 초 단위에서 밀리초 단위로 변경**.

**이유**: `PUBLISH_INTERVAL=0.5`(차량당 초당 2회 발행) 조건에서 InfluxDB 저장량을 측정했더니
목표 200 msg/s의 절반인 약 100 msg/s만 저장됐다. Kafka consumer lag은 0에 가까워 정상처럼
보였는데, 원인은 따로 있었다 — 시뮬레이터가 초 단위 타임스탬프(`%Y-%m-%dT%H:%M:%SZ`)를 보내고
백엔드가 `WritePrecision.S`로 기록하다 보니, 같은 차량의 같은 초 안에 도착한 두 번째 메시지가
InfluxDB에서 (measurement, tag, timestamp)가 완전히 같은 포인트로 취급되어 앞 메시지를 조용히
덮어썼다. `influx query`로 특정 차량의 연속 타임스탬프를 직접 확인해 초 단위로만 점이 찍히는
것을 보고 확정했다. 수정 후 동일 조건에서 저장량이 약 197 msg/s(목표의 98.5%)로 회복됐다.

**교훈**: Kafka consumer lag = 0은 "메시지를 다 소비했다"는 뜻이지 "데이터가 다 저장됐다"는
뜻이 아니다 — 저장 단계의 정합성은 별도로 검증해야 한다.

**결정 2 — `spring.kafka.listener.concurrency=3`으로 설정 (파티션 수와 일치)**.

**이유**: `vehicle-telemetry`/`vehicle-anomaly-alerts` 모두 파티션 3개인데 concurrency
미설정 시 기본값 1이라 파티션 수만큼 병렬 처리가 안 될 가능성이 있었다. 실측 결과, 이번
테스트로 도달 가능했던 부하 범위(~1,250 msg/s — 시뮬레이터 자체의 발행 한계)에서는
concurrency=1이어도 lag이 누적된 적이 없어 개선 효과가 뚜렷하지 않았다. 그럼에도 파티션
수와 컨슈머 스레드 수를 일치시키는 것 자체는 일반적으로 안전한 기본값이라 그대로 유지한다.
더 높은 처리량에서 실제 효과가 있는지는 시뮬레이터를 더 강하게 만들어야 확인 가능(향후 과제).

**결정 3(되돌림) — InfluxDB Java 클라이언트 OkHttp Dispatcher 확대 시도, 롤백**.

**이유**: REST API 부하 테스트에서 `/latest`·`/telemetry` 조회가 데이터량과 무관하게 비슷한
p95/p99를 보여 InfluxDB 클라이언트의 OkHttp Dispatcher(호스트당 동시 요청 기본값 5)가 병목으로
의심됐다. `Dispatcher`를 200/200으로 늘려 재측정했더니 p95가 3.82s→6.23s로, 에러율이
0.03%→1.43%로 오히려 악화됐고 InfluxDB/백엔드 로그에 `context canceled`,
`SocketTimeoutException`이 다수 발생했다. 즉 기본값 5는 병목이 아니라 로컬 InfluxDB
컨테이너의 실제 동시 쿼리 처리 용량을 클라이언트 쪽에서 우연히 지켜주고 있었던 것 —
풀을 넓히자 서버가 감당 못 할 요청이 한꺼번에 몰렸다. 원래 설정(기본값)으로 되돌렸다.
진짜 개선은 InfluxDB 자체 스케일링, 쿼리 최적화, 캐싱 등 더 큰 작업이 필요해 향후 과제로 남긴다.

**상세 수치**: `docs/load-test-plan.md` 5절.
