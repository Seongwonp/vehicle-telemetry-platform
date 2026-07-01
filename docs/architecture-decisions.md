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
