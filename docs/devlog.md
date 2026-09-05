# 개발 일지 (Dev Log)

> 날짜별 작업 내용, 결정 사항, 공부한 것, 막힌 부분을 기록한다.

---

## 2026-01-05

### 프로젝트 기획 시작

현대오토에버 공채 준비하면서 백엔드 포트폴리오가 필요하다고 생각했다.
단순 CRUD 프로젝트는 너무 흔하고, 모빌리티 IT 기업에 맞는 주제를 고민하다가
차량 실시간 텔레메트리 플랫폼을 떠올렸다.

**기획 방향**
- OBD-II 동글 기반 실제 차량 데이터를 수집하는 백엔드
- 실시간 파이프라인 + 이상 감지 + 모니터링까지 End-to-End로 구성
- 단순히 돌아가는 것뿐 아니라 왜 이 기술을 선택했는지 설명할 수 있어야 함

**사용할 기술 스택 초안**
- 데이터 수신: MQTT (IoT 업계 표준)
- 메시지 큐: Kafka
- 백엔드: Java 17 + Spring Boot 3
- 시계열 DB: InfluxDB
- 이상 감지: Python (룰 기반 + 머신러닝)
- 모니터링: Grafana + Prometheus

아직 Kafka랑 MQTT는 써본 적 없어서 공부가 필요하다.

---

## 2026-01-10

### Kafka 개념 공부

Kafka 공식 문서랑 유튜브 강의 보면서 핵심 개념 정리.

**이해한 것**
- Topic: 메시지를 분류하는 채널 (폴더 같은 개념)
- Partition: Topic 내부를 나눈 것, 병렬 처리 단위
- Consumer Group: 같은 그룹 내에서는 파티션을 나눠 처리 → 한 파티션을 여러 컨슈머가 동시에 읽지 않음
- Offset: 각 Consumer가 어디까지 읽었는지 기록

**헷갈렸던 부분**
Consumer Group이 왜 필요한지 처음엔 이해를 못했는데,
"같은 메시지를 다른 목적으로 여러 번 처리하고 싶을 때 그룹을 분리한다"고 이해했다.
이 프로젝트에서 저장 Consumer와 이상감지 Consumer가 같은 메시지를 각자 처리해야 하므로
그룹 분리가 핵심이 될 것 같다.

**참고**
- Kafka 파티션 키: 같은 키는 항상 같은 파티션으로 → 순서 보장
- 이 프로젝트에서 `vehicle_id`를 키로 쓰면 차량별 순서 보장 가능

---

## 2026-01-18

### MQTT 개념 공부 + OBD-II 조사

**MQTT**
- IoT에 최적화된 경량 pub/sub 프로토콜
- AMQP(RabbitMQ)보다 오버헤드 적음 → 배터리 기기나 차량 동글에 적합
- QoS 3단계: 0(최대 1회), 1(최소 1회), 2(정확히 1회)
- QoS 1이 "성능 vs 신뢰성" 균형상 적합할 것 같다

**OBD-II**
- 1996년 이후 출시된 차량에 의무 탑재
- ELM327 동글을 OBD-II 포트에 꽂으면 차량 데이터 읽기 가능
- python-obd 라이브러리로 접근 가능
- 지원 PID: 속도, RPM, 엔진 온도, 스로틀, 연료량 등

지금은 시뮬레이터로 개발하고, 나중에 실제 동글로 전환하는 구조로 만들어야겠다.

---

## 2026-01-26

### 프로젝트 구조 설계 + CLAUDE.md 작성

기술 스택 공부가 어느 정도 됐으니 실제 프로젝트 구조를 설계했다.

```
vehicle-telemetry-platform/
├── simulator/          # Python 차량 시뮬레이터
├── broker/             # Mosquitto 설정
├── kafka/              # 토픽 초기화 스크립트
├── backend/            # Spring Boot API
├── anomaly-detector/   # Python 이상 감지
├── monitoring/         # Grafana + Prometheus
└── docker-compose.yml
```

Phase 구분:
1. 데이터 파이프라인 (MQTT → Kafka → InfluxDB)
2. REST API + 인증
3. 이상 감지
4. 보안 강화
5. 모니터링 & 배포

CLAUDE.md 파일도 작성했다 — AI 협업 시 지켜야 할 원칙, 차량 데이터 스펙, 이상 감지 룰 정의.

**차량 데이터 스펙 확정**
```json
{
  "vehicle_id": "KR-GA-1234",
  "speed": 87.3,
  "rpm": 2400,
  "engine_temp": 92.1,
  "throttle_position": 34.5,
  "fuel_level": 67.0,
  "battery_voltage": 13.8,
  "gps": { "lat": 37.123456, "lng": 127.654321 },
  "dtc_codes": []
}
```

---

## 2026-02-04

### Phase 1 시작: Docker Compose 환경 구성

드디어 코드 작성 시작. 먼저 인프라 컨테이너부터.

**작성한 파일**
- `docker-compose.yml`: Mosquitto, Zookeeper, Kafka, InfluxDB, PostgreSQL, Redis
- `.env.example`: 전체 환경변수 목록
- `.gitignore`: .env, 인증서, 빌드 산출물 제외

**결정 사항: Kafka Zookeeper vs KRaft**
KRaft(Kafka 내장 합의 프로토콜)가 최신이지만 예제와 문서가 아직 Zookeeper 기반이 많다.
학습 편의를 위해 Zookeeper 방식으로 진행.

**헬스체크 설정 이유**
처음에 healthcheck 없이 `depends_on`만 써서 Kafka가 준비되기 전에 Spring Boot가 뜨는 문제가 있었다.
healthcheck + `condition: service_healthy` 조합으로 해결.

---

## 2026-02-10

### Mosquitto MQTT 브로커 설정

`broker/config/mosquitto.conf` 작성.

처음에 `allow_anonymous true`로 설정했다가 나중에 보안 파트에서 X.509 인증서 인증으로 바꾸려면
설정 파일을 다시 건드려야 한다는 게 번거로울 것 같아서,
TLS/X.509 설정을 주석으로 미리 써두고 나중에 주석만 해제하면 되도록 구성해뒀다.

Phase 1에서는 개발 편의상 1883 포트(평문) 사용, Phase 4에서 8883 포트(TLS)로 전환 예정.

---

## 2026-02-17

### Kafka 토픽 초기화 스크립트

`kafka/init-topics.sh` 작성.

| 토픽명 | 파티션 | 용도 |
|--------|--------|------|
| `vehicle-telemetry` | 3 | 차량 센서 원본 데이터 |
| `vehicle-anomaly-alerts` | 3 | 이상 감지 결과 |
| `vehicle-dtc-events` | 1 | DTC 진단 코드 (단건 처리라 파티션 1개) |

파티션을 3개로 정한 이유: 처음에 1개로 했다가 Kafka는 파티션 수를 줄이지 못한다는 걸 알았다.
나중에 차량이 늘어날 것을 고려해 3개로 시작. 늘리는 건 가능하니까.

---

## 2026-02-24

### Spring Boot 프로젝트 초기화

`backend/` 디렉토리에 Spring Boot 3.2.5 프로젝트 세팅.
빌드 도구는 Gradle 8.7.

**Spring Integration MQTT를 쓴 이유**
Eclipse Paho MQTT 클라이언트를 직접 쓰면 재연결 로직, 스레드 관리 등을 직접 구현해야 한다.
Spring Integration 쓰면 `@ServiceActivator`로 채널에 핸들러를 연결하기만 하면 된다.
처음엔 이 어노테이션이 낯설었는데, "Spring Integration 채널에 이 메서드를 연결해"라는 뜻이다.

**주요 의존성**
- `spring-integration-mqtt`: MQTT 수신
- `spring-kafka`: Kafka 연동
- `influxdb-client-java`: InfluxDB 쓰기
- `lombok`: 보일러플레이트 제거

---

## 2026-03-05

### MQTT → Kafka 파이프라인 구현

`MqttMessageHandler.java`, `TelemetryProducer.java` 작성.

**vehicle_id를 Kafka 파티션 키로 쓰는 이유**
키 없이 라운드로빈으로 보내면 같은 차량 메시지가 다른 파티션에 쌓인다.
이상 감지(Python)에서 "엔진 온도가 3틱 연속 상승" 같은 시계열 패턴을 보려면
같은 차량 메시지가 순서대로 와야 한다. 키를 `vehicle_id`로 지정하면 해결된다.

**막힌 부분**
`kafkaTemplate.send()`가 비동기라 에러를 바로 잡지 못했다.
`CompletableFuture.whenComplete()`로 콜백을 달아 전송 성공/실패를 로그로 남기도록 수정했다.

---

## 2026-03-12

### Kafka → InfluxDB 저장 구현

`TelemetryConsumer.java`, `TelemetryRepository.java` 작성.

**Consumer Group 두 개 쓰는 이유**
처음에 하나의 Consumer Group으로 묶었더니 `consumeForStorage`만 메시지를 받고
이상 감지 Consumer는 아무것도 못 받는 문제가 생겼다.
Kafka는 같은 그룹 내에서 파티션을 나눠주기 때문이다.
그룹을 분리(`telemetry-storage-group`, `anomaly-detector-group`)해서 각자 모든 메시지 수신.

**InfluxDB tag vs field**
`vehicle_id`를 tag로 쓰는 이유를 이해하는 데 시간이 걸렸다.
InfluxDB에서 tag는 인덱싱, field는 인덱싱 안 됨.
"KR-GA-1234의 데이터만 조회"하는 쿼리가 tag 필터는 빠르고, field 필터는 풀스캔이다.
당연히 vehicle_id는 tag로 써야 한다.

**트러블슈팅**
| 문제 | 원인 | 해결 |
|------|------|------|
| timestamp 파싱 오류 | Python 시뮬레이터가 ISO-8601 아닌 포맷으로 전송 | 시뮬레이터에서 `datetime.now(timezone.utc).isoformat()` 사용 통일 |

---

## 2026-03-20

### Python 차량 시뮬레이터 구현

`simulator/vehicle_simulator.py` 작성.

**설계 포인트**
- `VehicleState.next()`: 60초 사이클로 가속→순항→감속 자연스러운 패턴
  - 엔진 온도: 냉간(20°C)에서 워밍업 후 90°C 안정
  - RPM: 속도와 연동되되 급변 없도록 스무딩
  - 연료: 매 틱마다 미세 소모
- `inject_anomaly()`: `ANOMALY_RATE` 확률로 이상값 주입 (테스트용)
- MQTT 토픽: `vehicle/telemetry/{vehicle_id}`
- 여러 차량을 스레드로 동시 실행

**실제 OBD-II 전환 시**: `next()` 안의 랜덤 계산 부분만 python-obd 라이브러리 호출로 교체하면 됨.

---

## 2026-03-28

### Phase 1 통합 테스트

드디어 `docker-compose up -d`로 전체 스택 올리고 실제 데이터 흐름 확인.

```
Python 시뮬레이터 → MQTT → Spring Boot → Kafka → InfluxDB 저장 확인
```

InfluxDB UI(`localhost:8086`)에서 `vehicle_telemetry` 측정값에 데이터 쌓이는 거 직접 확인했다.

**아직 안 된 것**
- Spring Boot 서버가 띄워지긴 하는데 API 엔드포인트가 없음
- Phase 2 REST API 구현 필요

---

## 2026-04-05

### Phase 2 시작: 차량 관리 API

`Vehicle` 엔티티, `VehicleRepository`, `VehicleService`, `VehicleController` 작성.

**soft delete 방식**
처음엔 `DELETE FROM vehicles WHERE vehicle_id = ?`로 물리 삭제하려 했는데,
`vehicle_id`가 InfluxDB 텔레메트리 데이터의 태그로 연결되어 있다.
행을 지우면 "이 차량의 과거 센서 데이터"를 조회할 때 차량 메타데이터를 못 찾는다.
`active = false` 플래그로 비활성화하는 soft delete로 변경.

**`@Transactional(readOnly = true)` 클래스 레벨**
조회 메서드에 `@Transactional` 붙이는 걸 자꾸 빠뜨려서,
클래스 레벨에 `readOnly = true`를 달고 쓰기 메서드만 `@Transactional`로 오버라이드하는 방식 채택.
JPA 더티체킹도 스킵되어 조회 성능도 약간 올라간다고 한다.

---

## 2026-04-10

### JWT 인증 구현

`JwtTokenProvider`, `JwtAuthenticationFilter`, `SecurityConfig`, `AuthController` 작성.

**JWT를 선택한 이유**
세션 기반이면 Redis에 세션 저장소가 필요하고, 서버 여러 대 운영 시 세션 공유 문제가 생긴다.
JWT는 Stateless라 서버 어디에서든 검증 가능. 현재 단일 서버라도 확장성 고려.

**`@Component` 없이 new로 주입하는 이유**
처음에 `JwtAuthenticationFilter`에 `@Component`를 붙였더니
Spring Security 필터체인에 한 번, 서블릿 컨테이너에 한 번, 총 두 번 실행되는 버그가 있었다.
`SecurityConfig`에서 직접 `new JwtAuthenticationFilter()`로 생성해서 `addFilterBefore`에 넘기는 방식으로 해결.

**HMAC-SHA256 키 길이 오류**
처음에 짧은 secret 문자열을 써서 `WeakKeyException` 발생.
HMAC-SHA256은 256bit(32바이트) 이상 키가 필요하다. `.env`에서 긴 랜덤 문자열 사용.

---

## 2026-04-14

### Rate Limiting 구현 (Redis 기반)

`RateLimitInterceptor.java` 작성.

인메모리 Map으로 구현하면 서버 재시작 시 초기화되고 다중 인스턴스에서는 제대로 안 된다.
Redis의 `INCR`(원자 연산)으로 IP별 카운터 관리.

**TTL 설계에서 실수**
처음에 요청마다 `expire`를 호출했는데, 그렇게 하면 매 요청마다 1분 윈도우가 리셋되어
사실상 제한이 걸리지 않는 버그가 있었다.
첫 요청 시(`count == 1`)에만 TTL을 설정하도록 수정.

---

## 2026-04-18

### Swagger 연동 + 텔레메트리 조회 API

`SwaggerConfig.java`, `TelemetryQueryService.java`, `TelemetryController.java` 작성.

**Flux 쿼리의 pivot 필요성**
InfluxDB는 기본적으로 필드마다 별도 행을 반환한다.
`speed` 행, `rpm` 행, `engine_temp` 행... 이걸 타임스탬프 기준으로 묶어야
하나의 레코드(한 시점의 모든 센서 값)가 된다. `pivot`이 그 역할.

처음엔 왜 행이 여러 개 나오는지 이해를 못해서 Flux 문서를 한참 봤다.

**`range(start: -1h)` 가드**
이게 없으면 전체 기간을 스캔해서 데이터가 많아지면 OOM이 날 수 있다.
항상 시간 범위 필터를 먼저 걸어야 한다.

---

## 2026-04-25

### Phase 2 완료 점검

Swagger UI(`localhost:8080/swagger-ui.html`)에서 전체 API 직접 테스트.

| 테스트 항목 | 결과 |
|-------------|------|
| POST /api/auth/login → JWT 발급 | 정상 |
| Authorization 헤더 없이 API 호출 → 401 | 정상 |
| 차량 등록/목록/단건 조회/비활성화 | 정상 |
| 텔레메트리 최근 20건 조회 | 정상 |
| Rate Limit 초과 → 429 | 정상 |

---

## 2026-05-01

### Phase 3 시작: 룰 기반 이상 감지

`anomaly-detector/rules.py` 작성.

**이상 감지 룰 정의**
| 항목 | 조건 | 심각도 |
|------|------|--------|
| 엔진 온도 | > 105°C | HIGH |
| RPM | > 6000 | HIGH |
| 배터리 전압 | < 11.5V | MEDIUM |
| 배터리 전압 | > 15.0V | HIGH |
| 속도 | > 200km/h | HIGH |
| DTC 코드 | 배열 비어있지 않음 | HIGH |

룰을 하드코딩 if-else로 쓰는 대신, `(필드명, 조건함수, 설명, 심각도)` 튜플 리스트로 관리.
새 룰 추가 시 리스트에 한 줄만 추가하면 된다.

---

## 2026-05-03

### Isolation Forest 머신러닝 이상 감지

`anomaly-detector/ml_detector.py` 작성.

**Isolation Forest를 선택한 이유**
- 라벨 없는 데이터에서 비지도 학습으로 이상 감지 가능
- 차량 데이터는 "정상" 데이터가 압도적으로 많고 "이상" 데이터는 극히 드물어 불균형 — 이런 경우에 적합
- `contamination=0.05`로 "5%는 이상으로 본다"는 가정을 설정

**초기 200샘플 수집 후 학습 시작하는 이유**
데이터가 너무 적으면 모델이 정상/이상 경계를 못 잡는다.
처음 200개는 정상 데이터 위주로 쌓이므로 이 기반으로 학습하고 이후부터 판단하는 방식.

**룰 기반 + ML 상호 보완 구조**
- 룰은 명확한 임계값 초과 → 즉시 HIGH 판단
- ML은 어떤 단일 필드도 임계값을 안 넘는데 전체 패턴이 이상한 경우 감지

---

## 2026-05-05

### Phase 3 Kafka 연동 + Spring Boot 저장

`anomaly_detector.py`(Kafka Consumer + 발행), Spring Boot 쪽 `AnomalyService`, `AnomalyAlert` 엔티티 작성.

**Python → Kafka → Spring Boot 흐름**
```
Python이 vehicle-anomaly-alerts 토픽에 이벤트 발행
↓
Spring Boot TelemetryConsumer가 anomaly-storage-group으로 소비
↓
PostgreSQL anomaly_alerts 테이블에 저장
↓
GET /api/vehicles/{id}/anomalies 로 조회 가능
```

이상 감지 테스트:
```bash
ANOMALY_RATE=0.5 python simulator/vehicle_simulator.py
```
시뮬레이터에서 50% 확률로 이상값을 주입하게 해서 테스트.

---

## 2026-05-07

### Phase 4: 보안 강화

**추가한 보안 항목**

| 항목 | 내용 |
|------|------|
| MQTT TLS 준비 | `broker/certs/generate-certs.sh` 작성, mosquitto.conf에 주석으로 TLS 설정 준비 |
| BruteForce 차단 | `BruteForceDetector.java` — 5회 로그인 실패 시 15분 IP 차단 (Redis) |
| 감사 로그 (MDC) | `RequestLoggingFilter.java` — traceId, IP, HTTP 상태코드, 응답시간 기록 |
| 보안 헤더 | X-Frame-Options, HSTS, Content-Type-Options, Referrer-Policy |
| 인증 오류 메시지 | "아이디 또는 비밀번호 불일치"로 통일 (user enumeration 방지) |

**MDC(Mapped Diagnostic Context)를 쓴 이유**
요청이 여러 개 동시에 들어오면 로그가 섞여서 어떤 요청의 로그인지 구분이 안 된다.
MDC로 traceId를 쓰레드 로컬에 저장하면 하나의 요청 흐름을 추적할 수 있다.
응답 헤더에도 `X-Trace-Id`를 넣어서 클라이언트도 추적 가능.

**보안 자체 점검 보고서 작성**
`docs/security-report.md` — OWASP Top 10, UN R155 기준으로 점검.

---

## 2026-05-08

### Phase 5: 모니터링 설정

**Prometheus + Grafana 구성**
- `monitoring/prometheus/prometheus.yml`: Spring Boot Actuator 엔드포인트 수집
- `monitoring/grafana/provisioning/`: 데이터소스 자동 연결 (Prometheus, InfluxDB)
- `monitoring/grafana/dashboards/vehicle-telemetry.json`: 차량 센서 대시보드
- `monitoring/grafana/dashboards/backend-metrics.json`: 서버 JVM/요청 대시보드

Grafana 접속: `http://localhost:3000`

**Spring Boot Actuator + Prometheus**
`management.endpoints.web.exposure.include=health,info,prometheus` 설정으로
Prometheus가 `/actuator/prometheus` 엔드포인트를 30초마다 수집.

`docs/deployment-guide.md` 작성 — AWS EC2 배포 단계별 가이드.

---

## 2026-05-09

### 코드 품질 개선 + 문서 정리

Phase 1~5 전체 구현 완료 후 코드 전체를 다시 훑으며 마무리.

**개선한 것**
- WHY 주석 추가: 코드만 봐서는 이유를 알 수 없는 결정들 (QoS 1, TTL 설계, tag vs field, Consumer Group 분리 등)
- 로그 개선: 장애 발생 시 원인 추적에 필요한 정보가 로그에 포함되도록
- JUnit 5 + Mockito 테스트 코드 작성 (VehicleService, JwtTokenProvider, BruteForceDetector, VehicleController)
- pytest 테스트 코드 작성 (rules, ml_detector, vehicle_simulator)
- `docs/db-schema.md`: PostgreSQL + InfluxDB + Redis 스키마 문서
- `docs/architecture-decisions.md`: 기술 선택 이유 ADR 9개

**전체 Phase 완료 현황**

| Phase | 내용 | 상태 |
|-------|------|------|
| 1 | 데이터 파이프라인 (MQTT → Kafka → InfluxDB) | 완료 |
| 2 | REST API + JWT + Swagger + Rate Limiting | 완료 |
| 3 | 이상 감지 (룰 기반 + Isolation Forest) | 완료 |
| 4 | 보안 강화 (TLS, BruteForce, 감사로그) | 완료 |
| 5 | 모니터링 (Grafana + Prometheus) | 완료 |
| - | AWS EC2 배포 | 진행 예정 |

**향후 개선 예정**
- JWT 토큰 블랙리스트 (로그아웃 처리)
- IDOR 완전 차단 (사용자-차량 소유 검증)
- InfluxDB 배치 쓰기 (현재 단건 쓰기라 차량 수 늘면 부하)
- Dead Letter Queue (저장 실패 시 유실 방지)

---

## 2026-07-01

### 코드 감사 + Phase 6~9, 11 구현

CSAT_Forge(실서비스 운영 중인 다른 포트폴리오 프로젝트)와 나란히 대표작으로 내세우기로 하면서,
"완료"로 표시된 Phase 1~5가 실제로 문서를 뒷받침하는지 코드를 직접 감사했다.

**감사 중 새로 발견한 것 (기존 문서엔 없던 실제 버그)**
- `/actuator/prometheus`가 인증에 걸려있어 Prometheus가 매번 401만 받고 있었다 — Phase 5 "완료" 표시와 달리
  실제로는 Grafana에 백엔드 메트릭이 안 쌓이고 있었을 가능성이 높다.
- `/actuator/health`의 `show-details: always`가 익명 사용자에게 DB/Redis 연결 상태를 노출하고 있었다.
- `GlobalExceptionHandler`가 `AccessDeniedException`/`HttpMessageNotReadableException`/`DataIntegrityViolationException`을
  처리하지 않아 스프링 기본 에러 응답으로 새는 경로가 있었다.
- Flutter 앱(`vehicle-telemetry-app`)이 이미 `/api/auth/refresh`를 호출하고 있는데 백엔드엔 그 엔드포인트 자체가 없었다.
- **`./gradlew`가 레포에 한 번도 커밋된 적이 없었다.** README는 `./gradlew test`를 안내하는데 실제로는 실행 불가능한 상태 —
  지금까지 테스트를 IntelliJ에서만 돌렸다는 뜻이고, 그래서 `VehicleControllerTest`/`BruteForceDetectorTest`의
  잠재 버그(아래)도 발견되지 못했었다.

**Phase 6 — 버그 픽스**: 위 Actuator/예외처리 문제 수정.

**Phase 7 — Refresh Token + 로그아웃 무효화**: `RefreshTokenService`를 새로 만들어 Redis에 opaque token을
`refresh_token:{token} → username` 형태로 저장(TTL 14일). `POST /api/auth/refresh`(rotation), `POST /api/auth/logout`
추가. Access Token은 여전히 Stateless라 즉시 무효화는 못 하지만, 로그아웃하면 최소한 재발급 사슬은 끊긴다.
자세한 설계 이유는 `docs/architecture-decisions.md`의 ADR-010.

**Phase 8 — 파이프라인 안정성**: `TelemetryRepository`를 `WriteApiBlocking`(단건) → `WriteApi`(비동기 배치)로 전환.
`TelemetryConsumer`의 저장 실패 메시지를 `vehicle-telemetry-dlq`/`vehicle-anomaly-alerts-dlq` 토픽으로 옮기도록 변경.
ADR-011, ADR-012 참고.

**Phase 9 — AI 진단**: Gemini API 연동. WebClient/WebFlux 의존성을 새로 추가하는 대신 JDK 내장
`java.net.http.HttpClient`로 단발성 블로킹 호출만 처리하는 `DiagnosisService` 추가.
`GET /api/vehicles/{vehicleId}/diagnosis` — Flutter 앱의 `diagnosis_screen.dart`가 이미 기대하던 응답 형태
(`{diagnosis, dataPoints}`)를 그대로 맞췄다.

**Phase 11 — 테스트**: `RefreshTokenService`, `TelemetryConsumer`(저장/DLQ 분기), `GlobalExceptionHandler`,
`AnomalyService` 테스트 추가. 그리고 `./gradlew wrapper`로 빠진 wrapper 스크립트를 생성해 커밋 — 이제 클론 후
바로 `./gradlew test`가 된다.

**테스트 작성 중 발견한 기존 버그 2개**
- `VehicleControllerTest`(`@WebMvcTest`) — `WebMvcConfig`가 등록한 `RateLimitInterceptor`가 슬라이스 테스트에도
  같이 실행되는데 `StringRedisTemplate` mock이 없어서 `NoSuchBeanDefinitionException`으로 컨텍스트 로딩 자체가 실패하고 있었다.
- `BruteForceDetectorTest` — 공용 `@BeforeEach`에서 `opsForValue()`를 스텁했는데 `recordSuccess`는 그걸 안 쓰다 보니
  Mockito strict stubs가 "사용되지 않은 스텁"으로 그 테스트를 실패시키고 있었다. 둘 다 `./gradlew test`가
  한 번도 안 돌아봐서 아무도 몰랐던 문제 — 각 테스트에 필요한 스텁만 개별로 넣는 방식으로 고쳤다.

다중 사용자/IDOR 완전 차단은 이번엔 스코프에서 뺐다 — 지금은 admin 단일 계정이라 실질 위험이 낮다.

### Phase 10 — MQTT mTLS 실제 활성화

당일 이어서 진행. `mqtt.tls.enabled` 플래그(기본 false)로 평문/mTLS를 전환할 수 있게
`MqttConfig.java`에 SSL 소켓 팩토리 로직 추가.

**막힌 부분 1**: `broker/certs/generate-certs.sh`를 실제로 실행해본 게 이번이 처음이었다 —
`openssl x509 -req ... -quiet` 옵션이 `x509` 서브커맨드엔 없는 옵션이라(`req`에만 있음)
2단계(서버 인증서 생성)에서 항상 실패하고 있었다. 지금까지 아무도 이 스크립트를 돌려본 적이
없었다는 뜻. `-quiet` 제거로 수정.

**막힌 부분 2**: openssl이 만드는 개인키(PKCS#1)를 Java가 못 읽어서, Spring Boot용으로
`client.p12`(인증서+키)를 추가로 생성하도록 스크립트를 확장했다. 처음엔 CA 인증서만 담은
트러스트스토어도 `openssl pkcs12 -export -nokeys`로 만들었는데, `keytool -list`로 확인해보니
항목이 0개로 나왔다 — openssl이 만드는 cert-only PKCS12는 `trustedCertEntry` 속성이 없어서
Java `KeyStore`가 인식을 못 하는 것. `keytool -importcert`로 바꾸니 정상적으로 1개 항목이 잡혔다.
자세한 이유는 ADR-013.

**검증**: `openssl s_server`/`s_client`로 생성된 server/client 인증서 간 실제 TLS 1.2 mutual auth
핸드셰이크가 되는지 직접 확인(둘 다 `verify return:1`). Java 쪽은 별도 클래스로 `KeyStore` →
`KeyManagerFactory`/`TrustManagerFactory` → `SSLContext` 빌드까지 생성된 `client.p12`/`truststore.p12`
파일로 직접 돌려서 예외 없이 `SSLSocketFactory`가 만들어지는 것까지 확인.
Docker가 로컬에 안 떠 있어서 Mosquitto 컨테이너까지 붙인 완전한 end-to-end 테스트는 못 했다 —
다음에 실제로 켤 때 `docker-compose restart mosquitto` 후 백엔드 로그에서 연결 확인 필요.

기본값은 여전히 평문(1883) — 데모나 보안 점검 때만 `.env`에서 `MQTT_TLS_ENABLED=true` +
`MQTT_PORT=8883`으로 바꾸고 mosquitto.conf TLS 섹션 주석을 해제하면 켜지는 구조.

---

## 2026-08-07

### 12시간 soak test — Kafka lag이 0인데 데이터가 사라지고 있었다

이상 감지 서비스를 3인스턴스로 띄우고 12시간을 돌렸다. 목적은 "장시간 안정적인가"였는데,
정작 얻은 건 **저장 경로의 무음 유실 버그**였다.

증상이 지독했다. `telemetry-storage-group`의 Kafka lag은 12시간 내내 낮게 유지됐다.
지표만 보면 완벽히 정상이다. 그런데 InfluxDB 쓰기는 **테스트 시작 26초 만에 멈춰** 있었다.

원인은 `TelemetryConsumer`의 catch 하나였다. InfluxDB 쓰기 실패가 역직렬화 실패와 **같은
catch에 묶여** 있어서, DB 장애 중에도 원본을 DLQ로 옮기고 offset을 커밋해버렸다.
컨슈머 입장에선 "처리 완료"라 lag이 안 쌓인다. 조용히 12시간치가 날아갔다.

**여기서 배운 것**: lag이 0이라는 건 "잘 처리되고 있다"가 아니라 "offset이 전진했다"일
뿐이다. 두 문장은 전혀 다른데 그동안 같은 뜻으로 읽고 있었다.

이어서 세 개를 더 고쳤다.
- 쓰기 타임아웃 미지정 + Kafka 정적 멤버십 부재 → poll 초과 시 rebalance livelock
- 에러 핸들러가 기본값(10회 재시도)이라 처리량이 무너짐 → bounded retry로 명시
- 재시도 소진 시 recoverer가 없어 **로그만 남기고 버려지던** 경로 → DLQ recoverer 추가

세 번째는 첫 번째 수정이 없애려던 바로 그 유형의 유실이 다른 경로로 남아 있던 것이다.
하나를 고치면 같은 모양의 다른 구멍이 보인다.

---

## 2026-08-31

### 처리량이 300배 무너져 있었는데 아무도 몰랐다

soak test 후속으로 실부하를 다시 걸었더니 저장 처리량이 **8.2 msg/s**였다. 목표의 0.3%다.
그런데 `docker stats`로 본 InfluxDB는 CPU 1.66%, 메모리 91MB — **놀고 있었다.**
"DB가 느리다"가 아니라 **요청당 I/O 대기** 신호다.

범인은 2026-08-04의 보안·신뢰성 수정이었다. 그때 `WriteApi`(비동기 배치) →
`WriteApiBlocking`(단건)으로 바꿨는데, 그 대가를 **재측정하지 않았다.** 메시지마다
HTTP 요청 1회 + WAL fsync 1회 + Kafka 커밋 1회가 발생하고 있었다.

배치로 묶어 **8.2 → 약 9,600 msg/s**로 회복했다. 내구성 보장은 그대로 두고 건당
오버헤드만 없앤 것이라, 원래 수정의 목적은 하나도 훼손하지 않았다.

수집 경로에도 같은 모양이 있었다. MQTT 수신마다 spool 파일을 먼저 쓰고 있어서
**20 msg/s**였다 — 정상 경로에서는 디스크를 안 거치고 실패했을 때만 spool하도록 바꿔
**9,600 msg/s**가 됐다.

**여기서 배운 것**: "건당 호출 오버헤드가 지배한다"는 패턴이 이 프로젝트 전체에 반복된다.
InfluxDB 쓰기, spool 쓰기, 나중엔 sklearn `predict()`까지 전부 같은 모양이었다.
그리고 **성능에 영향을 주는 수정에는 재측정이 딸려야 한다** — 안 그러면 두 달을 모른다.
자세한 내용은 ADR-019.

---

## 2026-09-03

### ML 이상 감지 — 세 번 추측했고 세 번 다 틀렸다

ML을 켜자 처리량이 25배 떨어졌다. 원인을 세 번 짚었는데 세 번 다 측정이 뒤집었다.

1. **"재학습이 병목일 것"** → 아니었다. `fit`이 207.5ms인데 `predict(단건)`가 11.5ms라,
   500건 처리 시 **predict가 전체의 97%** 였다. 그런데 predict는 500건을 한 번에 넣어도
   비용이 1건과 사실상 같다(16.84ms). 배치화하니 303 → 4,550 msg/s.
2. **배치화하자 그제서야 재학습이 병목이 됐다.** 건수 기준이라 처리량이 오르니 빈도도
   같이 폭주했다(초당 2.67회 = 코어의 55%). 시간 하한 60초를 두어 **8,436 msg/s**.
3. **탐지 품질을 재려다 더 근본적인 걸 발견했다.** ML 알림이 처리 건수의 5.71%인데
   이게 `contamination=0.05` 설정값과 일치했다. `contamination`은 탐지율이 아니라
   **표시할 비율**이다 — 정상 차량만 있어도 항상 5%가 알림으로 뜬다.
   즉 "이상 감지"가 아니라 **"가장 바깥쪽 5% 표시"** 였다.

그다음 탐지 품질을 채점하려니 **정답이 없었다.** 시뮬레이터가 주입하던 이상값이 전부
룰 임계값과 대응해서, 정답이 곧 "룰이 잡는 것"이었다. 룰이 못 잡는 복합 패턴 4종과
정답 로그(`[GT]`)를 시뮬레이터에 추가하고 나서야 채점이 가능해졌다.

채점하니 복합 이상 recall 52%였고, 패턴별로 갈라보니 한 유형(`throttle_no_response`)만
**6.9%** 로 유독 낮았다. 원인은 튜닝이 아니라 **모델이 `throttle_position`을 아예 안
보는 것**이었다 — "스로틀을 밟는데 차가 안 나간다"를 스로틀 없이 보면 공회전과 구별이 안 된다.

**여기서 배운 것**: 평균은 원인을 감춘다. 52%라는 숫자만 봤으면 파라미터를 만졌을 텐데,
패턴별로 가르니 파라미터로는 못 고치는 문제가 드러났다.

---

## 2026-09-04

### ML 오탐의 진짜 원인 — 내가 만든 회귀였다

피처를 추가하니 그 유형이 6.9% → **99.6%** 가 됐다. 진단이 맞았다.
그런데 **오탐이 2.4배로 늘었다.** 원인을 스로틀 분산과 학습 버퍼 오염으로 추정했다.
**둘 다 틀렸다.**

진단 방법을 바꿨다 — **이상을 하나도 주입하지 않은 부하**를 걸면 ML 알림이 전부 오탐이라
정답 조인 없이 오탐률을 직접 잴 수 있다. 그렇게 재니 정상만 있는데 판정률이 24.44%였고,
재학습 로그를 경계로 **모델 세대별**로 가르자 원인이 한 곳에 몰려 있었다.

```
세대 0 (학습 표본   201건)  34,796건 판정  91.01%   ← 여기
세대 1~4 (학습 표본 2,000건) 128,150건 판정  6.1~7.6%
```

`min_samples`(200)로 학습한 최초 모델이 **정상 트래픽의 91%를 이상이라 찍고**,
34,796건을 판정할 때까지 교체되지 않았다. 원인은 내가 재학습 폭주를 잡으려고 넣은
`retrain_min_seconds=60`이 워밍업에도 걸린 것이다. 그 전에는 500건마다 재학습해
1초 만에 교체됐다. **처리량을 살린 수정이 탐지 품질에 낸 구멍**이었고,
알림'률'만 보고 **"알림이 언제 뜨는가"** 를 안 봐서 두 달 가까이 못 봤다.

워밍업 중에는 표본이 2배가 될 때마다 재학습하도록 고쳐 **24.44% → 6.55%**.
그러고 나서야 깨끗한 기준선 위에서 점수 임계값 재설계를 할 수 있었고,
recall 77.4% → **89.1%**, 알림 정확도 36.4% → **57.4%** 로 **둘이 동시에** 올랐다.

### 신뢰성 — 유실은 없지만 복구가 실용적이지 않다

CLAUDE.md 우선순위대로 세 가지를 실측했다.

**강제 종료·재전달**: `docker kill`로 백엔드를 죽이자 재전달 68건이 발생했는데
InfluxDB 행 수가 고유 키 수와 정확히 같았다(77,929 = 77,929). 포인트 identity가
(measurement, `vehicle_id`, ms)라 **덮어쓰기로 흡수**된다. 다만 이건 "중복이 안 생긴다"가
아니라 "덮어쓰기라서 무해하다"다 — identity가 같은데 값이 다르면 조용히 지워진다.

**DLQ 재처리**: 도구를 만들다 두 번 뒤집혔다. 먼저 DLQ 레코드에 **실패 원인이 없어서**
"다시 넣으면 되는 실패"와 "몇 번을 넣어도 실패하는 메시지"를 가를 수가 없었다.
헤더를 붙이고 나서는, **커서 없이 DLQ를 처음부터 읽어 되돌리는 순진한 구현이
실행할 때마다 레코드를 배로 늘렸다**(2 → 4 → 8 → 16 → 30). Kafka는 레코드를 지울 수
없다는 걸 Runbook에 적어놓고도 설계에 반영하지 못했다.

**장애 주입**: InfluxDB와 Kafka를 각각 90초 정지시켰다. **둘 다 유실 0**이고,
DLQ 재처리로 84,615 → 161,356(토픽 수와 정확히 일치) 완전 복구를 확인했다.
그런데 복구 경로가 둘 다 실용적이지 않았다.
- 90초 장애에 트래픽의 **47.6%가 DLQ행** — 재시도 예산이 3회/약 2초라 현실 장애보다 짧다.
- spool 드레인이 **19 msg/s** — 유입(1,700 msg/s)의 1/89라 90초 장애가 35분 복구를 만든다.

그리고 이 측정이 **바로 전날 만든 DLQ 도구의 버그를 잡았다.** 분류기가 Spring의
wrapper 예외(`ListenerExecutionFailedException`)만 보고 있어서 76,878건이 전부
`unknown`으로 분류됐다 — 진짜 원인은 `cause-fqcn`에 있었다. 장애를 주입하지 않았으면
도구가 실전에서 안 먹히는 상태로 남았을 것이다.

**이 시기 전체에서 배운 것**: 세 작업 모두 "설계한 대로 되지 않는 지점"을 측정이 잡아냈다.
메트릭 스냅샷 경합으로 재전달이 -270이 나오고, DLQ가 지수적으로 늘고, spool 경로를
잘못 봐서 한때 유실로 오판했다. 숫자가 이상할 때 **설명될 때까지 결론을 내지 않는 것**이
매번 결과를 살렸다. -270과 `_value`는 결과로 채택될 수 없는 값이었다.

---

## 2026-09-05

### 내가 만든 것이 세 번 틀렸고, 세 번 다 측정이 잡았다

우선순위 1·2·3번의 남은 갈래를 전부 닫은 날이다. 결과보다 **틀린 지점**이 더 남는다.

**MQTT 브로커 장애 — 72%가 사라지고 있었다.**
이 시나리오는 "정답 기준이 없어서" 계속 미뤄져 있었다. 브로커가 죽으면 그 아래 단계가
전부 비어 기준이 될 수 없고, 시뮬레이터는 발행 성공 건수를 세지 않았다.

기준을 만드는 데 측정의 절반이 들었고, **두 번 틀렸다.**
처음엔 시뮬레이터를 `docker rm -f`로 죽여서 5초 묵은 주기 로그를 읽었다 —
정답 기준이 backend 수신량보다 **작게** 나왔다. 기준이 관측치보다 작으면 기준이 아니다.
두 번째로 `publish()`가 `NO_CONN`을 반환한 건을 유실로 적었더니 `confirmed`가
`queued`보다 **크게** 나왔다. paho 소스를 열어보니 QoS 1 메시지는 `clean_session`과
무관하게 재연결 시 재전송된다 — 유실이 아니라 재전송 대상이었다.

기준이 서고 나서 나온 숫자는 컸다. 브로커가 PUBACK한 179,532건 중 backend에는
**50,087건만** 도착했다. 원인은 우리 쪽이었다 — `setMaxReconnectDelay`를 지정하지
않아 Paho 기본값 **128초**가 적용됐고, 그동안 브로커가 우리 세션 앞으로 큐잉하다
`max_queued_messages`(10,000)를 넘기면 **말없이 버렸다.** 재연결 후 받은 건 10,002건,
큐 크기와 정확히 같다. 상한을 5초로 낮추니 유실 0.

내가 계산한 유실(129,445)과 브로커 자신의 `$SYS` dropped(129,447)가 독립적으로
일치한 게 이 측정의 근거다. 그리고 그 dropped 지표는 **12시간 soak 사고 때 만들어둔
것**이었다 — 그때는 "브로커만이 아는 유실"을 위해 넣었고, 오늘 실제로 값을 했다.

**이상 알림 — 행은 안 늘었는데 알림은 다시 나갔다.**
DLQ 재처리가 PostgreSQL에 중복을 만드는지 재는 게 목표였다. 답은 "안 만든다"였다
(`UNIQUE(event_id)` + `ON CONFLICT DO NOTHING`, 두 번 되돌려도 행 증가 0).

정작 발견은 옆에 있었다. 컨슈머가 `insertIfAbsent`의 반환값을 **버리고** 무조건
WebSocket 브로드캐스트를 했다. 행은 막히지만 알림은 막을 것이 없어서, 재처리할 때마다
이미 저장된 알림이 다시 나갔다. 앱이 그 토픽을 아직 구독하지 않아 실피해는 없었지만,
"중복이 없다"는 결론만 적고 끝냈으면 못 봤을 것이다.

이 측정이 성립한 이유도 적어둘 만하다. DLQ 9건 중 **3건은 이미 저장돼 있었다** —
서버 커밋은 끝났는데 연결이 끊겨 클라이언트만 실패로 본 in-doubt 건이다.
그 3건이 없었으면 "중복이 안 났다"가 아무것도 증명하지 못했다. 그래서 측정 스크립트가
**"재처리 대상 중 이미 저장된 것"을 먼저 찍고, 0이면 경고**하게 만들었다.

**처리량 — "느리다"고 두 번 적고 재지는 않았다.**
PostgreSQL 장애 실험에서 200초를 기다려도 lag이 3,693 남았고, 리밸런싱 실험에서는
따라잡는 데 13분이 걸렸다. 두 번 다 "느리다"고만 적었다. 재보니 **49 msg/s**,
같은 부하의 알림 발생량이 193 msg/s라 **유입의 1/4**이었다. lag이 쌓이는 게 정상
동작이었던 셈이다.

원인은 레코드 단위 리스너 + `MANUAL_IMMEDIATE`이라 알림 한 건마다 PostgreSQL
커밋(fsync) 1회 + 브로커 왕복 1회. **저장 경로(InfluxDB)에서 똑같은 이유로 8 msg/s까지
떨어진 적이 있고 배치화로 고쳤는데(ADR-011), 알림 경로는 그때 같이 안 고쳤다.**
같은 모양으로 맞추니 lag 20,128 → 238, 4배 부하에서도 유입 전량 소화.

**그리고 그 배치화가 새 버그를 만들었다.** 300초 장애로 재보니 로그에
"배치 저장 실패 **0건**"이 반복해서 찍혔다. 저장할 게 없는데 저장이 실패한다 —
앞뒤가 안 맞는 숫자였다. 클래스 레벨 `@Transactional(readOnly = true)` 때문에
DB를 안 건드리는 `toEntity()`까지 트랜잭션을 열고 있었고, 그래서 DB 장애 중
**변환 단계에서** 실패해 레코드가 하나씩 DLQ로 갔다 — 배치화로 없애려던 바로 그 동작이다.
고친 뒤 같은 장애에서 DLQ 0건.

**덤 — "180초 예산"은 벽시계 시간이 아니었다.**
예산이 180초인데 300초 장애를 DLQ 0건으로 견뎠다. `ExponentialBackOff.maxElapsedTime`이
세는 것은 **백오프로 쉰 시간의 합**이고, 리스너가 실패하는 데 쓴 시간은 안 센다.
시도마다 HikariCP `connectionTimeout` 30초를 기다리니 백오프 합은 좀처럼 안 는다.
실효 내성은 대략 8분. 코드 주석부터 README까지 "총 경과 시간"이라고 적어둔 것을
전부 바로잡았다.

**고치지 않기로 한 것도 하나.**
같은 밀리초 키 충돌은 재현됐다 — 같은 타임스탬프로 500건을 보내면 InfluxDB에
**1행**만 남는다(499건이 에러도 로그도 없이 소실). 하지만 임계값이 차량당
1,000 msg/s(밀리초당 1건)이고 이 시스템은 차량당 5 msg/s다. 200배 여유다.
시퀀스 태그는 포인트마다 시리즈를 만들어 인덱스를 무너뜨리고, 마이크로초 정밀도는
데이터 스펙까지 바꿔야 한다. **재현하고, 임계값을 재고, 안 고치기로 하고, 그 근거를
`toPoint()` 주석에 남겼다.**

**앱 — 폭을 제한했더니 이번엔 안이 비었다.**
화면마다 `maxWidth`를 따로 정해 8종(420~1200)이 흩어져 있었다. 간격·글자에서 고쳤던
것과 같은 문제라 `ContentWidths` 4단으로 묶었다. 기준은 폭이 아니라 "한 줄에 무엇이
들어가는가"다. 그런데 1280px 스냅샷을 뽑아보니 이번엔 계측 카드가 **비어** 있었다 —
아크 게이지가 104~144px 고정이라 카드가 커져도 그대로였다. 폭 제한만으로는 부족했다.

**회귀 확인.** 하루에 MQTT 연결 옵션, 컨슈머 배치화, 트랜잭션 전파를 다 건드렸으니
기존 시나리오를 다시 돌렸다. InfluxDB·Kafka 90초 장애 모두 **유실 0**이고, 이번엔
새로 만든 발행 측 계측 덕분에 **전 단계가 한 줄로 대조된다**:

```
influxdb 시나리오 — 발행 162,402 = PUBACK 162,402 = MQTT 수신 162,402
                  = Kafka 162,402 = InfluxDB 행 162,402
kafka 시나리오   — 발행 170,079 = PUBACK 170,079 = MQTT 수신 170,079
                  = InfluxDB 행 170,079 (토픽은 170,080 — spool 재전송 1건이
                    InfluxDB 덮어쓰기로 흡수됐다)
```

예전에는 이 대조가 Kafka 토픽부터 시작했는데, 이제 **차량이 보낸 시점부터** 이어진다.

**이 날 배운 것**: 세 번 다 "내가 방금 만든 것"이 틀렸고, 세 번 다 **숫자가 앞뒤가
안 맞는 순간**에 잡혔다 — 기준이 관측치보다 작다, `confirmed`가 `queued`보다 크다,
저장할 게 없는데 저장이 실패한다. 셋 다 그냥 넘어갈 수 있는 모양이었다.
전날 적어둔 "숫자가 이상할 때 설명될 때까지 결론을 내지 않는다"가 그대로 또 필요했다.

---

## 로컬에서 다시 시작할 때

```bash
git clone <repo-url>
cd vehicle-telemetry-platform
cp .env.example .env
# .env에서 비밀번호들 채우기 (openssl rand -base64 32 으로 생성 추천)
docker-compose up -d
docker-compose ps  # 전체 서비스 healthy 확인
```

Swagger UI: `http://localhost:8080/swagger-ui.html`
Grafana: `http://localhost:3000`
InfluxDB: `http://localhost:8086`
