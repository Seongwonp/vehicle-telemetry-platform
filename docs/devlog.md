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
