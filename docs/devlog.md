# 개발 일지 (Dev Log)

> 날짜별 작업 내용, 결정 사항, 트러블슈팅을 기록합니다.
> 서버/환경이 바뀌어도 이 파일 하나로 맥락을 복원할 수 있도록 상세하게 작성합니다.

---

## 2026-05-09 (Day 1)

### 작업 환경
- OS: Linux (공용 서버 환경, JupyterHub 기반)
- 작업 디렉토리: `/home/jovyan/work/vehicle-telemetry-platform`
- Git 저장소 초기화 완료

### 오늘 한 일

#### 1. 프로젝트 기획 문서 작성 완료
- `vehicle-telemetry-platform-plan.md` — 전체 아키텍처, 기술 스택, Phase 로드맵, OBD-II 연결 방법, 면접 어필 포인트 정리
- `CLAUDE.md` — AI 협업 가이드 (개발 원칙, 차량 데이터 스펙, 이상 감지 룰, Phase 정의)

#### 2. README.md 생성
- 프로젝트 개요, 아키텍처 다이어그램, 기술 스택, Phase 진행 상황, 실행 방법 포함
- GitHub 포트폴리오용으로 작성

#### 3. 개발 일지(devlog.md) 생성
- 이 파일 — 날짜별 작업 내용 누적 기록 목적

### 현재 디렉토리 구조
```
vehicle-telemetry-platform/
├── .git/
├── .gitignore
├── CLAUDE.md
├── README.md
├── docs/
│   └── devlog.md          ← 이 파일
└── vehicle-telemetry-platform-plan.md
```

### 결정 사항
- 개발 순서: Phase 1 → 2 → 3 → 4 → 5 순서 엄수
- Phase 1 다음 작업: Docker Compose 기반 인프라 세팅부터 시작
  - Mosquitto (MQTT 브로커)
  - Kafka + Zookeeper
  - InfluxDB
  - PostgreSQL
  - Redis

### 다음 할 일 (Phase 1 시작)
- [ ] `docker-compose.yml` 초안 작성 (전체 인프라 컨테이너 정의)
- [ ] `.env.example` 작성 (환경변수 목록)
- [ ] `simulator/` 디렉토리 생성 및 Python 차량 시뮬레이터 구현
- [ ] `broker/` 디렉토리 — Mosquitto 설정 파일 (TLS 포함)
- [ ] `kafka/` 디렉토리 — Kafka 토픽 초기화 스크립트
- [ ] `backend/` — Spring Boot 프로젝트 초기화 (Gradle)

### 참고 메모
- 공용 서버 없어질 경우 로컬에서 계속 개발 가능: Docker Compose만 있으면 전체 스택 재현 가능
- Claude Code CLI로 AI 협업 진행 중 (모델: claude-sonnet-4-6)
- 화이트햇 프로그램 신청 완료 (합격 시 Phase 4 보안 파트에 경험 반영 예정)
- 불합격해도 UN R155 / ISO SAE 21434 기준으로 독학하며 보안 파트 강화할 것

### 전체 아키텍처 요약 (나중에 복습용)
```
[Python 시뮬레이터] → MQTT(TLS) → [Mosquitto] → [Spring Boot] → [Kafka]
                                                                      ├→ Consumer A: InfluxDB/PostgreSQL 저장
                                                                      └→ Consumer B: Python 이상감지 모듈
                                                                                          ↓
                                                              [Spring Boot REST API] ← JWT 인증, Rate Limiting
                                                                      ↓
                                                              [Grafana 대시보드]
```

### 언어별 역할 (나중에 복습용)
| 언어 | 담당 |
|------|------|
| Python | 차량 시뮬레이터, 이상 감지 모듈 |
| Java (Spring Boot) | MQTT 수신, Kafka 연동, REST API, DB 저장 |
| C | (선택) 실제 OBD-II 연동 시뮬레이터 |
| 설정 파일 | Mosquitto, Kafka, Docker Compose, Grafana |

---

## 2026-05-09 (Day 1 — 오후, Phase 1 시작)

### 오늘 한 일

#### 1. 전체 디렉토리 구조 생성
```
vehicle-telemetry-platform/
├── broker/
│   ├── certs/          ← TLS 인증서 (Phase 4에서 생성)
│   └── config/
│       └── mosquitto.conf
├── kafka/
│   └── init-topics.sh
├── simulator/          ← 다음 작업
├── backend/            ← Phase 2
├── anomaly-detector/   ← Phase 3
├── monitoring/         ← Phase 5
├── docs/
│   └── devlog.md
├── .env.example
├── .gitignore
├── docker-compose.yml
└── README.md
```

#### 2. docker-compose.yml 작성
포함된 서비스:
| 서비스 | 이미지 | 포트 | 역할 |
|--------|--------|------|------|
| mosquitto | eclipse-mosquitto:2.0 | 1883, 8883 | MQTT 브로커 |
| zookeeper | confluentinc/cp-zookeeper:7.6.1 | - | Kafka 코디네이터 |
| kafka | confluentinc/cp-kafka:7.6.1 | 9092 | 메시지 큐 |
| kafka-init | confluentinc/cp-kafka:7.6.1 | - | 토픽 초기화 (1회) |
| influxdb | influxdb:2.7 | 8086 | 시계열 DB |
| postgres | postgres:16-alpine | 5432 | 관계형 DB |
| redis | redis:7.2-alpine | 6379 | 캐시 |

- healthcheck 설정: kafka, influxdb, postgres, redis 전부 포함
- `telemetry-net` 브릿지 네트워크로 서비스 간 통신 격리
- 모든 비밀값은 .env에서 주입 (하드코딩 없음)

#### 3. broker/config/mosquitto.conf 작성
- Phase 1: 1883 포트, 평문 MQTT (개발용 allow_anonymous true)
- Phase 4 전환 시 주석 해제만 하면 TLS + X.509 인증으로 전환 가능하게 준비해둠

#### 4. kafka/init-topics.sh 작성
생성되는 토픽:
| 토픽명 | 파티션 | 용도 |
|--------|--------|------|
| vehicle-telemetry | 3 | 차량 센서 원본 데이터 |
| vehicle-anomaly-alerts | 3 | 이상 감지 결과 알림 (Phase 3) |
| vehicle-dtc-events | 1 | DTC 진단 코드 이벤트 (Phase 3) |

#### 5. .env.example 작성
- MQTT, Kafka, InfluxDB, PostgreSQL, Redis, JWT 환경변수 전부 정의
- 실제 값은 .env에 넣고 .gitignore로 커밋 차단

### 결정 사항
- Kafka는 Zookeeper 방식 사용 (KRaft보다 문서/예제가 많아 학습 편의성 우선)
- mosquitto Phase 1에서 allow_anonymous true — Phase 4에서 X.509로 교체
- .env, broker/certs/ 전부 .gitignore에 추가 (보안)

### 다음 할 일 (Phase 1 계속)
- [x] `simulator/vehicle_simulator.py` — Python 차량 시뮬레이터 구현
- [x] `simulator/requirements.txt` — paho-mqtt, 의존성 정의
- [x] `backend/` — Spring Boot 프로젝트 초기화 (Gradle)
- [x] Spring Boot MQTT Consumer → Kafka Producer 구현
- [x] Spring Boot Kafka Consumer → InfluxDB 저장 구현

---

## 2026-05-09 (Day 1 — 저녁, Phase 1 시뮬레이터)

### 오늘 한 일

#### 1. simulator/vehicle_simulator.py 작성
- `VehicleState` 데이터클래스로 차량 상태 관리
- `next()`: 60초 사이클로 가속→순항→감속 자연스럽게 표현
  - 엔진 온도: 냉간(20°C)에서 워밍업 후 90°C 안정
  - RPM: 속도에 비례하되 스무딩 처리
  - 연료: 틱마다 조금씩 소모
  - GPS: 매 틱마다 미세 이동
- `inject_anomaly()`: ANOMALY_RATE 확률로 이상값 주입 (테스트용)
  - 6가지 이상 타입: high_engine_temp, high_rpm, low/high_battery, high_speed, dtc_code
- MQTT 토픽: `vehicle/telemetry/{vehicle_id}`
- `VEHICLE_COUNT`개 차량을 스레드로 동시 실행
- `SIGINT/SIGTERM` 처리로 Ctrl+C 시 깔끔하게 종료

#### 2. simulator/requirements.txt
- `paho-mqtt>=1.6.1,<2.0`
- `python-dotenv>=1.0.0`

#### 3. simulator/Dockerfile
- python:3.11-slim 기반

#### 4. docker-compose.yml 업데이트
- simulator 서비스 추가 (profiles: ["simulator"])
- 기본 실행엔 포함 안 됨 — 별도로 `--profile simulator` 로 실행

### 실제 OBD-II로 전환할 때
`VehicleState.next()`의 랜덤 계산 부분을 아래로 교체:
```python
import obd
connection = obd.OBD()
speed = connection.query(obd.commands.SPEED).value.magnitude
rpm = connection.query(obd.commands.RPM).value.magnitude
# ...
```

---

## 2026-05-09 (Day 1 — 밤, Phase 1 백엔드)

### 오늘 한 일

#### 1. Spring Boot 프로젝트 구조 생성
```
backend/
├── build.gradle              # 의존성: Spring Integration MQTT, Kafka, InfluxDB, Lombok
├── settings.gradle
├── Dockerfile                # 멀티스테이지 빌드 (gradle builder → jre-alpine)
├── gradle/wrapper/
│   └── gradle-wrapper.properties  # Gradle 8.7
└── src/main/java/com/telemetry/
    ├── TelemetryApplication.java
    ├── config/
    │   ├── MqttConfig.java       # MQTT 연결 + 채널 설정
    │   ├── KafkaConfig.java      # 토픽 자동 생성 Bean
    │   └── InfluxDbConfig.java   # InfluxDB 클라이언트 Bean
    ├── domain/
    │   └── VehicleTelemetry.java # JSON 역직렬화 모델
    ├── mqtt/
    │   └── MqttMessageHandler.java  # MQTT 수신 → Kafka 전달
    ├── kafka/
    │   ├── TelemetryProducer.java   # vehicle_id 키로 파티셔닝
    │   └── TelemetryConsumer.java   # Consumer A(저장) + B(이상감지, Phase 3)
    └── influxdb/
        └── TelemetryRepository.java # InfluxDB Point 저장
```

#### 2. 핵심 데이터 흐름 구현
```
[MQTT broker] → MqttMessageHandler → TelemetryProducer → [Kafka: vehicle-telemetry]
                                                                    ↓
                                              TelemetryConsumer(storage-group) → TelemetryRepository → [InfluxDB]
                                              TelemetryConsumer(anomaly-group)  → TODO Phase 3
```

#### 3. 주요 설계 결정
- **MQTT**: Spring Integration MQTT 사용 (표준 Spring 방식)
- **Kafka 파티션 키**: `vehicle_id` 사용 → 같은 차량 데이터가 항상 같은 파티션 → 순서 보장
- **Consumer Group 분리**: 저장용(`telemetry-storage-group`) / 이상감지용(`telemetry-anomaly-group`) → 각각 독립적으로 같은 메시지 처리
- **InfluxDB 태그**: `vehicle_id`를 tag로 설정 (인덱싱됨, 차량별 쿼리 빠름)

#### 4. docker-compose.yml 업데이트
- `backend` 서비스 추가 (kafka, influxdb, postgres, redis healthy 후 시작)

### 다음 할 일
- [ ] 전체 통합 테스트 (docker-compose up → 시뮬레이터 → Swagger UI → 이상 감지 확인)
- [x] Phase 5: 모니터링/배포 설정 완료

---

## 2026-05-09 (Day 1 — Phase 5 모니터링 & 배포)

### 추가된 것

| 파일 | 내용 |
|------|------|
| `monitoring/prometheus/prometheus.yml` | Spring Boot Actuator 메트릭 수집 |
| `monitoring/grafana/provisioning/` | 데이터소스 자동 연결 (Prometheus, InfluxDB) |
| `monitoring/grafana/dashboards/vehicle-telemetry.json` | 차량 센서 대시보드 (속도/온도/RPM/배터리/연료) |
| `monitoring/grafana/dashboards/backend-metrics.json` | 서버 대시보드 (요청수/응답시간/JVM) |
| `docs/deployment-guide.md` | AWS EC2 배포 단계별 가이드 |

### Grafana 대시보드 접속
```
http://<서버IP>:3000
admin / .env의 GRAFANA_PASSWORD
```

### 배포 시 Sungwon이 직접 할 일
```bash
# EC2에서
git clone <repo>
cp .env.example .env
nano .env          # 비밀번호 전부 변경 (openssl rand -base64 32)
docker compose up -d
```
자세한 내용: docs/deployment-guide.md 참고

---

## 전체 프로젝트 완료 현황 (2026-05-09)

| Phase | 내용 | 상태 |
|-------|------|------|
| 1 | 데이터 파이프라인 (MQTT→Kafka→InfluxDB) | 완료 |
| 2 | REST API + JWT + Swagger + Rate Limiting | 완료 |
| 3 | 이상 감지 (룰 기반 + Isolation Forest) | 완료 |
| 4 | 보안 강화 (TLS, 브루트포스, 감사로그) | 완료 |
| 5 | 모니터링 설정 (Grafana + Prometheus) | 완료 |
| - | AWS EC2 배포 | Sungwon이 직접 진행 |

### 전체 파일 수
- Java 파일: 34개
- Python 파일: 6개
- 설정 파일: 15개
- 문서: 4개

---

## 2026-05-09 (Day 1 — Phase 4 보안 강화)

### 추가된 보안 항목

| 항목 | 구현 |
|------|------|
| MQTT TLS 인증서 생성 | `broker/certs/generate-certs.sh` |
| MQTT X.509 mTLS 설정 | `broker/config/mosquitto.conf` (주석 해제만 하면 활성화) |
| 시뮬레이터 TLS 지원 | `TLS_CA_CERT`, `TLS_CLIENT_CERT`, `TLS_CLIENT_KEY` 환경변수 |
| API 감사 로깅 (MDC) | `RequestLoggingFilter.java` — traceId, ip, status, duration |
| 브루트포스 차단 | `BruteForceDetector.java` — 5회 실패 → 15분 IP 차단 (Redis) |
| 보안 헤더 5종 | `SecurityConfig.java` — X-Frame-Options, HSTS 등 |
| 보안 자체 점검 보고서 | `docs/security-report.md` — OWASP + UN R155 기준 |

### MDC 로그 예시
```
10:23:15 [a3f91c2b] [192.168.1.10] WARN - POST /api/auth/login → 401 (23ms)
10:23:16 [b7e21a4c] [192.168.1.10] WARN - [BruteForce] 로그인 실패 count=3/5
```

### MQTT TLS 활성화 절차 (운영 배포 전)
```bash
# 1. 인증서 생성 (1회)
cd broker/certs && ./generate-certs.sh

# 2. mosquitto.conf TLS 섹션 주석 해제

# 3. 시뮬레이터 TLS 환경변수 설정
TLS_CA_CERT=broker/certs/ca.crt
TLS_CLIENT_CERT=broker/certs/client.crt
TLS_CLIENT_KEY=broker/certs/client.key
```

### 잔여 보안 개선 사항 (docs/security-report.md 참고)
- JWT 토큰 블랙리스트 (로그아웃)
- IDOR 완전 차단 (사용자-차량 소유 검증)
- 운영 시 MQTT 1883 포트 차단

---

## 2026-05-09 (Day 1 — Phase 3 이상 감지)

### 전체 Phase 3 데이터 흐름
```
[Kafka: vehicle-telemetry]
        ↓  (anomaly-detector-group)
[anomaly_detector.py]
   ├─ rules.py          → 룰 기반 즉시 판단 (항상 활성)
   └─ ml_detector.py    → IsolationForest (ML_ENABLED=true 시 활성, 200샘플 후)
        ↓
[Kafka: vehicle-anomaly-alerts]
        ├─ notifier.py  → Webhook 알림 (WEBHOOK_URL 설정 시)
        └─ Spring Boot TelemetryConsumer (anomaly-storage-group)
                ↓
        [PostgreSQL: anomaly_alerts]
                ↓
        GET /api/vehicles/{id}/anomalies (Swagger에서 조회 가능)
```

### Python anomaly-detector 파일 구조
| 파일 | 역할 |
|------|------|
| `anomaly_detector.py` | Kafka 소비 + 전체 파이프라인 조율 |
| `rules.py` | 룰 6가지 (엔진과열, RPM, 배터리, 과속, DTC) |
| `ml_detector.py` | IsolationForest, 200샘플 수집 후 자동 학습 |
| `notifier.py` | Webhook HTTP POST |

### Spring Boot 추가/변경
- `entity/AnomalyAlert.java` - PostgreSQL 저장 엔티티
- `repository/AnomalyAlertRepository.java`
- `service/AnomalyService.java`
- `controller/AnomalyController.java` - GET /api/vehicles/{id}/anomalies
- `dto/response/AnomalyResponse.java`
- `kafka/TelemetryConsumer.java` - Consumer B를 `vehicle-anomaly-alerts` 소비로 변경

### 이상 감지 테스트 방법
```bash
# 1. 시뮬레이터에서 이상값 강제 주입
ANOMALY_RATE=0.5 python simulator/vehicle_simulator.py

# 2. Swagger에서 이상 이력 확인
GET /api/vehicles/SIM-001/anomalies

# 3. ML 활성화 (200개 정상 데이터 수집 후 자동 학습)
ML_ENABLED=true docker-compose up anomaly-detector
```

### 설계 포인트 (면접 답변용)
- **룰 vs ML 분리**: 룰은 즉시 판단, ML은 복합 패턴 보완 → 서로 독립적 동작
- **Consumer Group 분리**: `anomaly-detector-group` (Python) + `anomaly-storage-group` (Java) → 같은 메시지를 각자 독립 처리
- **Isolation Forest 선택 이유**: 라벨 없는 데이터에서 비지도 학습 가능, 차량 데이터처럼 정상/이상 비율 불균형에 적합

---

## 2026-05-09 (Day 1 — Phase 2 REST API + Swagger)

### 오늘 한 일

#### 추가된 패키지 구조
```
controller/   AuthController, VehicleController, TelemetryController
service/      VehicleService, TelemetryQueryService
repository/   VehicleRepository (JPA)
entity/       Vehicle (PostgreSQL 테이블 매핑)
dto/request/  VehicleRegisterRequest, LoginRequest
dto/response/ VehicleResponse, TelemetryResponse, LoginResponse
security/     JwtTokenProvider, JwtAuthenticationFilter, SecurityConfig
config/       SwaggerConfig, RateLimitInterceptor, WebMvcConfig
exception/    GlobalExceptionHandler, ErrorResponse
```

#### 구현된 API 엔드포인트
| Method | Path | 설명 | 인증 |
|--------|------|------|------|
| POST | /api/auth/login | JWT 토큰 발급 | 불필요 |
| POST | /api/vehicles | 차량 등록 | 필요 |
| GET | /api/vehicles | 차량 목록 | 필요 |
| GET | /api/vehicles/{id} | 차량 단건 조회 | 필요 |
| DELETE | /api/vehicles/{id} | 차량 비활성화 | 필요 |
| GET | /api/vehicles/{id}/telemetry | 최근 텔레메트리 (기본 20건) | 필요 |
| GET | /api/vehicles/{id}/telemetry/latest | 최신 1건 | 필요 |

#### JWT 흐름
```
POST /api/auth/login → JWT 발급
→ 이후 모든 요청 Header: Authorization: Bearer <token>
→ JwtAuthenticationFilter가 토큰 검증 → SecurityContext에 인증 정보 세팅
```

#### Rate Limiting (Redis 기반)
- Redis key: `rate_limit:{client_ip}`
- 분당 60회 초과 시 429 Too Many Requests 반환
- 응답 헤더: `X-RateLimit-Limit`, `X-RateLimit-Remaining`

#### Swagger UI 접속
```
http://localhost:8080/swagger-ui.html
```
1. `/api/auth/login`으로 토큰 발급
2. 우측 상단 **Authorize** 버튼 → `Bearer <토큰>` 입력
3. 모든 API 직접 테스트 가능

### 추가된 의존성 (build.gradle)
- `spring-boot-starter-web` (REST API)
- `spring-boot-starter-data-jpa` + `postgresql` (DB)
- `spring-boot-starter-security` + `jjwt 0.12.5` (JWT)
- `spring-boot-starter-data-redis` (Rate Limiting)
- `springdoc-openapi-starter-webmvc-ui:2.5.0` (Swagger)

### 설계 결정
- 관리자 계정: `InMemoryUserDetailsManager` (Phase 4에서 DB 기반으로 교체 예정)
- 소프트 삭제: Vehicle `active=false` (물리 삭제 없음, 데이터 보존)
- InfluxDB 쿼리: Flux 언어, `pivot`으로 행 단위 변환

### 핵심 개념 복습 (나중에 참고)

**왜 Consumer Group을 두 개 나눴나?**
Kafka Consumer Group은 같은 그룹 내에서 파티션을 나눠서 처리.
다른 그룹이면 각자가 토픽의 모든 메시지를 독립적으로 소비.
→ storage-group은 저장만, anomaly-group은 이상감지만 담당하게 분리.

**왜 vehicle_id를 Kafka 파티션 키로?**
같은 키는 항상 같은 파티션 → 한 차량의 메시지 순서 보장.
차량별로 파티션 분산 → 병렬 처리 극대화.

---

### 로컬에서 다시 시작할 때
```bash
# 1. 저장소 클론
git clone <repo-url>
cd vehicle-telemetry-platform

# 2. 환경변수 설정
cp .env.example .env
# .env 파일 열어서 비밀번호들 채우기

# 3. 인프라 전체 실행
docker-compose up -d

# 4. 상태 확인
docker-compose ps
```

---

## 템플릿 (복사해서 사용)

```
## YYYY-MM-DD (Day N)

### 작업 환경
-

### 오늘 한 일

#### 1.

### 현재 디렉토리 구조
```

### 결정 사항
-

### 트러블슈팅
| 문제 | 원인 | 해결 |
|------|------|------|
|      |      |      |

### 다음 할 일
- [ ]
```

---
