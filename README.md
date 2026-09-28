# Telemetrix — 차량 텔레메트리 수집·모니터링 백엔드

> 차량 센서 데이터를 MQTT → Kafka → InfluxDB/PostgreSQL로 흘리는 파이프라인.
> **기능 수보다 "장애 중 무엇이 유실·중복·역전됐는지를 증명할 수 있는가"에 집중한** 백엔드 포트폴리오.

- Java 17 · Spring Boot 3 · Kafka · MQTT(Mosquitto, mTLS) · InfluxDB · PostgreSQL · Redis · Python(이상 감지) · Docker Compose
- 모바일 앱(Flutter): https://github.com/Seongwonp/vehicle-telemetry-app
- 개발: 2026.01 ~ · 박성원

## 아키텍처

```mermaid
flowchart LR
    SIM["차량 시뮬레이터 / OBD-II"] -->|MQTT QoS1| MQ["Mosquitto"]
    MQ --> BE["Spring Boot\nMQTT→Kafka 프로듀서\n(실패 시 로컬 spool)"]
    BE -->|"key=vehicle_id"| K["Kafka\nvehicle-telemetry"]
    K -->|"storage-group\n배치·수동 commit"| INF["InfluxDB"]
    K -->|"detector-group"| PY["Python 이상 감지\n×3 인스턴스"]
    PY --> KA["Kafka\nanomaly-alerts"] --> PG["PostgreSQL"]
    INF & PG & RD["Redis\n(rate limit)"] --- API["REST + WebSocket\nJWT"]
    BE -. DLQ .-> DLQ["*-dlq 토픽"]
    API --> PROM["Prometheus / Grafana"]
```

설계 결정 27건과 이유는 [ADR](docs/architecture-decisions.md).

## 핵심 결과 5개 — 결과별 재현·원본 보존 범위를 구분한다

초기 72% 유실 등 일부 과거 관찰은 원본 로그가 남아 있지 않다. 수정 후 반복 실행의
원본은 보존했지만 최초 실패의 원본을 대신하지 않는다. 각 결과 문서의 검증 상태·한계를 함께 본다.

| 무엇을 찾았나 | 원인 | 결과 | 근거 |
| --- | --- | --- | --- |
| **MQTT 브로커 90초 장애에서 72% 유실** | Paho 재연결 상한 기본값 128초 동안 브로커 큐(10k)가 넘침. 우리 지표는 "받은 것"만 세서 정상으로 보였고, 브로커 `$SYS` dropped만 알고 있었다 | 상한 5초·큐 100k → **유실 0, 3회 반복** | [ADR-021](docs/architecture-decisions.md), [결과](load-test/fault-injection/RESULT_20260905_mqtt_broker.md) |
| **수집 파이프라인 99.8% 유실** (10,000 → 20 msg/s) | "안전하게" 넣은 메시지당 spool 파일 쓰기 + InfluxDB 건당 HTTP가 직렬 병목. 바꾼 뒤 처리량을 안 재서 4주간 몰랐다 | 배치 쓰기 + spool은 실패 시에만 → **약 9,600 msg/s**, 안전장치 유지 | [ADR-011](docs/architecture-decisions.md), [ADR-019](docs/architecture-decisions.md) |
| **저장 실패가 조용히 유실되는 구조** | auto-commit + 단건 처리 | InfluxDB 저장 성공 **또는 DLQ 발행 성공 뒤에만** 수동 commit. DLQ 발행 실패면 offset 안 넘김. 재처리 횟수 헤더 전파 | [ADR-012](docs/architecture-decisions.md), [Runbook](docs/runbook/dlq-reprocessing.md) |
| **이상 감지 lag 200만+ 에도 저장 경로 무영향** | Consumer Group 분리 | 24h soak에서 저장 lag 수백 유지. 감지기 3인스턴스로 lag 1,500 이하 | [ADR-002](docs/architecture-decisions.md), [ADR-016](docs/architecture-decisions.md) |
| **같은 초 타임스탬프로 50% 덮어쓰기** | InfluxDB 초 정밀도. Kafka lag은 0이라 정상처럼 보임 | 밀리초 정밀도 → 유실 0. 이후 strict 입력 계약(누락 필드가 0으로 저장되던 것 차단) | [ADR-014](docs/architecture-decisions.md), [입력 계약](docs/telemetry-schema-decision-table.md) |

그 외: Redis 장애 시 조회 fail-open / 로그인·진단 fail-closed 분리([정책](docs/redis-failure-policy.md)),
독성 메시지 1건이 정상 100건을 막지 않음, REST 소유권 검사(ADR-025), MQTT mTLS(ADR-013),
사용자·소유권 RDB 모델과 200만 행 `EXPLAIN`으로 잡은 목록 N+1([ADR-027](docs/architecture-decisions.md), [실행계획](docs/verification/2026-09-27-postgres-explain.md)).
전체 실험 서사와 수치는 [상세 기록](docs/portfolio-detail.md).

## 검증 방식

- **테스트**: Java 300+건(Testcontainers 계약 5종, CI가 skip 0 강제) · Python pytest · Java/Python이 **같은 fixture 61건**으로 입력 계약 판정 일치 확인
- **실험 규칙**: 가설·대조군·성공 기준을 먼저 적고, 발행 시도 수가 아니라 **PUBACK·Kafka offset·InfluxDB 행 수를 대조**한다. 안정성 주장은 같은 조건 3회 이상에서만 — [증거 정책](docs/evidence-policy.md)
- **증거 보존**: 실행마다 `load-test/<시나리오>/evidence/<run-id>/`에 commit SHA·명령·원본 로그·checksum. CI가 checksum 전수 검사

## 말하지 않는 것

- exactly-once, 모든 조건에서 무손실, 고가용성(broker 1대, RF=1)
- 최대 처리량 수치(반복 결과가 크게 흔들려 철회)
- 실제 OBD-II 연결, 운영 배포, 앱 실기기 E2E — 전부 **미검증**

## 실행

```bash
cp .env.example .env        # 비밀값 채우기
docker compose -f docker-compose.yml -f docker-compose.dev.yml --profile simulator up -d   # 평문 dev
curl http://localhost:8080/actuator/health          # Swagger: /swagger-ui.html, Grafana: :3000
cd backend && ./gradlew test                        # Docker 필요(Testcontainers)
```

기본 `docker-compose.yml`만 쓰면 mTLS(8883)라 `broker/certs/generate-certs.sh`가 먼저 필요하다.

## 문서

| | |
| --- | --- |
| [상세 기록](docs/portfolio-detail.md) | 장애 시나리오별 동작, 실측 서사 전체 |
| [ADR](docs/architecture-decisions.md) | 왜 이렇게 골랐고 무엇을 포기했는지 |
| [로드맵](docs/roadmap.md) · [현재 상태 감사](docs/current-state-audit-2026-09-09.md) | 완료/미검증 경계와 다음 작업 |
| [Runbook](docs/runbook/) | DLQ 재처리, Redis 장애, 저장 스케일아웃, 사용자 스키마 전환 |
| [개발 일지](docs/devlog.md) | 어디서 틀렸고 무엇이 그걸 드러냈는지 |
