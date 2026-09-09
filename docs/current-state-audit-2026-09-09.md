# Telemetrix 현재 상태 감사 및 8주 실행 계획

> 기준일: 2026-09-09
>
> 백엔드 기준 커밋: `5556adf06c781bc6ffdbdbdbc5358ceb6b642f65`
>
> Flutter 앱 기준 커밋: `e4266a424581635167c28514d808809e7f105ea9`

## 1. 감사 범위와 검증 한계

이번 감사는 README의 설명만 믿지 않고 다음 자료를 서로 대조했다.

- Spring Boot, Python, Flutter 실제 코드와 설정
- Kafka topic, Consumer Group, offset 및 DLQ 처리 코드
- Docker Compose, GitHub Actions, Testcontainers 계약 테스트
- `load-test/**/RESULT_*.md`와 보존된 `evidence/` 원본
- ADR, Runbook, roadmap, devlog
- 두 저장소의 원격 `main` 및 최근 GitHub Actions 결과

현재 노트북에는 Docker와 Flutter SDK가 없어 전체 스택과 앱 렌더링을 로컬에서 다시
실행하지 못했다. Java 17은 설치되어 있다. 따라서 이번 문서에서 `검증 완료`는 기존
원본 증거와 성공한 CI가 확인되는 범위만 뜻한다. 문서에만 있고 대응하는 코드·테스트·원본을
찾지 못한 내용은 완료로 승격하지 않는다.

## 2. 결론

Telemetrix는 기술 목록을 보여주는 단계에서 벗어나, 차량 데이터 파이프라인의 유실·중복·순서,
offset commit, 장애 격리, 복구 시간을 실제 실험으로 설명할 수 있는 프로젝트가 됐다.
특히 다음 태도가 강점이다.

- 좋은 수치만 남기지 않고 잘못된 집계와 실패한 튜닝을 보존한다.
- `vehicle_id` 파티션 키와 Consumer Group 분리의 효과를 코드와 lag 변화로 함께 확인한다.
- 저장 성공 전에는 offset을 커밋하지 않고, DLQ 발행 실패도 처리 성공으로 간주하지 않는다.
- 단일 실행 결과를 안정성으로 일반화하지 않고 3회 반복과 대조군을 요구한다.

반면 상용 운영 시스템이라고 부르기에는 입력 스키마의 조용한 값 변환, 단일 Kafka 브로커,
Redis 장애 시 API 500, 로컬 spool volume 손실, 실기기 앱 검증 부재가 남아 있다. 지금 필요한
것은 Kubernetes나 서비스 추가 분리가 아니라 **증거 재현성 → 입력 계약 → 장애 정책 → 앱 실기기
검증** 순으로 경계를 닫는 작업이다.

## 3. 구조 감사

### 3.1 모듈과 책임

| 모듈 | 확인한 책임 | 감사 의견 |
| --- | --- | --- |
| `simulator/` | MQTT QoS 1 발행, PUBACK 기반 기준량 생성, 부하 생성 | 발행 시도와 브로커 수신을 분리해 세는 점이 좋다. 실제 OBD-II timestamp 특성은 미검증이다. |
| `broker/` | Mosquitto, mTLS, 차량별 인증서/ACL, 큐 제한 | 기본 Compose가 mTLS인 점은 좋다. 단일 브로커다. |
| `backend/` | MQTT ingress, Kafka 생산·소비, InfluxDB/PostgreSQL 저장, REST/WebSocket, 인증 | 한 프로세스가 여러 책임을 가지지만 개인 프로젝트 규모에서는 합리적이다. `mqtt.ingest.enabled`로 저장 전용 인스턴스를 구분한다. |
| `anomaly-detector/` | Kafka 소비, 룰/선택적 ML, 알림 발행, Redis 상태 | 저장 경로와 별도 Consumer Group이다. ML 품질과 drift 대응은 아직 제한적이다. |
| `monitoring/` | Prometheus, Grafana, pipeline funnel, alert rules | 단순 JVM 지표보다 발행·유입·저장 성공량 비교에 초점을 둔 점이 좋다. |
| `load-test/` | 부하·장애·순서·중복·DLQ·스케일 실험 | 프로젝트의 가장 강한 자산이다. 일부 실험은 1회이고 체크섬 이식성 문제가 새로 발견됐다. |
| Flutter 앱 | REST/JWT, 차량 목록·상세, STOMP 실시간 상태, stale 방어 | 코드/위젯 수준 검증은 좋지만 실제 기기 E2E는 미검증이다. |

### 3.2 데이터 흐름

확인된 기본 흐름은 다음과 같다.

1. 차량 또는 시뮬레이터가 `vehicle/telemetry/<vehicle_id>`로 MQTT QoS 1 발행
2. `MqttMessageHandler`가 JSON, Bean Validation, timestamp, topic/payload 차량 ID를 검사
3. `TelemetryProducer`가 `vehicle_id`를 key로 `vehicle-telemetry`에 발행
4. `telemetry-storage-group`이 InfluxDB에 배치 저장 후 수동 ack
5. `anomaly-detector-group`이 독립적으로 룰/ML 이상 감지
6. `vehicle-anomaly-alerts`를 `anomaly-storage-group`이 PostgreSQL에 저장
7. 새로 저장된 알림만 WebSocket으로 방송

저장과 이상 감지는 같은 topic을 읽지만 Consumer Group이 달라 한쪽 lag이 다른 쪽 offset을
막지 않는다. 다만 Kafka에 직접 메시지를 넣으면 MQTT 입구의 Bean Validation을 우회하므로,
현재 두 입구의 데이터 계약은 동일하지 않다.

### 3.3 Kafka와 실패 처리

- producer: `acks=all`, retries 3
- consumer: auto commit 비활성화, `MANUAL_IMMEDIATE`, 배치 listener
- 기본 파티션: telemetry 3, anomaly alerts 3
- 기본 replication factor: 1
- 재시도: 지수 backoff, 기본 예산 180초
- DLQ: 원본 topic별 명시적 DLQ, 발행 결과 확인 후에만 복구 성공 처리
- 정적 멤버십: 기본 ON, 빈 `GROUP_INSTANCE_ID_BASE`면 OFF
- session timeout: 실측한 동작에 맞춰 45,000ms 명시

이 구성은 단일 브로커 환경의 at-least-once 처리와 장애 가시성에는 적합하다. 그러나 브로커가
한 대이고 모든 topic의 replication factor가 1이므로 Kafka 고가용성을 증명한 것은 아니다.

### 3.4 저장소 역할

| 저장소 | 역할 | 남은 위험 |
| --- | --- | --- |
| InfluxDB | 차량별 시계열 센서 데이터 | 밀리초 identity 충돌 경계, query 병목, 단일 인스턴스 |
| PostgreSQL | 차량 메타데이터, 주행/이상 이력 | 다중 사용자 권한 모델 없음, 이상 이력 보존 기간 미정 |
| Redis | refresh token, rate limit, brute-force, 선택적 ML 상태 | 장애 시 rate limit 경로가 API 500을 만들며 refresh도 중단 |
| 로컬 spool | Kafka ACK 전 MQTT 수신 데이터 보존 | volume 자체가 손실되면 복구 불가 |

### 3.5 보안

확인된 장점은 JWT access/refresh 분리, Redis `GETDEL` 기반 refresh rotation, BCrypt,
관리자 기본 비밀번호 fail-fast, STOMP CONNECT/구독 인가, 기본 mTLS 및 차량별 ACL이다.

남은 한계는 다음과 같다.

- 사용자는 단일 in-memory admin 계정이다.
- `/actuator/prometheus`는 애플리케이션에서 공개하며 운영 네트워크 격리를 전제로 한다.
- Redis 장애 시 보안 기능과 API 가용성 사이의 정책이 결정되지 않았다.
- `vehicles.owner`는 자유 입력이며 실명 입력 방지·마스킹 정책이 없다.
- 앱 통합 테스트의 로컬 테스트 비밀번호가 소스 기본값으로 남아 있다. 실제 비밀은 아니지만
  포트폴리오 위생상 `--dart-define` 필수로 바꾸는 편이 낫다.

추적 파일 중 실제 `.env`, 개인 키, keystore는 확인되지 않았고 `.env.example`만 추적된다.

### 3.6 테스트와 자동화

- Java 테스트 파일 31개
- Python anomaly detector 테스트 파일 4개
- Flutter 일반 테스트 파일 15개, integration test 1개
- 백엔드 CI는 Java 17 테스트, Testcontainers 계약 5종 skip 0 강제, Python 테스트,
  Compose config/build/full-stack health를 수행한다.
- 앱 CI는 Flutter 3.41.6을 고정하고 format, analyze, widget/unit test를 수행한다.

부하·장애 실험을 CI에 모두 넣지 않은 것은 합리적이다. 장시간 실험은 호스트 성능과 Docker
환경의 영향을 크게 받으므로, 전용 데스크탑에서 실행 조건과 원본 증거를 함께 남기는 편이 낫다.

## 4. 냉정한 점수

아래 점수는 **취업용 백엔드 포트폴리오** 기준이다. 상용 운영 준비도와 동일하지 않다.

| 항목 | 점수 | 근거 |
| --- | ---: | --- |
| 백엔드 설계 | 8.8 | 처리 경계와 저장소 역할이 명확하다. Kafka 직접 입력의 검증 우회가 남아 있다. |
| 데이터 신뢰성 | 8.5 | 유실·중복·덮어쓰기·재처리를 실험했다. 누락 primitive와 일부 조용한 값 변환이 남아 있다. |
| Kafka 활용 | 9.0 | key, group 분리, 수동 commit, batch, retry, DLQ, 정적 멤버십 A/B까지 사용 이유를 설명할 수 있다. |
| 장애 복구 | 8.7 | MQTT/Kafka/DB/consumer 장애 증거가 강하다. 단일 broker와 spool volume은 단일 장애점이다. |
| 테스트 자동화 | 8.9 | CI와 Testcontainers 계약이 강하다. 무거운 실험과 앱 E2E는 수동/미완료다. |
| 관측 가능성 | 8.4 | lag, DLQ, pipeline funnel, 저장 성공량을 본다. 하나의 이벤트를 전 구간에서 상관 분석하기 어렵다. |
| 보안 | 7.5 | JWT rotate와 mTLS가 좋다. 단일 관리자, Redis 결합, 개인정보 정책이 약점이다. |
| 배포·운영 | 7.8 | Compose, Runbook, retention 절차는 좋다. 실제 배포·롤백·다중 노드 증거는 없다. |
| 문서화 | 9.1 | 실패와 측정 오류까지 보존한다. 중복과 상태 드리프트가 생길 정도로 문서가 커졌다. |
| Flutter 앱 | 7.8 | 반응형·테마·실시간 상태 방어와 CI가 있다. 실기기 E2E/스크린샷 증거가 없다. |
| 현대오토에버 직무 연결성 | 9.1 | 차량 데이터 수집, 인터페이스 연계, 성능·장애 분석 경험을 구체적으로 설명할 수 있다. |

종합 평가는 약 **8.7/10**이다. 대학생 개인 프로젝트로는 강하지만, 상용 운영 준비도로는
6점대 후반이 현실적이다.

## 5. 위험 목록

| 우선순위 | 위험 | 실제 근거 | 영향 | 완료 조건 |
| --- | --- | --- | --- | --- |
| P0 | Windows checkout에서 일부 evidence checksum 불일치 | 확장자 없는 `.prev_offsets`가 manifest에 포함되지만 `.gitattributes` 적용 밖이다 | 제3자가 원본 증거를 검증하지 못함 | Windows/Linux clean checkout에서 전체 manifest 통과 |
| P0 | 숫자 필드 누락이 0으로 조용히 변환될 수 있음 | DTO가 `double`/`int` primitive이고 presence 제약이 없음 | 결측 데이터가 정상 0으로 저장될 수 있음 | nullable wrapper + `@NotNull` 또는 동등한 strict schema 계약 |
| P0 | `rpm: 2000.7`이 2000으로 저장됨 | `ACCEPT_FLOAT_AS_INT` 기본 동작 실험 | 감지 원본과 저장값이 달라짐 | 허용/거부 정책과 테스트, 포트폴리오 문구 일치 |
| P0 | MQTT와 Kafka 직접 입력의 검증 수준이 다름 | validation은 MQTT handler에만 있음 | 다른 producer가 입력 계약을 우회 | 공통 decoder/validator와 경로별 DLQ 계약 테스트 |
| P0 | roadmap의 과거 `남은 것`이 완료 기록과 충돌 | NaN·정적 멤버십 OFF 문장 | 다음 작업자가 잘못된 우선순위를 잡음 | 상태 문장 정리 및 이 감사 문서를 현재 기준으로 연결 |
| P1 | Redis 장애 시 API 500 및 refresh 중단 | README 장애 시나리오 5 | 가용성/보안 정책 불명확 | endpoint별 fail-open/fail-closed 결정과 장애 실험 |
| P1 | 이벤트 전 구간 상관관계 부족 | HTTP traceId는 있으나 MQTT/Kafka/alert 공통 ID 없음 | 사고 분석 시 레코드 추적 비용 증가 | `event_id`로 로그·Kafka·DB·WebSocket 연결 |
| P1 | 일부 A/B 실험이 1회 | 정적 멤버십 OFF, timeout, poison 유형 | 안정성 주장 범위 제한 | 핵심 조건 3회 및 변동 폭 기록 |
| P1 | Flutter 실기기 E2E 미완료 | macOS Keychain entitlement에서 로그인 중단 | 앱의 실제 조작성·재연결 미검증 | 실제 Android 기기/에뮬레이터에서 전체 체크리스트 통과 |
| P2 | Kafka broker/RF=1 | topic 생성 설정 | broker 장애 중 서비스 지속 불가 | 필요성과 자원이 있을 때만 multi-broker 실험 |
| P2 | schema version 없음 | JSON payload에 버전/호환성 정책 없음 | 장치·서버 버전 분화에 취약 | JSON Schema 수준의 버전/호환성 fixture 테스트부터 도입 |
| P2 | 운영 롤백 자동화 없음 | Compose/배포 가이드는 있으나 롤백 검증 없음 | 잘못된 배포 복구 시간 불명 | 실제 배포 환경을 정한 뒤 rollback rehearsal |

## 6. 주장-증거 매트릭스

| 하고 싶은 주장 | 근거 | 상태 | 부족한 증거 / 허용 표현 |
| --- | --- | --- | --- |
| 저장과 이상 감지 경로가 격리된다 | Consumer Group 설정, anomaly lag 200만+ 실험, 저장 lag/행 수 | 검증 완료 | 실험 조건을 같이 쓴다. 모든 장애에 대한 완전 격리는 주장하지 않는다. |
| 저장 성공 후에만 offset을 커밋한다 | `TelemetryConsumer`, Testcontainers storage failure 계약 | 검증 완료 | at-least-once라고 표현한다. exactly-once라고 쓰지 않는다. |
| consumer 종료 후 재전달돼도 행 중복이 없다 | kill/redelivery 반복 결과, Influx identity 대조 | 해당 조건 검증 완료 | 서로 다른 timestamp를 가진 논리적 중복까지 막는 것은 아니다. |
| 알림 DLQ 재처리가 중복 행·중복 방송을 만들지 않는다 | `UNIQUE(event_id)`, `ON CONFLICT`, replay 결과 | 검증 완료 | PostgreSQL 알림 경로에 한정한다. |
| 저장 인스턴스 제거 시 유실 없이 복구한다 | 부하 중 scale-down 3회, topic=Influx rows | 해당 조건 3/3 완료 | 정적 멤버십 ON에서 46~54초 정지 파티션이 생긴다는 대가를 같이 쓴다. |
| 정적 멤버십이 스케일다운을 늦춘다 | ON 46/52/54초, OFF 9초 | 부분 검증 | OFF는 1회다. 폭풍 방지 효과는 이번 대조군에서 재현하지 못했다. |
| MQTT 90초 장애에서 데이터 유실이 없다 | PUBACK 기준량, broker/backend/Influx 대조 | 해당 조건 완료 | broker queue와 재연결 설정 범위 안의 결과다. 무조건 무손실이라고 하지 않는다. |
| 모든 비정상 메시지를 DLQ로 격리한다 | poison 실험과 DLQ 결과 | 부분 검증 | 누락 primitive, 소수 rpm, DTC null/comma는 조용히 통과한다. |
| 저장 경로 최대 처리량은 약 57K msg/s다 | 반복 결과가 56,985→44,279→26,564로 변동 | 반증됨 | 숫자 ceiling을 주장하지 않는다. Influx write latency 증가 방향만 설명한다. |
| Kafka가 고가용성이다 | 단일 broker, RF=1 | 미검증 | 주장하지 않는다. local spool 기반 broker 장애 복구 실험이라고 표현한다. |
| 앱이 stale·역전·동일 timestamp를 방어한다 | widget/unit tests와 CI | 코드 수준 검증 | 실기기 WebSocket 시나리오는 미검증이다. |

## 7. P0/P1/P2 백로그

### P0 — 신뢰성의 기반

1. evidence checksum 이식성 복구와 전체 manifest 검사 자동화
2. 누락/null/type mismatch를 포함한 telemetry schema 정책표 작성
3. MQTT와 Kafka 직접 입력이 공유하는 decoder/validator 구현
4. invalid 입력의 MQTT DLQ/Kafka DLQ 귀속 규칙과 계약 테스트
5. roadmap·README·CLAUDE 상태 문장 동기화

### P1 — 채용 포트폴리오에 직접 도움이 되는 검증

1. Redis 장애 정책 결정과 30/90초 장애 실험
2. 영구 실패 배치의 retry budget 소진과 DLQ 재처리
3. `event_id` 기반 MQTT→Kafka→DB→WebSocket 상관관계
4. 정적 멤버십 OFF와 timeout A/B 3회 반복
5. Flutter 실제 기기 E2E와 라이트/다크·오류·재연결 증거
6. 실험 순서 무작위화, 휴지 시간, 온도·클럭을 포함한 성능 측정 재설계

### P2 — P0/P1 이후 필요성이 확인된 경우

1. `schema_version`과 JSON 호환성 fixture
2. 데이터 보존·삭제 자동화와 개인정보 최소화
3. 성능 회귀 테스트
4. 실제 배포 환경의 롤백 연습
5. 실제 OBD-II 장치와 timestamp 정밀도 검증
6. 자원과 필요성이 있을 때만 multi-broker/RF>1 및 추가 수평 확장

## 8. 테스트 전략

| 계층 | 검증 대상 | 적합한 도구 | 도입 이유 / 유지 비용 |
| --- | --- | --- | --- |
| 순수 단위 | 유한값, timestamp, schema 규칙, dedup 판정, backoff 계산 | JUnit 5, pytest, Flutter test | 빠르고 실패 원인이 선명하다. fixture가 실제 입력과 드리프트하지 않게 관리해야 한다. |
| Spring slice | 인증/인가, JSON 역직렬화, 예외 응답, repository mapping | `@WebMvcTest`, Jackson test, repository slice | 전체 인프라 없이 경계 계약을 검증한다. slice마다 mock 설정 비용이 있다. |
| Testcontainers 통합 | Kafka offset/DLQ, PostgreSQL idempotency, Redis rotate, Influx query/write | 현재 Testcontainers 유지 | 실제 제품 동작과 가깝다. Docker와 CI 시간이 필요하다. 새 컨테이너 제품은 늘리지 않는다. |
| MQTT/Kafka/DB E2E | topic/payload ID, 정상/invalid 흐름, 저장량 대조 | Docker Compose + 기존 Python/Bash 도구 | 서비스 연결부를 증명한다. 테스트 ID와 시작 offset을 격리해야 한다. |
| 부하 테스트 | 처리량, p95/p99, lag, 발행량=유입량=저장량 | 기존 멀티프로세스 generator, Prometheus | 현재 도구를 재사용한다. 호스트 온도와 실행 순서가 결과를 오염시키므로 조건 기록이 필수다. |
| 장애·복구 | retry, offset, DLQ, recovery time, loss/duplicate | Docker stop/pause, Toxiproxy는 필요 시에만 | 컨테이너 중단은 유지비가 낮다. 지연/패킷 손실이 반복 필요할 때만 Toxiproxy를 추가한다. |
| 보안 | IDOR, JWT 만료/rotate, brute force, CORS/STOMP 권한 | Spring Security test, Testcontainers Redis | 기존 스택으로 가능하다. 공격 스캐너 추가보다 권한 불변식 자동화가 먼저다. |
| soak | 메모리, lag, rebalance, 저장 정합성, 로그/디스크 증가 | 기존 scripts + Prometheus/Grafana | 데스크탑 전용이며 6~24시간이 든다. 매 변경마다가 아니라 마일스톤에서 실행한다. |

## 9. 장애 실험 상태와 다음 성공 기준

| 실험 | 현재 상태 | 다음 가설과 성공 기준 |
| --- | --- | --- |
| MQTT broker 중단/복구 | 완료 | 기존 90초 결과를 회귀 기준으로 유지. PUBACK=backend 수신=Influx 행, broker dropped=0 |
| Kafka 중단/복구 | 완료 | spool pending이 증가하고 복구 후 0, Kafka ACK 이후에만 파일 삭제 |
| InfluxDB 지연/중단 | 90/720초 완료 | retry budget 경계 반복, offset 미커밋과 최종 topic=rows |
| Redis 중단 | 미검증 | endpoint별 fail-open/fail-closed 결정 후 상태 코드, 오류율, refresh 결과 수집 |
| storage consumer 강제 종료 | 반복 완료 | 재전달 후 유실 0, 허용 identity 범위에서 중복 행 0 |
| anomaly detector 강제 종료 | 경로 격리 확인 | 저장 lag 영향 0, 재기동 후 anomaly lag 회복 시간 수집 |
| 네트워크 지연/패킷 손실 | 미검증 | Docker 제어로 부족할 때만 Toxiproxy 도입, latency/loss와 retry 관계 확인 |
| 같은 메시지 중복 발행 | 부분 완료 | byte-identical과 논리적 중복을 구분하고 저장/알림 각각 기준 정의 |
| 과거 timestamp out-of-order | 완료 | 차량별 원본 보존과 앱 stale 방어를 함께 검증 |
| 비정상적으로 큰 메시지 | 미검증 | broker/backend/Kafka 크기 제한 중 최초 거부 지점과 DLQ/metric 확인 |
| 다른 schema 메시지 | 부분 완료 | missing/null/wrong type 전체 표를 만들고 공통 validator 통과/거부 확인 |
| DLQ 재처리 | telemetry/anomaly 완료 | 영구 poison 재투입 루프 방지와 mqtt envelope 경로는 남음 |
| 재시작 후 offset/저장 정합성 | 완료 | 마일스톤 soak에서 회귀 확인 |
| anomaly lag이 저장에 미치는 영향 | 완료 | 동일 부하 조건에서 저장 lag과 성공량 독립 유지 |

## 10. 8주 실행 로드맵

학업 병행 기준 주 5~8시간, 총 약 50시간으로 잡는다.

| 주차 | 목표 | 구현·테스트 | 산출물 | 완료 조건 | 예상 시간 |
| --- | --- | --- | --- | --- | ---: |
| 1 | 증거 체인 복구 | `.prev_offsets` 정책, 전체 checksum 검사, 문서 상태 정리 | 검사 스크립트, 검증 기록 | Windows/Linux clean checkout 통과 | 4~5h |
| 2 | strict schema 설계 | missing/null/type/DTC 정책과 단위 테스트 | schema decision table, 실패 fixture | 모든 필드의 허용/거부가 테스트로 표현됨 | 5~6h |
| 3 | 공통 ingress 검증 | MQTT/Kafka 공통 decoder/validator, Testcontainers/E2E | 코드, 계약 테스트, 결과 문서 | 두 입구가 같은 payload를 같은 이유로 거부 | 7~8h |
| 4 | Redis와 DLQ 복구 정책 | Redis 30/90초, 영구 poison budget 소진, replay | 결과 문서, Runbook | 상태 코드·유실·복구 절차가 수치로 남음 | 6~8h |
| 5 | 이벤트 상관관계 | `event_id` 전파, 로그/metric/dashboard | 추적 예시와 쿼리 | 한 ID로 각 단계를 찾을 수 있음 | 6~7h |
| 6 | Flutter 실기기 E2E | 로그인, refresh, 재연결, stale, 중복, 로그아웃 | 실행 로그, 스크린샷 | 실제 기기 체크리스트 통과, 실패도 기록 | 7~8h |
| 7 | 반복성과 성능 방법 개선 | 핵심 A/B 3회, 휴지/순서 섞기/온도 기록 | 원본 evidence, 변동 폭 표 | 수치와 환경 효과를 분리해 설명 가능 | 6~8h |
| 8 | 포트폴리오 정리 | README 압축, Runbook/ADR 링크 검증, 최종 감사 | README, 포트폴리오 문구, 데모 자료 | 모든 강한 주장에 코드·테스트·원본 링크 존재 | 5~6h |

노트북에서는 1·2·5·8주차의 문서/코드 리뷰와 빠른 테스트를 진행하고, Docker가 필요한
3·4·7주차 및 Flutter 실기기 작업은 데스크탑에서 수행한다.

## 11. 현대오토에버 지원 관점의 연결

과장 없이 다음과 같이 연결할 수 있다.

| 직무 요소 | Telemetrix의 실제 근거 |
| --- | --- |
| 효율적인 백엔드 서버 구현 | 레코드 단위 저장의 fsync 병목을 배치 처리로 바꾸고 처리량을 회복한 과정 |
| 고가용성 | 전체 HA가 아니라, 저장/이상 감지 Consumer Group 격리와 의존성 장애 후 복구 범위 |
| 성능 | 처리량뿐 아니라 lag, p95/p99, Influx write latency, 측정 변동 원인을 함께 분석 |
| 확장성 | 파티션 수와 consumer 수의 관계, 수집/저장 역할 분리, scale-down 비용을 실측 |
| 데이터 수집·처리 | MQTT QoS 1 → Kafka → InfluxDB/PostgreSQL 파이프라인 |
| 시스템 인터페이스 연계 | 장치 프로토콜, message broker, REST, WebSocket, 모바일 앱 연결 |
| 운영 중 문제 해결 | timestamp overwrite, MQTT queue drop, DLQ 무관측, 잘못된 집계 수정 사례 |
| 물리 시스템과 서버 연결 | 실제 OBD-II를 고려한 차량 ID, timestamp, 센서/DTC 모델. 실제 장치 연동은 아직 미검증이라고 명시 |

### 포트폴리오 문구 초안

> 차량 텔레메트리의 높은 처리량 자체보다, 장애가 발생했을 때 데이터가 어디까지 도달했고
> 무엇이 유실·중복됐는지를 증명하는 데 집중했습니다. MQTT PUBACK, Kafka offset, InfluxDB
> 저장 행, Consumer lag을 대조해 초 단위 timestamp 충돌과 MQTT broker queue 초과처럼
> 각 구성요소가 성공으로 보이는데 데이터는 사라지는 문제를 찾았습니다. 이후 저장 성공 뒤
> offset을 커밋하는 at-least-once 경로, DLQ 발행 확인, 저장·이상 감지 Consumer Group 격리를
> 코드와 장애 실험으로 검증했습니다. 단일 broker와 실제 OBD-II 미연동 등 현재 한계도 함께
> 공개하고 있습니다.

피해야 할 표현은 `exactly-once`, `완전 무손실`, `고가용성 Kafka`, `운영 규모에서 57K msg/s
보장`, `현대오토에버에 최적화된 프로젝트`다.

## 12. 다음에 바로 수행할 첫 작업

### 작업명

`P0 — evidence checksum clean-checkout 재현성 복구`

### 체크리스트

- [ ] `git status`가 clean이고 `main == origin/main`인지 확인한다.
- [ ] manifest에 포함된 `.prev_offsets`가 증거인지 집계용 scratch인지 결정한다.
- [ ] scratch면 manifest/추적 대상에서 제외하고, 증거면 LF 규칙 또는 `.txt` 이름을 적용한다.
- [ ] 모든 `load-test/**/evidence/**/checksums.txt`를 순회하는 검사 스크립트를 만든다.
- [ ] Windows clean checkout과 Linux CI에서 같은 결과인지 확인한다.
- [ ] 실패 시 파일 경로·기대 hash·실제 hash를 출력한다.
- [ ] 새 실험 스크립트가 manifest 생성 직후 자체 검증하도록 한다.
- [ ] 결과를 `docs/verification/`에 기록한다.
- [ ] `docs/evidence-policy.md`, roadmap, CLAUDE의 완료 상태를 함께 갱신한다.
- [ ] 코드 기능 변경은 이 작업에 섞지 않는다.

이 작업이 끝난 뒤에만 strict telemetry schema 작업으로 넘어간다.
