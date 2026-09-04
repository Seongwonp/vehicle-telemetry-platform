# Vehicle Telemetry Platform — AI 협업 가이드

## 프로젝트 개요

차량(OBD-II 동글 또는 시뮬레이터)에서 발생하는 실시간 센서 데이터를 수집하고, 이상 감지 및 모니터링까지 처리하는 백엔드 플랫폼.

- **개발자**: Sungwon
- **목적**: 백엔드/서버 엔지니어 포트폴리오 (현대오토에버 등 모빌리티 IT 기업 지원)
- **개발 기간**: 약 10주

---

## 기술 스택

| 영역 | 기술 |
|------|------|
| 데이터 수신 | MQTT (Eclipse Mosquitto) |
| 메시지 큐 | Apache Kafka |
| 백엔드 API | Java 17 + Spring Boot 3 |
| 이상 감지 | Python 3.11 (룰 기반 + scikit-learn 선택) |
| 시계열 DB | InfluxDB |
| 관계형 DB | PostgreSQL |
| 캐시 | Redis |
| 모니터링 | Grafana + Prometheus |
| 차량 시뮬레이터 | Python 또는 C |
| 보안 | JWT, TLS/SSL, Rate Limiting, 이상 접근 감지 |
| 인프라 | Docker Compose, (선택) AWS EC2 |

---

## 디렉토리 구조

```
vehicle-telemetry-platform/
├── simulator/          # 차량 데이터 시뮬레이터 (Python/C)
├── broker/             # Mosquitto MQTT 브로커 설정
├── kafka/              # Kafka 설정 및 토픽 초기화 스크립트
├── backend/            # Spring Boot API 서버
│   ├── src/
│   └── build.gradle
├── anomaly-detector/   # Python 이상 감지 모듈
├── monitoring/         # Grafana + Prometheus 설정
├── docker-compose.yml
├── CLAUDE.md
└── README.md
```

---

## 개발 원칙

- **보안 우선**: 모든 통신은 TLS, 인증 없는 엔드포인트 금지
- **단계별 구현**: Phase 순서를 지켜서 개발 (파이프라인 → API → 이상감지 → 보안 → 배포)
- **실제 OBD-II 연동 고려**: 시뮬레이터와 실제 OBD-II 동글 전환이 쉽도록 인터페이스 분리
- **포트폴리오용**: 코드 가독성, README, 아키텍처 문서 품질 중요

## 현재 최우선 작업 (2026-09-04)

**Kafka 저장 실패 계약 검증 완료** — Docker가 있는 데스크톱에서 실행해
`KafkaStorageFailureContractTest` 3케이스가 skip 없이 통과했고(18.0초), 전체
`./gradlew test --no-daemon`도 96개 전부 통과(skip 0), CI(`e4effc2`)도 success다.
**5회 반복 실행에서도 5/5 통과라 간헐 실패(flakiness)는 관찰되지 않았다.**
실행 기록: `docs/verification/2026-09-04-desktop-docker-contracts.md`.

**우선순위 1번(강제 종료·재전달 시 InfluxDB 중복·덮어쓰기 수량 측정)도 완료** —
`docker kill` 후 재전달 68건(0.0873%)이 발생했으나 InfluxDB 행 수가 고유 키 수와
정확히 같아(77,929 = 77,929) **덮어쓰기로 흡수됨(멱등)**을 확인했다. 같은 밀리초
키 충돌은 이 부하에서 0건이라 ms 정밀도가 충분했다. 측정 도구와 절차는
`load-test/storage-integrity/`, 결과는 같은 폴더의 `RESULT_20260904_kill_redelivery.md`.

다음 작업은 아래 우선순위 2번부터다.

## 다음 우선순위

1. ~~consumer 강제 종료와 재전달 시 InfluxDB 중복·덮어쓰기 수량 측정~~ — **완료(2026-09-04)**.
   남은 갈래: 같은 밀리초 키 충돌을 일부러 만들어 유실을 재현하고, identity에 밀리초
   이하 구분자(시퀀스 태그 등)를 넣을지 판단. 다중 인스턴스 리밸런싱 시 재전달 구간도 미측정
2. ~~DLQ 재처리 정책, 도구 및 운영 Runbook~~ — **완료(2026-09-04)**.
   DLQ 레코드에 실패 원인 헤더(`x-dlq-*`)를 붙이고(Java·Python 동일 규약),
   `dlq-tools/dlq.py`로 원인별 분류(transient/permanent/unknown)와 재처리를 한다.
   Runbook은 `docs/runbook/dlq-reprocessing.md`.
   남은 갈래: `vehicle-anomaly-alerts-dlq` 재처리 시 PostgreSQL 중복 알림 여부 **미검증**,
   `vehicle-telemetry-mqtt-dlq`는 payload가 envelope이라 재처리 미지원,
   분류 목록(`TRANSIENT_MARKERS`/`PERMANENT_MARKERS`)은 지금까지 본 예외만 담고 있음
3. MQTT/Kafka/InfluxDB 장애 주입과 복구 후 데이터 정합성 대조
4. 발행량·Kafka 유입량·저장 성공량을 한 화면에서 비교하는 관측성 보완
5. 위 신뢰성 작업 이후 Flutter 앱 실기기 검증과 UX 고도화

Flutter 앱 고도화는 폐기한 작업이 아니라 후순위 트랙이다. 앱 저장소의 `AGENTS.md`에 적힌
데스크톱 검증 절차를 먼저 수행한 뒤 WebSocket 재연결, 오래된 데이터 표시, 작은 화면
overflow, 접근성, 오류·빈 상태를 우선 개선한다. 새 화면이나 보여주기용 기능 추가는 그 뒤다.

---

## 차량 데이터 스펙

```json
{
  "vehicle_id": "KR-GA-1234",
  "timestamp": "2026-05-09T10:00:00Z",
  "speed": 87.3,
  "rpm": 2400,
  "engine_temp": 92.1,
  "throttle_position": 34.5,
  "fuel_level": 67.0,
  "battery_voltage": 13.8,
  "gps": {
    "lat": 37.123456,
    "lng": 127.654321
  },
  "dtc_codes": []
}
```

---

## 이상 감지 룰 (Phase 3 기준)

| 항목 | 이상 조건 |
|------|----------|
| 엔진 온도 | 105°C 초과 |
| RPM | 6000 초과 |
| 배터리 전압 | 11.5V 미만 또는 15V 초과 |
| 속도 | 200km/h 초과 |
| DTC 코드 | 배열이 비어있지 않을 때 |

---

## AI에게 요청할 때 참고사항

- 언어 기본값: **Java (Spring Boot)**, **Python**, **C** (시뮬레이터)
- 커밋과 푸시는 기본적으로 명령어만 안내하고, 사용자가 명시적으로 요청한 경우에만 직접 실행할 것
- 코드 생성 시 보안 취약점 (SQL Injection, IDOR, 인증 누락) 반드시 검토
- 새 기능 추가 전 현재 Phase가 완료됐는지 확인
- 테스트 코드는 JUnit 5 (Java), pytest (Python) 사용
- 환경변수는 `.env` 파일로 분리, 하드코딩 금지
- Docker Compose로 전체 스택 실행 가능하게 유지

---

## 구현 단계 (Phase)

### Phase 1 — 데이터 수집 파이프라인
- 차량 시뮬레이터 (MQTT Publisher)
- Mosquitto 브로커 구축 (TLS 적용)
- Kafka 클러스터 구성
- Spring Boot Kafka Consumer → InfluxDB 저장

### Phase 2 — REST API 서버
- 차량 등록/관리 API
- 실시간 데이터 조회 API
- JWT 인증/인가
- Rate Limiting

### Phase 3 — 이상 감지
- 룰 기반 이상 감지 + Kafka 이벤트 발행
- 알림 (Webhook 또는 이메일)
- (선택) Isolation Forest 머신러닝 감지

### Phase 4 — 보안 강화
- MQTT X.509 인증서 인증
- API 요청 로깅 + 이상 접근 감지
- 보안 취약점 자체 점검 및 보고서

### Phase 5 — 모니터링 & 배포
- Grafana 대시보드
- Prometheus + Spring Actuator
- Docker Compose 전체 컨테이너화
- AWS EC2 배포 (선택)
