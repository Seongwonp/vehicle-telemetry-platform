# Telemetrix 데스크탑 작업 인수인계

> 마지막 정리: 2026-09-09
>
> 기준 커밋: `5556adf06c781bc6ffdbdbdbc5358ceb6b642f65`

## 이 프로젝트에서 가장 중요한 것

Telemetrix는 차량 텔레메트리 파이프라인의 기술 개수를 늘리는 프로젝트가 아니다.
메시지 유실, 중복, 순서, offset commit, 장애 격리, 복구, 관측 가능성을 실제 코드와
반복 가능한 실험으로 증명하는 백엔드 포트폴리오다.

새로운 서비스, 데이터베이스, Kubernetes를 먼저 추가하지 않는다. 현재 위험을 재현하고,
성공 기준을 정하고, 가장 작은 변경으로 닫은 뒤 원본 증거를 남긴다.

## 작업 시작 전 읽을 문서

1. `docs/current-state-audit-2026-09-09.md` — 현재 평가, 위험, 주장-증거, 8주 계획
2. `docs/roadmap.md` — 완료/미검증 항목과 다음 작업 선택 기준
3. `docs/evidence-policy.md` — 몇 회 실행해야 어떤 표현을 쓸 수 있는지
4. `docs/issue-guidelines.md` — GitHub Issue 작성·종료 기준
5. 해당 작업의 ADR, Runbook, `load-test/<scenario>/RESULT_*.md`

`README.md`는 포트폴리오 독자를 위한 요약이다. 세부 작업 상태의 기준은 위 문서다.

## 현재 상태 요약

### 검증된 것

- `vehicle_id`가 telemetry topic의 Kafka partition key다.
- 저장(`telemetry-storage-group`)과 이상 감지(`anomaly-detector-group`)는 서로 다른
  Consumer Group이며, 이상 감지 lag이 200만 건 이상 밀려도 저장 경로가 따라갔다.
- 저장 listener는 InfluxDB 성공 또는 DLQ 발행 성공 뒤에만 수동으로 offset을 커밋한다.
- DLQ 발행 결과를 확인하며, DLQ 발행 실패 시 원본 offset을 넘기지 않는다.
- 초 단위 InfluxDB timestamp 충돌로 약 50%가 덮어써진 문제를 밀리초 정밀도로 수정했다.
- MQTT broker 90초 장애에서 재연결 상한과 queue 크기를 조정한 뒤 해당 조건 유실 0을 확인했다.
- storage consumer 강제 종료·재전달과 anomaly alert 재처리의 멱등성을 실험했다.
- 부하 중 저장 인스턴스 제거를 3회 실행했고, 정적 멤버십 ON에서 재할당 46/52/54초,
  정지 파티션 6개, topic=Influx rows로 유실 0을 확인했다.
- 정적 멤버십 OFF 대조군 1회에서는 재할당 9초, 정지 파티션 0개를 관찰했다.
- 같은 조건의 저장 처리량이 크게 흔들려 `약 57K msg/s ceiling` 주장은 철회했다.
- GitHub Actions는 Java/Testcontainers/Python/Compose smoke와 Flutter format/analyze/test를 수행한다.

### 아직 말하면 안 되는 것

- exactly-once 또는 모든 조건에서 무손실
- Kafka/시스템 전체 고가용성: broker 1대, replication factor 1이다.
- 운영 환경 최대 처리량 57K msg/s: 반복 결과가 크게 흔들려 숫자 주장을 철회했다.
- 모든 비정상 schema가 DLQ로 간다: 일부 누락/변환 경계가 아직 열린 상태다.
- 앱 실기기 동작 완료: widget/CI는 통과하지만 실제 기기 E2E는 미검증이다.
- 실제 OBD-II 연결 완료

## 다음 작업 순서

순서를 건너뛰지 않는다. 각 작업은 별도 커밋과 검증 기록으로 닫는다.

### 1. ~~P0 — evidence checksum 이식성 복구~~ — **완료(2026-09-09)**

감사가 지목한 대로였다. clean clone 전수 검사에서 **39개 중 3개**가 깨져 있었다
(전부 `.prev_offsets`, 전부 CRLF). 확장자 없는 숨김 파일이 `.gitattributes` 목록 밖이라
`core.autocrlf=true`가 적용됐고, **작업 트리에서는 원본 LF 파일이 남아 있어 안 보였다.**

git blob 해시가 manifest와 일치하므로 **증거는 처음부터 옳았고 checkout만 망가뜨렸다.**
그래서 evidence를 고치지 않고 규칙으로 살렸다.

- `.gitattributes`: `load-test/**/evidence/** text eol=lf` (확장자별 목록은 뚫린다)
- `load-test/lib/verify_evidence.sh`: 전수 검사. 통과 0 / 실패 1, CI가 본다
- `evidence_finish`: 숨김 파일 제외 + 생성 직후 자체 검증(`checksum_selfcheck.txt`)
- 실험 스크립트의 작업 파일을 evidence 밖으로

수정 전 3 실패 / 39 → 수정 후 **0 실패 / 39**(Windows clean clone, `autocrlf=true`).
`docs/verification/2026-09-09-evidence-checksum.md`.

**남은 것**: macOS 미확인. (Windows clean clone과 Linux CI 모두 39/39 통과 확인됨.)

### 2. P0 — strict telemetry schema

현재 열린 경계:

- DTO의 숫자 필드가 primitive라 누락되면 0이 될 수 있다.
- `rpm: 2000.7`은 2000으로 잘려 저장된다.
- `dtc_codes: [null]`은 `"null"`로 저장된다.
- 쉼표가 포함된 DTC 한 개와 DTC 두 개는 저장 후 구분되지 않는다.
- MQTT ingress는 Bean Validation을 하지만 Kafka 직접 주입은 이 검증을 우회한다.

구현 전에 missing/null/wrong type/range를 포함한 decision table과 실패 fixture부터 만든다.
그 뒤 nullable wrapper + `@NotNull`, DTC 원소 형식 검증, 공통 decoder/validator를 검토한다.

완료 조건:

- MQTT와 Kafka 직접 입력이 같은 payload를 같은 이유로 거부
- invalid 메시지의 DLQ 귀속이 명확함
- 정상 payload 회귀 없음
- 단위 테스트 + Testcontainers 계약 + MQTT→Kafka→DB E2E

### 3. P1 — Redis 장애 정책

현재 Redis가 중단되면 rate limit/brute-force interceptor 예외로 API가 500이 되고 refresh도
중단된다. endpoint별 fail-open/fail-closed 정책을 먼저 정한다. 인증/refresh와 일반 조회를
한 정책으로 묶지 않는다.

### 4. P1 — 이벤트 상관관계

HTTP MDC traceId만으로는 MQTT→Kafka→InfluxDB/Python→PostgreSQL/WebSocket 한 레코드를
찾기 어렵다. OpenTelemetry 전체 도입보다 payload/header의 `event_id`를 먼저 검토한다.

### 5. P1 — Flutter 실기기 검증

앱 저장소: `../vehicle-telemetry-app`

- 앱의 `AGENTS.md`와 `CLAUDE.md`를 먼저 읽는다.
- Android 실제 기기 또는 에뮬레이터에서 로그인, refresh, WebSocket 재연결, stale,
  out-of-order, 동일 timestamp, 로그아웃을 검증한다.
- macOS integration test는 secure storage Keychain entitlement 문제로 로그인 단계에서 막힌다.
- 사용한 앱·백엔드 commit, SDK, device, API URL, 로그와 스크린샷을 기록한다.

## 데스크탑 시작 절차

### 저장소 동기화

```powershell
git checkout main
git pull --ff-only
git status --short --branch
git log -1 --oneline
```

사용자 변경사항이 있으면 덮거나 정리하지 않는다. 충돌 가능성이 있으면 작업을 멈추고
변경 파일을 먼저 확인한다.

### 백엔드 빠른 검증

```powershell
cd backend
./gradlew test --no-daemon
cd ..

python -m pip install -r anomaly-detector/requirements.txt pytest
$env:PYTHONPATH = "anomaly-detector"
pytest anomaly-detector/tests -q
```

Docker가 실행 중이어야 Testcontainers 계약 5종이 실제로 수행된다. skip이 있으면 전체 통과로
기록하지 않는다. CI는 계약 결과 파일 5개와 skipped 0을 별도로 강제한다.

### Compose 검증

```powershell
docker compose config --quiet
docker compose build
docker compose --profile simulator up -d
docker compose ps
curl.exe --fail http://localhost:8080/actuator/health
```

기본 Compose는 mTLS 8883이다. 평문은 명시적 dev override에서만 사용한다. 실험이 끝난 뒤
volume을 삭제하는 `docker compose down -v`는 사용자 요청 또는 실험 절차가 명확할 때만 한다.

### scale 실험 전 확인

- 파티션 3개에서 backend 하나의 concurrency 3이 이미 모두 차지한다.
- 저장 인스턴스를 늘리기 전에 필요한 파티션 수를 먼저 확보한다.
- 실행 중 파티션을 늘리면 consumer가 `metadata.max.age.ms`까지 새 파티션을 못 볼 수 있다.
- 저장 전용 인스턴스는 `MQTT_INGEST_ENABLED=false`여야 한다.
- 각 인스턴스의 `GROUP_INSTANCE_ID_BASE`가 고유해야 한다.
- 정적 멤버십을 끌 때는 `GROUP_INSTANCE_ID_BASE=` 빈 값을 사용한다. `${VAR:-default}`로
  바꾸면 빈 값이 기본값으로 돌아가므로 OFF 대조군이 조용히 무효화된다.

## 실험 규칙

1. 가설, 변경하지 않은 대조군, 성공 기준을 실행 전에 적는다.
2. 발행 시도 수가 아니라 가능한 경우 MQTT PUBACK 또는 Kafka end offset을 기준량으로 쓴다.
3. 집계 파일만 믿지 않고 원본 로그에서 표본을 대조한다.
4. 안정성 주장은 같은 조건 3회 이상에서만 한다.
5. 평균만 쓰지 않고 p95/p99, 변동 폭, 오류율, lag, 저장 성공량을 함께 남긴다.
6. 회차 사이 휴식, 실행 순서, 호스트 온도·클럭을 기록한다.
7. 실패한 실행과 잘못된 측정 도구도 지우지 않고 무효 사유를 적는다.
8. `evidence/`를 삭제하지 않는다. 임시 파일도 증거인지 확인한 뒤 다룬다.

## 문서 역할

| 문서 | 역할 |
| --- | --- |
| `README.md` | 문제 → 설계 결정 → 검증 결과 → 한계의 포트폴리오 요약 |
| `docs/current-state-audit-2026-09-09.md` | 현재 평가, 위험, 주장-증거, 8주 계획 |
| `docs/roadmap.md` | 완료/진행/미검증 작업의 기준 상태 |
| `docs/architecture-decisions.md` | 선택하거나 선택하지 않은 이유와 trade-off |
| `load-test/**/RESULT_*.md` | 한 실험의 조건, 결과, 적용 범위와 한계 |
| `load-test/**/evidence/` | 재계산 가능한 원본 |
| `docs/runbook/` | 장애 판단과 복구 절차 |
| `docs/devlog.md` | 어디서 틀렸고 무엇이 그 오류를 드러냈는지 |

코드에 긴 실험 서사를 반복하지 않는다. 코드 주석은 안전성 불변식과 삭제하면 안 되는 이유만
남기고, 상세 수치와 과정은 RESULT/ADR/devlog로 이동한다.

## 작업 완료 조건

- 관련 단위·계약·E2E 테스트가 범위에 맞게 통과한다.
- Docker 또는 기기가 없어 실행하지 못한 검증은 `미검증`으로 남긴다.
- 결과 문서에 commit, 환경, 명령, 원본 evidence, 성공 기준, 한계를 적는다.
- roadmap의 완료 상태와 남은 갈래를 갱신한다.
- 동작을 바꾼 결정이면 ADR과 Runbook을 확인한다.
- README에는 채용 독자가 이해할 핵심 결과만 반영한다.
- 사용자가 명시적으로 요청한 경우에만 commit/push한다.

## 기술 기준

- Java 17 / Spring Boot 3 / JUnit 5
- Python 3.11 / pytest
- MQTT QoS 1 / Kafka at-least-once
- PostgreSQL / InfluxDB / Redis
- Prometheus / Grafana / Docker Compose
- 비밀정보는 `.env`로 분리하고 추적하지 않는다.
- 실제 근거 없이 `고가용성`, `무손실`, `exactly-once`, `운영 준비 완료`라고 쓰지 않는다.
