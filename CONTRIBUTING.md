# 기여 가이드

이 저장소에서 작업하는 사람이 지켜야 할 원칙, 검증 방법, 실험·증거 규칙을 정리한다.
프로젝트 소개는 [`README.md`](README.md), 설계 결정은 [`docs/architecture-decisions.md`](docs/architecture-decisions.md)에 있다.

## 프로젝트 원칙

Telemetrix는 차량 텔레메트리 파이프라인의 기술 개수를 늘리는 프로젝트가 아니다.
메시지 유실, 중복, 순서, offset commit, 장애 격리, 복구, 관측 가능성을 실제 코드와
반복 가능한 실험으로 증명하는 백엔드 프로젝트다.

- 새로운 서비스, 데이터베이스, Kubernetes를 먼저 추가하지 않는다. 현재 위험을 재현하고,
  성공 기준을 정하고, 가장 작은 변경으로 닫은 뒤 원본 증거를 남긴다. 분리·신규 저장소도 현재 문제와 측정 근거가 있을 때만 검토한다.
- 기능 추가보다 유실·중복·순서·장애 복구 주장의 재현성을 먼저 높인다.
- 보안 우선: 모든 통신은 TLS, 인증 없는 엔드포인트를 두지 않는다. 변경마다 SQL Injection, IDOR, 인증 누락을 검토한다.
- 시뮬레이터와 실제 OBD-II 동글을 바꿔 끼울 수 있게 인터페이스를 분리한다.
- 비밀정보는 `.env`로 분리하고 추적하지 않는다. 하드코딩하지 않는다.
- Docker Compose로 전체 스택을 실행할 수 있는 상태를 유지한다.
- 근거 없이 `고가용성`, `무손실`, `exactly-once`, `운영 준비 완료`라고 쓰지 않는다.

## 작업 전에 읽을 문서

1. [`docs/RESUME.md`](docs/RESUME.md) — 지금 상태와 다음 할 일
2. [`docs/roadmap.md`](docs/roadmap.md) — 완료/미검증 항목과 다음 작업 선택 기준
3. [`docs/evidence-policy.md`](docs/evidence-policy.md) — 몇 회 실행해야 어떤 표현을 쓸 수 있는지
4. [`docs/issue-guidelines.md`](docs/issue-guidelines.md) — GitHub Issue 작성·종료 기준
5. 해당 작업의 ADR, Runbook, `load-test/<scenario>/RESULT_*.md`, 계획 문서(`docs/plans/`)

`README.md`는 짧은 요약이고, 긴 판은 [`docs/portfolio-detail.md`](docs/portfolio-detail.md)다.

## 빌드와 테스트

```bash
cd backend
./gradlew test --no-daemon
cd ..

python -m pip install -r anomaly-detector/requirements.txt pytest
PYTHONPATH=anomaly-detector pytest anomaly-detector/tests -q   # PowerShell: $env:PYTHONPATH = "anomaly-detector"
```

- 언어와 테스트 도구: Java 17 / Spring Boot 3 / JUnit 5, Python 3.11 / pytest(감지기·시뮬레이터·CI).
- Docker가 실행 중이어야 Testcontainers 계약 8종(`backend/src/test/java/com/telemetry/contract/`)이 실제로 수행된다.
  **skip이 있으면 전체 통과로 기록하지 않는다.** CI는 계약 결과 파일 8개와 skipped 0을 별도로 강제한다(`.github/workflows/ci.yml`).
- `obd-bridge/`는 `obd-bridge/README.md`의 Python 3.12 venv에서 `pytest`로 확인한다(Windows는 `--basetemp`가 필요할 수 있다).

## Compose 검증

```bash
docker compose config --quiet
docker compose build
docker compose --profile simulator up -d
docker compose ps
curl --fail http://localhost:8080/actuator/health
```

- 기본 Compose는 MQTT mTLS(8883)다. 평문은 `docker-compose.dev.yml`을 명시한 개발 override에서만 쓴다.
- `docker compose down -v`(볼륨 삭제)는 하지 않는다. 정리가 필요하면 `docker compose stop`.
- 실차(동글) 수집 중에는 `--profile simulator`를 쓰지 않는다. Kafka offset 차이를 그 차량에만 귀속시키기 위해서다.

### scale 실험 전 확인

- 파티션 3개에서 backend 하나의 concurrency 3이 이미 모두 차지한다. 저장 인스턴스를 늘리기 전에 필요한 파티션 수를 먼저 확보한다.
- 실행 중 파티션을 늘리면 consumer가 `metadata.max.age.ms`까지 새 파티션을 못 볼 수 있다.
- 저장 전용 인스턴스는 `MQTT_INGEST_ENABLED=false`여야 하고, 각 인스턴스의 `GROUP_INSTANCE_ID_BASE`가 고유해야 한다.
- 정적 멤버십을 끌 때는 `GROUP_INSTANCE_ID_BASE=` 빈 값을 사용한다. `${VAR:-default}`로 바꾸면 빈 값이
  기본값으로 돌아가므로 OFF 대조군이 조용히 무효화된다.

## 실험 규칙

1. 가설, 변경하지 않은 대조군, 성공 기준을 실행 전에 적는다.
2. 발행 시도 수가 아니라 가능한 경우 MQTT PUBACK 또는 Kafka end offset을 기준량으로 쓴다.
3. 집계 파일만 믿지 않고 원본 로그에서 표본을 대조한다.
4. 안정성 주장은 같은 조건 3회 이상에서만 한다.
5. 평균만 쓰지 않고 p95/p99, 변동 폭, 오류율, lag, 저장 성공량을 함께 남긴다.
6. 회차 사이 휴식, 실행 순서, 호스트 온도·클럭을 기록한다.
7. 실패한 실행과 잘못된 측정 도구도 지우지 않고 무효 사유를 적는다.
8. `evidence/`를 삭제하지 않는다. 임시 파일도 증거인지 확인한 뒤 다룬다.

## 저장소·인증서·증거 규칙

- 푸시한 커밋·태그(`v0.9-pre-vehicle`)를 재작성하지 않는다. 여러 PC에서 작업하면 커밋 전에 `git fetch`로 원격 커밋부터 확인한다.
- 증거 manifest는 커밋될 바이트(LF)로 계산돼야 한다. `evidence_finish`가 manifest 계산 전에 최상위 텍스트 증거의 CRLF를 LF로 맞추고
  `eol_normalized.txt`에 남긴다([`load-test/lib/README.md`](load-test/lib/README.md)). 하위 디렉터리에 manifest를 따로 만들면 그쪽은 직접 LF로 쓴다.
  이미 커밋된 증거는 고치지 않는다 — 어긋나면 `.gitattributes`에 그 폴더만 `-text` 예외를 둔다(기존 예외 참고).
- 셸 스크립트는 전부 LF다(`.gitattributes`의 `*.sh text eol=lf`). CRLF로 체크아웃되면 `kafka-init`이 실패한다.
- `broker/certs/generate-certs.sh`를 차량 사이에 다시 돌리지 않는다 — CA가 새로 만들어져 기존 인증서·CRL 지문이 전부 무효가 된다.
  다른 환경의 `ca.key`를 복사하지 않는다. 환경마다 CA가 다르다.
- TLS 호스트명 검증을 끄지 않는다(`tls_insecure_set`, `CERT_NONE` 금지). SAN 없는 서버 인증서는 hosts `127.0.0.1 mosquitto` 매핑으로 붙는다.
- `broker/certs/`에 범위 없는 `cat`/`grep`을 하지 않는다. 비밀값·키 본문을 출력하지 않는다.
- 운영 client ID(`telemetry-backend`·`-sys`)로 clean session 접속하지 않는다.

## 문서 역할

| 문서 | 역할 |
| --- | --- |
| `README.md` | 문제 → 설계 결정 → 검증 결과 → 한계의 요약 |
| `docs/RESUME.md` | 지금 상태·재개 방법·하지 말 것 |
| `docs/plans/` | 실행 전에 고정한 가설·성공 기준·안전 규칙 |
| `docs/current-state-audit-2026-09-09.md` | 2026-09-09 시점 평가, 위험, 주장-증거(이력) |
| `docs/roadmap.md` | 완료/진행/미검증 작업의 기준 상태 |
| `docs/telemetry-schema-decision-table.md` | **입력 계약의 단일 기준** — 변경 전 측정, 정한 범위, 변경 후 검증 |
| `docs/references/telemetry-engineering-reference.md` | 참고 자료 → 채택한 정책 → 코드·검증 연결 |
| `docs/architecture-decisions.md` | 선택하거나 선택하지 않은 이유와 trade-off |
| `load-test/**/RESULT_*.md` | 한 실험의 조건, 결과, 적용 범위와 한계 |
| `load-test/**/evidence/` | 재계산 가능한 원본 |
| `docs/runbook/` | 장애 판단과 복구 절차 |
| `docs/devlog.md` | 어디서 틀렸고 무엇이 그 오류를 드러냈는지 |

코드에 긴 실험 서사를 반복하지 않는다. 코드 주석은 안전성 불변식과 삭제하면 안 되는 이유만 남기고,
상세 수치와 과정은 RESULT/ADR/devlog로 옮긴다.

## 작업 완료 조건

- 관련 단위·계약·E2E 테스트가 범위에 맞게 통과한다.
- Docker 또는 기기가 없어 실행하지 못한 검증은 `미검증`으로 남긴다.
- 결과 문서에 commit, 환경, 명령, 원본 evidence, 성공 기준, 한계를 적는다.
- roadmap의 완료 상태와 남은 갈래를 갱신한다.
- 동작을 바꾼 결정이면 ADR과 Runbook을 확인한다.
- README에는 읽는 사람이 이해할 핵심 결과만 반영한다.

## 기술 기준

- Java 17 / Spring Boot 3 / JUnit 5
- Python 3.11 / pytest. `obd-bridge`는 Python 3.12
- OBD-II: ELM327 계열 → `obd-bridge` → MQTT v5 mTLS (프로토타입, 실차 미검증)
- MQTT QoS 1 / Kafka at-least-once
- PostgreSQL / InfluxDB / Redis
- Prometheus / Grafana / Docker Compose
