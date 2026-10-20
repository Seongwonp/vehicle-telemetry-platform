# Telemetrix 작업 인수인계

> 마지막 정리: 2026-10-20
>
> 기준 커밋: `c12c003` (main). 기준점 태그 `v0.9-pre-vehicle` = `911c2e4`(실차 작업 전, CI success)

## 이 프로젝트에서 가장 중요한 것

Telemetrix는 차량 텔레메트리 파이프라인의 기술 개수를 늘리는 프로젝트가 아니다.
메시지 유실, 중복, 순서, offset commit, 장애 격리, 복구, 관측 가능성을 실제 코드와
반복 가능한 실험으로 증명하는 백엔드 포트폴리오다.

새로운 서비스, 데이터베이스, Kubernetes를 먼저 추가하지 않는다. 현재 위험을 재현하고,
성공 기준을 정하고, 가장 작은 변경으로 닫은 뒤 원본 증거를 남긴다.

## 작업 시작 전 읽을 문서

1. `docs/RESUME.md` — **현재 상태의 기준.** 일시 중지 사유, 노트북 로컬 상태, 하지 말 것, 다음 할 일
2. `docs/roadmap.md` — 완료/미검증 항목과 다음 작업 선택 기준
3. `docs/evidence-policy.md` — 몇 회 실행해야 어떤 표현을 쓸 수 있는지
4. `docs/issue-guidelines.md` — GitHub Issue 작성·종료 기준
5. 해당 작업의 ADR, Runbook, `load-test/<scenario>/RESULT_*.md`, 계획 문서(`docs/plans/`)

`README.md`는 채용 독자용 짧은 요약이고, 긴 판은 `docs/portfolio-detail.md`다. 세부 작업 상태의 기준은 위 문서다.

## 현재 상태 요약 (2026-10-20)

**OBD-II 동글(OBDLink EX)이 도착할 때까지 일시 중지.** 상세는 `docs/RESUME.md`.

- 기준점 `v0.9-pre-vehicle`(`911c2e4`) 뒤로는 문서·증거 도구(`evidence_finish`의 CRLF 정규화)·셸 스크립트 LF 규칙·주석 정리(주석만)·
  `RESUME.md`, 그리고 **동작 변경 1건**(2026-10-17 `5ac5008`·`ac15c05`·`5be75bc`): 차량 비활성화·비밀번호 변경/초기화 시 그 인스턴스의
  WebSocket 세션 종료(ADR-027 갱신 — 종료 동작을 확인하는 테스트 없음), 인프라 포트 `127.0.0.1` 게시, `MQTT_TLS_STORE_PASSWORD`·`GRAFANA_PASSWORD` 필수.
  확인: `git diff --stat v0.9-pre-vehicle HEAD -- backend/src/main anomaly-detector obd-bridge/obd_bridge docker-compose.yml`.
- 테스트: 백엔드 447건·실패 0·skip 0(2026-10-20, Docker 실행 중) · 감지기 pytest 232 · obd-bridge pytest 94.
- 끝난 것(기준점까지): 수동 MQTT ACK(ADR-029)·spool·DLQ·관측(`MqttAckWaitStuck` 대조군 1회), 선택 필드 fuel_level·battery_voltage(ADR-030,
  `last()` 옛 값 섞임 수정 포함), 인증서 접근 제한·CRL, Grafana 등록 차량 필터, 이벤트 상관관계 1단계(ADR-028, 2단계 미착수).
- `obd-bridge/`는 **프로토타입**이다. ELM327-emulator 리허설만 했다(2026-10-09, 각 1회 관찰: 60/60 acked = Kafka = InfluxDB,
  v5 ACL 음성 시험 0x87×18이 spool에 남음). **실동글·실차는 미검증.**
- 아직 말하면 안 되는 것: exactly-once·모든 조건 무손실, 고가용성(broker 1대, RF=1), 최대 처리량 수치(철회),
  운영 준비 완료, 실차 검증 완료, 앱 실기기 E2E 완료.

검증된 항목별 근거와 범위는 README·`docs/portfolio-detail.md`·roadmap에 있다. 2026-09-12 판의 "검증된 것"·
"다음 작업 순서"(P0-1, P0-2, Redis, 이벤트 상관관계, Flutter) 서사는 이력이다 — `docs/devlog.md`, `docs/roadmap.md`,
`docs/HANDOFF_*.md`에서 본다.

## 다음 작업 순서

`docs/RESUME.md` §1·§2를 따른다. 요약: 동글 정차 수집(코나 2017 가솔린 → 그랜저 2024 하이브리드,
`docs/plans/2026-10-10-dongle-stationary-collection.md`) → 주행 계획 → VM 배포(사람이 직접) → 포트폴리오 마무리.
ACK 구조 변경(C안)·WebSocket 세션 종료의 다중 인스턴스 확장 같은 새 경계 사례는 실차 한 사이클 전에는 하지 않는다.

## 시작 절차

데스크탑과 노트북(`LAPTOP-MV7C23S3`) 두 PC에서 작업한다. 인증서·`.env`·로컬 DB 상태는 PC마다 다르다(`docs/RESUME.md` §3).

### 저장소 동기화

```powershell
git checkout main
git fetch
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

Docker가 실행 중이어야 Testcontainers 계약 8종(`backend/src/test/java/com/telemetry/contract/`)이 실제로 수행된다.
skip이 있으면 전체 통과로 기록하지 않는다. CI는 계약 결과 파일 8개와 skipped 0을 별도로 강제한다(`.github/workflows/ci.yml`).
obd-bridge는 `obd-bridge/README.md`의 Python 3.12 venv에서 `pytest`(Windows는 `--basetemp` 필요할 수 있음).

### Compose 검증

```powershell
docker compose config --quiet
docker compose build
docker compose --profile simulator up -d
docker compose ps
curl.exe --fail http://localhost:8080/actuator/health
```

기본 Compose는 mTLS 8883이다. 평문은 명시적 dev override에서만 사용한다. 실험이 끝나도
volume을 삭제하는 `docker compose down -v`는 하지 않는다(볼륨에 실차 등록·MQTT 세션이 있다 — `docs/RESUME.md` §4).
정리가 필요하면 `docker compose stop`.
동글 수집 중에는 `--profile simulator`를 쓰지 않는다(Kafka offset 차이를 그 차량에만 귀속시키기 위해 — 동글 계획 문서).

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

## 저장소·인증서·증거 규칙 (2026-09-12 이후 추가)

- 커밋에 AI 공동저자 트레일러(`Co-Authored-By` 등)를 넣지 않는다. 푸시한 커밋·태그(`v0.9-pre-vehicle`)를 재작성하지 않는다.
- 두 PC에서 작업하므로 **커밋 전에 `git fetch`**로 다른 PC의 커밋부터 확인한다.
- 증거 manifest는 커밋될 바이트(LF)로 계산돼야 한다. `evidence_finish`가 manifest 계산 전에 최상위 텍스트 증거의 CRLF를 LF로 맞추고
  `eol_normalized.txt`에 남긴다(`load-test/lib/README.md`). 하위 디렉터리에 manifest를 따로 만들면 그쪽은 직접 LF로 쓴다.
  이미 커밋된 증거는 고치지 않는다 — 어긋나면 `.gitattributes`에 그 폴더만 `-text` 예외(기존 예외 4개 참고).
- 셸 스크립트는 전부 LF(`.gitattributes`의 `*.sh text eol=lf`). CRLF로 체크아웃되면 `kafka-init`이 실패한다.
- **`broker/certs/generate-certs.sh`를 차량(코나·그랜저) 사이에 다시 돌리지 않는다** — CA가 새로 만들어져 기존 인증서·CRL 지문이 전부 무효가 된다.
  다른 PC의 `ca.key`를 복사하지 않는다. 두 PC의 CA는 서로 호환되지 않는다.
- **TLS 호스트명 검증을 끄지 않는다**(`tls_insecure_set`, `CERT_NONE` 금지). SAN 없는 서버 인증서는 hosts `127.0.0.1 mosquitto` 매핑으로 붙는다.
- `broker/certs/`에 범위 없는 `cat`/`grep`을 하지 않는다. 비밀값·키 본문을 출력하지 않는다.
- `docker compose down -v`·볼륨 삭제 금지. 운영 client ID(`telemetry-backend`·`-sys`)로 clean session 접속 금지.

## 문서 역할

| 문서 | 역할 |
| --- | --- |
| `README.md` | 문제 → 설계 결정 → 검증 결과 → 한계의 포트폴리오 요약 |
| `docs/RESUME.md` | 지금 상태·재개 방법·하지 말 것(가장 최신) |
| `docs/HANDOFF_<날짜>.md` | 그날의 커밋·결정·보류 기록(그 날짜의 기록이라 고치지 않는다) |
| `docs/plans/` | 실행 전에 고정한 가설·성공 기준·안전 규칙 |
| `docs/current-state-audit-2026-09-09.md` | 2026-09-09 시점 평가, 위험, 주장-증거(이력) |
| `docs/roadmap.md` | 완료/진행/미검증 작업의 기준 상태 |
| `docs/telemetry-schema-decision-table.md` | **입력 계약의 단일 기준** — 변경 전 측정, 정한 범위, 변경 후 검증 |
| `docs/references/telemetry-engineering-reference.md` | 참고 자료 → 채택한 정책 → 코드·검증 연결. 자료의 성격 구분 |
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
- Python 3.11 / pytest (감지기·시뮬레이터·CI). obd-bridge는 노트북 Python 3.12 venv에서 확인
- OBD-II: ELM327 계열(OBDLink EX) → `obd-bridge` → MQTT v5 mTLS (프로토타입, 실차 미검증)
- MQTT QoS 1 / Kafka at-least-once
- PostgreSQL / InfluxDB / Redis
- Prometheus / Grafana / Docker Compose
- 비밀정보는 `.env`로 분리하고 추적하지 않는다.
- 실제 근거 없이 `고가용성`, `무손실`, `exactly-once`, `운영 준비 완료`라고 쓰지 않는다.
