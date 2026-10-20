# 재개 문서 — 다음에 이 프로젝트를 여는 사람(또는 도구)이 처음 읽는다

> 마지막 갱신: 2026-10-20. **프로젝트는 OBD-II 동글이 도착할 때까지 일시 중지 상태다.**
> 사람이든 AI 도구든 이 문서 → `.claude/CLAUDE.md`(작업 규칙) → 아래 "다음 할 일"의 계획 문서 순서로 읽는다.

## 1. 지금 어디인가

| | |
| --- | --- |
| 기준점 | 태그 `v0.9-pre-vehicle` — 백엔드 `911c2e4`, 앱 `0a4e1b4`. 실차 작업 전 마지막 기준 |
| 그 뒤에 바뀐 것 | 문서·증거 도구·주석 정리(주석만)·셸 스크립트 줄끝 규칙, 그리고 **동작 변경 1건**: 2026-10-17 `5ac5008`(테스트 `ac15c05`, CI `5be75bc`) — 차량 비활성화·비밀번호 변경/초기화 시 그 인스턴스의 WebSocket 세션 종료(ADR-027 갱신, 종료 동작 테스트 없음), Kafka·InfluxDB·PostgreSQL·Redis·Prometheus·Alertmanager·Grafana 포트 `127.0.0.1` 게시, `MQTT_TLS_STORE_PASSWORD`·`GRAFANA_PASSWORD` 필수. 노트북 리허설(10-09)은 이 변경 **전** 코드로 돌았다. 확인: `git diff --stat v0.9-pre-vehicle HEAD -- backend/src/main anomaly-detector obd-bridge/obd_bridge docker-compose.yml` |
| 테스트 | 백엔드 전체 **447건, 실패 0, skip 0**(2026-10-20 HEAD, Docker 실행 중). 감지기 pytest 232(2026-10-08, `HANDOFF_2026-10-08.md`), obd-bridge pytest 94(2026-10-09, `obd-bridge/README.md`) |
| 끝난 것 | 파이프라인 견고화(수동 ACK·spool·DLQ·관측), 선택 필드(ADR-030), 인증서 접근 제한·CRL, 노트북 리허설 2회(에뮬레이터, MQTT v5 확정) |
| 다음 | 동글 정차 수집(코나 2017 가솔린 → 그랜저 2024 하이브리드) → 주행 계획 → VM 배포 → 포트폴리오 마무리 |
| 일부러 안 하는 것 | ACK 구조 변경(C안), WebSocket 종료의 다중 인스턴스 확장·통합 테스트, 알림 재측정 같은 새 경계 사례. 실차 한 사이클 완성이 먼저다 |

최근 결정과 상태는 `docs/HANDOFF_2026-10-10.md`, 그 이전 맥락은 같은 폴더의 이전 인계 문서.

## 2. 동글이 오면 할 일

계획 문서 [`docs/plans/2026-10-10-dongle-stationary-collection.md`](plans/2026-10-10-dongle-stationary-collection.md)를 그대로 따른다.
가설·성공 기준·안전 규칙·증거 경로가 실행 전에 고정돼 있다. 순서만 요약하면:

1. 노트북 로컬 상태 확인(아래 §3 표) — 하나라도 없으면 복구부터.
2. 스택을 **mTLS 모드·시뮬레이터 없이** 기동(계획 문서 "노트북 준비 상태 → 남은 수동 단계").
3. 리허설 1을 한 번 더 돌려 연결이 살아 있는지 확인(에뮬레이터 60초).
4. 차에서: OBDwiz로 지원 PID 비트맵 → 정차 수집 10분 → 폴링 간격 → 발행·PUBACK·Kafka·InfluxDB 대조 → 브로커 정지 시험.
5. 결과는 `load-test/dongle-stationary/evidence/<UTC>-<차량>-<단계>/`에 남기고 `bash load-test/lib/verify_evidence.sh`로 확인.

확정된 결정(계획 문서의 "결정 대기" 표보다 이 줄이 최신이다):
D1 호스트명 = hosts 매핑 `127.0.0.1 mosquitto`(검증 끄기 금지) · D2 차량 ID = `KONA17-01`, `GRDR24H-01` ·
D3 MQTT = v5 · D4 `measure_polling.py --note` 필수 인자(해결) · D5 ML 오프라인 채점 = 보류.

## 3. 저장소에 없는 노트북 로컬 상태 (`LAPTOP-MV7C23S3`)

git이 추적하지 않는다. 다른 PC나 재설치 뒤에는 없다.

| 무엇 | 확인 | 잃었을 때 |
| --- | --- | --- |
| `broker/certs/` — **노트북 전용 CA**와 `KONA17-01`·`GRDR24H-01` 인증서 | `ls broker/certs/vehicles` | `MSYS_NO_PATHCONV=1 MQTT_VEHICLE_IDS="SIM-001,SIM-002,SIM-003,KONA17-01,GRDR24H-01" bash broker/certs/generate-certs.sh` **한 번만**. 그 뒤 `generate-crl.sh` |
| `.env`의 `MQTT_TLS_STORE_PASSWORD`·`GRAFANA_PASSWORD`(`5ac5008`부터 필수) | 키 이름만 확인(값 출력 금지) | 없으면 `docker compose`가 시작하지 않는다. `MQTT_TLS_STORE_PASSWORD`는 `generate-certs.sh`가 p12를 만들 때 쓴 값(그때 환경변수가 없었으면 스크립트 기본값)과 같아야 한다 |
| `.env`의 `GRAFANA_PG_READER_PASSWORD` | 키 이름만 확인(값 출력 금지) | 새 값을 넣고 스택 기동 → `sh scripts/grafana-pg-reader.sh` → grafana만 재생성 |
| PostgreSQL에 실차 등록(`KONA17-01`, `GRDR24H-01`, 소유자 admin) | `GET /api/vehicles` | 관리자로 `POST /api/vehicles` |
| `obd-bridge/.venv`(Python 3.12) | `obd-bridge/.venv/Scripts/python --version` | `obd-bridge/README.md`의 3.12 venv 명령 |
| `obd-bridge/.env`(TLS 경로·`mosquitto:8883`·v5·`KONA17-01`) | 파일 존재 | 계획 문서 "노트북 준비 상태"의 항목으로 다시 작성 |
| Windows hosts의 `127.0.0.1 mosquitto` | `Select-String -Path "$env:SystemRoot\System32\drivers\etc\hosts" -Pattern mosquitto` | 관리자 PowerShell: `Add-Content -Path "$env:SystemRoot\System32\drivers\etc\hosts" -Value "`r`n127.0.0.1 mosquitto" -Encoding ASCII` |

데스크탑은 별도 CA·포트 18883·SIM-024/088 폐기 상태를 가진다. **두 PC의 인증서는 서로 호환되지 않는다.**

## 4. 하지 말 것

- **인증서 생성 스크립트를 코나와 그랜저 사이에 다시 돌리지 않는다** — CA가 바뀌어 앞서 만든 인증서가 전부 무효가 된다.
- 다른 PC의 CA 개인키(`ca.key`)를 복사하지 않는다. `broker/certs/`에 범위 없는 `cat`/`grep`을 하지 않는다. 비밀값·키 본문을 출력하지 않는다.
- TLS 호스트명 검증을 끄지 않는다(`tls_insecure_set`, `CERT_NONE` 금지).
- `docker compose down -v`·볼륨 삭제 금지. 운영 client ID(`telemetry-backend`·`-sys`)로 clean session 접속 금지.
- 수집 중 시뮬레이터를 띄우지 않는다. 주행 중 노트북을 조작하지 않는다. OBDwiz의 DTC 삭제·ECU 쓰기 메뉴를 누르지 않는다.
- 공개 증거에 GPS·주소·번호판·VIN 전체를 넣지 않는다.
- 커밋에 AI 공동저자 트레일러를 넣지 않는다. 푸시한 커밋·태그를 재작성하지 않는다. 커밋 전에 `git fetch`로 다른 PC의 커밋부터 확인한다.

## 5. 사람이 직접 할 것 (AI에게 맡기지 않기로 한 것)

- VM 배포(클라우드 계정·예산 알림·Terraform·SAN 포함 서버 인증서·재시작 복구 확인).
- 포트폴리오 최종본: 전/후 그래프 한 장, 실차 30초 영상, README 실차 결과 한 줄, 면접 자료 정돈.
- 핵심 결정을 AI 없이 다시 설명하고 고쳐 보기: ADR-012(수동 commit), ADR-019(spool), ADR-027(소유권), ADR-030(선택 필드),
  인증 경로(JWT 필터·401/403·소유권 검사). 후보 리팩터링: `DiagnosisService`(LLM 호출을 인터페이스 뒤로).
