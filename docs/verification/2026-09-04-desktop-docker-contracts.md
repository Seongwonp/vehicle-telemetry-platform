# 2026-09-04 데스크톱(Docker) 인프라 계약 검증

`2026-09-04-local-baseline.md`에서 **미검증**으로 남겨둔 항목을 Docker가 있는 데스크톱에서
실행했다. 그 문서의 계약 5개 클래스는 노트북에 Docker가 없어 전부 skip됐고,
`KafkaStorageFailureContractTest`에 새로 추가한 계약은 `컴파일 검증` 상태였다.

## 실행 기준

- Git commit: `e4effc2c39fd29cee403ae16a9346188f1d66560`
- Branch: `main` (`origin/main`과 일치, 실행 시점에 로컬 변경 없음)
- OS: Windows 11 Pro 10.0.26200
- Java: OpenJDK 17.0.15 LTS
- Gradle: 8.7 (`--no-daemon`)
- Docker: Docker Desktop, Engine 29.7.2

## 결과 — 계약 테스트가 실제로 실행됐다

CLAUDE.md에 적힌 명령을 그대로 실행했다.

```powershell
cd backend
.\gradlew.bat test --tests "com.telemetry.contract.KafkaStorageFailureContractTest" --no-daemon
```

`BUILD SUCCESSFUL in 1m 13s`. XML 결과 기준 **tests=3, failures=0, errors=0, skipped=0**
(총 18.02초):

| 케이스 | 결과 | 소요 |
| --- | --- | ---: |
| `permanentBatchFailurePublishesEveryRecordToDlqAndCommitsPastBatch()` | PASS | 8.209s |
| `dlqPublishFailureLeavesOffsetAndRestartRedeliversSourceRecord()` | PASS | 6.460s |
| `permanentInfluxFailurePublishesToDlqBeforeCommittingSourceOffset()` | PASS | 3.349s |

즉 CLAUDE.md에 나열된 다섯 계약이 **실제로 검증됐다** — 3건 배치의 최초 1회 + 재시도 2회,
재시도 소진 후 세 레코드의 key·원본 payload가 모두 DLQ에 존재, source offset이 배치
마지막 레코드 다음으로 전진, DLQ 발행 실패 시 offset 미커밋, 동일 Consumer Group 재시작
시 미커밋 원본 재전달과 저장 성공 후 offset 전진.

## 결과 — 전체 백엔드 테스트

```powershell
.\gradlew.bat test --no-daemon
```

`BUILD SUCCESSFUL in 1m 38s`. **96개 중 실패 0, 에러 0, skip 0.**
기준선 문서의 "95개 중 89개 통과, 6개 skip"과 비교하면, Docker가 있으니 계약 6건이
skip되지 않고 실행됐고 새 케이스 1건이 늘어 96개가 됐다.

인프라 계약 5개 클래스 모두 skip 없이 실행:

| 계약 테스트 | tests | skip | fail |
| --- | ---: | ---: | ---: |
| `FlywayPostgresContractTest` | 1 | 0 | 0 |
| `InfluxDbContractTest` | 1 | 0 | 0 |
| `KafkaDlqContractTest` | 1 | 0 | 0 |
| `KafkaStorageFailureContractTest` | 3 | 0 | 0 |
| `RedisRefreshTokenContractTest` | 1 | 0 | 0 |

## 결과 — Python

로컬 Python 3.14에는 `scikit-learn`/`kafka-python`이 없어 컨테이너(`python:3.11-slim`)에서
requirements를 설치해 실행했다.

| 영역 | 결과 |
| --- | --- |
| `anomaly-detector/tests` | 73개 통과 |
| `simulator/tests` | 22개 통과 |

## 결과 — CI

현재 commit `e4effc2`의
[`backend-infrastructure-ci` run 33848707400](https://github.com/Seongwonp/vehicle-telemetry-platform/actions/runs/33848707400)이
2026-09-04 07:31:07 UTC에 **success**로 끝났다. 이 워크플로는 계약 5개 클래스의 결과
XML이 모두 생성되고 `skipped`가 0인지를 별도로 검사한다
(`if len(files) != 5 or skipped: sys.exit(...)`), 따라서 CI에서도 계약이 실제로
실행됐음이 보장된다.

## 완료 조건 대조

| CLAUDE.md 완료 조건 | 상태 |
| --- | --- |
| Testcontainers 테스트가 Docker 환경에서 실제로 성공 | **충족** (tests=3, skip=0) |
| 전체 `./gradlew test --no-daemon` 성공 | **충족** (96개, 실패 0, skip 0) |
| GitHub Actions `backend-infrastructure-ci` 성공 | **충족** (run 33848707400) |
| 실행 commit SHA와 CI 링크를 `docs/verification/`에 기록 | 이 문서 |

`KafkaStorageFailureContractTest`의 새 계약은 더 이상 `컴파일 검증`/`미검증`이 아니라
**Docker 환경 실행 검증 완료** 상태다.

## 남은 검증 (기준선 문서에서 이월)

- GitHub Actions의 개별 테스트 케이스 수와 원문 로그 장기 보관 여부 결정 — 미결
- Flutter SDK가 있는 데스크톱에서 앱의 analyze, unit/widget, integration test — 미실행
  (이 데스크톱에 Flutter SDK 설치 여부 미확인)

## 반복 실행 — 간헐 실패(flakiness) 확인

Testcontainers는 컨테이너 기동 타이밍에 민감해 1회 성공만으로는 안정성을 말할 수 없다.
`cleanTest`를 붙여 Gradle이 이전 결과를 재사용하지 못하게 하고 5회 반복했다.

| 회차 | 결과 | tests | failures | errors | skipped | 소요 |
| ---: | --- | ---: | ---: | ---: | ---: | ---: |
| 1 | BUILD_SUCCESS | 3 | 0 | 0 | 0 | 72.8s |
| 2 | BUILD_SUCCESS | 3 | 0 | 0 | 0 | 67.8s |
| 3 | BUILD_SUCCESS | 3 | 0 | 0 | 0 | 64.0s |
| 4 | BUILD_SUCCESS | 3 | 0 | 0 | 0 | 39.0s |
| 5 | BUILD_SUCCESS | 3 | 0 | 0 | 0 | 39.4s |

**5/5 통과, 간헐 실패 없음.** 소요 시간이 72.8초 → 39.4초로 줄어든 것은 Testcontainers가
컨테이너 이미지를 캐시했기 때문이고, 테스트 자체가 빨라진 게 아니다.

## 한계

- 반복 5회는 간헐 실패를 배제하기에 충분한 표본이 아니다. 드물게(예: 1% 확률로)
  터지는 실패는 5회로 잡히지 않는다 — "관찰되지 않았다"이지 "없다"가 아니다.
- 이 데스크톱 한 대에서만 돌렸다. CI 러너처럼 자원이 빠듯한 환경에서는 기동 타임아웃이
  달라질 수 있다.
- 이 문서는 로컬 실행 결과와 CI 결과를 대조한 것이지, 실제 운영 환경(다중 브로커,
  네트워크 지연, 디스크 포화)에서의 동작을 검증한 것이 아니다.
