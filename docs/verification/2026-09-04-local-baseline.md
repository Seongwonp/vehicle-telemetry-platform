# 2026-09-04 로컬 회귀 테스트 기준선

## 실행 기준

- Git commit: `d8ad069a491fee45ae0f537598fbe56c9edbc885`
- Branch: `main` (`origin/main`과 일치)
- OS: Windows
- Java: OpenJDK 17.0.20
- Gradle: 8.7
- Python: 3.12.10
- Docker: 이 노트북에 설치되지 않음

기존 `backend/build`가 OneDrive의 읽기 전용 reparse point라 Gradle이 stale output을
정리하지 못했다. 데스크톱의 기존 산출물을 건드리지 않기 위해 같은 commit의 임시 detached
worktree에서 깨끗한 빌드로 재실행했다. 두 번째 실행은 성공했다.

## 결과

| 영역 | 명령 | 결과 |
| --- | --- | --- |
| Backend | `./gradlew test --no-daemon` | BUILD SUCCESSFUL |
| Java/JUnit | Gradle XML 집계 | 95개 중 89개 통과, 6개 skip, 실패 0 |
| Anomaly detector | `python -m pytest anomaly-detector/tests -q` | 73개 통과 |
| Simulator | `python -m pytest simulator/tests -q` | 22개 통과 |

## 실행되지 않은 인프라 계약

Docker가 없어 `@Testcontainers(disabledWithoutDocker = true)`가 적용된 다음 계약은
실행되지 않았다. 따라서 이 로컬 결과만으로 인프라 계약까지 통과했다고 주장하지 않는다.

| 계약 테스트 | 케이스 | 상태 |
| --- | ---: | --- |
| `FlywayPostgresContractTest` | 1 | skip |
| `InfluxDbContractTest` | 1 | skip |
| `KafkaDlqContractTest` | 1 | skip |
| `KafkaStorageFailureContractTest` | 2 | skip |
| `RedisRefreshTokenContractTest` | 1 | skip |

CI는 위 5개 클래스가 skip 없이 실행됐는지를 별도로 검사한다. 현재 commit의
[`backend-infrastructure-ci`](https://github.com/Seongwonp/vehicle-telemetry-platform/actions/runs/33834110061)는
2026-09-04 03:46:21 UTC에 성공했다. 워크플로 정의상 다음 단계가 모두 성공해야 전체 실행이
성공으로 끝난다.

- Backend unit test와 Testcontainers 계약 테스트
- 인프라 계약 테스트 5개 클래스가 모두 생성되고 skip이 0인지 확인
- Python anomaly-detector 테스트
- Docker Compose 설정 검증과 이미지 빌드
- simulator profile을 포함한 전체 스택 기동 및 backend health check

## 남은 검증

- GitHub Actions의 개별 테스트 케이스 수와 원문 로그 장기 보관 여부 결정
- Flutter SDK가 있는 데스크톱에서 앱의 analyze, unit/widget, integration test
- 실행 결과 원문과 GitHub Actions URL 보관

## 기준선 이후 추가한 계약 테스트

같은 작업에서 `KafkaStorageFailureContractTest`에 3건짜리 배치가 영구 실패하는 경우를
추가했다. 테스트 전용 고유 source/DLQ topic을 만들고 다음 계약을 확인하도록 작성했다.

- 최초 1회와 재시도 2회 모두 동일한 3건 배치로 `saveAll()` 호출
- 재시도 소진 후 세 메시지의 key와 원본 payload가 모두 DLQ에 존재
- source consumer offset이 배치 마지막 레코드 다음 위치까지 커밋

새 테스트를 포함한 `compileTestJava`는 별도 worktree에서 성공했다. 하지만 이 노트북에는
Docker가 없어 테스트 본문은 실행하지 못했다. 위 GitHub Actions 성공 링크도 변경 전
`d8ad069`의 결과이므로, 추가 계약은 다음 CI가 성공하기 전까지 **컴파일 검증** 상태다.

같은 계약 클래스의 DLQ 발행 실패 테스트도 고유 topic을 사용하도록 격리하고, 미커밋 확인
뒤 동일 Consumer Group을 정상 저장 consumer로 재시작하는 단계까지 확장했다. 재시작한
consumer가 원본 1건을 다시 `saveAll()`로 처리하고 offset이 그 다음 위치로 전진해야 한다.
이 시나리오 역시 다음 Docker 기반 CI 실행 전까지는 미검증이다.
