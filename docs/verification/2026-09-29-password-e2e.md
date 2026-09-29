# 검증 — 비밀번호 변경·초기화가 실제 컨테이너 스택에서 refresh 토큰을 폐기하는가 (2026-09-29)

| 항목 | 값 |
| --- | --- |
| **검증 상태** | **변경·초기화 각 1회 관찰**(컨테이너 스택, dev 프로파일) — 안정성 주장 없음. 다중 사용자 동시 사용·반복은 안 봤다 |
| 대상 | `f3dd456` 비밀번호 변경(`POST /api/auth/password`)·관리자 초기화(`PUT /api/users/{username}/password`) — 커밋 메시지가 "E2E는 안 했다"고 적은 부분 |
| 코드 상태 | 실행 시 HEAD `84cffd5`(`b4f1073` 이후 커밋은 README·docs·`.claude/CLAUDE.md`만 바꿨다 — `git diff --stat b4f1073 HEAD -- backend anomaly-detector simulator` 빈 결과). 백엔드 이미지 `sha256:49b22b5b…`(HEAD 작업 트리에서 `up -d --build`), 감지기 `1f5d3d8b…`(재빌드했으나 digest가 오전과 같다) — `00_metadata.txt` |
| 환경 | 데스크탑, Docker Desktop 29.7.2 / compose 5.4.0, dev(평문) + simulator, 무부하. 백엔드만 DEBUG(저장소 밖 override, 종료 시 삭제). 기동 시 Flyway 검증은 통과했다(기동 로그) |
| 원본 | [`evidence/2026-09-29-password-e2e/`](evidence/2026-09-29-password-e2e/) — `e2e.py`(실행 스크립트), `01_e2e_results.txt` |
| 기록 원칙 | 관리자 계정은 `.env`에서 읽고 출력·기록하지 않았다. 테스트 사용자 비밀번호는 실행마다 무작위 생성해 메모리에만 두었다. **토큰·비밀번호는 기록하지 않았고 상태 코드와 `code`만 남겼다.** 관리자 이름은 결과 파일에서 `{admin}`으로 가렸다 |

테스트 계정 `e2e-pw-0929183104`(USER)는 **DB에 남아 있다.** 마지막 비밀번호는 기록하지 않았다.

## 1. 결과

| # | 단계 | 요청 | 기대 | 결과 |
| --- | --- | --- | --- | --- |
| 1 | 관리자 로그인 | `POST /api/auth/login` | 200 | 200 |
| 2 | 사용자 생성 | `POST /api/users`(관리자) | 201 | 201 |
| 3 | 사용자 로그인(access + refresh) | `POST /api/auth/login` | 200 | 200 |
| 4 | **현재 비밀번호 틀림** | `POST /api/auth/password` | 400 `CURRENT_PASSWORD_INCORRECT` | 400 `CURRENT_PASSWORD_INCORRECT` |
| 4b | 새 비밀번호 7자(추가) | 같음 | 400 `VALIDATION_FAILED` | 400 `VALIDATION_FAILED` |
| 5 | **현재 비밀번호 맞음 → 변경** | 같음 | 204 | 204 |
| 6 | **옛 refresh로 재발급** | `POST /api/auth/refresh` | 401 | **401** `UNAUTHORIZED` |
| 7 | 옛 비밀번호로 로그인(추가) | login | 401 | 401 |
| 8 | 새 비밀번호로 로그인 | login | 200 | 200 |
| 9 | **일반 사용자가 관리자 비밀번호 초기화** | `PUT /api/users/{admin}/password` | 403 | 403 `FORBIDDEN` |
| 9b | 관리자가 없는 사용자 초기화(추가) | `PUT /api/users/e2e-pw-nonexistent/password` | 404 | 404 `NOT_FOUND` |
| 10 | **관리자가 그 사용자 초기화** | `PUT /api/users/e2e-pw-0929183104/password` | 204 | 204 |
| 11 | 초기화 전 발급된 refresh로 재발급(추가) | refresh | 401 | 401 `UNAUTHORIZED` |
| 12 | **초기화 비밀번호로 로그인** | login | 200 | 200 |
| 12b | 초기화 직전 비밀번호로 로그인(추가) | login | 401 | 401 |

요청한 단계(4·5·6·8·9·10·12)와 추가 단계(4b·7·9b·11·12b)가 모두 기대와 같았다. 원본은 `01_e2e_results.txt`(각 줄에 시각·상태 코드·응답 시간).

## 2. 이 결과가 말해 주는 것

- 변경 성공(5) 직후 **변경 전에 발급된 refresh가 재발급을 거부**했다(6). 저장 구조가 uuid→username 단방향이라 SCAN 후 값 비교로 폐기한다는 커밋 설명이 **실제 Redis에서 동작**했다(Testcontainers 계약 테스트 1건이 있었지만 컨테이너 스택에서 본 것은 처음이다).
- 관리자 초기화(10)도 그 시점의 refresh(8에서 받은 것)를 폐기했다(11).
- `CURRENT_PASSWORD_INCORRECT`가 401이 아니라 **400**으로 나왔다(4) — 클라이언트가 세션 만료로 오해하지 않는다는 설계 의도와 일치.

## 3. 한계

- **각 1회**, 사용자 1명, 무부하. 반복·동시 요청은 안 봤다.
- 6과 11의 401은 "refresh가 폐기됐다"와 "그 토큰이 애초에 유효하지 않았다"를 이 관찰만으로는 가르지 못한다. 대조군으로 **변경 전 같은 refresh가 200을 주는지**는 이 실행에서 재지 않았다(로그인 직후 곧바로 변경했다). 테스트 계층의 검증(`RefreshTokenServiceTest`, Redis 계약)이 폐기 자체를 확인하지만 **이 스택에서의 대조군은 없다** — 다음 실행에서 "변경 전 refresh 200 → 변경 → 401"을 넣으면 닫힌다.
- **access token이 변경 후에도 유효한지**는 재지 않았다(설계상 만료까지 유효 — ADR-027 한계).
- 초기화 시 RefreshToken 폐기 대상이 "그 사용자의 것만"인지(다른 사용자 refresh는 유지되는지)는 안 봤다.
- Redis 장애 중 변경(503 `REDIS_UNAVAILABLE`, 트랜잭션 롤백)은 안 봤다.
- 로그인 rate limit·brute-force 카운터에 실패 로그인(7·12b)이 각 1회 쌓였다. 5회 임계에는 못 미쳤다.
- 테스트 계정이 남아 있다(`e2e-pw-0929183104`).
