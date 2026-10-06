# QA 계정 비활성화와 refresh token 폐기 — 2026-10-08

상태: **완료(1회 확인).** 2026-10-07 앱 확인용으로 만든 로컬 dev 계정 `qa-admin`(ADMIN)·`qa-user`(USER)를 비활성화하고 refresh token을 전부 지웠다.
사용자 결정(HANDOFF_2026-10-07 §3-2). `qa-admin` 비밀번호가 10-07 에이전트 도구 출력에 한 번 찍힌 것이 계기다.

## 환경

| 항목 | 값 |
| --- | --- |
| 백엔드 | `0530af4`, 이미지 `sha256:fe411c07201c…`(10-07 재빌드) |
| 스택 | `docker compose -f docker-compose.yml -f docker-compose.dev.yml up -d` — **QA override 없이** 저장소 `.env` 기준 |
| 대상 | 로컬 dev PostgreSQL `users`, Redis `refresh_token:*` |

## 방법

관리자 API에 비활성화 엔드포인트가 없고(`UserController`는 생성·비밀번호 초기화·목록만), 쓸 수 있는 관리자 자격도 없어 **저장소를 직접** 바꿨다.
스크립트: [`evidence/2026-10-08-qa-account-deactivation/deactivate_and_verify.sh`](evidence/2026-10-08-qa-account-deactivation/deactivate_and_verify.sh).

1. 비활성화 **전에** 두 계정으로 한 번씩 로그인해 access·refresh token을 확보했다 — "기존 토큰이 막히는가"를 보려면 비활성화 전에 발급된 토큰이 필요하다.
   계정 값 파일(scratchpad `qa/qa.env`)은 셸 변수로만 읽었고 화면·파일·로그에 출력하지 않았다. 확인이 끝난 뒤 파일과 override를 **삭제했다**.
2. PostgreSQL: `UPDATE users SET active = FALSE WHERE username IN ('qa-admin','qa-user')` → 2행.
3. Redis: `refresh_token:*`를 SCAN해 값이 두 사용자 이름인 키를 지웠다 → **6개 삭제, 남은 키 0**(`RefreshTokenService.revokeAll`과 같은 방식).
4. 확인 요청.

## 결과

| 계정 | 비활성화 전 `/api/auth/me` | 옛 access token `/api/auth/me` | 옛 refresh token `/api/auth/refresh` | `/api/auth/login` |
| --- | :---: | :---: | :---: | :---: |
| qa-admin | 200 | **401** | **401** | **401** |
| qa-user | 200 | **401** | **401** | **401** |

원본: [`result.txt`](evidence/2026-10-08-qa-account-deactivation/result.txt)(상태 코드와 DB 행만, 토큰·비밀번호 없음).

- 옛 access token이 401인 이유: `JwtAuthenticationFilter`가 요청마다 `DbUserDetailsService`로 사용자를 다시 읽고 `User::canLogin`(활성 + 해시 있음)이 거짓이면
  인증을 세우지 않는다(ADR-027). 서명·만료가 유효한 JWT여도 막힌다.
- 옛 refresh token은 Redis에서 지워져 rotate가 실패한다. 지우지 않았어도 refresh 경로가 DB 활성 여부를 다시 본다(`AuthController`).
- SIM-002(소유자 `qa-user`)는 **그대로 두었다**(활성, 소유자 유지). 소유자가 비활성이라 그 차량은 관리자만 볼 수 있다.

## 한계

- **이미 열린 WebSocket 연결은 끊지 않는다** — ADR-027의 알려진 한계와 같다. 소유권 검사가 SUBSCRIBE 시점에만 돌아, 비활성화 전에 붙어 있던 세션은
  access token 만료까지 프레임을 받을 수 있다. 10-07 에뮬레이터에 `qa-user`로 로그인된 앱이 남아 있었다면 그 경우다.
- 계정은 **삭제하지 않았다**(비활성). DB 직접 변경이라 감사 로그가 없다 — 이 문서와 스크립트가 기록이다.
- 1회 확인.
