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
  (후속 수정 뒤에도 **기존 구독 전달은 그대로**다 — 아래 "후속 수정" 참고. 바뀐 것은 새 SUBSCRIBE뿐이다.)
- 계정은 **삭제하지 않았다**(비활성). DB 직접 변경이라 감사 로그가 없다 — 이 문서와 스크립트가 기록이다.
- 1회 확인.

## 추가 — 비활성이 각 경로에 적용되는 방식 (코드 확인, 요청 반복 없음)

위 401 결과(`result.txt`)를 근거로 두고, 각 경로가 **왜** 그렇게 동작하는지 코드로 확인했다. 새 요청은 보내지 않았다.

| 경로 | 비활성 확인 위치 | 기대 정책 | 실제 |
| --- | --- | --- | --- |
| 로그인 | `AuthenticationManager` → `DbUserDetailsService.loadUserByUsername`이 `User::canLogin`(활성 + 해시) 필터 → 없으면 `UsernameNotFoundException` → `DaoAuthenticationProvider`가 `BadCredentialsException`으로 바꿈(사용자 없음 숨김 기본값) | 거부 | **일치**(401). 그래서 비활성 계정 로그인 시도도 `bruteForceDetector.recordFailure`로 IP 잠금 집계에 들어간다(2차 리뷰) |
| refresh | `RefreshTokenService.rotate`(옛 토큰 즉시 폐기) 뒤 `loadUserByUsername`으로 DB 재확인 | 거부, 체인 종료 | **일치**(401). Redis에서 지운 뒤라 rotate 단계에서 이미 실패 |
| 기존 access token (REST) | `JwtAuthenticationFilter`가 요청마다 `loadUserByUsername` | 즉시 거부 | **일치**(401) — JWT 서명·만료가 유효해도 |
| WebSocket 새 CONNECT | `WebSocketAuthChannelInterceptor.authenticate`가 `loadUserByUsername` | 거부 | **일치**(코드 기준, 요청은 안 보냄) |
| **이미 열린 WebSocket 세션** | CONNECT 때 세운 principal을 세션이 계속 씀. SUBSCRIBE는 `VehicleAccessService.canAccess`로 **소유권·관리자 권한만** 보고 활성 여부는 다시 보지 않음. 세션은 JWT 만료 시각에 닫힘(`sessionRegistry.scheduleExpiration`) | ADR-027 한계: 만료 전 잔여 수신 | **기대보다 넓다** — 기존 프레임 수신만이 아니라 **열린 세션에서 새 SUBSCRIBE도 통과**한다. 비활성 관리자(`qa-admin`) 세션이면 만료 전까지 **모든 활성 차량**을 새로 구독할 수 있다. 상한은 access token 수명(이 스택 `JWT_EXPIRATION_MS` 설정값 기준 10분 — 컨테이너 env에서 분 단위로만 읽음). 세션 principal의 권한도 낡은 채라, 강등된 관리자도 만료까지 관리자로 남는다 |

- 이번 실제 노출: 10-07 에뮬레이터의 `qa-user` 세션은 비활성화 시점(10-08)에 이미 토큰 수명(10분)을 훨씬 넘겨 닫혔을 것이다(추정, 확인 안 함).
- 수정은 하지 않았다(사용자 지시: 계정·정책은 지금 손대지 않음). 후보: SUBSCRIBE 때 `loadUserByUsername`으로 활성 재확인, 또는 비활성화 시 그 사용자의 세션을 닫는 등록부 조회 — ADR-027 한계 항목에 "새 SUBSCRIBE 포함"을 더하는 문서 수정만이라도 필요하다.

## 후속 수정 — 열린 세션의 새 SUBSCRIBE를 현재 상태로 판정 (2026-10-08, 사용자 승인)

위 표의 마지막 행("열린 세션에서 새 SUBSCRIBE도 통과", "강등된 관리자도 만료까지 관리자")을 고쳤다.
상세·테스트·종료 분석: [`2026-10-08-websocket-subscribe-auth.md`](2026-10-08-websocket-subscribe-auth.md).

- `WebSocketAuthChannelInterceptor`가 SUBSCRIBE마다 `DbUserDetailsService.loadUserByUsername`으로 사용자를 다시 읽고(`User::canLogin`),
  **새로 읽은 권한**으로 `VehicleAccessService.canAccess`를 판정한다. CONNECT principal의 권한은 더 이상 쓰지 않는다.
- JWT 만료 검사는 원래 모든 비-CONNECT 프레임에 있었다(`rejectExpired`) — 바꾸지 않았고 테스트로 고정했다.

**수정 뒤 비활성(또는 삭제) 계정이 아직 할 수 있는 것**

| 행동 | 수정 전 | 수정 후 |
| --- | --- | --- |
| 열린 세션에서 **새 SUBSCRIBE** | 통과(비활성 관리자는 모든 활성 차량) | **거부** → 서버가 STOMP ERROR를 보내고 **연결을 닫는다**(그 연결의 기존 구독도 함께 끝남) |
| 강등된 관리자의 새 SUBSCRIBE(남의 차량) | 통과 | **거부**(자기 소유 차량은 통과) |
| **이미 맺은 구독으로의 수신** | 계속 | **여전히 계속** — 클라이언트가 아무 프레임도 보내지 않으면 **JWT 만료 시각까지**. 그 시각에 서버 스케줄러가 WebSocket 연결 자체를 닫는다(실측, 통합 테스트) |
| 하트비트 | 통과 | 통과(명령 없는 프레임은 검사 대상이 아님) |
| UNSUBSCRIBE·DISCONNECT | 통과 | 통과(만료만 검사, 계정 재확인 안 함) |

상한은 access token 수명이다(이 스택 설정값 10분, 위 표 기준). **즉시 종료는 구현하지 않았다** — 설계안은 새 문서 §4.
