# 검증 — 다중 사용자 소유권 E2E (2026-09-27)

| 항목 | 값 |
| --- | --- |
| **검증 상태** | **1회 관찰** — 실제 컨테이너 스택, dev(평문) 프로파일, 무부하 |
| 대상 | ADR-025 소유권 규칙이 **두 계정**에서 실제로 갈리는가. 2026-09-16까지 사용자가 한 명이라 미검증이었다 |
| 코드 상태 | ADR-027 변경(V4·V5, DB 기반 인증, `POST /api/users`)이 들어간 이미지. 기준 커밋 `a245402` 위 작업 트리 |
| 환경 | 이 PC Docker Desktop, `docker compose -f docker-compose.yml -f docker-compose.dev.yml --profile simulator`, 시뮬레이터 SIM-001~003이 admin 소유로 등록된 상태 |
| 도구 | curl, `psql` |

## 절차와 결과

| # | 요청 | 기대 | 결과 |
| --- | --- | --- | --- |
| 1 | admin → `POST /api/users` {hong, USER} | 201, 응답에 해시 없음 | **201** `{"username":"hong","role":"USER","loginEnabled":true,...}` |
| 2 | hong → `POST /api/auth/login` | JWT 발급 | **발급됨** (DB 기반 `DbUserDetailsService`) |
| 3 | hong → `GET /api/vehicles` | 빈 목록 (admin 차량 3대가 안 보여야 한다) | **`[]`** |
| 4 | hong → `GET /api/vehicles/SIM-001/telemetry/latest` | **404** (403이면 차량 존재를 알려준다) | **404** |
| 5 | admin → 같은 요청 | 200 | **200** |
| 6 | hong → `POST /api/vehicles` owner=admin | 403 | **403** |
| 7 | hong → `POST /api/vehicles` owner 생략 | 201, owner=hong | **201** `"owner":"hong"` |
| 8 | hong → `GET /api/vehicles` | 자기 차량 1대 | **`['HONG-CAR-1/hong']`** |
| 9 | admin → `GET /api/vehicles` | 4대 전부 + HIGH 건수 | **4대**, `SIM-001 HIGH=80 · SIM-002 66 · SIM-003 78 · HONG-CAR-1 0` (GROUP BY 1회 경로) |
| 10 | hong → `POST /api/users` {mallory, ADMIN} | 403 | **403** |
| 11 | admin → `POST /api/vehicles` owner=ghost | 400 (FK 이전엔 조용히 들어갔다) | **400** `존재하지 않는 사용자입니다: ghost` |
| 12 | `psql` users ⟕ vehicles | admin ADMIN/해시 있음/3대, hong USER/해시 있음/1대 | 일치 |

기동 로그: V5 적용 `Successfully applied 1 migration ... now at version v5`(V4는 직전 기동에서 적용). 기존 admin 소유 차량 3대는
V4 백필로 `owner_id`가 admin에 연결됐다(12번).

## 한계

- 1회, 무부하, 평문 프로파일. mTLS·WebSocket 구독 경로(`WebSocketAuthChannelInterceptor`)는 이번에 두 계정으로 안 봤다.
- 비밀번호 변경·비활성화 후 토큰 만료 동작은 안 봤다(API 자체가 없다).
- 앱은 아직 소유자 이름을 자유 입력받는다 — 앱 후속 작업.
