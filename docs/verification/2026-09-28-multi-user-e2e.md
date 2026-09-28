# 검증 — 다중 사용자 소유권 E2E 2회차, 관리자 전용 등록, 비활성 계정 (2026-09-28)

| 항목 | 값 |
| --- | --- |
| **검증 상태** | **2회차 관찰**(1회차 [`2026-09-27-multi-user-e2e.md`](2026-09-27-multi-user-e2e.md)) — 이번 실행에서 **결함 2건**을 찾아 고쳤고, 고친 이미지로 다시 확인했다 |
| 대상 | ADR-025/027 소유권 규칙 + 2026-09-27 제3자 리뷰 후속(관리자만 등록, `/api/auth/me`, 비활성 계정의 JWT·refresh) |
| 코드 상태 | 기준 커밋 `5410970` + 이 문서와 같이 커밋되는 수정 2건. 실행 이미지 ① `3113f27f0709`(5410970 그대로) ② `ffd7db27f760`(403 수정) ③ `d33b4eed66d7`(Flux 수정) |
| 환경 | 이 노트북 Docker Desktop, `docker-compose.yml + docker-compose.dev.yml --profile simulator`, 시뮬레이터 SIM-001~003 실행 중(27시간째). PostgreSQL은 [runbook](../runbook/user-schema-upgrade.md)으로 V6까지 전환한 상태 |
| 도구 | curl, `psql`, `influx query` |

## 0. 먼저 한 것 — DB 전환(runbook 0~3단계)

이 노트북의 `telemetry-postgres`는 옛 V4 checksum(`-1626396431`, `username` 50자)이었다. 새 이미지 기동 →
`Resolved locally : 514518207` 확인 → history UPDATE 1행 → 재기동에서 V6 적용(`now at version v6`), 길이 100.
데이터: users 2 · vehicles 4 · anomaly_alerts 1,905 — 삭제 없음.

기동 직후 `Error subscribing to [vehicle/telemetry/#]`(Paho timeout) 1회가 났지만 재시도로 붙었다 —
기동 후 수신 카운터 `telemetry_mqtt_messages_received_total` 291. 이전 기동 로그에도 같은 패턴이 있어 이번 변경과 무관하다.

## 1. 절차와 결과 — 이미지 ③ 기준 (①·②에서 달랐던 칸은 표시)

| # | 요청 | 기대 | 결과 |
| --- | --- | --- | --- |
| 1 | 익명 → `GET /api/auth/me` | 401 JSON | **401** `UNAUTHORIZED` |
| 2 | admin → `POST /api/users` {e2e-probe, USER} | 201 | **201** |
| 3 | probe → `GET /api/auth/me` | `ROLE_USER` | **200** `roles:["ROLE_USER"]` |
| 4 | probe → `POST /api/vehicles` owner 생략 | **403**(09-27에는 201이었다 — 정책 변경) | **403** `FORBIDDEN` — **①에서는 401**(아래 2-1) |
| 5 | probe → `POST /api/vehicles` owner=admin | 403 | **403** — ①에서는 401 |
| 6 | probe → `POST /api/users` {mallory, ADMIN} | 403 | **403** — ①에서는 401 |
| 7 | probe → `GET /api/users` | 403 | **403** |
| 8 | admin → `POST /api/vehicles` owner=e2e-probe | 201 | **201** `owner:"e2e-probe"` |
| 9 | 일반 사용자 → `GET /api/vehicles` | 자기 차량만 | **`['E2E2-DRV']`**(① 1회차 계정 e2e-driver로 확인) |
| 10 | probe → `GET /api/vehicles/SIM-001/telemetry/latest` | 404 | **404** `등록되지 않은 차량입니다` |
| 11 | probe → 남의 새 차량 `E2E2-ADM` | 404 | **404** |
| 12 | probe → 자기 차량 `E2E2-PROBE` | 404 **데이터 없음**(존재는 통과) | **404** `수신된 텔레메트리 데이터가 없습니다` — 10·11과 메시지가 다르다 |
| 13 | admin → SIM-001 latest | 200 | **200** |
| 14 | admin → `GET /api/vehicles` | 7대, OK/NO_DATA | **①·②에서 7대 전부 `UNAVAILABLE`**(아래 2-2) → ③에서 SIM 3대 `OK`, 나머지 `NO_DATA`, **0.06~0.28초** |
| 15 | admin → `POST /api/vehicles` owner=ghost | 400 | **400** `존재하지 않는 사용자입니다: ghost` |
| 16 | probe 로그인 → `psql`로 `active=false` → 그 access token으로 `GET /api/auth/me` | 401(500 아님) | **401** `UNAUTHORIZED` |
| 17 | 같은 refresh token → `POST /api/auth/refresh` | 401 | **401** `유효하지 않거나 만료된 리프레시 토큰` |
| 18 | 비활성 계정 로그인 | 401 | **401** |
| 19 | 비활성 계정의 유효 token으로 자기 차량 조회 | 401 | **401** |
| 20 | `psql` users ⟕ vehicles | 소유 건수 일치 | admin 4 · hong 1 · e2e-driver 1 · e2e-inactive(비활성) 0 · e2e-probe(비활성) 1 |

## 2. 이 실행에서 찾은 결함 2건

### 2-1. 인가 거부 403이 실제 서버에서는 401로 나갔다 (4·5·6)

- **현상**: 일반 사용자의 `POST /api/vehicles`·`POST /api/users`가 `403 FORBIDDEN`이 아니라 entry point의
  `401 {"code":"UNAUTHORIZED","message":"인증이 필요합니다"}`로 응답. `RequestLoggingFilter`에는 이 요청이 **아예 안 찍혔다**
  (그 필터는 Security 체인 뒤에 있다).
- **원인**: 기본 `AccessDeniedHandlerImpl`이 `sendError(403)`을 부르면 Tomcat이 `/error`로 **ERROR 디스패치**를 한다.
  `JwtAuthenticationFilter`는 `OncePerRequestFilter`라 ERROR 디스패치를 건너뛰고, `/error`는 `anyRequest().authenticated()`에
  걸려 **익명 → entry point → 401**. `SecurityBoundaryTest`(MockMvc)는 ERROR 디스패치를 하지 않아 403을 봤다.
- **수정**: `accessDeniedHandler`가 entry point처럼 JSON을 직접 쓴다(`SecurityConfig.writeJson`). 테스트는 `$.code == FORBIDDEN`까지
  본다 — `sendError`였다면 MockMvc에서 본문이 비어 실패한다.
- **교훈**: MockMvc 슬라이스는 서블릿 컨테이너의 에러 디스패치를 재현하지 않는다. 인가 응답 코드는 실제 컨테이너에서 한 번은 봐야 한다.

### 2-2. 차량 목록의 fleet 요약이 전부 `UNAVAILABLE` (14)

- **현상**: `GET /api/vehicles`의 `summaryStatus`가 7대 전부 `UNAVAILABLE`, 로그 `[InfluxDB] 쿼리 실패 context=fleet` +
  `InfluxException: timeout`. 단건 `GET .../telemetry/latest`는 정상.
- **측정**(`influx query`, 24시간 창, 시뮬레이터 3대, `speed` 기준 30일 95,512 포인트):

  | Flux | 시간 |
  | --- | --- |
  | 기존: `contains(value: r.vehicle_id, set: [...])` → pivot → sort → limit | **7.8 s** (클라이언트 read timeout 5 s 초과) |
  | `contains()` + `last()` | 5.1 s |
  | 등호 OR 체인 + pivot(기존 모양) | 1.07 s |
  | **등호 OR 체인 + `last()` → pivot** | **0.44 s** |
  | 필터 없이 전 차량 + `last()` | 0.43 s |
  | 단건 `r.vehicle_id == "SIM-001"`(기존 getLatest) | 0.64 s |

- **원인**: `contains()`는 스토리지 푸시다운이 안 된다 — InfluxDB가 24시간치 전 시리즈를 읽어 올린 뒤 Flux에서 걸렀다.
  등호 비교는 푸시다운된다. `last()`를 pivot 앞에 두면 시리즈당 1행만 pivot한다.
- **수정**: `TelemetryQueryService.getLatestByVehicleIds`를 등호 OR 체인 + `last()`로. 단위 테스트는 `contains(` 부재와 OR 체인을,
  Testcontainers `InfluxDbContractTest`는 실제 InfluxDB 2.7에서 차량 2대·포인트 3개로 **최신 포인트**가 돌아오는지 본다.
- **결과**: 목록 응답 0.277 / 0.068 / 0.063 s(3회), fleet 실패 로그 0.
- **왜 지금까지 몰랐나**: 데이터가 적을 때는 `contains()`도 5초 안에 끝났다. 시뮬레이터가 하루 넘게 돌아 24시간 창이 꽉 찬
  뒤에야 timeout을 넘었다. 1회차(09-27) 표에는 `summaryStatus`를 안 적어서 그때 어땠는지 모른다.

## 3. 한계

- 관찰은 이 노트북 1대, 무부하, 평문 프로파일. Flux 시간은 Docker Desktop(WSL2) 디스크 기준이고 **각 1회**다 — 배수(18배)가 아니라
  "푸시다운 여부가 갈린다"는 계획의 차이를 근거로 썼다.
- WebSocket `SEND` 거부·구독 소유권은 이번에 두 계정으로 안 봤다(단위 테스트만).
- 비활성화는 `psql UPDATE`로 했다 — 비활성화 API가 없다(ADR-027 한계).
- 12번의 메시지 차이("등록되지 않은" vs "수신된 데이터가 없습니다")는 **소유 여부를 메시지로 구분할 수 있다**는 뜻이다.
  본인 차량에서만 후자가 나오므로 남의 차량 존재 여부는 새지 않지만, 기록해 둔다.
