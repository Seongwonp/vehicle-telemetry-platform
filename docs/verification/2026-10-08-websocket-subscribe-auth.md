# WebSocket SUBSCRIBE 재검사와 열린 연결의 실제 종료 시점 — 2026-10-08

상태: **구현 + 단위·통합 테스트 통과(대상 클래스만).** 전체 `./gradlew test` 집계는 이 작업에서 **실행하지 못했다**(도구 권한 거부) — 미검증으로 남긴다.
기준 커밋: `d276d84` + 미커밋 변경(다른 작업의 미커밋 파일 포함 작업 트리). 계기: [`2026-10-08-qa-account-deactivation.md`](2026-10-08-qa-account-deactivation.md) "추가" 표.

## 1. 변경

`backend/src/main/java/com/telemetry/config/WebSocketAuthChannelInterceptor.java` 한 곳.

- SUBSCRIBE마다 CONNECT principal의 **이름만** 꺼내 `UserDetailsService.loadUserByUsername`(= `DbUserDetailsService`, `User::canLogin` 필터)으로 다시 읽는다.
  - 비활성·삭제·해시 없음 → `UsernameNotFoundException` → `AccessDeniedException`(구독 거부).
  - 새로 읽은 `UserDetails`의 **권한**으로 만든 `Authentication`을 `VehicleAccessService.canAccess`에 넘긴다 → 강등된 관리자는 남의 차량을 못 구독한다.
- JWT 만료 검사(`rejectExpired`, 모든 비-CONNECT 프레임)는 원래 있었다. 바꾸지 않았다.
- 비용: SUBSCRIBE 1건당 `users` 단건 조회 1회(CONNECT와 같은 쿼리). 데이터 프레임·하트비트에는 추가 조회 없음.

**이것은 기존 구독의 전달을 멈추지 않는다.** §3.

## 2. 테스트

| 테스트 | 무엇을 고정하나 |
| --- | --- |
| `WebSocketSubscribeRevalidationTest` (6, 단위) — 실제 `DbUserDetailsService`·`VehicleAccessService`·`JwtTokenProvider`, 저장소만 메모리 모형, 같은 세션 속성·principal로 CONNECT→SUBSCRIBE 반복 | |
| `같은세션_비활성화전_구독허용_비활성화후_새구독거부` | 같은 세션에서 비활성화 전 통과 → 후 `AccessDeniedException` |
| `같은세션_계정삭제후_새구독거부` | 계정 행이 사라진 경우 |
| `같은세션_JWT만료전_구독허용_만료후_프레임거부_세션종료요청` | 수명 1.5초 토큰. 만료 전 통과 → 만료 뒤 `AuthenticationCredentialsNotFoundException` + `closeExpired(sessionId)` 호출 |
| `강등된관리자_같은세션에서_남의차량_새구독거부_자기권한범위는유지` | ADMIN으로 CONNECT, 남의 차량 통과 → role=USER로 바꾼 뒤 남의 차량 거부, 자기 차량 통과 |
| `차량접근_소유자허용_비소유자거부_비활성차량거부` | 소유자 통과, 비소유자 거부, 비활성 차량은 소유자·관리자 모두 거부 |
| `연결중_차량비활성화후_새구독거부` | 연결 중 차량이 비활성화되면 새 구독 거부 |
| `WebSocketExpiryIntegrationTest` (2, 통합) — **실제 Tomcat + `WebSocketStompClient`**, WebSocket 구성(WebSocketConfig·인터셉터·세션 등록부·simple broker)만 띄움, 하트비트 없음 | |
| `JWT만료시각에_서버가_연결을닫고_기존구독으로의_브로드캐스트가_끊긴다` | 구독 후 수신 확인 → **클라이언트는 아무것도 안 보냄** → 만료+1.5초 안에 클라이언트가 연결 끊김을 받고 `isConnected()=false`, 이후 브로드캐스트 미수신 |
| `비활성화는_기존구독전달을_멈추지않고_새SUBSCRIBE는_거부되어_연결이닫힌다` | 비활성화 뒤에도 **기존 구독에 계속 전달**(한계 고정) → 새 SUBSCRIBE → 서버가 연결을 닫음 → 이후 미수신 |

기존 `WebSocketAuthChannelInterceptorTest`(5)는 재조회 스텁만 추가했다. `simpleBrokerKeepsDeliveringAfterAccessIsRevoked`(브로커가 프레임마다 권한을 묻지 않음)는 그대로 유효하다.

실행: `./gradlew test --no-daemon --tests "com.telemetry.config.WebSocket*"` → 3 클래스 13건, failures 0, errors 0, skipped 0(`build/test-results/test/TEST-com.telemetry.config.WebSocket*.xml`). 1회.

**음성 대조군은 못 했다** — `scheduleExpiration` 호출을 잠시 빼고 통합 테스트가 실패하는지 보려 했으나 도구 권한에서 거부됐다. "만료 종료의 원인이 스케줄러"는 코드 읽기로 판단한 것이다(아래 §3: 클라이언트가 프레임을 보내지 않고 하트비트도 없으므로 서버 쪽 종료 경로는 그것뿐).

## 3. 열린 연결은 실제로 언제 닫히나

| 경로 | 동작 | 근거 |
| --- | --- | --- |
| **JWT 만료(시간)** | CONNECT 때 `scheduleExpiration(sessionId, exp)`가 단일 스레드 스케줄러에 `closeExpired`를 건다. `closeExpired`는 전송 데코레이터가 등록한 **실제 `WebSocketSession`을 `close(1008 "JWT expired")`** 한다 — 인바운드 프레임만 거부하는 것이 아니다. 연결 종료로 `SubProtocolWebSocketHandler`가 브로커에 세션 종료를 알려 **구독이 지워지고 브로드캐스트가 멈춘다.** | `WebSocketSessionRegistry`, `WebSocketConfig.configureWebSocketTransport`; 통합 테스트 1 |
| 만료 뒤 클라이언트 프레임 | `rejectExpired`가 거부 + `closeExpired` 호출(스케줄러가 놓친 경우의 이중 장치) | 단위 테스트 |
| **거부된 SUBSCRIBE/SEND** | 인터셉터 예외 → Spring `StompSubProtocolHandler`가 ERROR 프레임을 보내고 **연결을 닫는다** → 그 연결의 기존 구독도 끝 | 통합 테스트 2 |
| **비활성화·강등·비밀번호 변경** | **아무것도 닫지 않는다.** 세션 등록부에 사용자 색인이 없고, 브로커는 프레임마다 권한을 묻지 않는다. 클라이언트가 새 SUBSCRIBE(또는 SEND)를 보내야 위 행으로 닫힌다 | 통합 테스트 2, `simpleBrokerKeepsDeliveringAfterAccessIsRevoked` |
| 하트비트 | 명령 없는 프레임이라 인터셉터가 검사하지 않음 → 연결 유지에 영향 없음 | 코드 |

정리: **비활성 계정의 열린 연결은, 클라이언트가 조용히 있으면 JWT 만료 시각까지 기존 구독을 받는다.** 상한 = access token 수명(이 스택 설정 10분).

범위 밖·미확인: 다중 인스턴스(각 인스턴스가 자기 세션만 닫으므로 구조상 문제는 없을 것으로 보지만 미실행), `close()`가 `IOException`을 던지는 경우(로그 후 등록 해제만 — 연결이 남는지 미확인), SockJS(미사용), 앱이 재연결 시 재구독하는지(앱 미확인). 실제 컨테이너 스택 E2E 아님.

## 4. 즉시 종료가 필요하면 — 최소 설계안 (구현 안 함)

이 프로젝트에서 비활성화는 **관리자 API가 없고 DB 직접 변경**이다(QA 문서). 그래서 이벤트 훅만으로는 그 경로를 못 잡는다.

1. **주기적 재검증(권장 최소안)** — `WebSocketSessionRegistry`에 `sessionId → username`(CONNECT에서 기록)을 두고, 기존 스케줄러로 N초(예: 60초)마다
   세션별 `loadUserByUsername` + 권한 비교. 실패·권한 변경이면 `closeExpired`와 같은 방식으로 닫는다. DB 직접 변경도 잡고, 인스턴스 간 조율이 필요 없다.
   대가: 열린 세션 수 × 1조회/주기, 노출 상한이 "토큰 수명"에서 "주기"로 줄어들 뿐 0은 아님. 강등 시 남은 구독 중 권한 밖 차량만 골라 끊는 것은 하지 않고 연결 전체를 닫는다(앱은 재연결·재구독).
2. **이벤트 즉시 종료(보완)** — 같은 색인 `username → sessionIds`에 `closeAllFor(username)`을 두고 비밀번호 변경·초기화(`UserService`, 이미 refresh 전부 폐기하는 자리), 이후 생길
   비활성화·역할 변경 API에서 호출. 다중 인스턴스면 Redis pub/sub 같은 전파가 필요하다. DB 직접 변경은 못 잡으므로 1과 함께 써야 한다.
3. 채택하지 않을 것: 아웃바운드 채널에서 프레임마다 권한 재검사(초당 프레임 × DB 조회).
