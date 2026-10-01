# 보안 자체 점검 — 현재 구현과 남은 한계

점검일: 2026-10-01. 기준: `21cf99c`의 코드·설정 정적 대조.
침투 테스트 완료나 UN R155 / ISO/SAE 21434 적합성 인증을 뜻하지 않는다.
이전 문서의 단일 관리자·IDOR 보류·Access Token 24시간·TLS 주석 해제 안내는 현재 구현과 달라 철회한다.

## 구현과 증거

| 영역 | 현재 구현 | 근거와 한계 |
| --- | --- | --- |
| 인증 | PostgreSQL users, BCrypt, DB 기반 UserDetailsService. JWT 인증 때 활성 사용자 조회 | DbUserDetailsServiceTest, JwtAuthenticationFilterTest. 테스트 존재와 이번 실행 결과는 구분 |
| 토큰 | Access 기본 600,000ms(10분), 환경변수로 변경 가능. Redis refresh rotation | RedisRefreshTokenContractTest, RefreshInactiveUserTest. 로그아웃은 해당 refresh 폐기; Access 즉시 폐기가 아님 |
| 비밀번호 | 본인 변경·관리자 초기화, 성공 시 사용자 refresh 전부 폐기 | PasswordApiSecurityTest. 기존 Access는 자체 만료까지 유효 |
| 차량 인가 | 활성 차량에 소유자 또는 관리자 접근. 차량 등록·비활성화와 사용자 관리 ADMIN 전용 | VehicleAccessService, VehicleAccessInterceptor, SecurityBoundaryTest, [다중 사용자 E2E](verification/2026-09-27-multi-user-e2e.md) |
| WebSocket | CONNECT 인증, SUBSCRIBE 차량 권한 검사, 클라이언트 SEND 차단, 만료 세션 정리 | WebSocketAuthChannelInterceptorTest. 차량 비활성화가 열린 구독을 즉시 끊지는 않음(ADR-027) |
| MQTT | 운영 8883 mTLS, 인증서 CN 기반 ACL, topic/payload 차량 ID 일치 검사 | broker/config/mosquitto.conf, broker/config/acl, MqttMessageHandlerTest. dev override는 평문 1883; dev 부하 실험을 TLS 검증으로 쓰지 않음 |
| 로그인 방어 | 실패 누적 IP 차단, 로그인 rate limit, 신뢰 프록시 조건부 IP 해석 | BruteForceDetectorTest, LoginRateLimiterTest, ClientIpResolverTest |
| SQL·응답 | 저장소 파라미터 바인딩, 401/403 JSON | 구현을 전체 SQL Injection 침투 테스트 완료로 확대하지 않음 |

Java 소스는 `backend/src/main/java/com/telemetry/`, 테스트는 `backend/src/test/java/com/telemetry/` 기준.

## 자산·위협·대응

간단한 위협 모델이며 정식 TARA의 위험 산정·승인을 대신하지 않는다.

| 자산 | 위협 | 현재 대응 | 남은 일 |
| --- | --- | --- | --- |
| 계정·토큰 | 대입 공격, 탈취 토큰 재사용 | BCrypt, 로그인 제한, 짧은 Access, refresh rotation | Access 즉시 철회 요구와 키 회전 정책 |
| 차량 위치·이력 | 타인 데이터 열람 | DB 소유권 및 구독 인가 | 열린 구독의 권한 변경 반영 |
| 수집 데이터 | 차량 사칭, topic 위조 | 운영 mTLS·CN ACL·ID 일치 검사 | 인증서 교체·폐기 실험, 지연 재전송을 허용하는 중복 정책 |
| 메시지·세션·spool | 프로세스 종료, 큐 포화, 디스크 실패 | persistent session, Kafka ACK, 실패 spool, 지표 | [ACK 작업 계획](plans/2026-10-01-mqtt-ack-boundary.md). 모든 장애에서 무손실을 보장하지 않음 |
| 운영 정보·비밀값 | 메트릭 노출, 키·로그 유출 | 환경변수, ignore 규칙, payload digest | Prometheus·Swagger는 코드상 익명 접근 가능. 배포 경계 제한은 실제 확인 필요 |

## 운영 한계

- 인증서 생성기는 암호화하지 않은 개인키와 10년 인증서를 만든다. 키 접근권한·보관·교체·폐기 절차가 필요하다. `-nodes` 하나가 침해 증거는 아니다.
- 환경변수·ignore 사용은 Git 전체 이력에 비밀값이 없다는 증거가 아니다. 이번 점검에서는 전체 이력 스캔을 실행하지 않았다.
- HSTS 설정만으로 실제 외부 HTTPS·방화벽 배포 완료를 주장하지 않는다.
- Alertmanager default receiver에는 외부 수신처가 없다. 규칙과 운영자 통보는 구분한다.
- 메시지 replay 방어는 미구현. 과거 timestamp를 일괄 거부하면 정상 장애 복구 데이터를 버리므로 적용하지 않는다.
- ML은 기본 비활성인 실험 기능이다. 실차 학습·탐지 성능이 입증된 것으로 설명하지 않는다.

## 브로커 기본값

Compose의 `eclipse-mosquitto:2.0`은 이동 가능한 태그다. 2026-10-01 로컬 이미지 `199ea8ef2e35`를 네트워크 없이 `mosquitto -h`로 실행해 **2.0.22**를 확인했다. 다른 호스트까지 같은 버전으로 단정하지 않는다.

dev·운영 모두 persistence=true, max_queued_messages=100000. 생략된 기본값은 2.0.22 소스로 확인했다.

| 설정 | 기본값 | 범위 |
| --- | --- | --- |
| autosave_interval | 1800초, autosave_on_changes=false | 주기·정상 종료·명시적 저장 요청에 따른 저장. 메시지별 fsync 보장이 아님 |
| max_inflight_messages | 20 | 클라이언트로 전송 중인 QoS 1/2 한도. 대기열 제한과 별개 |

[버전 고정 기본값](https://github.com/eclipse-mosquitto/mosquitto/blob/v2.0.22/src/conf.c), [설정 설명](https://mosquitto.org/man/mosquitto-conf-5.html).
브로커 강제 종료 때 마지막 저장 이후 상태를 잃을 수 있다. autosave 단축을 내구성 보장과 동일시하지 않는다.
