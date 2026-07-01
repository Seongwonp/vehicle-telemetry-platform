# 보안 자체 점검 보고서

> **프로젝트**: Vehicle Telemetry Platform  
> **점검 기준**: OWASP Top 10, UN R155 / ISO SAE 21434 (차량 사이버보안)  
> **점검일**: 2026-05-09 (2026-07-01 Phase 6~10 반영 갱신)  
> **작성자**: Sungwon

---

## 1. 점검 요약

| 항목 | 위험도 | 상태 |
|------|--------|------|
| MQTT 평문 통신 | 높음 | 완화 (TLS 클라이언트 코드 구현 완료 — Phase 10. 기본값은 여전히 평문, 플래그로 전환) |
| 브루트포스 로그인 공격 | 높음 | 완화 (Redis 기반 IP 차단) |
| JWT 토큰 탈취/무효화 불가 | 높음 | 완화 (만료시간, HTTPS 적용, Phase 7에서 Refresh Token 로그아웃 무효화 추가) |
| SQL Injection | 높음 | 완화 (JPA 파라미터 바인딩) |
| Rate Limiting 미적용 | 중간 | 완화 (Redis 기반, 분당 60회) |
| 보안 헤더 누락 | 중간 | 완화 (5개 헤더 적용) |
| IDOR (권한 우회) | 중간 | 부분 완화 (현재 admin 단일 계정이라 실질 위험 낮음. 다중 사용자 도입 시 필요) |
| 민감 정보 하드코딩 | 높음 | 완화 (.env 분리, .gitignore 처리) |
| 인증서 없는 MQTT 클라이언트 | 높음 | 완화 (X.509 mTLS 클라이언트 코드 구현 완료 — Phase 10) |
| 이상 접근 미감지 | 중간 | 완화 (감사 로그 + IP 차단) |
| Actuator 인증 우회/정보 노출 | 중간 | 완화 (Phase 6 — `/actuator/prometheus` 인증 예외 처리, health `show-details: never`) |
| 예외 응답 정보 노출 | 낮음 | 완화 (Phase 6 — AccessDenied/DataIntegrity/MessageNotReadable 핸들러 추가) |

---

## 2. 상세 점검 결과

---

### 2-1. MQTT 통신 보안

**위험**: MQTT 평문 통신 시 센서 데이터 도청 및 위변조 가능

**재현 시나리오**:
```bash
# 공격자가 네트워크에서 MQTT 패킷 스니핑
tcpdump -i eth0 -w capture.pcap port 1883
# → 차량 위치, 속도, 배터리 상태 등 민감 데이터 평문 노출
```

**적용된 대책**:
- TLS 1.2 이상 암호화 (8883 포트) 설정 준비
- 서버/클라이언트 상호 인증 (mTLS) — X.509 인증서 발급 스크립트 제공
- 클라이언트 인증서 없으면 브로커 연결 거부
- Spring Boot 백엔드(`MqttConfig.java`)와 Python 시뮬레이터 양쪽 모두 mTLS 클라이언트 코드 구현 완료(Phase 10)
  — `mqtt.tls.enabled` 플래그로 켜고 끌 수 있어 평소 로컬 개발은 평문으로 유지

**활성화 방법**:
```bash
# 1. 인증서 생성 (client.p12 / truststore.p12까지 함께 생성됨)
cd broker/certs && ./generate-certs.sh

# 2. mosquitto.conf 의 TLS 섹션 주석 해제
# 3. docker-compose restart mosquitto

# 4. 백엔드: .env에서 MQTT_TLS_ENABLED=true, MQTT_PORT=8883로 변경 후 재시작
# 5. 시뮬레이터: .env에서 TLS_CA_CERT/TLS_CLIENT_CERT/TLS_CLIENT_KEY 경로 설정 시 자동 적용
```

> `client.p12`의 개인키는 openssl이 만드는 PKCS#1 PEM을 Java가 직접 읽지 못해서 PKCS12로
> 한 번 변환한 것 — Mosquitto 서버/Python 시뮬레이터는 원본 PEM(`client.crt`/`client.key`)을 그대로 쓴다.

---

### 2-2. 인증 / JWT 보안

**위험**: 토큰 탈취, 브루트포스 공격

**적용된 대책**:

| 대책 | 구현 위치 |
|------|----------|
| HMAC-SHA256 서명 | `JwtTokenProvider.java` |
| 토큰 만료 (기본 24시간) | `application.yml` |
| 5회 실패 → 15분 IP 차단 | `BruteForceDetector.java` |
| STATELESS 세션 | `SecurityConfig.java` |
| Refresh Token (Redis opaque token, rotation) | `RefreshTokenService.java`, `AuthController.java` |
| 로그아웃 시 토큰 무효화 | `AuthController.logout()` — Redis에서 Refresh Token 삭제. Access Token은 Stateless라 자체 만료 전까지는 유효(블랙리스트 아님) |

**남은 개선 사항**:
- Access Token 만료시간 단축(현재 24h) — 로그아웃 즉시 무효화가 필요하면 블랙리스트 도입 검토

---

### 2-3. SQL Injection

**위험**: 악의적인 SQL 구문 삽입으로 DB 무단 접근

**테스트**:
```
POST /api/vehicles
{ "vehicleId": "'; DROP TABLE vehicles; --" }
```

**결과**: 차단됨  
**이유**: Spring Data JPA는 모든 쿼리를 PreparedStatement로 실행 — 파라미터 값은 SQL 구문으로 해석되지 않음

```java
// JPA 내부 동작
vehicleRepository.findByVehicleId(vehicleId)
// → SELECT * FROM vehicles WHERE vehicle_id = ? (바인딩)
```

---

### 2-4. IDOR (Insecure Direct Object Reference)

**위험**: 다른 사용자의 차량 데이터에 무단 접근

**현재 상태**: 부분 완화  
- 현재 단일 admin 계정 사용 → IDOR 위험 낮음
- Phase 4 개선 필요: 사용자-차량 소유 관계 검증

**개선 방향**:
```java
// 추가 예정: 차량 조회 시 소유자 검증
public VehicleResponse findByVehicleId(String vehicleId, String requestingUser) {
    Vehicle vehicle = vehicleRepository.findByVehicleId(vehicleId)...;
    if (!vehicle.getOwner().equals(requestingUser)) {
        throw new AccessDeniedException("접근 권한 없음");
    }
    return new VehicleResponse(vehicle);
}
```

---

### 2-5. 보안 헤더

**적용 헤더**:

| 헤더 | 값 | 목적 |
|------|-----|------|
| `X-Frame-Options` | DENY | Clickjacking 방지 |
| `X-Content-Type-Options` | nosniff | MIME 스니핑 방지 |
| `Strict-Transport-Security` | max-age=31536000 | HTTPS 강제 |
| `Referrer-Policy` | strict-origin-when-cross-origin | 레퍼러 정보 최소화 |
| `X-Trace-Id` | UUID | 요청 추적 |

---

### 2-6. Rate Limiting

**적용**: `RateLimitInterceptor.java`
- 분당 60회 초과 → 429 Too Many Requests
- Redis key: `rate_limit:{ip}`, TTL 1분
- 응답 헤더: `X-RateLimit-Limit`, `X-RateLimit-Remaining`
- 예외: `/api/auth/login` (브루트포스 감지가 별도 처리)

---

### 2-7. 민감 정보 관리

**점검 항목**:

| 항목 | 상태 |
|------|------|
| DB 비밀번호 | `.env`에만 존재, 코드 없음 |
| JWT 시크릿 | `.env`에만 존재, 코드 없음 |
| TLS 인증서 | `.gitignore`에 등록 (`broker/certs/*.key`) |
| `.env` 파일 | `.gitignore`에 등록 |
| InfluxDB 토큰 | `.env`에만 존재 |

---

### 2-8. 이상 접근 감지

**적용 내용**:
- 모든 요청 MDC 로깅 (`RequestLoggingFilter.java`)
  - traceId, clientIp, method, URI, status, duration
- 4xx/5xx 요청 자동 경고/에러 레벨 로깅
- 브루트포스 감지 (`BruteForceDetector.java`)

**로그 샘플**:
```
10:23:15 [a3f91c2b] [192.168.1.10] WARN  c.t.security.RequestLoggingFilter
  - [a3f91c2b] POST /api/auth/login → 401 (23ms) ip=192.168.1.10
10:23:16 [b7e21a4c] [192.168.1.10] WARN  c.t.security.BruteForceDetector
  - [BruteForce] 로그인 실패 ip=192.168.1.10 count=3/5
```

---

### 2-9. Actuator 엔드포인트 노출 (Phase 6에서 발견 및 수정)

**위험**: Prometheus 스크레이핑 대상인 `/actuator/prometheus`가 인증이 걸린 채 방치되어 있었고,
동시에 인증이 필요 없는 `/actuator/health`는 `show-details: always`로 DB/Redis 연결 상태 등
상세 정보를 익명 사용자에게 노출하고 있었다. Phase 5(모니터링) 완료 표시와 달리
실제로는 Prometheus가 401만 받고 메트릭을 전혀 수집하지 못하는 상태였다.

**수정**:
```java
// SecurityConfig.java
.requestMatchers("/actuator/health", "/actuator/prometheus").permitAll()
```
```yaml
# application.yml
management.endpoint.health.show-details: never
```

**운영 배포 시 추가 권고**: 애플리케이션 레벨 인증 대신 보안그룹/리버스프록시로
`/actuator/**` 자체를 내부망에서만 접근 가능하도록 제한할 것 (`docs/deployment-guide.md` 참고).

---

## 3. UN R155 / ISO SAE 21434 대응 현황

자동차 사이버보안 국제 규제 기준으로 점검:

| 규제 요구사항 | 구현 여부 | 비고 |
|--------------|----------|------|
| 차량-서버 통신 암호화 | 준비됨 | MQTT TLS 설정 완료 |
| 접근 제어 및 인증 | 구현됨 | JWT + X.509 |
| 보안 이벤트 로깅 | 구현됨 | MDC 감사 로그 |
| 이상 접근 감지 | 구현됨 | BruteForce, RateLimit |
| 보안 취약점 관리 | 진행중 | 이 문서 |
| 펌웨어/소프트웨어 보안 업데이트 | 미구현 | Phase 5 배포 시 고려 |

---

## 4. 잔여 취약점 및 향후 계획

| 취약점 | 우선순위 | 계획 |
|--------|----------|------|
| IDOR 완전 차단 | 높음(다중 사용자 도입 시) | 현재 admin 단일 계정이라 보류. 다중 사용자 도입 시 사용자-차량 소유 관계 검증 필요 |
| MQTT 1883 포트 운영 차단 | 높음 | 코드/설정은 Phase 10에서 완료. 운영 배포 시 인증서 발급 + `mqtt.tls.enabled=true` + 1883 포트 자체를 닫는 것은 배포 단계에서 적용 필요 |
| CSP 헤더 추가 | 낮음 | Content Security Policy 설정 |
| 의존성 취약점 스캔 | 중간 | OWASP Dependency-Check 도입 |
| Access Token 즉시 무효화 | 낮음 | 현재는 만료시간(24h)까지 유효. 필요 시 블랙리스트 또는 만료시간 단축 검토 |

---

## 5. 참고 기준

- [OWASP Top 10 2021](https://owasp.org/Top10/)
- [UN Regulation No. 155 (Vehicle Cybersecurity)](https://unece.org/transport/documents/2021/03/standards/un-regulation-no-155-cyber-security-and-cyber-security)
- [ISO/SAE 21434 Road Vehicles — Cybersecurity Engineering](https://www.iso.org/standard/70918.html)
- [MQTT Security Fundamentals](https://www.hivemq.com/mqtt-security-fundamentals/)
