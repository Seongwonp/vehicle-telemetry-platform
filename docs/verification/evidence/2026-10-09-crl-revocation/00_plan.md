# 00 — 실행 전 기준 (2026-10-07 21:46 KST 작성, 실행 전)

기준 커밋 `ebae223` + 이 작업의 미커밋 변경. Docker engine 29.7.2, Compose 5.4.0, mosquitto 2.0.22, 호스트 OpenSSL 3.2.1(Git for Windows mingw64).

대상(공개 정보만): SIM-024 serial `4A42F92149A5A5AB7C66574317E62916C6738F96`, SIM-088 serial `4A42F92149A5A5AB7C66574317E62916C6738FD6`.
발급 CA: `CN=TelemetryCA`, SHA-256 `14:5E:75:E1:...:9F:96`.

| # | 확인 | 성공 기준 | 횟수 |
| --- | --- | --- | --- |
| 1 | CRL 내용 | `openssl crl -noout -text`에 위 두 serial만, `nextUpdate` = 발행 + 365일, CA 서명 검증 OK | 1 |
| 2 | SIM-024·SIM-088로 8883 발행 | 둘 다 `mosquitto_pub` 0이 아닌 종료, 브로커 로그에 revoked 사유 | 각 1 |
| 3 | 정상 연결 유지 | 재기동 뒤 backend `u'telemetry-backend'` 8883 접속·`Error subscribing` 0, SIM-001~003 접속 + InfluxDB speed 행 증가 | 1 |
| 4 | 폐기 안 된 차량 | SIM-050 1회 발행 exit 0, 구독(backend 인증서 아님 — 브로커 로그 접속 + ACL상 쓰기) 확인 | 1 |
| 5 | 인증서 없는 클라이언트 | 거부, `peer did not return a certificate` | 1 |
| 6 | CRL 만료 | 문서로 판단(시간 조작 실험 안 함). 갱신 절차·날짜 기록 | — |
| 7 | 되돌리기 | `crlfile` 줄 제거 + 재시작 → SIM-024 접속 성공 → 재적용 → 다시 거부 | 1 |

CI/새 환경: 임시 디렉터리 사본에서 `generate-certs.sh` → `crl.pem` 생성(새 CA라 폐기 0건), `crl.pem` 자리 빈 디렉터리 가드.
