# 차량 개인키 일부 노출 범위 — 2026-10-08

상태: **SIM-024·SIM-088 인증서 폐기(CRL) 적용 완료 — 2026-10-07 실행, 각 1회 관찰.** 아래 [§CRL 적용 결과](#crl-적용-결과-2026-10-07). 새 키·인증서는 발급하지 않았다(요청 없음).

노출 범위: **확인 완료(범위 한정).** **확인 가능한 기록 53개에서 두 키의 본문 각 1줄을 발견했으며, 그 밖의 노출 여부까지 보장하지는 않는다**(이 PC에 남은 이 세션 기록만 봤다 — 모델 API로 전송된 사본·다른 도구·다른 세션은 확인 범위 밖). 키 내용은 이 문서·증거·도구 출력 어디에도 다시 쓰지 않았다. 식별은 파일 경로·차량 ID·인증서 지문·줄 위치(번호)로만 한다.

## 비밀번호 출력 사건과 별개다

| | 10-08 `MQTT_TLS_STORE_PASSWORD` 출력 | 10-07 차량 키 일부 줄 |
| --- | --- | --- |
| 누가·언제 | 10-08 ① 작업 서브에이전트, 백엔드 env 확인 중 | 10-07 obd-bridge 서브에이전트, 범위 없는 `grep`을 `broker/`에 돌림 |
| 무엇 | keystore 비밀번호(저장소에 커밋된 **공개 기본값**) | **차량 개인키 파일 2개의 본문 줄 각 1개** |
| 성격 | 공개 문자열 — 새로 알려진 정보 없음 | 비공개 자료의 **일부** |

## 확인 방법

이 세션의 모든 대화 기록(`~/.claude/projects/D--vehicle-telemetry-platform/**/*.jsonl`, 메인 + 서브에이전트 37개)과 `tool-results` 파일 13개, 총 53개를 스크립트로 훑었다.
`broker/certs/*.key`·`broker/certs/vehicles/*.key`(103개)의 **본문 줄**을 메모리에 올려, 기록의 JSON 문자열을 풀어 줄 단위로 **같은 줄이 있는지**만 셌다. 출력은 파일명·건수·줄 번호뿐이다.

## 결과

| 노출 위치 | 대상 | 노출된 것 | 전체 대비 |
| --- | --- | --- | --- |
| 10-07 obd-bridge 서브에이전트 대화 기록(그 도구 출력 1건) | `broker/certs/vehicles/SIM-024.key` | 본문 **15번째 줄 1줄** | 본문 26줄 중 1줄(약 4%) |
| 같은 출력 | `broker/certs/vehicles/SIM-088.key` | 본문 **13번째 줄 1줄** | 본문 26줄 중 1줄(약 4%) |

- **PEM 헤더만 나온 것이 아니라 본문 줄이 나왔다.** 다만 **키 전체가 아니다**(줄 1개씩, 그 기록에 `BEGIN PRIVATE KEY` 헤더는 없음).
- 다른 52개 기록에는 **어떤 키의 본문 줄도 없다.** 메인 세션 기록의 `BEGIN PRIVATE KEY` 31건·다른 서브에이전트의 6·11건은 "키 종류 표시"(파일 첫 줄 형식 집계·문서 문장)로, 본문 줄 일치는 0이다.
- **CA 키(`ca.key`)·서버 키·백엔드 키의 본문 줄은 어디에도 없다.** CA 교체 근거 없음.

| 차량 | 인증서 지문(SHA-256) | 만료 | 현재 사용 |
| --- | --- | --- | --- |
| SIM-024 | `06:EE:04:88:42:AC:C9:8D:1C:29:25:92:EA:6B:54:3E:E2:93:7D:02:F4:B9:32:A6:D9:4E:41:27:C2:AF:2D:B9` | 2036-09-02 | **미사용** — 시뮬레이터 기본 `VEHICLE_ID_OFFSET=0`·`VEHICLE_COUNT=3`(`.env`도 3) → SIM-001~003만 접속 |
| SIM-088 | `A8:48:02:EB:2E:3C:E4:52:40:5B:6B:BE:2D:F7:1E:FC:B1:A6:DC:E1:DC:40:BA:41:11:21:F3:29:9F:31:17:70` | 2036-09-02 | **미사용** — 같음 |

## 판단

- 노출은 키 2개의 본문 약 4%씩이다. 이것만으로 키를 복원할 수 있다는 근거는 없지만(부분 노출 공격은 보통 훨씬 큰 비율이나 구조화된 위치를 요구한다), **0이라고 단정하지도 않는다.**
- 두 차량은 **계속 사용하지 않는다**(시뮬레이터 범위를 24·88을 포함하게 늘리지 않는다). **미사용만으로 옛 인증서의 접속이 막히지는 않는다** — 그래서 CRL로 막았다(아래).
- 외부 장치(실동글)에 이 두 ID를 쓰지 않는다.

## 교체·차단 방법 (10-08 준비 → 1번은 2026-10-07 실행, 결과는 아래 §CRL 적용 결과)

**새 인증서를 만드는 것만으로 옛 인증서는 무효화되지 않는다.** 브로커는 `require_certificate true` + `use_identity_as_username true`로 **CA가 서명한 인증서라면 누구든** 그 CN으로 받아들인다.
(작성 당시 `mosquitto.conf`에 `crlfile`이 없었다 — 지금은 있다.)

1. **옛 인증서 차단(필수)** — CRL:
   - 두 인증서의 일련번호를 공개 정보로 확인: `openssl x509 -in broker/certs/vehicles/SIM-024.crt -noout -serial`(SIM-088도).
   - `generate-certs.sh`는 `openssl x509 -req`로 발급해 `openssl ca`용 인덱스가 없다 → 임시 CA 작업 디렉터리(호스트, 저장소 밖)에 `index.txt`·`crlnumber`·최소 `openssl.cnf`를 만들고 `openssl ca -revoke <crt>` 두 번 → `openssl ca -gencrl -out crl.pem`. **CA 키는 이 호스트 작업에서만 쓰고 컨테이너에 마운트하지 않는다.**
   - `mosquitto.conf` 8883 리스너에 `crlfile /mosquitto/certs/crl.pem`, compose mosquitto에 `crl.pem` 한 파일만 읽기 전용 마운트, `docker compose restart mosquitto`.
   - 확인: 옛 SIM-024 인증서로 접속 → 거부(TLS 실패), SIM-001로 접속 → 정상. CRL에도 만료(`nextUpdate`)가 있으므로 갱신 주기를 정해야 한다.
   - 대안(미검증): ACL로 `user SIM-024`의 쓰기를 막는 방법은 `pattern write vehicle/telemetry/%u`와의 우선순위를 확인하지 않았다 — 접속 자체를 막지 못하므로 CRL이 정공법이다.
2. **새 키·인증서(필요할 때만)** — 같은 CA로 해당 차량만 재발급(`generate-certs.sh`의 차량 발급 부분과 같은 `openssl req`/`x509 -req`, 새 키). **CA 키 노출 근거가 없으므로 CA 전체 교체는 하지 않는다.**
3. ~~후속 보안 작업으로 남긴다~~ → 옛 두 인증서 차단(CRL)은 사용자 승인으로 실행했다(아래). **새 키·인증서 발급은 여전히 하지 않았다** — 두 ID를 다시 쓸 일이 생길 때만.

### CRL 적용 시 검증 계획 (적용할 때 실행)

실행 전에 기준을 적고, 각 1회 관찰로 기록한다.

| # | 확인 | 성공 기준 |
| --- | --- | --- |
| 1 | CRL 내용 | `openssl crl -in crl.pem -noout -text`에 SIM-024·SIM-088 일련번호 **두 개만**, `nextUpdate` 확인 |
| 2 | 옛 SIM-024·SIM-088 인증서로 8883 접속 | 둘 다 TLS 핸드셰이크 거부(`mosquitto_pub` 실패, 브로커 로그에 인증서 폐기 사유) |
| 3 | **정상 인증서 연결 유지** | 백엔드(`telemetry-backend`)가 재기동 뒤 8883 재접속·구독 성공(`Error subscribing` 없음), 시뮬레이터 SIM-001~003 발행이 InfluxDB에 계속 쌓임(행 수 증가) |
| 4 | 다른 차량 인증서 | 폐기 안 된 차량(예: SIM-050)으로 1회 발행 → 수신 |
| 5 | 인증서 없는 클라이언트 | 여전히 거부(`peer did not return a certificate`) |
| 6 | CRL 만료 대비 | `nextUpdate` 전 재생성 절차와 날짜를 runbook에 기록. 만료된 CRL로 브로커가 **정상 차량까지** 거부하는지 확인 후 운영 규칙 결정 |
| 7 | 되돌리기 | `crlfile` 줄·마운트 제거 + 재시작으로 원상 복귀되는지 |

CA 키는 이 작업에서도 호스트의 임시 디렉터리에서만 쓰고 컨테이너에 마운트하지 않는다. CA 전체 교체는 하지 않는다(CA 키 노출 근거 없음).

## CRL 적용 결과 (2026-10-07)

- 기준 커밋 `ebae223` + 이 작업의 미커밋 변경. Windows 11 Pro, Docker engine 29.7.2 / Compose 5.4.0, mosquitto 2.0.22, 호스트 OpenSSL 3.2.1(Git for Windows).
  호스트 8883이 이 PC의 Windows 동적 제외 포트 범위라 셸 env `MQTT_TLS_PORT=18883`으로 게시했다(`.env` 수정 없음). 컨테이너 안은 8883 그대로.
- **기본(mTLS) 프로파일**에서 확인했다. dev override(`mosquitto-dev.conf`)는 1883 평문 리스너뿐이라 8883이 없고 CRL이 적용될 자리도 없다(마운트는 같이 들어가지만 안 읽힌다).
- 증거: [`evidence/2026-10-09-crl-revocation/`](evidence/2026-10-09-crl-revocation/) — 키·비밀번호 내용 없음(serial·subject·날짜·지문·종료 코드·브로커 로그 줄). 실행 전 기준은 `00_plan.md`.

### 폐기한 인증서 (공개 정보)

| CN | serial | 사유 | 폐기 시각(UTC) |
| --- | --- | --- | --- |
| SIM-024 | `4A42F92149A5A5AB7C66574317E62916C6738F96` | keyCompromise | 2026-10-07 12:46:14 |
| SIM-088 | `4A42F92149A5A5AB7C66574317E62916C6738FD6` | keyCompromise | 2026-10-07 12:46:14 |

발급 CA `CN=TelemetryCA`(SHA-256 `14:5E:75:E1:…:9F:96`). CA는 다시 만들지 않았고 새 차량 인증서도 발급하지 않았다.

### 어떻게 만들었나 — 위 1번 절차를 상태 없는 스크립트로

- **추적 파일 `broker/certs/revoked-certs.txt`**가 폐기 목록의 원본이다(CA 지문·serial·시각·사유·CN, 공개 정보만).
- **`broker/certs/generate-crl.sh`**가 그 목록으로 매번 저장소 밖 임시 디렉터리(`mktemp -d`)에 `index.txt`·`crlnumber`·최소 `openssl.cnf`를 만들고
  `openssl ca -gencrl -crldays 365`로 `broker/certs/crl.pem`을 쓴 뒤 임시 디렉터리를 지운다. CA 서명 검증이 통과해야만 교체한다.
  `ca.key`는 이 호스트 작업에서만 읽는다 — 어느 컨테이너에도 마운트되지 않는다(`02_…`: `ca.key` 마운트 서비스 `[]`).
  위 1번에 적은 `openssl ca -revoke` 대신 목록에서 index를 만든 이유: CRL은 365일마다 다시 서명해야 하는데, 임시 디렉터리의 index는 남지 않는다.
  목록 파일이 있으면 갱신이 같은 명령 한 줄이다.
- 목록의 CA 지문이 지금 `ca.crt`와 다른 줄은 건너뛴다 → **CA를 새로 만드는 환경(CI, 새 PC)에서는 빈 CRL**이 나온다.
- `crl.pem`은 `.gitignore`의 `broker/certs/*.pem`에 걸린다 — 공개 정보지만 다른 인증서 산출물처럼 **생성물로 두고 추적하지 않는다**(원본은 목록 파일).
- `mosquitto.conf` 8883 리스너에 `crlfile /mosquitto/certs/crl.pem`, compose는 **mosquitto에만** `crl.pem` 한 파일 읽기 전용 마운트.

### 검증 (각 1회)

| # | 확인 | 결과 | 증거 |
| --- | --- | --- | --- |
| 1 | CRL 내용 | 폐기 **2건(위 두 serial만)**, 사유 Key Compromise, Last Update 2026-10-07 12:54:35 GMT, **Next Update 2027-10-07 12:54:35 GMT**, CA 서명 `verify OK`. 호스트 `openssl verify -crl_check`: SIM-024·088 `error 23 certificate revoked`, SIM-050·SIM-001·backend `OK` | `01_…` |
| 2 | SIM-024·SIM-088로 8883 발행 | 둘 다 `mosquitto_pub exit=7`(`The connection was lost`). 브로커 로그는 `OpenSSL Error … certificate verify failed`까지만 남긴다 → **사유는 서버가 보낸 TLS alert로 확인**: `ssl/tls alert certificate revoked … SSL alert number 44`(호스트 `openssl s_client`, 둘 다 exit 1). 폐기된 CN의 접속 성공 줄 0 | `03_…`·`06_…` |
| 3 | 정상 인증서 연결 유지 | 기본 프로파일로 재생성한 backend(`MQTT_TLS_ENABLED=true`, `MQTT_PORT=8883`): `u'telemetry-backend'` 두 연결, `mqttInbound`·`mqttBrokerMetricsInbound` 시작, `Error subscribing` 0. simulator `u'SIM-001'`~`SIM-003'` 접속. InfluxDB speed 행 SIM-001 135 → 151 → 166(15초 간격, 002·003 같음). **mosquitto 재기동 뒤**에도 backend·simulator 재접속, 행 267 → 282 → … 538 | `05_…`·`06_…`·`07_…` |
| 4 | 폐기 안 된 차량(SIM-050) 1회 발행 | `mosquitto_pub exit=0`, 브로커 `u'SIM-050'` 접속, **InfluxDB에 SIM-050 1행**(테스트 행으로 남김) | `04_…`·`05_…` |
| 5 | 인증서 없는 클라이언트 | `exit=7`, 브로커 로그 `peer did not return a certificate` | `04_…` |
| 6 | CRL 만료 | 아래 §만료 위험. 시계 조작 실험은 하지 않았다 | `09_…` |
| 7 | 되돌리기 → 재적용 | `crlfile` 줄만 뺀 설정으로 mosquitto 재생성 → SIM-024·088 TLS 핸드셰이크 **성공**(`Verification: OK`, MQTT 발행은 안 함). 저장소 설정으로 재생성 → 다시 `alert certificate revoked`. **거부 원인이 CRL이라는 대조이기도 하다** | `07_…` |

- #3 관찰 하나: 마지막 mosquitto 재시작(최종 `crl.pem`으로) 뒤 backend에 `Error subscribing … Timed out waiting for a response from the server`가 **1건** 났다. TLS 접속은 성공한 뒤의 MQTT SUBSCRIBE 응답 시간 초과로,
  [`2026-10-01-mqtt-ack-boundary.md`](2026-10-01-mqtt-ack-boundary.md)에 이미 기록된 현상이다. 수신은 계속됐다(10초에 1552 → 1584). 그 앞 재기동에서는 0건이었다.
- 브로커의 거부 로그만으로는 "폐기"와 다른 검증 실패(만료·다른 CA)를 구분할 수 없다. 운영에서 사유를 보려면 클라이언트 쪽 TLS alert(44)나 `openssl verify -crl_check`로 본다.

### CRL 만료 위험 (#6)

- **`nextUpdate`(지금 2027-10-07 12:54:35 GMT)가 지나면 정상 차량과 backend까지 전부 거부될 것으로 판단한다.** 근거: 호스트 OpenSSL에서 같은 leaf CRL 검사(`-crl_check`)를
  `-attime`으로 돌리면 nextUpdate 직전은 SIM-001·backend `OK`, 직후(2027-10-08)는 셋 다 `error 12 CRL has expired`(`09_…`). mosquitto는 `crlfile`이 있으면 OpenSSL의
  CRL 검사를 켜고, 검증 콜백이 OpenSSL의 사전 검증 결과를 그대로 따르는 것으로 알고 있다 — **이번에 mosquitto 소스를 다시 열어 확인하지 않았고 브로커 시계를 바꿔 재보지도 않았다.**
- **운영 규칙**: 만료 30일 전(2027-09-07 무렵)까지 `bash broker/certs/generate-crl.sh` → `docker compose restart mosquitto`(브로커는 기동할 때만 CRL을 읽는다).
  남은 기간 확인: `openssl crl -in broker/certs/crl.pem -noout -nextupdate`. 만료를 잡는 알림은 없다 — 백로그.
- `generate-certs.sh`를 다시 돌리면 CA부터 새로 만들어 모든 인증서가 바뀐다. **폐기만 할 때는 쓰지 않는다.**

### 되돌리기 절차

1. `broker/config/mosquitto.conf`에서 `crlfile /mosquitto/certs/crl.pem` 줄을 지운다(마운트 줄은 남아도 무해 — 지우려면 `docker-compose.yml` mosquitto의 `crl.pem` 줄도).
2. `docker compose up -d mosquitto`(이번 확인은 재생성으로 했다 — 편집 후 `restart`만으로 반영되는지는 안 쟀다). backend·simulator는 자동 재접속.
3. 확인: 폐기된 인증서로 TLS 핸드셰이크가 다시 성공한다(#7) — **즉 두 노출 키로 다시 붙을 수 있게 된다.** CRL 만료 사고 같은 긴급 상황에서만 쓰고, 그때는 CRL 재생성이 먼저다.

### 새 환경·CI

- `generate-certs.sh`가 끝에서 `generate-crl.sh`를 부른다 → 새 CA라 **폐기 0건 빈 CRL**. 빈 디렉터리 가드에 `crl.pem`을 넣었다(인증서 전에 `up`하면 Docker가 `crl.pem/` 디렉터리를 만든다).
- 임시 사본에서 확인(`08_…`): Windows Git Bash(`MSYS_NO_PATHCONV=1`)와 Linux(Ubuntu 22.04 · OpenSSL 3.0.2, CI 러너 흉내) 둘 다 exit 0, `crl.pem/` 빈 디렉터리 제거 후 `No Revoked Certificates`, `verify OK`.
  **Windows 첫 실행은 실패했다** — `MSYS_NO_PATHCONV=1`이면 Git Bash가 `/c/...` 경로를 mingw openssl에 바꿔주지 않는다. CA 경로를 `cygpath -m`으로 넘기도록 고쳤다(Linux엔 cygpath가 없어 그대로).
- `docker compose config --quiet`: 기본 / dev+simulator+scale 둘 다 exit 0(`02_…`). CI 자체(GitHub Actions)는 이번에 돌리지 않았다 — **미검증**.

### 한계

- 각 항목 1회 관찰. 부하 중 CRL 검사 비용은 재지 않았다(연결 수립 때만 검사).
- 거부 사유(alert 44)는 TLS 1.2 핸드셰이크에서 확인했다. TLS 1.3 클라이언트는 alert가 첫 읽기 시점에 보일 수 있다(동작은 같은 거부).
- native Linux 파일 바인드에서 `crl.pem` 교체가 재시작 없이 보이는지는 미검증 — 어차피 재시작이 절차다.
- obd-bridge 등 외부 장치 경로는 확인하지 않았다(이 PC 밖 MQTT 클라이언트 없음, cert-access-scope §3).

## 이후 규칙

`broker/certs/` 아래를 대상으로 하는 범위 없는 `grep`·`cat`·`strings` 금지. 인증서 확인은 `openssl x509 -noout -subject -issuer -dates -fingerprint`와 파일 첫 줄 형식 집계만.
