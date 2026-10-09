# 동글 정차 수집 계획 — 2026-10-10

## 실행 상태

- 상태: **실차 미실행.** 집 리허설 1·2(에뮬레이터)만 2026-10-09 각 1회 통과 — 아래 "노트북 준비 상태". 이 문서는 실행 전에 가설·대조군·성공 기준을 고정하기 위한 것이다(실험 규칙 1).
- 범위: **정차·시동 공회전(또는 READY)·주차 브레이크 상태의 수집만.** 주행 시험은 이 계획 밖이다.
- 장비: OBDLink EX(USB) → Windows 노트북의 `obd-bridge` → mTLS MQTT → 기존 스택.
- 차량 순서: **1차 코나 2017 가솔린**, 1차가 5단계까지 판정된 뒤 **2차 그랜저 2024 하이브리드**.
- 기준 코드: 태그 `v0.9-pre-vehicle`(= `911c2e4`). 이후 HEAD `303651e`까지는 문서 3개만 바뀌었다
  (`git diff --stat v0.9-pre-vehicle HEAD` — 코드·compose·broker 변경 0). 실행 당일 다시 확인해 기록한다.
- 근거 문서: [`obd-bridge/README.md`](../../obd-bridge/README.md), [ELM327 실측 절차](../runbook/elm327-measurement.md),
  ADR-030, [증거 정책](../evidence-policy.md). 결과는 단일 차량·각 1회라 **`부분 검증`이 상한**이다.

## 안전 — 협상 대상 아님

- **주행 중 조작 금지.** 모든 단계는 정차·변속 P·주차 브레이크 체결 상태에서만 한다. 기어를 D/R에 넣는 순간 그 회차는 중단·무효.
- 실내 밀폐 공간(지하 주차장 환기 불량, 차고 문 닫힘)에서 엔진 공회전을 하지 않는다. 야외 또는 환기되는 곳.
- 노트북은 운전석이 아닌 곳(조수석 바닥·트렁크 쪽)에 두고, 케이블이 페달·변속 레버에 걸리지 않게 고정한다.
- **읽기 전용 요청만** 쓴다. OBDwiz의 DTC 삭제(Mode 04)·액추에이터·ECU 쓰기 메뉴를 누르지 않는다.
- 핫스팟 끊기(5단계)는 휴대폰 조작이다 — 차량 조작이 아니므로 정차 상태에서만 한다.

## 공개 증거 마스킹

- **GPS**: 브리지는 `gps` 키를 **보내지 않는다**(README 계약 매핑 표, `test_mapping_contract.py`가 "gps/dtc 키 없음"을 단언).
  실행 후 `records.log`에서 `"gps"` 0건을 `grep -c`로 확인해 증거에 남긴다(0이 아니면 그 파일은 비공개로만 보관).
- **위치**: 스크린샷(OBDwiz·지도·휴대폰 핫스팟 화면)·사진의 주소·좌표·간판·번호판은 공개 전에 가린다. 원본은 비공개 보관.
- **VIN**: OBDwiz가 Mode 09로 VIN을 표시할 수 있다. 공개본은 앞 3자리(WMI)만 남기고 `KMH**************` 형태로 가린다.
- 공개 샘플은 마스킹 후 checksum을 기록한다(runbook "기록 필드"). 키·`ca.key`는 어떤 증거에도 넣지 않는다.

## 출발 전 확정할 연결 사항 (집에서 리허설로 닫는다)

리허설은 **같은 노트북·같은 핫스팟·ELM327-emulator**로 1~4단계를 한 번 돌려 연결 문제를 차 앞에서 처음 만나지 않게 한다.

| 항목 | 현재 사실 | 계획 |
| --- | --- | --- |
| 브로커 위치 | compose 8883은 기본 `127.0.0.1` 게시(`MQTT_TLS_BIND` 기본), 다른 장치에서 닿지 않는다 | **권장: 스택을 같은 노트북에서 띄운다.** 그러면 `MQTT_TLS_BIND`를 열 필요가 없다. 다른 PC를 쓰면 그 PC `.env`에 `MQTT_TLS_BIND=<NIC IP>` — 공유망 금지 |
| 호스트 포트 | 이 개발 PC는 Windows 동적 제외 범위(8875–8974)에 8883이 걸려 `MQTT_TLS_PORT=18883`으로 돌렸다(deployment-guide §8) | `netsh int ipv4 show excludedportrange protocol=tcp` 출력 저장. 바꿨으면 브리지 `--mqtt-port`도 같은 값 |
| 호스트명 검증 | 서버 인증서 `CN=mosquitto`, **SAN 없음**(`openssl x509 -ext subjectAltName` → "No extensions"). paho `tls_set`은 호스트명 검증을 켠다 | **검증을 끄지 않는다**(`tls_insecure_set`·`CERT_NONE` 금지). `localhost`로 붙으면 CN 불일치로 실패할 것으로 예상(미확인). `hosts`에 `127.0.0.1 mosquitto`를 넣고 `--mqtt-host mosquitto`로 붙는다. SAN 없는 CN 대체 매칭을 이 Python/OpenSSL이 받아주는지는 **리허설에서 확인**하고, 거부되면 출발하지 않는다(서버 인증서 SAN 재발급은 결정 대기 D1) |
| 차량 인증서 | 공용 CA로 `SIM-001`~`SIM-100` 발급. **`SIM-024`·`SIM-088`은 CRL로 폐기**(`broker/certs/revoked-certs.txt`) | **`SIM-024`·`SIM-088` 사용 금지.** `VEHICLE_ID` = 인증서 CN이어야 한다(ACL `vehicle/telemetry/%u`). 결정 대기 D2 |
| CN 불일치 | MQTT 3.1.1에서 ACL 거부가 PUBACK과 함께 조용히 버려질 수 있다(README 결정 대기 3, 미측정) | 4단계 대조가 이를 잡는다(PUBACK 수 > Kafka 고유 수). 리허설에서 Kafka 도착을 먼저 확인 |
| 시뮬레이터 | 기본 `--profile simulator`가 SIM-001~003을 발행 | **수집 중 시뮬레이터를 띄우지 않는다** — Kafka end offset 차이를 이 차량에만 귀속시키기 위해 |
| baud | 브리지는 `OBD_BAUDRATE=0`(자동). `tools/measure_polling.py`는 `--baudrate` 기본 38400을 그대로 넘긴다 | 리허설·1단계에서 OBDwiz가 보고한 baud를 기록하고 `measure_polling.py --baudrate`에 같은 값을 준다 |

### 결정 대기 (출발 전 사용자 결정)

- **D1 — 서버 인증서 호스트명.** (a) `hosts` 매핑 `mosquitto` (코드·인증서 변경 없음, 권장) (b) SAN 포함 서버 인증서 재발급 —
  `generate-certs.sh`는 **CA까지 새로 만들어** CRL 지문(`revoked-certs.txt` 첫 칸)과 기존 차량 인증서를 전부 무효화하므로 그대로 돌리지 않는다.
- **D2 — 차량 ID.** (a) 폐기되지 않은 기존 인증서 하나(예: `SIM-099`, 시뮬레이터 `VEHICLE_COUNT` 범위 밖) — 즉시 가능하지만
  실차 데이터가 `SIM-` 이름으로 저장·집계된다 (b) 기존 CA로 실차 전용 CN(예: `KONA17-01`, `GRDR24H-01`, 정규식 `^[A-Z0-9-]{4,20}$`)
  하나씩 발급 — 스크립트 전체가 아니라 4단계 openssl 두 줄만 기존 CA로. 어느 쪽이든 `vehicle_id`와 CN이 같아야 한다.
- ~~**D3 — MQTT 프로토콜.**~~ — **v5로 확정(2026-10-09 리허설 2, 1회 관찰).** v5에서 CN 불일치 발행이 PUBACK 0x87로 브리지에 드러났고
  (`rejected_by_broker=18`, `acked=0`) 거부분은 spool에 남았다 — 조용한 유실 아님. 실차 회차는 `MQTT_PROTOCOL=5`(노트북 `.env`)로 하고 환경 기록에 남긴다.
  3.1.1 쪽 동작(조용한 PUBACK 여부)은 여전히 미측정.
- ~~**D4 — `measure_polling.py`의 고정 문자열.**~~ — **해결(2026-10-09).** `--note`가 **필수 인자**가 되어 결과 JSON `note`에 그대로 들어가고(상수 삭제),
  `adapter`(포트·프로토콜·`ATI`·`STI` 응답)를 함께 남긴다(`tests/test_measure_polling.py`). 3단계 명령에 `--note "실차 — <차종>, OBDLink EX, <시동 상태>"`를 붙인다.
- **D5 — ML 오프라인 채점 도구.** 아래 가설 H의 ML 칸은 저장된 payload를 시뮬레이터 학습 모델로 채점하는 도구가 필요하다(미작성).

## 노트북 준비 상태 (2026-10-09)

노트북 `LAPTOP-MV7C23S3`(Windows 11 Pro 10.0.26200), 저장소 `5561d98`(태그 `v0.9-pre-vehicle` 그대로). **실차·동글은 아직 연결하지 않았다.**

### 끝난 것

| 항목 | 결과 |
| --- | --- |
| Python | `obd-bridge/.venv` = Python 3.12.10(`python -m venv`). `requirements.txt` 그대로 설치(`PYTHONUTF8=1`). pytest **90 passed**(수정 전) → D4 수정 후 **94 passed, skip 0**. 3.12 고유 실패 0. 첫 실행의 setup 오류 42건은 `%TEMP%\pytest-of-<사용자>` 폴더 권한 문제(`--basetemp`로 해결, README) |
| D4 | 해결 — 위 "결정 대기" |
| 인증서 | 이 노트북에서 `generate-certs.sh` **1회** 실행 → **이 노트북 전용 새 CA**(데스크톱 CA와 다름). 차량 `SIM-001~003`, `KONA17-01`, `GRDR24H-01`. `openssl verify -crl_check`: KONA17-01 **OK**, GRDR24H-01 **OK**, 서버 `CN=mosquitto` **OK**. 이 노트북에서는 D2를 (b) "실차 전용 CN"으로, 단 **기존 CA가 아니라 새 CA로** 닫았다 |
| CRL | `generate-crl.sh` → **폐기 0건**(예상대로 — `revoked-certs.txt`의 `SIM-024`·`SIM-088` 줄은 데스크톱 CA 지문이라 이 CA에 해당 없음). `crl.pem` 존재, nextUpdate 2027-10-09 |
| 포트 | `netsh int ipv4 show excludedportrange protocol=tcp`: 제외 범위 50000–50059, 50836–50935, 51390–51489, 53872–53971, 56298–56397, 59386–59485, 59744–59843, 61014–61113 — **8883은 걸리지 않는다.** `MQTT_TLS_PORT=8883` 그대로, 게시 `127.0.0.1:8883` |
| 스택 | 기본 compose(mTLS, simulator 프로파일 없음) `docker compose up -d --build`. backend `/actuator/health` UP. **`kafka-init`이 첫 기동에서 실패**(`kafka/init-topics.sh`가 `core.autocrlf=true`로 CRLF 체크아웃 → `$'\r': command not found`). 작업 트리 파일만 LF로 바꿔(내용 diff 0) 재실행 → 토픽 6개 생성. **새로 클론하면 재발한다**(`.gitattributes`에 `*.sh` LF 규칙 없음 — 미수정) |
| Grafana | `.env`에 `GRAFANA_PG_READER_PASSWORD` 생성·추가(값 비공개) → `scripts/grafana-pg-reader.sh` OK → grafana 재생성. 대시보드 변수 쿼리(`telemetry-pg-registry`)가 `GRDR24H-01`·`KONA17-01` 포함 9대를 반환 |
| 차량 등록 | 관리자 로그인 200, `POST /api/vehicles` `KONA17-01`("코나 2017 가솔린") **201**, `GRDR24H-01`("그랜저 2024 하이브리드") **201**, 소유자 = 관리자. `GET /api/vehicles`에 둘 다 `active` |
| 브리지 설정 | `obd-bridge/.env`(추적 안 함): `VEHICLE_ID=KONA17-01`, `MQTT_HOST=mosquitto`, `MQTT_PORT=8883`, **`MQTT_PROTOCOL=5`**(D3을 이 노트북 설정에서는 5로 — 환경 기록에 남긴다), TLS 경로 3개(이 노트북 `broker/certs`의 `ca.crt`·`vehicles/KONA17-01.crt/.key`), `OBD_BAUDRATE=0`. `OBD_PORT`·`SPOOL_DIR`은 비워 두고 회차마다 CLI로 준다. 브리지는 dotenv를 읽지 않으므로 셸로 올린다(파일 머리 주석) |
| 호스트명 검증(사전 확인) | `--mqtt-host localhost`로 브리지 기동 → `connect_failures=4`, `published=0`, 브로커 로그 `tls/alert bad certificate`(클라이언트가 서버 인증서를 거부). 같은 인증서로 Python ssl 핸드셰이크만 따로: `server_hostname=localhost` → **`Hostname mismatch, certificate is not valid for 'localhost'`**, `server_hostname=mosquitto`(127.0.0.1로 TCP) → **OK, TLSv1.3, peer CN mosquitto**. 즉 **검증은 켜져 있고, 이 Python 3.12.10/OpenSSL 3.0.16은 SAN 없는 CN 대체 매칭을 받아준다**(`hostname_checks_common_name=True`). paho·브로커 ACL까지 포함한 최종 확인은 `hosts` 뒤 리허설에서 → **리허설 1에서 연결·발행 성공(1회, 아래 4번)** |
| 에뮬레이터 폴링·store-and-forward(**에뮬레이터, 실차 아님**) | ELM327-emulator(`socket://127.0.0.1:35000`, 38400) + MQTT 닿지 않는 곳(`127.0.0.1:1`) 34초: `cycles=30 spooled=30 skipped_*=0 connect_failures=5 published=0 spool 미전송=30`. `records.log` 발행 간격 n=29, median 1003 ms, p95 1017 ms, min 974, max 1017. `"gps"` 0건. `measure_polling.py --note ...` 50주기 스모크: median 5.86 ms, p95 7.26 ms, max 24.93 ms, `adapter` = `ELM327 v1.5` / `STN1100 v1.2.3`(에뮬레이터 응답) |

이 숫자들은 리허설 전 점검용이며 증거 폴더(`load-test/dongle-stationary/evidence/`)에 넣지 않았다(원본은 작업용 임시 폴더, 각 1회).

### 남은 수동 단계 (사용자)

1. **hosts 한 줄(관리자 PowerShell)** — 에이전트는 hosts를 건드리지 않았다.
   ```powershell
   Add-Content -Path C:\Windows\System32\drivers\etc\hosts -Value "127.0.0.1 mosquitto"
   Resolve-DnsName mosquitto -Type A    # 127.0.0.1 확인
   ```
2. **스택 기동**(시뮬레이터 없이, 볼륨 유지): 저장소 루트에서
   ```powershell
   docker compose up -d
   docker compose ps
   curl.exe --fail http://localhost:8080/actuator/health
   ```
   `telemetry-kafka-init`이 `Exited (0)`인지 본다(작업 트리 `kafka/init-topics.sh`는 LF로 바꿔 둠).
3. **브리지 환경 올리기**(각 PowerShell 창에서, `obd-bridge` 폴더):
   ```powershell
   cd obd-bridge
   $env:PYTHONUTF8 = "1"
   Get-Content .env | Where-Object { $_ -match '^[A-Z_]+=' } | ForEach-Object { $k,$v = $_ -split '=',2; Set-Item "env:$k" $v }
   ```
4. **리허설 1 — 정상 경로(에뮬레이터, 60초)**. 창 A에서 에뮬레이터, 창 B에서 브리지. 끝나면 **에뮬레이터도 반드시 종료**(끊긴 뒤 `WinError 10053` 로그를 무한 반복한다 — README).
   ```powershell
   # 창 A
   .\.venv\Scripts\python -m elm -n 35000 -s car -b elm-batch-r1.out
   # 창 B — 전/후 Kafka end offset을 함께 남긴다
   docker exec telemetry-kafka kafka-run-class kafka.tools.GetOffsetShell --broker-list localhost:9092 --topic vehicle-telemetry --time -1
   .\.venv\Scripts\python -m obd_bridge --obd-port socket://127.0.0.1:35000 --obd-baudrate 38400 --spool-dir spool-rehearsal-r1 2>&1 | Tee-Object bridge-r1.log
   #   60초 뒤 Ctrl+C
   docker exec telemetry-kafka kafka-run-class kafka.tools.GetOffsetShell --broker-list localhost:9092 --topic vehicle-telemetry --time -1
   ```
   기대: 종료 줄 `connect_failures=0`, `acked` = `published` = `cycles`, `rejected_by_broker=0`, `spool 미전송=0`, end offset 합 증가 = `acked`(재전송 없을 때).
   여기서 연결이 거부되면(호스트명·인증서) **출발하지 않는다**(D1).
   - **결과(2026-10-09, 1회 관찰, 에뮬레이터 수치 — 실차 아님): 통과.** v5, `--mqtt-host mosquitto`(호스트명 검증 켬) mTLS 연결 성공.
     `cycles=60 published=60 acked=60 rejected_by_broker=0 connect_failures=0 skipped_*=0`, 미전송 0. Kafka end offset 합 +60(p0만), KONA17-01 레코드 60·고유 ts 60·중복 0,
     InfluxDB `speed` 60(`fuel_level`·`battery_voltage`도 60 — 에뮬레이터는 012F·0142 응답, `omitted_by_field` 0), DLQ 변화 0, 표본 5/5 일치. 발행 간격 median 1001·p95 1020·max 1049 ms.
     증거 `load-test/dongle-stationary/evidence/20261009T112958Z-rehearsal/`. 첫 시도(`20261009T111728Z-rehearsal`)는 측정 도구 결함으로 **무효**(준비 확인용 TCP 접속이 에뮬레이터를 망가뜨림 — `INVALID.txt`).
5. **리허설 2 — v5 음성 ACL 시험(필수)**: GRDR24H-01 인증서로 `KONA17-01` 토픽에 쓰기 → 브로커가 거부해야 한다. 에뮬레이터를 새로 띄운 뒤:
   ```powershell
   .\.venv\Scripts\python -m obd_bridge --obd-port socket://127.0.0.1:35000 --obd-baudrate 38400 `
     --vehicle-id KONA17-01 --mqtt-protocol 5 `
     --tls-client-cert ..\broker\certs\vehicles\GRDR24H-01.crt --tls-client-key ..\broker\certs\vehicles\GRDR24H-01.key `
     --spool-dir spool-rehearsal-neg 2>&1 | Tee-Object bridge-neg.log
   #   20초 뒤 Ctrl+C
   ```
   **판정: `rejected_by_broker > 0`, `acked = 0`, `spool 미전송` = `cycles`, Kafka `vehicle-telemetry` end offset 변화 0.**
   `rejected_by_broker = 0`이고 `acked > 0`이면 v5에서도 조용한 유실이 가능하다는 뜻 — **출발하지 않고** 결과로 기록한다.
   이 회차의 `spool-rehearsal-neg`는 정상 회차에 재사용하지 않는다(다시 띄우면 재전송을 시도한다).
   - **결과(2026-10-09, 1회 관찰, 에뮬레이터 수치): 통과.** `cycles=18 published=18 acked=0 rejected_by_broker=18`(PUBACK reason **0x87 Not authorized** ×18),
     미전송 18(= cycles, 증거 폴더 `spool-neg/`에 보존), Kafka end offset 변화 0, InfluxDB 0행, DLQ 변화 0.
     **예상과 다른 점**: mosquitto 로그에 ACL 거부 줄이 없다 — `log_type`이 error·warning·notice뿐이라 거부(디버그 수준)가 안 찍힌다. 접속 줄의 `u'GRDR24H-01'`만 보인다. 브로커 거부의 근거는 PUBACK reason code다.
6. **리허설 3 — 브로커 정지 시험(5단계 리허설, 선택)**: 새 `--spool-dir`로 정상 1분 → `docker stop telemetry-mosquitto` 1분 → `docker start telemetry-mosquitto` 후 2분.
   기대: 정지 중 `records.log`−`acks.log` 차이가 약 1건/초로 증가, 재연결 후 0, `reconnects ≥ 1`.
7. 리허설이 끝나면 `docker compose stop`(볼륨 유지 — `down -v` 금지). 실차 당일에는 2단계 "사전"부터.

### 스택 상태

준비 작업 뒤 **`docker compose stop`으로 정지**(볼륨·등록 차량·Grafana 역할 유지). 리허설은 hosts 추가가 먼저라 위 2번에서 다시 띄운다.
2026-10-09 리허설 1·2(hosts 추가 뒤, 시뮬레이터 없이) 후 다시 `docker compose stop`. 리허설 3(브로커 정지)은 미실행.

## 환경 기록 (`00_metadata.txt`, 회차마다)

- 소스: `git rev-parse HEAD`, `git describe --tags`, `git status --porcelain`(dirty면 파일별 SHA-256), 실행 중 컨테이너 image ID
- 브리지 설정: `VEHICLE_ID`, `OBD_PORT`(COM 번호), baud, `MQTT_HOST`/`MQTT_PORT`/`MQTT_PROTOCOL`, `POLL_INTERVAL`(1.0 고정), `SPOOL_DIR`(회차마다 새 폴더),
  사용 인증서 CN·일련번호(`openssl x509 -noout -subject -serial`) — 키 내용은 기록하지 않는다
- 노트북: 모델, `platform.platform()`, Python, `pip freeze`(obd·paho·pyserial 버전), 전원 연결 여부, **절전·화면 끄기 해제** 여부, 시계 동기화 상태(`w32tm /query /status`)
- 동글: OBDLink EX 펌웨어(`ATI`·`STI` 응답), USB 드라이버 버전, OBDwiz 버전, 프로토콜(예: ISO 15765-4 CAN 11/500)
- 차량: 차종·연식·연료, **주행거리(odometer)**, 시동 상태(공회전/READY/키 ON 엔진 OFF), 냉간·온간 시작, 공조 ON/OFF, 12V 전압(OBDwiz)
- 주변: 외기 온도, 장소 유형(야외/환기 주차장 — 주소 아님), 핫스팟 통신사·신호 막대, 시작·종료 UTC

## 1단계 — 지원 PID 비트맵 (OBDwiz)

- 실행: OBDwiz 연결 → Mode 01 PID `00`·`20`·`40` 응답 원문(hex 4바이트씩) 저장. 비트맵에서 `0C`·`0D`·`05`·`11`·`2F`·`42` 비트를 손으로 풀어 표로 적는다.
- **OBDwiz를 완전히 종료한 뒤** 다음 단계로 간다(COM 포트 점유).
- 판정(사전 등록):
  - 필수 넷(`0D`·`0C`·`05`·`11`) 중 하나라도 미지원 → **그 차량 2~5단계 중단.** 브리지는 아무것도 보내지 않으므로 수집이 성립하지 않는다(실패가 아니라 결과로 기록 — ADR-030 범위 재검토 근거).
  - `2F`·`42`는 지원·미지원 모두 결과다. 미지원이면 2단계에서 `omitted_by_field`가 사이클 수와 같아야 한다.
  - 2단계 브리지 기동 로그의 미지원 경고(python-OBD `supports()`)와 OBDwiz 비트맵이 다르면 **불일치 자체를 결과로 기록**(무효 아님).
- 증거: 비트맵 hex 원문 텍스트, 마스킹한 스크린샷, 해석 표.

## 2단계 — 정차 수집 10분

- 사전: 스택 기동(시뮬레이터 없이), `hosts`·포트 확인, **수집 전 스냅숏**(아래 4단계 "전" 명령), 엔진 공회전 5분 이상(코나) / READY(그랜저).
- 실행: `python -m obd_bridge --vehicle-id <ID> --obd-port COMn --mqtt-host mosquitto --mqtt-port <포트> --tls-... --spool-dir spool-<run> --interval 1.0`,
  콘솔 출력을 `bridge.log`로 tee. 600초 뒤 Ctrl+C(SIGINT) — 종료 줄의 `stats=`·`publisher=`·`spool 미전송=`이 집계 원본이다.
- 대조군: 같은 날 같은 노트북에서 리허설한 ELM327-emulator 실행(같은 명령, `socket://`). 차이는 장비뿐이어야 한다.
- 판정(사전 등록):
  - 성공: `cycles` ≥ 590, `skipped_missing` = 0, `skipped_out_of_range` = 0, 종료 시 `spool 미전송` = 0, `connect_failures`·`ack_timeouts` = 0, MQTT DLQ·Kafka DLQ 증가 0.
  - 실패: `skipped_missing` > 0(필수 PID가 지원 비트맵과 달리 null), DLQ 증가 > 0(계약 거부 — 사유를 DLQ 헤더로 확인), 종료 시 미전송 > 0.
  - **무효**: 노트북 절전·화면 잠금으로 루프 정지, 시계 점프(`ts_ms`가 뒤로 감), 시동 상태 변경, 핫스팟 끊김(5단계 전에), 시뮬레이터가 같이 돈 경우.
- 차량이 **실제로 0을 보고한 값**(정차 속도 0)은 0으로 간다 — 정상이다(지어낸 0이 아님).

## 3단계 — 실제 폴링 간격

두 값을 따로 잰다. 섞지 않는다.

| 무엇 | 출처 | 통계 |
| --- | --- | --- |
| **발행 간격** — 브리지가 실제로 낸 주기 | 2단계 `spool-<run>/records.log`의 연속 `ts_ms` 차이(주기 시작 시각) | n, median, p95, max (+`skipped_overrun`) |
| **한 주기 읽기 시간** — PID 6개 연속 query | 2단계 직후 같은 상태에서 `tools/measure_polling.py --port COMn --baudrate <1단계 값> -n 300 --note "실차 — ..." --out ...` | n, median, p95, max, PID별 median, `unsupported`, `incomplete_cycles` |

- 비교 기준(에뮬레이터, README): `fast=off` 3회 median 8.3~9.4 ms, p95 12.6~16.4 ms, max 21~35 ms. **실차 값으로 이 범위를 예측하지 않는다** — 에뮬레이터에는 UART·CAN 응답 지연이 없다.
- 판정(사전 등록):
  - "1초 주기 유지" 성공: `skipped_overrun` = 0 **그리고** 발행 간격 p95 ≤ 1,100 ms, max ≤ 2,000 ms.
  - 읽기 시간 p95 > 1,000 ms면 `POLL_INTERVAL=1.0`은 이 장비에서 유지 불가 — 주기 재결정 근거로 기록(코드 변경 없음).
  - 무효: `measure_polling.py`가 연결 실패(baud 불일치 등) — 사유를 적고 발행 간격만 보고한다.
- `records.log`는 ack 1000건에서 compaction되므로(README 전송·spool 5) **회차당 1000건 미만**(10분 = 600)이고 회차마다 새 `SPOOL_DIR`을 쓴다. 종료 직후 폴더째 복사.

## 4단계 — 발행 · PUBACK · Kafka · InfluxDB 대조

기준량은 **PUBACK**(실험 규칙 2)이고, 비교 단위는 **고유 timestamp**다(at-least-once라 재전송 중복이 있을 수 있다).

| 열 | 값 | 명령(패턴은 `docs/verification/evidence/2026-10-05-*/offsets.sh`·`2026-10-06-pause-alert-timing/kafka_unique.sh`·`influx_count.sh`) |
| --- | --- | --- |
| A spool 기록 | `records.log` 줄 수, 고유 `ts` 수 | `wc -l`, `grep -o '"ts": "[^"]*"' records.log \| sort -u \| wc -l` |
| B 발행 | 종료 로그 `publisher.stats.published`(재전송 포함) | `bridge.log` 종료 줄 |
| C PUBACK | `acks.log` 줄 수 = `publisher.stats.acked` | `wc -l acks.log` — 둘이 다르면 그것부터 결과 |
| D Kafka end offset 차이 | 수집 전·후 `vehicle-telemetry` 파티션별 end offset 합의 차이 | `docker exec telemetry-kafka kafka-run-class kafka.tools.GetOffsetShell --broker-list localhost:9092 --topic vehicle-telemetry --time -1` (전·후 각각, `vehicle-telemetry-mqtt-dlq`·`vehicle-telemetry-dlq`도) |
| E Kafka 레코드 | 해당 파티션의 "전" offset부터 이 `vehicle_id`만: records / unique_ts / dup_ts | `kafka_unique.sh <tag> <VID> <파티션> <전 offset>` 패턴(증거 경로만 이 실행 폴더로) |
| F InfluxDB | `vehicle_telemetry`, `vehicle_id`=ID, `_field=="speed"`의 `count()` (= 고유 `_time` 수, 같은 timestamp는 덮어쓴다) | `influx_count.sh <VID>` 패턴. `range(start:)`는 수집 시작 1분 전으로 좁힌다 |
| G 선택 필드 | 같은 쿼리를 `fuel_level`·`battery_voltage`로 — 미지원이면 0행이어야 한다(0값 행이 아님) | 위와 같음 |

- 파티션은 `vehicle_id` 키로 하나다 — E는 그 파티션만 읽는다. 시뮬레이터를 끈 상태라 D = E.records가 기대값.
- 판정(사전 등록):
  - 성공: A.unique = C(중복 PUBACK 없을 때) = E.unique_ts = F, DLQ 차이 0, D = E.records. `E.dup_ts`는 0이 아니어도 **실패가 아니라 집계**한다(2단계는 장애가 없으므로 0을 기대 — 0이 아니면 원인 기록).
  - 실패: C > E.unique_ts(PUBACK 받았는데 Kafka에 없음 — 3.1.1 ACL 조용한 유실 후보), E.unique_ts > F(저장 유실), A.unique > C(종료 시 미전송).
  - 무효: D ≠ E.records인데 다른 생산자가 있었음이 확인된 경우.
- 표본 대조(실험 규칙 3): 무작위 5개 timestamp를 `records.log` 원문 → Kafka 레코드 → Influx 값까지 손으로 맞춰 `derived/sample_trace.txt`에 남긴다.

## 5단계 — 브로커 정지 시험 · store-and-forward

새 `SPOOL_DIR`로 별도 회차: **정상 2분 → 핫스팟 OFF 2분 → ON 후 4분**(총 약 480건, compaction 전).
스택이 같은 노트북이면 핫스팟을 꺼도 브로커가 닿는다 — **그 경우 이 단계가 성립하지 않는다.** 5단계는 브로커가 다른 장치(D1·D2 확정 후)이거나,
같은 노트북이면 `docker stop telemetry-mosquitto` 2분으로 대체하고 **"핫스팟 시험 아님, 브로커 정지"**로 이름을 바꿔 기록한다.
**이 계획은 스택을 같은 노트북에 띄우므로(아래 "노트북 준비 상태") 5단계는 브로커 정지 시험이다** — 핫스팟 끊김 시험이 아니다.

- 관찰: 5초마다 `records.log`·`acks.log` 줄 수를 찍어 spool 깊이(= 차이)를 시계열로 저장(PowerShell 루프, 시각은 UTC).
- 판정(사전 등록):
  - 성공: 끊김 동안 spool 깊이가 약 1건/초로 증가(2분 → 110~130), 재연결 후 깊이가 0으로 돌아옴, 끊긴 구간의 모든 `ts`가 E·F에 존재(**유실 0**),
    재전송분이 Kafka 파티션 안에서 `ts` 오름차순으로 도착(E의 레코드 순서에서 끊긴 구간 `ts`의 역전 0건), `reconnects` ≥ 1.
  - 중복은 실패가 아니다 — `E.dup_ts`와 `B − C`를 **세어서 적는다**. Influx는 같은 timestamp를 덮어쓰므로 F에는 안 보인다.
  - 실패: 끊긴 구간 `ts` 중 하나라도 E 또는 F에 없음, 재연결 후 3분 안에 깊이 0이 안 됨, 끊김 동안 `skipped_overrun` 증가(spool I/O가 폴링을 굶김 — 결정 대기 8).
  - 무효: 끊김 중 시동 상태 변경, 노트북이 핫스팟 대신 다른 망에 붙음(Wi-Fi 자동 전환 — 미리 다른 저장 네트워크 자동 연결 해제).
- 1회 결과이므로 "store-and-forward 무손실"이라고 쓰지 않는다 — "이 조건 1회 유실 0"까지.

## 가설 H — 하이브리드 EV 모드의 `speed > 0 · rpm = 0` 오탐 (측정 항목)

### 지금 코드가 하는 일

| 경로 | 무엇이 반응할 수 있나 | 근거 |
| --- | --- | --- |
| 룰 | **speed·rpm을 조합하는 룰이 없다.** `rpm > 6000`·`speed > 200`만 있어 `rpm = 0`이나 `speed > 0 & rpm = 0`으로 발화하는 룰은 없다 → 예측: 룰 알림 0 | `anomaly-detector/rules.py` `_RULES` |
| ML | 기본 **꺼짐**(`ML_ENABLED` 기본 false, `docker-compose.yml`). 켜면 파티션별 IsolationForest가 `speed, rpm, engine_temp, battery_voltage, fuel_level, throttle_position` 6개를 본다. 시뮬레이터는 rpm을 **800~4500으로 고정**(`simulator/vehicle_simulator.py:120`)하므로 시뮬레이터로 학습한 모델에서 rpm 0은 분포 밖 → 이상 점수 후보 | `ml_detector.py` `FEATURES`, `anomaly_detector.py` `PartitionedMLDetectors` |
| ML 학습 오염 | 반대로 실차 혼자 쓰는 파티션이면 그 차의 정차 200건으로 학습(`ML_MIN_SAMPLES` 200)해 rpm 0이 "정상"이 된다 — 결과가 학습 혼합에 달렸다 | 같음 |
| ML 건너뜀 | `fuel_level`·`battery_voltage`가 미지원이면 ML은 그 레코드를 아예 채점하지 않는다(ADR-030) — 그 경우 H의 ML 칸은 관측 불가 | `missing_features()` |
| 수집 자체 | 엔진이 꺼진 동안 ECU가 `010C`에 NO DATA로 답하면 rpm이 null → **필수 결측으로 그 주기 미전송**. 오탐이 아니라 **데이터 공백**이 될 수 있다 | README "미지원·null PID" |

### 이 계획에서 관측 가능한 것 / 불가능한 것

| 항목 | 정차에서 | 방법 |
| --- | :---: | --- |
| `speed > 0 & rpm = 0` 표본 수 | **관측 불가**(정차라 speed 0) — 0건이 나와도 가설에 대해 아무것도 말하지 않는다 | `records.log` payload 집계(그래도 센다) |
| `speed = 0 & rpm = 0` 표본 수(그랜저 READY·엔진 정지) | 관측 가능 | 같음 |
| 엔진 정지 중 `010C` 응답이 0인지 NO DATA인지 | 관측 가능 | `missing_by_field["rpm"]`, `skipped_missing`, OBDwiz 실시간 값 |
| rpm 0 구간의 룰 알림 수 | 관측 가능(예측 0) | `vehicle-anomaly-alerts` 토픽 이 vehicle 레코드 수, `SELECT detector, anomaly_type, count(*) FROM anomaly_alerts WHERE vehicle_id='<ID>' AND detected_at >= '<시작>' GROUP BY 1,2;` |
| ML 알림·점수 | **이 계획에서 미관측** — 실행은 기본 설정(ML off)을 유지한다. 저장한 payload의 오프라인 채점은 D5 도구가 생긴 뒤 | — |
| 주행 중 EV 모드 | **범위 밖** | 별도 주행 계획(정차 성공 후) |

판정: H는 이 계획으로 **참·거짓을 내지 않는다.** 남기는 것은 rpm 0 표본 수, rpm null 주기 수, 룰 알림 수(예측 0과 비교)뿐이다.
코나 2017 가솔린은 공회전 중 rpm > 0이 기대값이라 H의 대조 차량 역할만 한다.

## 증거 (`load-test/dongle-stationary/evidence/<UTC>-<차량>-<단계>/`)

- `00_metadata.txt`(환경 기록 전부), `bitmap_0100_0120_0140.txt`, `bridge.log`, `spool-<run>/`(records·acks 원본 — **비공개**, 공개본은 마스킹 사본),
  `measure_polling_<run>.json`, `offsets_before.txt`·`offsets_after.txt`, `kafka_records.txt`, `influx_count_<field>.txt`,
  `dlq_offsets.txt`, `spool_depth.tsv`(5단계), `alerts_query.txt`, 마스킹 스크린샷.
- 파생값은 `derived/`(간격 통계·대조표·`sample_trace.txt`) — 원본과 섞지 않는다. 종료 시 `evidence_finish` 체크섬, 실패·무효 회차도 지우지 않는다.
- 결과는 `load-test/dongle-stationary/RESULT_<날짜>_<차량>.md`에: 명령, 위 판정 표의 실제 값, 무효 사유, **"단일 차량·각 1회 = 부분 검증"** 한계.

## 이 계획이 끝나도 말하지 않는 것

- 실차 폴링 주기의 일반값, OBD-II 연결 완료, store-and-forward 무손실, 하이브리드 오탐 여부, 주행 중 동작.
