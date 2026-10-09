# obd-bridge — OBD-II → MQTT 브리지 (프로토타입)

> 작성: 2026-10-06 · 상태: **프로토타입. 실차·실동글 미검증.** Compose·CI에 넣지 않았다.

## 목적

ELM327 계열 OBD-II 어댑터에서 PID 6개를 읽어 **기존 입력 계약 그대로** MQTT(QoS 1)로
`vehicle/telemetry/{vehicleId}`에 발행한다. 연결이 끊긴 동안이나 PUBACK을 받기 전의 메시지는
디스크 spool에 fsync로 남기고, 재연결 시 timestamp 순서로 다시 보낸 뒤 **PUBACK을 받은 것만** 지운다.

이 폴더가 증명하려는 것은 "OBD 연결 완료"가 아니다. **계약 밖의 값을 지어내지 않는 것**과
**PUBACK 전에는 지우지 않는 것**을 테스트로 고정하는 것이다.

## 계약 매핑

계약의 단일 기준은 [`docs/telemetry-schema-decision-table.md`](../docs/telemetry-schema-decision-table.md)이고,
테스트는 [`anomaly-detector/contract.py`](../anomaly-detector/contract.py)를 **경로로 직접 불러** 검증한다(사본 없음).

| 계약 필드 | PID | python-OBD 명령 | 변환 단위 | 계약 범위 | 필수 | OBD에서 |
| --- | --- | --- | --- | --- | :---: | --- |
| `speed` | 010D | `SPEED` | kph | 0 ~ 255 | O | 1바이트 |
| `rpm` | 010C | `RPM` | rpm | 0 ~ 16383.75 | O | (256A+B)/4 — **0.25 단위 그대로**, 반올림 안 함 |
| `engine_temp` | 0105 | `COOLANT_TEMP` | degC | -40 ~ 215 | O | A−40, 냉각수 온도 |
| `throttle_position` | 0111 | `THROTTLE_POS` | percent | 0 ~ 100 | O | 100A/255 |
| `fuel_level` | 012F | `FUEL_LEVEL` | percent | 0 ~ 100 | **X**(ADR-030) | 100A/255. 미지원·null이면 **키 생략** |
| `battery_voltage` | 0142 | `CONTROL_MODULE_VOLTAGE` | volt | 0 ~ 65.535 | **X**(ADR-030) | 미지원·null이면 **키 생략**. (256A+B)/1000 — **제어 모듈 전압**. 동글 자체 측정(`ELM_VOLTAGE`)이 아니다(결정표 3절) |
| `vehicle_id` | — | 설정값 | — | `^[A-Z0-9-]{4,20}$` | O | 시작 시 검사, 위반이면 기동 거부 |
| `timestamp` | — | 주기 시작 시각 | — | ISO-8601 UTC 밀리초 `Z` (시뮬레이터와 같은 형식) | O | 브리지 호스트 시계 |
| `gps` | — | — | — | 선택 | X | **보내지 않는다(키 생략)** |
| `dtc_codes` | — | — | — | 선택 | X | **보내지 않는다(키 생략)** |

- 단위는 Pint 수량을 **명시 단위로 변환해서** 꺼낸다. 라이브러리가 mph·°F를 돌려줘도 km/h·°C가 된다(테스트 있음).
- topic은 `{MQTT_TOPIC_PREFIX}/{vehicle_id}`. 백엔드는 topic 끝과 payload `vehicle_id`가 다르면
  `TOPIC_VEHICLE_MISMATCH`로 거부하고, 운영 broker ACL은 `vehicle/telemetry/%u`(인증서 CN)에만 쓰기를 허용한다.
  즉 **`VEHICLE_ID`는 클라이언트 인증서 CN과 같아야 한다.**
- python-OBD가 표현할 수 있는 2바이트 전 범위(0..65535)를 decoder에 넣어 만든 payload **65,536개 전부**를
  `contract.validate`에 통과시킨다(표본이 아니라 전수, 약 0.7초). 1바이트 PID는 상위 바이트로 함께 돈다.

### 미지원·null PID — 0으로 채우지 않는다

2026-10-06부터(ADR-030) 필수와 선택이 갈린다.

| 구분 | PID | 미지원(`supports()` 거짓)이거나 응답이 null이면 |
| --- | --- | --- |
| **필수** | 010D·010C·0105·0111 (속도·RPM·냉각수·스로틀) | **그 주기는 payload를 만들지 않고 보내지 않는다.** `skipped_missing`·`missing_by_field`를 올리고 경고 로그 |
| **선택** | 012F·0142 (연료량·제어 모듈 전압) | **그 키를 생략하고 보낸다** — 0도 `null`도 넣지 않는다. `omitted_by_field`를 올린다(매 주기 로그는 남기지 않고, 미지원은 기동 시 한 번 경고) |

선택 PID라도 **값이 오면** 범위 검사는 그대로다(범위 밖이면 그 주기 미전송). 필수 하나와 선택이 함께 없으면 필수 규칙이 이긴다(미전송).
**배포 순서**: 키를 생략한 payload는 구 계약이 거부하므로 감지기 → 백엔드 → **브리지 마지막**
(`tests/test_unsupported_null.py`가 구 계약 동결본으로 그 거부를 고정한다). 0.0으로 채우면 "시속 0으로 주행 중"처럼
그럴듯한 값이 되어 눈에 띄지 않는다 — 결정표 2절 1·2·10·14번이 바로 그 사례다.
반대로 차량이 **실제로 0을 보고하면 0을 보낸다**(금지하는 것은 0이 아니라 지어내는 것).

계약 범위 밖·NaN·Infinity 값도 **잘라 넣지 않고** 보내지 않는다(`skipped_out_of_range`).

## 전송·spool

| 단계 | 동작 |
| --- | --- |
| 1 | 주기마다 payload를 만들면 **먼저** `records.log`에 추가: write → flush → `os.fsync`. 새 파일이면 디렉터리도 fsync |
| 2 | spool의 미전송분을 `(timestamp, seq)` 순서로 `publish(qos=1)`. inflight 창 기본 20 |
| 3 | `on_publish`(= PUBACK)가 온 것만 `acks.log`에 seq 추가 + fsync → 메모리에서 제거 |
| 4 | 끊김·CONNACK 실패·**TCP/TLS 연결 실패(`on_connect_fail`)**·`publish` rc 오류·PUBACK 30초 무응답 → 그 paho 클라이언트를 **버리고** 이 브리지의 백오프(1→30초) 뒤 새 클라이언트로 2부터 |
| 5 | ack가 1000건 쌓이면 compaction: 미전송분을 임시 파일에 쓰고 fsync → `os.replace` → 디렉터리 fsync → 그 뒤 `acks.log` 비움 |

- 마지막 줄이 잘린 경우(쓰는 중 크래시)는 그 줄만 잘라낸다. **중간 줄 손상은 조용히 버리지 않고 기동을 멈춘다.**
- paho의 자체 재전송에 맡기지 않는 이유: paho는 미확인 QoS 1을 자기 큐에 들고 있다가 재연결 때 다시 보내므로,
  spool 재전송과 겹치면 순서·중복을 통제할 수 없다. 옛 클라이언트의 늦은 PUBACK은 세대 번호로 무시한다.
  폐기할 때도 세대를 올려, 같은 클라이언트의 두 번째 끊김 알림이 폐기·백오프를 두 번 일으키지 않는다.
- **paho 자체 재접속은 끈다**(`reconnect_on_failure=False`). 첫 연결 재시도는 그 설정과 무관하게 paho가
  1→120초 백오프로 조용히 반복하므로, `on_connect_fail`을 받아 곧바로 폐기한다(`connect_failures` 카운터).
  만료 인증서 같은 TLS 실패도 이 경로로 보이고, paho 로그는 `obd_bridge.paho` logger로 나온다.
- **폐기 순서: `disconnect()` 먼저, `loop_stop()`(timeout 없는 join)은 폴링 스레드 밖(reaper 스레드)에서.**
  paho 2.1.0의 `loop_forever`는 종료 요청을 받아도 미확인 QoS 1(`_out_messages`)이 빌 때까지 돈다. 그래서
  PUBACK은 안 오고 TCP는 살아 있으면 `loop_stop()`을 먼저 부른 폴링 스레드가 **무기한** 멈췄다(수정 전,
  실제 paho + stub 브로커 테스트로 재현 — 15초 안에 돌아오지 않음). `disconnect()`가 DISCONNECT를 쓰면
  paho가 소켓을 닫고 루프가 끝난다. 송신 버퍼가 막혀 못 쓰는 경우엔 keepalive(30초) 경과 시 paho가 닫는다
  — 그래서 join을 폴링 스레드에서 하지 않는다. 종료(`close`)는 DISCONNECT에 최대 2초를 준다.
- 폴링이 밀려 건너뛴 예정 주기는 `skipped_overrun`으로 세고 경고 로그를 남긴다(따라잡기 연사는 안 한다).
- **at-least-once다.** PUBACK이 유실되면 같은 메시지가 다시 간다. InfluxDB는 같은 `vehicle_id`+timestamp를 덮어쓰고,
  감지기 알림은 `event_id`로 중복을 막는다(기존 설계). exactly-once가 아니다.

## 실행

Python **3.11 이상**. 3.11.9(데스크톱, 2026-10-06)와 3.12.10(노트북, 2026-10-09)에서 같은 `requirements.txt`로 설치·테스트했다.

```powershell
cd obd-bridge
py -3.11 -m venv .venv            # 3.11이 없으면 아래 3.12 판
$env:PYTHONUTF8 = "1"   # Windows cp949에서 ELM327-emulator sdist 빌드가 UnicodeDecodeError로 실패한다
.\.venv\Scripts\python -m pip install -r requirements.txt
.\.venv\Scripts\python -m pytest -q
.\.venv\Scripts\python -m obd_bridge --help
```

3.12(노트북 LAPTOP-MV7C23S3, 2026-10-09에 실제로 쓴 명령 — Git Bash):

```bash
cd obd-bridge
python -m venv .venv              # python = 3.12.10
PYTHONUTF8=1 ./.venv/Scripts/python -m pip install -r requirements.txt
PYTHONUTF8=1 ./.venv/Scripts/python -m pytest -q
```

- `.venv/`는 루트 `.gitignore`(`obd-bridge/.venv/`)로 추적하지 않는다.
- 노트북에서 첫 pytest가 tmp_path를 쓰는 테스트 42건에서 `PermissionError: [WinError 5] ... pytest-of-<사용자>`로 **setup 오류**가 났다.
  Python 3.12 문제가 아니라 예전(9/27)에 만들어진 `%TEMP%\pytest-of-<사용자>` 폴더 권한 문제다 — `--basetemp=<쓰기 가능한 폴더>`를 주면 전부 통과했다.

설정(CLI 인자 또는 환경변수 — **이름만** 적는다):

| 환경변수 | CLI | 기본 |
| --- | --- | --- |
| `VEHICLE_ID` | `--vehicle-id` | (필수) |
| `OBD_PORT` | `--obd-port` | (필수) `COM5`, `/dev/ttyUSB0`, `socket://127.0.0.1:35000` |
| `OBD_BAUDRATE` | `--obd-baudrate` | 0(자동). **`socket://`은 자동 탐지가 안 된다** — 38400 지정 |
| `MQTT_HOST` / `MQTT_PORT` | `--mqtt-host` / `--mqtt-port` | `localhost` / **8883** |
| `MQTT_PROTOCOL` | `--mqtt-protocol` | `3.1.1` (`5` 선택 가능 — 아래 결정 대기 3) |
| `MQTT_TOPIC_PREFIX` | `--topic-prefix` | `vehicle/telemetry` |
| `TLS_CA_CERT` / `TLS_CLIENT_CERT` / `TLS_CLIENT_KEY` | `--tls-*` | 없음. **없으면 기동 거부** |
| `MQTT_INSECURE_PLAINTEXT=1` | `--insecure-plaintext` | 꺼짐. dev override 전용 |
| `SPOOL_DIR` | `--spool-dir` | `./spool` (gitignore 됨) |
| `POLL_INTERVAL` | `--interval` | 1.0초 |

TLS 변수 이름은 시뮬레이터와 같다. 기본이 mTLS 8883이고 평문은 명시적으로 켜야 한다(저장소 규칙과 같다).

**브리지를 스택과 다른 장치(노트북·라즈베리파이 등)에서 돌릴 때**: compose의 8883은 기본이
`127.0.0.1` 게시라 다른 장치에서 닿지 않는다(2026-10-08). 스택을 띄우는 PC의 `.env`에
`MQTT_TLS_BIND=0.0.0.0`(또는 그 PC의 특정 NIC IP)을 넣고 `docker compose up -d mosquitto`로
다시 만든다. 브리지 장치에는 `ca.crt`와 **그 차량의** `vehicles/<VEHICLE_ID>.crt/.key`만 복사한다
(`ca.key`·다른 차량 키는 복사하지 않는다). `--mqtt-host`는 스택 PC의 IP, 브로커 인증서 CN은
`mosquitto`라 호스트명 검증에 걸리면 별도 결정이 필요하다(미검증 — 아래 "미검증").
스택 PC에서 `MQTT_TLS_PORT`를 바꿨다면(Windows 제외 포트 충돌 등, `docs/deployment-guide.md` §8) 브리지의 `--mqtt-port`/`MQTT_PORT`도 **같은 값으로** 맞춘다.
공유망(학교·카페)에서는 열지 않는다.

## 테스트 (하드웨어 없음)

`.\.venv\Scripts\python -m pytest -q` → **82 passed, skip 0** (2026-10-06, Windows 11, Python 3.11.9).
2026-10-09 노트북(Windows 11, Python 3.12.10): `5561d98` 그대로 **90 passed, skip 0**(82 이후 추가된 테스트 포함), D4 수정 후 **94 passed, skip 0**(`test_measure_polling.py` 4건 추가).

| 파일 | 무엇 |
| --- | --- |
| `tests/test_mapping_contract.py` | 매핑 범위 == `contract._NUMERIC`, 선택 PID == `contract.OPTIONAL_NUMERIC`, python-OBD **실제 decoder**에 원시 바이트(2바이트 전 범위)를 넣어 만든 payload 65,536개 전부 `contract.validate` 통과, 0.25 rpm·0.001 V 보존, gps/dtc 키 없음, 단위 변환, vehicle_id·timestamp 형식, 범위 밖 미전송 |
| `tests/test_unsupported_null.py` | 6개 필드 각각 미지원/null → None(0 아님). 필수 넷은 그 주기 spool 0건, 선택 둘(012F·0142)은 키 생략 payload가 새 계약 통과·**구 계약(동결본) 거부**. None 조합 63가지 전부 — 필수가 빠지면 payload 없음, 선택만 빠지면 그 키만 없음(0·null 없음). 실제 0은 0으로 보냄 |
| `tests/test_spool.py` | fsync 순서(파일 → 디렉터리, 내용이 다 쓰인 뒤 fsync), compaction 순서(records 확정 → acks 비움), 재시작 후 보존, timestamp 순서, 잘린 꼬리 복구, 중간 손상 예외, compaction 중 크래시 후 seq 재사용 방지 |
| `tests/test_publisher.py` | PUBACK 전 미삭제(메모리·디스크), 끊김 중 쌓인 것 timestamp 순 재전송, 일부만 ack된 뒤 끊기면 나머지만 새 클라이언트로 재전송, 옛 클라이언트 늦은 PUBACK 무시, backlog 뒤에 새 메시지, rc 오류·PUBACK timeout 재연결, v5 PUBACK 실패 코드는 ack 아님(경계 0x80, 0x10은 ack — 실제 `ReasonCode`), 두 번째 끊김 알림에 이중 폐기 없음, `on_connect_fail` → 폐기·자체 백오프, mTLS 없으면 거부 |
| `tests/test_publisher_real_paho.py` | **실제 paho 2.1.0** + 순수 Python stub 브로커(CONNACK·PINGRESP만, PUBACK 없음): PUBACK timeout 폐기가 5초 안에 끝나고 3건이 spool(메모리·디스크)에 남음, paho 스레드 종료·DISCONNECT 수신, 미확인분 있는 `close()`도 5초 안. 닫힌 포트: 연결 실패마다 우리 쪽 새 클라이언트, 옛 paho 스레드 잔존 없음, paho 로그가 `obd_bridge.paho`로 |
| `tests/test_bridge_overrun.py` | pump가 0.55초 멈추면(주기 0.1초) 건너뛴 예정 주기를 `skipped_overrun`으로 셈 |
| `tests/test_measure_polling.py` | `tools/measure_polling.py`의 `--note`가 필수이고 결과 `note`에 그대로 들어감(상수 없음), 어댑터 정보(`ATI`·`STI`·포트·프로토콜) 수집은 `force=True`로 보내고, 하나가 실패해도 나머지를 남기며 무응답은 None |

**테스트가 실제로 막는지 변이로 확인했다**(각 1회, 되돌림, **출력 원본 미보존** — 70건 시점의 테스트 세트 기준):
reader가 None 대신 0.0을 넣게 바꾸면 18건 실패, `publish()` 직후 spool에서 지우게 바꾸면 6건 실패.
폐기 순서 테스트는 수정 전 순서(`loop_stop` → `disconnect`)를 끼워 넣어 실패(15초 안에 pump 미반환)하는 것을 1회 확인했다(원본 미보존). `quantity_to_float`만 0을 돌려주게 바꾼 변이는
**실패 0** — reader가 그 앞에서 null을 걸러내서 도달하지 않는 경로였다(방어가 두 겹이라 한 겹만 바꾼 변이는 안 잡힌다).

## 폴링 주기 측정 — **실차 아님 — ELM327-emulator**

**재는 것**: PID 6개를 `obd.OBD.query()`로 쉬지 않고 연속으로 읽는 한 주기 시간(= 이 구성에서 가능한 최소 폴링 간격).
브리지의 실제 발행 간격은 `POLL_INTERVAL`(기본 1초)이 정한다.

| 항목 | 값 |
| --- | --- |
| 날짜 | 2026-10-06 (UTC 03:13~03:14) |
| OS | Windows 11 Pro 10.0.26200 (`platform.platform()` = `Windows-10-10.0.26200-SP0`) |
| Python | 3.11.9 |
| 패키지 | obd 0.7.3, ELM327-emulator 4.0.0, pyserial 3.5 |
| 연결 | 에뮬레이터 `python -m elm -n 35000 -s car -b <파일>`(TCP, 127.0.0.1) ↔ python-OBD `socket://127.0.0.1:35000`, baudrate 38400 |
| 프로토콜 | ISO 15765-4 (CAN 11/500) — 에뮬레이터가 보고한 값 |
| 측정 도구 | `tools/measure_polling.py`, 회당 300주기, 회차마다 에뮬레이터 새로 기동 (당시 `note`는 도구 상수였다 — 아래) |
| 원본 | `measurements/20261006-emu-*.json` (주기별·PID별 원시 ms 포함) |

| 회차 | python-OBD `fast` | n | median | p95 | max | 미완성 주기 |
| --- | :---: | ---: | ---: | ---: | ---: | ---: |
| 1 | off | — | — | — | — | **무효**: 측정은 끝났으나 결과 출력에서 cp949 `UnicodeEncodeError`로 죽어 원시값 유실. 도구를 "파일 먼저 쓰고 출력"으로 고침 |
| 2 | off | 300 | 8.30 ms | 12.63 ms | 35.36 ms | 0 |
| 4 | off | 300 | 9.36 ms | 16.40 ms | 25.31 ms | 0 |
| 5 | off | 300 | 9.20 ms | 14.59 ms | 20.98 ms | 0 |
| 3 | **on** | 300 | 13.60 ms | 19.49 ms | 29.60 ms | 0 |

- `fast=off` 3회(같은 조건): median 8.3~9.4 ms, p95 12.6~16.4 ms, max 21~35 ms. PID 하나는 median 약 1.3 ms.
- `fast=on`은 **1회뿐**이라 비교 주장을 하지 않는다. 이 1회에서는 더 느렸다. 브리지는 `fast=False`로 연결한다.
- 연결 초기화(`obd.OBD()` 생성)는 회마다 약 4.0초.
- 회차 2의 원본 JSON은 `protocol`이 빈 문자열이다 — 연결을 닫은 뒤 읽는 도구 버그였고 이후 회차에서 고쳤다.
- 300주기 전부 6개 PID가 응답했고, 값은 전부 계약 범위 안이었다(`incomplete_cycles` 0 = `build_payload` 성공).

**`--note` 필수(2026-10-09, 동글 계획 D4 해결)**: 예전에는 결과 JSON의 `note`가 "실차 아님 — ELM327-emulator"로 **도구에 박혀** 실차 결과에도 찍혔다.
이제 `--note "<무엇을 쟀나>"`가 없으면 도구가 시작하지 않고, 결과 JSON에 `adapter`(python-OBD의 `port_name`·`protocol_id`·`protocol_name`·`status`,
`ATI`(ELM 버전)·`STI`(OBDLink/STN 펌웨어) 응답 — 실패하면 `"error: <예외>"`, 무응답은 null)가 같이 남는다. 측정 루프 **전에** 읽어 주기 시간에 섞이지 않는다.
위 2026-10-06 원본 JSON들은 그 이전 형식이다(`adapter` 없음).

```
python tools/measure_polling.py --port COM5 --baudrate <OBDwiz 값> -n 300 --note "실차 — 코나 2017 가솔린, OBDLink EX, 공회전" --out measure_polling_<run>.json
```

**이 숫자가 말하지 않는 것**: 에뮬레이터는 응답을 즉시 돌려준다. 즉 이 값은 python-OBD + pyserial 소켓 +
에뮬레이터의 소프트웨어 오버헤드다. **실제 ELM327의 UART/블루투스 지연과 차량 CAN 응답 시간은 들어 있지 않다.**
실차 폴링 주기는 **미측정**이며 이 숫자로 추정하지 않는다.

### 에뮬레이터를 Windows에서 돌리며 겪은 것

- `pip install ELM327-emulator`가 cp949 로캘에서 `UnicodeDecodeError`로 실패 → `PYTHONUTF8=1`로 해결.
- pty가 없어 **TCP 옵션 `-n`**을 썼다. 대화형 모드 대신 `-b <파일>`(batch)로 백그라운드 실행.
- python-OBD의 baud 자동 탐지는 소켓에서 실패한다(`Failed to choose baud`) → `baudrate=38400` 명시.
- **클라이언트가 끊긴 뒤 에뮬레이터가 `WinError 10053` 오류 로그를 무한 반복**했다(첫 시도에서 종료하기 전까지 출력이 64MB를 넘었다).
  그래서 한 번 연결해서 끝까지 재고, 측정마다 에뮬레이터를 종료·재기동했다.
- 에뮬레이터는 실행 디렉터리에 회전 로그 `elm.log*`(약 1MB씩)를 남긴다 — gitignore에 넣었고 원본 측정값이 아니다.

## 미검증

- **실차·실동글(ELM327 USB/BT) 미검증.** 위 숫자는 전부 에뮬레이터다.
- **실제 broker(mosquitto, mTLS 8883) 연결 미검증.** MQTT는 가짜 클라이언트로만 테스트했다. 백엔드까지 E2E 미실행.
- 정전·강제 종료 시 spool 내구성은 **실제로 전원을 끊어 본 적 없다.** fsync 호출 순서만 테스트했다.
- **Windows는 디렉터리 fsync를 지원하지 않는다**(`fsync_dir`이 아무것도 안 하고 False). 새 파일·rename의 디렉터리 엔트리
  내구성은 NTFS 메타데이터 저널에 기댄다 — 확인하지 않았다. POSIX에서는 디렉터리를 fsync한다(경로는 테스트에서 주입으로만 확인).
- spool 크기 상한 없음. 디스크 가득 참 동작 미검증.
- 장시간 실행(메모리·파일 증가) 미검증.

## Compose / CI

**넣지 않았다.** `docker-compose*.yml`, `.github/` 변경 없음. CI에 pytest를 추가할지는 리뷰 뒤 결정.

## 결정 대기

1. ~~**필수 PID 미지원 차량 — 가장 큰 공백.**~~ — **해결(2026-10-06, ADR-030): (b)를 연료량·전압 두 필드에만 적용했다.**
   `012F`·`0142`는 계약상 선택이 되어, 미지원·null이면 키를 생략하고 보낸다(위 "미지원·null PID"). 저장은 그 필드를
   쓰지 않고(0 아님), 감지기는 전압이 없으면 전압 룰을 평가하지 않으며 ML은 그 레코드를 건너뛴다.
   **속도·RPM·냉각수·스로틀은 여전히 필수**라 그중 하나라도 미지원인 차량에서는 여전히 아무것도 보내지 않는다.
   어느 PID가 실제 차량에서 흔히 미지원인지는 **실차로 확인하지 않았다**(SAE J1979의 PID 지원 비트맵으로 확인할 수 있다 —
   python-OBD는 연결 시 이 비트맵으로 `supports()`를 채운다). 실차에서 다른 PID 미지원이 관찰되면 그때 범위를 다시 정한다.
2. **범위 밖 값** — 지금은 브리지에서 버리고 카운트만 한다. 보내서 DLQ로 격리해 서버 쪽에서 보이게 할지 미정.
3. **MQTT 3.1.1의 ACL 거부** — mosquitto는 3.1.1에서 권한 없는 publish에도 PUBACK을 돌려주고 메시지를 버린다고
   알려져 있다(이 저장소에서 측정 안 함). 그러면 브리지는 받았다고 보고 spool에서 지운다 → **조용한 유실.**
   `VEHICLE_ID`≠인증서 CN이면 발생한다. MQTT 5는 PUBACK에 실패 사유를 실어 브리지가 지우지 않는다(테스트 있음).
   기본 프로토콜을 5로 바꿀지 미정.
4. **브로커가 거부한 메시지(v5)** — 지우지 않고 이번 세션에선 재전송하지 않는다. 재시작하면 다시 시도한다.
   격리 파일로 옮길지 등 처리 방식 미정.
5. **gps** — OBD-II에 없다. GNSS 모듈·휴대폰 등 출처가 정해지기 전까지 키를 생략한다(계약상 허용).
6. **dtc_codes** — Mode 03(`GET_DTC`)으로 읽을 수 있지만 이번 범위가 아니다. 키 생략은 "모름"이고,
   **빈 배열 `[]`은 "고장 코드 없음"이라는 다른 뜻**이라 모를 때 `[]`을 보내면 안 된다.
7. **timestamp 출처** — 주기 시작 시각(호스트 시계)이다. PID 6개는 그 뒤 순차로 읽히므로 실제 측정 시각은
   주기 길이만큼 퍼진다. 차량 내 장치의 시계 동기화(NTP/RTC)는 미정이고, 시계가 뒤로 가면 같은 timestamp가
   생겨 InfluxDB에서 덮어써질 수 있다(밀리초 정밀도 — 기존 유실 사례와 같은 메커니즘).
8. **긴 장애 뒤 backlog 배출 비용** — `spool.pending()`은 pump마다(50ms) spool 전체를 정렬하고, `ack()`는 메시지마다
   폴링 스레드에서 fsync한다. 장애가 길어 수만 건이 쌓이면 배출 중 폴링이 굶을 수 있다(측정 안 함 — 폴링이 밀리면
   `skipped_overrun`에 보인다). 선택지: 힙/한 번만 정렬, ack fsync 묶음 처리, spool I/O 전용 스레드.
9. **시계 타당성** — RTC 없는 호스트는 부팅 직후 1970년이나 오래된 시각을 낼 수 있고, 계약은 timestamp **형식만**
   보므로 그대로 통과한다. 선택지: 시계가 그럴듯해질 때(NTP 동기화 확인 또는 하한 시각)까지 spool에 쓰지 않는다.
