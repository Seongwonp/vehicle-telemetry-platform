# 인증서·키 접근 범위 축소 + 8883 기본 바인딩 — 2026-10-08

계기: [`2026-10-08-secret-scope-mqtt-tls-store.md`](2026-10-08-secret-scope-mqtt-tls-store.md)에서
`./broker/certs` 디렉터리 전체(**CA 개인키 `ca.key` 포함**)가 런타임 서비스 6개에 마운트되고,
8883이 모든 인터페이스에 게시되는 것을 확인했다. 사용자 승인 범위: 서비스별 필요 파일만 읽기 전용
마운트, `ca.key`는 런타임 서비스 어디에도 안 들어감, 현재 외부 장치 의존이 없으면 8883 기본을
localhost로. **인증서는 재발급하지 않는다.**

- 기준 커밋: `d276d84`(main) + 이 작업의 미커밋 변경. 다른 작업의 미커밋 변경도 트리에 있다(손대지 않음).
- 환경: Windows 11 Pro, Docker Desktop(engine 29.7.2, Compose v5.4.0). 이미지는 다시 빌드하지 않았다
  (`--no-build`; 이번 변경은 compose·스크립트뿐이라 이미지와 무관).
- 증거: [`evidence/2026-10-08-cert-access-scope/`](evidence/2026-10-08-cert-access-scope/) —
  키 내용·비밀번호 값은 어디에도 없다(파일명, 크기, PEM 첫 줄 종류, 설정 여부만).

## 1. 무엇이 어떤 파일을 읽을 수 있나 — 전/후

| 서비스 | 전(디렉터리 `./broker/certs` 통째) | 후 | 근거 |
| --- | --- | --- | --- |
| mosquitto | 전부(아래 "전부" 참고) | `ca.crt`, `server.crt`, `server.key` | `mosquitto.conf`의 `cafile`/`certfile`/`keyfile` 세 줄뿐 |
| backend | 전부 | `backend.p12`, `truststore.p12` | `MQTT_TLS_KEYSTORE_PATH`/`TRUSTSTORE_PATH` 두 경로뿐(`MqttConfig`) |
| backend-storage-1/2/3 (profile `scale`) | 전부 | `backend.p12`, `truststore.p12`(backend와 같은 두 파일, TLS 설정 **변경 없음**) | 아래 §2 — 처음엔 마운트를 빼고 TLS를 껐다가 되돌림 |
| simulator (profile `simulator`) | 전부 | `ca.crt`, `vehicles/`(차량 100대 `.crt`+`.key`) | `vehicle_simulator.py` 376–386행: `TLS_CA_CERT` + `TLS_VEHICLE_CERT_DIR/<ID>.crt/.key` |
| anomaly-detector 등 나머지 | 없음 | 없음 | 원래 마운트 없음 |

"전부" = `ca.key`, `ca.crt`, `ca.srl`, `server.crt/.key`, `backend.crt/.key/.p12`, `truststore.p12`,
`generate-certs.sh`, `vehicles/` 200개(개인키 합계 103개, 전부 평문 PKCS#8) — `00_host_cert_dir_inventory.txt`.

**`ca.key`를 읽을 수 있는 컨테이너: 전 6개 → 후 0개.** 호스트에서 `generate-certs.sh`를 돌리는
이 PC 로컬 계정만 읽는다. `backend.key`(평문 PEM)도 이제 어느 컨테이너에도 없다 — backend는
같은 키를 `backend.p12`(비밀번호 보호 경로)로만 받는다.

남는 범위(의도된 것): simulator는 여전히 차량 키 100개 전부를 본다. `VEHICLE_ID_OFFSET`·`VEHICLE_COUNT`로
쓰는 차량이 실행마다 바뀌고 부하 실험이 오프셋을 달리해 여러 개를 띄우기 때문에 디렉터리째 둔다.

### 경로는 컨테이너 안에서 그대로다

`/mosquitto/certs/*`, `/app/certs/backend.p12`, `/app/certs/truststore.p12`, `/app/certs/ca.crt`,
`/app/certs/vehicles` — 설정·코드·CI는 바꿀 필요가 없었다. CI(`ci.yml`)는 `generate-certs.sh` 후
기본 compose(`--profile simulator`)를 띄우므로 같은 파일 이름이 그대로 생긴다.

## 2. storage 인스턴스는 인증서가 필요 없나

브로커에는 붙지 않는다 — `mqttInbound`·`mqttBrokerMetricsInbound`가 `mqtt.ingest.enabled=true`
조건이고 storage는 `MQTT_INGEST_ENABLED=false`다. **그러나 `MqttConfig.mqttClientFactory()`는 조건 없이
만들어지고, TLS가 켜져 있으면 기동 시 keystore를 읽는다**(`buildSslSocketFactory`). 그래서 인증서만
빼면 기동이 실패한다.

**정정(2026-10-07 확인)**: 첫 변경은 storage 3개의 마운트를 빼고 `MQTT_TLS_ENABLED: "false"`를 **새로 넣었다**(변경 전
기본 compose의 storage는 공통 env의 `MQTT_TLS_ENABLED: "true"`를 받았다 — dev override에서만 false). 실제 통신의 TLS를 끈 것은
아니었다: MQTT 클라이언트 팩토리를 쓰는 곳은 수신 어댑터 두 개뿐이고(`grep mqttClientFactory`), 둘 다
`MQTT_INGEST_ENABLED=false`면 생성되지 않아 storage에는 MQTT 연결 자체가 없다. 그래도 **설정을 원래대로 되돌리고**
backend와 같은 두 파일(`backend.p12`·`truststore.p12`)만 읽기 전용으로 준다 — `ca.key`·서버·차량 키는 없다.

**런타임 확인(1회)**: `docker compose --profile scale build backend backend-storage-1`(최신 코드, 이미지 `61c5b01897fd`),
기본 mTLS 스택 + `backend-storage-1` 기동(볼륨 유지). 기동 성공(`Started TelemetryApplication`, ERROR 없음), 컨테이너 안
`/app/certs`는 두 파일뿐, `MQTT_INGEST_ENABLED=false`·`MQTT_TLS_ENABLED=true`(`10_…`·`11_…`). 파티션 3개를 backend(concurrency 3)가
모두 쥐어 storage-1은 처음에 할당 0(`12_…`, 알려진 구성) → backend를 잠시 `stop`하자 약 40초 뒤 storage-1이 3개를 받았고(`13_…`),
Kafka에 직접 넣은 3건(`STOR1-CHK`, 선택 필드 있는 2건·없는 1건)이 InfluxDB에 저장됐다(speed 3행, 선택 필드 2행 — `15_…`,
storage-1 자체 지표 `points_written_total 3`, `write_failures 0` — `16_…`). 그 뒤 backend 재기동, 스택 `stop`.

## 3. 8883 바인딩 — 기본 `127.0.0.1`, 외부 장치는 명시적으로

`"${MQTT_TLS_BIND:-127.0.0.1}:${MQTT_TLS_PORT:-8883}:8883"`.

**지금 이 PC 밖에서 MQTT로 붙는 것이 있나** — 없었다:

| 후보 | 확인 | 결과 |
| --- | --- | --- |
| Flutter 앱(`../vehicle-telemetry-app`, `0a4e1b4`) | `mqtt`/`8883`/`paho` 검색 | 0건. `api_client.dart`가 HTTP `:8080`, 실시간은 WebSocket |
| obd-bridge | `obd-bridge/README.md` | "프로토타입. 실차·실동글 미검증", "실제 broker 연결 미검증", Compose·CI에 없음, 기본 `MQTT_HOST=localhost` |
| simulator | compose | 같은 compose 네트워크에서 `mosquitto:8883` — 포트 게시와 무관 |
| EC2 배포 | `docs/deployment-guide.md`, 앞 문서 | 가이드만 있고 배포 설정·원격 호스트 없음 |

같은 네트워크의 backend·simulator는 영향이 없다(아래 §4에서 그대로 연결됨).
**외부 장치를 붙일 때**: 스택 PC의 `.env`에 `MQTT_TLS_BIND=0.0.0.0`(또는 특정 NIC IP) →
`docker compose up -d mosquitto`. `docker compose config`로 `0.0.0.0:8883->8883`이 되는 것을 확인했다
(`04_…`). 안내는 `.env.example`, `obd-bridge/README.md`, `docs/deployment-guide.md`에 넣었다.
다른 포트(8080·5432·6379·9092 등)는 여전히 모든 인터페이스다 — 이번 범위 밖(§6).

## 4. 검증

| 항목 | 결과 | 증거 |
| --- | --- | --- |
| `docker compose config --quiet` (기본 / dev+simulator+scale) | 둘 다 통과 | `09_…` |
| 마운트 변화(compose config, env 값은 출력 안 함) | 위 표대로 | `01_…`·`03_…`·`09_…` |
| 인증서 생성 — **임시 디렉터리 사본**에서(기존 `broker/certs`는 그대로) | 수정 전 스크립트 2회(2회차 = 같은 자리 갱신) exit 0, `openssl verify` 4/4 OK, compose가 마운트하는 6개 경로 전부 생성, truststore `trustedCertEntry` 1, backend.p12 `PrivateKeyEntry` 1 | `05_…` |
| 빈 디렉터리 가드(수정 후 스크립트) | `ca.crt/`·`server.key/` 빈 디렉터리를 미리 만든 임시 디렉터리에서 exit 0, 둘 다 일반 파일로 생성, verify OK | `05_…` |
| dev override 기동 | 컨테이너 3개 마운트가 표대로, `ls` 결과에 `ca.key` 없음, `find`도 0건, backend 수신 카운터 증가 | `07_…` |
| **기본 mTLS 기동** | mosquitto: `New client connected … on port 8883` — simulator `u'SIM-001'`~`SIM-003`, backend `u'telemetry-backend'`(인증서 CN이 사용자명) | `08_…` |
| backend mTLS | `MQTT_TLS_ENABLED=true`, `MQTT_PORT=8883`, `mqttInbound`·`mqttBrokerMetricsInbound` 시작, 이번 실행 `Error subscribing` 0건 | `08_…` |
| 인증서 없는 클라이언트 거부 | `mosquitto_sub`(CA만) → `exit=7`, 브로커 로그 `peer did not return a certificate` | `08_…` |
| 데이터가 InfluxDB까지 | `vehicle_telemetry` speed 행: SIM-001 125 → 140 → 156(15초 간격), SIM-002/003 같은 추세(초당 약 1건 = `PUBLISH_INTERVAL 1.0`) | `08_…` |
| 각 런타임 컨테이너 인증서 디렉터리 | mosquitto `ca.crt server.crt server.key` / backend `backend.p12 truststore.p12` / simulator `ca.crt vehicles`(200개) — **`ca.key` 없음** | `07_…`·`08_…` |
| 8883 게시 | `127.0.0.1:8883` | `08_…`(`docker port`) |

dev 실행에서 기동 직후 `Error subscribing … Timed out waiting for a response`가 1회 났다. 이번 변경 전부터
기록된 현상이다([`2026-10-01-mqtt-ack-boundary.md`](2026-10-01-mqtt-ack-boundary.md) 88·160행). 세션 구독이
유지돼 수신이 계속됐다(카운터 501).

### 파일 단위 마운트에서 걸린 것 (`06_…`)

- **인증서를 만들기 전에 `up`하면 Docker가 없는 파일 자리에 빈 디렉터리를 만든다.** 긴 문법에
  `bind.create_host_path: false`를 줘도 이 PC의 Docker Desktop은 디렉터리를 만들었다(처음엔 막힐 거라 예상했고
  compose 주석에도 그렇게 적었다가, 재 보고 지웠다). 그러면 다음 `generate-certs.sh`가 "Is a directory"로
  실패한다 → 스크립트 시작에 **비어 있는 디렉터리만** `rmdir`하는 가드를 넣었다(내용이 있으면 rmdir이 실패해
  아무것도 안 지운다).
- **갱신**: 같은 inode 덮어쓰기와 삭제 후 재생성 모두 이 PC(Docker Desktop 파일 공유)에선 재시작 없이 보였다.
  native Linux에서는 바인드된 파일이 재시작 전까지 옛 inode를 가리킬 수 있다(미검증). 어차피 mosquitto·backend는
  기동 시 인증서를 읽으므로 갱신 뒤에는 `docker compose restart mosquitto backend simulator`.

## 5. 재발급 판단 — 근거와 정책을 나눠서

**(a) 키 접근·노출 근거**

| 키 | 접근 가능했던 범위(전) | 실제 노출 근거 |
| --- | --- | --- |
| `ca.key` | 호스트 계정 + 컨테이너 6개(읽기 전용) | **없음.** 도구 출력·git 이력·증거에 찍힌 기록 없음. 컨테이너 안 프로세스가 읽었다는 근거도 없다. 다만 안 읽었다는 감사 기록도 없다(읽을 수 있었다 ≠ 읽었다) |
| `vehicles/*.key` | 위와 같음 | **있음(부분)**: 10-07 에이전트 도구 출력에 일부 줄이 찍혔고 그 출력은 모델 API로 나갔다. 완전한 키인지는 미확인 |
| `server.key`, `backend.key`/`.p12` | 위와 같음 | 없음. 비밀번호(공개 기본값)는 도구 출력에 1회 — 저장소에 이미 있는 문자열 |

- 8883이 LAN에서 닿았다는 것은 **키 노출 근거가 아니다.** 인증서 없는 접속은 거부된다(§4). 의미가 있는 것은
  "키가 새어 나갔다면 그 키로 붙을 수 있는 곳의 범위"다 — 이제 기본은 이 PC뿐이다.
- 평문 PEM은 비밀번호를 "무의미하게" 만든 것이 아니라 **별개의 읽기 경로**였다. 그 경로를 연 것은 디렉터리
  마운트였고, 이번에 컨테이너 쪽은 닫았다(backend는 p12만 받는다).

**(b) dev 환경 정책**

- 이 CA는 이 PC에서 만든 자체 서명 CA이고, 이 CA를 신뢰하는 것은 이 저장소의 mosquitto뿐이다. 운영 배포 없음.
- mosquitto에 CRL이 없다(`mosquitto.conf`에 `crlfile` 없음). 차량 인증서 한 장만 무효화할 수 없고, 무효화하려면 CA까지 새로 만들어야 한다.

**판단**: 지금 재발급할 근거는 약하다. `ca.key`는 노출 근거가 없고, 부분 노출된 것은 dev 차량 키뿐이다. 그 키는
이 dev CA를 신뢰하는 브로커에만 쓸 수 있고, 그 브로커는 이제 기본으로 이 PC에서만 닿는다.
**재발급이 필요해지는 조건**: (1) `MQTT_TLS_BIND`로 8883을 공유망·외부에 열 때, (2) dev 밖(시연 서버·EC2)에서 쓸 때,
(3) 10-07 출력이 완전한 키였다고 확인될 때. 이 경우 비밀번호만 바꾸면 안 되고 `generate-certs.sh`로 **CA부터 전부**
다시 만든다(CRL이 없으니 차량 키만 바꿔서는 옛 키가 막히지 않는다). 이번에는 재발급하지 않았다.

## 6. 한계

- storage 인스턴스는 대표 1개(storage-1)만 1회 기동·저장 확인(§2). storage-2·3과 3개 동시 기동은 보지 않았다.
- native Linux에서 파일 단위 마운트 갱신 동작은 미검증(CI는 새로 만들고 기동만 하므로 영향 없음).
- macOS Docker 미확인.
- obd-bridge를 다른 장치에서 `MQTT_TLS_BIND=0.0.0.0`으로 붙이는 경로는 미검증이다. 서버 인증서 CN이 `mosquitto`이고
  SAN이 없어서, IP로 접속하면 호스트명 검증에서 막힐 가능성이 높다. 별도 결정이 필요하다.
- 다른 게시 포트(8080, 3000, 5432, 6379, 8086, 9090, 9092, 9093)는 여전히 `0.0.0.0`이다. 이번 범위 밖이다.
- 호스트의 `broker/certs`(`ca.key` 포함)는 그대로 이 PC 로컬 계정이 읽는다. 파일 권한은 바꾸지 않았다.
- simulator는 차량 키 100개 전부를 본다(§1, 의도).

## 바꾼 파일

- `docker-compose.yml` — mosquitto·backend·simulator를 파일 단위 읽기 전용 마운트로 바꿨고, storage 3개는 backend와 같은 두 p12만 마운트한다(TLS 설정 변경 없음). 8883은 `MQTT_TLS_BIND` 기본 `127.0.0.1`
- `broker/certs/generate-certs.sh` — 시작할 때 빈 디렉터리를 지우는 가드만 넣었다(생성·갱신 로직은 그대로)
- `.env.example` — `MQTT_TLS_BIND` 주석
- `docs/deployment-guide.md`, `obd-bridge/README.md` — 외부 장치용 opt-in 안내
- `docs/verification/2026-10-08-secret-scope-mqtt-tls-store.md` — 과장 정정(비밀번호의 의미, LAN 도달성 ≠ 유출·재발급 사유)

## 부록 — 다른 공개 포트 (조사·제안만, 변경 안 함)

`docker compose --profile scale --profile simulator config` 기준(2026-10-07). 8883만 `127.0.0.1`이고 나머지는 **모든 인터페이스**다.

| 서비스 | 포트 | 이 PC 밖에서 쓰는 곳 | 제안 |
| --- | --- | --- | --- |
| backend | 8080 | 앱. 에뮬레이터는 `10.0.2.2` → 호스트 루프백이라 `127.0.0.1`로 충분. **실기기(같은 Wi-Fi 폰)**는 LAN 주소가 필요 | 기본 `127.0.0.1` + `.env` opt-in(8883과 같은 방식). 실기기 검증(P1)을 할 때만 연다 |
| kafka | 9092 | 호스트의 부하·검증 스크립트. advertised listener가 localhost라 LAN에서 붙어도 어차피 동작 안 함 | `127.0.0.1` |
| postgres 5432 · redis 6379 · influxdb 8086 | 각 | 호스트의 점검·증거 수집 명령(대부분 `docker exec`라 포트 불필요) | `127.0.0.1` — 인증은 있지만 dev 기본값이 섞여 있다 |
| prometheus 9090 · alertmanager 9093 · grafana 3000 | 각 | 브라우저(같은 PC) | `127.0.0.1`. grafana는 `GF_SECURITY_ADMIN_PASSWORD` 기본값이 compose에 있다 |

일괄 변경은 하지 않았다(사용자 결정). 바꿀 때는 서비스별로 `${X_BIND:-127.0.0.1}:` 형식과 `.env.example` 설명을 같이 넣는다.
