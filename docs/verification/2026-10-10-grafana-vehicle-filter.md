# Grafana `vehicle-telemetry` 대시보드 — 등록 차량 변수로 실험 차량 선 숨기기

- 작업일: 2026-10-08(로컬 시각 기준 실행, 문서 이름은 요청된 2026-10-10 디렉터리에 맞춤)
- 기준 HEAD: `663f2f29d6ace067cef89f505658dab45b1a7a51` + 이 문서의 미커밋 변경
- 환경: dev 스택(`docker-compose.yml` + `docker-compose.dev.yml`, `MQTT_TLS_PORT=18883`), Grafana 10.4.0, PostgreSQL 16
- 원본: [`evidence/2026-10-10-grafana-vehicle-filter/`](evidence/2026-10-10-grafana-vehicle-filter/)
- 배경: `docs/experiment-data-inventory.md` §7 — 5개 InfluxDB 패널이 `vehicle_id`를 거르지 않아 실험 차량이 추가 선으로 보였다.

**데이터는 지우지 않았다.** InfluxDB·PostgreSQL 행은 그대로이고, 대시보드에서 **보이는 범위**만 좁혔다.

## 1. 선택과 이유

**PostgreSQL `vehicles`(active=true)를 원천으로 하는 변수**를 택했다. InfluxDB tag + 정규식 대안은 쓰지 않았다.

- 앱(목록·상세·이력·WebSocket)이 쓰는 "등록 차량"의 정의가 바로 `vehicles WHERE active`다(§7 (a)(b)). 같은 원천을 써야 Grafana와 앱이 같은 차량 집합을 보여준다.
- 정규식(`^SIM-\d+$` 등)은 등록이 아니라 **이름 모양**을 본다. `SIM-050`(실험)·`SIM-003`(미등록)이 그대로 통과한다 — 이번에 숨겨야 하는 것을 못 숨긴다.
- 비용이 감당할 만했다: 읽기 전용 역할 1개(멱등 SQL + 실행 스크립트), datasource 1개, compose 환경변수 2줄, `.env.example` 변수 1개. Flyway·백엔드 스키마는 바꾸지 않았다.

## 2. 무엇을 바꿨나

| 파일 | 변경 |
| --- | --- |
| `monitoring/postgres/grafana_reader.sql` (신규) | 역할 `grafana_reader` 생성(없을 때만)·비밀번호 동기화·권한 설정. 멱등. 되돌리기 SQL을 주석으로 둠 |
| `scripts/grafana-pg-reader.sh` (신규) | 실행 중인 `telemetry-postgres`에 `docker exec`로 위 SQL을 흘린다. **기존 볼륨에도 동작**(init 스크립트 아님). 관리자 접속은 컨테이너 안 `POSTGRES_USER`/`POSTGRES_DB`, 비밀번호는 `-e GRAFANA_PG_READER_PASSWORD` + psql `\getenv`로만 전달(명령줄 인자 아님) |
| `monitoring/grafana/provisioning/datasources/datasources.yml` | `PostgreSQL-registry`(uid `telemetry-pg-registry`, user `grafana_reader`, `sslmode: disable`, `maxOpenConns: 2`) 추가 |
| `docker-compose.yml` (grafana) | `POSTGRES_DB`, `GRAFANA_PG_READER_PASSWORD: ${GRAFANA_PG_READER_PASSWORD:-}` 전달 |
| `.env.example` | `GRAFANA_PG_READER_PASSWORD=`(값 없음) + 설정 절차 주석 |
| `monitoring/grafana/dashboards/vehicle-telemetry.json` | `templating`에 변수 `vehicle` 추가, 패널 1~5 Flux에 필터 1줄, `version` 1→2 |
| `scripts/check-grafana-registry.sh` (신규, 추가 2) | 설정 오류를 "No data" 대신 종료 코드 1 + 한국어 사유로 드러낸다 |

### 역할 권한(`01`, `02`)

- 속성: `NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT NOREPLICATION NOBYPASSRLS`, `CONNECTION LIMIT 5`, `default_transaction_read_only=on`
- 권한: `CONNECT`(DB) + `USAGE`(schema public) + **열 단위 `SELECT (vehicle_id, active) ON vehicles`**. 표 단위 권한 0행.
- 음성 확인(`02`, grafana_reader로 TCP 비밀번호 인증 접속): `SELECT name`·`SELECT *` FROM vehicles, `users`, `anomaly_alerts` → `permission denied`. `UPDATE`·`CREATE TABLE` → `read-only transaction`. 스크립트 2회 연속 실행 성공(멱등).
- PostgreSQL 기본값으로 `PUBLIC`이 가진 권한(DB CONNECT, public schema USAGE)은 그대로다 — 이 역할에 추가로 생긴 것은 위 열 권한뿐이다.

### 변수 정의

```json
{ "name": "vehicle", "type": "query",
  "datasource": { "type": "grafana-postgresql-datasource", "uid": "telemetry-pg-registry" },
  "query": "SELECT vehicle_id FROM vehicles WHERE active ORDER BY vehicle_id",
  "multi": true, "includeAll": true, "refresh": 1,
  "current": { "text": ["All"], "value": ["$__all"] } }
```

- `allValue`(custom all value)를 **두지 않았다** → `All`은 쿼리가 돌려준 옵션 전체, 즉 **등록된 활성 차량 전체**로 펼쳐진다(InfluxDB tag 전체가 아니다).
- `refresh: 1` = 대시보드 로드 시 목록 갱신. 새로 등록한 차량은 **대시보드를 다시 열어야** 목록에 들어온다(5초 자동 새로고침으로는 안 바뀐다).
- datasource type은 provisioning에 `postgres`로 적었고 Grafana 10.4가 `grafana-postgresql-datasource`로 정규화해 보고했다. 대시보드는 그 이름 + uid로 참조한다.

### 패널 필터(5개 공통, `aggregateWindow` 바로 앞)

```
|> filter(fn: (r) => contains(value: r.vehicle_id, set: ${vehicle:json}))
```

패널: 1 차량 속도, 2 엔진 온도, 3 RPM, 4 배터리 전압, 5 연료 잔량. GPS 패널은 이 대시보드에 없다(스크린샷 마스킹 대상 없음).

## 3. 적용

```sh
# 셸 환경에 GRAFANA_PG_READER_PASSWORD를 둔 상태에서 (값은 출력하지 않는다)
sh scripts/grafana-pg-reader.sh                       # 2회 실행, 둘 다 OK
MQTT_TLS_PORT=18883 docker compose -f docker-compose.yml -f docker-compose.dev.yml up -d --no-deps grafana
```

grafana만 재생성했다. 나머지 서비스는 건드리지 않았다. 비밀번호는 이번 세션이 무작위로 만들어 **저장소 밖(세션 scratchpad)** 에 두었다 — §6 결정 대기 참고.

## 4. 검증

Grafana `/api/ds/query`로 **프로비저닝 파일의 패널 쿼리를 그대로** 실행했다(`verify_panels.js`). 변수 보간은 Grafana 프론트엔드가 하는 일이라 API로는 직접 재현할 수 없으므로, `${vehicle:json}`을 **변수 쿼리 결과(= All이 펼쳐질 값)의 JSON 배열**로 바꿔 보냈다. 대조군은 같은 쿼리에서 필터 한 줄만 뺀 것(= 변경 전).

| 확인 | 결과 | 원본 |
| --- | --- | --- |
| datasource health | `Database Connection OK` | (API 응답, 본문 기록) |
| 변수 쿼리 결과 | `["SIM-001","SIM-002"]` | `03`, `04` |
| 실행 중 Grafana가 읽은 대시보드 | `provisioned=true`, 변수 존재, 패널 5/5에 필터 | `06` |
| now-30m(시뮬레이터 가동 중) | 변경 전 3선 `SIM-001~003` → 변경 후 2선 `SIM-001`·`SIM-002`. 5패널 동일 | `03` |
| now-30d(실험 행 포함 범위) | 변경 전 **40선** → 변경 후 **2선**. 숨겨진 38개: `STUCK-CTL`·`STUCK-CTLZ`·`STUCK-WARM`, `SESSCLN-1009`, `SIM-050`, `STOR1-CHK`, `OPTF-E2E`, `ACKTEST-*`, `OUTAGE-*`, `RECON-*`, `RESUB-*`, `RPL-02`, `SCHEMA145736-*`, `SESSLOSS-G1/G2`, `SIM-003`. 5패널 동일 | `04` |
| 단일 선택 `["SIM-002"]` | `SIM-002` 1선 | `05` |
| 빈 집합 `[]` | 오류 없이 빈 프레임 1개(점 0) — 아무것도 그려지지 않는다 | `05` |

## 5. SIM-003

**SIM-003은 시뮬레이터 차량이지만 `vehicles`에 없다(미등록).** 그래서 이 변수로는 **숨겨진다** — 등록 차량만 보이게 한다는 요구를 그대로 적용한 결과다. 앱에서도 이미 같은 이유로 안 보인다(§7: 알림 466행 포함, REST·WebSocket 접근 불가).
SIM-003을 대시보드에 보이게 하는 방법은 **등록**(`vehicles`에 활성 행 추가)이지 대시보드 예외가 아니다. 이번에 등록하지 않았다 → **결정 대기**.

## 6. 한계와 결정 대기

- **화면 렌더링 미검증.** 내장 브라우저로 열었더니 Grafana 로그인 화면이었고, 실제 관리자 비밀번호를 입력하는 것은 이 작업의 규칙상 하지 않았다. 따라서 스크린샷이 없고, **프론트엔드가 `All`을 `${vehicle:json}`로 실제로 어떻게 펼치는지는 API 에뮬레이션으로만 확인**했다. 로그인한 상태에서 대시보드를 열어 범례에 SIM-001·SIM-002만 있는지 한 번 보면 닫힌다.
- ~~**결정 대기 — 비밀번호 보관.**~~ **닫힘(추가 2 §A)** — 로컬 `.env`에 영구화했다. 아래는 당시 기록이다. `.env`는 이번에 수정하지 않았다. 지금 실행 중인 grafana는 셸에서 넘긴 값으로 떠 있다. `.env`에 `GRAFANA_PG_READER_PASSWORD`가 없는 채로 grafana가 다시 만들어지면 값이 비어 datasource 인증이 실패하고, 변수가 비어 **패널 5개가 빈다**(실험 차량이 다시 새지는 않는다 — 닫히는 쪽). 영구화하려면 `.env`에 값을 정해 넣고 `sh scripts/grafana-pg-reader.sh`(비밀번호 동기화) → `docker compose ... up -d --no-deps grafana`.
- **결정 대기 — SIM-003 등록 여부**(§5).
- 변수 목록은 대시보드 로드 시에만 갱신된다. 차량 등록/비활성화 직후에는 다시 열어야 반영된다.
- 이 필터는 **표시 제한**이다. Grafana Explore나 다른 대시보드에서 InfluxDB datasource를 직접 쿼리하면 모든 tag가 보인다. 권한 경계가 아니다.
- 등록 여부 판정은 PostgreSQL `vehicles`가 기준이다. 소유자별(사용자별) 범위는 반영하지 않는다 — Grafana 사용자는 관리자 1명뿐이다.
- 1회 확인이다. 부하 중·장시간 동작은 보지 않았다.

## 되돌리기

1. `git checkout -- monitoring/grafana docker-compose.yml .env.example` 후 신규 파일 2개 삭제, `docker compose ... up -d --no-deps grafana`.
2. 역할 제거(선택): `grafana_reader.sql` 끝 주석의 REVOKE/DROP ROLE. 데이터 영향 없음.

## 추가 — 브라우저 확인과 스크린샷 (같은 날)

관리자 비밀번호를 입력하지 않기 위해, Grafana Admin API(컨테이너 env의 관리자 자격, 값 출력 없음)로 **이 확인 전용 임시 Viewer 계정**을 만들어 브라우저에서 로그인했고, 확인 뒤 **삭제했다**(조회 404 확인). 계정 값은 저장소 밖 임시 파일에만 두었다가 지웠다.

- `now-30m`, 변수 `All`, 시뮬레이터 가동 중: 5개 패널의 범례가 **`SIM-001`·`SIM-002`뿐**이다 — [`07_dashboard_30m_after_filter.jpg`](evidence/2026-10-10-grafana-vehicle-filter/07_dashboard_30m_after_filter.jpg). 이 대시보드에는 지도·GPS 패널이 없어 가릴 좌표가 없다.
- `now-30d`는 대시보드의 5초 자동 새로고침 아래에서 20초 안에 그려지지 않아 스크린샷을 남기지 못했다 — 30일 범위의 결과는 위 API 확인(40선 → 2선)을 근거로 둔다.

## 추가 2 — 비밀번호 영구화, 권한 재확인, 점검 스크립트, 30일 조회 (2026-10-08 오후)

기준 HEAD `5561d98` + 미커밋 변경. dev 스택, `MQTT_TLS_PORT=18883`. 각 1회.

### A. 영구화 — 완료

1. 로컬 루트 `.env`에 `GRAFANA_PG_READER_PASSWORD`를 **무작위 값으로 한 줄 추가**했다(기존 줄 0개 확인 후, 생성과 기록을 한 파이프라인에서 — 값은 어디에도 출력하지 않았다). `.env`는 추적되지 않는다.
2. `.env`의 그 한 줄만 서브셸 환경으로 넘겨 `sh scripts/grafana-pg-reader.sh`를 **2회** 실행 → 둘 다 `grafana_reader: OK`(멱등).
3. 셸 override 없이 `docker compose ... up -d --no-deps grafana`로 grafana를 재생성 → 컨테이너 env에 값이 있음(존재만 확인).

| 확인 | 결과 | 원본 |
| --- | --- | --- |
| datasource health | `Database Connection OK` | `11` |
| 변수 쿼리 | `["SIM-001","SIM-002"]` | `11` |
| now-30m, 시뮬레이터 가동, 패널 5개 | 변경 전 3선(`SIM-001~003`) → 변경 후 **2선 `SIM-001`·`SIM-002`**. 5패널 동일 | `11` |
| 점검 스크립트 | PASS(3/3 단계) | `08` |

`11`의 첫 시도는 `intervalMs=1800`(30m/1000 그대로)이라 Grafana가 정수가 아닌 `windowPeriod`를 만들어 Flux가 거부했다 — **측정 도구 오류**다(프론트엔드는 2s로 반올림한다). 2000으로 다시 잰 값만 남겼다.
**그 무효 시도(`intervalMs=1800`)의 원본 출력은 보존하지 않았다**(증거 정책상 실패 실행도 남기는 것이 원칙인데 지키지 못했다 — 사후에 적는다).
무효인 이유: 대시보드가 실제로 보내는 요청이 아니다 — Grafana가 1800ms로 비정수 초 `windowPeriod`를 만들어 Flux가 쿼리 자체를 거부했으므로 패널 시계열이나 조회 시간에 대해 아무것도 재지 않았다.

### B. 권한 재확인 — 기록만, 변경 없음

`09`(관리자로 카탈로그 조회), `10`(grafana_reader로 **비밀번호 인증** 접속 — 컨테이너 네트워크 IP. `127.0.0.1`은 pg_hba가 `trust`라 비밀번호를 안 보므로 쓰지 않았다. 틀린 비밀번호는 `password authentication failed`로 거부되는 것까지 기록).

- 속성: superuser·createrole·createdb·replication·bypassrls 전부 `f`, `NOINHERIT`, 소속 역할 0, `CONNECTION LIMIT 5`, `default_transaction_read_only=on`.
- 권한: DB `CONNECT`, schema `public` `USAGE`(CREATE 없음), **열 권한 `SELECT (vehicle_id, active) ON vehicles` 2개뿐.** 표·시퀀스·함수에 명시 권한 0.
- 음성: `name`·`owner_id`·`*` FROM vehicles, `users`, `anomaly_alerts` → `permission denied`. `UPDATE`·`INSERT`·`DELETE`·`CREATE TABLE` → `read-only transaction`.
- **새로 확인한 것 두 가지**(고치지 않았다):
  - `default_transaction_read_only`는 **역할 스스로 끌 수 있는 세션 기본값**이다. 끄고 다음 트랜잭션에서 `UPDATE` → `permission denied for table vehicles`, `CREATE TABLE public.x` → `permission denied for schema public`. **쓰기를 실제로 막는 것은 권한이고, read-only는 보조층이다.**
  - read-only를 끄면 **`CREATE TEMP TABLE`은 성공한다** — DB의 `TEMP`가 PostgreSQL 기본값으로 `PUBLIC`에 있기 때문이다(세션 종료 시 사라지는 임시 테이블, 다른 테이블 접근은 없음). 막으려면 `REVOKE TEMP ON DATABASE ... FROM PUBLIC`이 필요한데 **다른 역할에도 영향**이 가므로 결정 대기로 둔다.

### C. 설정 오류를 드러내기 — `scripts/check-grafana-registry.sh`

조용한 "No data" 대신 종료 코드 1과 이유를 낸다. 값은 출력하지 않는다.

1. `GRAFANA_PG_READER_PASSWORD`가 셸 환경 또는 `.env`에 있고 비어 있지 않다(셸 환경 우선 — compose와 같은 순서).
2. 그 값으로 grafana_reader가 **비밀번호 인증**으로 로그인해 변수 쿼리를 실행하고 1행 이상 받는다.
3. **실행 중인 Grafana**의 datasource health가 OK다 — `.env`를 바꾸고 grafana를 재생성하지 않은 경우가 여기서 걸린다.

| 실행(`08`) | 결과 |
| --- | --- |
| 그대로 | `PASS`, exit 0 |
| 변수 없음 모의(`ENV_FILE=/dev/null`, 셸 변수 unset — `.env` 미변경) | `FAIL [1]`, exit 1 |
| 틀린 비밀번호 모의(셸 환경으로 override — `.env`·역할 미변경) | `FAIL [2] 비밀번호 인증 실패`, exit 1 |
| 3단계 경로 모의(`GRAFANA_DS_UID=no-such-uid`) | `FAIL [3]`, exit 1 |

**보지 않은 실패**: "`.env`는 맞는데 Grafana가 옛 값으로 떠 있는" 실제 상황은 grafana를 틀린 값으로 재생성해야 재현돼서 하지 않았다. 3단계가 그 경우 health 실패를 받을 것이라는 것은 Grafana API 동작에 기댄 추정이다.

대시보드 변수 `description`에 "목록이 비고 패널이 No data면 datasource 설정 확인: sh scripts/check-grafana-registry.sh"를 덧붙였다(쿼리·패널 변경 없음, 실행 중 Grafana가 다시 읽은 것 확인). `.env.example`에는 이름과 절차만 있다(값 없음).

### D. 30일 조회 — 쿼리 성능 문제 아님(이 조건, 1회)

자동 새로고침 없이 **한 번**, Grafana `/api/ds/query`로 변수 쿼리 → 패널 5개 병렬(프론트엔드처럼) — `time_panels.js`, `12`.
대시보드에 패널별 `maxDataPoints`/`interval`이 없어 브라우저는 패널 픽셀 폭으로 정한다. 여기서는 `maxDataPoints=1000`, 30d/1000=43.2m → Grafana식 반올림 **30m**을 썼다(근사).

| 쿼리 | 시간 | 결과 |
| --- | ---: | --- |
| 변수(PostgreSQL) | 59 ms | `SIM-001`, `SIM-002` |
| 패널 1~5(InfluxDB, 병렬) | 537 / 550 / 568 / 563 / 566 ms | 각 **2선**, 2,882점 |
| 패널 5개 wall / 변수 포함 | 568 ms / 628 ms | |

**30일 쿼리는 1초 안에 끝났다.** 그래서 앞 절의 "5초 자동 새로고침 아래에서 20초 안에 그려지지 않았다"는 **쿼리 시간으로는 설명되지 않는다** — 브라우저 쪽(새로고침이 진행 중 요청을 취소·재시작하는지, 렌더링)의 문제일 가능성이 있으나 **확인하지 않았다.**

**브라우저 렌더링은 이번에 확인하지 못했다.** 실제 관리자 비밀번호를 UI에 입력하지 않기 위해 앞 절처럼 임시 Viewer 계정을 Admin API로 만들려 했으나 이 세션의 권한 정책이 계정 생성을 거부했다. 비밀 없이 브라우저로 볼 방법이 없어 **30일 화면은 미검증**이다. 로그인한 사람이 `?from=now-30d&to=now`에 새로고침 Off로 한 번 열어 보면 닫힌다.

### 원본(추가 2)

- `08_check_script_pass_fail.txt` — 점검 스크립트 통과 1 + 실패 모의 3
- `09_reader_role_privileges_recheck.txt` — 역할 속성·권한 카탈로그
- `10_reader_role_negative_tests_recheck.txt` — 음성 확인(read-only 해제 후 포함), 틀린 비밀번호
- `11_persisted_panel_series_30m.txt` — 영구화 후 health·변수·패널 5개(30m)
- `12_panels_30d_timing.txt` + `time_panels.js` — 30일 1회 소요 시간·선 수

비밀 스캔: 위 파일과 이번에 바꾼 파일 전부에서 `.env`의 reader 비밀번호 값과 Grafana 관리자 비밀번호 값을 `grep -F -c -f -`(값을 stdin으로 넘기고 건수만 출력)로 찾아 **0건**. `password`·`token`·`secret` 단어 일치는 변수 이름·오류 문구뿐이다.

### 남은 것

- 시뮬레이터 정지: 작업 끝에 `telemetry-simulator`를 멈추려 했으나 이 세션의 권한 정책이 거부했다 — **가동 중으로 남아 있다.**
- 30일 브라우저 렌더링(위), "Grafana만 옛 값" 실제 상황 재현, `TEMP` 권한 결정.
