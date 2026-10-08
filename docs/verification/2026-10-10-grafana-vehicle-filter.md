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
- **결정 대기 — 비밀번호 보관.** `.env`는 이번에 수정하지 않았다. 지금 실행 중인 grafana는 셸에서 넘긴 값으로 떠 있다. `.env`에 `GRAFANA_PG_READER_PASSWORD`가 없는 채로 grafana가 다시 만들어지면 값이 비어 datasource 인증이 실패하고, 변수가 비어 **패널 5개가 빈다**(실험 차량이 다시 새지는 않는다 — 닫히는 쪽). 영구화하려면 `.env`에 값을 정해 넣고 `sh scripts/grafana-pg-reader.sh`(비밀번호 동기화) → `docker compose ... up -d --no-deps grafana`.
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
