# 데이터 보존·삭제와 개인정보 처리

`docs/roadmap.md` P2. **어떤 데이터가 어디에 얼마나 남는지**를 실제 설정에서 확인해 적는다.
"이렇게 해야 한다"가 아니라 **지금 이렇다**가 기준이고, 안 되어 있는 것은 안 되어 있다고 쓴다.

작성 근거는 2026-09-06의 설정 파일 확인이다(`kafka/init-topics.sh`, `docker-compose.yml`,
`backend/src/main/resources/db/migration/V1__initial_schema.sql`, `docs/db-schema.md`).

---

## 1. 무엇이 어디에 남는가

| 저장소 | 데이터 | 개인정보 성격 | 보존 |
| --- | --- | --- | --- |
| InfluxDB `vehicle_telemetry` | 속도·RPM·온도 등 + **`lat`/`lng`** | **위치정보** — 차량의 이동 경로가 그대로 남는다 | **무기한**(아래 2절) |
| PostgreSQL `vehicles` | `vehicle_id`, `name`, **`owner`** | 소유자 이름(자유 입력 문자열) | 무기한, **물리 삭제 없음** |
| PostgreSQL `anomaly_alerts` | `vehicle_id`, 이상 유형·값·시각 | 위치 없음. 차량 단위 이력 | 무기한 |
| Kafka `vehicle-telemetry` | 원본 payload(**GPS 포함**) | 위치정보 | **1시간**(`retention.ms=3600000`) |
| Kafka DLQ 토픽들 | 실패한 원본 payload | 위치정보 | 1시간 |
| Redis | Rate Limit·BruteForce 카운터 (**클라이언트 IP 기준 키**) | 접속 IP | **TTL 1분 / 15분** — 자동 소멸 |
| 컨테이너 로그 | 요청 로그에 **`clientIp`** (로그 패턴에 포함) | 접속 IP | **정책 없음**(아래 4절) |
| Prometheus | 지표만(차량 단위 식별자 없음) | — | 15일(`--storage.tsdb.retention.time`) |

계정 정보는 DB에 없다 — 관리자 계정은 환경변수(`ADMIN_USERNAME`/`ADMIN_PASSWORD`)로만
주입되고 `users` 테이블 자체가 없다.

## 2. InfluxDB가 무기한이었다

`docker-compose.yml`의 InfluxDB 초기화에 **`DOCKER_INFLUXDB_INIT_RETENTION`이 없었다.**
InfluxDB 2.x에서 이 값을 주지 않으면 버킷 보존 기간은 **무제한**이다. 즉 위치정보가
계속 쌓인다.

**기본값을 90일로 정하고 환경변수로 뺐다**(`INFLUXDB_RETENTION`).

- 90일인 이유: 이 프로젝트의 조회 기능이 보는 범위가 최근 데이터다 — 실시간 화면,
  `/latest`, 주행 기록(`TRIP_DEFAULT_HOURS` 기본 1시간, 최대 6시간), 이상 이력.
  90일을 넘겨 조회하는 기능이 **하나도 없다.**
- 무제한을 유지할 근거가 없다. "나중에 볼지도 모른다"는 근거가 아니다.

**중요한 제약**: `DOCKER_INFLUXDB_INIT_*`는 **버킷을 처음 만들 때만** 적용된다.
이미 볼륨이 있는 환경은 이 값을 바꿔도 그대로다. 기존 환경에서 바꾸려면
`influx bucket update --name <bucket> --retention 90d`를 직접 실행해야 한다.

## 3. 삭제 요청을 처리할 경로가 없다

`DELETE /api/vehicles/{vehicleId}`는 **소프트 삭제**다 — `vehicles.active`를 false로
내릴 뿐이고 InfluxDB의 텔레메트리(위치 포함)와 `anomaly_alerts`는 **그대로 남는다.**
코드 주석에도 그렇게 적혀 있다("차량 ID는 InfluxDB 텔레메트리 데이터와 연결되어 있어
행을 지우면 이력 조회가 깨질 수 있다").

**지금은 이것을 고치지 않는다.** 근거:

- 실제 삭제를 하려면 세 저장소(InfluxDB·PostgreSQL·Kafka)에서 같은 차량의 데이터를
  일관되게 지워야 하고, **Kafka는 특정 레코드만 지울 수 없다**(리텐션이 지나야 사라진다).
  즉 "지웠다"고 말할 수 있으려면 최소한 Kafka 리텐션(1시간)이 지난 뒤여야 한다.
- 삭제 API는 인증·인가가 잘못되면 그 자체가 사고다. 지금 단일 관리자 계정 구조에서
  차량 단위 삭제 권한을 어떻게 나눌지 정하지 않았다.
- **90일 보존이 걸리면 시간이 지나면 사라진다.** 즉시 삭제가 필요한 요구가 확인되면
  그때 만든다.

운영 중 즉시 삭제가 필요하면 수동으로 한다:

```bash
# InfluxDB — 해당 차량의 텔레메트리 삭제
docker exec telemetry-influxdb influx delete \
  --bucket "$INFLUXDB_BUCKET" --org "$INFLUXDB_ORG" --token "$INFLUXDB_TOKEN" \
  --start 1970-01-01T00:00:00Z --stop "$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
  --predicate '_measurement="vehicle_telemetry" AND vehicle_id="KR-XX-0000"'

# PostgreSQL — 이상 이력과 차량 행
docker exec telemetry-postgres psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" \
  -c "DELETE FROM anomaly_alerts WHERE vehicle_id = 'KR-XX-0000';" \
  -c "DELETE FROM vehicles WHERE vehicle_id = 'KR-XX-0000';"
```

Kafka에 남은 원본은 리텐션(1시간)으로 사라진다.

## 4. 로그의 클라이언트 IP

요청 로그 패턴에 `clientIp`가 들어간다(`logging.pattern.console`). 접속 IP는 개인을
식별할 수 있는 정보인데 **로그 보존 정책이 없었다** — 컨테이너 로그가 무한히 쌓인다.

`backend` 서비스에 로그 로테이션을 넣었다(`max-size` 50m × `max-file` 3, 최대 약 150MB).
**backend만 한 이유**는 `clientIp`를 남기는 것이 이 서비스이기 때문이다 — 나머지 컨테이너는
개인 식별 정보를 로그에 남기지 않는다(브로커·Kafka·DB는 자체 로그 형식).

이건 **용량 기준**이지 "며칠 보관"이 아니다 — 트래픽에 따라 보관 기간이 달라진다.
기간 기준이 필요해지면 로그 수집기를 붙여야 한다(현재 없음).

## 5. 정리 — 정해진 것과 안 정한 것

**정했다**
- InfluxDB 버킷 보존 **90일**(신규 환경 기준, `INFLUXDB_RETENTION`)
- 컨테이너 로그 로테이션 50m × 3
- Kafka 리텐션 1시간 유지 — 원본 payload가 GPS를 담고 있어 **짧은 편이 낫다**
- 차량 삭제는 **소프트 삭제 유지**, 즉시 삭제는 위 수동 절차

**안 정했다 / 안 했다**
- PostgreSQL `anomaly_alerts`의 보존 기간(현재 무기한). 위치는 없지만 차량 단위 이력이다
- 삭제 요청 API와 그 권한 모델
- 로그의 **기간 기준** 보존
- ~~기존 환경의 버킷 보존 변경(수동 명령 필요) — 이 저장소 어디에도 마이그레이션 절차가 없다~~
  → **절차를 만들고 실측했다(2026-09-07)**: [`docs/runbook/influxdb-retention-change.md`](runbook/influxdb-retention-change.md).
  **알아야 할 것 셋**: (1) 삭제는 설정 변경 즉시가 아니라 **검사 주기마다** 일어난다
  (운영 기본 30분 — 그동안 초과 데이터가 남아 있다). (2) 삭제 중에도 **쓰기는 안 막힌다**
  (실패 0건). (3) **되돌릴 수 없다** — 보존을 다시 무기한으로 바꿔도 지워진 데이터는
  돌아오지 않는다. 측정: [`load-test/retention-migration/RESULT_20260907_migration.md`](../load-test/retention-migration/RESULT_20260907_migration.md).
  **남은 미검증**: 큰 데이터(수억 행)에서의 삭제 소요와 그때의 쓰기 영향, shard 경계 동작.
- `vehicles.owner`는 자유 입력이라 실명이 들어갈 수 있는데, **입력 단계의 안내나 마스킹이 없다**
