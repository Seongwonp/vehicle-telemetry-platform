# Runbook — 이미 돌고 있는 환경의 InfluxDB 보존 기간 변경

`docker-compose.yml`의 `INFLUXDB_RETENTION`(기본 90일)은 **버킷을 처음 만들 때만**
적용된다. 이미 볼륨이 있는 환경은 compose를 고쳐도, 재시작해도 **보존이 그대로다.**

> 이 문서가 없어서 `docs/data-retention.md` 5절에 "기존 환경의 버킷 보존 변경 절차가
> 이 저장소 어디에도 없다"가 미해결로 남아 있었다. 텔레메트리에 GPS가 들어 있으므로
> "설정을 90일로 했다"와 "90일 넘은 위치정보가 실제로 없다"는 **다른 말**이고,
> 개인정보 처리에는 후자가 필요하다.
>
> 아래 수치는 실측이다 — [`load-test/retention-migration/RESULT_20260907_migration.md`](../../load-test/retention-migration/RESULT_20260907_migration.md).

## ⚠ 먼저 알아야 할 것 — 이 작업은 **되돌릴 수 없다**

보존을 줄이면 초과분이 **다음 검사 주기 안에 삭제**되고, **설정을 되돌려도 데이터는
돌아오지 않는다**(실측: 무기한으로 되돌린 뒤에도 0행). 줄이기 전에 그 데이터가
정말 필요 없는지 확인한다.

## 0. 지금 이 환경은 어떤 상태인가

```bash
docker exec telemetry-influxdb influx bucket list --host http://localhost:8086 --token "$INFLUXDB_TOKEN" --org "$INFLUXDB_ORG"
```

출력의 **3번째 컬럼이 `Retention`**이다(4번째는 shard group duration이니 헷갈리지 말 것).
`infinite`면 보존이 안 걸려 있다. compose가 90d로 되어 있어도 **이 값이 권위다.**

## 1. 버킷 ID를 먼저 얻는다 — 이름으로는 못 바꾼다

`influx bucket update`의 `--name`은 **대상 선택이 아니라 "새 이름 설정"** 옵션이다.
대상 지정은 `--id`뿐이므로 ID를 먼저 찾아야 한다.

```bash
docker exec telemetry-influxdb influx bucket list --host http://localhost:8086 --token "$INFLUXDB_TOKEN" --org "$INFLUXDB_ORG" | awk '$2=="telemetry"{print $1}'
```

## 2. 바꾼다 — `--org`를 붙이면 실패한다

`bucket update`는 **`--org` 플래그를 받지 않는다**(`list`·`write`·`query`는 받는다).
붙이면 `flag provided but not defined: -org`로 죽는다.

```bash
docker exec telemetry-influxdb influx bucket update --id "<위에서 얻은 ID>" --retention 90d --host http://localhost:8086 --token "$INFLUXDB_TOKEN"
```

## 3. 언제 실제로 지워지는가 — **즉시가 아니다**

InfluxDB는 보존 정책을 **검사 주기마다** 적용한다. 설정 변경과 실제 삭제 사이에 간격이 있다.

| | 실측 |
| --- | --- |
| 변경 직후(t+1s, t+23s, t+44s) | 초과분 **400행 그대로** |
| t+65s (검사 주기 60초로 낮춘 조건) | **0행** — 이때 지워졌다 |
| 삭제 중 쓰기 | **실패 0건** (서비스 안 멈춘다) |
| 90일 이내 데이터 | 영향 없음 |

**운영 기본값은 30분이다**(`INFLUXD_STORAGE_RETENTION_CHECK_INTERVAL` 미설정 시).
위 65초는 실험용으로 60초로 낮춘 값이므로 **그대로 인용하면 안 된다.**
운영에서는 **최대 30분까지 초과 데이터가 남아 있을 수 있다.**

삭제를 앞당기려면 이 값을 낮춰 컨테이너를 재기동한다:

```yaml
services:
  influxdb:
    environment:
      INFLUXD_STORAGE_RETENTION_CHECK_INTERVAL: "5m"
```

## 4. 확인

```bash
docker exec telemetry-influxdb influx query --host http://localhost:8086 --token "$INFLUXDB_TOKEN" --org "$INFLUXDB_ORG" --raw 'from(bucket:"telemetry") |> range(start: -120d, stop: -91d) |> filter(fn:(r) => r._measurement == "vehicle_telemetry" and r._field == "speed") |> group() |> count()'
```

0이 나와야 한다. **`stop: now`는 400 오류다** — Flux에서 `now`는 함수라 `now()`로 써야 한다.

## 5. 되돌리기 — 설정만 돌아오고 데이터는 안 돌아온다

```bash
docker exec telemetry-influxdb influx bucket update --id "<ID>" --retention 0 --host http://localhost:8086 --token "$INFLUXDB_TOKEN"
```

`0`은 무기한이다. **이미 지워진 데이터는 복구되지 않는다**(실측 확인). 이 명령은
"앞으로 더 지우지 않는다"는 뜻일 뿐이다.

## 아직 모르는 것

- **큰 데이터에서의 동작.** 위 실측은 600행 규모다. 수억 행에서 삭제가 얼마나 걸리는지,
  그때도 쓰기가 안 막히는지는 **미측정**이다.
- **shard 경계.** InfluxDB는 shard 단위로 지우므로 shard group duration(기본 24시간)
  경계에 걸친 데이터가 언제 사라지는지는 확인하지 않았다.
- 삭제 중 **조회** 성능(쓰기만 봤다).
