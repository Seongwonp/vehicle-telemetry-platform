# Runbook — V4를 이미 적용한 DB의 `users.username` 길이 전환 (V4 checksum 불일치)

`V4__users_and_vehicle_owner.sql`은 2026-09-27 커밋(`48d363a`) 뒤에 **한 번 수정됐다** —
`users.username`을 `VARCHAR(50)`에서 `VARCHAR(100)`으로. 옛 `vehicles.owner`가 100자였는데
V4의 백필이 그보다 짧은 컬럼에 넣고 있어서, 51~100자 소유자가 있는 DB에서는 V4 자체가 죽는다.

> **적용된 마이그레이션을 고치는 것은 원칙상 하지 않는다.** 이번엔 V4가 커밋된 지 하루였고 적용된 DB가
> 이 PC의 `telemetry-postgres` 하나뿐이라 고쳤다. 그 대가가 이 문서다 — Flyway는 적용된 파일의 checksum을
> 기억하므로, **옛 V4를 이미 적용한 DB는 새 코드로 기동하면 validate에서 멈춘다.**
>
> 새 DB는 이 문서와 무관하다. 새 V4가 바로 100자로 만들고, V6의 `ALTER`는 같은 타입이라 아무것도 안 바꾼다.

## 0. 이 DB가 어느 쪽인지 확인한다

```bash
docker exec telemetry-postgres sh -c 'psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -At -c \
  "SELECT version, checksum, success FROM flyway_schema_history ORDER BY installed_rank;"'
docker exec telemetry-postgres sh -c 'psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -At -c \
  "SELECT character_maximum_length FROM information_schema.columns WHERE table_name='"'"'users'"'"' AND column_name='"'"'username'"'"';"'
```

| 결과 | 뜻 | 할 일 |
| --- | --- | --- |
| version 4의 checksum이 `-1626396431`, 길이 50 | **옛 V4를 적용한 DB**(이 PC, 2026-09-28 확인) | 아래 1~3 |
| version 6이 있고 길이 100 | 이미 전환됨 | 없음 |
| `users` 없음 | V4 전 DB | 없음 — 새 코드가 새 V4·V6를 순서대로 적용한다 |

## 1. 새 코드로 기동해 보면 이렇게 멈춘다

```
FlywayValidateException: Validate failed: Migrations have failed validation
Migration checksum mismatch for migration version 4
-> Applied to database : -1626396431
-> Resolved locally    : <새 checksum>
```

**여기서 `Resolved locally` 값을 적어 둔다.** 새 checksum은 파일 내용(줄 끝 포함)으로 계산되므로 문서에
박아 두지 않는다 — 기동 로그가 알려 주는 값이 그 파일의 값이다.

## 2. history의 checksum만 바꾼다 — 스키마는 건드리지 않는다

```bash
docker exec telemetry-postgres sh -c 'psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -v ON_ERROR_STOP=1 -c \
  "UPDATE flyway_schema_history SET checksum = <Resolved locally 값> WHERE version = '"'"'4'"'"' AND checksum = -1626396431;"'
```

`UPDATE 1`이어야 한다. `UPDATE 0`이면 0단계로 돌아간다 — 이미 바뀐 DB거나 다른 checksum이다.

이것은 Flyway `repair`가 하는 일 중 checksum 갱신만 손으로 한 것이다. `repair`는 실패한 행 삭제도
하는데 여기엔 실패한 행이 없고(전부 `success = t`), 이 저장소에 Flyway CLI·Gradle 플러그인이 없다.
**`spring.flyway.validate-on-migrate=false`로 우회하지 않는다** — 다음 불일치도 조용히 지나간다.

## 3. 다시 기동하면 V6가 컬럼을 넓힌다

```bash
docker compose -f docker-compose.yml -f docker-compose.dev.yml up -d --build backend
docker compose logs backend | grep -i "flyway\|migrat"
```

기동 뒤 0단계의 두 번째 질의가 **100**이고 `flyway_schema_history`에 version 6이 `success = t`로 있어야 한다.
`ALTER TABLE ... TYPE VARCHAR(100)`은 넓히는 방향이라 데이터·FK(`vehicles.owner_id`)를 건드리지 않는다 —
`FlywayPostgresContractTest.widensAlreadyAppliedUserSchema`가 50자 스키마에 V6만 적용해 이를 확인한다.

## 하지 말 것

- `docker compose down -v`·볼륨 삭제로 "깨끗이" 시작하지 않는다 — 실험 증거와 등록 데이터가 같이 사라진다.
- 이 문서를 근거로 다른 적용된 마이그레이션을 고치지 않는다. 다음부터는 새 버전 파일을 추가한다.
