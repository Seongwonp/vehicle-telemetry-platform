# Runbook — Redis 장애

> 범위: **지금 확정된 동작만 적는다.** fail-open 여부는 미결정이므로 여기 없다
> (`docs/redis-failure-policy.md` 3-2절 ②③④⑥).
>
> 근거: `load-test/redis-outage/evidence/20260912-115730/` (각 1회, dev, 무부하)

## 1. 이게 Redis 문제인지 먼저 가른다

**증상**: `/api/**`가 **503 + `{"code":"REDIS_UNAVAILABLE"}`**를 약 2초 만에 돌려준다.

```bash
curl -s -o /dev/null -w '%{http_code} %{time_total}\n' http://localhost:8080/api/vehicles
```

| 보이는 것 | 뜻 |
| --- | --- |
| **503 `REDIS_UNAVAILABLE`, ~2초** | Redis에 못 닿는다. 이 문서 |
| **500 `INTERNAL_ERROR`** | Redis 문제가 **아니다.** 다른 예외다 |
| **403, 수 ms** | 인증이 없는 것뿐이다. Spring Security가 Redis 앞에서 거절한다 |
| **~60초 뒤 응답** | timeout 설정이 안 실린 이미지다 — §5를 본다 |

지표로도 본다:

```bash
curl -s localhost:8080/actuator/prometheus | grep telemetry_redis_unavailable_total
```

라벨 `route`는 **경로 템플릿**(`/api/vehicles/{vehicleId}/anomalies`)이다.
어느 경로가 막혔는지는 보이지만 **어느 차량인지는 안 보인다** — 의도한 것이다.

## 2. 지금 무엇이 안 되고 무엇이 되나

| | 상태 |
| --- | --- |
| 모든 `/api/**` 조회 | **안 된다** (503) |
| 로그인 / refresh / logout | **안 된다** (503) — 인터셉터 밖이지만 `AuthController`가 Redis를 직접 쓴다 |
| `/actuator/health` | **503** — Redis DOWN이 전체 health를 내린다 |
| 미인증 요청 | 403, 정상 속도. **이건 서버가 살아 있다는 뜻일 뿐 API가 된다는 뜻이 아니다** |
| MQTT → Kafka → InfluxDB 저장 | **계속 흐른다.** Redis를 안 쓴다 |
| WebSocket 실시간 스트림 | **코드상 Redis 비의존**(핸드셰이크가 `/api/**` 인터셉터 밖) — **실측 안 함** |

**즉 수집은 멈추지 않고 조회만 막힌다.** 운영자가 "데이터는 쌓이는데 화면이 503"을 본다.

## 3. 복구

```bash
docker start telemetry-redis
```

**재기동이 필요 없다.** Lettuce `ConnectionWatchdog`이 자동 재연결하고,
측정에서는 **8초** 뒤 로그인이 200으로 돌아왔다(적용 전후 동일).

확인:

```bash
curl -s -o /dev/null -w '%{http_code}\n' http://localhost:8080/actuator/health   # 200
```

`telemetry_redis_unavailable_total`은 **누적 카운터라 줄지 않는다.** 복구 여부는
값이 아니라 **증가가 멈췄는지**로 본다:

```
rate(telemetry_redis_unavailable_total[1m])
```

## 4. 하면 안 되는 것

- **`docker compose down -v`** — 다른 볼륨까지 지운다. Redis만 다룰 때 쓸 명령이 아니다.
- **Redis를 비우고 넘어가기** — refresh 토큰 저장소다. 비우면 **모든 세션이 끊긴다.**
  장애가 아니라 우리가 만든 로그아웃이 된다.
- **timeout을 다시 늘리기** — 60초는 Lettuce 기본값이지 우리가 고른 값이 아니다.
  늘려야 할 근거가 생겼다면 `REDIS_TIMEOUT` 환경변수로 올리고 **왜 올렸는지 적는다.**

## 5. 60초 매달림이 보이면 — 이미지가 낡은 것이다

timeout은 `application.yml`에 있고 **이미지에 구워진다.** 배포본이 옛날 이미지면
설정이 안 실린다. 확인 방법:

```bash
docker image inspect vehicle-telemetry-platform-backend:latest --format '{{.Id}}'
docker inspect telemetry-backend --format '{{.Image}}'
```

**두 값이 달라야 의심하는 게 아니라, 같아야 안심한다.** 2026-09-12에 빌드 실패를
`>/dev/null`로 가린 채 낡은 이미지를 재고 "고쳐지지 않았다"고 잠깐 믿었다
(`docs/devlog.md` 2026-09-12).

## 6. 이 문서가 다루지 않는 것

- **지속 장애**(30초·90초·그 이상)에서 달라지는지 — 미측정
- **부하 중 장애** — 미측정. 위 표는 전부 무부하 관측이다
- **스레드 고갈 임계** — 미측정. 매달림이 1/30로 줄었으니 나아지는 방향인 것은
  **계산이지 측정이 아니다**
- **mTLS 프로파일** — 미측정
- **Redis 자체의 영속성·복제** — 단일 인스턴스다
