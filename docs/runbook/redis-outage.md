# Runbook — Redis 장애

> 정책은 `docs/redis-failure-policy.md`에서 정했다. 여기는 **판단과 절차**만 적는다.
>
> 근거: `load-test/redis-outage/evidence/20260912-123533/` (각 1회, dev, 무부하)

## 1. 이게 Redis 문제인지 먼저 가른다

**Redis 장애의 특징은 "전부 죽는다"가 아니라 "갈라진다"이다.**

```bash
curl -s -o /dev/null -w '%{http_code} %{time_total}\n' http://localhost:8080/actuator/health/liveness
```

| 보이는 것 | 뜻 |
| --- | --- |
| liveness **200**인데 readiness **503** | 프로세스는 멀쩡하고 **의존 저장소가 없다.** 이 문서 |
| liveness도 **503** | Redis 문제가 아니다. 프로세스 자체를 본다 |
| 응답 자체가 없음 | 네트워크·컨테이너를 먼저 본다 |

확정하려면 `/api/**`에서 **503 `REDIS_UNAVAILABLE`**이 나오는지 본다:

```bash
curl -s http://localhost:8080/api/vehicles/KR-GA-1234/diagnosis -H "Authorization: Bearer $TOKEN"
```

**~60초 뒤에 응답이 온다면 timeout 설정이 안 실린 낡은 이미지다** — §6.

## 2. 무엇이 되고 무엇이 안 되나 — **일부러 갈라놨다**

| | Redis 중지 중 | 왜 |
| --- | --- | --- |
| **일반 조회**(`/api/vehicles`, `.../anomalies`) | **된다** (200, 약 2초) | fail-open — 제한만 못 걸고 데이터는 정확하다 |
| **진단**(`.../diagnosis`) | **안 된다** (503, 약 4초) | fail-closed — 비용이 드는 호출이다 |
| **로그인 / refresh / logout** | **안 된다** (503, 약 2초) | fail-closed — 보안 통제와 토큰 저장소다 |
| **MQTT → Kafka → InfluxDB 수집** | **계속 흐른다** | Redis를 안 쓴다 |
| **WebSocket 실시간 스트림** | 코드상 Redis 비의존 — **실측 안 함** | 핸드셰이크가 `/api/**` 밖이다 |
| `/actuator/health/liveness` | **200** (8ms) | 재시작 대상이 아니다 |

**운영자가 보게 될 그림**: 데이터는 계속 쌓이고 조회 화면도 뜨는데(느리다),
**로그인이 안 되고 진단만 실패한다.** 이건 고장이 아니라 설계된 동작이다.

### 2-1. 지금 제한이 안 걸리고 있다

조회가 통과하는 동안은 **남용 방어가 없는 상태**다. 확인:

```bash
curl -s localhost:8080/actuator/prometheus | grep telemetry_ratelimit_failopen_total
```

**이 값을 "통과한 요청 수"로 읽지 않는다.** 정확한 뜻은
**"일반 rate limit이 적용되지 않은 요청 수"**다 — 그중 일부는 그 뒤 진단 제한에서
거부된다. 그래서 `/diagnosis`는 `failopen`과 `redis_unavailable`에 **동시에** 오른다.
**둘이 같이 오르는 것이 정상이다.**

```bash
curl -s localhost:8080/actuator/prometheus | grep telemetry_redis_unavailable_total
```

라벨 `route`는 **경로 템플릿**이다 — 어느 경로인지는 보이지만 **어느 차량인지는 안 보인다.**

## 3. 복구

```bash
docker start telemetry-redis
```

**재기동이 필요 없다.** Lettuce `ConnectionWatchdog`이 자동 재연결한다.
측정에서 로그인 200까지 **8초**(오전)와 **30초**(오후)가 나왔다 —
**왜 다른지 확인하지 않았으므로 "N초면 된다"고 기대하지 말고 아래로 확인한다.**

```bash
curl -s http://localhost:8080/actuator/health/readiness     # {"status":"UP"}
```

제한이 실제로 재개됐는지는 헤더로 본다:

```bash
curl -s -D - -o /dev/null http://localhost:8080/api/vehicles -H "Authorization: Bearer $TOKEN" | grep X-RateLimit
```

`X-RateLimit-Remaining`이 다시 붙으면 fail-open이 끝난 것이다.
**장애 중에는 이 헤더가 없다** — 남은 횟수를 모르는데 숫자를 적지 않기 때문이다.

지표는 **누적 카운터라 줄지 않는다.** 복구 여부는 값이 아니라 증가가 멈췄는지로 본다:

```
rate(telemetry_ratelimit_failopen_total[1m])
rate(telemetry_redis_unavailable_total[1m])
```

## 4. 하면 안 되는 것

- **인스턴스를 재시작하지 않는다.** liveness가 200인 이유가 그것이다 —
  재시작해도 Redis는 안 살아나고 멀쩡한 인스턴스만 죽인다.
- **`docker compose down -v`** — 다른 볼륨까지 지운다. Redis만 다룰 때 쓸 명령이 아니다.
- **Redis를 비우고 넘어가기** — refresh 토큰 저장소다. 비우면 **모든 세션이 끊긴다.**
  장애가 아니라 우리가 만든 로그아웃이 된다.
- **"조회가 되니 괜찮다"로 닫지 않는다.** 조회가 되는 동안 **남용 방어가 없다**(§2-1).
- **timeout을 다시 늘리지 않는다.** 60초는 Lettuce 기본값이지 우리가 고른 값이 아니다.
  늘려야 할 근거가 생겼다면 `REDIS_TIMEOUT` 환경변수로 올리고 **왜 올렸는지 적는다.**

## 5. 장애가 길어지면 — **아직 모르는 것**

- **지속 장애(30초·90초·그 이상)에서 달라지는지 미측정.**
- **부하 중 장애 미측정.** fail-open은 통과시키되 **요청마다 2초를 쓴다** —
  유입이 많으면 그 2초가 스레드 점유로 바뀐다. **고갈 임계는 안 쟀다.**
  장애 중 응답이 급격히 느려지면 이 경로를 의심하되, **원인으로 단정하지 말고 재라.**
- **fail-open 중 실제 남용이 일어났을 때의 피해 미측정.**

## 6. 60초 매달림이 보이면 — 이미지가 낡은 것이다

timeout은 `application.yml`에 있고 **이미지에 구워진다.**

```bash
docker image inspect vehicle-telemetry-platform-backend:latest --format '{{.Id}}'
docker inspect telemetry-backend --format '{{.Image}}'
```

**두 값이 같아야 안심한다.** 2026-09-12에 빌드 실패를 `>/dev/null`로 가린 채
낡은 이미지를 재고 "고쳐지지 않았다"고 잠깐 믿었다(`docs/devlog.md` 2026-09-12).

## 7. 이 문서가 다루지 않는 것

- **Redis 자체의 영속성·복제** — 단일 인스턴스다.
- **mTLS 프로파일** — 미측정.
- **WebSocket이 장애 중 실제로 흐르는지** — 코드로만 확인했다.
