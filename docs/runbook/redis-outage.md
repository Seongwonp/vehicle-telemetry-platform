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
| liveness **200**, readiness **200**, 종합 `/actuator/health` **503** | 프로세스도 라우팅도 멀쩡하고 **공유 저장소(Redis 등)가 없다.** 이 문서 |
| liveness도 **503** | Redis 문제가 아니다. 프로세스 자체를 본다 |

**readiness는 Redis 장애에 503이 되지 않는다(2026-09-13 변경).** 일반 조회를 fail-open으로
살려두는데 readiness가 503이면 프록시가 전 인스턴스를 빼서 그 조회가 도달하지 못한다.
Redis 상태는 **종합 health·지표·알림**에서 본다(`docs/redis-failure-policy.md` §9-1).
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
| **로그인 / logout** | **안 된다** (503, 약 2초) | fail-closed — 보안 통제와 토큰 저장소다 |
| **refresh** | **안 된다** (503, 약 4초) | 일반 rate limit(fail-open 2초)을 지난 뒤 토큰 저장소에서 다시 막힌다 |
| **MQTT → Kafka → InfluxDB 수집** | **계속 흐른다** | Redis를 안 쓴다 |
| **WebSocket 실시간 스트림** | 코드상 Redis 비의존 — **실측 안 함** | 핸드셰이크가 `/api/**` 밖이다 |
| `/actuator/health/liveness` | **200** (8ms) | 재시작 대상이 아니다 |
| `/actuator/health/readiness` | **200** | 공유 저장소 장애로 전 인스턴스를 빼지 않는다(§9-1) |
| `/actuator/health` (종합) | **503** | Redis 상태는 여기서 본다 |

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
관측값은 실행마다 달랐다 — 로그인 200까지 8초·30초(2026-09-12), 기동 명령 기준 앱 경로 회복이
30초 중단 뒤 **+4·+24·+6초**, 90초 중단 뒤 **+25·+34·+34초**(2026-09-13, 각 3회, 무부하). 6회 모두 회복이 backend 로그
`Reconnected to redis`와 겹쳤고 **재연결 대기가 주요 후보**다(Lettuce 기본 재연결 간격 상한 30초). **Redis가 PONG을 돌려준 뒤에도
앱이 30초 넘게 503을 낸 실행이 있었다.** "N초면 된다"고 기대하지 말고 아래로 확인한다.

```bash
docker logs --since 10m telemetry-backend 2>&1 | grep -E "Reconnected to redis|Cannot reconnect"
```

```bash
curl -s http://localhost:8080/actuator/health     # {"status":"UP"} — readiness는 장애 중에도 UP이라 복구 판정에 못 쓴다
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

### 3-1. 알림이 뜻하는 것과 오는 시각 (2026-09-13 규칙 변경 후)

두 알림은 **요청 처리에서 관찰한 Redis 영향**이 이어지는 동안 울린다 — `sum(increase(telemetry_*_all_total[1m])) > 0` + `for: 1m`.
**Redis 자체의 가용성 감시도, 장애 지속 시간 측정도 아니다.** 합계 카운터(`_all_total`)는 기동 시 0으로 등록돼 첫 실패부터 보인다.
근거: `load-test/redis-outage/RESULT_20260913_alert_timing.md` §6·§7-4.

| 각 1회 관측 (중지 완료 기준) | 90초 중단 | 30초 중단 | 실패 1건 |
| --- | --- | --- | --- |
| firing(Prometheus) | +70.6s — **실패 중** | +85.0s — 복구 뒤, 평가 1회 | 없음 |
| Redis 응답(PONG) | +91.9s | +31.7s | — |
| 앱 경로 첫 성공 | +124.4s | +36.1s | — |
| 해제 | 마지막 증가 뒤 약 53s | 약 53s | 약 53s |
| **수신기가 받은 firing** | +115.5s | **없음** | 없음 |
| **수신기가 받은 resolved** | +415.5s | 없음 | 없음 |

- **짧은 장애는 알림이 오지 않는다 — 수용한 한계다**(2026-09-13 결정). firing이 Alertmanager 첫 발송 대기(`group_wait: 30s`)보다
  먼저 끝나면 발송되지 않는다. 장애가 있었는지는 알림이 아니라 카운터·대시보드·로그·Prometheus `ALERTS` 이력으로 확인한다.
- **firing이 왔을 때 Redis는 이미 살아 있을 수 있고, 앱도 회복했을 수 있다.** 90초 세 실행에서 수신은 모두 Redis 응답 뒤였고,
  앱 경로 회복보다 뒤였던 실행이 1회 있었다. 알림을 받으면 §1·§3 명령으로 **지금** 상태를 본다.
  Alertmanager 대기 시간은 바꾸지 않기로 했다.
- **resolved 알림은 해제보다 최대 5분 늦게 온다**(`group_interval: 5m`). 복구 판단은 알림이 아니라 §3의 확인 명령으로 한다.
- **요청이 없으면 울리지 않는다.** Redis가 죽어 있어도 Redis를 쓰는 요청이 없으면 카운터가 0에 머문다.
- "어디서" 막혔는지는 경로별 카운터(`telemetry_*_total{route}`)로 본다. 알림은 합계만 본다.

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
