# Runbook — 저장 경로 수평 확장 (profile `scale`)

이 앱은 **MQTT 수집과 Kafka→InfluxDB 저장을 겸한다.** 저장이 밀릴 때 통째로 복제하면
수집까지 이중화돼서 오히려 망가진다. 그래서 **수집을 끈 저장 전용 인스턴스**를 따로 띄운다.

## 먼저 확인할 것 — 이걸 켜서 해결되는 문제인가

`telemetry-storage-group`의 lag이 계속 오르는데 다음이 모두 참일 때만 의미가 있다.

1. **파티션이 3보다 많다.** `concurrency: 3`이라 backend 하나가 파티션 3개를 다 가져간다.
   파티션이 3이면 인스턴스를 늘려도 **유휴 멤버만 는다.**
2. **InfluxDB 쓰기가 병목이 아니다.** 2026-09-07 실측에서 인스턴스를 늘릴수록
   `telemetry.influx.write` 지연이 늘었다(1개 388ms → 4개 665ms). 지연이 이미 높다면
   인스턴스를 늘려도 **지연만 더 는다.**
3. 호스트에 여유가 있다. 인스턴스 하나가 JVM 하나다.

> **이득의 크기를 미리 말하지 마라.** 같은 조건 3회에서 8 인스턴스가
> 56,985 / 44,279 / 26,564 msg/s로 흔들렸다. **배수는 시스템의 성질이 아니라 부하 조건과
> 그날 호스트 상태의 함수다** — `load-test/storage-scale/RESULT_20260907_repeat3.md`.

## 절차

### 1. 파티션 확인·확장 (되돌릴 수 없다)

```bash
docker exec telemetry-kafka kafka-topics --bootstrap-server localhost:29092 \
  --describe --topic vehicle-telemetry | grep -c "Partition:"
```

부족하면 늘린다. **파티션은 줄일 수 없다.** 그리고 키 해싱이 바뀌므로 같은 `vehicle_id`가
다른 파티션으로 갈 수 있다 — 이 파이프라인은 파티션 내 순서만 보장한다.

```bash
docker exec telemetry-kafka kafka-topics --bootstrap-server localhost:29092 \
  --alter --topic vehicle-telemetry --partitions 9
```

목표: `파티션 수 = 인스턴스 수 × 3`.

### 2. 저장 인스턴스 기동

```bash
bash scripts/scale-storage.sh up 2
```

이 스크립트가 compose 프로파일 `scale`을 켜고 **Prometheus 스크레이프 대상 파일도 같이
쓴다**. 인스턴스만 늘리고 관측을 안 늘리면 늘어난 인스턴스가 일하는지 알 수 없다 —
2026-09-06에 실제로 그 상태로 한참을 봤다.

평문(dev) 스택이면:

```bash
COMPOSE_FILES="-f docker-compose.yml -f docker-compose.dev.yml" bash scripts/scale-storage.sh up 2
```

### 3. 확인 (리밸런싱 후 30~60초)

```bash
bash scripts/scale-storage.sh status
```

봐야 할 것:

| 항목 | 정상 | 아니면 |
| --- | --- | --- |
| 멤버(컨슈머 스레드) | 인스턴스 수 × 3 | 안 늘면 4번 참고 |
| 정적 id 종류 | 멤버 수와 **같다** | 적으면 id가 겹쳐 서로 fencing 한다 |
| 할당된 파티션 합 | 멤버 수와 같다 | 적으면 파티션이 모자라 유휴 멤버가 있다 |
| fencing | 0건 | 1번이라도 있으면 `GROUP_INSTANCE_ID_BASE` 충돌 |
| MQTT 연결 끊김 | 0건 | 저장 인스턴스가 수집을 켰다는 뜻 |
| Prometheus 대상 | 인스턴스마다 `up=1` | 대상 파일·네트워크 확인 |

### 4. 멤버가 안 늘 때

거의 항상 **식별자 충돌**이다. 2026-09-06에 이걸로 막혔다.

- MQTT `client-id`가 같으면(`cleanSession=false`) 브로커가 앞 세션을 끊는다
  → 저장 인스턴스는 `MQTT_INGEST_ENABLED=false`여야 한다.
- Kafka `group.instance.id`가 같으면 정적 멤버가 서로를 밀어낸다
  → 인스턴스마다 `GROUP_INSTANCE_ID_BASE`가 달라야 한다.

두 값은 `docker-compose.yml`의 `backend-storage-*`에 이미 다르게 들어가 있다.
**직접 `docker run`으로 복제할 때 빠뜨리기 쉬운 것이 이 둘이다.**

### 5. 내릴 때 — 즉시 반영되지 않는다

```bash
bash scripts/scale-storage.sh down
```

명령은 몇 초 만에 끝나지만 **컨슈머 그룹이 재할당하는 데 약 45초가 걸린다**
(2026-09-07 실측 44초, 1회). 정적 멤버십이라 컨슈머가 종료해도 그룹을 즉시 떠나지 않고
`session.timeout.ms`(클라이언트 기본 45초)를 기다리기 때문이다.

**그 45초 동안 그 인스턴스가 맡던 파티션은 아무도 소비하지 않는다.** 유실은 아니다
(lag으로 쌓였다가 재할당 후 처리된다). 부하가 걸린 상태에서 내리면 **그 파티션의 지연이
그만큼 튄다** — 트래픽이 낮은 시간에 내리는 게 맞다.

> **늘리는 건 싸고 줄이는 건 45초짜리다.** 리밸런싱 폭풍을 피하려고 정적 멤버십을 켠
> 대가이고(12시간 soak test에서 그 폭풍에 갇힌 적이 있다), 지금은 이 트레이드오프를
> 유지한다.

## 4개 이상이 필요하면

`docker-compose.yml`에 `backend-storage-4`를 추가한다 — 앵커를 쓰므로 8줄이고,
`GROUP_INSTANCE_ID_BASE`만 다르게 준다. `scripts/scale-storage.sh`의 `MAX`도 같이 올린다.

## 부하 실험과 이름이 겹친다

`load-test/storage-scale/run_throughput.sh`는 같은 이름
(`telemetry-backend-storage-N`)으로 최대 7개를 **`docker run`으로** 띄운다. 실험 목적상
compose가 선언한 3개보다 많아야 해서 그대로 뒀다. 실험 스크립트는 시작할 때
`docker rm -f`로 같은 이름을 먼저 지우므로 **실험을 돌리면 compose로 띄운 저장 인스턴스가
사라진다.** 둘을 동시에 쓰지 마라.

## 관련 문서

- 왜 이 구조인가: [`docs/architecture-decisions.md`](../architecture-decisions.md) ADR-023
- 구조 검증 기록: [`docs/verification/2026-09-07-compose-scale-profile.md`](../verification/2026-09-07-compose-scale-profile.md)
- 처리량 측정과 그 한계: `load-test/storage-scale/RESULT_20260907_repeat3.md`
- 관측 지표 읽는 법: [`docs/runbook/pipeline-observability.md`](pipeline-observability.md)
