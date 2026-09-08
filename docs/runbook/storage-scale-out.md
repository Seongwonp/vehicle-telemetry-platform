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

> **⚠ 이미 떠 있는 컨슈머는 새 파티션을 바로 못 본다.** 파티션을 늘려도 컨슈머가 들고 있는
> 토픽 메타데이터는 `metadata.max.age.ms`(기본 **5분**)마다 갱신된다. 그 전에 저장
> 인스턴스를 띄우면 **리밸런싱은 도는데 할당은 늘지 않는다** — 새 스레드가 전부 유휴로
> 붙어서 "인스턴스를 늘렸는데 처리량이 그대로"로 보인다.
> 2026-09-08 실측: 파티션 3 → 9로 늘린 뒤 **약 4분 30초 뒤에** 재리밸런싱이 돌면서
> 할당이 3 → 9가 됐다(`docs/verification/2026-09-08-dev-profile-scale.md`).
>
> **그래서 순서가 중요하다.** 스택을 새로 띄운다면 **backend 기동 전에** 파티션을 늘려라
> (그러면 이 대기가 없다). 이미 떠 있는 스택에서 늘렸다면 **최대 5분 기다린 뒤에**
> `status`로 판정하라. 할당된 파티션 합이 멤버 수보다 적으면 아직 기다리는 중이거나
> 파티션이 정말 모자란 것이다.

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
# status·down에도 같은 COMPOSE_FILES를 넘겨야 한다(2026-09-08에 이 경로로 실기동 확인).
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
(dev 프로파일 3회 실측 46/48/47초, mTLS 1회 44초). 정적 멤버십이라
컨슈머가 종료해도 그룹을 즉시 떠나지 않고
`session.timeout.ms`(현재 45초 — 아래 "이 45초는 어디에 적혀 있나")를 기다리기 때문이다.

**그 45초 동안 그 인스턴스가 맡던 파티션은 아무도 소비하지 않는다.** 유실은 아니다
(lag으로 쌓였다가 재할당 후 처리된다). 부하가 걸린 상태에서 내리면 **그 파티션의 지연이
그만큼 튄다** — 트래픽이 낮은 시간에 내리는 게 맞다.

> **늘리는 건 싸고 줄이는 건 45초짜리다.** 리밸런싱 폭풍을 피하려고 정적 멤버십을 켠
> 대가이고(12시간 soak test에서 그 폭풍에 갇힌 적이 있다), 지금은 이 트레이드오프를
> 유지한다.

### 부하 중에 내리면 어떻게 보이는가 (실측 3회)

추론이 아니라 재봤다. 파티션 9 · 인스턴스 3 · 프로듀서 2로 부하를 걸고 내렸다.

| 다운 후 경과 | 제거된 인스턴스가 쥔 6개 (평균 lag) | 살아있는 backend의 3개 |
| ---: | ---: | ---: |
| 직전 | 7,137 | 2,244 |
| +20s | 109,408 | **1,812** |
| +38s | 177,952 | 12,891 |
| +43s | 199,501 | **1,324** |
| +46~54s | 재할당 — backend가 전부 인수 | |

**그 6개는 완전히 멈추고, 살아있는 3개는 평평하다.** 격리는 성립하고, 대가는 그 6개다.
정지 구간에 파티션당 약 17만 건(합계 약 100만 건)이 쌓였고 재할당 후 주 backend가
통째로 인수한다. **유실은 0**이다(토픽 총 offset = InfluxDB 행 수, **3회 모두 일치**).

> **⚠ `--describe`로 "주인이 다 있으니 정상"이라고 판단하면 틀린다.**
> 정적 멤버십에서는 **죽은 멤버가 세션 만료까지 소유권을 유지한다.** 내린 직후에도
> 9개 파티션 전부 `CONSUMER-ID`가 채워져 있어 멀쩡해 보인다.
> **소비되고 있는지는 lag이나 `CURRENT-OFFSET` 진행으로 봐야 한다.**
> (이 함정에 실험 계측이 먼저 걸렸다 — 자세한 것은
> `load-test/storage-scale/RESULT_20260908_scaledown_under_load.md`.)

### 이 45초는 어디에 적혀 있나

`backend/src/main/resources/application.yml`의
`spring.kafka.consumer.properties.session.timeout.ms`다. 환경변수
`KAFKA_SESSION_TIMEOUT_MS`로 재빌드 없이 바꿀 수 있다.

**2026-09-08까지는 아무 데도 안 적혀 있었다** — 45초는 Kafka 클라이언트 기본값이었고,
우리는 그 기본값에 기대어 "스케일 다운은 45초짜리"라고 Runbook에 써두고 있었다.
클라이언트를 올리다 기본값이 바뀌면 이 문서가 조용히 틀린 말이 된다. 같은 값을 명시로
고정하고(동작 변화 없음) `KafkaConsumerContractTest`로 회귀를 막았다.

**바꾸기 전에**: 줄이면 스케일 다운이 빨라지는 대신 GC 정지나 네트워크 순단을 죽음으로
오판해 리밸런싱이 는다. 이 프로젝트는 12시간 soak test에서 리밸런싱 폭풍에 갇힌 적이
있고 정적 멤버십은 그걸 피하려고 켠 것이다. **줄이려면 그 대가를 먼저 재라 — 아직 안 쟀다.**
9초 밑으로 내리려면 `heartbeat.interval.ms`(기본 3초, 세션의 1/3 이하 권장)도 같이 봐야 한다.


### 정적 멤버십을 끄면 45초가 9초가 된다 — 그리고 폭풍 위험이 돌아온다

이 45초는 **정적 멤버십의 대가**다. 대조군으로 확인했다(2026-09-09).

| | 정적 멤버십 ON (3회) | OFF (1회) |
| --- | ---: | ---: |
| 멤버 9 → 3 | 다운 후 46 / 52 / 54초 | **다운 후 9초** |
| 동시 정지 파티션 | **6개** | **0개** |
| 유실 | 0 (3/3) | 0 |

끄는 방법 — `GROUP_INSTANCE_ID_BASE`를 **빈 값**으로 준다(Kafka가 빈 문자열을 거부하므로
`KafkaConfig`가 속성 자체를 제거한다). 저장 인스턴스도 같이 꺼야 한다.

```bash
GROUP_INSTANCE_ID_BASE= STORAGE_1_GROUP_INSTANCE_ID= STORAGE_2_GROUP_INSTANCE_ID= docker compose --profile scale up -d
```

기동 로그에 `group.instance.id가 비어 있어 정적 멤버십을 끕니다` 경고가 찍히면 꺼진 것이다.
`kafka-consumer-groups --describe --members` 출력에서 `GROUP-INSTANCE-ID` 컬럼이 사라진다.

> **⚠ 근거 없이 끄지 마라.** 정적 멤버십은 스케일 다운을 느리게 하려고 켠 게 아니라
> **리밸런싱 폭풍을 피하려고** 켠 것이다(12시간 soak test에서 `MemberIdRequiredException →
> group is already rebalancing`에 갇혀 복구가 안 됐다). 끄면 GC 정지나 순단으로
> `max.poll.interval.ms`(5분)를 넘길 때 그 위험이 돌아온다.
> **그 대가는 아직 측정하지 않았다** — 이득만 재고 끄는 것은 반쪽 판단이다.
> 기본값은 **켜짐**이다.
## 4개 이상이 필요하면

`docker-compose.yml`에 `backend-storage-4`를 추가한다 — 앵커를 쓰므로 8줄이고,
`GROUP_INSTANCE_ID_BASE`만 다르게 준다. `scripts/scale-storage.sh`의 `MAX`도 같이 올린다.

## 부하 실험과의 관계

`load-test/storage-scale/run_throughput.sh`와 `run_scenario.sh`는 같은 구성을
**`docker run`으로** 최대 7개까지 띄운다(실험은 compose가 선언한 3개보다 많아야 한다).

이름은 **`telemetry-backend-storage-exp-N`으로 갈라놨다.** 실험 스크립트는 시작할 때
같은 이름을 `docker rm -f`로 지우는데, 이름이 겹쳐 있으면 **실험을 돌리는 순간 compose로
띄운 저장 인스턴스가 지워진다.** 처음엔 겹쳐 있었고, 2026-09-07에 갈랐다.

그래도 **둘은 같은 컨슈머 그룹과 같은 InfluxDB를 쓴다.** 동시에 띄우면 서로의 부하가 된다.
측정할 때는 `scripts/scale-storage.sh down`으로 먼저 내려라.

## 관련 문서

- 왜 이 구조인가: [`docs/architecture-decisions.md`](../architecture-decisions.md) ADR-023(수집/저장 분리), ADR-024(정적 멤버십 끄기)
- 구조 검증 기록: [`docs/verification/2026-09-07-compose-scale-profile.md`](../verification/2026-09-07-compose-scale-profile.md)
- 처리량 측정과 그 한계: `load-test/storage-scale/RESULT_20260907_repeat3.md`
- 관측 지표 읽는 법: [`docs/runbook/pipeline-observability.md`](pipeline-observability.md)
- 부하 중 스케일 다운 실측: `load-test/storage-scale/RESULT_20260908_scaledown_under_load.md`
- 정적 멤버십 대조군: `load-test/storage-scale/RESULT_20260909_static_membership_control.md`
