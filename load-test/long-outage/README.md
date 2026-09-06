# 장기 의존성 장애 — retry / rebalance / DLQ 경계

```bash
bash load-test/long-outage/run_scenario.sh influxdb 720   # 저장 경로(즉시 실패형 의존성)
bash load-test/long-outage/run_scenario.sh postgres 720   # 알림 경로(30초 블로킹형 의존성)

SAMPLE_SEC=15 VEHICLES=100 bash load-test/long-outage/run_scenario.sh influxdb 900
```

## 무엇을 재나

**장애가 재시도 예산을 넘겨 계속되면 무엇이 먼저 무너지는가.**

지금까지의 장애 실험은 전부 90~300초였고 전부 "유실 0"으로 끝났다. 그래서
`telemetry.kafka.retry.budget-ms`(180초)가 **실제로 소진되는 모습을 한 번도 못 봤다.**
2026-09-05에 확인한 대로 이 예산은 벽시계가 아니라 **백오프로 쉰 시간의 합**이라,
의존성이 어떻게 실패하느냐에 따라 실효 내성이 달라진다.

| 의존성 | 실패 방식 | 예상 실효 내성 |
| --- | --- | --- |
| InfluxDB | 컨테이너가 없으면 **즉시 연결 거부** — 실패에 시간이 안 든다 | 백오프 합 ≈ 벽시계, 약 3분 |
| PostgreSQL | HikariCP `connectionTimeout` 30초를 시도마다 기다린다 | 그 시간은 예산에 안 세므로 약 8분 |

**같은 180초 설정이 의존성에 따라 3분과 8분이 된다**는 게 지금까지의 서술이고,
그 너머는 아무도 안 봤다. 두 시나리오를 같은 장애 길이로 돌려 그 차이를 직접 본다.

## 결과물은 최종 수치가 아니라 시계열이다

질문이 "언제 무너졌나"라서, 30초마다 한 줄씩 `evidence/<run-id>/timeline.csv`에 남긴다.

```
t_sec,phase,group_state,members,lag,dlq_offset,src_offset,
influx_write_failures,dlq_published,rebalance_log,backoff_exhausted
```

여기서 뽑는 값 넷:

1. **재시도 예산이 소진되기 시작한 t** (`backoff_exhausted`가 처음 0이 아니게 된 시점)
2. **DLQ가 늘기 시작한 t** (`dlq_offset`이 기준선을 넘은 시점)
3. **리밸런싱 발생 여부** — 재시도 중 poll을 못 하면 `max.poll.interval.ms`(300초)를
   넘겨 컨슈머가 그룹에서 쫓겨난다. 정적 멤버십(`group.instance.id`)이 이걸 막아준다는
   것은 12시간 soak 사고 때의 **추정이지 측정이 아니다**
4. **복구 시간** — 복구 후 lag이 0으로 돌아오는 데 걸린 시간

## 주의

- **`down -v`로 시작한다** (스크립트가 직접 한다).
- 리밸런싱 로그는 **그룹별로** 센다. 백엔드 한 프로세스에 컨슈머가 둘이라 전체를 세면
  다른 경로의 리밸런싱이 섞인다. 문구는 Java 클라이언트 기준이다 —
  kafka-python 문구로 찾으면 0이 나온다.
- Kafka 조회 네 개를 **한 번의 `docker exec`으로 묶는다.** 따로 부르면 회당 3~5초씩
  들어 30초 간격 안에 안 들어오고, 그러면 시계열의 `t`가 실제 시각과 어긋난다.
- 부하 정지는 `SIGKILL`이 아니라 `SIGTERM`이다. 시뮬레이터 최종 집계를 읽어야 한다.
- `postgres` 시나리오는 이상 알림이 충분히 나와야 하므로 `ANOMALY_RATE=0.3`으로 띄운다.
