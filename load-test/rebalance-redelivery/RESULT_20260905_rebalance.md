# 리밸런싱 구간 재전달 — 실측 (2026-09-05)

| 항목 | 값 |
| --- | --- |
| **검증 상태** | **부분 검증** — 1회 실행 |
| 적용 범위 | 단일 Docker Compose, 100대 / 0.2초 / 이상률 0.3, `anomaly-detector` 3 → 1 → 3 |
| 코드 상태 | `4afaf23`로 커밋됨 |
| 실행 명령 | `bash load-test/rebalance-redelivery/run_scenario.sh` |
| 환경 | [`docs/verification/2026-09-05-environment.md`](../../docs/verification/2026-09-05-environment.md) |
| 원본 증거 | **보존하지 않음** — 아래 "이 문서의 한계" 참고 |

> **1회 실행이다.** "재전달 40건(0.100%)"은 리밸런싱 시점의 in-flight 양에 좌우되는
> 값이라 **반복하면 달라진다.** 이 문서가 보이는 것은 "재전달이 생기지만 행은 안 는다"는
> 불변식이지, 40이라는 수가 아니다.

우선순위 1번의 마지막 남은 갈래. 강제 종료(`docker kill`) 시 재전달은 이미 쟀지만
(`load-test/storage-integrity/RESULT_20260904_kill_redelivery.md`),
**다중 인스턴스가 붙었다 떨어질 때** 도는 리밸런싱 구간은 안 쟀다.
실제 운영에서 훨씬 흔한 쪽은 이쪽이다 — 배포, 스케일 조정, OOM kill이 전부
리밸런싱을 만든다.

**결론: 재전달은 0.100%(40건/39,864) 발생했고, PostgreSQL 행은 하나도 늘지 않았다.**

절차: `bash load-test/rebalance-redelivery/run_scenario.sh`
(100대 / 0.2초 / 이상률 0.3, `anomaly-detector` 3 → 1 → 3)

---

## 왜 `anomaly-detector`인가

이 프로젝트에서 **실제로 여러 인스턴스로 도는 유일한 컨슈머 그룹**이다
(`replicas: 3`, ADR-016). 파티션 3개에 1:1로 배정돼 있어 인스턴스 수를 바꾸면
반드시 리밸런싱이 돈다.

정답 기준은 **토픽의 고유 `event_id` 수**다. 재전달이 생기면 같은 원본 메시지가
다시 처리되어 **같은 event_id가 여러 번 발행**되므로,
`토픽 메시지 수 − 고유 event_id 수`가 곧 재전달량이다.

## 결과

| 항목 | 값 |
| --- | ---: |
| Kafka `vehicle-anomaly-alerts` 메시지 | 39,864 |
| 고유 `event_id` (= 정답 기준) | 39,824 |
| **재전달** | **40건 (0.100%)** |
| PostgreSQL 행 | **39,824** |
| `telemetry_anomaly_stored{result="new"}` | 39,824 |
| `telemetry_anomaly_stored{result="duplicate"}` | **40** |
| DLQ | 0 |
| 내용이 다른데 event_id 같음(뭉개짐) | 0 |

**세 값이 독립적으로 일치한다** — 토픽 초과분(40), 중복 event_id 발행분(40),
그리고 오늘 새로 넣은 `duplicate` 지표(40). 서로 다른 출처라 교차 검증이 된다.

행 수(39,824)가 고유 event_id 수와 정확히 같다 — **재전달이 행을 하나도 늘리지
않았고, 유실도 없다.** 어제 넣은 `UNIQUE(event_id)` + `ON CONFLICT DO NOTHING`이
DLQ 재처리뿐 아니라 리밸런싱 재전달에도 그대로 작동한다는 뜻이다.

재전달분이 알림을 다시 밀어내지도 않았다 — 오늘 고친 "insert 성공에만 브로드캐스트"가
이 40건을 걸렀다(ADR-020). 그 수정이 없었다면 배포할 때마다 사용자에게 중복 알림이
갔을 것이다.

리밸런싱은 실제로 돌았다(`Revoking previously assigned partitions`,
`is rebalancing; rejoining` 등 로그 29건).

## 측정 중 틀린 것 — 리밸런싱 검증 grep

처음엔 결과에 "리밸런싱 로그 건수 0"이 찍혔다. 그대로 믿었으면 "리밸런싱이 안 돌아서
재전달 40건은 다른 원인"이라는 잘못된 결론을 냈을 것이다.

원인은 grep 문구였다. `anomaly-detector`는 **kafka-python**이라
`Revoking previously assigned partitions`인데, Java 클라이언트 문구
(`Revoke previously assigned partitions`)로 찾고 있었다. 고쳤다.

## 남은 것

- **스케일 다운으로 사라진 인스턴스의 로그는 함께 사라진다.** 리밸런싱 횟수는
  남아 있는 인스턴스 기준의 **하한**이다.
- **재전달률은 리밸런싱 시점의 in-flight 양에 비례한다.** 이번엔 커밋 배치가
  100건/5초(ADR-017)인 상태에서 0.1%가 나왔다. 배치를 키우면 재전달도 늘어난다 —
  그 관계는 재지 않았다.
- **저장 경로(Java, `telemetry-storage-group`)의 리밸런싱은 이번 범위 밖이다.**
  단일 백엔드 프로세스 안의 concurrency 3이라 인스턴스를 늘려야 재현되는데,
  `container_name`과 포트 바인딩이 고정돼 있어 스케일이 안 된다.
- **드레인이 13분 걸렸다**(12:05 정지 → 12:18 lag 0). `anomaly-storage-group`이
  레코드 단위 리스너 + `MANUAL_IMMEDIATE`이라 느리다. PostgreSQL 장애 실험에서도
  같은 현상을 봤다(`load-test/anomaly-dlq-idempotency/`). **이후 배치화했고
  49 msg/s → 유입 전량이 됐다**(`../anomaly-storage-throughput/`, ADR-022).

## 이 문서의 한계

- **1회 실행이다.** 재전달량은 리밸런싱 시점의 in-flight에 좌우되므로 40이라는 수는
  재현되지 않는다. `docs/roadmap.md` P0-2가 이 실험의 3회 반복을 요구한다.
- **원본 로그를 보존하지 않았다.** `_result.txt`를 요약 후 지웠다
  (`docs/evidence-policy.md` P0-1 위반).
- **리밸런싱 횟수는 하한이다.** 스케일 다운으로 사라진 인스턴스의 로그도 함께 사라진다.
- 단일 머신, 단일 백엔드 인스턴스 결과다.
