# 장기 장애 2회차 — 리밸런싱이 왜 안 도는지 (2026-09-06)

| 항목 | 값 |
| --- | --- |
| **검증 상태** | **부분 검증** — 같은 조건 2회(1회차 `RESULT_20260906_influxdb.md`) |
| 적용 범위 | 단일 Docker Compose(dev, 평문), 50대 × 0.2초, `LISTENER_LOG_LEVEL=DEBUG` |
| 코드 상태 | `015a2fd` + 작업 트리 변경 |
| 실행 명령 | `LISTENER_LOG_LEVEL=DEBUG bash load-test/long-outage/run_scenario.sh influxdb 720` |
| 환경 | [`docs/verification/2026-09-05-environment.md`](../../docs/verification/2026-09-05-environment.md) |
| 원본 증거 | `evidence/20260906-103356/` |

`docs/roadmap.md` P1.5-2. 1회차에서 재시도가 `max.poll.interval.ms`(300초)를 두 배 넘게
초과했는데도 리밸런싱이 0이었다. **정적 멤버십(`group.instance.id`) 덕인지, 리스너가
백오프 동안 컨슈머를 살려두는 것인지 구분하지 못한 채** 닫았다. 그것만 가른다.

---

## 1. 답 — 에러 핸들러가 컨슈머를 살려둔다

리스너 로거를 DEBUG로 올리자 재시도의 주체가 그대로 찍혔다.

```
DEBUG o.s.k.l.FallbackBatchErrorHandler - Retry failed for:
      vehicle-telemetry-0@7578,…,vehicle-telemetry-0@7600
```

같은 offset 묶음이 약 30초 간격(최대 백오프)으로 반복된다. 이 클래스가 무엇을 하는지는
**실제로 쓰이는 jar의 바이트코드로 확인했다**(spring-kafka 3.1.4, Gradle 캐시):

```
FallbackBatchErrorHandler → Consumer.assignment, Consumer.pause, ErrorHandlingUtils.retryBatch
ErrorHandlingUtils.retryBatch 안의 호출:
    Consumer.pause / Consumer.poll ×2 / Consumer.resume ×3
    ConsumerPauseResumeEventPublisher.publishConsumerResumedEvent ×3
poll 호출부는 둘 다 Duration.ZERO다:
    149: getstatic Duration.ZERO   152: invokeinterface Consumer.poll
    323: getstatic Duration.ZERO   326: invokeinterface Consumer.poll
```

즉 **백오프로 쉬는 동안 파티션을 pause한 채 `poll(0)`을 계속 호출한다.** poll이 계속
불리므로 `max.poll.interval.ms`가 만료되지 않는다.

**정적 멤버십이 버텨준 것이 아니다.** 파티션 할당 로그도 기동 시 한 번뿐이고
(`listener-debug-lines.txt`, 6줄) 12분 동안 재할당이 없다.

이것으로 `KafkaConfig.buildBackOff`의 서술을 정정할 수 있다 — "리스너가 오래 붙잡히면
예산과 무관하게 리밸런싱이 돌 수 있다"는 **이 코드 경로에서는 성립하지 않는다.**
단, 아래 한계를 함께 본다.

## 2. 반복 — 같은 조건 2회

| | 1회차 | 2회차 | 차이 |
| --- | ---: | ---: | ---: |
| 첫 DLQ 발생 | t=220초 | **t=222초** | 2초 |
| DLQ 증가 | 6,158 | **6,185** | 27 (0.4%) |
| 리밸런싱 | 0 | **0** | — |
| 유실(토픽 − 저장 − DLQ) | −47 | **−23** | in-doubt 건수 차이 |
| 복구 후 lag 0 | 부하 정지 +10초 | **+9초** | 1초 |
| 토픽 총량 | 258,726 | 264,661 | 2.3% |

**첫 DLQ 발생 시점(220 / 222초)이 거의 같다.** 이 값은 백오프 합(180초)에 실패 시간이
더해진 것이라 부하와 무관하게 재현되는 성질로 보인다 — 다만 2회다.

DLQ 건수(6,158 / 6,185)도 0.4% 안에서 같다. 둘 다 `3 컨슈머 × max.poll.records 2,000`에
장애 시작 시점의 잔여 배치가 더해진 값이다.

**in-doubt 건수만 흔들린다**(47 → 23). `docker stop` 순간 서버까지 닿은 쓰기가 몇 건이냐는
타이밍 문제라 흔들리는 게 자연스럽다. 이 값을 성질처럼 인용하면 안 된다.

## 3. 이 문서의 한계

- **정적 멤버십을 끈 대조군은 돌리지 않았다.** 바이트코드가 pause-and-poll을 보여주므로
  "정적 멤버십이 없어도 리밸런싱이 안 돈다"가 유력하지만, **직접 확인한 것은 아니다.**
  가르려면 `group.instance.id`를 빼고 같은 장애를 돌려야 하는데, 빈 값을 주면 Kafka가
  설정 자체를 거부해서 스크립트 밖의 코드 변경이 필요하다 — 이번 범위에서 뺐다.
- 2회다. 3회는 아니다.
- `poll(0)`이 불린다는 것은 바이트코드로 확인했지만, **실제로 몇 번 불렸는지는 세지
  않았다**(그 로그는 DEBUG에도 안 남는다).
- 이 결론은 **배치 리스너 + 배치 전체 실패** 경로에 한정된다. 레코드 단위 실패
  (`BatchListenerFailedException`)는 다른 경로이고 이번에 보지 않았다.
