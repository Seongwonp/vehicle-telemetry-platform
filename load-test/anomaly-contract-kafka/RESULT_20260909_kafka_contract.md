# 감지 경로의 offset·격리·복구 — **실제 Kafka** 검증 (P0-2a 항목 2)

| 항목 | 값 |
| --- | --- |
| 검증 상태 | **부분 검증 (각 시나리오 1회)** — `docs/evidence-policy.md` 기준 "그 조건에서 관찰됨" |
| 적용 범위 | dev(평문) 프로파일, 감지기 단일 프로세스, ML 비활성, 파티션 1 |
| 실행 명령 | `bash load-test/anomaly-contract-kafka/run_integration.sh` |
| 원본 | `evidence/20260909-223547/` |
| 판정 | **PASS 4 / FAIL 0** |

## 왜 따로 쟀나 — 단위 테스트가 증명하지 못하는 것

`anomaly-detector/tests/test_consume_loop.py`는 **운영 소비 루프를 실행한 단위 테스트**다.
`main()`을 그대로 돌리므로 **분기·예외 전파·커밋 호출 방식**은 진짜다.

**그런데 consumer와 producer가 가짜다.** 브로커가 실제로 offset을 어떻게 커밋하는지,
재전달이 진짜 일어나는지는 거기서 증명되지 않는다.
이 문서는 그 간격을 닫는다 — **committed offset을 브로커에게 물어본다**(`consumer.committed(tp)`).

## 격리

시나리오마다 **전용 토픽과 전용 Consumer Group**을 쓴다(`itc-<run>-<slot>-*`).
운영 토픽·그룹은 건드리지 않고, **volume도 지우지 않는다**(`down -v` 미사용).
끝나면 전용 토픽만 지운다.

**s2와 s3은 일부러 같은 슬롯을 쓴다** — s3이 "s2의 발행 실패를 제거하고 재시작"이라
상태를 이어받아야 의미가 있다.

## 결과

| # | 시나리오 | 확인한 것 | 결과 |
| --- | --- | --- | :---: |
| s1 | 정상 / 계약 위반 / 정상 | committed offset **3**, DLQ 1건, 원본 offset 1, **원본 바이트 보존**, 사유 `PAYLOAD_VALIDATION_FAILED` | PASS |
| s2 | **클라이언트 크기 제한으로 DLQ 발행 실패를 주입** | 예외 전파, **committed 없음** — 실패 레코드를 넘어서지 않았다 | PASS |
| s3 | 발행 실패를 제거하고 재시작 | 재전달되어 격리 완료, committed **3**, DLQ 원본 offset `['1']` | PASS |
| s4 | **알림 발행 실패 → 원본 DLQ 격리 성공 → offset 진행 → 명시적 재처리** | 재처리에서 **고유 event_id 1개(동일)** | PASS |

## 발행 실패를 어떻게 주입했나 — 그리고 무엇을 검증하지 **않았나**

`max_request_size=1`은 **클라이언트 쪽 크기 제한**이다. `send()`가 브로커에 닿기도 전에
`MessageSizeTooLargeError`를 즉시 낸다. **발행이 확실히 실패한 상태**를 만든다.

**이 실험이 검증한 것**: 발행이 실패했을 때 **실제 Kafka의 committed offset과 재전달이
어떻게 되는가.**

**검증하지 않은 것** — 이렇게 읽으면 안 된다:

- 브로커 장애·네트워크 단절
- **timeout 후 복구**
- **발행 성공 여부가 불확실한 상황**(브로커가 받았는지 모르는 경우).
  여기서는 **안 받은 것이 확실**하다. in-doubt 상태는 재시도·중복 판단이 전혀 다르다 —
  PostgreSQL 쪽에서 그 상태를 실제로 겪은 기록이 있다
  (`load-test/anomaly-dlq-idempotency/RESULT_20260905_alert_replay.md`의 in-doubt 3건:
  서버 커밋은 끝났는데 연결이 끊긴 경우).

처음에는 도달 불가 브로커로 향하게 해봤는데 `KafkaProducer` **생성자가 부트스트랩에서
5분씩 블록**해서(`max_block_ms`는 `send()`에만 걸린다) 이 방식으로 바꿨다.
**더 약한 주입이라는 것을 알고 고른 것**이고, 그 대가가 위 세 줄이다.

## s4의 실제 순서 — 자동 복구가 아니다

**알림 발행 실패 → 원본 DLQ 격리 성공 → offset 진행 → 명시적 재처리.**

감지기에는 **재시도가 없다**(`docs/anomaly-path-contract.md` 2절). 알림 발행이 실패하면
그 레코드는 **원본이 `vehicle-telemetry-anomaly-dlq`로 격리**되고, 격리에 성공했으므로
offset은 진행한다. **그 시점에 알림은 나가지 않는다.**

그 뒤 **사람이 원본을 다시 흘려보내야** 알림이 나간다. 실제 절차는:

```bash
docker run --rm --network vehicle-telemetry-platform_telemetry-net \
  -v "$PWD/dlq-tools:/w" -w /w vehicle-telemetry-platform-anomaly-detector \
  python dlq.py --topic vehicle-telemetry-anomaly-dlq replay --target vehicle-telemetry --execute
```

분류·판단 절차는 `docs/runbook/dlq-reprocessing.md` 2-2절.
**이 시나리오는 같은 원본을 다시 주입하는 것으로 그 재처리를 흉내낸다** —
`dlq.py replay` 자체를 돌린 것은 아니다.

### 확인한 것 — 중복 방지의 **전제**

"`UNIQUE(event_id)`가 막는다"는 주장은 **같은 원본이 같은 event_id를 만든다**는 전제
위에서만 성립한다. 매번 새 ID면 UNIQUE 인덱스는 아무것도 막지 못한다.

같은 원본을 두 번 처리했더니 **고유 event_id가 1개**였다.
`event_id`는 `vehicle_id|timestamp|anomaly_type|field|detector`의 SHA-256이고
**`detected_at`은 키에 없다** — 그래서 결정적이다.
Java `AnomalyService.resolveEventId`도 **같은 다섯 필드·같은 순서**로 재계산한다.

**여기서 확인한 범위는 "같은 원본 재처리에서 event_id가 동일하다"까지다.**
그 전제 위에서 기존 증거가 이어진다:

| 무엇 | 어디 |
| --- | --- |
| 재처리해도 행이 안 는다(같은 DLQ 2회 되돌림, 행 증가 0) | `load-test/anomaly-dlq-idempotency/RESULT_20260905_alert_replay.md` |
| 중복 알림 브로드캐스트 차단 | `TelemetryConsumerTest.consumeAnomalyAlerts_중복이면_브로드캐스트안함` |
| 키 구성이 양쪽에서 같다 | `test_consume_loop.py.test_event_id는_원본_필드만으로_결정된다` |
| 재처리 시 동일 ID | `test_consume_loop.py.test_같은_레코드를_다시_처리하면_event_id가_같다` + 이 문서 s4 |

## 만들면서 틀린 것 둘

**1) 시나리오가 토픽·그룹을 공유했다.** 첫 실행에서 s2·s3이 FAIL로 나왔는데,
**동작은 옳았고 판정이 틀렸다** — s1이 남긴 committed offset(3)을 s2가 물려받아
"진행하지 않음"을 지켰는데도 3으로 읽혔다. 슬롯을 나눴다.
(`evidence/20260909-222856/INVALID.md`)

**2) 드라이버의 알림 producer에 직렬화기가 없었다.** 운영 `make_producer()`는
`value_serializer`/`key_serializer`를 쓰는데 내가 뺐더니 dict를 보내다 `TypeError`가 났고,
그게 "알림 발행 실패"로 잡혀 **엉뚱한 경로를 쟀다**.
실패를 주입하려면 **나머지는 운영과 같아야 한다** — 안 그러면 무엇이 실패한 건지 알 수 없다.
(`evidence/20260909-223211/INVALID.md`)

## 이 문서의 한계

- **각 시나리오 1회**다. 반복하지 않았다.
- **파티션 1, 단일 인스턴스**다. 리밸런싱 중 커밋은 안 봤다.
- **부하가 없다.** 배치가 잘게 쪼개졌을 가능성이 높다.
- **발행 실패는 클라이언트 크기 제한으로 주입했다.** 브로커 장애·timeout 복구·
  발행 성공 여부가 불확실한 상황은 **검증하지 않았다**(위 절).
- **`dlq.py replay`를 실제로 돌리지 않았다.** s4의 재처리는 같은 원본을 다시 주입한
  것이다 — replay 도구 자체의 동작은 `load-test/anomaly-dlq-idempotency/`에 있다.
- **ML은 껐다.** webhook 발행 실패 경로도 안 봤다.
