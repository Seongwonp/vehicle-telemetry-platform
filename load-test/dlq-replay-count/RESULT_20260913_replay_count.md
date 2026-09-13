# 감지기 DLQ ↔ 재처리 왕복에서 `x-dlq-replay-count`가 사라진다 — 재현과 수정

> roadmap 4-b. 실제 Kafka, 전용 토픽·그룹, 각 1회 실행.

## 조건

| | |
| --- | --- |
| 도구 | `bash load-test/dlq-replay-count/run_repro.sh <라벨>` → `load-test/anomaly-contract-kafka/driver.py s5` |
| 격리 | 실행마다 `itc-<run>-s5-{telemetry,dlq,alerts}` 토픽과 `itc-<run>-s5-group`·`-replay` 그룹. **끝나면 이 실행이 만든 것만 삭제** |
| 코드 | 감지기·`dlq.py` 모두 **작업 트리 마운트**(이미지는 의존성만) — 같은 스크립트로 수정 전·후 비교 |
| 실패 주입 | 알림 발행에 `max_request_size=1`(s4와 같음) — 같은 원본이 **매번 같은 이유로** 다시 실패 |
| 재처리 | `dlq.py --max-replays 2 replay --execute --include-unknown`(주입 실패 `MessageSizeTooLargeError`는 분류 `unknown`) |

**기대(계약)**: DLQ replay-count `(없음) → 1 → 2`, 3회차 replay는 `--max-replays 2`로 **차단**.

## 수정 전 — `evidence/20260913-134236/` (감지기 sha256 `a0b382bd…`)

| 회차 | 최신 DLQ replay-count | replay 발행 | 원본 토픽에 되돌린 레코드의 replay-count |
| --- | --- | --- | --- |
| 1 | (없음) | 예 | 1 |
| 2 | **(없음)** | 예 | **1** |
| 3 | **(없음)** | 예 — **차단 안 됨** | **1** |

판정 **FAIL**. 브로커 덤프(`topic_dlq_headers.txt`)의 DLQ 레코드 3건 모두 `x-dlq-replay-count`가 없다.

**헤더가 사라지는 곳**: `dlq.py`는 원본 토픽에 `x-dlq-replay-count=1`을 붙여 보냈다(드라이버가 원본 토픽에서 읽어 확인).
감지기가 그 레코드를 다시 DLQ로 보낼 때(`anomaly_detector.dlq_headers`) **새 헤더 목록만 만들어** 그 값을 버렸다.

**Runbook 재처리 제한에 미치는 영향**: `docs/runbook/dlq-reprocessing.md`는 "이 헤더는 컨슈머가 이어받아야 동작한다 —
**Java·Python 양쪽에 구현돼 있고 회귀 테스트로 고정했다**"고 적었다. **감지기(Python 컨슈머)에는 구현돼 있지 않았다.**
감지기 DLQ에서는 `--max-replays`가 **아무것도 막지 못한다** — 커서(`--group`)가 한 번 실행에서 같은 레코드를 두 번 보내는 것은
막지만, 실행을 되풀이하면 같은 원본이 계속 원본 토픽으로 돌아간다. 발생 조건은 `dlq.py`가 기본으로 영구·불명 실패를
건너뛰어 좁다 — **일시 실패로 분류되는 실패가 반복되거나 `--include-unknown`을 줄 때**다.

## 수정

`anomaly_detector.dlq_headers`가 원본 레코드의 `x-dlq-replay-count`(마지막 값)를 **그대로** 이어 붙인다.
다른 `x-dlq-*` 헤더는 복사하지 않는다 — 이번 실패의 위치·원인을 새로 적어야 한다. Java `TelemetryConsumer.sendToDlq`와 같은 계약이다.
회귀 테스트: `anomaly-detector/tests/test_anomaly_detector.py` — `test_재처리_횟수_헤더를_이어받는다`, `test_재처리_이력이_없으면_횟수_헤더도_없다`.
테스트용 `FakeMessage`에 선택 인자 `headers`를 더했고, 수정 코드는 `headers` 속성이 없는 메시지도 받는다(`getattr`) —
기존 테스트를 고치지 않기 위해서다.

감지기 단위 테스트(작업 트리 마운트, 감지기 이미지의 Python 3.11): **166 passed**, 새 회귀 테스트 2건 포함.
`dlq.py`는 바꾸지 않았다. **DLQ 도구 분류 테스트(`dlq-tools/test_dlq.py`)는 로컬에서 돌리지 않았다** —
두 묶음 모두 커밋 CI의 `Python tests` 단계가 실행하며, 그 결과는 `docs/HANDOFF_2026-09-13.md` §2-5에 남긴다.

## 수정 후 — `evidence/20260913-135253/` (감지기 sha256 `a236cdca…`)

| 회차 | 최신 DLQ replay-count | replay 발행 | 원본 토픽에 되돌린 레코드의 replay-count |
| --- | --- | --- | --- |
| 1 | (없음) | 예 | 1 |
| 2 | **1** | 예 | **2** |
| 3 | **2** | **아니오 — 횟수 초과로 차단** | - |

판정 **PASS**. 브로커 덤프에서 DLQ offset 1·2에 `x-dlq-replay-count:1`·`:2`, 원본 토픽에 되돌린 레코드에 `:1`·`:2`가 있다.

## 이 결과가 말하지 않는 것

- **각 1회.** 실패 주입은 클라이언트 크기 제한 하나뿐이다 — 브로커 장애·timeout 경로의 DLQ 발행에서 같은지는 보지 않았다
  (헤더 구성 코드는 경로와 무관하게 하나다).
- 이미 운영 DLQ에 쌓인, 헤더를 잃은 레코드는 **소급해서 고치지 않는다.** 그 레코드들은 카운트가 0부터 다시 센다.
- Java 저장 경로는 이번에 다시 재지 않았다 — 기존 회귀 테스트(`dlq전송실패_offset미커밋` 계열)와 Runbook 실측에 기댄다.
