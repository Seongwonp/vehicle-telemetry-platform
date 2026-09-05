# 독성 메시지 격리 경계 — 실측 (2026-09-06)

| 항목 | 값 |
| --- | --- |
| **검증 상태** | **부분 검증** — 유형별 1회 실행 |
| 적용 범위 | 단일 Docker Compose, 배경 부하 없음, 유형당 정상 레코드 100건 + 독성 1건 |
| 코드 상태 | `a17daed` + 작업 트리 변경 |
| 실행 명령 | `bash load-test/poison-message/run_scenario.sh 100` (유형 한정: `TYPES=infinity ...`) |
| 환경 | [`docs/verification/2026-09-05-environment.md`](../../docs/verification/2026-09-05-environment.md) |
| 원본 증거 | `evidence/20260906-001443`(5유형), `-002231`(infinity 재실행) |

`docs/roadmap.md` P1-2. 질문은 하나다 — **한 건의 처리 불가능한 메시지가 정상 메시지까지
막는가.**

저장 경로는 배치 리스너다. 역직렬화와 포인트 변환은 레코드별 `try/catch`로 감싸져
있지만, **`saveAll()` 단계에서 실패하는 독성은 그 방어를 통과한다.** 그 경계를 민다.

---

## 설계 — 대조군이 핵심이다

독성 1건마다 **같은 파티션 키의 정상 레코드 100건**을 함께 넣고, 가운데에 독성을 끼운다.
나중에 InfluxDB에 그 100건이 몇 개 남았는지로 "정상 레코드가 함께 막혔는가"를 센다.
대조군이 없으면 "독성이 DLQ로 갔다"까지만 알 수 있는데, 그건 이 실험의 질문이 아니다.

MQTT 단계는 이미 JSON·Bean Validation·타임스탬프·토픽 일치를 검사해 거르므로
(`MqttMessageHandler`), 그 뒤 단계를 보려면 **Kafka 토픽에 직접 넣어야 한다.**
실제로도 프로듀서 버그나 스키마 변경으로 이런 레코드가 들어올 수 있다.

## 결과

| 유형 | 대조군 저장 | 독성의 종착지 | 격리 |
| --- | ---: | --- | --- |
| `malformed_json` | 100 / 100 | DLQ (`JsonParseException`) | ✅ |
| `bad_timestamp` | 100 / 100 | DLQ (`DateTimeParseException`) | ✅ |
| `wrong_schema` | 100 / 100 | DLQ (`NullPointerException`) | ✅ |
| `huge_payload` (약 300KB) | 101 / 100 | **정상 저장됨** — 독성이 아니었다 | — |
| `infinity` (`1e309`) | 100 / 100 | **저장됐지만 `speed` 필드가 사라졌다** | ⚠ 아래 |

**정상 레코드가 함께 막힌 유형은 없다.** 배치 안의 레코드별 격리가 의도대로 동작한다.

DLQ 4건은 전부 `permanent`로 분류됐고, `x-dlq-*` 헤더로 원본 토픽·파티션·offset과
실패 원인·메시지가 추적된다(`evidence/*/dlq-inspect.txt`).

## 발견 — Infinity는 조용히 필드만 사라진다

`{"speed": 1e309}`는 **유효한 JSON**이고 Jackson이 `Double.POSITIVE_INFINITY`로 파싱한다.
InfluxDB 라인 프로토콜에 Infinity는 쓸 수 없으니 쓰기가 실패할 것으로 예상했다.

실제로는 **쓰기가 성공했고, `speed` 필드만 빠진 채 저장됐다.**

```
POISON-INFINITY 의 필드별 행 수
  speed        100   ← 독성 레코드에는 없다
  rpm          101
  engine_temp  101
```

- DLQ 0건, `telemetry_influx_write_failures` 0, 에러 로그 없음
- 어떤 카운터로도 드러나지 않는다 — `points.written`도 정상적으로 올라간다

즉 **레코드는 남았는데 값 하나가 조용히 사라지는 부분 유실**이다.
정합성 대조(토픽 수 = 행 수)로도 안 잡힌다. 행은 있기 때문이다.

영향 범위는 좁다 — 현재 발행 측(시뮬레이터·실제 동글)이 Infinity를 만들 경로가 없고,
MQTT 경로로 들어오면 Bean Validation 이전에 Jackson이 통과시키더라도 이 실험처럼
저장까지 간다. **재현은 됐지만 실제 발생 가능성은 확인하지 않았다.**

## 측정 도구가 처음에 틀렸다

첫 실행에서 `infinity`가 `JsonParseException`으로 DLQ에 갔다 — 의도한 `saveAll` 단계까지
가지도 못했다. 원인은 도구 쪽이었다: Python `json.dumps`가 `float('inf')`를
`Infinity`라는 **비표준 리터럴**로 쓰는데, Jackson은 그걸 역직렬화 단계에서 거부한다.

JSON을 손으로 만들어 `1e309` 리터럴을 넣고서야 의도한 경로를 탔다.
**"DLQ에 갔으니 격리가 잘 된다"로 끝냈으면 위 발견을 못 봤다.**

## 남은 것

- **재처리 횟수 초과는 이번에 재지 않았다.** `--max-replays 3` 상한은 2026-09-04에
  텔레메트리 DLQ로 확인했지만(`docs/runbook/dlq-reprocessing.md`), 이번에 만든
  `permanent` 레코드로 다시 밀어보지는 않았다.
- **Infinity 부분 유실을 막을지 정하지 않았다.** `toPoint()`에서 유한값 검사를 넣으면
  레코드별 DLQ로 보낼 수 있지만, 발생 가능성을 확인하지 않은 상태에서 검사 비용을
  모든 메시지에 물리는 것이 맞는지 판단하지 않았다.
- **유형별 1회 실행이다.**
- MQTT 단계(브로커 `max_packet_size` 초과 등)의 경계는 이번 범위 밖이다.
