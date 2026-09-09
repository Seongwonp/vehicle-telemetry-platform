# 언어 중립 계약 fixture

## 왜 파일 하나인가

같은 입력에 대한 판정을 **Java와 Python이 각각 자기 테스트에 적어두면 갈라진다.**
그리고 갈라진 것은 측정에서 안 드러난다 — 양쪽 다 자기 테스트를 통과하기 때문이다.
`docker-compose.yml`의 환경변수를 YAML 앵커 하나로 묶은 것과 같은 이유다(2026-09-07).

그래서 **입력과 기대 판정을 이 파일 하나에** 두고, 양쪽 테스트가 이 파일을 읽는다.
어느 한쪽이 달라지면 그쪽 테스트가 깨진다.

## 이 파일이 정하지 않는 것

**두 경로가 같아야 하는지 자체는 여기서 정하지 않는다.** 역할이 다르므로 정책도
다를 수 있다(`docs/anomaly-path-contract.md`). 이 파일은 각 fixture에 대해
**어느 쪽이 무엇을 해야 하는지를 따로** 적는다 — 같으면 같게, 달라야 하면 다르게.

## 스키마 (`cases.json`)

```
{
  "version": 1,
  "cases": [
    {
      "id":       "안정적인 식별자. 테스트 이름에 쓴다",
      "payload":  "원문 그대로의 문자열. **JSON 객체가 아니라 문자열이다** —
                   `null` 리터럴이나 깨진 JSON도 담아야 하기 때문",
      "storage":  { "verdict": "accept" | "reject", "reason": "<사유 코드>|null" },
      "detector": { "verdict": "alert" | "no_alert" | "dlq" | "UNMEASURED",
                    "note": "왜 그런지 / 무엇이 미확정인지" }
    }
  ]
}
```

`verdict` 값의 뜻:

| 값 | 경로 | 뜻 |
| --- | --- | --- |
| `accept` | 저장 | InfluxDB에 저장된다 |
| `reject` | 저장 | 저장되지 않고 DLQ로 간다. `reason`은 `TelemetryContractException`의 사유 코드 |
| `alert` | 감지 | 이상 알림이 발행된다 |
| `no_alert` | 감지 | **예외 없이** 지나간다 — 알림도 DLQ도 없다 |
| `dlq` | 감지 | 처리 중 예외가 나서 `vehicle-telemetry-anomaly-dlq`로 간다 |
| `UNMEASURED` | 아무거나 | **아직 안 쟀다.** 기대값이 아니라 미측정 표시다 |

## 읽는 쪽

- Java: `backend/src/test/java/com/telemetry/domain/SharedFixtureContractTest.java` (미구현)
- Python: `anomaly-detector/tests/test_shared_fixtures.py` (미구현)

**둘 다 아직 없다.** 이 파일은 P0-2a 조사 단계에서 **현재 동작을 기록하는 용도**로
먼저 만들었다. 구현은 정책을 확정한 뒤다 — `docs/anomaly-path-contract.md`.
