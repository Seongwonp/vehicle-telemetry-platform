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

| 읽는 칸 | 테스트 |
| --- | --- |
| `storage` | `backend/src/test/java/com/telemetry/domain/SharedFixtureContractTest.java` |
| `detector` | `anomaly-detector/tests/test_shared_fixtures.py` |

**정책이 확정됐다**(2026-09-09) — 공통 입력 계약을 유지한다. 그래서 `storage`와
`detector`의 **거부 여부가 일치해야 한다**: `storage: reject`면 `detector: dlq`다.
적용 전에는 21칸 중 8칸이 갈렸다.

`detector_before` 칸은 **정책 적용 전** 동작이다. **지우지 마라** — 무엇이 어떻게
바뀌었는지가 파일 안에 남아야 하고, 결과 문서와 devlog가 이걸 참조한다.

동작을 바꾸면 테스트가 깨진다. 그때 **테스트를 지우지 말고 이 파일의 기대값을 옮겨라.**

**Gradle 주의**: 이 파일은 `backend/` 밖이라 `build.gradle`에 테스트 입력으로 선언해뒀다.
그 선언이 없으면 이 파일을 고쳐도 `:test UP-TO-DATE`로 건너뛴다 — 실제로 겪었다.

**`inputs.file`이 아니라 `inputs.files`여야 한다.** `backend/Dockerfile`은 컨텍스트가
`./backend`라 저장소 루트가 아예 없다. `inputs.file`은 `optional(true)`를 줘도
**파일이 없으면 태스크 설정 단계에서 실패**해서 CI 이미지 빌드를 깨뜨렸다.
그리고 그 환경에서는 `SharedFixtureContractTest`가 **skip**된다 —
fixture가 지워진 경우(루트는 있는데 파일이 없음)와는 갈라서 처리한다.
