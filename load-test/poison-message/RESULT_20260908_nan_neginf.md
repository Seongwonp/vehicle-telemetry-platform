# NaN·-Infinity 주입 — 하나는 검사에 걸리고, 하나는 검사까지 오지도 않는다 (2026-09-08)

| 항목 | 값 |
| --- | --- |
| **검증 상태** | **부분 검증(각 조건 1회)** |
| 적용 범위 | dev(평문) 프로파일, 파티션 3, 유형당 대조군 100건 |
| 코드 상태 | `9db93bf` + 작업 트리(`inject_poison.py`에 두 유형 추가) |
| 실행 명령 | `TYPES="negative_infinity nan" bash load-test/poison-message/run_scenario.sh 100` |
| 원본 증거 | `evidence/20260908-212816/` (COMPLETE) |
| 성공 기준 | 독성 1건이 같은 배치의 정상 레코드를 막지 않는다 |

2026-09-06 독성 실험에서 `speed: 1e309`(양의 Infinity)만 봤다. 그때 남긴 미검증이
**NaN과 음의 Infinity**였다. 부호가 다르면 다른 경로를 타는지, NaN이 애초에 들어오기는
하는지 확인한 적이 없었다.

## 결과

| 유형 | 대조군 저장/주입 | 독성의 종착지 | 어디서 걸렸나 |
| --- | ---: | --- | --- |
| `negative_infinity` (`speed: -1e309`) | **100 / 100** | DLQ 1건 (`permanent`) | `toPoint()`의 `finite()` 검사 |
| `nan` (`speed: NaN`) | **100 / 100** | DLQ 1건 (`permanent`) | **역직렬화** — `finite()`에 도달 못 함 |

토픽 202건(독성 2 + 대조군 200), DLQ **2건**, `unknown` 분류 **0건**.
**정상 레코드가 함께 막힌 유형은 없다.**

### 1. `-Infinity`는 예상대로 `finite()`에 걸린다

```
[org.springframework.dao.InvalidDataAccessApiUsageException]
  key      : POISON-NEGATIVE-INF
  원인 메시지: InfluxDB에 쓸 수 없는 값 — speed=-Infinity (유한하지 않은 값은 필드가 조용히 사라진다)
```

양수 쪽과 **같은 예외·같은 분류**다. 2026-09-06에 양의 Infinity로 이 검사를 넣으면서
`Double.isFinite()`를 썼으니 부호와 무관한 게 맞는데, **그건 코드를 읽은 추론이었고
이번에 실측이 됐다.** DLQ 분류가 `permanent`로 나오는 것도 그대로다(그때 `unknown`으로
빠지던 것을 고쳤고, 음수 쪽도 같은 예외 클래스라 회귀가 없다).

### 2. NaN은 `finite()`까지 오지도 않는다

```
[com.fasterxml.jackson.core.JsonParseException]
  key      : POISON-NAN
  원인 메시지: Non-standard token 'NaN': enable `JsonReadFeature.ALLOW_NON_NUMERIC_NUMBERS` to allow
```

**`NaN`은 JSON 리터럴이 아니다.** JSON 스펙에 없는 비표준 확장이고, Spring Boot 기본
`ObjectMapper`는 `ALLOW_NON_NUMERIC_NUMBERS`가 꺼져 있어 **역직렬화 단계에서 거부**한다.
그래서 `malformed_json`과 같은 경로(레코드별 격리)로 끝난다.

즉 `finite()`의 `Double.isNaN` 쪽 분기는 **이 입구(Kafka JSON)로는 도달할 수 없다.**
MQTT 경로도 같은 `ObjectMapper`를 쓰므로 마찬가지다.

**"검사가 있으니 안전하다"와 "그 경로로는 들어올 수 없다"는 다른 말이다.** 어느 쪽인지
모르면 나중에 누가 `finite()`를 정리하려 할 때 판단할 근거가 없다. 지금은 이렇게 적어둔다 —
NaN 분기는 **JSON 입구에 대해서는 잉여**이고, 산술로 NaN이 생기는 경로가 새로 들어오면
그때 유효해진다.

## 이 문서의 한계

- **각 조건 1회**다. 다만 판정이 예외 클래스로 갈리는 결정적 동작이라 회차 변동 요소가 적다.
- `ALLOW_NON_NUMERIC_NUMBERS`를 켰을 때 NaN이 `finite()`에 걸리는지는 **확인하지 않았다**
  (켤 이유가 없어서 켜보지 않았다). 켜면 이 결론이 바뀐다.
- MQTT 경로로는 주입하지 않았다. 같은 `ObjectMapper`를 쓴다는 코드 근거만 있다.
- `rpm`은 정수 필드라 별도 경로일 수 있는데 **안 봤다.** 이번엔 `speed`(double)만 넣었다.
- 파티션 3, 부하 없는 상태다. 배치가 크게 묶일 때도 같은지는 안 봤다.
