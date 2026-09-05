# 독성 메시지 격리 경계

```bash
bash load-test/poison-message/run_scenario.sh 100      # 5유형 전부
TYPES="infinity" bash load-test/poison-message/run_scenario.sh 100   # 한 유형만
python inject_poison.py --list                          # 유형 목록
```

## 무엇을 재나

**한 건의 처리 불가능한 메시지가 정상 메시지까지 막는가.**

저장 경로는 배치 리스너다. 역직렬화와 포인트 변환은 레코드별 `try/catch`로 감싸져
있지만 **`saveAll()` 단계에서 실패하는 독성은 그 방어를 통과한다.** 그 경계를 민다.

## 대조군이 이 실험의 핵심이다

독성 1건마다 **같은 파티션 키의 정상 레코드 N건**을 넣고 가운데에 독성을 끼운다.
InfluxDB에 그 N건이 몇 개 남았는지로 "함께 막혔는가"를 센다.
대조군이 없으면 "독성이 DLQ로 갔다"까지만 알 수 있고, 그건 질문이 아니다.

키를 같게 쓰는 이유는 같은 파티션·같은 poll 배치에 담기게 하기 위해서다.
키가 다르면 파티션이 갈려 배치 경계를 못 본다. 배경 부하(시뮬레이터)도 띄우지 않는다.

## 결과 (2026-09-06)

`RESULT_20260906_poison.md`. 요약: **정상 레코드가 함께 막힌 유형은 없다.**
DLQ 4건은 전부 `permanent`로 분류되고 `x-dlq-*` 헤더로 원본 위치가 추적된다.

다만 **`speed: 1e309`(유효한 JSON)는 저장은 되는데 `speed` 필드만 조용히 사라진다** —
DLQ도 에러도 카운터도 없다. 정합성 대조(토픽 수 = 행 수)로도 안 잡힌다.

## 주의

- **`down -v`로 시작한다.**
- 독성 payload를 만들 때 `json.dumps(float('inf'))`를 쓰지 마라 — `Infinity`라는
  비표준 리터럴이 나가서 Jackson이 역직렬화 단계에서 거부하고, 그러면 의도한
  `saveAll` 경로를 못 본다. `1e309` 리터럴을 손으로 넣어야 한다.
- `huge_payload`(약 300KB)는 독성이 아니다 — Kafka·InfluxDB 모두 통과한다.
  브로커 한계(`max.message.bytes` 1MB)를 넘기면 토픽에 들어가지도 않아 컨슈머 격리를
  볼 수 없으므로 일부러 그 아래로 잡았다.
