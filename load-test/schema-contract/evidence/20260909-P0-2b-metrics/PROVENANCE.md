# 손으로 만든 증거 — 실행 스크립트가 없다

dev 스택을 띄우고 네 건을 주입한 뒤 Prometheus에 직접 물어서 만든 기록이다.

| | |
| --- | --- |
| 조건 | dev(평문), backend 1 + 감지기 3, ML 비활성 |
| 주입 | MQTT 2건(필드 누락·필드명 오타), Kafka 2건(범위 밖·깨진 JSON) |
| 확인 | `sum by (entrance, reason)`로 **세 입구가 구분**되는지, DLQ 단계 4종이 각각 잡히는지 |
| 무결성 | 이 디렉터리의 `checksums.txt` |

**재현 조건**: 같은 스택에 같은 네 건을 주입하면 같은 집계가 나온다. 다만 카운터는
프로세스 수명 동안 누적되므로 **재기동 없이 다시 주입하면 값이 는다** —
그것 자체가 "시도 횟수이지 고유 메시지 수가 아니다"의 실물이다.

카운터 증가 **위치**는 단위 테스트가 고정한다 —
`ContractMetricsTest`, `TelemetryConsumerTest`(지표_*), `MqttMessageHandlerTest`(계약위반_*),
`anomaly-detector/tests/test_consume_loop.py`(test_지표_*).
