# 손으로 만든 증거 — 실행 스크립트가 없다

`run_e2e.sh`처럼 스크립트가 남긴 것이 아니라, dev 스택을 띄우고 6건을 주입한 뒤
Prometheus와 Kafka에 직접 물어서 만든 기록이다.

| | |
| --- | --- |
| 조건 | dev(평문) 프로파일, 감지기 3 인스턴스(`replicas: 3`), ML 비활성 |
| 주입 | Kafka `vehicle-telemetry`에 직접 6건 (정상 1, 계약 위반 4종, 이상값 1) |
| 확인 | Prometheus `sum by (reason)`, 대상 발견 상태, DLQ 헤더, 처리 건수 |
| 무결성 | 이 디렉터리의 `checksums.txt` |

**재현 조건**: 같은 스택을 띄우고 같은 6건을 주입하면 같은 집계가 나온다. 다만
Prometheus 카운터는 프로세스 수명 동안 누적되므로 **재기동 없이 다시 주입하면 값이 는다.**

이후 같은 계약을 **실제 Kafka 통합 시나리오**로 다시 확인했다 —
`load-test/anomaly-contract-kafka/RESULT_20260909_kafka_contract.md`. 그쪽은 스크립트가 있다.
