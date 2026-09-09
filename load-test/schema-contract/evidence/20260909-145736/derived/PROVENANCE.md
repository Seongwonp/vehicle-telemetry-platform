# 파생 증거 — 원본이 아니다

| | |
| --- | --- |
| 생성 스크립트 | `load-test/schema-contract/collect_dlq_attribution.sh` |
| 생성 명령 | `bash load-test/schema-contract/collect_dlq_attribution.sh load-test/schema-contract/evidence/20260909-145736` |
| 생성 시각 | 2026-09-09T15:08:15+09:00 |
| 입력 (원본) | Kafka 토픽 `vehicle-telemetry-dlq`, `vehicle-telemetry-mqtt-dlq` — 실행이 남긴 상태 그대로 |
| 대조용 입력 | `../manifest.json` (주입한 차량 ID·시나리오·기대), `../e2e_results.csv` (판정) |
| 무결성 | 이 디렉터리의 `checksums.txt` — **상위의 실행 매니페스트와 별개다** |

**이 파일들은 실행이 끝난 뒤 토픽을 다시 읽어 만든 것이다.** 실행 시점에 봉인된 원본은
상위 디렉터리의 `checksums.txt`가 덮는 파일들뿐이다. 여기 있는 것을 원본인 것처럼
그 매니페스트에 섞지 않는다 — 2026-09-08에 자기검사 결과를 매니페스트 안의
`metadata.txt`에 붙이려다 같은 함정을 봤다.

**재현 조건**: DLQ 토픽의 보존 기간 안이고, 그 사이 다른 실행이 같은 토픽에 쓰지 않았다면
같은 결과가 나온다. 둘 중 하나라도 깨지면 재현되지 않는다 — 그때는 이 파일이 유일한 기록이다.
`run_e2e.sh`는 매 실행 시작에 `down -v`로 토픽을 비우므로, **다음 실행이 돌면 재현 불가**다.

## 파일

| 파일 | 내용 |
| --- | --- |
| `dlq-kafka-attribution.txt` | 차량 ID(레코드 키) / 예외 클래스 / 사유 문자열 |
| `dlq-mqtt-attribution.txt` | 차량 ID(토픽 마지막 마디) / 사유 코드 — **상세 없음** |
| `reason-counts.txt` | 사유 코드별 건수 (`docs/runbook/dlq-reprocessing.md` 2-1절과 같은 방법) |
