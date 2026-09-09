# 입력 계약(P0-2) 파이프라인 E2E — 두 입구가 끝까지 같은 판정을 내리는가

| 항목 | 값 |
| --- | --- |
| 검증 상태 | **부분 검증 (1회 실행)** — `docs/evidence-policy.md` 기준 "그 조건에서 관찰됨" |
| 적용 범위 | **dev(평문) 프로파일, 무부하, 단일 backend, 파티션 3** |
| 코드 상태 | 최종 코드(`TelemetryDecoder`의 JSON `null` 가드 포함). 커밋 전 워킹 트리 |
| 실행 명령 | `bash load-test/schema-contract/run_e2e.sh` |
| 원본 | `evidence/20260909-145736/` |
| 파생(사후 분석) | `evidence/20260909-145736/derived/` — 별도 매니페스트, `PROVENANCE.md` 참고 |
| 판정 | **PASS 21 / FAIL 0** |

## 이 실험이 완료로 주장하는 범위

**"MQTT 입구와 Kafka 저장 입구의 공통 계약 구현, 그리고 dev 프로파일에서의 E2E 1회 검증."**
그 밖은 이 문서가 주장하지 않는다.

| | |
| --- | --- |
| 주장한다 | 두 입구가 같은 decoder를 쓰고, 같은 payload에 같은 판정을 내리며, 거부는 각 DLQ로 사유와 함께 귀속된다 |
| **주장하지 않는다** | mTLS 프로파일, 부하 상황, 이상 감지 경로(`anomaly-detector`)의 계약 정합성, 반복 안정성 |

## 왜 단위 테스트로 안 끝냈나

단위·핸들러 테스트는 **decoder가 거부한다**까지만 본다. 그 뒤에 브로커·Kafka·백엔드·
InfluxDB·DLQ가 있고, **거부된 메시지가 정말 저장되지 않고 지정된 DLQ로 가는지**는
거기까지 가봐야 안다. 반대쪽도 마찬가지다 — 계약 안의 이상치(온도 106°C)가 **저장되고
이상 감지까지 도달하는지**는 감지기를 띄워야 확인된다.

## 결과

시나리오 10종 × 입구 2 + 혼합 배치 1 = 21칸. 차량 ID를 시나리오·입구별로 나눠
저장 결과에서 어느 칸인지 귀속시킨다.

| # | 시나리오 | 기대 | MQTT | Kafka 직접 |
| ---: | --- | --- | :---: | :---: |
| 1 | 정상 | 저장 | 1행 | 1행 |
| 2 | `rpm: 2000.7` | 저장(소수 보존) | 1행 | 1행 |
| 3 | `engine_temp: 106` (감지 임계 초과, 계약 내) | 저장 + 이상 알림 | 1행 | 1행 |
| 4 | `speed` 필드 누락 | 거부 | 0행 | 0행 |
| 5 | `speed: null` | 거부 | 0행 | 0행 |
| 6 | `gps.lat` 누락 | 거부 | 0행 | 0행 |
| 7 | 필드명 오타 (`sped`) | 거부 | 0행 | 0행 |
| 8 | `speed: 300` (계약 상한 255 초과) | 거부 | 0행 | 0행 |
| 9 | `dtc_codes: [null]` | 거부 | 0행 | 0행 |
| 10 | **payload가 `null` 네 글자** | 거부 | 0행 | 0행 |
| — | 혼합 배치(정상 2 + 위반 1) | 2행 저장 | — | **2행** |

**값 보존**: `rpm`이 양쪽 다 **2000.7**로 저장됐다. 변경 전에는 `int`라 2000으로 잘렸다.

**이상 감지 도달**: 106°C가 양쪽 다 `anomaly_alerts` **1건**을 만들었다.
검증 범위(상한 215)가 감지 임계(105)보다 넓다는 설계가 실제로 성립한다 —
좁았으면 이상 데이터가 저장되지 않아 감지가 무의미해진다.

**DLQ 증가**: kafka-dlq **8** (거부 7 + 혼합 배치 1), mqtt-dlq **7**. 숫자가 정확히 맞는다.

## "거부됐다"로 끝내지 않았다 — 사유를 레코드마다 귀속시켰다

행이 0이라는 것은 **거부됐다**는 뜻이지 **의도한 이유로 거부됐다**는 뜻이 아니다.
이 구분이 실제로 값을 냈다(아래 "두 번 무효였다"). DLQ 레코드를 열어 키(차량 ID)와
사유를 맞췄다. 수집은 `collect_dlq_attribution.sh`가 한다.

| 차량 ID | 예외 | 사유 |
| --- | --- | --- |
| `…-K04` | `TelemetryContractException` | `PAYLOAD_VALIDATION_FAILED: speed must not be null` |
| `…-K05` | `TelemetryContractException` | `PAYLOAD_VALIDATION_FAILED: speed must not be null` |
| `…-K06` | `TelemetryContractException` | `PAYLOAD_VALIDATION_FAILED: gps.lat must not be null` |
| `…-K07` | `TelemetryContractException` | `UNKNOWN_FIELD: sped` |
| `…-K08` | `TelemetryContractException` | `PAYLOAD_VALIDATION_FAILED: speed must be less than or equal to 255` |
| `…-K09` | `TelemetryContractException` | `PAYLOAD_VALIDATION_FAILED: dtcCodes[0].<list element> must not be null` |
| `…-K10` | `TelemetryContractException` | **`TYPE_MISMATCH: (최상위 null)`** |
| `…-MIX` | `TelemetryContractException` | `PAYLOAD_VALIDATION_FAILED: speed must not be null` |

MQTT 쪽 7건은 `…-M04`~`…-M10`에 각각 대응한다. 사유 코드 집계가 양쪽에서
`PAYLOAD_VALIDATION_FAILED` 5 / `UNKNOWN_FIELD` 1 / `TYPE_MISMATCH` 1로 같다
(Kafka는 혼합 배치 1건이 더해져 `PAYLOAD_VALIDATION_FAILED`가 6).

**입구별 차이 하나**: Kafka DLQ 헤더에는 **사유 코드 + 상세**가 실리는데,
MQTT DLQ envelope에는 **사유 코드만** 있다. 둘 다 귀속은 되지만 MQTT 쪽은 어느 필드가
문제인지 payload를 직접 봐야 안다. 조사 절차는 `docs/runbook/dlq-reprocessing.md` 2-1절.

### `null` 네 글자 — 최종 점검이 찾은 구멍이고, 이 실행이 그 수정을 확인한다

`readValue("null")`은 **예외 없이 null을 돌려준다.** 그걸 그대로 `validate()`에 넘기면
Hibernate Validator가 `IllegalArgumentException`(HV000116)을 던지는데 **계약 예외가 아니다.**
`MqttMessageHandler`는 `TelemetryContractException`만 잡으므로 **거부가 아니라 핸들러 밖으로
전파**됐고, Kafka 쪽은 잡히긴 해도 `dlq.py`가 낯선 타입이라 `unknown`으로 분류했다.

`TelemetryDecoder`에 null 가드를 넣어 `TYPE_MISMATCH`로 막았고, **이 실행에서 두 입구 모두
`TYPE_MISMATCH`로 거부되는 것을 확인했다**(시나리오 10). 단위 근거는
`TelemetryContractTest.예외_타입_봉인.널_리터럴`.

## offset 보호 — 이 계약 변경이 깨뜨리지 않았음을 어디서 보나

계약 위반은 **레코드 단위 영구 실패**라 offset을 커밋한다. 반면 **DLQ 발행 실패**와
**저장 실패**는 offset을 넘기면 안 된다. 이 계약 변경으로 예외 타입이 바뀌었으므로
그 경계가 그대로인지 확인이 필요했다.

| 보호 대상 | 근거 테스트 | 원본 결과 |
| --- | --- | --- |
| DLQ 발행 실패 시 원본 offset 미커밋 | `TelemetryConsumerTest.dlq전송실패_offset미커밋` | — (단위) |
| 저장 실패 시 offset 미커밋 + 예외 전파(재시도 유도) | `TelemetryConsumerTest.consumeForStorage_저장실패_재시도유도` | — (단위) |
| DLQ 발행이 ack보다 **먼저** 일어난다 | `KafkaDlqContractTest.malformedTelemetryIsPublishedToRealDlqBeforeAcknowledgment` | Testcontainers |
| 영구 InfluxDB 실패는 DLQ 발행 뒤에만 커밋 | `KafkaStorageFailureContractTest.permanentInfluxFailurePublishesToDlqBeforeCommittingSourceOffset` | Testcontainers |
| DLQ 발행 실패 시 재기동하면 원본이 재전달된다 | `KafkaStorageFailureContractTest.dlqPublishFailureLeavesOffsetAndRestartRedeliversSourceRecord` | Testcontainers |
| 배치 전체 영구 실패도 전 레코드 DLQ 후 진행 | `KafkaStorageFailureContractTest.permanentBatchFailurePublishesEveryRecordToDlqAndCommitsPastBatch` | Testcontainers |
| 장애 중 유실 0 (예산 소진 경로) | — | `load-test/long-outage/RESULT_20260906_influxdb.md`, `RESULT_20260906_postgres.md` |
| 배치 안의 **계약 위반** 1건만 격리 | `TelemetryConsumerTest.consumeForStorage_혼합배치_계약위반건만_DLQ이동` | 이 문서(혼합 배치 칸) |

전체 스위트 **173건, 실패 0, skip 0**(Testcontainers 계약 5종 실제 실행).

## 여기 오기까지 세 번 무효였다 — 그게 이 문서의 요점이다

**1회차(14:10:21) — 옛 이미지를 쟀다.**
`docker compose up`은 이미지가 있으면 재사용한다. 코드를 고쳐놓고 빌드를 안 해서
**변경 전 backend**를 측정했다. 결과가 "전부 저장됨, DLQ 증가 0"으로 아주 깨끗하게 나왔고,
그게 오히려 신호였다 — 거부가 하나도 없을 리가 없었다.
`run_e2e.sh`에 빌드 단계를 **필수로** 넣고 이 사고를 주석에 남겼다.

**2회차(14:15:33) — 차량 ID가 계약을 위반했다. 19칸 중 15칸이 PASS로 보였는데 전부 못 믿는다.**
ID를 `<prefix>-KAFKA-01` 형태로 만들었더니 **21자**가 됐다. 계약의 `vehicle_id` 상한은
**20자**다(`^[A-Z0-9-]{4,20}$`). MQTT 쪽(`-MQTT-01`)은 정확히 20자라 통과했다.

그래서 Kafka 열에서:
- 저장 기대 3칸은 **FAIL** — 정상 payload인데 ID 때문에 거부됐다.
- 거부 기대 6칸은 **PASS** — 그런데 **시나리오 때문이 아니라 ID 길이 때문**이었다.

**여기서 배운 것은 "PASS가 PASS를 뜻하지 않는다"이다.** FAIL 3칸이 없었으면 이 실행을
성공으로 읽었을 것이고, 그때 6칸의 PASS는 전부 거짓이었다. **원인을 확정한 것은 판정표가
아니라 DLQ 헤더**였다 — `PAYLOAD_VALIDATION_FAILED: vehicleId must match "^[A-Z0-9-]{4,20}$"`.
2026-09-09에 적은 **"결과는 집계가 아니라 증거 파일에서 읽는다"**가 그대로 다시 걸렸다.

고친 것 둘: 입구 코드를 **한 글자**(M/K)로 줄였고, **길이 가드**(`check_id()`)를 넣어
주입기가 계약 밖 ID를 만들면 즉시 중단하게 했다. 판정 스크립트도 같은 형식으로 맞췄다.

**3회차(14:28:12, 19/19 PASS) — 판정은 옳았지만 최종 코드가 아니었다.**
이 실행 뒤에 `TelemetryDecoder`의 `null` 가드가 들어갔다(이미지 빌드 14:21 / 가드 14:39).
**검증 대상과 최종 코드가 다르면 그 검증은 최종 코드의 근거가 아니다.**
시나리오 10(`null_literal`)을 추가하고 새 이미지로 다시 돌린 것이 이 문서의 4회차다.
3회차 디렉터리(`evidence/20260909-142812`)는 지우지 않고 남겼다 — 대체됐다는 표시와 함께.

**가드는 조용한 실패를 시끄럽게 만든 것이지 버그를 고친 게 아니다.** 이 하네스가
계약을 위반할 수 있는 다른 경로(타임스탬프 형식, 토픽 길이)는 여전히 가드가 없다.

## 이 문서의 한계

- **1회 실행이다.** 안정성 주장은 못 한다. 반복은 `docs/roadmap.md`에.
- **dev(평문) 프로파일만** 쟀다. mTLS 프로파일에서는 안 봤다.
- **부하가 없는 상태**다. 21건뿐이라 배치가 잘게 쪼개졌을 가능성이 높다.
- **혼합 배치 3건이 정말 같은 poll 배치에 담겼는지는 밖에서 확인할 수 없다.**
  같은 키로 연속 발행해 같은 파티션에 넣었을 뿐이다. 배치 격리의 직접 증거는 단위 테스트
  (`TelemetryConsumerTest.consumeForStorage_혼합배치_계약위반건만_DLQ이동`)이고,
  이 E2E는 그것과 **모순되지 않는다**는 확인이다.
- **이상 감지기가 거부된 payload를 어떻게 처리했는지는 안 봤다.** 감지기는 이 계약을
  거치지 않는다(별도 Consumer Group + 자체 파싱). `roadmap.md` P0-2a로 등록했다.
- 드레인 대기가 **고정 40초**다. 조건이 바뀌면(부하, 느린 디스크) 이 값으로는 부족할 수 있고,
  그러면 "저장 안 됨"이 **거부가 아니라 아직 안 온 것**일 수 있다.
- `null_literal` 칸은 payload에 차량 ID가 없어 **저장 행 수로는 귀속되지 않는다.**
  판정은 "저장 0행"이고 귀속은 DLQ 쪽(Kafka는 레코드 키, MQTT는 토픽)에서 한다.
