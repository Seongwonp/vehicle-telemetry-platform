# 이상 감지 경로의 입력 계약 정합성 (P0-2a) — 조사와 결정표

> 상태: **정책 확정, 구현 완료** (2026-09-09)
>
> 정책은 **공통 입력 계약 유지**다. 필수 센서 누락/null, 범위 밖, 타입 오류,
> unknown 필드는 **감지 이전에 거부**한다. 정상 필드만 골라 부분 감지하지 않는다 —
> 그건 별개 설계이고 필요성이 확인되면 그때 한다.
>
> 아래 1~2절은 **정책 적용 전 측정**이고 3절이 확정된 정책, 5절이 구현과 검증이다.
> 변경 전 동작은 `contract-fixtures/cases.json`의 `detector_before` 칸에도 남아 있다.
>
> 측정 대상 커밋: `1fa11b7`. fixture: `contract-fixtures/cases.json` (21칸).
> 저장 칸은 `TelemetryDecoder`를, 감지 칸은 `anomaly_detector.process()`를
> **실제로 호출해서** 쟀다. 추론이 아니다.

## 0. 무엇을 묻고 있나

P0-2에서 저장 경로 두 입구(MQTT·Kafka)를 공통 계약으로 묶었다. 그런데
`anomaly-detector`(Python)는 같은 토픽 `vehicle-telemetry`를 **별도 Consumer Group**으로
읽고 **자체 파싱**한다. 그 계약을 안 거친다.

**"저장 입구를 막았으니 이제 안전하다"가 성립하지 않는다.** 무엇이 어떻게 다른지를
먼저 재고, 그 다음에 같아야 하는지를 정한다. 이 문서는 앞의 절반이다.

**바로 Java 계약을 Python으로 옮기지 않는다.** 두 벌을 손으로 맞추면 갈라지고,
갈라진 것은 양쪽 테스트가 다 통과하므로 측정에서 안 드러난다.

## 1. 측정 결과 — 두 경로가 어떻게 갈렸나 (**정책 적용 전**)

`accept/reject`는 저장, `alert/no_alert/dlq`는 감지다.

| 조합 | 칸 수 | 뜻 |
| --- | ---: | --- |
| `accept` × `alert` | 4 | 저장되고 이상으로 잡힌다 — **설계대로** |
| `accept` × `no_alert` | 3 | 저장되고 정상 판정 — **설계대로** |
| `reject` × `dlq` | 6 | 양쪽 다 격리 — **결과는 같지만 사유가 다르다**(§1-4) |
| **`reject` × `alert`** | **3** | **저장 안 된 데이터로 알림이 나간다** |
| **`reject` × `no_alert`** | **5** | **저장은 거부하는데 감지는 조용히 지나간다** |

### 1-1. `reject` × `no_alert` — 조용히 눈이 먼다

| fixture | 저장 | 감지 | 로그 |
| --- | --- | --- | --- |
| `speed_missing` | `PAYLOAD_VALIDATION_FAILED` | **no_alert** | **없음** |
| `speed_null` | `PAYLOAD_VALIDATION_FAILED` | **no_alert** | **없음** |
| `field_name_typo` (`sped`) | `UNKNOWN_FIELD` | **no_alert** | **없음** |
| `gps_lat_missing` | `PAYLOAD_VALIDATION_FAILED` | no_alert (gps는 어느 룰에도 안 쓰인다) | **없음** |
| `vehicle_id_missing` | `PAYLOAD_VALIDATION_FAILED` | **no_alert** | **없음** |
| (대조) `normal` | accept | no_alert | **없음** |

원인은 `rules.py:34`의 `data.get(field)`다. 값이 없으면 `None`이고, `None`이면
**그 룰이 통째로 건너뛰어진다.**

**로그 칸은 실측이다.** 각 fixture를 넣고 로거를 가로채서 줄 수를 셌다 — 다섯 칸 모두
**0줄**이고, 그건 **정상 메시지와 똑같다**(맨 아래 대조 행). 지표도 없다
(감지기에는 처리 건수 카운터만 있고 룰별 평가 여부를 세는 것이 없다).

**이게 왜 나쁜가**: 발행 측이 `speed`를 빠뜨리면 그 차량은 **과속 감지가 영원히 안 된다.**
저장은 거부되니 DLQ에서 보이지만, **감지 쪽은 알림·로그·지표 어디에도 흔적이 없다.**
"감지기가 도는데 알림이 없다"와 "감지기가 그 필드를 못 보고 있다"가 **밖에서 구분되지 않는다.**

`rpm_null_with_other_anomaly`가 이걸 제일 잘 보여준다 — `rpm`이 null이라 RPM 룰만
건너뛰고 엔진 과열은 정상적으로 잡힌다. 로그에는 **"[이상 감지] 엔진 과열" WARNING이
정상적으로 찍힌다.** 즉 **부분적으로 눈이 먼 상태가 로그상으로는 더 건강해 보인다.**

### 1-2. `reject` × `alert` — 저장 안 된 데이터로 알림이 나간다

| fixture | 저장 | 감지 |
| --- | --- | --- |
| `speed_out_of_range` (300) | 거부 (상한 255) | **과속 알림** |
| `dtc_comma_inside` (`"P0301,P0420"`) | 거부 (형식) | **DTC 알림** |
| `numeric_string_over_threshold` (`"201"`) | **저장됨** | 과속 알림 |

앞의 둘은 **알림은 받았는데 대시보드에 그 시점 데이터가 없다.** 운영에서 이건
"알림 오류"로 보이지 저장 거부로 보이지 않는다.

이게 단순한 버그는 아니다. `speed: 300`은 **감지 관점에서는 과속이 맞다.**
저장 계약이 그 값을 못 믿을 값으로 본 것뿐이다. **정책 결정이 필요한 칸이다**(§3).

### 1-3. 조건부로 터진다 — 같은 결함이 데이터에 따라 다르게 보인다

| fixture | 감지 |
| --- | --- |
| `vehicle_id_missing` (이상 없음) | **no_alert** — 조용 |
| `vehicle_id_missing_with_anomaly` (`engine_temp` 106) | **dlq** `KeyError: 'vehicle_id'` |
| `timestamp_missing_with_anomaly` | **dlq** `KeyError: 'timestamp'` |

`rules.py`는 이상이 걸릴 때만 `data["vehicle_id"]`를 읽는다. 그래서 **같은 결함이
값에 따라 조용하기도 하고 DLQ로 가기도 한다.** 장애 조사에서 제일 나쁜 형태다 —
재현이 데이터에 달려 있다.

### 1-4. 결과는 같은데 사유가 다른 칸

`reject` × `dlq` 6칸은 둘 다 격리되지만 **왜 격리됐는지가 다르다.**

| fixture | 저장 사유 | 감지 예외 |
| --- | --- | --- |
| `malformed_json` | `MALFORMED_JSON` | `JSONDecodeError` |
| `null_literal` | `TYPE_MISMATCH` | `AttributeError` (`None.get`) |
| `speed_not_a_number` | `TYPE_MISMATCH` | `ValueError` (`float('fast')`) |
| `dtc_null_element` | `PAYLOAD_VALIDATION_FAILED` | `TypeError` (`','.join`) |
| `vehicle_id_missing_with_anomaly` | `PAYLOAD_VALIDATION_FAILED` | `KeyError` |
| `timestamp_missing_with_anomaly` | `PAYLOAD_VALIDATION_FAILED` | `KeyError` |

**`null_literal`은 두 언어가 같은 함정에 빠졌다** — `json.loads("null")`도
`readValue("null")`도 예외 없이 null을 돌려준다. 저장 쪽은 오늘 가드를 넣었고,
감지 쪽은 우연히 그 다음 줄에서 터진다.

## 2. 실패했을 때 어떻게 되나 — 재시도·격리·복구 (**정책 적용 전**)

코드를 읽고(`anomaly_detector.py:376-455`) 표로 정리한 것이다. **장애 주입으로
재지는 않았다** — 그건 정책을 정한 뒤에 한다.

| 항목 | 현재 동작 |
| --- | --- |
| 재시도 | **없다.** 레코드별 `try/except` 한 번, 실패하면 바로 DLQ |
| 격리 단위 | 레코드 1건. 역직렬화(`json.loads`)와 처리(`process`)가 각각 잡는다 |
| offset | 성공이든 DLQ든 `safe_offsets`에 넣고 배치·시간 기준으로 커밋 |
| DLQ 발행 실패 | 예외를 **다시 던진다** → 그 offset은 커밋되지 않는다 (`anomaly_detector.py:317`) |
| 알림 발행 실패 | `future.get()`이 던진다 → 그 레코드가 DLQ로 (`process()` 안) |
| ML 예측 실패 | **DLQ로 안 보낸다.** 그 배치의 ML 판정만 포기하고 룰은 계속. ERROR 로그 |
| 복구 | `dlq-tools/dlq.py`로 `vehicle-telemetry-anomaly-dlq`를 재처리 |

**로그는 위 표와 달리 실측했다.** 판정별로 이렇게 갈린다.

| 감지 판정 | 로그 |
| --- | --- |
| `alert` | `WARNING [이상 감지] vehicle=… type=… field=… value=… severity=…` 이벤트당 1줄 |
| `dlq` | 소비 루프가 `ERROR ... DLQ로 이동 partition=… offset=…` + 스택 트레이스 |
| `no_alert` | **아무것도 없다** — 정상 메시지와 구분되지 않는다 |

**`alert` 로그에 값이 그대로 찍힌다**(`value=300.0`). 저장 경로는 개인정보 때문에
거부 사유에서 값을 뺐는데(`TelemetryDecoder.describe()`), 감지 경로는 차량 ID와 값을
같이 남긴다. **성질이 다르므로 그 자체가 결함은 아니다** — 알림은 사람이 보고 조치해야
하고 어느 차량 몇 km/h인지가 없으면 쓸모가 없다. 다만 `docs/data-retention.md`의
로그 보존 기간이 이 줄에도 적용된다는 것은 짚어둔다.

**저장 경로와 다른 점 하나**: 저장은 의존성 장애에 **180초 재시도 예산**을 쓰는데
(`ExponentialBackOff`), 감지기는 재시도가 없다. 알림 발행이 한 번 실패하면 바로 DLQ다.
이건 계약 문제가 아니라 별개의 설계 차이이고, **이 문서의 범위가 아니다.**

### 2-1. 여기서 실제 결함을 하나 찾았다 — 감지기 예외가 전부 `unknown`으로 분류된다

`dlq.py`의 분류 목록에 감지기가 **실제로 내는 예외가 거의 없다.**

| 예외 | `classify()` | 실제 성질 |
| --- | --- | --- |
| `JSONDecodeError` | `permanent` | 맞다 |
| `UnicodeDecodeError` | `permanent` | 맞다 |
| **`KeyError`** | **`unknown`** | 영구 (payload에 필드가 없다) |
| **`TypeError`** | **`unknown`** | 영구 |
| **`AttributeError`** | **`unknown`** | 영구 |
| **`ValueError`** | **`unknown`** | 영구 |
| **`KafkaTimeoutError`** | **`unknown`** | **일시** |
| **`NoBrokersAvailable`** | **`unknown`** | **일시** |

`unknown`은 **자동 재처리 대상에서 빠진다.** 그건 의도된 설계지만
(모르는 것을 일시적이라고 가정하지 않는다), 여기서는 **우리가 아는 것을 안 적어둔 것**이다.

**같은 실수를 저장 경로에서 두 번 했다** — 2026-09-06 `InfluxException`,
2026-09-09 `TelemetryContractException`. 감지 경로는 아무도 안 봤다.

**고치는 것은 이 조사의 범위 밖으로 둔다.** 목록에 넣는 것 자체는 한 줄이지만,
`ValueError`·`TypeError`가 정말 항상 영구인지는 **감지기 코드가 바뀌면 달라진다.**
정책(§3)을 정하면서 같이 결정한다. 다만 `dlq-tools/test_dlq.py`에 **현재 상태를
고정하는 테스트를 넣어**, 모르는 채로 넘어가지 않게 했다.

## 3. 확정된 정책 — 공통 입력 계약을 유지한다

### 3-1. 두 경로의 역할이 다르다는 것부터 분명히 한다

| | 저장 경로 | 감지 경로 |
| --- | --- | --- |
| 목적 | 나중에 **다시 읽을 데이터**를 남긴다 | **지금 위험한 상태**를 알린다 |
| 틀렸을 때 대가 | 잘못된 값이 영구히 남고 소급 구분이 안 된다 | 오탐(귀찮다) 또는 미탐(위험하다) |
| 그래서 | **엄격한 게 맞다** — 못 믿을 값은 안 받는다 | **너무 엄격하면 위험을 놓친다** |

**두 경로가 무조건 같아야 한다는 전제부터가 틀렸다.** 예를 들어 `speed: 300`은
저장 계약 밖이지만 **감지 관점에서는 과속이 맞다.** 여기서 감지까지 막으면
"계약 밖 값이라 위험 신호를 버렸다"가 된다.

### 3-2. 확정된 정책

| # | 질문 | **확정** | 근거 |
| ---: | --- | --- | --- |
| 1 | 필수 센서 누락/null | **감지 이전에 거부(DLQ)** | 공통 입력 계약을 유지한다. 정상 필드만 골라 부분 감지하면 **기존 필수 입력 계약을 바꾸는 결정**이 된다 |
| 2 | 계약 범위 밖의 값(`speed: 300`) | **입력 오류로 거부** | 과속 임계는 넘지만 **계약에서 유효하지 않은 값**이다. 신뢰할 수 있는 과속 알림으로 쓸지는 별도 정책이 필요하다 |
| 3 | `dtc_codes` 형식 위반 | **저장과 같이 거부** | 선택 필드라는 성질은 유지하되(없어도 되고 비어도 된다) **원소 형식은 계약이다** |
| 4 | `vehicle_id`·`timestamp` 누락·형식 오류 | **감지 이전에 판정** | 이상 감지 여부에 따라 실패가 달라지면 안 된다. 조건부는 재현이 데이터에 달려 조사가 불가능하다 |
| 5 | 감지기 예외의 DLQ 분류 | **출처로 가른다** | 검증기가 명시적으로 만든 계약 오류만 영구. 일반 `TypeError`/`KeyError`는 **구현 버그일 수 있어** `unknown` (§5-4) |
| 6 | 공유 fixture를 누가 읽나 | **양쪽 테스트** | 한쪽만 읽으면 갈라진다 (§4) |

**처음에 내가 1번을 "지표만 올리고 계속 감지", 2번을 "감지하되 표시"로 기울였다가
뒤집혔다.** 지적의 요지는 이것이다 — **부분 감지는 기존 필수 입력 계약을 바꾸는 결정**이고,
`speed: 300`을 신뢰할 수 있는 과속 알림으로 쓸지는 그 자체로 별도 정책이 필요하다.
"위험 신호를 버리지 않는다"는 명분으로 계약을 느슨하게 하면, **못 믿는 값으로 알림을
내보내면서 그게 못 믿는 값이라는 것은 어디에도 안 남는다.**
부분 데이터 감지가 필요해지면 그때 별도로 설계한다.

### 3-2-a. "입력 오류"와 "차량 이상"을 구분한다

| 입력 | 무엇인가 | 결과 |
| --- | --- | --- |
| `speed: 201` | **차량 이상** — 계약 안(상한 255)이고 임계(200) 초과 | 알림 |
| `rpm: 6001` | **차량 이상** | 알림 |
| `engine_temp: 106` | **차량 이상** | 알림 |
| `speed: 300` | **입력 오류** — 계약 밖 | DLQ |
| `speed` 누락 | **입력 오류** | DLQ |

같은 필드의 같은 방향인데 하나는 알림이고 하나는 격리다. 가르는 것은 **계약 경계**이지
임계값이 아니다.

### 3-2-b. timestamp — 형식 검증과 신선도는 다르다

**형식만 본다.** 오래된 시각도 통과한다 — 과거 데이터를 거부하면 **DLQ 재처리와 백필이
불가능해진다.** 신선도가 필요하면 그건 별도 정책이지 입력 계약이 아니다.

형식을 정하면서 **어느 한쪽 표준 라이브러리도 기준이 될 수 없다**는 것을 실측으로 확인했다.

| 입력 | Java `Instant.parse` | Python `fromisoformat` |
| --- | --- | --- |
| `2026-09-09t10:00:00z` (소문자) | 통과 | **거부** |
| `2026-09-09T10:00:00` (오프셋 없음) | **거부** | 통과 |
| `2026-09-09 10:00:00Z` (공백 구분자) | **거부** | 통과 |

그래서 **정규식을 계약으로 선언**하고(`VehicleTelemetry.TIMESTAMP_PATTERN`) 양쪽이 그걸
구현한다. 오프셋은 필수다. 다만 정규식은 **모양**만 보므로 `2026-13-45T99:00:00Z`가
통과한다 — 달력 검증을 한 단계 더 한다.

**이 결정이 저장 경로의 동작도 바꿨다.** 예전에는 잘못된 타임스탬프가 `toPoint()`의
`Instant.parse`까지 가서 `DateTimeParseException`으로 DLQ에 갔다. 지금은 계약 단계에서
`PAYLOAD_VALIDATION_FAILED`로 거부된다. 감지 경로에는 `toPoint()` 단계가 아예 없어서,
그대로 두면 같은 payload가 경로에 따라 다르게 끝났다.

### 3-3. 하지 않기로 미리 정한 것

- **감지기를 저장 경로 뒤로 옮기지 않는다.** 지금 병렬인 것은 의도다 —
  저장 장애가 감지를 막지 않는다(12시간 soak test에서 저장이 멈춰도 감지는 돌았다).
  계약 일치와는 별개 문제다.
- **Java 계약을 Python으로 옮겨 적지 않는다.** §4.
- **감지기에 재시도 예산을 넣지 않는다.** 별개 설계 항목이고 근거가 아직 없다.

## 4. 공유 fixture — 왜 파일 하나인가

`contract-fixtures/cases.json` (21칸). 스키마는 `contract-fixtures/README.md`.

두 벌을 손으로 맞추면 갈라지는데, **갈라진 것은 양쪽 테스트가 다 통과하므로
측정에서 안 드러난다.** `docker-compose.yml`의 환경변수를 YAML 앵커 하나로 묶은 것과
같은 이유다(2026-09-07 — "복제하면 언젠가 갈라지는데 그건 측정에서 안 드러난다").

각 칸이 **저장 칸과 감지 칸을 따로** 갖는다. 같아야 하면 같게 적고, 달라야 하면
다르게 적는다 — **파일 구조 자체가 "두 경로가 같아야 한다"를 전제하지 않는다.**

`payload`는 파싱된 객체가 아니라 **원문 문자열**이다. `null` 리터럴과 깨진 JSON을
담아야 하고, 그 둘이 실제로 함정이 있던 칸이기 때문이다.

**읽는 쪽을 양쪽에 만들었다**(2026-09-09) — 다만 **특성화 테스트**다.

| 읽는 칸 | 테스트 | 건수 |
| --- | --- | ---: |
| `storage` | `SharedFixtureContractTest` | 22 |
| `detector` | `anomaly-detector/tests/test_shared_fixtures.py` | 22 |

**정책 테스트가 아니라 드리프트 방지다.** 지금 고정한 값은 §1의 갈림을 그대로 담고
있고, 그게 옳다는 뜻이 아니다. 정책(§3-2)이 정해지면 동작을 바꾸고 fixture의 기대값을
같이 옮긴다 — **테스트를 지우지 않는다.** 그래야 무엇이 바뀌었는지 diff에 남는다.

Python 쪽에는 **갈림의 개수 자체를 고정하는 테스트**도 넣었다(`조용한 칸 5`,
`알림 나가는 칸 3`). 칸별 테스트만으로는 아무도 총계를 안 세고, 그러면 이 문서의 §1 표가
조용히 낡는다.

**만들면서 검사가 안 도는 것을 하나 잡았다.** fixture가 `backend/` 밖이라 Gradle이
입력으로 몰랐고, 기대값을 틀리게 바꿔놓고 돌렸는데 `:test UP-TO-DATE`로
**BUILD SUCCESSFUL**이 나왔다. `build.gradle`에 선언해 고쳤고, 양쪽 다 일부러 틀리게
만들어 실제로 깨지는 것을 확인했다 — Java 1건, Python 2건.

**그 수정이 CI를 깨뜨렸다.** `backend/Dockerfile`은 컨텍스트가 `./backend`라 저장소
루트가 없는데, `inputs.file`은 `optional(true)`를 줘도 **파일이 없으면 태스크 설정
단계에서 실패**한다. 게다가 fixture가 없으면 `@ParameterizedTest`의 인자가 0건이 되어
이 JUnit 버전은 그것도 오류로 본다. 셋 다 고쳤다 —
`inputs.files`(FileCollection은 없는 경로를 비워둔다), sentinel 한 건으로 0건 방지,
그리고 **"루트가 없어서 못 읽음"과 "지워져서 못 읽음"을 갈라서** 앞은 skip, 뒤는 실패.
`docker compose build backend`로 실제 재현하고 고친 뒤 다시 통과하는 것까지 확인했다.

## 5. 구현과 검증 (2026-09-09)

### 5-1. 무엇을 만들었나

| 무엇 | 어디 |
| --- | --- |
| Python 계약 검증기 | `anomaly-detector/contract.py` — 사유 코드·순서가 Java와 같다 |
| 소비 루프 연결 | `anomaly_detector.py` — 룰 판정 **앞**에서 검증, 위반은 DLQ |
| 사유별 지표 | `telemetry_contract_rejected_total{reason}` — 시계열 4개 |
| DLQ 사유 귀속 | `x-dlq-contract-reason`, `x-dlq-source-path` 헤더 추가 |
| 사유 순서 고정(Java) | `TelemetryDecoder` — 단계를 나눠 payload 순서 의존을 없앴다 |
| timestamp 형식 계약 | `VehicleTelemetry.TIMESTAMP_PATTERN` + 달력 검증 |

**지표 라벨은 사유 코드 4종뿐이다.** 차량 ID·payload·예외 메시지는 넣지 않는다 —
카디널리티도 문제지만 Prometheus 라벨은 보존 기간 내내 남아 개인정보가 새는 경로가 된다.
어느 필드가 문제인지는 지표가 아니라 DLQ에서 본다.

감지기는 `replicas: 3`이고 이름·포트가 고정되지 않아 `static_configs`로는 한 대만 잡힌다.
compose 내장 DNS가 서비스 이름을 **모든 컨테이너 IP**로 풀어주므로 `dns_sd_configs`를 쓴다.

### 5-2. 갈림이 0이 됐다

`contract-fixtures/cases.json` **61칸**(21칸에서 시작해 언어 차이·사유 순서·timestamp·trailing 추가).

| | 정책 적용 전 | **적용 후** |
| --- | ---: | ---: |
| 저장 `reject` × 감지 무반응 | 5 | **0** |
| 저장 `reject` × 감지 알림 | 3 | **0** |
| 저장 `reject` × 감지 DLQ | 6 | **40** |
| 저장 `accept` × 감지 정상 | 7 | 13 |

**바뀐 칸은 정확히 8개**이고 조사에서 찾은 그 8개다 —
`speed_missing`, `speed_null`, `speed_out_of_range`, `gps_lat_missing`,
`field_name_typo`, `dtc_comma_inside`, `vehicle_id_missing`, `rpm_null_with_other_anomaly`.
변경 전 값은 fixture의 `detector_before`에 남아 있다.

**Java와 Python이 53/53 같은 판정**을 내는 것을 확인했다(사유 코드까지).

### 5-3. 실제 파이프라인에서 확인했다

`load-test/schema-contract/evidence/20260909-P0-2a-detector/verification.txt`.
dev(평문) 프로파일, 감지기 3 인스턴스, Kafka에 6건 직접 주입.

- 사유 4종이 **각각 1건씩** 격리됐다(`sum by (reason)`로 확인).
- Prometheus가 **세 인스턴스를 모두** 발견해 `up` 상태다.
- DLQ 헤더에 `origin-topic/partition/offset` + `contract-reason` + `source-path`가 실린다.
- 계약을 통과한 2건만 `telemetry_anomaly_processed_total`에 잡힌다.

### 5-4. 소비 루프 — **가짜 Kafka(단위)**와 **실제 Kafka(통합)**를 나눠서 쟀다

**둘의 범위가 다르다. 흐리면 "가짜에서 확인한 동작"이 "실제에서 확인한 보장"처럼 읽힌다.**

| | `test_consume_loop.py` | `load-test/anomaly-contract-kafka/` |
| --- | --- | --- |
| 무엇 | **운영 소비 루프를 실행한 단위 테스트** | 실제 Kafka 통합 검증 |
| `main()` | 그대로 실행 | 그대로 실행 |
| consumer/producer | **가짜** | **진짜 브로커** |
| offset | 가짜 consumer가 기록한 값 | **브로커에게 물어본다** |
| 증명하는 것 | 분기·예외 전파·커밋 호출 방식 | 실제 커밋·재전달·발행 실패 |

먼저 단위 테스트로 분기를 봤다. `main()`을 그대로 돌린다 — 루프를 발췌해서 테스트하면
그 발췌본을 검증하는 셈이라, Kafka만 가짜로 바꿨다.

| 상황 | 결과 |
| --- | --- |
| 정상/실패/정상 배치 | 실패 1건만 DLQ, 세 건 다 offset 진행 |
| **DLQ 발행 실패** | **아무것도 커밋되지 않는다** |
| 알림 발행 실패 | 그 레코드만 DLQ, offset 진행 (격리에 성공했으므로) |
| 재시작 | 커밋이 없으므로 **배치 전체가 재전달**되고 이번엔 격리된다 |
| 커밋 방식 | 항상 명시적 offset. 인자 없는 `commit()`은 격리 실패분을 삼킨다 |

**DLQ 발행 실패에서 내 기대가 틀렸다.** "실패한 레코드 앞까지는 커밋될 것"이라고 봤는데
실측은 **아무것도 커밋 안 됨**이었다 — 검증 단계에서 실패하면 앞선 레코드들은 아직
`parsed`에 모여 있을 뿐 처리 전이기 때문이다. 중복 처리는 생기지만
**격리 못 한 것을 완료로 치지는 않는다.** 요구된 것은 후자이고, 실제가 기대보다 안전했다.

### 5-4-a. 실제 Kafka로 같은 계약을 다시 확인했다 — **4/4 PASS**

`load-test/anomaly-contract-kafka/RESULT_20260909_kafka_contract.md`.
시나리오마다 **전용 토픽·전용 Consumer Group**, volume은 유지.

| # | 확인 | 결과 |
| --- | --- | :---: |
| s1 | 정상/위반/정상 → committed **3**, DLQ 1(원본 offset 1, 원본 바이트 보존) | PASS |
| s2 | **클라이언트 크기 제한으로 DLQ 발행 실패 주입** → committed 없음 | PASS |
| s3 | 발행 실패 제거 후 재시작 → 재전달·격리 완료, committed **3** | PASS |
| s4 | **알림 발행 실패 → 원본 DLQ 격리 성공 → offset 진행 → 명시적 재처리** → event_id 동일 | PASS |

**발행 실패는 `max_request_size=1`, 즉 클라이언트 쪽 크기 제한으로 주입했다.**
`send()`가 브로커에 닿기도 전에 실패하므로 **발행이 확실히 실패한 상태**를 만든다.
그래서 검증된 것은 **"발행 실패 시 실제 Kafka의 offset·재전달"**뿐이다 —
**브로커 장애·timeout 복구·발행 성공 여부가 불확실한 상황은 검증하지 않았다.**
더 약한 주입이라는 것을 알고 고른 것이다(도달 불가 브로커는 생성자가 5분씩 블록했다).

**s4는 자동 복구가 아니다.** 감지기에는 재시도가 없다 — 알림 발행이 실패하면 원본이
DLQ로 격리되고 offset은 진행하며, **그 시점에 알림은 나가지 않는다.** 그 뒤 사람이
`dlq.py replay`로 되돌려야 한다(Runbook 2-2절). 이 시나리오는 같은 원본을 다시 주입해
그 재처리를 흉내낸 것이고, replay 도구 자체를 돌린 것은 아니다.

### 5-4-b. 중복 방지의 **전제**를 확인했다

"`UNIQUE(event_id)`가 막는다"는 **같은 원본이 같은 event_id를 만든다**는 전제 위에서만
성립한다. 매번 새 ID면 UNIQUE 인덱스는 아무것도 막지 못한다. **지금까지 아무도 안 봤다.**

같은 원본을 두 번 처리해 **고유 event_id 1개**를 확인했다(s4 + 단위 테스트 2건).
`event_id`는 `vehicle_id|timestamp|anomaly_type|field|detector`의 SHA-256이고
**`detected_at`은 키에 없다.** Java `AnomalyService.resolveEventId`도 같은 다섯 필드다.

**여기서 확인한 범위는 "같은 원본 재처리에서 event_id가 동일하다"까지다.**
DB 행 멱등성과 방송 중복 차단은 기존 증거로 연결한다 —
`RESULT_20260905_alert_replay.md`(같은 DLQ 2회 되돌림, 행 증가 0)와
`consumeAnomalyAlerts_중복이면_브로드캐스트안함`.

### 5-5. DLQ 분류는 이름이 아니라 출처로 가른다

조사에서 "감지기 예외가 전부 `unknown`"을 찾고 **전부 영구로 넣으려 했는데 그게 틀렸다.**

| 예외 | 분류 | 왜 |
| --- | --- | --- |
| `ContractViolation` | **permanent** | 검증기가 명시적으로 만든 것. payload의 성질이라 되돌려도 같은 자리에서 실패한다 |
| `TypeError`/`KeyError`/`ValueError`/`AttributeError` | **unknown** | 계약을 통과한 뒤 나오면 **구현 버그일 수 있다.** 버그면 고친 뒤 재처리가 성공하므로 영구가 아니고, 고치기 전에는 계속 실패하므로 일시도 아니다 |
| `KafkaTimeoutError`/`NoBrokersAvailable` | **unknown** | **발생 위치에 따라 다르다.** 알림 발행 중이면 브로커가 이미 받았을 수 있어 재처리가 중복 알림을 만든다(다만 `UNIQUE(event_id)`로 행은 안 는다 — 2026-09-05 확인). DLQ 발행 중이면 애초에 DLQ에 레코드가 안 남는다 |

`unknown`은 자동 재처리에서 빠지고 사람이 본다. **모르는 것을 안다고 적는 것보다 낫다.**

### 5-6. 사유 선택 순서 — payload 필드 순서에 흔들리던 것을 고쳤다

측정해보니 **같은 오류 조합인데 필드 순서만 바꾸면 다른 사유가 나왔다.**

```
{"zzz":1, "speed":"fast"}  →  UNKNOWN_FIELD
{"speed":"fast", "zzz":1}  →  TYPE_MISMATCH
{"zzz":1, "speed":         →  UNKNOWN_FIELD   ← 잘린 JSON인데 필드명 문제로 보고
```

마지막이 제일 나쁘다 — 운영자가 존재하지도 않는 필드명을 고치러 간다.
Jackson도 `json.loads`도 문서를 앞에서부터 읽다가 **처음 만난 문제**에서 멈추기 때문이다.

확정한 순서(양쪽 동일):

1. `MALFORMED_JSON` — 문서 전체가 JSON으로 읽히는가
2. `TYPE_MISMATCH` — 최상위가 객체인가 (배열·숫자·문자열·`null` 거부)
3. `UNKNOWN_FIELD` — 계약에 없는 필드 (**이름순** 전부 보고)
4. `TYPE_MISMATCH` — 각 필드의 타입
5. `PAYLOAD_VALIDATION_FAILED` — 값의 범위 (위반은 **정렬**해서 보고)

Java는 `readTree` → 최상위 검사 → unknown 검사 → `treeToValue` → Bean Validation으로
단계를 나눴다. **파싱이 두 번 도는 것은 아니다** — 트리에서 바인딩하므로 렉싱은 1회다.

### 5-6-a. 항목 점검에서 **갈림을 하나 더 찾았다** — trailing 내용

단계적 파싱으로 바꾸면서 trailing JSON·중복 필드 정책이 안 바뀌었는지 나란히 재봤다
(예전 `readValue` 경로 vs 지금 `decode`). **정책은 안 바뀌었다** — 둘 다 trailing을
무시하고, 중복 필드는 마지막이 이긴다.

**그런데 그 "안 바뀐 정책"이 Python과 달랐다.**

| 입력 | Java(예전·지금 동일) | Python |
| --- | --- | --- |
| `{...} {"speed":1}` | **통과** | 거부 `MALFORMED_JSON` |
| `{...} garbage` | **통과** | 거부 `MALFORMED_JSON` |
| `{...}   `(공백) | 통과 | 통과 |
| `{"speed":1,"speed":2}` | 마지막 승 | 마지막 승 |

잘린 메시지가 다른 메시지 뒤에 붙는 것이 실제 시나리오라 **받아들이면 안 되는 쪽**이 맞다.
telemetry 매퍼에만 `FAIL_ON_TRAILING_TOKENS`를 켰다(전역 Boot 매퍼는 그대로).

**빈 문서와 `null` 리터럴도 갈렸다.** Java가 둘 다 `TYPE_MISMATCH`였는데,
빈 문서는 "최상위가 null"이 아니라 **문서가 없는 것**이라 `MALFORMED_JSON`이 정확하다.
Python(`json.loads("")` → JSONDecodeError)에 맞췄다.

이 둘을 고치고 fixture가 53칸 → **61칸**이 됐고 **61/61 일치**다.

### 5-7. 언어 차이를 fixture에 넣었다

| 칸 | 무엇이 다른가 |
| --- | --- |
| `speed_boolean_true` / `_false` | Python에서 `bool`은 `int`의 하위 타입이라 `float(True)`가 1.0이다. 그냥 두면 **조용히 통과**하고, `false`는 **0.0**이라 정지 상태로 저장될 뻔했다 |
| `speed_nan_literal`, `speed_infinity_literal` | Python `json.loads`는 `NaN`/`Infinity` 리터럴을 **기본으로 받는다**. JSON 표준에 없고 Jackson은 거부한다 — `parse_constant`로 막았다 |
| `timestamp_lowercase` / `_no_offset` / `_space` | 3-2-b의 표 |

**이 칸들을 빼면 두 구현이 갈라져도 안 드러난다.** `test_shared_fixtures.py`가 목록의
존재 자체를 단언한다 — 줄면 커버리지가 준 것이다.

### 5-8. 테스트

| 대상 | 어디 | 결과 |
| --- | --- | --- |
| 저장 계약 + 사유 순서 + 강제 변환 | `TelemetryContractTest` | 통과 |
| 공유 fixture(저장 칸) | `SharedFixtureContractTest` | 54건 |
| 공유 fixture(감지 칸) + 언어 차이 | `test_shared_fixtures.py` | 66건 |
| 운영 소비 루프(가짜 Kafka) offset·복구 | `test_consume_loop.py` | 10건 |
| **실제 Kafka** offset·재전달·발행 실패 | `load-test/anomaly-contract-kafka/` | **4/4 PASS** |
| 두 저장 입구의 사유 순서·timestamp 계약 | `BothEntrancesSameContractTest` | 통과 |
| DLQ 분류 | `dlq-tools/test_dlq.py` | 25건 |
| 전체 | Java **248** / anomaly-detector **159** / dlq-tools **25** | **실패 0, skip 0** |

## 6. 한계 — 여전히 안 잰 것

- **ML 경로는 껐다**(`ML_ENABLED=false`). 계약을 통과한 payload만 `update_batch`에
  들어가므로 이전보다 안전해졌지만, **ML이 켜진 상태로 재지 않았다.**
- **부하 중에 재지 않았다.** 실제 파이프라인 확인은 6건 주입이다. 계약 검증이
  처리량에 주는 영향은 **미측정**이다 — 레코드마다 정규식 두 개와 dict 순회가 는다.
- **webhook 발행 실패 경로는 안 봤다.** 소비 루프 테스트는 Kafka 발행만 실패시킨다.
- **각 확인 1회**다. `docs/evidence-policy.md` 기준으로 "그 조건에서 관찰됨"이다.
- **`toPoint()`의 `finite()` 검사가 이제 완전히 잉여일 수 있다.** timestamp와 범위가
  계약 단계로 올라가면서, 계약을 통과한 payload가 `toPoint()`에서 실패할 경로가
  남아 있는지 **확인하지 않았다.** 지우기 전에 재야 한다.
- **저장 경로의 사유별 지표는 아직 없다**(P0-2b). 감지 경로에만 넣었다.
- **감지기의 재시도 예산은 그대로 없다.** 저장 경로는 180초 예산을 쓰는데 감지기는
  알림 발행이 한 번 실패하면 바로 DLQ다. 계약과 별개 설계 항목이고 근거가 아직 없다.
- **`x-dlq-source-path` 헤더를 재처리 도구가 아직 안 쓴다.** 넣어두기만 했다.
- **실제 Kafka 검증은 각 시나리오 1회, 파티션 1, 단일 인스턴스**다. 리밸런싱 중 커밋은
  안 봤다. 발행 실패도 `max_request_size`로 만든 것이라 브로커 장애·네트워크 단절과
  재시도 동작이 같은지는 확인하지 않았다.
- **trailing 내용을 이번에 막았다**(`FAIL_ON_TRAILING_TOKENS`). 예전에는 `{...} garbage`가
  **통과**했고 Python은 거부해서 두 경로가 갈렸다. 저장 경로의 기존 데이터 중 이런 payload가
  통과해 저장된 것이 있는지는 **소급 조사하지 않았다.**
