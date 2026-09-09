# 이상 감지 경로의 입력 계약 정합성 (P0-2a) — 조사와 결정표

> 상태: **조사 완료, 정책 미확정. 감지기 동작은 하나도 안 바꿨다** (2026-09-09)
>
> 공유 fixture와 그걸 읽는 특성화 테스트만 넣었다(§4) — 현재 동작을 고정해서
> 정책이 정해지기 전에 조용히 달라지는 것을 막는 용도다.
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

## 1. 측정 결과 — 두 경로가 어떻게 갈리나

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

## 2. 실패했을 때 어떻게 되나 — 재시도·격리·복구

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

## 3. 정해야 할 정책 — 같아야 하는 것과 달라야 하는 것

**아직 정하지 않았다.** 아래는 결정해야 할 목록과 각 선택지의 대가다.

### 3-1. 두 경로의 역할이 다르다는 것부터 분명히 한다

| | 저장 경로 | 감지 경로 |
| --- | --- | --- |
| 목적 | 나중에 **다시 읽을 데이터**를 남긴다 | **지금 위험한 상태**를 알린다 |
| 틀렸을 때 대가 | 잘못된 값이 영구히 남고 소급 구분이 안 된다 | 오탐(귀찮다) 또는 미탐(위험하다) |
| 그래서 | **엄격한 게 맞다** — 못 믿을 값은 안 받는다 | **너무 엄격하면 위험을 놓친다** |

**두 경로가 무조건 같아야 한다는 전제부터가 틀렸다.** 예를 들어 `speed: 300`은
저장 계약 밖이지만 **감지 관점에서는 과속이 맞다.** 여기서 감지까지 막으면
"계약 밖 값이라 위험 신호를 버렸다"가 된다.

### 3-2. 결정 항목

| # | 질문 | 선택지 | 지금 기울어진 쪽 |
| ---: | --- | --- | --- |
| 1 | 필드 누락에 감지기는 어떻게 반응하나 | (a) 조용히 건너뛴다(현재) / (b) 지표를 올린다 / (c) DLQ | **(b)** — 실측 결과 알림·로그·지표 **전부 0**이라 정상과 구분되지 않는다. DLQ는 과하다(값 자체는 멀쩡할 수 있다) |
| 2 | 계약 범위 밖의 값(`speed: 300`)은 | (a) 감지한다(현재) / (b) 무시 / (c) 감지하되 표시 | **(c)** — 위험 신호는 살리되 "못 믿는 값"임을 알린다 |
| 3 | `dtc_codes` 형식 위반 | (a) 감지(현재) / (b) 저장과 같이 거부 | **(a)** — Java가 막은 이유(저장 후 코드 개수 구분 불가)가 감지에는 없다 |
| 4 | `vehicle_id`·`timestamp` 누락 | (a) 조건부 KeyError(현재) / (b) 항상 DLQ | **(b)** — 조건부는 조사가 불가능하다. 알림에 필수 필드다 |
| 5 | 감지기 예외의 DLQ 분류 | (a) 그대로 `unknown` / (b) 목록에 추가 | **(b)** — §2-1 |
| 6 | ~~공유 fixture를 누가 읽나~~ | (a) 양쪽 테스트 / (b) Python만 | **(a)로 정하고 이미 넣었다**(§4) — 동작을 안 바꾸므로 정책과 무관하다 |

**1번과 4번이 핵심이다.** 나머지는 그 둘이 정해지면 따라온다.

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

## 5. 이 조사의 한계

- **`process()`를 직접 호출해서 쟀다.** 실제 Kafka 소비 루프를 통과시킨 것이 아니다.
  루프의 DLQ·offset 처리는 **코드를 읽어서** 정리했고(§2) 주입으로 확인하지 않았다.
- **ML 경로는 껐다**(`ML_ENABLED=false`). ML이 켜져 있을 때 계약 위반 payload가
  `update_batch`에서 어떻게 되는지는 **안 쟀다.** 배치 전체가 실패하면 룰만 남는다는
  것은 코드에 적혀 있지만, 그게 이 fixture들에서 실제로 일어나는지는 미확인이다.
- **webhook 발행을 막고 쟀다.** 알림 전송 실패 경로는 안 봤다.
- **각 칸 1회**다. 판정은 결정적일 것으로 보이지만 반복하지 않았다.
- **offset 동작은 코드를 읽어서 정리했다**(§2 표). 로그와 판정은 실측했지만
  offset 커밋은 실제 소비 루프를 돌려 확인하지 않았다 — 정책을 정하고 구현할 때
  장애 주입으로 확인해야 한다.
- **성능 영향은 안 쟀다.** 정책 1번(b)처럼 지표를 올리면 레코드마다 비용이 는다.
- **21칸이 전부가 아니다.** 실제 장치가 보낼 수 있는 형태(초 단위 타임스탬프,
  다른 PID 해상도)는 담기지 않았다.
