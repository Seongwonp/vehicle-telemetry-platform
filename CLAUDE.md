# Telemetrix 데스크탑 작업 인수인계

> 마지막 정리: 2026-09-12
>
> 기준 커밋: `2e12b43d1f960067d442469c8ff786a5ba492119` (origin/main, CI success)

## 이 프로젝트에서 가장 중요한 것

Telemetrix는 차량 텔레메트리 파이프라인의 기술 개수를 늘리는 프로젝트가 아니다.
메시지 유실, 중복, 순서, offset commit, 장애 격리, 복구, 관측 가능성을 실제 코드와
반복 가능한 실험으로 증명하는 백엔드 포트폴리오다.

새로운 서비스, 데이터베이스, Kubernetes를 먼저 추가하지 않는다. 현재 위험을 재현하고,
성공 기준을 정하고, 가장 작은 변경으로 닫은 뒤 원본 증거를 남긴다.

## 작업 시작 전 읽을 문서

1. `docs/current-state-audit-2026-09-09.md` — 현재 평가, 위험, 주장-증거, 8주 계획
2. `docs/roadmap.md` — 완료/미검증 항목과 다음 작업 선택 기준
3. `docs/evidence-policy.md` — 몇 회 실행해야 어떤 표현을 쓸 수 있는지
4. `docs/issue-guidelines.md` — GitHub Issue 작성·종료 기준
5. 해당 작업의 ADR, Runbook, `load-test/<scenario>/RESULT_*.md`

`README.md`는 포트폴리오 독자를 위한 요약이다. 세부 작업 상태의 기준은 위 문서다.

## 현재 상태 요약

### 검증된 것

- `vehicle_id`가 telemetry topic의 Kafka partition key다.
- 저장(`telemetry-storage-group`)과 이상 감지(`anomaly-detector-group`)는 서로 다른
  Consumer Group이며, 이상 감지 lag이 200만 건 이상 밀려도 저장 경로가 따라갔다.
- 저장 listener는 InfluxDB 성공 또는 DLQ 발행 성공 뒤에만 수동으로 offset을 커밋한다.
- DLQ 발행 결과를 확인하며, DLQ 발행 실패 시 원본 offset을 넘기지 않는다.
- 초 단위 InfluxDB timestamp 충돌로 약 50%가 덮어써진 문제를 밀리초 정밀도로 수정했다.
- MQTT broker 90초 장애에서 재연결 상한과 queue 크기를 조정한 뒤 해당 조건 유실 0을 확인했다.
- storage consumer 강제 종료·재전달과 anomaly alert 재처리의 멱등성을 실험했다.
- 부하 중 저장 인스턴스 제거를 3회 실행했고, 정적 멤버십 ON에서 재할당 46/52/54초,
  정지 파티션 6개, topic=Influx rows로 유실 0을 확인했다.
- 정적 멤버십 OFF 대조군 1회에서는 재할당 9초, 정지 파티션 0개를 관찰했다.
- 같은 조건의 저장 처리량이 크게 흔들려 `약 57K msg/s ceiling` 주장은 철회했다.
- GitHub Actions는 Java/Testcontainers/Python/Compose smoke와 Flutter format/analyze/test를 수행한다.
- Redis를 내리면 경로별로 **갈린다** — 조회는 통과(fail-open, 약 2초), 진단·로그인은
  503 `REDIS_UNAVAILABLE`, liveness는 8ms에 200. 적용 전에는 전부 60초 뒤 500이었다.
  각 1회, dev 프로파일, 무부하 — `load-test/redis-outage/evidence/20260912-123533/`.

### 아직 말하면 안 되는 것

- exactly-once 또는 모든 조건에서 무손실
- Kafka/시스템 전체 고가용성: broker 1대, replication factor 1이다.
- 운영 환경 최대 처리량 57K msg/s: 반복 결과가 크게 흔들려 숫자 주장을 철회했다.
- 모든 비정상 schema가 DLQ로 간다: 일부 누락/변환 경계가 아직 열린 상태다.
- 앱 실기기 동작 완료: widget/CI는 통과하지만 실제 기기 E2E는 미검증이다.
- 실제 OBD-II 연결 완료

## 다음 작업 순서

순서를 건너뛰지 않는다. 각 작업은 별도 커밋과 검증 기록으로 닫는다.

### 1. ~~P0 — evidence checksum 이식성 복구~~ — **완료(2026-09-09)**

감사가 지목한 대로였다. clean clone 전수 검사에서 **39개 중 3개**가 깨져 있었다
(전부 `.prev_offsets`, 전부 CRLF). 확장자 없는 숨김 파일이 `.gitattributes` 목록 밖이라
`core.autocrlf=true`가 적용됐고, **작업 트리에서는 원본 LF 파일이 남아 있어 안 보였다.**

git blob 해시가 manifest와 일치하므로 **증거는 처음부터 옳았고 checkout만 망가뜨렸다.**
그래서 evidence를 고치지 않고 규칙으로 살렸다.

- `.gitattributes`: `load-test/**/evidence/** text eol=lf` (확장자별 목록은 뚫린다)
- `load-test/lib/verify_evidence.sh`: 전수 검사. 통과 0 / 실패 1, CI가 본다
- `evidence_finish`: 숨김 파일 제외 + 생성 직후 자체 검증(`checksum_selfcheck.txt`)
- 실험 스크립트의 작업 파일을 evidence 밖으로

수정 전 3 실패 / 39 → 수정 후 **0 실패 / 39**(Windows clean clone, `autocrlf=true`).
`docs/verification/2026-09-09-evidence-checksum.md`.

**남은 것**: macOS 미확인. (Windows clean clone과 Linux CI 모두 39/39 통과 확인됨.)

### 2. ~~P0 — strict telemetry schema~~ — **완료(2026-09-09), E2E까지. 남은 갈래는 백로그**

감사가 "구현 전에 decision table과 실패 fixture부터"라고 한 그 단계를 끝냈다.
**표는 코드를 읽고 쓴 것이 아니라 fixture 15건으로 잰 것이다** —
`backend/src/test/java/com/telemetry/domain/TelemetryPayloadBoundaryTest.java`.
결정표: [`docs/telemetry-schema-decision-table.md`](docs/telemetry-schema-decision-table.md).

측정하면서 **테스트가 앱과 다른 매퍼를 쓰고 있는 것**을 잡았다. `new ObjectMapper()`는
`FAIL_ON_UNKNOWN_PROPERTIES`가 켜져 있고 Boot 자동 구성 매퍼는 꺼져 있다.
안 잡았으면 **운영과 다른 것을 잰 표**를 정책 근거로 쓸 뻔했다.

측정된 것 중 제일 나쁜 칸:

| 이상 입력 | MQTT | Kafka 직접 | 저장되는 값 |
| --- | :---: | :---: | --- |
| `speed` 필드 없음 / `null` | **통과** | **통과** | **0.0** ("시속 0으로 주행 중") |
| `gps.lat` 없음 | **통과** | **통과** | **lat=0** (인도네시아 앞바다의 실재 좌표) |
| 필드명 오타(`sped`) | **통과** | **통과** | **speed=0.0**, 오타 필드는 조용히 버려짐 |

**어제 내가 `rpm: 2000.7`과 `dtc [null]`을 "눈에 띄니 안 고친다"로 닫은 근거가 여기서
무너진다** — 바로 옆 칸들은 눈에 안 띈다. `0.0`은 이상해 보이지 않는다.

**2단계(구현) 완료.** 순서를 지켰다 — 공통 decoder → `Double + @NotNull` → 범위·DTC → unknown 필드.
뒤집으면 Kafka 경로가 검증을 안 거치는 상태에서 wrapper가 되어 `toPoint()`에서 NPE가 난다.

| 무엇 | 어디 |
| --- | --- |
| 공통 decoder | `domain/TelemetryDecoder.java` — 두 입구가 같이 쓴다 |
| 거부 사유 | `TelemetryContractException` 4종. 두 입구가 같은 코드를 쓴다 |
| 계약 | `VehicleTelemetry` 애너테이션(숫자는 결정표가 기준) |
| DLQ 분류 | `dlq-tools/dlq.py`에 새 예외를 `permanent`로 추가 |

**전역 Boot 매퍼는 안 바꿨다.** decoder가 앱 매퍼를 `copy()`해서 telemetry에 필요한
`FAIL_ON_UNKNOWN_PROPERTIES`만 켠다. 테스트도 `TestDecoders`로 **운영과 같은 decoder**를 쓴다.

**검증**: `ValidationBehaviorProbeTest`(8) — 이 버전들의 실제 동작을 재서 설계 근거로 삼았다.
`TelemetryContractTest`(34) — 변경 전과 같은 fixture + 적대적 입력 + 강제 변환.
`BothEntrancesSameContractTest`(12) — **실제 핸들러 두 개**로 동일 판정 확인.
`dlq-tools/test_dlq.py`(14) — 분류 회귀(Python).
전체 **173건 통과, skip 0**.

**완료 범위는 "MQTT·Kafka 저장 입구의 공통 계약 구현 + dev 프로파일 E2E 1회"다.**
mTLS·부하·이상 감지 경로는 **완료에 넣지 않는다.**

**파이프라인 E2E — 21/21 PASS**(`evidence/20260909-145736`, 무부하).
시나리오 10종 × 입구 2 + 혼합 배치. `rpm 2000.7` 양쪽 보존, 106°C 양쪽 저장 +
이상 알림 1건씩, 거부 14건이 각 DLQ로 정확히 귀속(kafka 8 = 거부 7 + 혼합 1, mqtt 7).
**"거부됐다"로 끝내지 않고 DLQ 레코드의 키와 사유를 맞춰봤다** —
`collect_dlq_attribution.sh`가 `derived/`에 **별도 매니페스트**로 남긴다(원본과 섞지 않는다).
`load-test/schema-contract/RESULT_20260909_contract_e2e.md`.

**offset 보호가 안 깨졌다는 근거를 테스트 이름으로 연결해뒀다**(RESULT 문서의 표) —
DLQ 발행 실패는 `dlq전송실패_offset미커밋`, 저장 실패는 `consumeForStorage_저장실패_재시도유도`,
실제 브로커 쪽은 `KafkaDlqContractTest`·`KafkaStorageFailureContractTest` 4종.

**여기서 2회차 실행이 통째로 무효였고 그게 이 작업에서 제일 값이 나온 지점이다.**
차량 ID를 `-KAFKA-01`로 만들었더니 21자가 되어 계약(20자)을 넘겼다. 19칸 중 15칸이
PASS로 보였는데 **그중 6칸은 시나리오가 아니라 ID 길이 때문에 거부된 거짓 PASS**였다.
FAIL 3칸이 없었으면 성공으로 읽었을 것이다. 원인을 확정한 건 판정표가 아니라 DLQ 헤더다.
같은 날 아침에 적은 **"결과는 집계가 아니라 증거 파일에서 읽는다"**를 오후에 또 겪었다 —
이번엔 집계가 아니라 **내가 만든 판정 스크립트**였다. **판정이 PASS를 돌려주는 것과
그 PASS가 옳은 것은 다르다.**

**최종 점검에서 구멍을 하나 더 찾았다.** "레코드별 try/catch니 안전하다"로 끝내지 않고
적대적 입력을 직접 던졌더니 **`null` 네 글자**가 걸렸다. `readValue`가 예외 없이 null을
돌려주고, 그걸 `validate()`에 넘기면 Hibernate Validator가 `IllegalArgumentException`을
던진다 — **계약 예외가 아니다.** MQTT 입구는 그걸 안 잡아 **거부가 아니라 전파**됐고,
Kafka 쪽은 잡히지만 `dlq.py`가 `unknown`으로 분류했다. decoder에 null 가드를 넣어 막았다.
**입구가 둘인데 고친 자리는 한 곳** — 공통 decoder를 만든 값이 여기서 나왔다.

**"strict schema"라는 표현은 쓰지 않는다.** `{"speed": "87.3"}`은 **통과하고** 87.3으로
저장된다. Jackson 강제 변환을 **끄지 않기로 한 결정**이다(값이 바뀌지도 잃지도 않으므로).
strict한 건 **필드 집합과 값의 범위**이지 표현 타입이 아니다. 결정표 3절에 표로 적었고
그 표의 각 칸을 테스트로 만들었다.

**설계 중 예상이 한 번 틀렸다**: `@DecimalMin/@DecimalMax`가 NaN을 **거부한다**
(예상은 "못 잡는다"였다). 안 쟀으면 검증 계층에 불필요한 유한값 검사를 하나 더 넣을 뻔했다.

**남은 것** (둘 다 `docs/roadmap.md`에 등록했다 — 이번에 구현하지 않았다):
- **P0-2a — 완료(2026-09-09).** `docs/anomaly-path-contract.md`.
  **정책은 공통 입력 계약 유지다** — 필수 센서 누락/null, 범위 밖, 타입 오류, unknown
  필드는 **감지 이전에 거부**한다. 부분 감지는 도입하지 않았다.
  `speed: 300`은 과속 임계를 넘지만 **계약 밖이라 입력 오류**이고 201km/h가 차량 이상이다.
  **"입력 오류"와 "차량 이상"을 가르는 것은 계약 경계이지 임계값이 아니다.**
  결과: fixture 61칸에서 **갈림 0**(적용 전 8칸), Java·Python **61/61 같은 판정**.
  실제 파이프라인에서 사유 4종이 각각 격리되고 Prometheus 지표가 오르는 것까지 확인했다
  (`load-test/schema-contract/evidence/20260909-P0-2a-detector/`).
  **소비 루프는 두 층으로 쟀다** — `main()`을 그대로 돌린 **단위 테스트(가짜 Kafka)**로
  분기를 보고, **실제 Kafka 통합**(`load-test/anomaly-contract-kafka/`, **4/4 PASS**)으로
  브로커의 committed offset·재전달을 봤다. **이 둘의 범위를 흐리면 안 된다.**
  **발행 실패는 클라이언트 크기 제한(`max_request_size=1`)으로 주입했다** — 발행이
  확실히 실패한 상태다. **브로커 장애·timeout 복구·in-doubt(발행 성공 여부 불확실)는
  검증하지 않았다.**
  DLQ 발행 실패에서 기대가 틀렸고 **실제가 더 안전했다**(아무것도 커밋 안 됨 → 배치 전체 재전달).
  **중복 방지의 전제도 확인했다** — 같은 원본이 같은 `event_id`를 만든다(`detected_at`은
  키에 없다). 이게 아니면 `UNIQUE(event_id)`는 아무것도 못 막는다.
  **trailing 내용도 갈리고 있었다** — `{...} garbage`를 Java가 받고 Python이 거부했다.
  `FAIL_ON_TRAILING_TOKENS`로 막았다.
  **분류는 이름이 아니라 출처로 가른다** — `ContractViolation`만 영구고,
  `TypeError`/`KeyError`는 구현 버그일 수 있어 `unknown`, Kafka 타임아웃은 발생 위치에
  따라 다르다. 처음에 "전부 영구로 넣자"고 기울었다가 뒤집혔다.
  **`timestamp` 형식이 계약으로 들어오면서 저장 경로 동작도 바뀌었다** —
  잘못된 타임스탬프가 `toPoint()`가 아니라 계약 단계에서 거부된다.
  **사유 선택 순서도 고정했다**: 예전에는 payload 필드 순서에 따라 사유가 달라졌고
  잘린 JSON이 `UNKNOWN_FIELD`로 보고됐다.
- **P0-2b — 완료(2026-09-09).** `docs/rejection-metrics-design.md`.
  세 입구(MQTT·Kafka 저장·감지기)가 같은 이름·같은 라벨로 사유별 거부를 올리고,
  **실제 Prometheus에서 `sum by (entrance, reason)`으로 구분 조회**되는 것을 확인했다.
  **네 단계를 구분한다** — 거부 **판정** / DLQ 발행 **시도** / **성공 확인** /
  **실패·timeout 관찰**(+그중 **발행 여부 불명**). **빼서 다른 뜻을 만들지 않는다.**
  초안에서 `invalid − published`를 "미격리 건수"로 정의했다가 지적으로 고쳤다 —
  처리 단계·집계 대상·재시도 횟수가 다르고 재시작 시점도 다르다.
  **timeout은 "발행되지 않았다"가 아니다** — 브로커가 받았는지 모른다.
  **고유 건수는 입구마다 식별 범위가 다르다** — Kafka 원본은 DLQ 헤더의
  `(origin-topic, partition, offset)`이 고유 키지만, **MQTT 거부에는 그 식별자가 없어
  고유 건수를 셀 수 없다.** payload 해시(밀리초 충돌로 정상 메시지를 접는다)나
  MQTT packet ID(세션 안에서만 유일)를 고유 ID로 쓰지 않는다.
  기존 알림이 보는 `dlq.published`/`publish.failures`는 **이름·의미 그대로** 두고 추가만 했다.
  남은 것: 알림 임계(정상 구간 미측정), 부하 중 영향, MQTT 고유 건수 식별.
- **DLQ Runbook은 갱신했다** — §2-1에 `TelemetryContractException` 4종 사유 코드,
  영구 분류, 사유별 집계 명령(실제로 돌려서 확인), 고치고 되돌리는 절차를 넣었다.
  분류 회귀는 `dlq-tools/test_dlq.py`가 막는다.
- 기존 저장 데이터의 `speed=0`·`lat=0`은 진짜와 누락을 구분할 수 없다. 소급 정리 안 함.
- 의존성 업그레이드는 이번 범위에 넣지 않았다. 버전이 바뀌면 참고자료 문서의 1.3~1.6을
  다시 확인해야 한다.

**범위를 확정했다(2026-09-09, 지적 반영).** 처음에 `속도 -50~500`, `온도 -60~300` 같은 값을
"역산했다"고 썼는데 **역산이 아니라 감지 임계값 위에 여유를 얹어 고른 것**이었다.
기준을 **OBD-II PID 표현 범위**로 바꿨다 — speed `0~255`, rpm `0~16383.75`,
engine_temp(냉각수, PID 05) `-40~215`, battery(제어 모듈 전압, PID 42) `0~65.535`,
throttle/fuel `0~100`. 원칙도 "물리적으로 불가능한 값 거부"에서
**"입력 계약 밖의 값 거부, 계약 안의 이상치는 감지"**로 고쳤다.

**`rpm`은 `Integer`가 아니라 `Double`이다.** OBD-II RPM은 0.25 단위라 `2000.25`가 유효하다.
확인해보니 **파이프라인의 나머지는 이미 전부 double**이다 — 저장(`toPoint`)과 응답 DTO가
둘 다 `(double)` 캐스팅한다. DTO의 `int` 하나가 유일한 축소 지점이었고,
거기서 자른 값을 다시 double로 넓혀 저장하고 있었다.

**좁히는 변경이라 우리 도구가 깨지는지 먼저 쟀다** — 시뮬레이터 정상 주행, **이상값 주입**
(temp 106~115, rpm 6100~7000, speed 201~230), 부하 도구가 전부 새 범위 안에 든다.
다만 **속도 여유가 25km/h뿐**이라, 주입 상한을 255 위로 올리면 그 메시지는 거부되어
이상 감지에 도달하지 못한다.

### 3. ~~P1 — Redis 장애 정책~~ — **정책 확정·구현 완료(2026-09-12). 남은 건 측정이다**

[`docs/redis-failure-policy.md`](docs/redis-failure-policy.md) /
[Runbook](docs/runbook/redis-outage.md).

감사 문서가 "500이 된다"까지 적은 것과 실제가 달랐다 — **500이 아니라 60초 뒤 500**이었고
(`application.yml`에 timeout이 없어 Lettuce 기본값 60초), **로그인도 죽었다**
(인터셉터에서 제외돼 있지만 `AuthController`가 Redis 컴포넌트를 직접 부른다).

**정책은 한 방향으로 묶지 않았다. 그게 이 작업의 핵심이다.**

| 경로 | 정책 | Redis 중지 중 |
| --- | --- | --- |
| 일반 조회 | **fail-open** | **200** (약 2초) + `telemetry.ratelimit.failopen{route}` |
| 진단 | **fail-closed** | 503 `REDIS_UNAVAILABLE` (약 4초) |
| 로그인 보호·refresh | **fail-closed** | 503 `REDIS_UNAVAILABLE` (약 2초) |
| `/actuator/health/liveness` | 외부 의존 제외 | **200** (8ms) — 재시작 대상 아님 |
| `/actuator/health/readiness` | Redis 포함 | 503 — 라우팅에서 제외 |

**조회 fail-open과 로그인 fail-closed가 반대 방향인 것이 요점이다** — 하나로 묶으면
조회를 살리려다 **Redis를 죽이는 것이 곧 brute force 방어를 끄는 방법**이 된다.

**읽을 때 틀리기 쉬운 것**(결정표 §8-6):
- `failopen`은 **"통과한 요청 수"가 아니다** — "일반 rate limit이 적용되지 않은 요청 수"다.
  진단 요청은 인터셉터를 둘 타서 **두 지표에 동시에 오른다.** 정상이다.
- **fail-open은 "빠르다"를 사지 않았다** — 통과하지만 Redis timeout까지 2초를 쓴다.
- **복구 시간이 관측마다 달랐다**(8초/30초). **원인 미확인.**

**남은 측정**: 30/90초 지속 장애, **부하 중 장애와 스레드 고갈 임계**(fail-open 도입으로
더 중요해졌다), fail-open 중 실제 남용 피해, 회로 차단기 필요 여부, WebSocket 실측.

### 4. P1 — 이벤트 상관관계

HTTP MDC traceId만으로는 MQTT→Kafka→InfluxDB/Python→PostgreSQL/WebSocket 한 레코드를
찾기 어렵다. OpenTelemetry 전체 도입보다 payload/header의 `event_id`를 먼저 검토한다.

### 5. P1 — Flutter 실기기 검증

앱 저장소: `../vehicle-telemetry-app`

- 앱의 `AGENTS.md`와 `CLAUDE.md`를 먼저 읽는다.
- Android 실제 기기 또는 에뮬레이터에서 로그인, refresh, WebSocket 재연결, stale,
  out-of-order, 동일 timestamp, 로그아웃을 검증한다.
- macOS integration test는 secure storage Keychain entitlement 문제로 로그인 단계에서 막힌다.
- 사용한 앱·백엔드 commit, SDK, device, API URL, 로그와 스크린샷을 기록한다.

## 데스크탑 시작 절차

### 저장소 동기화

```powershell
git checkout main
git pull --ff-only
git status --short --branch
git log -1 --oneline
```

사용자 변경사항이 있으면 덮거나 정리하지 않는다. 충돌 가능성이 있으면 작업을 멈추고
변경 파일을 먼저 확인한다.

### 백엔드 빠른 검증

```powershell
cd backend
./gradlew test --no-daemon
cd ..

python -m pip install -r anomaly-detector/requirements.txt pytest
$env:PYTHONPATH = "anomaly-detector"
pytest anomaly-detector/tests -q
```

Docker가 실행 중이어야 Testcontainers 계약 5종이 실제로 수행된다. skip이 있으면 전체 통과로
기록하지 않는다. CI는 계약 결과 파일 5개와 skipped 0을 별도로 강제한다.

### Compose 검증

```powershell
docker compose config --quiet
docker compose build
docker compose --profile simulator up -d
docker compose ps
curl.exe --fail http://localhost:8080/actuator/health
```

기본 Compose는 mTLS 8883이다. 평문은 명시적 dev override에서만 사용한다. 실험이 끝난 뒤
volume을 삭제하는 `docker compose down -v`는 사용자 요청 또는 실험 절차가 명확할 때만 한다.

### scale 실험 전 확인

- 파티션 3개에서 backend 하나의 concurrency 3이 이미 모두 차지한다.
- 저장 인스턴스를 늘리기 전에 필요한 파티션 수를 먼저 확보한다.
- 실행 중 파티션을 늘리면 consumer가 `metadata.max.age.ms`까지 새 파티션을 못 볼 수 있다.
- 저장 전용 인스턴스는 `MQTT_INGEST_ENABLED=false`여야 한다.
- 각 인스턴스의 `GROUP_INSTANCE_ID_BASE`가 고유해야 한다.
- 정적 멤버십을 끌 때는 `GROUP_INSTANCE_ID_BASE=` 빈 값을 사용한다. `${VAR:-default}`로
  바꾸면 빈 값이 기본값으로 돌아가므로 OFF 대조군이 조용히 무효화된다.

## 실험 규칙

1. 가설, 변경하지 않은 대조군, 성공 기준을 실행 전에 적는다.
2. 발행 시도 수가 아니라 가능한 경우 MQTT PUBACK 또는 Kafka end offset을 기준량으로 쓴다.
3. 집계 파일만 믿지 않고 원본 로그에서 표본을 대조한다.
4. 안정성 주장은 같은 조건 3회 이상에서만 한다.
5. 평균만 쓰지 않고 p95/p99, 변동 폭, 오류율, lag, 저장 성공량을 함께 남긴다.
6. 회차 사이 휴식, 실행 순서, 호스트 온도·클럭을 기록한다.
7. 실패한 실행과 잘못된 측정 도구도 지우지 않고 무효 사유를 적는다.
8. `evidence/`를 삭제하지 않는다. 임시 파일도 증거인지 확인한 뒤 다룬다.

## 문서 역할

| 문서 | 역할 |
| --- | --- |
| `README.md` | 문제 → 설계 결정 → 검증 결과 → 한계의 포트폴리오 요약 |
| `docs/current-state-audit-2026-09-09.md` | 현재 평가, 위험, 주장-증거, 8주 계획 |
| `docs/roadmap.md` | 완료/진행/미검증 작업의 기준 상태 |
| `docs/telemetry-schema-decision-table.md` | **입력 계약의 단일 기준** — 변경 전 측정, 정한 범위, 변경 후 검증 |
| `docs/references/telemetry-engineering-reference.md` | 참고 자료 → 채택한 정책 → 코드·검증 연결. 자료의 성격 구분 |
| `docs/architecture-decisions.md` | 선택하거나 선택하지 않은 이유와 trade-off |
| `load-test/**/RESULT_*.md` | 한 실험의 조건, 결과, 적용 범위와 한계 |
| `load-test/**/evidence/` | 재계산 가능한 원본 |
| `docs/runbook/` | 장애 판단과 복구 절차 |
| `docs/devlog.md` | 어디서 틀렸고 무엇이 그 오류를 드러냈는지 |

코드에 긴 실험 서사를 반복하지 않는다. 코드 주석은 안전성 불변식과 삭제하면 안 되는 이유만
남기고, 상세 수치와 과정은 RESULT/ADR/devlog로 이동한다.

## 작업 완료 조건

- 관련 단위·계약·E2E 테스트가 범위에 맞게 통과한다.
- Docker 또는 기기가 없어 실행하지 못한 검증은 `미검증`으로 남긴다.
- 결과 문서에 commit, 환경, 명령, 원본 evidence, 성공 기준, 한계를 적는다.
- roadmap의 완료 상태와 남은 갈래를 갱신한다.
- 동작을 바꾼 결정이면 ADR과 Runbook을 확인한다.
- README에는 채용 독자가 이해할 핵심 결과만 반영한다.
- 사용자가 명시적으로 요청한 경우에만 commit/push한다.

## 기술 기준

- Java 17 / Spring Boot 3 / JUnit 5
- Python 3.11 / pytest
- MQTT QoS 1 / Kafka at-least-once
- PostgreSQL / InfluxDB / Redis
- Prometheus / Grafana / Docker Compose
- 비밀정보는 `.env`로 분리하고 추적하지 않는다.
- 실제 근거 없이 `고가용성`, `무손실`, `exactly-once`, `운영 준비 완료`라고 쓰지 않는다.
