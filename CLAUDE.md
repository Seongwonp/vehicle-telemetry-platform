# Vehicle Telemetry Platform — AI 협업 가이드

## 프로젝트 개요

차량(OBD-II 동글 또는 시뮬레이터)에서 발생하는 실시간 센서 데이터를 수집하고, 이상 감지 및 모니터링까지 처리하는 백엔드 플랫폼.

- **개발자**: Sungwon
- **목적**: 백엔드/서버 엔지니어 포트폴리오 (현대오토에버 등 모빌리티 IT 기업 지원)
- **개발 기간**: 약 10주

---

## 기술 스택

| 영역 | 기술 |
|------|------|
| 데이터 수신 | MQTT (Eclipse Mosquitto) |
| 메시지 큐 | Apache Kafka |
| 백엔드 API | Java 17 + Spring Boot 3 |
| 이상 감지 | Python 3.11 (룰 기반 + scikit-learn 선택) |
| 시계열 DB | InfluxDB |
| 관계형 DB | PostgreSQL |
| 캐시 | Redis |
| 모니터링 | Grafana + Prometheus |
| 차량 시뮬레이터 | Python 또는 C |
| 보안 | JWT, TLS/SSL, Rate Limiting, 이상 접근 감지 |
| 인프라 | Docker Compose, (선택) AWS EC2 |

---

## 디렉토리 구조

```
vehicle-telemetry-platform/
├── simulator/          # 차량 데이터 시뮬레이터 (Python/C)
├── broker/             # Mosquitto MQTT 브로커 설정
├── kafka/              # Kafka 설정 및 토픽 초기화 스크립트
├── backend/            # Spring Boot API 서버
│   ├── src/
│   └── build.gradle
├── anomaly-detector/   # Python 이상 감지 모듈
├── monitoring/         # Grafana + Prometheus 설정
├── docker-compose.yml
├── CLAUDE.md
└── README.md
```

---

## 개발 원칙

- **보안 우선**: 모든 통신은 TLS, 인증 없는 엔드포인트 금지
- **단계별 구현**: Phase 순서를 지켜서 개발 (파이프라인 → API → 이상감지 → 보안 → 배포)
- **실제 OBD-II 연동 고려**: 시뮬레이터와 실제 OBD-II 동글 전환이 쉽도록 인터페이스 분리
- **포트폴리오용**: 코드 가독성, README, 아키텍처 문서 품질 중요

## 작업 시작 전 기준 문서

Telemetrix는 기능 수보다 차량 데이터의 유실, 중복, 순서, 장애 격리, 복구와 관측
가능성을 실제 테스트로 증명하는 것을 우선한다. 새 작업을 시작하기 전에 아래 문서를
확인한다.

- 우선순위와 의도적으로 미루는 작업: `docs/roadmap.md`
- GitHub Issue 작성 및 종료 기준: `docs/issue-guidelines.md`
- 주장에 필요한 증거와 검증 상태 표기: `docs/evidence-policy.md`

Kubernetes, 서비스 추가 분리, 신규 저장소나 메시징 기술은 현재 문제와 측정 근거가
있을 때만 검토한다. 한 번의 성공 결과를 일반화하지 않고, 확인하지 못한 항목은
`미검증`으로 남긴다.

## 현재 최우선 작업 (2026-09-05)

**우선순위 1·2·3번을 모두 닫았다.**

- 1번 남은 갈래 둘 다 완료: (a) 밀리초 키 충돌을 재현했고(500건 → 1행) 임계값이
  차량당 1,000 msg/s, 현재 5 msg/s라 200배 여유여서 **고치지 않기로 판단**하고
  임계값을 코드에 남겼다. (b) 리밸런싱 재전달은 **40건(0.100%), 행 증가 0**.
- 2번 남은 갈래(이상 알림 DLQ 재처리 중복 검증): 행 중복 0. 대신 중복 *알림*을
  찾아 고쳤다.
- 3번 남은 갈래(MQTT 브로커 장애): **유실 129,445건(72.1%)을 찾아 0으로 고쳤다.**
  Paho 재연결 백오프 상한이 기본 128초였다.

추가로 **이상 알림 저장 경로를 배치화했다(ADR-022)** — 측정해보니 49 msg/s로
유입(193 msg/s)의 1/4만 처리하고 있었다. 배치 리스너 + 배치당 트랜잭션 1건으로
같은 부하의 lag이 **20,128 → 238**이 됐고, 4배 부하에서도 유입을 전량 소화한다.
DB 장애 시 DLQ 직행이던 것도 재시도(180초 예산)로 바꿔 텔레메트리 경로와 정책을 맞췄다.
같은 PostgreSQL 60초 장애를 다시 주입해 확인하니 **유실 6건 → 0건**이고,
DLQ에 남은 6건은 전부 이미 저장된 in-doubt 건이라 **복구가 필요한 알림이 하나도 없다.**
멱등성(재처리 2회에 행 증가 0)과 중복 알림 차단도 회귀 없이 유지된다.
결과: `load-test/anomaly-storage-throughput/`, `load-test/anomaly-dlq-idempotency/`.

PostgreSQL **300초** 장애도 재봤다 — DLQ 0건, 리밸런싱 0건, 유실 0건.
그 과정에서 배치화하며 내가 만든 버그(`toEntity()`가 트랜잭션을 열어 DB 장애 중
변환 단계에서 실패)를 찾아 고쳤고, **"180초 예산"이 벽시계가 아니라 백오프 합**임을
확인해 코드·문서 서술을 바로잡았다.

**남은 것은 대부분 실기기·실환경이 있어야 하는 것들이다.** 다음 후보:
7번(앱 실기기 검증 — 기기 필요), mTLS 프로파일에서 브로커 장애 재측정,
알림 저장 처리량 상한 확정(시뮬레이터를 다중 프로세스로 띄워야 잴 수 있다),
HikariCP `connectionTimeout` 30초 조정 여부.

---

## 이전 최우선 작업 (2026-09-04)

**Kafka 저장 실패 계약 검증 완료** — Docker가 있는 데스크톱에서 실행해
`KafkaStorageFailureContractTest` 3케이스가 skip 없이 통과했고(18.0초), 전체
`./gradlew test --no-daemon`도 96개 전부 통과(skip 0), CI(`e4effc2`)도 success다.
**5회 반복 실행에서도 5/5 통과라 간헐 실패(flakiness)는 관찰되지 않았다.**
실행 기록: `docs/verification/2026-09-04-desktop-docker-contracts.md`.

**우선순위 1번(강제 종료·재전달 시 InfluxDB 중복·덮어쓰기 수량 측정)도 완료** —
`docker kill` 후 재전달 68건(0.0873%)이 발생했으나 InfluxDB 행 수가 고유 키 수와
정확히 같아(77,929 = 77,929) **덮어쓰기로 흡수됨(멱등)**을 확인했다. 같은 밀리초
키 충돌은 이 부하에서 0건이라 ms 정밀도가 충분했다. 측정 도구와 절차는
`load-test/storage-integrity/`, 결과는 같은 폴더의 `RESULT_20260904_kill_redelivery.md`.

다음 작업은 아래 우선순위 2번부터다.

## 다음 우선순위

1. ~~consumer 강제 종료와 재전달 시 InfluxDB 중복·덮어쓰기 수량 측정~~ — **완료(2026-09-04)**.
   **같은 밀리초 키 충돌도 재현·판단 완료(2026-09-05)** — 같은 타임스탬프로 500건을
   보내면 **1행만 남는다**(499건이 에러·로그 없이 소실). 다만 임계값이 **차량당
   1,000 msg/s**(밀리초당 1건)이고 실측 유실률은 500→0.02% / 1,000→0.64% / 2,000→50.20%다.
   이 시스템은 차량당 5 msg/s라 **200배 여유**이고, 총량은 차량 수로 늘리므로
   (`vehicle_id`가 태그) 총 처리량과 무관하다. **그래서 고치지 않기로 했다** —
   시퀀스 태그는 포인트마다 시리즈를 만들어 인덱스를 무너뜨리고, `WritePrecision.US`는
   타임스탬프 계약(데이터 스펙 포함)을 바꿔야 한다. 대신 임계값을 `toPoint()` 주석에
   남겨 발행 주기를 줄이려는 사람이 먼저 보게 했다.
   도구·결과: `load-test/storage-integrity/ms_collision.py`,
   `RESULT_20260905_ms_collision.md`.
   **다중 인스턴스 리밸런싱 재전달도 측정 완료(2026-09-05)** —
   `anomaly-detector`를 3 → 1 → 3으로 스케일해 리밸런싱을 일으켰다.
   재전달 **40건(0.100%)**, PostgreSQL 행 증가 **0**, 유실 **0**.
   토픽 초과분·중복 event_id 발행분·`telemetry_anomaly_stored{result="duplicate"}`
   세 출처가 모두 40으로 일치했다. 오늘 고친 "insert 성공에만 브로드캐스트"가
   이 40건의 중복 알림도 걸렀다.
   도구·결과: `load-test/rebalance-redelivery/`.
   남은 갈래: 저장 경로(Java, `telemetry-storage-group`)는 `container_name`·포트
   고정 때문에 스케일이 안 돼 **미측정**, 커밋 배치 크기와 재전달률의 관계 **미측정**,
   실제 OBD-II 동글의 타임스탬프 정밀도 **미확인**(초 단위면 위 분석의 전제가 무너진다)
2. ~~DLQ 재처리 정책, 도구 및 운영 Runbook~~ — **완료(2026-09-04)**.
   DLQ 레코드에 실패 원인 헤더(`x-dlq-*`)를 붙이고(Java·Python 동일 규약),
   `dlq-tools/dlq.py`로 원인별 분류(transient/permanent/unknown)와 재처리를 한다.
   Runbook은 `docs/runbook/dlq-reprocessing.md`.
   **`vehicle-anomaly-alerts-dlq` 중복 알림 검증도 완료(2026-09-05)** —
   PostgreSQL 60초 장애 후 같은 DLQ 레코드를 커서를 바꿔 **두 번** 되돌려도
   행 증가는 0건이었고(`UNIQUE(event_id)` + `ON CONFLICT DO NOTHING`),
   행 수가 토픽의 고유 event_id 수와 정확히 일치했다(16,636 = 16,636, 유실 0).
   측정 도구·결과는 `load-test/anomaly-dlq-idempotency/`.
   **대신 결함을 하나 찾아 고쳤다** — 컨슈머가 `insertIfAbsent` 반환값을 버리고
   무조건 WebSocket 브로드캐스트를 해서, 재처리 때마다 이미 저장된 알림이 다시
   나가고 있었다(1회차 3건, 2회차 9건). `SaveResult(alert, inserted)`로 바꾸고
   지표 `telemetry.anomaly.stored{result=new|duplicate}`를 추가했다.
   현재 Flutter 앱은 이 토픽을 구독하지 않아 실피해는 없었다(잠재 결함).
   같이 알아낸 것: **PostgreSQL 장애는 DLQ를 거의 만들지 않는다** —
   컨슈머가 HikariCP `connectionTimeout`만큼 30초씩 붙잡혀서 60초 장애에 DLQ 9건뿐이고
   나머지는 lag으로 쌓인다. 그리고 그 9건 중 **3건은 이미 저장돼 있었다**
   (서버 커밋은 끝났는데 연결이 끊긴 in-doubt 트랜잭션).
   **300초 장애도 측정 완료(2026-09-05)** — DLQ 0건, 리밸런싱 0건, 유실 0건
   (행 43,347 = 토픽 고유 event_id 43,347). 그 과정에서 **내가 만든 버그**를 찾아
   고쳤다: `AnomalyService`의 클래스 레벨 `@Transactional(readOnly = true)` 때문에
   DB를 안 건드리는 `toEntity()`까지 트랜잭션을 열어, DB 장애 중 **변환 단계에서**
   실패해 레코드가 하나씩 DLQ로 갔다(DLQ 19건 = 고유 알림 8건).
   `NOT_SUPPORTED` + 빈 배치 early return으로 고쳤고 회귀 테스트 2건 추가.
   또 **"180초 예산"이 벽시계가 아님**을 확인했다 — `maxElapsedTime`은 백오프로 쉰
   시간의 합이라, 시도마다 HikariCP 30초를 기다리면 실효 내성이 약 8분이 된다.
   코드·yaml·Runbook의 "총 경과 시간" 서술을 바로잡았다.
   `connectionTimeout` 30초는 **줄이지 않기로 결정** — 원래 근거("줄이면 DLQ가 쌓여
   재처리 절차가 의미를 갖는다")가 사라졌고(300초 장애에 DLQ 0건), 오히려 그 30초가
   백오프 합을 천천히 차게 해 실효 내성 8분을 만들어낸다. 줄이면 DLQ로 더 빨리 간다.
   남은 갈래:
   8분을 넘는 장애에서 리밸런싱이 도는지 **미측정**,
   `vehicle-telemetry-mqtt-dlq`는 payload가 envelope이라 재처리 미지원,
   분류 목록(`TRANSIENT_MARKERS`/`PERMANENT_MARKERS`)은 지금까지 본 예외만 담고 있음
   (PostgreSQL 장애 예외 2종은 이번 실측에서 100% `transient`로 분류돼 문제없었다)
3. ~~MQTT/Kafka/InfluxDB 장애 주입과 복구 후 데이터 정합성 대조~~ — **완료(2026-09-05)**.
   **MQTT 브로커 장애까지 측정했고, 유실 129,445건(72.1%)을 찾아 0으로 고쳤다.**
   정답 기준을 만드는 게 절반이었다 — 시뮬레이터에 `PublishStats`를 넣어
   `publish()` 성공(= paho 큐 적재)과 PUBACK 수신(= 브로커가 받음)을 분리해서 센다.
   원인은 `MqttConfig`에 `setMaxReconnectDelay`가 없어 Paho 기본값 **128초**가 적용된 것.
   `cleanSession=false`라 그동안 브로커가 우리 세션 앞으로 큐잉하다
   `max_queued_messages`(10,000)를 넘기면 말없이 버린다 — 브로커 로그
   `Outgoing messages are being dropped for client telemetry-backend`가 그 순간이다.
   5초로 낮춘 뒤 재측정: 유실 0, 브로커 `$SYS` dropped 0, 복구 후 브로커 수신
   137,702 = backend 수신 증가분 137,702로 정확히 일치.
   결과는 `load-test/fault-injection/RESULT_20260905_mqtt_broker.md`, 회귀 방지는
   `MqttConnectOptionsTest`.
   남은 갈래: 종료 시 flush 45초 안에 못 간 3,542건(발행 측 한계, 파이프라인 유실 아님),
   재연결 실제 소요 시간 분해.
   **`max_queued_messages`도 측정·결정 완료(2026-09-05)** — 90초는 유실 0이었지만
   **300초로 늘리자 17,243건(4.7%)이 다시 버려졌다.** 원인이 다르다: 128초 백오프 때는
   *백엔드가 없어서*였고, 이번엔 2초 만에 붙었는데도 밀린 32만 건이 한꺼번에 쏟아져
   **구독자 드레인 속도를 잠깐 넘어섰기** 때문이다. 같은 증상에 원인이 둘이었고
   첫 번째만 고쳐둔 상태였다. 10,000 → **100,000**으로 올려 유실 0
   (InfluxDB 행 364,802 = 정답 기준 일치, dropped 0). 약 30MB.
   이걸로 유실이 없어지는 게 아니라 뒤로 밀릴 뿐이라, 진짜 방어선은
   `MqttBrokerDroppingMessages` 알림이다(이번에 실제로 값을 냈다).
   **mTLS 프로파일에서도 재측정 완료(2026-09-05)** — 100대 기준 유실 0,
   브로커 `$SYS` dropped 0, 재연결 약 2초로 TLS 핸드셰이크가 붙어도 5초 상한 안이다.
   여기서 **ACL 버그**를 찾았다: 코드는 `$SYS/broker/publish/messages/received`를
   구독하는데 `broker/config/acl`은 옛 토픽만 허용해, **운영 프로파일에서만**
   파이프라인 대조 대시보드 1단계가 비었다(평문은 ACL이 없어 안 드러났다).
   고친 뒤 복구 후 브로커 수신 70,810 = backend 수신 증가분 70,811로 확인.
   측정 도구도 둘 고쳤다 — 복구 판정을 TCP 확인으로(키 파일 0600이라 컨테이너 안에서
   mTLS 클라이언트로 못 붙는다), 시뮬레이터 최종 집계는 종료 마커를 기다린 뒤 읽도록
   (묵은 주기 로그를 읽어 "저장이 발행보다 많다"는 불가능한 결과가 나왔었다).

   (이전 기록 — InfluxDB·Kafka, 2026-09-04)
   InfluxDB·Kafka 90초 장애를 주입해 **둘 다 유실 0**을 확인했다(InfluxDB는 DLQ 재처리로
   84,615 → 161,356 완전 복구, Kafka는 spool이 전량 보관). 도구·결과는
   `load-test/fault-injection/`. **다만 복구 경로가 둘 다 실용적이지 않다** —
   아래 5번 항목 참고(재시도 예산은 그 뒤 180초로 바꿔 해결했다).
4. ~~spool 드레인 속도~~ — **완료(2026-09-04)**. `telemetry.spool.retry-batch`를
   설정으로 빼고 기본값을 100 → **10,000**으로 올렸다(계측 후 결정: 배치 100 = 19 msg/s,
   2,000 = 387 msg/s, 10,000 = 부하 정지 후 약 1분 내 완전 드레인). 스캔은 병목이
   아니었다 — 파일 15만 개에서 회당 121ms로 5초 주기의 2.4%다.
   지표 3종 추가(`telemetry.spool.pending/scan/drained`).
   남은 갈래: 드레인 중에는 새 메시지도 spool로 가므로 **드레인 − 유입**이 실제 감소
   속도인데, 배치 10,000의 이론치 2,000 msg/s는 유입 1,700 대비 여유가 300 msg/s뿐이다.
   더 키우려면 프로듀서 `buffer.memory`(기본 32MB)와 `max.block.ms`를 함께 봐야 하고
   **거기까지는 재지 않았다**
5. ~~재시도 예산 재검토~~ — **완료(2026-09-04)**. `FixedBackOff(1000L, 2L)`(3회/약 2초) →
   `ExponentialBackOff` + **재시도 예산 180초**(`telemetry.kafka.retry.budget-ms`).
   (이 값은 벽시계가 아니라 **백오프로 쉰 시간의 합**이다 — 아래 2번 항목 참고)
   같은 InfluxDB 90초 장애에서 **DLQ 76,878건(47.6%) → 0건**, InfluxDB 행이 토픽 수와
   정확히 일치(180,329)해 수동 재처리가 아예 불필요해졌다. 리밸런싱·백오프 소진 로그 0건.
   예전 값의 근거("한 레코드가 파티션을 막는다")는 이 코드에 해당하지 않았다 —
   레코드 단위 실패는 이미 개별 catch로 DLQ 처리되므로 에러 핸들러까지 오는 건
   의존성 장애뿐이다.
   남은 갈래: 영구 실패 배치가 이제 180초를 붙드는 경우 **미재현**,
   예산을 더 늘리려면 `max.poll.interval.ms`도 올려야 하는데 그러면 죽은 컨슈머 감지가
   느려지는 트레이드오프가 있고 **재지 않았다**
6. ~~발행량·Kafka 유입량·저장 성공량을 한 화면에서 비교하는 관측성 보완~~ —
   **완료(2026-09-04)**. 파이프라인 4단계 대조 대시보드(`monitoring/grafana/dashboards/
   pipeline-funnel.json`) + 알림 3종 추가. 저장 **성공**만 세는
   `telemetry.influx.points.written` 카운터를 새로 넣었다(기존 batch.size.sum은 진입
   시점 기록이라 실패해도 오른다). 12시간 soak 사고를 재현해 `TelemetryStorageStalled`가
   실제로 발동하는 것을 확인했고, 그때 `kafka_consumer_records_lag_max`는 값 자체가 없어
   기존 lag 알림으로는 **원천적으로 못 잡는** 상황이었다. Runbook은
   `docs/runbook/pipeline-observability.md`.
   만들다 기존 버그도 하나 고쳤다 — `MqttIngestFallingBehind`가 `$SYS/broker/messages/received`
   (모든 MQTT 패킷)를 PUBLISH만 센 값과 비교해 **정상 부하에서 계속 울리고 있었다**
   (실측 2배 차이). `publish/messages/received`로 교체.
   남은 갈래: 단계 1은 브로커 전체 수치라 다른 클라이언트가 붙으면 함께 오른다.
   포인트 수와 메시지 수를 직접 비교하는 것은 1:1 구조에서만 성립한다
7. Flutter 앱 — **디자인/반응형 1차 완료(2026-09-05)**, 실기기 검증은 남음.
   앱 저장소(`Seongwonp/vehicle-telemetry-app`)에서 진행했다:
   - 반응형 자동 검사 도입(`test/responsive_overflow_test.dart`) — 구성요소 × 너비 4종 ×
     글자배율 3종. 돌리자마자 overflow 12건이 나왔고 **차량 카드는 320px에서 기본
     글자 크기로도 99px 잘리고 있었다**. 전부 수정
   - 디자인 토큰 도입(`lib/core/theme/design_tokens.dart`, `docs/design-system.md`).
     스케일 벗어난 값 164곳 → 14곳
   - `flutter analyze` 40건 → 0건, 테스트 37 → 181개
   **넓은 화면 가독 폭과 그라데이션 방침도 완료(2026-09-05)**:
   - 화면마다 `maxWidth`를 따로 정해 **8종**(420~1200)이 흩어져 있던 것을
     `ContentWidths` 4단(form 480 / reading 640 / feed 840 / grid 1200)으로 묶었다.
     기준은 폭이 아니라 "한 줄에 무엇이 들어가는가"다. 1280px 스냅샷을 폭 제한
     없는 것과 적용한 것 두 장으로 뽑아 비교했다
   - 폭만 제한하니 계측 카드가 비었다 — 아크 게이지가 104~144px 고정이라 카드가
     커져도 그대로였다. 상한을 220으로 올려 함께 커지게 했다
   - `primaryGradient`는 랜딩·로그인 CTA 두 곳뿐이라 **유지**(진입 화면은 계측
     도구가 아니다). `accentGlow`는 **아무 데서도 안 쓰여서 삭제** — 지우니
     `AppSemanticColors` 전체가 `const`가 됐다
   - 반응형 자동 검사에 데스크톱 폭(1024/1280) 추가. 테스트 181 → 253개, analyze 0
   남은 것: **실기기 검증**(전부 위젯 테스트 기준이다), 스케일 밖 값 14곳 개별 검토,
   대시보드 그리드 열 수(데스크톱 4열)가 새 내용 폭에서 적정한지 미검토

Flutter 앱 고도화는 폐기한 작업이 아니라 후순위 트랙이다. 앱 저장소의 `AGENTS.md`에 적힌
데스크톱 검증 절차를 먼저 수행한 뒤 WebSocket 재연결, 오래된 데이터 표시, 작은 화면
overflow, 접근성, 오류·빈 상태를 우선 개선한다. 새 화면이나 보여주기용 기능 추가는 그 뒤다.

---

## 차량 데이터 스펙

```json
{
  "vehicle_id": "KR-GA-1234",
  "timestamp": "2026-05-09T10:00:00Z",
  "speed": 87.3,
  "rpm": 2400,
  "engine_temp": 92.1,
  "throttle_position": 34.5,
  "fuel_level": 67.0,
  "battery_voltage": 13.8,
  "gps": {
    "lat": 37.123456,
    "lng": 127.654321
  },
  "dtc_codes": []
}
```

---

## 이상 감지 룰 (Phase 3 기준)

| 항목 | 이상 조건 |
|------|----------|
| 엔진 온도 | 105°C 초과 |
| RPM | 6000 초과 |
| 배터리 전압 | 11.5V 미만 또는 15V 초과 |
| 속도 | 200km/h 초과 |
| DTC 코드 | 배열이 비어있지 않을 때 |

---

## AI에게 요청할 때 참고사항

- 언어 기본값: **Java (Spring Boot)**, **Python**, **C** (시뮬레이터)
- 커밋과 푸시는 기본적으로 명령어만 안내하고, 사용자가 명시적으로 요청한 경우에만 직접 실행할 것
- 코드 생성 시 보안 취약점 (SQL Injection, IDOR, 인증 누락) 반드시 검토
- 새 기능 추가 전 현재 Phase가 완료됐는지 확인
- 테스트 코드는 JUnit 5 (Java), pytest (Python) 사용
- 환경변수는 `.env` 파일로 분리, 하드코딩 금지
- Docker Compose로 전체 스택 실행 가능하게 유지

---

## 구현 단계 (Phase)

### Phase 1 — 데이터 수집 파이프라인
- 차량 시뮬레이터 (MQTT Publisher)
- Mosquitto 브로커 구축 (TLS 적용)
- Kafka 클러스터 구성
- Spring Boot Kafka Consumer → InfluxDB 저장

### Phase 2 — REST API 서버
- 차량 등록/관리 API
- 실시간 데이터 조회 API
- JWT 인증/인가
- Rate Limiting

### Phase 3 — 이상 감지
- 룰 기반 이상 감지 + Kafka 이벤트 발행
- 알림 (Webhook 또는 이메일)
- (선택) Isolation Forest 머신러닝 감지

### Phase 4 — 보안 강화
- MQTT X.509 인증서 인증
- API 요청 로깅 + 이상 접근 감지
- 보안 취약점 자체 점검 및 보고서

### Phase 5 — 모니터링 & 배포
- Grafana 대시보드
- Prometheus + Spring Actuator
- Docker Compose 전체 컨테이너화
- AWS EC2 배포 (선택)
