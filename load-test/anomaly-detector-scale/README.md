# 이상 감지 서비스 다중화 검증 — 원시 로그

`docs/load-test-plan.md` 5절 "Track A — 이상 감지 서비스 다중화"에서 요약한 수치의 원본이다.
코덱스 리뷰(2026-08)에서 "요약 표만 있고 원본이 없어 독립 재검증이 안 된다"는 지적을 받아 추가했다.

## 파일

- `sample.sh` — Kafka consumer lag(`kafka-consumer-groups --describe`), InfluxDB 저장 건수,
  `docker stats`를 주기적으로 찍어 로그 파일에 남기는 스크립트.
  사용법: `./sample.sh <라벨> <총 관찰 시간(초)> <샘플 간격(초)>`
- `S0_isolation_single-instance-1200msgs.txt` — 0단계(원인 분리): 시뮬레이터 2프로세스
  (~1,200 msg/s), 단일 anomaly-detector 인스턴스, 12분 관찰.
- `S2_multiplex3_3instances-2300msgs.txt` — 2단계(재검증): 시뮬레이터 6프로세스(~2,300 msg/s),
  anomaly-detector 3인스턴스, 11분 관찰.

## 알려진 한계 (정직하게 밝힘)

- **1차 측정(A0~A4, 단일 인스턴스 24시간 발산 테스트)의 원시 로그는 없다.** 세션 스크래치패드에
  있었는데 세션이 끝나며 삭제됐다. `docs/load-test-plan.md`에 남긴 요약 수치(1,264 → 200만+)는
  당시 터미널 출력을 옮겨 적은 것이며, 재현하려면 이 디렉터리의 `sample.sh`로 동일한 절차를
  다시 밟아야 한다.
- **before/after가 완전히 동일한 조건이 아니다.** 단일 인스턴스 테스트는 ~2,500-2,700 msg/s로
  24시간, 3인스턴스 재검증은 ~2,300 msg/s로 11분이었다 — 부하 수준과 관찰 시간이 다르고,
  재빌드 과정에서 `kafka-python`도 3.0.7→3.0.9로 바뀌었다. 이 차이만으로도 결과가 달라질
  가능성을 배제할 수 없다는 지적을 받았고, 타당하다고 판단한다.
- **11분 관찰로 "발산 없음"을 확정할 수는 없다.** 마지막 두 샘플(2,879 → 7,368)은 오히려
  상승했다 — 노이즈 폭이 커서 7개 샘플만으로 완만한 우상향과 구분하기 어렵다.
- **원인 분리 테스트(0단계)도 두 변수(부하량, 호스트 경합)를 동시에 바꿨다** — "순수 알고리즘
  한계"와 "호스트 경합"을 깔끔하게 분리하지 못했다.

## 제대로 검증하려면 (향후 과제 — 4차 측정으로 절반 진행됨, 아래 참고)

- 동일 이미지·동일 부하(예: 정확히 2,500 msg/s)·동일 시작 lag으로 1인스턴스 vs 3인스턴스를
  각각 최소 1시간 나란히 비교
- 3인스턴스 구성으로 12~24시간 soak test (진짜 장기 안정성 확인)
- 호스트 경합만 분리하려면 부하 생성기를 다른 호스트로 옮기거나 컨테이너 CPU quota를 고정하고
  동일 부하량으로 프로세스 수만 바꿔가며 비교
- `docs/load-test-plan.md` 5절, ADR-016에 "정직한 재작성"으로 반영함 — 과장된 표현
  ("병목 해소 완료", "안정 — 발산 없음", "원인 분리 완료")을 실측이 뒷받침하는 수준으로 낮췄다.

## 4차 측정 (2026-08-03, 동일 조건 A/B) — 1시간 비교는 완료, soak은 미완

위 "제대로 검증하려면"의 첫 항목(동일 이미지·동일 부하·동일 시작 lag 1시간 비교)을 실행했다.

- **`AB1_1instance_1h_samehost.txt`** — 1인스턴스, 시뮬레이터 6프로세스(200대/0.05초 ×6,
  kafka-python==3.0.9로 고정한 동일 이미지), 시작 lag 0에서 1시간(3,600초) 관찰, 60초 간격 샘플.
- **`AB2_3instances_1h_samehost.txt`** — 위와 정확히 같은 조건(같은 이미지, 같은 부하)에서
  인스턴스만 3개로 바꾸고, 1인스턴스 구간에서 쌓인 백로그를 lag<500까지 드레인한 뒤 시작 lag
  0에서 1시간 관찰.
- **결과 요약**: 1인스턴스는 시작 4,119 → 약 30분 만에 140만대까지 치솟은 뒤 후반 30분은
  144만~167만 사이에서 오르내리며 정체(계속 무한정 발산하지는 않고, 처리량과 유입량이
  비슷한 수준에서 진동하는 것으로 보임 — 이전 24시간 테스트의 "200만+ 계속 증가"와는
  다른 양상인데, 그사이 코드가 바뀌어서(ADR-017 수동 커밋+DLQ, ADR-018 파티션별 ML 관리 등)
  완전히 같은 비교는 아니라는 점은 감안해야 한다). 3인스턴스는 같은 부하를 완전히 드레인한
  뒤 1시간 내내 lag이 낮은 수준을 유지했다 — **이 두 로그가 이번 세션에서 가장 신뢰도 높은
  "동일 조건" 비교 데이터다.**
- **soak test(3인스턴스, 12시간)는 이번 세션에서 두 번 시도했고 둘 다 중간에 끊겼다** —
  `AB3_soak_v1_interrupted_1h46m.txt`(1시간 46분 만에 중단, 원인: 호스트가 Idle Sleep +
  "Dark Wake Thermal Emergency"로 두 차례 잠들었고, 그 여파로 Docker exec 하나가 응답 없이
  멈췄는데 당시 `sample.sh`에 타임아웃이 없어 루프 전체가 6시간 넘게 조용히 정지),
  `AB3_soak_v2_interrupted_2h.txt`(sample.sh에 타임아웃을 추가하고 `caffeinate`로 절전까지
  막은 뒤 재시도했지만, 이번엔 절전 없이도 약 2시간 만에 Docker Desktop 자체가 다시 응답
  불능 상태가 됐다 — 6개 시뮬레이터+3개 이상감지 인스턴스+Kafka+백엔드로 호스트 CPU를
  600~700%대까지 장시간 유지한 게 원인으로 추정. 타임아웃 덕분에 루프 자체는 안 멈추고
  `[TIMEOUT]` 표시를 남기며 계속 돌았다). 진행 상황과 타임라인은 `AB_timeline_v1.txt`,
  `AB_timeline_v2.txt` 참고.
- **결론**: 이 호스트(8코어)로는 6프로세스 부하 + 3인스턴스 + 나머지 전체 스택을 12시간
  동안 안정적으로 유지하기 어렵다는 것 자체가 하나의 관찰 결과다.

## 5차 측정 (데스크탑 재시도, 2026-08-06~07) — 12시간 soak 완주

더 여유 있는 호스트(10코어/16스레드)에서 SOAK_HANDOFF.md 절차 그대로 재시도했다.

- **`AB4_soak_desktop_12h.txt`** — 3인스턴스, 4차와 동일한 부하(시뮬레이터 6프로세스,
  200대/0.05초 ×6, ~2,300-2,700 msg/s), 시작 lag 0(드레인 확인 후 시작), 12시간(43,200초)
  관찰, 60초 간격 샘플 469개. **중단 없이 완주**했다.
- **결과**: `anomaly-detector-group` lag 최대 100, 평균 27.4, 500 초과 샘플 0건. `docker exec`
  15초 타임아웃은 469개 샘플 중 6건 발생했지만 산발적이고 연속 실패는 없었다(`sample.sh`의
  watchdog kill 덕분에 루프 자체는 멈추지 않음 — v1의 실패 원인이었던 문제가 실제로 해결돼
  있음을 확인).
- **결론**: 4차의 1시간 A/B 우위가 12시간 장시간 관찰에서도 유지된다. 3인스턴스 다중화는
  이 부하 수준에서 안정적 — "완전 해소는 미확정" 상태를 해소로 정정.
- **별개로 발견한 버그(다중화 검증과 무관)**: soak 도중 InfluxDB 원시 텔레메트리(`speed` 등)
  저장이 시작 26초 만에(06:45:26Z) 멈춰 이후 12시간 동안 0건 기록됐다. 같은 구간
  `telemetry-storage-group`의 Kafka lag은 낮게 유지돼 정상처럼 보였고 backend 로그에도 에러가
  없었다(무음 실패) — lag 기반 모니터링으로는 못 잡는 유형. ADR-014의 `WritePrecision.S` 버그
  재발이 아님(현재 `WritePrecision.MS`)을 코드로 확인했으나 근본 원인은 미해결로 남겨둔다.
  별도로 08:25:39부터 종료까지 MQTT `MqttPahoMessageDrivenChannelAdapter`가 시간당 155~160회,
  총 1,677회 "Lost connection: MqttException"을 기록했다 — InfluxDB 저장 중단(06:45)보다
  1h40m 늦게 시작했으므로 같은 원인은 아니고, 장시간 부하에서의 MQTT 연결 안정성 문제로
  별도 이슈다.

## 부하 테스트 전 필수 확인 — 부하가 실제로 도달했는가

2026-08-31에 확인된 사실: **여러 차례의 "2,400 msg/s 부하 테스트"가 실제로는 초당
15-20건만 파이프라인에 도달한 상태에서 측정됐다.** 시뮬레이터는 정상 발행하고
브로커도 정상 수신했지만, 백엔드가 못 받아가 브로커가 나머지를 조용히 버리고 있었다.
Kafka lag은 들어온 게 없으니 정상으로 보였다.

그래서 **부하를 걸었으면 반드시 입력단에서 도달량을 확인한다.** 소요 1분.

```bash
# 1) 시뮬레이터가 실제로 몇 건 발행하는지 (프로세스 1개 기준, ×프로세스 수)
A=$(docker logs telemetry-sim-0 2>&1 | wc -l); sleep 15
B=$(docker logs telemetry-sim-0 2>&1 | wc -l); echo "발행: $(( (B-A)/15 )) msg/s"

# 2) Kafka에 실제로 도착한 양
A=$(docker exec telemetry-kafka kafka-run-class kafka.tools.GetOffsetShell \
      --broker-list localhost:9092 --topic vehicle-telemetry | awk -F: '{s+=$3} END{print s}')
sleep 20
B=$(docker exec telemetry-kafka kafka-run-class kafka.tools.GetOffsetShell \
      --broker-list localhost:9092 --topic vehicle-telemetry | awk -F: '{s+=$3} END{print s}')
echo "수집: $(( (B-A)/20 )) msg/s"

# 3) 브로커가 버린 게 있는지 (0이 아니면 유실 중)
curl -s localhost:8080/actuator/prometheus | grep -E "^telemetry_mqtt_broker_messages_dropped"
```

`발행 ≈ 수집`이고 `dropped`가 안 늘어야 그 측정을 신뢰할 수 있다. 크게 벌어지면
그 회차 수치는 **의도한 부하의 결과가 아니다** — 원인을 먼저 잡고 다시 측정한다.

상시 감시는 `monitoring/prometheus/alerts.yml`의 `MqttBrokerDroppingMessages`(브로커가
버린 메시지 발생)와 `MqttIngestFallingBehind`(브로커 수신 대비 백엔드 처리 부족)가 맡는다.
두 알림은 브로커 큐를 10으로 줄여 유실을 인위적으로 만든 뒤 실제로 감지되는 것까지
확인했다(브로커 수신 474,017건 / 버림 429건 / 백엔드 수신 261,423건).

## ML 평가용 시뮬레이터 시나리오 (2026-09-03 추가)

12차 측정에서 **이 시뮬레이터로는 ML 탐지 품질을 잴 수 없다**는 걸 확인했다 —
`inject_anomaly()`가 주입하는 이상값이 전부 룰 임계값과 대응해서, 정답 데이터가 곧
룰이 잡는 것이고 "룰이 못 잡는 복합 패턴"의 정답이 없었다. 그 공백을 메우는 두 가지를
시뮬레이터에 추가했다. **둘 다 기본값 0이라 켜지 않으면 기존 측정과 동일하게 동작한다.**

### 1. 복합 이상 — `COMPOSITE_ANOMALY_RATE`

개별 필드는 전부 룰 임계값 안이지만 조합이 비정상인 패턴. 룰은 원칙적으로 못 잡고
다변량 이상치를 보는 ML만 잡을 수 있어야 한다.

| 라벨 | 패턴 | 왜 룰에 안 걸리나 |
| --- | --- | --- |
| `clutch_slip` | 속도 5-15km/h인데 RPM 4,000-4,500 | RPM < 6,000 |
| `alternator_degrading` | 전압 11.6-12.2V + RPM 2,500-4,000 | 전압 > 11.5 |
| `overheat_at_idle` | 공회전(RPM 800-1,000)인데 온도 100-103°C | 온도 < 105 |
| `throttle_no_response` | 스로틀 80-100%인데 속도·RPM 정지 수준 | 단일 필드 임계값 없음 |

"개별 필드가 전부 임계값 안"이라는 전제는 단위 테스트로 강제한다
(`TestCompositeAnomaly::test_모든_복합_이상이_룰_임계값을_넘지_않는다`, 300회 반복).
이 전제가 깨지면 룰이 잡아버려 ML 평가가 무의미해지므로 반드시 유지돼야 한다.

### 2. 분포 이동 — `DRIFT_TEMP_DELTA` / `DRIFT_START_SECONDS` / `DRIFT_RAMP_SECONDS`

엔진 온도 기준선을 `DRIFT_START_SECONDS` 이후부터 `DRIFT_RAMP_SECONDS`에 걸쳐
`DRIFT_TEMP_DELTA`만큼 선형으로 올린다. 이건 이상이 아니라 **새로운 정상**이므로,
잘 동작하는 감지기라면 재학습 후 알림률이 원래 수준으로 돌아와야 한다 —
`retrain_min_seconds`가 적절한지 재는 기준이 된다.

### 정답 로그 형식

페이로드에는 라벨을 넣지 않는다(감지기가 정답을 볼 수 있게 되고 운영 스키마도 오염된다).
대신 시뮬레이터 로그에 한 줄씩 남기고, `(vehicle_id, timestamp)`로 알림과 조인한다.

```
[GT] vehicle=SIM-060 ts=2026-09-03T10:11:12.345Z label=clutch_slip kind=composite
```

`kind`는 `rule`(룰이 잡아야 함) / `composite`(ML만 잡을 수 있어야 함)로 나뉜다.

### 실행 예

```bash
# 복합 이상 2% + 5분 뒤부터 5분에 걸쳐 온도 기준선 +8°C 이동
COMPOSITE_ANOMALY_RATE=0.02 DRIFT_TEMP_DELTA=8 \
  docker compose -f docker-compose.yml -f docker-compose.dev.yml \
  run -d --rm --name telemetry-sim-0 \
  -e VEHICLE_COUNT=200 -e PUBLISH_INTERVAL=0.05 simulator

# 정답만 뽑기
docker logs telemetry-sim-0 2>&1 | grep '^\[GT\]'
```

**아직 측정하지 않았다** — 이 시나리오로 ML precision/recall과 재학습 주기 적정성을
재는 것은 다음 작업이다.

---

## ML 탐지 품질 측정 절차 (15~17차에서 정립)

### 1. 오탐만 따로 재려면 — 이상을 하나도 넣지 않는다

`ANOMALY_RATE=0 COMPOSITE_ANOMALY_RATE=0`으로 부하를 걸면 **ML 알림은 전부 오탐**이라
정답 로그와 조인할 필요조차 없다. 15차에서 "정상만 있는데 판정률 24.4%"를 이렇게 잡았다.

### 2. 세대별로 갈라 본다

`ML_SCORE_DUMP=true`로 띄우면 메시지마다 `[SCORE] vehicle=... ts=... score=... flag=...`가
남는다. 재학습 로그(`Isolation Forest (재)학습 완료 (윈도우 샘플: N개)`)를 경계로
**모델 세대별 판정률**을 내면 문제 구간이 바로 보인다 — 전체 평균만 보면
"최초 모델이 91%를 찍고 나머지는 7%"인 상황이 24%로 뭉개진다.

```bash
docker logs <detector> | python - <<'PY'
import sys, re
gen=-1; tot={}; fl={}; bs={}
for line in sys.stdin:
    if 'Isolation Forest' in line:
        gen+=1; m=re.search(r'(\d+)개', line)
        if m: bs[gen]=int(m.group(1))
        continue
    if not line.startswith('[SCORE]') or 'score=nan' in line or gen<0: continue
    tot[gen]=tot.get(gen,0)+1
    if line.rstrip().endswith('flag=1'): fl[gen]=fl.get(gen,0)+1
for g in sorted(tot):
    print(g, bs.get(g), tot[g], f"{fl.get(g,0)/tot[g]*100:.2f}%")
PY
```

### 3. 임계값은 스윕으로 정한다 — 부하를 반복하지 않는다

임계값 후보마다 부하를 돌리면 측정 한 번에 점 하나뿐이다. 점수를 전부 덤프해두면
오프라인으로 곡선 전체를 얻는다. 17차에서 예측(87.3%) 대비 실측(89.1%)으로 검증됐다.

```bash
python score_ml.py \
  --gt-files gt-0.txt,gt-1.txt \
  --score-files det1.txt,det2.txt,det3.txt \
  --bootstrap kafka:29092 --sweep --sweep-steps 24
```

### 반드시 지킬 것

- **측정 전에 알림 토픽을 비운다.** 안 그러면 앞선 실행의 알림까지 채점돼 오탐률이
  엉뚱하게 나온다(16차에서 당했다 — 탐지기 자체 판정률과 20배 어긋나 발견). 단,
  지우고 **곧바로 다시 만든 뒤** 탐지기를 재시작할 것 — 돌아가는 중에 지우면 프로듀서가
  계속 실패하며 컨슈머가 멈춘다.
- **Redis의 ML 학습 상태를 지우고 시작한다.** 안 그러면 직전 실험의 모델을 이어받아
  워밍업 구간이 관찰되지 않는다.
- **컨테이너를 `--force-recreate`로 새로 띄운다.** 컨테이너 로그가 곧 측정 데이터라,
  이전 실행분이 섞이면 세대 구분이 깨진다.
- **시뮬레이터 로그를 내리기 전에 파일로 받아둔다** (`--rm`이면 컨테이너와 함께 사라진다).
- `ML_SCORE_DUMP`는 **측정 전용**이다 — 메시지마다 한 줄이 나가 처리량이 떨어지므로
  처리량을 재는 측정에는 끄고 돌려야 한다.
