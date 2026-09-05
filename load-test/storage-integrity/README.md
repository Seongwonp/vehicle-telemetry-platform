# 저장 경로 정합성 측정 — 강제 종료·재전달 시 중복과 덮어쓰기

CLAUDE.md 우선순위 1번("consumer 강제 종료와 재전달 시 InfluxDB 중복·덮어쓰기 수량 측정").

## 무엇을 재는가

Kafka는 at-least-once다. consumer가 offset을 커밋하기 전에 죽으면 같은 메시지가 다시 온다.
그때 InfluxDB에 무슨 일이 생기는지는 **포인트 identity**로 결정된다 —
`TelemetryRepository.toPoint()` 기준으로 `vehicle_telemetry` measurement +
`vehicle_id` 태그 + **밀리초** 타임스탬프다.

| 상황 | identity | 결과 |
| --- | --- | --- |
| 같은 메시지가 재전달 | 동일, 필드 값도 동일 | 덮어쓰기 → **멱등**, 행이 안 는다 |
| 다른 메시지가 같은 `(vehicle_id, ms)` | 동일, 필드 값은 다름 | 뒤엣것이 앞엣것을 덮음 → **조용한 유실** |

두 번째가 실제로 터진 적이 있다 — `WritePrecision.S`(초)였을 때 차량당 초당 2건이면
50%가 사라졌고 Kafka lag은 0이라 정상처럼 보였다(ADR-014).
정밀도를 ms로 올려 고쳤지만, **"고쳤다"를 숫자로 확인한 적은 없다.** 이 측정이 그것이다.

## 산식

```
재전달 건수    = saveAll에 전달된 포인트 수 − 토픽 메시지 수
덮어쓰기 유실  = 토픽의 고유 (vehicle_id, ms) 수 − InfluxDB 행 수
```

`saveAll에 전달된 포인트 수`는 Micrometer DistributionSummary
`telemetry_influx_write_batch_size_sum`이다 — 재전달분이 그대로 포함된다.
`/actuator/prometheus`는 인증 없이 열려 있다(Prometheus 스크레이핑용).

## 절차

```bash
# 1. 스택 기동 (anomaly-detector는 이 측정과 무관하므로 띄우지 않는다)
docker compose -f docker-compose.yml -f docker-compose.dev.yml down -v
docker compose -f docker-compose.yml -f docker-compose.dev.yml \
  up -d mosquitto zookeeper kafka influxdb postgres redis backend

# 2. 부하 (고유 키 충돌을 피하려면 차량 수 대비 발행 간격을 너무 좁히지 말 것)
docker compose -f docker-compose.yml -f docker-compose.dev.yml \
  run -d --rm --name telemetry-sim-0 \
  -e VEHICLE_COUNT=200 -e PUBLISH_INTERVAL=0.2 simulator

# 3. 처리 중에 강제 종료 — SIGKILL이라 정상 종료 훅이 안 돌고 in-flight offset이 미커밋된다
docker kill telemetry-backend

# 4. 재기동 후 lag 0까지 드레인
docker compose -f docker-compose.yml -f docker-compose.dev.yml up -d backend

# 5. 부하 정지 후 드레인 완료를 기다렸다가 측정
docker rm -f telemetry-sim-0
python measure_integrity.py --bootstrap kafka:29092 \
  --influx-url http://influxdb:8086 --influx-token "$INFLUXDB_TOKEN" \
  --influx-org "$INFLUXDB_ORG" --influx-bucket "$INFLUXDB_BUCKET" \
  --metrics-url http://backend:8080/actuator/prometheus
```

## 반드시 지킬 것

- **`docker kill`을 쓴다** (`stop`이 아니라). `stop`은 SIGTERM이라 Spring이 정상 종료하며
  in-flight를 커밋해버려서, 재전달 자체가 일어나지 않는다 — 재려는 현상이 안 생긴다.
- **메트릭은 백엔드 재시작으로 리셋된다.** `telemetry_influx_write_batch_size_sum`은
  프로세스 생명주기 카운터라, 강제 종료 이후 값은 **재시작 후 처리분만** 센다.
  따라서 재전달 건수를 온전히 내려면 종료 직전 값을 따로 받아두고 합산해야 한다
  (스크립트가 음수를 감지하면 경고한다).
- **lag 0까지 드레인한 뒤 잰다.** 처리 중에 재면 저장 시도 < 토픽 수라 음수가 나온다.
- **측정 전 InfluxDB를 비운다**(`down -v`). 이전 실행의 행이 남아 있으면 행 수 비교가 깨진다.
- 이 스크립트는 consumer group을 만들지 않는다(`group_id=None`) — 여러 번 돌려도
  실제 서비스의 offset에 영향이 없다.

---

## 같은 밀리초 키 충돌 — `ms_collision.py`

위 재전달 실험에서 충돌 0건이 나왔지만, 그건 "그 부하에서 안 났다"는 뜻이지
"구조적으로 안 난다"는 뜻이 아니다. 따로 재현하고 임계값을 쟀다.

```bash
# 재현 — 같은 타임스탬프로 N건
python ms_collision.py --mode collide --count 500 --vehicle MSCOLLIDE-01

# 임계값 — 차량당 발행 속도를 고정해 충돌률 측정
python ms_collision.py --mode natural --seconds 10 --rate 2000 --vehicle MSRATE-2000
```

결과는 `RESULT_20260905_ms_collision.md`. 요약:

- **재현됨**: 500건 발행 → **1행**. 499건이 에러도 로그도 없이 사라진다.
  MQTT PUBACK 500, Kafka 500, InfluxDB 쓰기 성공 — 행 수를 직접 세야 보인다.
- **임계값은 차량당 1,000 msg/s**(밀리초당 1건). 500→0.02%, 1,000→0.64%, 2,000→50.20%.
- **판단: 고치지 않는다.** 차량당 5 msg/s라 200배 여유고, 총량은 차량 수로 늘리므로
  (`vehicle_id`가 태그) 총 처리량과 무관하다. 시퀀스 태그는 카디널리티를 무너뜨리고,
  마이크로초 정밀도는 타임스탬프 계약을 바꿔야 한다.

`--rate`를 주지 않으면 최대 속도로 발행하는데, 초당 10만 건이 나가면서 paho 큐가
넘쳐 측정이 오염된다. **임계값을 잴 때는 반드시 속도를 고정하라.**
