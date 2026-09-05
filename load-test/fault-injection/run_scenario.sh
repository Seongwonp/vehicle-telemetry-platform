#!/bin/bash
# 의존성 장애 주입 → 복구 → 정합성 대조.
#
# 무엇이 "정답"인지가 시나리오마다 다르다는 게 핵심이다.
#   influxdb  : 메시지는 Kafka까지 정상 도착하므로 **Kafka 토픽**이 정답.
#               검증 대상은 "재시도 → DLQ → 재처리"로 전부 복구되는가.
#   kafka     : 토픽 자체가 못 받으므로 **backend의 MQTT 수신 카운터**가 정답.
#               검증 대상은 로컬 spool이 유실을 막는가.
#   mosquitto : 브로커가 죽으면 그 아래 단계가 전부 비어 있어 기준이 될 수 없다.
#               **시뮬레이터가 PUBACK을 받은 건수(`confirmed`)**가 정답이다.
#               publish()의 반환값은 기준이 아니다 — 클라이언트 큐에 넣은 것까지만
#               보장하고, 브로커가 죽은 뒤에도 한동안 성공을 반환한다.
#               검증 대상은 "브로커가 받았다고 확인해준 것"이 backend까지 오는가.
#
# 사용법: bash run_scenario.sh <influxdb|kafka|mosquitto> [장애 지속 초]
set -euo pipefail
cd "$(dirname "$0")/../.."

SCENARIO="${1:?시나리오를 지정해라: influxdb | kafka | mosquitto}"
OUTAGE_SEC="${2:-90}"
# 기본은 평문(dev) 프로파일이다. `PROFILE=tls`를 주면 운영 프로파일(8883/mTLS)로 돌린다 —
# 브로커 장애 시나리오는 TLS 핸드셰이크가 재연결 시간에 얹히므로 결과가 달라질 수 있어
# 따로 재봐야 한다. 미리 `broker/certs/generate-certs.sh`로 차량 인증서를 만들어 둬야 하고,
# 차량 수는 인증서를 만들어 둔 개수(MQTT_VEHICLE_IDS)를 넘을 수 없다.
PROFILE="${PROFILE:-dev}"
if [ "$PROFILE" = "tls" ]; then
  COMPOSE="docker compose -f docker-compose.yml"
  VEHICLES="${VEHICLES:-100}"
else
  COMPOSE="docker compose -f docker-compose.yml -f docker-compose.dev.yml"
  VEHICLES="${VEHICLES:-200}"
fi
# `set -e` 아래에서는 `$(test && echo ...)` 형태를 쓰면 안 된다 — test가 거짓일 때
# 명령 치환의 종료 코드가 1이라 **대입문 자체가 실패해 스크립트가 조용히 끝난다**
# (실제로 그렇게 만들어 놓고 "출력 없이 exit 0"으로 한 번 헤맸다).
OUT_SUFFIX=""
if [ "$PROFILE" = "tls" ]; then
  OUT_SUFFIX="_tls"
fi
OUT="load-test/fault-injection/_result_${SCENARIO}${OUT_SUFFIX}.txt"

TOK=$(grep '^INFLUXDB_TOKEN=' .env | cut -d= -f2-)
ORG=$(grep '^INFLUXDB_ORG=' .env | cut -d= -f2-)
BKT=$(grep '^INFLUXDB_BUCKET=' .env | cut -d= -f2-)

log() { echo "[$(date +%H:%M:%S)] $*" | tee -a "$OUT"; }
wait_sec() { local s=$1 t0; t0=$(date +%s); until [ $(( $(date +%s) - t0 )) -ge "$s" ]; do sleep 5; done; }

influx_rows() {
  docker exec telemetry-influxdb influx query \
    "from(bucket: \"$BKT\") |> range(start: -60m) |> filter(fn: (r) => r._measurement == \"vehicle_telemetry\" and r._field == \"speed\") |> group() |> count()" \
    --org "$ORG" --token "$TOK" --raw 2>/dev/null \
    | tr -d '\r' | awk -F, '$0 ~ /^,/ && $NF ~ /^[0-9]+$/ {v=$NF} END{print v+0}'
}
metric() {  # $1 = prometheus 메트릭 이름
  curl -s http://localhost:8080/actuator/prometheus 2>/dev/null \
    | awk -v n="$1" '$1 ~ "^"n"([{]|$)" {gsub(/.* /,""); s+=$0} END{printf "%d", s+0}'
}
topic_end_offsets() {  # $1 = 토픽
  docker exec telemetry-kafka kafka-run-class kafka.tools.GetOffsetShell \
    --broker-list localhost:29092 --topic "$1" 2>/dev/null \
    | awk -F: '{s+=$3} END{print s+0}'
}
storage_lag() {
  docker exec telemetry-kafka kafka-consumer-groups --bootstrap-server localhost:29092 \
    --describe --group telemetry-storage-group 2>/dev/null | awk 'NR>1{s+=$6} END{print s+0}'
}
# 시뮬레이터가 남기는 `[STATS] attempted=… queued=… rejected=… confirmed=…`의 마지막 줄.
# 브로커 장애의 정답 기준은 여기 있다 — 아래 단계가 전부 비어 있기 때문이다.
# 컨테이너를 `--rm`으로 띄우므로 **지우기 전에** 읽어야 한다.
sim_stat() {  # $1 = attempted|queued|rejected|confirmed
  docker logs telemetry-sim-0 2>&1 | grep -o '\[STATS\].*' | tail -1 \
    | tr ' ' '\n' | awk -F= -v k="$1" '$1==k {print $2+0}' | tail -1
}

: > "$OUT"
log "=== 시나리오: $SCENARIO (장애 ${OUTAGE_SEC}초) ==="

# ── 1. 깨끗한 스택 ─────────────────────────────────────────────
$COMPOSE down -v >/dev/null 2>&1 || true
$COMPOSE up -d mosquitto zookeeper kafka influxdb postgres redis backend >/dev/null 2>&1 || true
until [ "$(docker inspect telemetry-postgres --format '{{.State.Health.Status}}' 2>/dev/null)" = "healthy" ]; do sleep 10; done
$COMPOSE up -d backend >/dev/null 2>&1
until curl -s -o /dev/null -w '%{http_code}' http://localhost:8080/actuator/health 2>/dev/null | grep -q 200; do sleep 5; done
log "스택 기동 완료"

# ── 2. 부하 ────────────────────────────────────────────────────
# --rm을 쓰지 않는다. 정답 기준을 시뮬레이터 로그에서 읽어야 하는데, --rm이면
# 정지와 동시에 컨테이너가 사라져 최종 집계를 읽을 기회가 없다. 대신 아래에서
# docker stop(SIGTERM)으로 정상 종료시켜 최종 집계를 남기고, 읽은 뒤 직접 지운다.
docker rm -f telemetry-sim-0 >/dev/null 2>&1 || true
$COMPOSE run -d --name telemetry-sim-0 \
  -e VEHICLE_COUNT=$VEHICLES -e PUBLISH_INTERVAL=0.2 simulator >/dev/null 2>&1
log "시뮬레이터 기동 (${VEHICLES}대, 프로파일 ${PROFILE}) — 60초 정상 구간"
wait_sec 60

# ── 3. 장애 주입 ───────────────────────────────────────────────
# docker stop(SIGTERM)을 쓴다 — 의존성이 "정상 종료"한 상태를 흉내내는 것이고,
# 여기서 보려는 건 우리 쪽의 복구 동작이지 의존성의 크래시 복구가 아니다.
log "--- 장애 주입: telemetry-$SCENARIO 정지 ---"
docker stop "telemetry-$SCENARIO" >/dev/null
wait_sec "$OUTAGE_SEC"
log "장애 중 상태: mqtt_received=$(metric telemetry_mqtt_messages_received_total) influx_write_failures=$(metric telemetry_influx_write_failures_total) dlq_published=$(metric telemetry_kafka_dlq_published_total)"

# ── 4. 복구 ────────────────────────────────────────────────────
log "--- 복구: telemetry-$SCENARIO 재기동 ---"
docker start "telemetry-$SCENARIO" >/dev/null
case "$SCENARIO" in
  influxdb)
    until [ "$(docker inspect telemetry-influxdb --format '{{.State.Health.Status}}' 2>/dev/null)" = "healthy" ]; do sleep 5; done ;;
  mosquitto)
    # healthcheck가 없어서 리스너 포트가 열렸는지로 판정한다.
    #
    # MQTT로 붙어보는 쪽이 더 정확하지만 두 프로파일 모두에서 안 된다 —
    # TLS 프로파일은 1883이 아예 없고, mTLS로 붙으려면 클라이언트 인증서가 필요한데
    # 키 파일이 0600이라 컨테이너 안의 mosquitto 사용자가 읽지 못한다
    # ("Problem setting TLS options: File not found"로 나온다).
    # 헬스체크 메시지를 텔레메트리 토픽에 쏘는 것도 안 된다 — backend가 그것을
    # 형식 오류로 세어 측정이 오염된다.
    #
    # 그래서 TCP 연결만 확인한다. 브로커가 리스너를 열었다는 뜻이면 충분하고,
    # 이 시나리오가 실제로 재는 것은 그 뒤 backend가 얼마나 빨리 따라붙느냐다.
    MQ_PORT=$([ "$PROFILE" = "tls" ] && echo 8883 || echo 1883)
    until docker exec telemetry-kafka bash -c \
      "timeout 3 bash -c '</dev/tcp/telemetry-mosquitto/$MQ_PORT'" >/dev/null 2>&1; do sleep 5; done ;;
  *)
    until docker exec telemetry-kafka kafka-topics --bootstrap-server localhost:29092 --list >/dev/null 2>&1; do sleep 5; done ;;
esac
log "의존성 복구 확인"
wait_sec 60

# ── 5. 부하 정지 후 드레인 ──────────────────────────────────────
# **SIGKILL(rm -f)이 아니라 SIGTERM으로 세운다.** 정답 기준을 시뮬레이터가 세는데,
# 강제 종료하면 마지막 주기 로그(최대 STATS_INTERVAL초 묵은 값)밖에 못 읽는다.
# 초당 1,000건 부하에서 5초는 5,000건이고, 실제로 그것 때문에 "정답 기준"이
# backend 수신량보다 작게 나와 기준 구실을 못 한 적이 있다.
log "부하 정지 (SIGTERM) — 최종 집계 대기"
docker stop -t 90 telemetry-sim-0 >/dev/null 2>&1 || true
# `docker stop`이 돌아와도 마지막 로그 줄이 아직 안 보일 수 있다(로그 드라이버 flush 경합).
# 그러면 최종 집계가 아니라 **최대 STATS_INTERVAL초 묵은 주기 로그**를 읽게 된다.
# 100대 부하(초당 500건)에서 2초만 묵어도 약 1,000건이 빠져, 정답 기준이 실제보다
# 작아지고 "저장이 발행보다 많다"는 불가능한 결과가 나온다 — 실제로 그렇게 나와서 찾았다.
# 종료 마커가 보일 때까지 기다린 뒤에 읽는다.
for _ in $(seq 1 30); do
  docker logs telemetry-sim-0 2>&1 | grep -q "전체 시뮬레이터 종료 완료" && break
  sleep 2
done
docker logs telemetry-sim-0 2>&1 | grep -q "전체 시뮬레이터 종료 완료" \
  || log "** 시뮬레이터 최종 집계 줄을 못 찾았다 — 아래 수치는 주기 로그라 실제보다 작다"
SIM_ATTEMPTED=$(sim_stat attempted)
SIM_QUEUED=$(sim_stat queued)
SIM_REJECTED=$(sim_stat rejected)
SIM_CONFIRMED=$(sim_stat confirmed)
log "시뮬레이터 최종: 시도 $SIM_ATTEMPTED / publish() 성공 $SIM_QUEUED / publish() 실패 $SIM_REJECTED / 브로커 확인 $SIM_CONFIRMED"
docker rm -f telemetry-sim-0 >/dev/null 2>&1 || true
log "드레인 대기"
# spool 드레인(5초 주기)과 컨슈머 드레인을 모두 기다린다.
wait_sec 30
for _ in $(seq 1 60); do
  [ "$(storage_lag)" = "0" ] && break
  sleep 10
done
log "드레인 완료 (lag=$(storage_lag))"

# ── 6. 집계 ────────────────────────────────────────────────────
MQTT_RECEIVED=$(metric telemetry_mqtt_messages_received_total)
MQTT_INVALID=$(metric telemetry_mqtt_messages_invalid_total)
TOPIC=$(topic_end_offsets vehicle-telemetry)
DLQ=$(topic_end_offsets vehicle-telemetry-dlq)
ROWS=$(influx_rows)
WRITE_FAILURES=$(metric telemetry_influx_write_failures_total)

log ""
log "=== 결과 ==="
log "시뮬레이터 publish() 시도 : $SIM_ATTEMPTED"
log "  클라이언트 큐 적재      : $SIM_QUEUED  (적재 실패 $SIM_REJECTED)"
log "  브로커 확인(PUBACK)     : $SIM_CONFIRMED"
log "MQTT 수신 (backend)      : $MQTT_RECEIVED  (형식 오류 $MQTT_INVALID)"
log "Kafka vehicle-telemetry  : $TOPIC"
log "Kafka vehicle-telemetry-dlq: $DLQ"
log "InfluxDB 행              : $ROWS"
log "InfluxDB 쓰기 실패 누적   : $WRITE_FAILURES"
log ""
if [ "$SCENARIO" = "kafka" ]; then
  log "정답 기준 = MQTT 수신($MQTT_RECEIVED). 토픽과의 차이 = $((MQTT_RECEIVED - TOPIC))"
  log "  (spool이 제대로 동작하면 0에 가까워야 한다)"
elif [ "$SCENARIO" = "mosquitto" ]; then
  log "정답 기준 = 브로커가 확인해준 건수($SIM_CONFIRMED)."
  log "  backend 수신과의 차이 = $((SIM_CONFIRMED - MQTT_RECEIVED))  ← 유실"
  log "  InfluxDB 행과의 차이  = $((SIM_CONFIRMED - ROWS))"
  log ""
  log "  시도($SIM_ATTEMPTED) - 브로커 확인($SIM_CONFIRMED) = $((SIM_ATTEMPTED - SIM_CONFIRMED))"
  log "    발행 측에서 브로커까지 못 간 건수. QoS 1은 publish()가 NO_CONN을 반환해도"
  log "    송신 큐에 남아 재연결 때 재전송되므로, publish() 실패($SIM_REJECTED)가"
  log "    곧 유실은 아니다. 종료 시 flush까지 기다린 뒤에도 안 빠진 것만 유실이다."
else
  log "정답 기준 = Kafka 토픽($TOPIC). InfluxDB와의 차이 = $((TOPIC - ROWS))"
  log "  (그 차이가 DLQ($DLQ)로 설명되면 유실이 아니라 격리다 — 재처리로 복구 가능)"
fi
log "DONE"
