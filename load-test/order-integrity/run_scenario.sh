#!/bin/bash
# 차량별 순서 보장 범위 측정 (docs/roadmap.md P1-1).
#
# 두 경로를 나눠 본다.
#   normal : 장애 없이 정상 Kafka 경로만. 여기서 역전이 나오면 프로듀서 설정 문제다.
#   kafka  : Kafka를 정지시켜 spool에 쌓았다가 드레인. `TelemetryProducer.retryPending()`
#            주석이 "복구 직후 짧은 구간에서 순서가 뒤집힐 수 있다"고 적어둔 경로다.
#
# **성공 기준을 "역전 0"으로 미리 정하지 않는다.** 역전이 나오면 얼마나, 어디서,
# 얼마나 과거로 되돌아가는지를 재고 그 다음에 허용할지 보정할지 정한다.
#
# 사용법: bash run_scenario.sh <normal|kafka> [장애 지속 초]
set -euo pipefail
cd "$(dirname "$0")/../.."

MODE="${1:?모드를 지정해라: normal | kafka}"
OUTAGE_SEC="${2:-90}"
VEHICLES="${VEHICLES:-200}"
LOAD_SEC="${LOAD_SEC:-90}"
COMPOSE="docker compose -f docker-compose.yml -f docker-compose.dev.yml"
OUT="load-test/order-integrity/_result_${MODE}.txt"
NET="vehicle-telemetry-platform_telemetry-net"
IMG="vehicle-telemetry-platform-anomaly-detector"

# shellcheck source=../lib/evidence.sh
. load-test/lib/evidence.sh
evidence_init "order-integrity" "bash load-test/order-integrity/run_scenario.sh $MODE $OUTAGE_SEC"
evidence_input mode "$MODE"
evidence_input vehicles "$VEHICLES"
evidence_input publish_interval_sec 0.2
evidence_input load_sec "$LOAD_SEC"
evidence_input kafka_outage_sec "$([ "$MODE" = "kafka" ] && echo "$OUTAGE_SEC" || echo 0)"

log() { echo "[$(date +%H:%M:%S)] $*" | tee -a "$OUT"; }
wait_sec() { local s=$1 t0; t0=$(date +%s); until [ $(( $(date +%s) - t0 )) -ge "$s" ]; do sleep 5; done; }
storage_lag() {
  docker exec telemetry-kafka kafka-consumer-groups --bootstrap-server localhost:29092 \
    --describe --group telemetry-storage-group 2>/dev/null | awk 'NR>1{s+=$6} END{print s+0}'
}
topic_end_offsets() {
  docker exec telemetry-kafka kafka-run-class kafka.tools.GetOffsetShell \
    --broker-list localhost:29092 --topic vehicle-telemetry 2>/dev/null \
    | awk -F: '{s+=$3} END{print s+0}'
}
metric() {
  curl -s http://localhost:8080/actuator/prometheus 2>/dev/null \
    | awk -v n="$1" '$1 ~ "^"n"([{]|$)" {gsub(/.* /,""); s+=$0} END{printf "%d", s+0}'
}
WINPWD=$(pwd -W 2>/dev/null || pwd)
check_order() {
  MSYS_NO_PATHCONV=1 docker run --rm --network "$NET" \
    -v "$WINPWD/load-test/order-integrity:/w" -w /w "$IMG" \
    python check_order.py "$@"
}

: > "$OUT"
log "=== 순서 보장 측정: $MODE (차량 ${VEHICLES}대) ==="

$COMPOSE down -v >/dev/null 2>&1 || true
$COMPOSE up -d mosquitto zookeeper kafka influxdb postgres redis backend >/dev/null 2>&1 || true
wait_until 300 "PostgreSQL healthy" bash -c '[ "$(docker inspect telemetry-postgres --format "{{.State.Health.Status}}" 2>/dev/null)" = healthy ]'
$COMPOSE up -d backend >/dev/null 2>&1
wait_until 300 "backend actuator 200" bash -c 'curl -s -o /dev/null -w "%{http_code}" http://localhost:8080/actuator/health 2>/dev/null | grep -q 200'
log "스택 기동 완료"

# 프로듀서 설정을 증거로 남긴다. 순서 보장은 acks/retries/idempotence/in-flight의
# 조합에서 나오므로, 실제로 적용된 값을 봐야 결과를 해석할 수 있다.
#
# **`ProducerConfig values` 블록 안에서만** 찾는다. 전체 로그에서 `retries =`를 grep하면
# AdminClientConfig의 기본값(2147483647)이 같이 잡혀 "yml의 retries: 3이 안 먹었다"는
# 오독을 부른다(실제로 한 번 그렇게 읽었다).
docker logs telemetry-backend 2>&1 | awk '/ProducerConfig values/,/^$/' \
  | grep -E "acks =|retries =|enable\.idempotence =|max\.in\.flight|partitioner\.class" \
  > "$EVIDENCE_DIR/producer-config.txt" 2>/dev/null || true
log "프로듀서 설정(ProducerConfig 블록):"
sed 's/^\s*/  /' "$EVIDENCE_DIR/producer-config.txt" 2>/dev/null | tee -a "$OUT" || true

docker rm -f telemetry-sim-0 >/dev/null 2>&1 || true
$COMPOSE run -d --name telemetry-sim-0 \
  -e VEHICLE_COUNT=$VEHICLES -e PUBLISH_INTERVAL=0.2 simulator >/dev/null 2>&1
log "부하 기동 — ${LOAD_SEC}초"
wait_sec "$LOAD_SEC"

if [ "$MODE" = "kafka" ]; then
  log "--- Kafka 정지 ${OUTAGE_SEC}초 (spool에 쌓인다) ---"
  docker stop telemetry-kafka >/dev/null
  wait_sec "$OUTAGE_SEC"
  log "spool pending(최소) = $(metric telemetry_spool_pending)"
  log "--- Kafka 재기동 ---"
  docker start telemetry-kafka >/dev/null
  wait_until 300 "kafka 응답" bash -c 'docker exec telemetry-kafka kafka-topics --bootstrap-server localhost:29092 --list'
  # 드레인 구간이 이 측정의 핵심이다. 새 메시지와 spool 드레인이 겹치는 시간을 준다.
  log "드레인 구간 관찰 — 120초"
  wait_sec 120
  evidence_count spool_drained_total "$(metric telemetry_spool_drained)"
fi

log "부하 정지"
docker stop -t 90 telemetry-sim-0 >/dev/null 2>&1 || true
docker rm -f telemetry-sim-0 >/dev/null 2>&1 || true
wait_sec 30
for _ in $(seq 1 60); do [ "$(storage_lag)" = "0" ] && break; sleep 10; done
log "드레인 완료 (lag=$(storage_lag))"

log ""
log "=== 순서 검사 ==="
check_order --bootstrap kafka:29092 2>&1 | tee -a "$OUT" | tee "$EVIDENCE_DIR/order-report.txt"
SUMMARY=$(check_order --bootstrap kafka:29092 --json 2>/dev/null | tail -1)
echo "$SUMMARY" > "$EVIDENCE_DIR/order-summary.json"

get() { echo "$SUMMARY" | python -c "import sys,json;print(json.load(sys.stdin).get('$1',0))" 2>/dev/null || echo 0; }
TOPIC=$(topic_end_offsets)
evidence_capture_prometheus final
evidence_capture_topic_offsets vehicle-telemetry
evidence_count kafka_topic_end_offset "$TOPIC"
for k in total_messages vehicles vehicles_multi_partition inversions vehicles_with_inversion max_backward_ms; do
  evidence_count "$k" "$(echo "$SUMMARY" | grep -oE "\"$k\": *[0-9]+" | grep -oE '[0-9]+$' || echo 0)"
done

INV=$(echo "$SUMMARY" | grep -oE '"inversions": *[0-9]+' | grep -oE '[0-9]+$' || echo 0)
MULTI=$(echo "$SUMMARY" | grep -oE '"vehicles_multi_partition": *[0-9]+' | grep -oE '[0-9]+$' || echo 0)

# 판정은 "역전 0"이 아니다. 키 파티셔닝이 깨졌는지만 실패로 본다 —
# 역전 자체는 이 실험이 재려는 대상이라 미리 실패로 정해두면 안 된다.
CRIT="차량이 단일 파티션에 유지된다 (역전 건수는 판정이 아니라 관측 대상)"
if [ "${MULTI:-0}" -ne 0 ]; then
  VERDICT="FAIL (차량 $MULTI대가 여러 파티션에 걸쳤다 — 순서 보장의 전제가 깨짐)"
else
  VERDICT="관찰 (역전 ${INV}건)"
fi
log "판정: $CRIT → $VERDICT"
evidence_capture_file "$OUT" console.log
evidence_finish "$CRIT" "$VERDICT"
log "DONE"
