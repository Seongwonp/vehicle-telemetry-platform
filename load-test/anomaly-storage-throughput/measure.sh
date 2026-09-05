#!/bin/bash
# `anomaly-storage-group`(Kafka → PostgreSQL 알림 저장) 처리량 측정.
#
# 두 번의 다른 실험에서 같은 증상을 봤다 — 부하를 멈춘 뒤 이 컨슈머가 따라잡는 데
# **13분**이 걸렸고(`load-test/rebalance-redelivery/`), PostgreSQL 60초 장애 때는
# 200초를 기다려도 lag 3,693이 남았다(`load-test/anomaly-dlq-idempotency/`).
# 그때마다 "느리다"고 적기만 하고 **얼마나 느린지는 재지 않았다.**
#
# 여기서 재는 건 하나다: **백로그를 초당 몇 건씩 줄이는가.**
# 부하를 걸어 lag을 쌓고, 부하를 끊은 뒤 lag 감소 속도를 샘플링한다.
# 부하가 멈춘 뒤에는 유입이 0이라 lag 감소분이 곧 처리량이다.
#
# 사용법: bash measure.sh [부하 지속 초] [샘플 수] [차량 수]
set -euo pipefail
cd "$(dirname "$0")/../.."

LOAD_SEC="${1:-120}"
VEHICLES="${3:-100}"
SAMPLES="${2:-12}"
COMPOSE="docker compose -f docker-compose.yml -f docker-compose.dev.yml"
OUT="load-test/anomaly-storage-throughput/_result.txt"

# 원본 증거 보존(docs/evidence-policy.md P0-1).
# shellcheck source=../lib/evidence.sh
. load-test/lib/evidence.sh
evidence_init "anomaly-storage-throughput" \
  "bash load-test/anomaly-storage-throughput/measure.sh $LOAD_SEC $SAMPLES $VEHICLES"
evidence_input vehicles "$VEHICLES"
evidence_input publish_interval_sec 0.2
evidence_input anomaly_rate 0.3
evidence_input load_sec "$LOAD_SEC"
evidence_input samples "$SAMPLES"

log() { echo "[$(date +%H:%M:%S)] $*" | tee -a "$OUT"; }
wait_sec() { local s=$1 t0; t0=$(date +%s); until [ $(( $(date +%s) - t0 )) -ge "$s" ]; do sleep 5; done; }
alert_lag() {
  docker exec telemetry-kafka kafka-consumer-groups --bootstrap-server localhost:29092 \
    --describe --group anomaly-storage-group 2>/dev/null | awk 'NR>1{s+=$6} END{print s+0}'
}

: > "$OUT"
log "=== anomaly-storage-group 처리량 ==="

$COMPOSE down -v >/dev/null 2>&1 || true
$COMPOSE up -d mosquitto zookeeper kafka influxdb postgres redis >/dev/null 2>&1 || true
wait_until 300 "PostgreSQL healthy" bash -c '[ "$(docker inspect telemetry-postgres --format "{{.State.Health.Status}}" 2>/dev/null)" = healthy ]'
$COMPOSE up -d backend anomaly-detector >/dev/null 2>&1
wait_until 300 "backend actuator 200" bash -c 'curl -s -o /dev/null -w "%{http_code}" http://localhost:8080/actuator/health 2>/dev/null | grep -q 200'
log "스택 기동 완료"

docker rm -f telemetry-sim-0 >/dev/null 2>&1 || true
$COMPOSE run -d --name telemetry-sim-0 \
  -e VEHICLE_COUNT=$VEHICLES -e PUBLISH_INTERVAL=0.2 -e ANOMALY_RATE=0.3 simulator >/dev/null 2>&1
log "부하 기동 (${VEHICLES}대/0.2초, 이상률 0.3) — ${LOAD_SEC}초"
wait_sec "$LOAD_SEC"
log "부하 중 lag=$(alert_lag)"

docker stop -t 30 telemetry-sim-0 >/dev/null 2>&1 || true
docker rm -f telemetry-sim-0 >/dev/null 2>&1 || true
log "부하 정지 — 여기서부터 lag 감소분이 곧 처리량이다"

PREV=$(alert_lag); T0=$(date +%s); LAG_START=$PREV
# 회차별 값을 남긴다 — 평균만 적으면 편차를 알 수 없다(docs/evidence-policy.md).
echo "sample,elapsed_sec,lag,drop,msg_per_sec" > "$EVIDENCE_DIR/lag-samples.csv"
log "샘플 시작: lag=$PREV"
for i in $(seq 1 "$SAMPLES"); do
  wait_sec 15
  NOW=$(alert_lag); T=$(date +%s)
  RATE=$(( (PREV - NOW) / 15 ))
  log "  +$(( T - T0 ))초  lag=$NOW  (감소 $((PREV - NOW)) → ${RATE} msg/s)"
  echo "$i,$(( T - T0 )),$NOW,$((PREV - NOW)),$RATE" >> "$EVIDENCE_DIR/lag-samples.csv"
  [ "$NOW" = "0" ] && break
  PREV=$NOW
done

log ""
log "지표:"
curl -s http://localhost:8080/actuator/prometheus 2>/dev/null \
  | grep -E "telemetry_anomaly_(stored|save)" | tee -a "$OUT"

# ── 원본 증거 ──────────────────────────────────────────────────
STORED=$(curl -s http://localhost:8080/actuator/prometheus 2>/dev/null \
  | grep 'telemetry_anomaly_stored_total{.*result="new"' | awk '{print $2+0}' | tail -1)
evidence_capture_prometheus final
evidence_capture_kafka_groups anomaly-storage-group anomaly-detector-group
evidence_capture_topic_offsets vehicle-anomaly-alerts

evidence_count lag_at_load_end "$LAG_START"
evidence_count lag_at_sampling_end "$(alert_lag)"
evidence_count anomaly_stored_new "${STORED:-0}"
# 샘플 구간의 중앙값이 아니라 회차별 값 전체를 lag-samples.csv에 남긴다.
evidence_count drain_rate_min_msg_per_sec \
  "$(awk -F, 'NR>1 && $5!="" {if(m==""||$5<m)m=$5} END{print m+0}' "$EVIDENCE_DIR/lag-samples.csv")"
evidence_count drain_rate_max_msg_per_sec \
  "$(awk -F, 'NR>1 && $5!="" {if($5>m)m=$5} END{print m+0}' "$EVIDENCE_DIR/lag-samples.csv")"

CRIT="부하 중 lag이 발산하지 않고, 부하 정지 후 lag이 감소한다"
if [ "$LAG_START" -lt 1000 ]; then
  VERDICT="PASS (부하 중 lag=$LAG_START — 유입 전량 소화, 드레인 잴 백로그 없음)"
elif [ "$(alert_lag)" -lt "$LAG_START" ]; then
  VERDICT="관찰 (백로그 $LAG_START에서 드레인 중 — lag-samples.csv 참고)"
else
  VERDICT="FAIL (lag이 줄지 않음)"
fi
log "판정: $CRIT → $VERDICT"
evidence_capture_file "$OUT" console.log
evidence_finish "$CRIT" "$VERDICT"
log "DONE"
