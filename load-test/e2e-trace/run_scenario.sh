#!/bin/bash
# MQTT → Kafka → InfluxDB → REST → WebSocket 엔드투엔드 추적 (docs/roadmap.md P1-5).
#
# 배경 부하를 함께 돌린다. 부하가 없으면 "빈 파이프라인에서의 지연"을 재게 되는데,
# 그건 운영에서 보게 될 값이 아니다. 배경 부하는 시뮬레이터가 만들고, 마커는 별도의
# 차량 ID로 흘려서 배경과 섞이지 않게 한다.
#
# 사용법: bash run_scenario.sh [마커 수] [간격 초]
set -euo pipefail
cd "$(dirname "$0")/../.."

COUNT="${1:-20}"
GAP="${2:-0.5}"
VEHICLES="${VEHICLES:-50}"
VEHICLE_ID="${VEHICLE_ID:-E2E-TRACE-01}"
COMPOSE="docker compose -f docker-compose.yml -f docker-compose.dev.yml"
OUT="load-test/e2e-trace/_result.txt"
NET="vehicle-telemetry-platform_telemetry-net"
IMG="telemetrix-e2e-trace"

# shellcheck source=../lib/evidence.sh
. load-test/lib/evidence.sh
evidence_init "e2e-trace" "bash load-test/e2e-trace/run_scenario.sh $COUNT $GAP"
evidence_input marker_count "$COUNT"
evidence_input marker_gap_sec "$GAP"
evidence_input marker_vehicle_id "$VEHICLE_ID"
evidence_input background_vehicles "$VEHICLES"
evidence_input publish_interval_sec 0.2

log() { echo "[$(date +%H:%M:%S)] $*" | tee -a "$OUT"; }
wait_sec() { local s=$1 t0; t0=$(date +%s); until [ $(( $(date +%s) - t0 )) -ge "$s" ]; do sleep 5; done; }

: > "$OUT"
log "=== P1-5 엔드투엔드 추적 (마커 ${COUNT}건, 간격 ${GAP}초) ==="

# ── 1. 스택 ────────────────────────────────────────────────────
$COMPOSE down -v >/dev/null 2>&1 || true
$COMPOSE up -d mosquitto zookeeper kafka influxdb postgres redis >/dev/null 2>&1 || true
wait_until 300 "PostgreSQL healthy" bash -c '[ "$(docker inspect telemetry-postgres --format "{{.State.Health.Status}}" 2>/dev/null)" = healthy ]'
$COMPOSE up -d backend anomaly-detector >/dev/null 2>&1
wait_until 300 "backend actuator 200" bash -c 'curl -s -o /dev/null -w "%{http_code}" http://localhost:8080/actuator/health 2>/dev/null | grep -q 200'
log "스택 기동 완료"

# ── 2. 배경 부하 ───────────────────────────────────────────────
docker rm -f telemetry-sim-0 >/dev/null 2>&1 || true
$COMPOSE run -d --name telemetry-sim-0 \
  -e VEHICLE_COUNT=$VEHICLES -e PUBLISH_INTERVAL=0.2 simulator >/dev/null 2>&1
log "배경 부하 기동 (${VEHICLES}대) — 60초 안정화"
wait_sec 60

# ── 3. 추적 ────────────────────────────────────────────────────
# 자격 증명은 .env에서 읽어 **환경변수로만** 넘긴다. 출력에도 증거에도 남기지 않는다
# (docs/evidence-policy.md).
docker build -q -t "$IMG" load-test/e2e-trace >/dev/null
WINPWD=$(pwd -W 2>/dev/null || pwd)
log "추적 시작"
set +e
MSYS_NO_PATHCONV=1 docker run --rm --network "$NET" \
  -v "$WINPWD/load-test/e2e-trace:/w" -w /w \
  --env-file .env \
  "$IMG" python -u trace.py --vehicle-id "$VEHICLE_ID" --count "$COUNT" --gap-sec "$GAP" \
  2>&1 | tee -a "$OUT"
TRACE_RC=${PIPESTATUS[0]}
set -e
log "추적 종료 (rc=$TRACE_RC)"

# ── 4. 정리 ────────────────────────────────────────────────────
docker stop -t 60 telemetry-sim-0 >/dev/null 2>&1 || true
docker rm -f telemetry-sim-0 >/dev/null 2>&1 || true

# ── 5. 집계 ────────────────────────────────────────────────────
# 마커별 원본은 trace.py가 CSV_BEGIN/CSV_END 사이에 찍는다. 그 구간만 잘라 남긴다.
awk '/^CSV_BEGIN$/{f=1;next} /^CSV_END$/{f=0} f' "$OUT" > "$EVIDENCE_DIR/markers.csv" || true
stage_count() { awk -F, -v c="$1" 'NR>1 && $c=="1"' "$EVIDENCE_DIR/markers.csv" | wc -l | tr -d ' '; }
K=$(stage_count 2); I=$(stage_count 3); R=$(stage_count 4); W=$(stage_count 5)

evidence_capture_prometheus final
evidence_capture_topic_offsets vehicle-telemetry
evidence_capture_file "$OUT" console.log
evidence_count markers_published "$COUNT"
evidence_count arrived_kafka "$K"
evidence_count arrived_influx "$I"
evidence_count arrived_rest "$R"
evidence_count arrived_websocket "$W"

log ""
log "=== 단계별 도착 (마커 $COUNT건) ==="
log "Kafka $K / InfluxDB $I / REST $R / WebSocket $W"

CRIT="마커 전량이 5단계(MQTT·Kafka·InfluxDB·REST·WebSocket)에 도달"
if [ "$K" = "$COUNT" ] && [ "$I" = "$COUNT" ] && [ "$R" = "$COUNT" ] && [ "$W" = "$COUNT" ]; then
  VERDICT="PASS (전 단계 $COUNT/$COUNT)"
else
  VERDICT="관찰 (kafka $K, influx $I, rest $R, ws $W / $COUNT)"
fi
log "판정: $CRIT → $VERDICT"
evidence_finish "$CRIT" "$VERDICT"
log "DONE"
