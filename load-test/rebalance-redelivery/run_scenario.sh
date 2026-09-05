#!/bin/bash
# 컨슈머 리밸런싱 구간의 재전달량 측정.
#
# 우선순위 1번의 남은 갈래. 강제 종료(`docker kill`) 시 재전달은 이미 쟀지만
# (`load-test/storage-integrity/`), **다중 인스턴스가 붙었다 떨어질 때** 도는
# 리밸런싱 구간은 안 쟀다. 실제 운영에서 훨씬 흔한 쪽은 이쪽이다 —
# 배포, 스케일 조정, OOM kill이 전부 리밸런싱을 만든다.
#
# 대상은 `anomaly-detector`다. 이 프로젝트에서 실제로 여러 인스턴스로 도는 유일한
# 컨슈머 그룹이고(replicas 3, ADR-016), 파티션 3개에 1:1로 배정돼 있어
# 인스턴스 수를 바꾸면 반드시 리밸런싱이 돈다.
#
# 정답 기준은 **토픽의 고유 event_id 수**다. 재전달이 생기면 같은 event_id가 여러 번
# 발행되므로, "토픽 메시지 수 − 고유 event_id 수"가 곧 재전달량이다.
# 그게 PostgreSQL 행을 늘리지 않는지(멱등)까지 한 번에 본다.
#
# 사용법: bash run_scenario.sh
set -euo pipefail
cd "$(dirname "$0")/../.."

COMPOSE="docker compose -f docker-compose.yml -f docker-compose.dev.yml"
OUT="load-test/rebalance-redelivery/_result.txt"
NET="vehicle-telemetry-platform_telemetry-net"
IMG="vehicle-telemetry-platform-anomaly-detector"
WINPWD=$(pwd -W 2>/dev/null || pwd)

PG_DB=$(grep '^POSTGRES_DB=' .env | cut -d= -f2-)
PG_USER=$(grep '^POSTGRES_USER=' .env | cut -d= -f2-)

log() { echo "[$(date +%H:%M:%S)] $*" | tee -a "$OUT"; }
wait_sec() { local s=$1 t0; t0=$(date +%s); until [ $(( $(date +%s) - t0 )) -ge "$s" ]; do sleep 5; done; }
psql_scalar() {
  docker exec telemetry-postgres psql -U "$PG_USER" -d "$PG_DB" -tAc "$1" 2>/dev/null | tr -d '\r' | head -1
}
rows() { psql_scalar "SELECT count(*) FROM anomaly_alerts;"; }
topic_end_offsets() {
  docker exec telemetry-kafka kafka-run-class kafka.tools.GetOffsetShell \
    --broker-list localhost:29092 --topic "$1" 2>/dev/null | awk -F: '{s+=$3} END{print s+0}'
}
alert_lag() {
  docker exec telemetry-kafka kafka-consumer-groups --bootstrap-server localhost:29092 \
    --describe --group anomaly-storage-group 2>/dev/null | awk 'NR>1{s+=$6} END{print s+0}'
}
detector_lag() {
  docker exec telemetry-kafka kafka-consumer-groups --bootstrap-server localhost:29092 \
    --describe --group anomaly-detector-group 2>/dev/null | awk 'NR>1{s+=$6} END{print s+0}'
}
metric() {  # $1 = 메트릭 이름(라벨 포함 문자열로 grep)
  curl -s http://localhost:8080/actuator/prometheus 2>/dev/null \
    | grep -F "$1" | awk '{print $2+0}' | tail -1
}
ground_truth() {
  MSYS_NO_PATHCONV=1 docker run --rm --network "$NET" \
    -v "$WINPWD/load-test/anomaly-dlq-idempotency:/w" -w /w "$IMG" \
    python count_events.py --topic vehicle-anomaly-alerts
}
# 리밸런싱이 실제로 돌았는지는 로그로 확인한다. 안 돌았으면 이 측정은 무의미하다.
#
# 문구는 클라이언트마다 다르다. anomaly-detector는 **kafka-python**이라
# "Revoking previously assigned partitions"이고, Java 쪽의 "Revoke previously
# assigned partitions"로 grep하면 0이 나온다(처음에 그렇게 재서 "리밸런싱 0건"이라는
# 잘못된 결론을 낼 뻔했다). 스케일 다운으로 사라진 컨테이너의 로그는 함께 사라지므로,
# 남아 있는 인스턴스 기준의 **하한**이라는 점도 감안해서 읽어야 한다.
rebalance_count() {
  $COMPOSE logs anomaly-detector 2>/dev/null \
    | grep -ciE "Revoking previously assigned partitions|is rebalancing; rejoining" || true
}

: > "$OUT"
log "=== 리밸런싱 재전달 측정 ==="

$COMPOSE down -v >/dev/null 2>&1 || true
$COMPOSE up -d mosquitto zookeeper kafka influxdb postgres redis >/dev/null 2>&1 || true
until [ "$(docker inspect telemetry-postgres --format '{{.State.Health.Status}}' 2>/dev/null)" = "healthy" ]; do sleep 10; done
$COMPOSE up -d backend anomaly-detector >/dev/null 2>&1
until curl -s -o /dev/null -w '%{http_code}' http://localhost:8080/actuator/health 2>/dev/null | grep -q 200; do sleep 5; done
log "스택 기동 완료 (anomaly-detector $($COMPOSE ps -q anomaly-detector | wc -l)개)"

docker rm -f telemetry-sim-0 >/dev/null 2>&1 || true
$COMPOSE run -d --name telemetry-sim-0 \
  -e VEHICLE_COUNT=100 -e PUBLISH_INTERVAL=0.2 -e ANOMALY_RATE=0.3 simulator >/dev/null 2>&1
log "부하 기동 (100대/0.2초, 이상률 0.3) — 60초 정상 구간"
wait_sec 60
log "정상 구간 종료: 행=$(rows) 탐지 lag=$(detector_lag)"

# ── 리밸런싱 1회차: 3 → 1 ──────────────────────────────────────
log "--- 리밸런싱 #1: anomaly-detector 3 → 1 ---"
$COMPOSE up -d --scale anomaly-detector=1 --no-recreate anomaly-detector >/dev/null 2>&1
wait_sec 60

# ── 리밸런싱 2회차: 1 → 3 ──────────────────────────────────────
log "--- 리밸런싱 #2: anomaly-detector 1 → 3 ---"
$COMPOSE up -d --scale anomaly-detector=3 --no-recreate anomaly-detector >/dev/null 2>&1
wait_sec 60

# ── 정지 후 드레인 ─────────────────────────────────────────────
log "부하 정지 (SIGTERM)"
docker stop -t 60 telemetry-sim-0 >/dev/null 2>&1 || true
docker rm -f telemetry-sim-0 >/dev/null 2>&1 || true
for _ in $(seq 1 120); do
  [ "$(detector_lag)" = "0" ] && [ "$(alert_lag)" = "0" ] && break
  sleep 5
done
log "드레인 완료 (탐지 lag=$(detector_lag), 저장 lag=$(alert_lag))"

# ── 집계 ───────────────────────────────────────────────────────
TOPIC=$(topic_end_offsets vehicle-anomaly-alerts)
DLQ=$(topic_end_offsets vehicle-anomaly-alerts-dlq)
ROWS=$(rows)
NEW=$(metric 'telemetry_anomaly_stored_total{application="vehicle-telemetry-backend",result="new",}')
DUP=$(metric 'telemetry_anomaly_stored_total{application="vehicle-telemetry-backend",result="duplicate",}')

log ""
log "=== 결과 ==="
log "리밸런싱 로그 건수        : $(rebalance_count)  (0이면 이 측정은 무의미하다)"
log "Kafka vehicle-anomaly-alerts : $TOPIC   (DLQ $DLQ)"
log "PostgreSQL 행             : $ROWS"
log "telemetry_anomaly_stored  : new=$NEW  duplicate=$DUP"
log ""
log "정답 기준 산출:"
ground_truth | tee -a "$OUT"
log ""
log "  토픽 메시지 − 고유 event_id = 재전달량"
log "  고유 event_id == PostgreSQL 행($ROWS) 이면 재전달이 행을 늘리지 않은 것이다"
log "DONE"
