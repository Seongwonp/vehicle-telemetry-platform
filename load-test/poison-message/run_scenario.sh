#!/bin/bash
# 독성 메시지 격리 경계 (docs/roadmap.md P1-2).
#
# 질문은 하나다: **한 건의 처리 불가능한 메시지가 정상 메시지까지 막는가.**
#
# 독성 유형마다 같은 파티션 키의 정상 레코드 N건을 함께 넣고, InfluxDB에 몇 건이
# 남았는지로 센다. 대조군이 없으면 "독성이 DLQ로 갔다"까지만 알 수 있는데
# 그건 이 실험의 질문이 아니다.
#
# 사용법: bash run_scenario.sh [대조군 수]
set -euo pipefail
cd "$(dirname "$0")/../.."

CONTROLS="${1:-100}"
COMPOSE="docker compose -f docker-compose.yml -f docker-compose.dev.yml"
OUT="load-test/poison-message/_result.txt"
NET="vehicle-telemetry-platform_telemetry-net"
IMG="vehicle-telemetry-platform-anomaly-detector"
TYPES="${TYPES:-malformed_json bad_timestamp wrong_schema infinity huge_payload}"

# shellcheck source=../lib/evidence.sh
. load-test/lib/evidence.sh
evidence_init "poison-message" "bash load-test/poison-message/run_scenario.sh $CONTROLS"
evidence_input controls_per_type "$CONTROLS"
evidence_input poison_types "$TYPES"

TOK=$(grep '^INFLUXDB_TOKEN=' .env | cut -d= -f2-)
ORG=$(grep '^INFLUXDB_ORG=' .env | cut -d= -f2-)
BKT=$(grep '^INFLUXDB_BUCKET=' .env | cut -d= -f2-)

log() { echo "[$(date +%H:%M:%S)] $*" | tee -a "$OUT"; }
wait_sec() { local s=$1 t0; t0=$(date +%s); until [ $(( $(date +%s) - t0 )) -ge "$s" ]; do sleep 5; done; }
storage_lag() {
  docker exec telemetry-kafka kafka-consumer-groups --bootstrap-server localhost:29092 \
    --describe --group telemetry-storage-group 2>/dev/null | awk 'NR>1{s+=$6} END{print s+0}'
}
topic_end_offsets() {
  docker exec telemetry-kafka kafka-run-class kafka.tools.GetOffsetShell \
    --broker-list localhost:29092 --topic "$1" 2>/dev/null | awk -F: '{s+=$3} END{print s+0}'
}
rows_for() {  # $1 = vehicle_id
  docker exec telemetry-influxdb influx query \
    "from(bucket: \"$BKT\") |> range(start: -2h) |> filter(fn: (r) => r._measurement == \"vehicle_telemetry\" and r._field == \"speed\" and r.vehicle_id == \"$1\") |> group() |> count()" \
    --org "$ORG" --token "$TOK" --raw 2>/dev/null \
    | tr -d '\r' | awk -F, '$0 ~ /^,/ && $NF ~ /^[0-9]+$/ {v=$NF} END{print v+0}'
}
WINPWD=$(pwd -W 2>/dev/null || pwd)
inject() {
  MSYS_NO_PATHCONV=1 docker run --rm --network "$NET" \
    -v "$WINPWD/load-test/poison-message:/w" -w /w "$IMG" python inject_poison.py "$@"
}
dlq_tool() {
  MSYS_NO_PATHCONV=1 docker run --rm --network "$NET" \
    -v "$WINPWD/dlq-tools:/w" -w /w "$IMG" python dlq.py "$@"
}

: > "$OUT"
log "=== 독성 메시지 격리 경계 (대조군 유형당 ${CONTROLS}건) ==="

$COMPOSE down -v >/dev/null 2>&1 || true
$COMPOSE up -d mosquitto zookeeper kafka influxdb postgres redis backend >/dev/null 2>&1 || true
wait_until 300 "PostgreSQL healthy" bash -c '[ "$(docker inspect telemetry-postgres --format "{{.State.Health.Status}}" 2>/dev/null)" = healthy ]'
$COMPOSE up -d backend >/dev/null 2>&1
wait_until 300 "backend actuator 200" bash -c 'curl -s -o /dev/null -w "%{http_code}" http://localhost:8080/actuator/health 2>/dev/null | grep -q 200'
log "스택 기동 완료 (시뮬레이터는 띄우지 않는다 — 배경 부하가 섞이면 배치 경계가 흐려진다)"

# 유형을 하나씩, 앞 유형의 처리가 끝난 뒤에 넣는다. 한꺼번에 넣으면 어느 독성이
# 어느 배치를 망쳤는지 구분할 수 없다.
for t in $TYPES; do
  log ""
  log "--- 주입: $t ---"
  inject --type "$t" --controls "$CONTROLS" 2>&1 | sed 's/^/  /' | tee -a "$OUT"
  # 재시도 예산(180초)을 넘겨야 배치 실패형 독성이 DLQ까지 간다.
  wait_sec 30
  for _ in $(seq 1 60); do [ "$(storage_lag)" = "0" ] && break; sleep 10; done
  log "  lag=$(storage_lag)"
done

log ""
log "드레인 대기"
wait_sec 30
for _ in $(seq 1 90); do [ "$(storage_lag)" = "0" ] && break; sleep 10; done
log "드레인 완료 (lag=$(storage_lag))"

log ""
log "=== 결과: 유형별 대조군 생존 ==="
log "유형              대조군 저장/주입   판정"
ALL_OK=1
for t in $TYPES; do
  vid="POISON-$(echo "$t" | tr 'a-z_' 'A-Z-' | cut -c1-12)"
  vid="${vid:0:20}"
  n=$(rows_for "$vid")
  evidence_count "controls_stored_$t" "$n"
  if [ "$n" -ge "$CONTROLS" ]; then verdict="격리됨"; else verdict="**정상 레코드도 막힘**"; ALL_OK=0; fi
  log "$(printf '%-16s %6s / %-6s   %s' "$t" "$n" "$CONTROLS" "$verdict")"
done

TDLQ=$(topic_end_offsets vehicle-telemetry-dlq)
log ""
log "vehicle-telemetry-dlq : $TDLQ"
evidence_count telemetry_dlq_records "$TDLQ"
evidence_count telemetry_topic "$(topic_end_offsets vehicle-telemetry)"

log ""
log "=== DLQ 분류와 추적 정보 ==="
dlq_tool --topic vehicle-telemetry-dlq inspect --show-samples 2>&1 | tee -a "$OUT" \
  > "$EVIDENCE_DIR/dlq-inspect.txt" || true
sed -n '1,40p' "$EVIDENCE_DIR/dlq-inspect.txt" 2>/dev/null | tee -a "$OUT" || true

evidence_capture_prometheus final
evidence_capture_kafka_groups telemetry-storage-group
evidence_capture_topic_offsets vehicle-telemetry vehicle-telemetry-dlq
evidence_capture_log_lines telemetry-backend \
  "역직렬화|변환 실패|배치 저장|DLQ로 이동|Backoff" backend-key-lines.txt

CRIT="독성 1건이 같은 배치의 정상 레코드를 막지 않는다"
if [ "$ALL_OK" -eq 1 ]; then VERDICT="PASS (모든 유형에서 대조군 전량 저장)"
else VERDICT="FAIL (일부 유형에서 정상 레코드가 함께 막혔다 — 위 표 참고)"; fi
log "판정: $CRIT → $VERDICT"
evidence_capture_file "$OUT" console.log
evidence_finish "$CRIT" "$VERDICT"
log "DONE"
