#!/bin/bash
# 입력 계약(P0-2)을 **실제 파이프라인 끝까지** 검증한다.
#
# ## 왜 필요한가
#
# 단위·핸들러 테스트는 "decoder가 거부한다"까지만 본다. 실제로는 그 뒤에 브로커·Kafka·
# 백엔드·InfluxDB·DLQ가 있고, **거부된 메시지가 정말 저장되지 않고 지정된 DLQ로 가는지**는
# 거기까지 가봐야 안다. 2026-09-09 P0-2 완료 조건이다.
#
# ## 무엇을 가르는가
#
# 차량 ID를 **시나리오·입구별로 분리**한다(`SCHEMA141533-M04` 등). 같은 ID를 쓰면
# 저장 결과에서 어느 시나리오 것인지 귀속시킬 수 없다.
# 입구는 **한 글자**(M/K)다 — 계약의 ID 상한이 20자라 `-KAFKA-01`이면 넘친다.
# DLQ는 **시작 offset을 먼저 기록**하고 증가분만 본다 — 이전 실행의 잔여물이 섞이지 않게.
#
# 사용법: bash load-test/schema-contract/run_e2e.sh
set -uo pipefail
cd "$(dirname "$0")/../.."

PREFIX="${PREFIX:-SCHEMA$(date +%H%M%S)}"
COMPOSE="docker compose -f docker-compose.yml -f docker-compose.dev.yml"
OUT="load-test/schema-contract/_result_e2e.txt"
# paho(MQTT)와 kafka-python이 **둘 다** 있는 이미지여야 한다. anomaly-detector에는 paho가,
# simulator에는 kafka가 없다 — e2e-trace 이미지가 두 입구를 다 쓸 수 있는 유일한 것이다.
TOOLS="telemetrix-e2e-trace:latest"
NET="vehicle-telemetry-platform_telemetry-net"

# shellcheck source=../lib/evidence.sh
. load-test/lib/evidence.sh
evidence_init "schema-contract" "bash load-test/schema-contract/run_e2e.sh"
evidence_input prefix "$PREFIX"
evidence_input entrances "MQTT + Kafka 직접"
evidence_input profile "dev(평문)"

TOK=$(grep '^INFLUXDB_TOKEN=' .env | cut -d= -f2-)
ORG=$(grep '^INFLUXDB_ORG=' .env | cut -d= -f2-)
BKT=$(grep '^INFLUXDB_BUCKET=' .env | cut -d= -f2-)
PGUSER=$(grep '^POSTGRES_USER=' .env | cut -d= -f2-)
PGDB=$(grep '^POSTGRES_DB=' .env | cut -d= -f2-)

log() { echo "[$(date +%H:%M:%S)] $*" | tee -a "$OUT"; }
WINPWD=$(pwd -W 2>/dev/null || pwd)

topic_end() {  # $1 = topic
  docker exec telemetry-kafka kafka-run-class kafka.tools.GetOffsetShell \
    --broker-list localhost:29092 --topic "$1" 2>/dev/null \
    | awk -F: '{s+=$3} END{print s+0}'
}

# 한 차량의 저장 행 수. **필드 하나(speed)만 세서 행 수와 같게 만든다.**
rows_for() {
  docker exec telemetry-influxdb influx query \
    "from(bucket: \"$BKT\") |> range(start: -1h) |> filter(fn: (r) => r._measurement == \"vehicle_telemetry\" and r._field == \"speed\" and r.vehicle_id == \"$1\") |> group() |> count()" \
    --org "$ORG" --token "$TOK" --raw 2>/dev/null \
    | tr -d '\r' | awk -F, '$0 ~ /^,/ && $NF ~ /^[0-9]+$/ {v=$NF} END{print v+0}'
}

# 저장된 필드 값 하나. **헤더에서 `_value` 열을 찾아 읽는다** —
# 열 위치를 상수로 박으면 필드 이름이 그 자리에 잡혀 "rpm"이 값으로 나온다(첫 실행에서 그랬다).
field_value() {  # $1 = vehicle_id, $2 = field
  docker exec telemetry-influxdb influx query \
    "from(bucket: \"$BKT\") |> range(start: -1h) |> filter(fn: (r) => r._measurement == \"vehicle_telemetry\" and r._field == \"$2\" and r.vehicle_id == \"$1\") |> group() |> last()" \
    --org "$ORG" --token "$TOK" --raw 2>/dev/null \
    | tr -d '\r' \
    | awk -F, '
        /^,result/ { for (i = 1; i <= NF; i++) if ($i == "_value") c = i; next }
        /^,/ && c > 0 { v = $c }
        END { print v }'
}

anomaly_count() {  # $1 = vehicle_id
  docker exec telemetry-postgres psql -U "$PGUSER" -d "$PGDB" -tAc \
    "SELECT COUNT(*) FROM anomaly_alerts WHERE vehicle_id = '$1'" 2>/dev/null | tr -d '\r '
}

: > "$OUT"
log "=== 입력 계약 E2E (prefix=$PREFIX) ==="

# ── 1. 스택 ────────────────────────────────────────────────────
$COMPOSE --profile scale down -v >/dev/null 2>&1 || true
$COMPOSE up -d mosquitto zookeeper kafka influxdb postgres redis kafka-init >/dev/null 2>&1 || true
wait_until 300 "Kafka healthy" bash -c '[ "$(docker inspect telemetry-kafka --format "{{.State.Health.Status}}" 2>/dev/null)" = healthy ]'
wait_until 300 "토픽 생성" bash -c 'docker exec telemetry-kafka kafka-topics --bootstrap-server localhost:29092 --list 2>/dev/null | grep -q vehicle-telemetry'
# **반드시 빌드한다.** compose는 이미지가 있으면 재사용하므로, 코드를 고쳐도 옛 이미지가 뜬다.
# 2026-09-09 첫 실행이 정확히 그래서 "전부 저장됨 / DLQ 증가 0"으로 나왔다 —
# 계약 검증이 통째로 무효였다(옛 코드를 잰 것이다). CLAUDE.md에도 같은 경고가 있었다.
log "backend 이미지 빌드 (캐시된 옛 이미지로 검증하지 않기 위해)"
$COMPOSE build backend >/dev/null 2>&1 || { log "** 빌드 실패 — 중단"; exit 1; }
$COMPOSE up -d backend anomaly-detector >/dev/null 2>&1
wait_until 300 "backend actuator 200" bash -c 'curl -s -o /dev/null -w "%{http_code}" http://localhost:8080/actuator/health 2>/dev/null | grep -q 200'
log "스택 기동 완료 (감지기 포함)"

# 감지기가 그룹에 붙을 때까지 — 안 붙으면 이상 감지 시나리오가 거짓 실패한다.
wait_until 180 "anomaly-detector-group 등록" bash -c '
  docker exec telemetry-kafka kafka-consumer-groups --bootstrap-server localhost:29092 \
    --list 2>/dev/null | grep -q anomaly-detector-group'

# ── 2. 시작 offset ─────────────────────────────────────────────
DLQ_KAFKA_BEFORE=$(topic_end vehicle-telemetry-dlq)
DLQ_MQTT_BEFORE=$(topic_end vehicle-telemetry-mqtt-dlq)
log "시작 offset — kafka-dlq=$DLQ_KAFKA_BEFORE mqtt-dlq=$DLQ_MQTT_BEFORE"
evidence_count dlq_kafka_before "$DLQ_KAFKA_BEFORE"
evidence_count dlq_mqtt_before "$DLQ_MQTT_BEFORE"

# ── 3. 주입 ────────────────────────────────────────────────────
log "두 입구로 시나리오 주입"
MSYS_NO_PATHCONV=1 docker run --rm --network "$NET" \
  -v "$WINPWD/load-test/schema-contract:/w" -w /w "$TOOLS" \
  python inject_contract_cases.py --prefix "$PREFIX" --mixed-batch \
  > "$EVIDENCE_DIR/manifest.json" 2>"$EVIDENCE_DIR/inject.err"
if [ ! -s "$EVIDENCE_DIR/manifest.json" ]; then
  log "** 주입 실패 — inject.err 확인"; sed -n '1,15p' "$EVIDENCE_DIR/inject.err" | tee -a "$OUT"; exit 1
fi
log "주입 완료 ($(grep -c vehicle_id "$EVIDENCE_DIR/manifest.json")건)"

log "드레인 대기 40초"
st=$(date +%s); until [ $(( $(date +%s) - st )) -ge 40 ]; do sleep 5; done

# ── 4. 판정 ────────────────────────────────────────────────────
RESULT="$EVIDENCE_DIR/e2e_results.csv"
echo "entrance,case,vehicle_id,expect,rows,verdict" > "$RESULT"
PASS=0; FAIL=0

log ""
log "입구   시나리오                차량ID                 기대      행수  판정"
while IFS=$'\t' read -r entrance case vid expect; do
  [ -n "$vid" ] || continue
  rows=$(rows_for "$vid")
  verdict="?"
  case "$expect" in
    stored)   [ "${rows:-0}" -ge 1 ] && verdict=PASS || verdict=FAIL ;;
    rejected) [ "${rows:-0}" -eq 0 ] && verdict=PASS || verdict=FAIL ;;
    2_of_3_stored) [ "${rows:-0}" -eq 2 ] && verdict=PASS || verdict=FAIL ;;
  esac
  [ "$verdict" = PASS ] && PASS=$((PASS+1)) || FAIL=$((FAIL+1))
  printf "%-6s %-22s %-22s %-9s %-5s %s\n" "$entrance" "$case" "$vid" "$expect" "${rows:-0}" "$verdict" | tee -a "$OUT"
  echo "$entrance,$case,$vid,$expect,${rows:-0},$verdict" >> "$RESULT"
done < <(MSYS_NO_PATHCONV=1 docker run --rm -i "$TOOLS" python -c '
import json,sys
for m in json.load(sys.stdin):
    print("\t".join([m["entrance"], m["case"], m["vehicle_id"], m["expect"]]))
' < "$EVIDENCE_DIR/manifest.json")

# ── 5. 값 보존과 이상 감지 ─────────────────────────────────────
log ""
# 입구 코드는 **한 글자**다(M/K). 계약의 차량 ID 상한이 20자라
# `-MQTT-01`/`-KAFKA-01`로 만들면 Kafka 쪽만 21자가 되어 **시나리오가 아니라 ID 때문에**
# 거부된다 — 2026-09-09 2회차 실행이 통째로 그래서 무효였다.
# 여기 형식은 inject_contract_cases.py의 build()와 반드시 같아야 한다.
for e in M K; do
  vid=$(echo "${PREFIX}-${e}02" | tr '[:lower:]' '[:upper:]')
  v=$(field_value "$vid" rpm)
  log "소수 RPM 보존 ($e): 저장값=$v (기대 2000.7)"
  evidence_count "rpm_preserved_${e}" "${v:-없음}"
done
for e in M K; do
  vid=$(echo "${PREFIX}-${e}03" | tr '[:lower:]' '[:upper:]')
  a=$(anomaly_count "$vid")
  log "이상 감지 ($e): ${vid} 알림 ${a:-0}건 (기대 1건 이상)"
  evidence_count "anomaly_alerts_${e}" "${a:-0}"
done

# ── 6. DLQ 귀속 ────────────────────────────────────────────────
DLQ_KAFKA_AFTER=$(topic_end vehicle-telemetry-dlq)
DLQ_MQTT_AFTER=$(topic_end vehicle-telemetry-mqtt-dlq)
log ""
log "DLQ 증가 — kafka-dlq=$(( DLQ_KAFKA_AFTER - DLQ_KAFKA_BEFORE )) mqtt-dlq=$(( DLQ_MQTT_AFTER - DLQ_MQTT_BEFORE ))"
evidence_count dlq_kafka_delta "$(( DLQ_KAFKA_AFTER - DLQ_KAFKA_BEFORE ))"
evidence_count dlq_mqtt_delta "$(( DLQ_MQTT_AFTER - DLQ_MQTT_BEFORE ))"
evidence_count e2e_pass "$PASS"
evidence_count e2e_fail "$FAIL"

log ""
log "=== 요약: PASS $PASS / FAIL $FAIL ==="
VERDICT=$([ "$FAIL" -eq 0 ] && echo "PASS" || echo "FAIL")
log "판정: $VERDICT"

evidence_capture_kafka_groups telemetry-storage-group
evidence_capture_topic_offsets vehicle-telemetry vehicle-telemetry-dlq vehicle-telemetry-mqtt-dlq
evidence_capture_log_lines telemetry-backend "PAYLOAD_VALIDATION_FAILED|UNKNOWN_FIELD|TYPE_MISMATCH|MALFORMED_JSON|계약" backend-reject-lines.txt
evidence_capture_file "$OUT" console.log
evidence_finish "두 입구가 같은 계약대로 저장·거부하고 DLQ 귀속이 분명하다" "$VERDICT"
log "DONE"
