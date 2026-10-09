#!/bin/bash
# 동글 정차 수집 — 집 리허설 1(정상, 에뮬레이터 60초) + 2(v5 음성 ACL, 20초).
# 비밀값은 출력하지 않는다: obd-bridge/.env는 run_bridge.py 안에서만 읽는다.
set -uo pipefail
REPO="/c/Users/삼성/Desktop/Dev/Telemetrix/vehicle-telemetry-platform"
SCR="/c/Users/삼성/AppData/Local/Temp/claude/C--Users----Desktop-Dev-Telemetrix/401906c5-6872-4df2-a3dd-79c3197fef2a/scratchpad"
cd "$REPO"
export PYTHONUTF8=1
PY="$REPO/obd-bridge/.venv/Scripts/python.exe"
R1_SECS=${R1_SECS:-60}; R2_SECS=${R2_SECS:-20}
source load-test/lib/evidence.sh
evidence_init dongle-stationary "bash rehearsal.sh (R1=${R1_SECS}s KONA17-01 v5, R2=${R2_SECS}s GRDR24H-01 cert -> KONA17-01 topic v5)"
UTC_ID="$(date -u +%Y%m%dT%H%M%SZ)-rehearsal"
NEW="load-test/dongle-stationary/evidence/$UTC_ID"
mv "$EVIDENCE_DIR" "$NEW"; EVIDENCE_DIR="$NEW"
sed -i "s/^run_id .*/run_id           : $UTC_ID/" "$EVIDENCE_DIR/metadata.txt"
E="$REPO/$EVIDENCE_DIR"; EW="$(cygpath -m "$E")"
cp "$SCR/rehearsal.sh" "$SCR/run_bridge.py" "$E/"
echo "EVIDENCE=$EVIDENCE_DIR"

CERTS="$REPO/broker/certs"
cert_info() { openssl x509 -in "$1" -noout -subject -serial -enddate 2>&1; }

# ---------- 00_metadata ----------
{
  echo "# 동글 정차 수집 리허설 (ELM327-emulator — 실차 아님)"
  echo "utc_started        : $(date -u +%FT%TZ)"
  echo "host               : $(hostname)"
  echo "git_head           : $(git rev-parse HEAD)"
  echo "git_describe       : $(git describe --tags --always --dirty 2>&1)"
  echo "## git status --porcelain (파일별 sha256는 provenance_source.txt)"
  git status --porcelain | grep -v '^?? load-test/dongle-stationary/'
  echo "## 컨테이너 image ID (containers_start.csv 전체)"
  docker ps --format '{{.Names}}' | grep -E '^telemetry-|^vehicle-telemetry-platform-' | sort | while read -r n; do
    echo "$n $(docker inspect -f '{{.Config.Image}} {{.Image}}' "$n")"; done
  echo "simulator_running  : $(docker ps --format '{{.Names}}' | grep -c simulator)"
  echo "## 브리지 설정 (값 대신 이름·CN — 키 내용 없음)"
  echo "VEHICLE_ID         : KONA17-01 (R1 .env, R2 CLI --vehicle-id)"
  echo "OBD_PORT           : socket://127.0.0.1:35000 (ELM327-emulator -s car), baud 38400"
  echo "MQTT               : host mosquitto (hosts 127.0.0.1), port 8883, protocol 5 (.env MQTT_PROTOCOL=5, R2는 CLI로도 5)"
  echo "POLL_INTERVAL      : 1.0"
  echo "SPOOL_DIR          : R1 spool-r1/, R2 spool-neg/ (이 폴더 안, 회차마다 새 폴더)"
  echo "cert_R1            : $(cert_info "$CERTS/vehicles/KONA17-01.crt" | tr '\n' ' ')"
  echo "cert_R2            : $(cert_info "$CERTS/vehicles/GRDR24H-01.crt" | tr '\n' ' ')"
  echo "## 노트북"
  "$PY" -c "import platform,sys,ssl;print('platform           :',platform.platform());print('python             :',sys.version.split()[0]);print('openssl            :',ssl.OPENSSL_VERSION)"
  echo "pip                : $("$PY" -m pip freeze 2>/dev/null | grep -iE '^(obd|paho-mqtt|pyserial|ELM327-emulator)==' | tr '\n' ' ')"
  echo "mosquitto          : $(docker exec telemetry-mosquitto mosquitto -h 2>&1 | head -1)"
  echo "## 전원(Win32_Battery: BatteryStatus 2=AC 연결)"
  powershell -NoProfile -File "$(cygpath -w "$SCR/battery.ps1")" 2>&1 | tr -d '\r'
  echo "## hosts 해석"
  powershell -NoProfile -Command "Resolve-DnsName mosquitto -Type A | ForEach-Object { \$_.Name + ' ' + \$_.IPAddress }" 2>&1 | tr -d '\r'
  echo "## w32tm /query /status"
  w32tm //query //status 2>&1 | tr -d '\r'
  echo "## netsh 제외 포트 범위"
  netsh int ipv4 show excludedportrange protocol=tcp 2>&1 | tr -d '\r' | sed '/^\s*$/d'
} > "$E/00_metadata.txt" 2>&1

offsets() {  # $1 = 파일
  for t in vehicle-telemetry vehicle-telemetry-mqtt-dlq vehicle-telemetry-dlq; do
    echo "# $t @ $(date -u +%FT%T.%3NZ)"
    docker exec telemetry-kafka kafka-run-class kafka.tools.GetOffsetShell --broker-list localhost:9092 --topic "$t" --time -1 2>/dev/null | tr -d '\r'
  done > "$1"
}
parts() { awk '/^# /{t=$2} t=="vehicle-telemetry" && /^vehicle-telemetry:/{split($0,a,":");print a[2]":"a[3]}' "$1" | sort; }
sum_vt() { parts "$1" | awk -F: '{s+=$2} END{print s+0}'; }
influx_count() {  # $1=field $2=start RFC3339
  docker exec -e V=KONA17-01 -e FLD="$1" -e ST="$2" telemetry-influxdb sh -c 'influx query --org "$DOCKER_INFLUXDB_INIT_ORG" --token "$DOCKER_INFLUXDB_INIT_ADMIN_TOKEN" --raw "from(bucket: \"$DOCKER_INFLUXDB_INIT_BUCKET\") |> range(start: $ST) |> filter(fn: (r) => r._measurement == \"vehicle_telemetry\" and r.vehicle_id == \"$V\" and r._field == \"$FLD\") |> group() |> count()"' 2>&1 | grep -v '^#' | grep -v '^,result' | grep -v '^$' | tr -d '\r'
}
start_emu() {  # $1 = tag
  ( cd "$SCR" && "$PY" -m elm -n 35000 -s car -b "$SCR/elm-batch-$1.out" > "$SCR/elm-console-$1.txt" 2>&1 ) &
  EMU_SH=$!
  for i in $(seq 1 30); do (echo > /dev/tcp/127.0.0.1/35000) 2>/dev/null && break; sleep 1; done
  sleep 1
}
stop_emu() {
  for wp in $(powershell -NoProfile -File "$(cygpath -w "$SCR/emu_pids.ps1")" | tr -d '\r'); do
    taskkill //PID "$wp" //T //F >/dev/null 2>&1; done
  wait "$EMU_SH" 2>/dev/null
}
run_bridge() {  # $1 = tag $2 = secs, 나머지 = 브리지 인자
  local tag="$1" secs="$2"; shift 2
  ( cd "$REPO/obd-bridge" && "$PY" "$SCR/run_bridge.py" "$REPO/obd-bridge/.env" "$secs" "$EW/${tag}_bridge_stats.json" -- "$@" ) > "$E/${tag}_bridge_console.txt" 2>&1
  echo "$tag bridge exit=$?"
}
logs_since() {  # $1 = tag $2 = since
  docker logs --since "$2" telemetry-mosquitto 2>&1 | grep -E "obd-bridge-|KONA17-01|GRDR24H-01|Denied|denied" | tr -d '\r' > "$E/${1}_mosquitto_log.txt"
  { echo "# backend 로그 중 KONA17-01/GRDR24H-01/거부·DLQ 관련 줄 (since $2)"
    docker logs --since "$2" telemetry-backend 2>&1 | grep -E "KONA17-01|GRDR24H-01|MISMATCH|TelemetryContract|DLQ|dlq" | tr -d '\r' | tail -200; } > "$E/${1}_backend_log.txt"
}
kafka_records() {  # $1 = tag $2 = before file $3 = after file
  : > "$E/${1}_kafka_records.txt"
  join -t: -j1 <(parts "$2") <(parts "$3") | while IFS=: read -r p b a; do
    [ "$a" -gt "$b" ] || continue
    docker exec telemetry-kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic vehicle-telemetry \
      --partition "$p" --offset "$b" --max-messages $((a-b)) --timeout-ms 15000 2>/dev/null | tr -d '\r' >> "$E/${1}_kafka_records.txt"
  done
}

# ================= 리허설 1 =================
R1_T0=$(date -u +%FT%TZ); R1_RANGE=$(date -u -d "-60 sec" +%FT%TZ)
offsets "$E/r1_offsets_before.txt"
for f in speed fuel_level battery_voltage; do echo "$f before(range>=$R1_RANGE): $(influx_count $f "$R1_RANGE")"; done > "$E/r1_influx_before.txt"
start_emu r1
run_bridge r1 "$R1_SECS" --obd-port socket://127.0.0.1:35000 --obd-baudrate 38400 --spool-dir "$EW/spool-r1"
stop_emu
sleep 10
offsets "$E/r1_offsets_after.txt"
kafka_records r1 "$E/r1_offsets_before.txt" "$E/r1_offsets_after.txt"
{ echo "# range(start: $R1_RANGE) — run $R1_T0 ~ $(date -u +%FT%TZ), vehicle_id=KONA17-01, count() = 고유 _time 수"
  for f in speed fuel_level battery_voltage; do echo "$f: $(influx_count $f "$R1_RANGE")"; done; } > "$E/r1_influx_count.txt"
logs_since r1 "$R1_T0"
R1_END=$(date -u +%FT%TZ)

# ================= 리허설 2 (v5 음성 ACL) =================
sleep 3
R2_T0=$(date -u +%FT%TZ)
offsets "$E/r2_offsets_before.txt"
start_emu r2
run_bridge r2 "$R2_SECS" --obd-port socket://127.0.0.1:35000 --obd-baudrate 38400 \
  --vehicle-id KONA17-01 --mqtt-protocol 5 \
  --tls-client-cert "$(cygpath -m "$CERTS/vehicles/GRDR24H-01.crt")" --tls-client-key "$(cygpath -m "$CERTS/vehicles/GRDR24H-01.key")" \
  --spool-dir "$EW/spool-neg"
stop_emu
sleep 10
offsets "$E/r2_offsets_after.txt"
kafka_records r2 "$E/r2_offsets_before.txt" "$E/r2_offsets_after.txt"
{ echo "# range(start: $R2_T0) — R2 구간에 KONA17-01로 저장된 행(기대 0)"
  echo "speed: $(influx_count speed "$R2_T0")"; } > "$E/r2_influx_count.txt"
logs_since r2 "$R2_T0"

# ---------- spool 집계(원본은 spool-*/ — *.log라 Git 제외) ----------
for s in r1:spool-r1 r2:spool-neg; do
  tag=${s%%:*}; d="$E/${s#*:}"
  { echo "# ${s#*:}/"
    echo "records_lines=$(wc -l < "$d/records.log" 2>/dev/null || echo NA)"
    echo "records_unique_ts=$(grep -oE '"ts": ?"[^"]*"' "$d/records.log" 2>/dev/null | sort -u | wc -l)"
    echo "acks_lines=$(cat "$d/acks.log" 2>/dev/null | wc -l)"
    echo "gps_count=$(grep -c '"gps"' "$d/records.log" 2>/dev/null || true)"
    echo "sha256:"; ( cd "$d" && sha256sum * 2>/dev/null ); } > "$E/${tag}_spool_summary.txt"
done
{ echo "r1_vt_before=$(sum_vt "$E/r1_offsets_before.txt") r1_vt_after=$(sum_vt "$E/r1_offsets_after.txt")"
  echo "r2_vt_before=$(sum_vt "$E/r2_offsets_before.txt") r2_vt_after=$(sum_vt "$E/r2_offsets_after.txt")"
  echo "r1_kafka_records=$(grep -c KONA17-01 "$E/r1_kafka_records.txt") r1_kafka_unique_ts=$(grep -o '"timestamp":"[^"]*"' "$E/r1_kafka_records.txt" | sort -u | wc -l) r1_kafka_dup_ts=$(grep -o '"timestamp":"[^"]*"' "$E/r1_kafka_records.txt" | sort | uniq -d | wc -l)"
  echo "r2_kafka_records=$(wc -l < "$E/r2_kafka_records.txt")"
  echo "R1 $R1_T0 ~ $R1_END / R2 $R2_T0 ~ $(date -u +%FT%TZ)"
} > "$E/summary_raw.txt"
cat "$E/summary_raw.txt"
echo "EVIDENCE_DIR=$EVIDENCE_DIR" > "$SCR/last_evidence.txt"
