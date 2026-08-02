#!/bin/bash
# 사용법: sample.sh <stage_label> <duration_seconds> <interval_seconds>
STAGE="$1"
DURATION="${2:-300}"
INTERVAL="${3:-30}"
OUTDIR="/private/tmp/claude-501/-Users-parkseong-won-IdeaProjects-vehicle-telemetry-platform/25c5d0c9-024f-4349-b174-0829576d65bf/scratchpad/loadtest2"
LOG="$OUTDIR/${STAGE}.log"
TOKEN="localtoken1234567890localtoken1234567890abcdefgh"
ORG="vehicle-telemetry"

echo "=== stage=$STAGE duration=${DURATION}s interval=${INTERVAL}s start=$(date -u +%FT%TZ) ===" > "$LOG"

START_ISO=$(date -u +%FT%TZ)
END_TS=$((SECONDS + DURATION))

while [ $SECONDS -lt $END_TS ]; do
  TS=$(date -u +%FT%TZ)
  echo "--- sample @ $TS ---" >> "$LOG"

  echo "[kafka lag]" >> "$LOG"
  docker exec telemetry-kafka kafka-consumer-groups --bootstrap-server localhost:9092 --describe --group telemetry-storage-group 2>/dev/null >> "$LOG"
  docker exec telemetry-kafka kafka-consumer-groups --bootstrap-server localhost:9092 --describe --group anomaly-detector-group 2>/dev/null >> "$LOG"

  echo "[influx count since $START_ISO]" >> "$LOG"
  docker exec telemetry-influxdb influx query "
from(bucket:\"telemetry\")
  |> range(start: $START_ISO)
  |> filter(fn: (r) => r._measurement == \"vehicle_telemetry\")
  |> filter(fn: (r) => r._field == \"speed\")
  |> count()
  |> group()
  |> sum()
" --org "$ORG" --token "$TOKEN" 2>/dev/null >> "$LOG"

  echo "[docker stats]" >> "$LOG"
  docker stats --no-stream --format "table {{.Name}}\t{{.CPUPerc}}\t{{.MemUsage}}" telemetry-backend telemetry-kafka telemetry-influxdb telemetry-anomaly-detector 2>/dev/null >> "$LOG"

  sleep "$INTERVAL"
done

echo "=== stage=$STAGE end=$(date -u +%FT%TZ) ===" >> "$LOG"
