#!/bin/bash
for t in vehicle-telemetry vehicle-telemetry-dlq vehicle-telemetry-mqtt-dlq; do
  echo -n "$t "; docker exec telemetry-kafka kafka-run-class kafka.tools.GetOffsetShell --broker-list localhost:9092 --topic $t --time -1 2>/dev/null | awk -F: '{s+=$3; printf "p%s=%s ", $2,$3} END{print "SUM="s}'
done
