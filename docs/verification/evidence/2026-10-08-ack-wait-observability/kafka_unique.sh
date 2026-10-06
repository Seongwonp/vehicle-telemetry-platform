#!/bin/bash
# usage: kafka_unique.sh TAG VID PARTITION START_OFFSET -> 레코드 저장 + 고유/중복 수
T=$1; V=$2; P=$3; O=$4
E=docs/verification/evidence/2026-10-08-ack-wait-observability
docker exec telemetry-kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic vehicle-telemetry --partition $P --offset $O --timeout-ms 15000 2>/dev/null | grep "\"$V\"" > $E/${T}_kafka_records.txt
echo "records=$(wc -l < $E/${T}_kafka_records.txt) unique_ts=$(grep -o '"timestamp":"[^"]*"' $E/${T}_kafka_records.txt | sort -u | wc -l) dup_ts=$(grep -o '"timestamp":"[^"]*"' $E/${T}_kafka_records.txt | sort | uniq -d | wc -l)"
