#!/bin/bash
# usage: expE3.sh TAG VID STOP_SECS   (E3: STOP_SECS=150, S: STOP_SECS=20)  720건 약 3/s, 시작 30s 뒤 kafka stop
cd /d/vehicle-telemetry-platform
E=docs/verification/evidence/2026-10-06-timeout-alert
PY=/c/Users/USER/AppData/Local/Programs/Python/Python311/python.exe
T=$1; V=$2; D=$3
M=$E/${T}_marks.txt; : > $M
mk(){ echo "$1=$(date -u +%Y-%m-%dT%H:%M:%S.%3NZ)" >> $M; }
mk begin
bash $E/offsets.sh > $E/${T}_before_offsets.txt
curl -s localhost:8080/actuator/prometheus | grep -E '^telemetry_(mqtt_messages_received|mqtt_messages_invalid|spool_drained|spool_pending|mqtt_ack)' > $E/${T}_before_metrics.txt
$PY $E/genpaced.py $V 720 333 > $E/${T}_payloads.jsonl
( while IFS= read -r l; do echo "$l"; sleep 0.333; done < $E/${T}_payloads.jsonl ) | docker run --rm -i --network vehicle-telemetry-platform_telemetry-net eclipse-mosquitto:2.0 mosquitto_pub -d -h mosquitto -p 1883 -q 1 -t vehicle/telemetry/$V -l > $E/${T}_pub.log 2>&1 &
PP=$!
mk publish_start
sleep 30; mk kafka_stop_begin; docker stop telemetry-kafka >/dev/null; mk kafka_stop_done
sleep $D; mk kafka_start_begin; docker start telemetry-kafka >/dev/null; mk kafka_start_done
for i in $(seq 1 60); do s=$(docker inspect -f '{{.State.Health.Status}}' telemetry-kafka); [ "$s" = healthy ] && break; sleep 2; done; mk kafka_healthy
wait $PP; mk publish_end
echo "sent=$(grep -ci 'sending PUBLISH' $E/${T}_pub.log) puback_rc0=$(grep -c 'received PUBACK.*RC:0' $E/${T}_pub.log)" >> $M
