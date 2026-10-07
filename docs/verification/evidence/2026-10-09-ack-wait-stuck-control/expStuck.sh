#!/bin/bash
# 대조군: delivery.timeout.ms=120000(저장소 밖 override)에서 Kafka 150초 pause, 끊기 조작 없음
cd /d/vehicle-telemetry-platform
E=docs/verification/evidence/2026-10-09-ack-wait-stuck-control
PY=/c/Users/USER/AppData/Local/Programs/Python/Python311/python.exe
V=STUCK-CTL
M=$E/C_marks.txt; : > $M; : > $E/C_gauge_poll.txt; : > $E/C_alerts_poll.txt
mk(){ echo "$1=$(date -u +%Y-%m-%dT%H:%M:%S.%3NZ)" >> $M; }
met(){ curl -s localhost:8080/actuator/prometheus | grep -E '^telemetry_(mqtt_messages|mqtt_ack|mqtt_decode|spool)'; }
met > $E/C_before_metrics.txt
bash $E/offsets.sh > $E/C_before_offsets.txt
$PY $E/genpaced.py $V 720 333 > $E/C_payloads.jsonl
bash $E/poll.sh $E 340 &
POLL=$!
mk poll_start
sleep 10
( while IFS= read -r l; do echo "$l"; sleep 0.333; done < $E/C_payloads.jsonl ) | MSYS_NO_PATHCONV=1 docker run --rm -i --name pubSTUCK --network vehicle-telemetry-platform_telemetry-net eclipse-mosquitto:2.0 mosquitto_pub -d -h mosquitto -p 1883 -q 1 -t vehicle/telemetry/$V -l > $E/C_pub.log 2>&1 &
PUB=$!
mk publish_start
sleep 30
mk pause_send; docker pause telemetry-kafka > /dev/null; mk pause_done
sleep 150
mk unpause_send; docker unpause telemetry-kafka > /dev/null; mk unpause_done
wait $PUB
mk publish_end
sleep 30
met > $E/C_after_metrics.txt
bash $E/offsets.sh > $E/C_after_offsets.txt
echo "sent=$(grep -ci 'sending PUBLISH' $E/C_pub.log) puback_rc0=$(grep -c 'received PUBACK.*RC:0' $E/C_pub.log)" >> $M
wait $POLL
mk end
