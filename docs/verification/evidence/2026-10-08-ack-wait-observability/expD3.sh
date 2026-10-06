#!/bin/bash
# 실험 D3: Kafka 75초 pause(> delivery.timeout 30초) 중 dynsec role 변경(publishClientSend)으로 MQTT 연결을 끊는다
cd /d/vehicle-telemetry-platform
E=docs/verification/evidence/2026-10-08-ack-wait-observability
PY=/c/Users/USER/AppData/Local/Programs/Python/Python311/python.exe
V=RECON-D3
M=$E/D3_marks.txt; : > $M; : > $E/D3_gauge_poll.txt; : > $E/D3_alerts_poll.txt
mk(){ echo "$1=$(date -u +%Y-%m-%dT%H:%M:%S.%3NZ)" >> $M; }
met(){ curl -s localhost:8080/actuator/prometheus | grep -E '^telemetry_(mqtt_messages|mqtt_ack|mqtt_decode|spool)'; }
met > $E/D3_before_metrics.txt
bash $E/offsets.sh > $E/D3_before_offsets.txt
$PY $E/genpaced.py $V 540 333 > $E/D3_payloads.jsonl
bash $E/poll.sh $E 330 &
POLL=$!
mk poll_start
sleep 10
( while IFS= read -r l; do echo "$l"; sleep 0.333; done < $E/D3_payloads.jsonl ) | MSYS_NO_PATHCONV=1 docker run --rm -i --name pubD3 --network vehicle-telemetry-platform_telemetry-net eclipse-mosquitto:2.0 mosquitto_pub -d -h mosquitto -p 1883 -q 1 -t vehicle/telemetry/$V -l > $E/D3_pub.log 2>&1 &
PUB=$!
mk publish_start
sleep 30
mk pause_send; docker pause telemetry-kafka > /dev/null; mk pause_done
sleep 15
mk kick1_send
bash $E/ctl.sh '{"command":"addRoleACL","rolename":"revocable","acltype":"publishClientSend","topic":"d3/noop1","priority":0,"allow":true}' > $E/D3_kick1_response.txt 2>&1
mk kick1_done
sleep 52
mk unpause_send; docker unpause telemetry-kafka > /dev/null; mk unpause_done
sleep 30
mk kick2_send
bash $E/ctl.sh '{"command":"addRoleACL","rolename":"revocable","acltype":"publishClientSend","topic":"d3/noop2","priority":0,"allow":true}' > $E/D3_kick2_response.txt 2>&1
mk kick2_done
wait $PUB
mk publish_end
sleep 30
met > $E/D3_after_metrics.txt
bash $E/offsets.sh > $E/D3_after_offsets.txt
echo "sent=$(grep -ci 'sending PUBLISH' $E/D3_pub.log) puback_rc0=$(grep -c 'received PUBACK.*RC:0' $E/D3_pub.log)" >> $M
wait $POLL
mk end
