#!/bin/bash
# 실험 D2: Kafka pause로 저장 확인 대기를 만들고 dynsec role 변경(admin 끊김)으로 MQTT 연결을 끊는다
cd /d/vehicle-telemetry-platform
E=docs/verification/evidence/2026-10-07-recheck-2aba182
PY=/c/Users/USER/AppData/Local/Programs/Python/Python311/python.exe
V=RECON-D2
M=$E/D2_marks.txt; : > $M
mk(){ echo "$1=$(date -u +%Y-%m-%dT%H:%M:%S.%3NZ)" >> $M; }
met(){ curl -s localhost:8080/actuator/prometheus | grep -E '^telemetry_(mqtt_messages|mqtt_ack|mqtt_decode|spool)'; }
met > $E/D2_before_metrics.txt
bash $E/offsets.sh > $E/D2_before_offsets.txt
$PY $E/genpaced.py $V 360 333 > $E/D2_payloads.jsonl
( while IFS= read -r l; do echo "$l"; sleep 0.333; done < $E/D2_payloads.jsonl ) | MSYS_NO_PATHCONV=1 docker run --rm -i --name pubD2 --network vehicle-telemetry-platform_telemetry-net eclipse-mosquitto:2.0 mosquitto_pub -d -h mosquitto -p 1883 -q 1 -t vehicle/telemetry/$V -l > $E/D2_pub.log 2>&1 &
PUB=$!
mk publish_start
sleep 30
mk pause_send; docker pause telemetry-kafka > /dev/null; mk pause_done
sleep 8
mk kick1_send
bash $E/ctl.sh '{"command":"addRoleACL","rolename":"revocable","acltype":"publishClientReceive","topic":"d2/noop1","priority":0,"allow":true}' > $E/D2_kick1_response.txt 2>&1
mk kick1_done
sleep 5
mk unpause_send; docker unpause telemetry-kafka > /dev/null; mk unpause_done
sleep 40
mk kick2_send
bash $E/ctl.sh '{"command":"addRoleACL","rolename":"revocable","acltype":"publishClientReceive","topic":"d2/noop2","priority":0,"allow":true}' > $E/D2_kick2_response.txt 2>&1
mk kick2_done
wait $PUB
mk publish_end
sleep 20
met > $E/D2_after_metrics.txt
bash $E/offsets.sh > $E/D2_after_offsets.txt
echo "sent=$(grep -ci 'sending PUBLISH' $E/D2_pub.log) puback_rc0=$(grep -c 'received PUBACK.*RC:0' $E/D2_pub.log)" >> $M
mk end
