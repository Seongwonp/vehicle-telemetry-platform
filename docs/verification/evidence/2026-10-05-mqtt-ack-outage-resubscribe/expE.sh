#!/bin/bash
# 실험 E: 720건 약 3/s, 시작 30s 뒤 kafka stop, 150s 뒤 start. 공급 속도는 sleep 0.333 (호스트 드리프트 있음)
cd /d/vehicle-telemetry-platform
E=docs/verification/evidence/2026-10-05-mqtt-ack-outage-resubscribe
PY=/c/Users/USER/AppData/Local/Programs/Python/Python311/python.exe
V=OUTAGE-E
$PY $E/genpaced.py $V 720 333 > $E/E_payloads.jsonl
M=$E/E_marks.txt; : > $M
mk(){ echo "$1=$(date -u +%Y-%m-%dT%H:%M:%S.%3NZ)" >> $M; }
( while IFS= read -r l; do echo "$l"; sleep 0.333; done < $E/E_payloads.jsonl ) | docker run --rm -i --network vehicle-telemetry-platform_telemetry-net eclipse-mosquitto:2.0 mosquitto_pub -d -h mosquitto -p 1883 -q 1 -t vehicle/telemetry/$V -l > $E/E_pub.log 2>&1 &
PP=$!
mk publish_start
sleep 30; mk kafka_stop_begin; docker stop telemetry-kafka >/dev/null; mk kafka_stop_done
sleep 150; mk kafka_start_begin; docker start telemetry-kafka >/dev/null; mk kafka_start_done
wait $PP; mk publish_end
echo "sent=$(grep -ci 'sending PUBLISH' $E/E_pub.log) puback_rc0=$(grep -c 'received PUBACK.*RC:0' $E/E_pub.log)" >> $M
