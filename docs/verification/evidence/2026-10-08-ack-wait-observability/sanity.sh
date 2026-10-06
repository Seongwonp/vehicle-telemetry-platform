#!/bin/bash
# 원복 뒤 5건 확인: 발행 PUBACK, InfluxDB 행 수
cd /d/vehicle-telemetry-platform
E=docs/verification/evidence/2026-10-08-ack-wait-observability
PY=/c/Users/USER/AppData/Local/Programs/Python/Python311/python.exe
$PY $E/genpaced.py RECON-D3Z 5 200 > $E/Z_restore_payloads.jsonl
MSYS_NO_PATHCONV=1 docker run --rm -i --network vehicle-telemetry-platform_telemetry-net eclipse-mosquitto:2.0 mosquitto_pub -d -h mosquitto -p 1883 -q 1 -t vehicle/telemetry/RECON-D3Z -l < $E/Z_restore_payloads.jsonl > $E/Z_restore_pub.log 2>&1
sleep 8
echo "puback_rc0=$(grep -c 'received PUBACK.*RC:0' $E/Z_restore_pub.log) influx=$(bash $E/influx_count.sh RECON-D3Z | awk -F, '{print $NF}') gauge=$(curl -s localhost:8080/actuator/prometheus | grep -E '^telemetry_mqtt_ack_wait_(in_progress|seconds_count)' | tr '\n' ' ')" > $E/Z_restore_result.txt
cat $E/Z_restore_result.txt
