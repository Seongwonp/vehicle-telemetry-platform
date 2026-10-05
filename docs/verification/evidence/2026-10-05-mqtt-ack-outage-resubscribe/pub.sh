#!/bin/bash
# usage: pub.sh VID N OUTPREFIX  (payload file + publisher log)
cd /d/vehicle-telemetry-platform
V=$1; N=$2; P=$3
/c/Users/USER/AppData/Local/Programs/Python/Python311/python.exe "$(dirname "$0")/gen.py" $V $N > "$P.payloads.jsonl"
docker run --rm -i --network vehicle-telemetry-platform_telemetry-net eclipse-mosquitto:2.0 mosquitto_pub -d -h mosquitto -p 1883 -q 1 -t vehicle/telemetry/$V -l < "$P.payloads.jsonl" > "$P.pub.log" 2>&1
echo "exit=$? sent=$(grep -ci 'sending PUBLISH' $P.pub.log) puback_rc0=$(grep -c 'received PUBACK.*RC:0' $P.pub.log) puback_all=$(grep -c 'received PUBACK' $P.pub.log)"
