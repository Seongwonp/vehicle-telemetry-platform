#!/bin/bash
# 실험 H2: dynsec 구독 거부(SUBACK 0x80) 상태에서 백엔드 재기동 + 계속 발행 + ALERTS 폴링
cd /d/vehicle-telemetry-platform
E=docs/verification/evidence/2026-10-06-timeout-alert
S=C:/Users/USER/AppData/Local/Temp/claude/D--vehicle-telemetry-platform/d456ac25-a739-4b31-8e65-c5c2cb0ea080/scratchpad
PY=/c/Users/USER/AppData/Local/Programs/Python/Python311/python.exe
V=OUTAGE-H2
M=$E/H2_marks.txt; : > $M
mk(){ echo "$1=$(date -u +%Y-%m-%dT%H:%M:%S.%3NZ)" >> $M; }
q(){ curl -s localhost:9090/api/v1/query --data-urlencode "query=$1" | $PY -c "import sys,json;r=json.load(sys.stdin)['data']['result'];print(';'.join((x['metric'].get('alertstate','')+':'+x['value'][1]) for x in r) or '-')" 2>/dev/null; }
mk begin
docker compose -f docker-compose.yml -f docker-compose.dev.yml -f $S/debug-override.yml -f $S/h/override-h2.yml up -d --force-recreate --no-deps backend > /dev/null 2>&1
mk backend_recreate_done
$PY $E/genpaced.py $V 3000 333 > $E/H2_payloads.jsonl
( while IFS= read -r l; do echo "$l"; sleep 0.333; done < $E/H2_payloads.jsonl ) | docker run --rm -i --network vehicle-telemetry-platform_telemetry-net eclipse-mosquitto:2.0 mosquitto_pub -d -h mosquitto -p 1883 -q 1 -t vehicle/telemetry/$V -l > $E/H2_pub.log 2>&1 &
mk publish_start
P=$E/H2_alert_poll.txt; echo "utc | ALERTS | backend_rate2m | broker_rate2m | backend_recv_total" > $P
fired=0
for i in $(seq 1 64); do
  a=$(q 'ALERTS{alertname="MqttIngestStopped"}'); b=$(q 'sum(rate(telemetry_mqtt_messages_received_total[2m]))'); r=$(q 'sum(rate(telemetry_mqtt_broker_messages_received[2m]))'); t=$(q 'sum(telemetry_mqtt_messages_received_total)')
  echo "$(date -u +%H:%M:%S) | $a | $b | $r | $t" >> $P
  case "$a" in *pending*) grep -q first_pending $M || mk first_pending_seen;; esac
  case "$a" in *firing*) [ $fired = 0 ] && mk first_firing_seen && fired=$i;; esac
  [ $fired != 0 ] && [ $i -ge $((fired+4)) ] && break
  sleep 15
done
mk poll_end
