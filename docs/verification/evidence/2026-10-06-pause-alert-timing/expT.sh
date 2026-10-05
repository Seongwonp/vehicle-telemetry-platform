#!/bin/bash
# 실험 T: dynsec 런타임 회수로 백엔드 수신만 멈추고 MqttIngestStopped 탐지 시간 측정
cd /d/vehicle-telemetry-platform
E=docs/verification/evidence/2026-10-06-pause-alert-timing
S=C:/Users/USER/AppData/Local/Temp/claude/D--vehicle-telemetry-platform/d456ac25-a739-4b31-8e65-c5c2cb0ea080/scratchpad
PY=/c/Users/USER/AppData/Local/Programs/Python/Python311/python.exe
V=OUTAGE-T
M=$E/T_marks.txt; : > $M
mk(){ echo "$1=$(date -u +%Y-%m-%dT%H:%M:%S.%3NZ)" >> $M; }
q(){ curl -s localhost:9090/api/v1/query --data-urlencode "query=$1" | $PY -c "import sys,json;r=json.load(sys.stdin)['data']['result'];print(';'.join((x['metric'].get('alertname','')+'/'+x['metric'].get('alertstate','')+':'+x['value'][1]) if 'alertstate' in x['metric'] else x['value'][1] for x in r) or '-')" 2>/dev/null; }
P=$E/T_poll.txt; echo "utc | phase | ALERTS | backend_rate2m | broker_rate2m | backend_recv_total | up" > $P
poll(){ echo "$(date -u +%H:%M:%S) | $1 | $(q 'ALERTS') | $(q 'sum(rate(telemetry_mqtt_messages_received_total[2m]))') | $(q 'sum(rate(telemetry_mqtt_broker_messages_received[2m]))') | $(q 'sum(telemetry_mqtt_messages_received_total)') | $(q 'sum(up{job="telemetry-backend"})')" >> $P; }
mk begin
docker compose -f docker-compose.yml -f docker-compose.dev.yml -f $S/debug-override.yml -f $S/h/override-t.yml up -d --force-recreate --no-deps backend > /dev/null 2>&1
for i in $(seq 1 40); do curl -sf localhost:8080/actuator/health >/dev/null && break; sleep 3; done
mk backend_ready
bash $E/offsets.sh > $E/T_before_offsets.txt
$PY $E/genpaced.py $V 3600 333 > $E/T_payloads.jsonl
( while IFS= read -r l; do echo "$l"; sleep 0.333; done < $E/T_payloads.jsonl ) | MSYS_NO_PATHCONV=1 docker run --rm -i --name pubT --network vehicle-telemetry-platform_telemetry-net eclipse-mosquitto:2.0 mosquitto_pub -d -h mosquitto -p 1883 -q 1 -t vehicle/telemetry/$V -l > $E/T_pub.log 2>&1 &
mk publish_start
for i in $(seq 1 13); do poll baseline; sleep 15; done
mk revoke_send
bash $E/ctl.sh '{"command":"addRoleACL","rolename":"revocable","acltype":"publishClientReceive","topic":"vehicle/telemetry/#","priority":0,"allow":false}' > $E/T_revoke_response.txt 2>&1
mk revoke_done
fired=0
for i in $(seq 1 44); do
  poll revoked; a=$(tail -1 $P)
  case "$a" in *MqttIngestStopped/pending*) grep -q first_pending_seen $M || mk first_pending_seen;; esac
  case "$a" in *MqttIngestStopped/firing*) if [ $fired = 0 ]; then mk first_firing_seen; fired=$i; curl -s localhost:9093/api/v2/alerts > $E/T_alertmanager_alerts.json; fi;; esac
  [ $fired != 0 ] && [ $i -ge $((fired+4)) ] && break
  sleep 15
done
mk restore_send
bash $E/ctl.sh '{"command":"removeRoleACL","rolename":"revocable","acltype":"publishClientReceive","topic":"vehicle/telemetry/#"}' > $E/T_restore_response.txt 2>&1
mk restore_done
for i in $(seq 1 20); do
  poll restored; a=$(tail -1 $P)
  case "$a" in *"| - |"*) grep -q alert_cleared_seen $M || { mk alert_cleared_seen; cl=$i; };; esac
  [ -n "$cl" ] && [ $i -ge $((cl+2)) ] && break
  sleep 15
done
mk poll_end
docker stop pubT >/dev/null 2>&1; mk publisher_stopped
echo "sent=$(grep -ci 'sending PUBLISH' $E/T_pub.log) puback_rc0=$(grep -c 'received PUBACK.*RC:0' $E/T_pub.log)" >> $M
