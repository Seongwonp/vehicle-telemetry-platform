#!/bin/bash
# 실험 U: dynsec로 $SYS 전달만 막아 MqttBrokerMetricsStale 탐지 시간과 MqttIngestStopped 동작을 본다
cd /d/vehicle-telemetry-platform
E=docs/verification/evidence/2026-10-07-recheck-2aba182
PY=/c/Users/USER/AppData/Local/Programs/Python/Python311/python.exe
V=OUTAGE-U
M=$E/U_marks.txt; : > $M
mk(){ echo "$1=$(date -u +%Y-%m-%dT%H:%M:%S.%3NZ)" >> $M; }
q(){ curl -s localhost:9090/api/v1/query --data-urlencode "query=$1" | $PY -c "import sys,json;r=json.load(sys.stdin)['data']['result'];print(';'.join((x['metric'].get('alertname','')+'/'+x['metric'].get('alertstate','')+':'+x['value'][1]) if 'alertstate' in x['metric'] else x['value'][1] for x in r) or '-')" 2>/dev/null; }
P=$E/U_poll.txt; echo "utc | phase | ALERTS | sys_age_s | backend_rate2m | broker_rate2m | broker_recv_gauge | up" > $P
poll(){ echo "$(date -u +%H:%M:%S) | $1 | $(q 'ALERTS') | $(q 'time() - max(telemetry_mqtt_broker_last_update_seconds)') | $(q 'sum(rate(telemetry_mqtt_messages_received_total[2m]))') | $(q 'sum(rate(telemetry_mqtt_broker_messages_received[2m]))') | $(q 'max(telemetry_mqtt_broker_messages_received)') | $(q 'sum(up{job="telemetry-backend"})')" >> $P; }
seen(){ grep -q "^$1=" $M || mk $1; }
ctl(){ bash $E/ctl.sh "$1" > $E/U_$2_response.txt 2>&1; }
# U0: 발행 없이 게이지 나이
mk idle_start
for i in $(seq 1 28); do poll idle; sleep 15; done; curl -s localhost:9093/api/v2/alerts > $E/U0_alertmanager_idle.json
bash $E/offsets.sh > $E/U_before_offsets.txt
$PY $E/genpaced.py $V 4200 333 > $E/U_payloads.jsonl
( while IFS= read -r l; do echo "$l"; sleep 0.333; done < $E/U_payloads.jsonl ) | MSYS_NO_PATHCONV=1 docker run --rm -i --name pubU --network vehicle-telemetry-platform_telemetry-net eclipse-mosquitto:2.0 mosquitto_pub -d -h mosquitto -p 1883 -q 1 -t vehicle/telemetry/$V -l > $E/U_pub.log 2>&1 &
mk publish_start
for i in $(seq 1 9); do poll baseline; sleep 15; done
mk t0_send; ctl '{"command":"addRoleACL","rolename":"revocable","acltype":"publishClientReceive","topic":"$SYS/#","priority":0,"allow":false}' t0; mk t0_done
f=0
for i in $(seq 1 44); do
  poll sys_denied; a=$(tail -1 $P)
  case "$a" in *MqttBrokerMetricsStale/pending*) seen stale_pending_seen;; esac
  case "$a" in *MqttBrokerMetricsStale/firing*) if [ $f = 0 ]; then seen stale_firing_seen; f=$i; curl -s localhost:9093/api/v2/alerts > $E/U_alertmanager_stale.json; fi;; esac
  [ $f != 0 ] && [ $i -ge $((f+2)) ] && break
  sleep 15
done
mk t2_send; ctl '{"command":"addRoleACL","rolename":"revocable","acltype":"publishClientReceive","topic":"vehicle/telemetry/#","priority":0,"allow":false}' t2; mk t2_done
for i in $(seq 1 20); do poll sys_and_telemetry_denied; case "$(tail -1 $P)" in *MqttIngestStopped*) seen ingest_alert_seen_while_sys_denied;; esac; sleep 15; done
mk t3_send; ctl '{"command":"removeRoleACL","rolename":"revocable","acltype":"publishClientReceive","topic":"$SYS/#"}' t3; mk t3_done
f=0
for i in $(seq 1 20); do
  poll telemetry_denied; a=$(tail -1 $P)
  case "$a" in *MqttIngestStopped/pending*) seen ingest_pending_seen;; esac
  case "$a" in *MqttIngestStopped/firing*) if [ $f = 0 ]; then seen ingest_firing_seen; f=$i; curl -s localhost:9093/api/v2/alerts > $E/U_alertmanager_ingest.json; fi;; esac
  case "$a" in *MqttBrokerMetricsStale*) ;; *) seen stale_cleared_seen;; esac
  [ $f != 0 ] && [ $i -ge $((f+2)) ] && break
  sleep 15
done
mk t4_send; ctl '{"command":"removeRoleACL","rolename":"revocable","acltype":"publishClientReceive","topic":"vehicle/telemetry/#"}' t4; mk t4_done
c=0
for i in $(seq 1 20); do
  poll restored; a=$(tail -1 $P)
  case "$a" in *"| restored | - |"*) [ $c = 0 ] && { seen alerts_cleared_seen; c=$i; };; esac
  [ $c != 0 ] && [ $i -ge $((c+2)) ] && break
  sleep 15
done
docker stop pubU > /dev/null 2>&1; mk publisher_stopped
echo "sent=$(grep -ci 'sending PUBLISH' $E/U_pub.log) puback_rc0=$(grep -c 'received PUBACK.*RC:0' $E/U_pub.log)" >> $M
bash $E/offsets.sh > $E/U_after_offsets.txt
mk end
