#!/bin/bash
# 실험 D2b(큐 포화·unpause 지연·스레드 덤프), D2c(큐 비포화)
# usage: expD2bc.sh b|c
cd /d/vehicle-telemetry-platform
E=docs/verification/evidence/2026-10-07-recheck-2aba182
PY=/c/Users/USER/AppData/Local/Programs/Python/Python311/python.exe
X=$1; T=D2$X; V=RECON-D2$(echo $X | tr a-z A-Z)
M=$E/${T}_marks.txt; : > $M
mk(){ echo "$1=$(date -u +%Y-%m-%dT%H:%M:%S.%3NZ)" >> $M; }
met(){ curl -s localhost:8080/actuator/prometheus | grep -E '^telemetry_(mqtt_messages|mqtt_ack|mqtt_decode|spool_(pending|drained|stored))'; }
pubrun(){ ( while IFS= read -r l; do echo "$l"; sleep 0.333; done < $1 ) | MSYS_NO_PATHCONV=1 docker run --rm -i --name pub$T$3 --network vehicle-telemetry-platform_telemetry-net eclipse-mosquitto:2.0 mosquitto_pub -d -h mosquitto -p 1883 -q 1 -t vehicle/telemetry/$V -l > $2 2>&1; }
kick(){ bash $E/ctl.sh "{\"command\":\"addRoleACL\",\"rolename\":\"revocable\",\"acltype\":\"publishClientReceive\",\"topic\":\"$1\",\"priority\":0,\"allow\":true}" > $E/${T}_kick_response.txt 2>&1; }
met > $E/${T}_before_metrics.txt
bash $E/offsets.sh > $E/${T}_before_offsets.txt
mk start
if [ "$X" = b ]; then
  $PY $E/genpaced.py $V 240 333 > $E/${T}_payloads.jsonl
  pubrun $E/${T}_payloads.jsonl $E/${T}_pub.log 1 &
  PUB=$!
  mk publish_start; sleep 30
  mk pause_send; docker pause telemetry-kafka > /dev/null; mk pause_done
  sleep 8
  mk kick_send; kick d2b/noop & K=$!
  sleep 4; mk threaddump_send; docker kill --signal=QUIT telemetry-backend > /dev/null; mk threaddump_done
  wait $K; mk kick_done
  sleep 8
  mk unpause_send; docker unpause telemetry-kafka > /dev/null; mk unpause_done
  wait $PUB; mk publish_end
else
  $PY $E/genpaced.py $V 63 333 > $E/${T}_payloads.jsonl
  head -60 $E/${T}_payloads.jsonl > $E/${T}_payloads_a.jsonl
  tail -3 $E/${T}_payloads.jsonl > $E/${T}_payloads_b.jsonl
  mk publish_a_start; pubrun $E/${T}_payloads_a.jsonl $E/${T}_pub_a.log a; mk publish_a_end
  sleep 3
  mk pause_send; docker pause telemetry-kafka > /dev/null; mk pause_done
  mk publish_b_start; pubrun $E/${T}_payloads_b.jsonl $E/${T}_pub_b.log b; mk publish_b_end
  sleep 5; mk threaddump_send; docker kill --signal=QUIT telemetry-backend > /dev/null; mk threaddump_done
  sleep 1
  mk kick_send; kick d2c/noop; mk kick_done
  sleep 5
  mk unpause_send; docker unpause telemetry-kafka > /dev/null; mk unpause_done
  cat $E/${T}_pub_a.log $E/${T}_pub_b.log > $E/${T}_pub.log
fi
sleep 20
met > $E/${T}_after_metrics.txt
bash $E/offsets.sh > $E/${T}_after_offsets.txt
echo "sent=$(grep -ci 'sending PUBLISH' $E/${T}_pub.log) puback_rc0=$(grep -c 'received PUBACK.*RC:0' $E/${T}_pub.log)" >> $M
mk end
