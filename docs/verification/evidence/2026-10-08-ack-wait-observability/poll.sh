#!/bin/bash
# usage: poll.sh OUTDIR SECONDS — 백엔드 게이지 2초 간격, Prometheus 알림 5초 간격(백그라운드용)
D=$1; N=$2; end=$(( $(date +%s) + N )); i=0
while [ $(date +%s) -lt $end ]; do
  ts=$(date -u +%Y-%m-%dT%H:%M:%S.%3NZ)
  g=$(curl -s -m 2 localhost:8080/actuator/prometheus | grep -E '^telemetry_mqtt_ack_wait_(in_progress|elapsed_seconds)[ {]' | tr '\n' ' ')
  echo "$ts ${g:-ABSENT}" >> $D/D3_gauge_poll.txt
  if [ $((i % 5)) -eq 0 ]; then
    a=$(curl -s -m 2 localhost:9090/api/v1/alerts | /c/Users/USER/AppData/Local/Programs/Python/Python311/python.exe -c "import json,sys; d=json.load(sys.stdin); print(' '.join(f\"{x['labels']['alertname']}:{x['state']}\" for x in d['data']['alerts']) or 'none')" 2>&1)
    echo "$ts $a" >> $D/D3_alerts_poll.txt
  fi
  i=$((i+2)); sleep 2
done
