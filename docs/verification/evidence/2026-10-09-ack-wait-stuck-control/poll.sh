#!/bin/bash
# usage: poll.sh OUTDIR SECONDS — 백엔드 게이지 2초 간격, Prometheus 알림 5초 간격(백그라운드용). D3 poll.sh와 같고 파일 접두만 C_
D=$1; N=$2; end=$(( $(date +%s) + N )); i=0
PY=/c/Users/USER/AppData/Local/Programs/Python/Python311/python.exe
while [ $(date +%s) -lt $end ]; do
  ts=$(date -u +%Y-%m-%dT%H:%M:%S.%3NZ)
  g=$(curl -s -m 2 localhost:8080/actuator/prometheus | grep -E '^telemetry_mqtt_ack_wait_(in_progress|elapsed_seconds)[ {]' | tr '\n' ' ')
  echo "$ts ${g:-ABSENT}" >> $D/C_gauge_poll.txt
  if [ $((i % 5)) -eq 0 ]; then
    a=$(curl -s -m 2 localhost:9090/api/v1/alerts | $PY -c "import json,sys; d=json.load(sys.stdin); print(' '.join(f\"{x['labels']['alertname']}:{x['state']}:{x.get('activeAt','')}\" for x in d['data']['alerts']) or 'none')" 2>&1)
    echo "$ts $a" >> $D/C_alerts_poll.txt
  fi
  i=$((i+2)); sleep 2
done
