#!/bin/bash
# usage: sample_fast.sh <out> <seconds> — 15초 간격, prometheus 지표만(로그 집계는 끝에서 --since로)
out=$1; n=$(( $2 / 15 ))
echo "utc pending drained_total" > $out
for i in $(seq 0 $n); do
  s=$(date +%s); t=$(date -u +%H:%M:%SZ)
  m=$(curl -s --max-time 4 localhost:8080/actuator/prometheus)
  p=$(echo "$m" | grep '^telemetry_spool_pending' | awk '{print $2}')
  d=$(echo "$m" | grep '^telemetry_spool_drained_total' | awk '{print $2}')
  echo "$t ${p:-NA} ${d:-NA}" >> $out
  e=$(date +%s); sl=$((15-(e-s))); [ $sl -gt 0 ] && sleep $sl
done
