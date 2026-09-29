#!/bin/bash
# usage: sample.sh <out> <seconds>  — 15초 간격으로 pending과 누적 로그 줄 수를 기록
out=$1; n=$(( $2 / 15 ))
echo "utc pending drained_counter sent_log_total drain_log_total" > $out
for i in $(seq 0 $n); do
  t=$(date -u +%H:%M:%SZ)
  m=$(curl -s --max-time 5 localhost:8080/actuator/prometheus)
  p=$(echo "$m" | grep '^telemetry_spool_pending' | awk '{print $2}')
  d=$(echo "$m" | grep '^telemetry_spool_drained' | awk '{print $2}' | head -1)
  l=$(docker logs telemetry-backend 2>&1)
  s=$(echo "$l" | grep -c "\[Kafka\] 전송 완료")
  r=$(echo "$l" | grep -c "spool 드레인 완료")
  echo "$t ${p:-NA} ${d:-NA} $s $r" >> $out
  sleep 15
done
