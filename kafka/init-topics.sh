#!/bin/bash
# ================================================================
# Kafka 토픽 초기화 스크립트
# docker-compose up 시 kafka-init 컨테이너가 1회 실행
# ================================================================
set -e

KAFKA_BROKER="kafka:29092"

echo "================================================================"
echo "Kafka 토픽 초기화 시작..."
echo "브로커: $KAFKA_BROKER"
echo "================================================================"

# Kafka가 완전히 뜰 때까지 대기
echo "Kafka 준비 대기 중..."
sleep 10

create_topic() {
  local topic=$1
  local partitions=${2:-3}
  local replication=${3:-1}

  kafka-topics --bootstrap-server $KAFKA_BROKER \
    --create \
    --if-not-exists \
    --topic "$topic" \
    --partitions "$partitions" \
    --replication-factor "$replication"

  echo "[OK] 토픽 생성: $topic (파티션: $partitions, 복제: $replication)"
}

# 차량 텔레메트리 원본 데이터 (핵심 토픽)
create_topic "vehicle-telemetry" 3 1

# 이상 감지 결과 알림 (Phase 3에서 사용)
create_topic "vehicle-anomaly-alerts" 3 1

# DTC 진단 코드 이벤트 (Phase 3에서 사용)
create_topic "vehicle-dtc-events" 1 1

echo "================================================================"
echo "생성된 토픽 목록:"
kafka-topics --bootstrap-server $KAFKA_BROKER --list
echo "================================================================"
echo "Kafka 토픽 초기화 완료!"
