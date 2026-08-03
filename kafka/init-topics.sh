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
  # Kafka는 이 프로젝트에서 최종 저장소가 아니라 InfluxDB/PostgreSQL로 가기 전 임시
  # 통로일 뿐이다. 브로커 기본 리텐션(7일)을 그대로 두면 시뮬레이터가 계속 도는 동안
  # 무한정 쌓여 디스크를 다 채운다 — 실제로 부하 테스트를 반복하다 호스트 디스크가
  # 꽉 차서 Docker 자체가 응답 불능이 되는 일을 겪었다. 그래서 1시간으로 짧게 잡는다.
  local retention_ms=${4:-3600000}

  kafka-topics --bootstrap-server $KAFKA_BROKER \
    --create \
    --if-not-exists \
    --topic "$topic" \
    --partitions "$partitions" \
    --replication-factor "$replication" \
    --config retention.ms="$retention_ms"

  echo "[OK] 토픽 생성: $topic (파티션: $partitions, 복제: $replication, 리텐션: ${retention_ms}ms)"
}

# 차량 텔레메트리 원본 데이터 (핵심 토픽)
create_topic "vehicle-telemetry" 3 1

# 이상 감지 결과 알림 (Phase 3에서 사용)
create_topic "vehicle-anomaly-alerts" 3 1

# DTC 진단 코드 이벤트 (Phase 3에서 사용)
create_topic "vehicle-dtc-events" 1 1

# DLQ — 저장 실패한 원본 메시지 격리 (Phase 8, backend KafkaConfig가 부팅 시에도 자동 생성함)
create_topic "vehicle-telemetry-dlq" 1 1
create_topic "vehicle-anomaly-alerts-dlq" 1 1

# DLQ — 이상 감지(Python) 처리 실패 원본 메시지 격리. vehicle-telemetry-dlq는 Java
# 저장 경로 전용이라 재사용하면 "어느 경로가 실패했는지" 구분이 안 돼 별도 토픽으로 분리.
create_topic "vehicle-telemetry-anomaly-dlq" 1 1

echo "================================================================"
echo "생성된 토픽 목록:"
kafka-topics --bootstrap-server $KAFKA_BROKER --list
echo "================================================================"
echo "Kafka 토픽 초기화 완료!"
