# E2E 추적 도구 전용 이미지.
#
# 한 프로세스가 MQTT 발행 · Kafka 조회 · InfluxDB 조회 · REST 호출 · WebSocket 구독을
# 다 해야 지연을 **같은 시계**로 잴 수 있다. 기존 이미지 어느 것도 그 조합을 갖고
# 있지 않다 — anomaly-detector에는 kafka-python과 requests만, simulator에는 paho만 있다.
#
# 시계를 섞으면 안 되는 이유는 실제로 겪었다: 컨테이너 시각과 브로커 타임스탬프를
# 섞어 계산했다가 음수 지연이 나온 적이 있다. 발행과 수신을 한 프로세스에서 재면
# 그 문제가 원천적으로 없다.
FROM vehicle-telemetry-platform-anomaly-detector

# 이미지에 없는 것만 추가한다(kafka-python, requests는 이미 있다).
RUN pip install --no-cache-dir "paho-mqtt>=1.6.1,<2.0" "websocket-client>=1.7.0"

WORKDIR /w
