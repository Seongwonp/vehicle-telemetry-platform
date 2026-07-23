#!/usr/bin/env python3
"""
Vehicle Telemetry Simulator

OBD-II 차량 센서 데이터를 시뮬레이션하여 MQTT로 전송.
실제 OBD-II 동글로 교체할 때는 VehicleState.next() 부분만
obd 라이브러리 호출로 바꾸면 됨.
"""
import os
import json
import math
import time
import random
import signal
import logging
import threading
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import List

import paho.mqtt.client as mqtt
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("simulator")

# ── 설정 ────────────────────────────────────────────────────────
MQTT_HOST        = os.getenv("MQTT_HOST", "localhost")
MQTT_PORT        = int(os.getenv("MQTT_PORT", "1883"))
TOPIC_PREFIX     = os.getenv("MQTT_TOPIC_PREFIX", "vehicle/telemetry")
PUBLISH_INTERVAL = float(os.getenv("PUBLISH_INTERVAL", "1.0"))   # 초
VEHICLE_COUNT    = int(os.getenv("VEHICLE_COUNT", "1"))
ANOMALY_RATE     = float(os.getenv("ANOMALY_RATE", "0.02"))       # 2% 확률

# Phase 4 TLS 설정 (인증서 경로 설정 시 자동 활성화)
TLS_CA_CERT      = os.getenv("TLS_CA_CERT", "")        # broker/certs/ca.crt
TLS_CLIENT_CERT  = os.getenv("TLS_CLIENT_CERT", "")    # broker/certs/client.crt
TLS_CLIENT_KEY   = os.getenv("TLS_CLIENT_KEY", "")     # broker/certs/client.key

# 서울 근처 GPS 기준점
BASE_LAT = 37.4563
BASE_LNG = 127.1313

# 이상 감지 룰 임계값 (CLAUDE.md 기준)
THRESHOLD_ENGINE_TEMP_HIGH = 105.0
THRESHOLD_RPM_HIGH         = 6000
THRESHOLD_BATTERY_LOW      = 11.5
THRESHOLD_BATTERY_HIGH     = 15.0
THRESHOLD_SPEED_HIGH       = 200.0


@dataclass
class VehicleState:
    """
    차량 상태 객체.
    매 틱마다 next()를 호출하면 현실적으로 변화하는 센서값 반환.
    """
    vehicle_id: str
    speed: float          = 0.0
    rpm: int              = 800
    engine_temp: float    = 20.0   # 냉간 시동 상태에서 시작
    throttle: float       = 0.0
    fuel_level: float     = field(default_factory=lambda: random.uniform(30.0, 90.0))
    battery_voltage: float = 13.8
    lat: float            = field(default_factory=lambda: BASE_LAT + random.uniform(-0.05, 0.05))
    lng: float            = field(default_factory=lambda: BASE_LNG + random.uniform(-0.05, 0.05))
    dtc_codes: List[str]  = field(default_factory=list)
    _tick: int            = 0

    def next(self) -> dict:
        """정상 주행 데이터 생성 (자연스러운 가속/순항/감속 사이클)"""
        self._tick += 1

        # 60틱(60초) 주기로 가속→순항→감속 반복
        phase = (self._tick % 60) / 60.0
        target_speed = 80.0 + 40.0 * math.sin(phase * 2 * math.pi)
        target_speed = max(0.0, target_speed)

        # 스무딩 처리 (급격한 변화 방지)
        self.speed += (target_speed - self.speed) * 0.1 + random.uniform(-1.0, 1.0)
        self.speed = max(0.0, min(160.0, self.speed))

        # RPM — 속도 비례
        target_rpm = 800 + int(self.speed * 22)
        self.rpm += int((target_rpm - self.rpm) * 0.15) + random.randint(-50, 50)
        self.rpm = max(800, min(4500, self.rpm))

        # 엔진 온도 — 워밍업(처음 30틱) 후 90°C 안정
        if self._tick < 30:
            self.engine_temp += random.uniform(1.5, 2.5)
        else:
            target_temp = 90.0 + (self.rpm - 2000) * 0.003
            self.engine_temp += (target_temp - self.engine_temp) * 0.05
            self.engine_temp += random.uniform(-0.2, 0.2)
        self.engine_temp = max(20.0, min(103.0, self.engine_temp))

        # 스로틀 포지션
        self.throttle = min(100.0, max(0.0,
            (self.speed / 160.0) * 60.0 + random.uniform(-5.0, 5.0)
        ))

        # 연료 소모
        self.fuel_level -= (self.rpm / 1_000_000.0) * PUBLISH_INTERVAL
        self.fuel_level = max(0.0, self.fuel_level)

        # 배터리 전압 (정상 범위 내 미세 변동)
        self.battery_voltage = 13.8 + random.uniform(-0.1, 0.1)

        # GPS 이동
        self.lat += random.uniform(-0.0001, 0.0001)
        self.lng += random.uniform(-0.0001, 0.0001)

        self.dtc_codes = []
        return self._to_payload()

    def inject_anomaly(self) -> dict:
        """이상 감지 테스트용 — 룰 임계값을 초과하는 값 주입"""
        anomaly_type = random.choice([
            "high_engine_temp", "high_rpm",
            "low_battery", "high_battery",
            "high_speed", "dtc_code",
        ])

        if anomaly_type == "high_engine_temp":
            self.engine_temp = random.uniform(106.0, 115.0)
        elif anomaly_type == "high_rpm":
            self.rpm = random.randint(6100, 7000)
        elif anomaly_type == "low_battery":
            self.battery_voltage = random.uniform(10.0, 11.4)
        elif anomaly_type == "high_battery":
            self.battery_voltage = random.uniform(15.1, 16.0)
        elif anomaly_type == "high_speed":
            self.speed = random.uniform(201.0, 230.0)
        elif anomaly_type == "dtc_code":
            self.dtc_codes = [random.choice(["P0300", "P0171", "P0420", "B0001"])]

        logger.warning(f"[{self.vehicle_id}] 이상값 주입 → {anomaly_type}")
        return self._to_payload()

    def _to_payload(self) -> dict:
        return {
            "vehicle_id": self.vehicle_id,
            # 초 단위 문자열이면 PUBLISH_INTERVAL이 1초 미만일 때 같은 차량의 여러 메시지가
            # InfluxDB에서 동일 타임스탬프로 충돌해 뒤 값이 앞 값을 덮어쓴다(부하 테스트로 발견).
            # 밀리초까지 남겨서 충돌을 없앤다.
            "timestamp": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z",
            "speed": round(self.speed, 1),
            "rpm": self.rpm,
            "engine_temp": round(self.engine_temp, 1),
            "throttle_position": round(self.throttle, 1),
            "fuel_level": round(self.fuel_level, 1),
            "battery_voltage": round(self.battery_voltage, 2),
            "gps": {
                "lat": round(self.lat, 6),
                "lng": round(self.lng, 6),
            },
            "dtc_codes": self.dtc_codes,
        }


def run_vehicle(vehicle_id: str, stop_event: threading.Event) -> None:
    """단일 차량 시뮬레이션 스레드"""
    log = logging.getLogger(f"vehicle.{vehicle_id}")
    state = VehicleState(vehicle_id=vehicle_id)
    topic = f"{TOPIC_PREFIX}/{vehicle_id}"

    client = mqtt.Client(client_id=f"simulator-{vehicle_id}")

    def on_connect(c, userdata, flags, rc):
        if rc == 0:
            log.info(f"MQTT 연결 성공 → {MQTT_HOST}:{MQTT_PORT} | 토픽: {topic}")
        else:
            log.error(f"MQTT 연결 실패 (rc={rc})")

    def on_disconnect(c, userdata, rc):
        if rc != 0:
            log.warning(f"MQTT 연결 끊김 (rc={rc}), 재연결 시도 중...")

    client.on_connect = on_connect
    client.on_disconnect = on_disconnect

    # Phase 4: TLS 인증서가 설정된 경우 mTLS 활성화
    if TLS_CA_CERT and TLS_CLIENT_CERT and TLS_CLIENT_KEY:
        import ssl
        client.tls_set(
            ca_certs=TLS_CA_CERT,
            certfile=TLS_CLIENT_CERT,
            keyfile=TLS_CLIENT_KEY,
            tls_version=ssl.PROTOCOL_TLSv1_2,
        )
        log.info("TLS mTLS 활성화됨")

    try:
        client.connect(MQTT_HOST, MQTT_PORT, keepalive=60)
        client.loop_start()

        while not stop_event.is_set():
            payload = (
                state.inject_anomaly()
                if random.random() < ANOMALY_RATE
                else state.next()
            )

            result = client.publish(topic, json.dumps(payload), qos=1)

            if result.rc == mqtt.MQTT_ERR_SUCCESS:
                log.info(
                    f"speed={payload['speed']:>6.1f}km/h  "
                    f"rpm={payload['rpm']:>4d}  "
                    f"temp={payload['engine_temp']:>5.1f}°C  "
                    f"bat={payload['battery_voltage']:.2f}V  "
                    f"fuel={payload['fuel_level']:.1f}%"
                )
            else:
                log.warning(f"발행 실패 (rc={result.rc})")

            stop_event.wait(PUBLISH_INTERVAL)

    except ConnectionRefusedError:
        log.error(f"MQTT 브로커에 연결할 수 없습니다 ({MQTT_HOST}:{MQTT_PORT}). docker-compose가 실행 중인지 확인하세요.")
    except Exception as e:
        log.error(f"오류 발생: {e}", exc_info=True)
    finally:
        client.loop_stop()
        client.disconnect()
        log.info("차량 시뮬레이터 종료")


def main() -> None:
    logger.info("=" * 60)
    logger.info("  Vehicle Telemetry Simulator")
    logger.info("=" * 60)
    logger.info(f"  MQTT 브로커  : {MQTT_HOST}:{MQTT_PORT}")
    logger.info(f"  차량 수      : {VEHICLE_COUNT}대")
    logger.info(f"  전송 주기    : {PUBLISH_INTERVAL}초")
    logger.info(f"  이상값 확률  : {ANOMALY_RATE * 100:.1f}%")
    logger.info(f"  토픽 prefix  : {TOPIC_PREFIX}/<vehicle_id>")
    logger.info("=" * 60)

    stop_event = threading.Event()

    def handle_signal(signum, frame):
        logger.info("종료 신호 수신 — 시뮬레이터를 중지합니다...")
        stop_event.set()

    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)

    threads = []
    for i in range(1, VEHICLE_COUNT + 1):
        vehicle_id = f"SIM-{i:03d}"
        t = threading.Thread(
            target=run_vehicle,
            args=(vehicle_id, stop_event),
            name=f"vehicle-{vehicle_id}",
            daemon=True,
        )
        threads.append(t)
        t.start()
        time.sleep(0.1)  # 스레드 시작 간격 (브로커 부하 분산)

    for t in threads:
        t.join()

    logger.info("전체 시뮬레이터 종료 완료")


if __name__ == "__main__":
    main()
