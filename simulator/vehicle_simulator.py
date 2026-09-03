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
# 부하 테스트용 — 시뮬레이터 인스턴스를 여러 프로세스(컨테이너)로 병렬 실행할 때
# vehicle_id가 겹치지 않도록 인스턴스마다 다른 오프셋을 준다. GIL 때문에 스레드 수를
# 한 프로세스 안에서 늘리는 건 ~1,000-1,250 msg/s에서 벽에 부딪힌다(부하 테스트로 확인) —
# 진짜 더 센 부하를 걸려면 프로세스(=GIL 인스턴스)를 늘려야 한다.
VEHICLE_ID_OFFSET = int(os.getenv("VEHICLE_ID_OFFSET", "0"))

# ── ML 평가용 시나리오 (기본 비활성 — 켜지 않으면 기존 동작 그대로) ────────
# 룰이 잡는 이상값(ANOMALY_RATE)만으로는 ML 탐지 품질을 잴 수 없다. 주입한 정답이
# 곧 룰이 잡는 것이라, "룰이 못 잡는 복합 패턴"에 대한 정답 데이터가 없기 때문이다
# (12차 측정에서 확인). 아래 두 가지가 그 공백을 메운다.
#
# 1) 복합 이상 — 개별 필드는 전부 룰 임계값 안이지만 조합이 비정상인 패턴.
#    룰은 원칙적으로 못 잡고 ML(다변량 이상치)만 잡을 수 있어야 하는 케이스다.
COMPOSITE_ANOMALY_RATE = float(os.getenv("COMPOSITE_ANOMALY_RATE", "0.0"))
#
# 2) 분포 이동(concept drift) — 정상 자체가 서서히 변하는 상황. 재학습 주기가
#    적절한지 재려면 "정상이 변했는데 모델이 언제 따라오는가"를 볼 수 있어야 한다.
#    DRIFT_TEMP_DELTA만큼 엔진 온도 기준선을 DRIFT_RAMP_SECONDS에 걸쳐 선형으로 올린다.
DRIFT_TEMP_DELTA    = float(os.getenv("DRIFT_TEMP_DELTA", "0.0"))
DRIFT_START_SECONDS = float(os.getenv("DRIFT_START_SECONDS", "300"))
DRIFT_RAMP_SECONDS  = float(os.getenv("DRIFT_RAMP_SECONDS", "300"))

# 정답 로그 접두사 — 채점 스크립트가 이 줄만 뽑아 알림과 조인한다.
GROUND_TRUTH_PREFIX = "[GT]"

# Phase 4 TLS 설정 (인증서 경로 설정 시 자동 활성화)
TLS_CA_CERT      = os.getenv("TLS_CA_CERT", "")        # broker/certs/ca.crt
TLS_CLIENT_CERT  = os.getenv("TLS_CLIENT_CERT", "")    # 단일 인증서 로컬 호환 모드
TLS_CLIENT_KEY   = os.getenv("TLS_CLIENT_KEY", "")
TLS_VEHICLE_CERT_DIR = os.getenv("TLS_VEHICLE_CERT_DIR", "")

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
    # 드리프트 경과 시간의 기준점. 틱이 아니라 실제 시각을 쓰는 이유는
    # PUBLISH_INTERVAL이 바뀌어도 "몇 분 뒤부터 이동" 이라는 의미가 유지되게 하기 위함이다.
    _started_at: float    = field(default_factory=time.time)

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
            target_temp = 90.0 + (self.rpm - 2000) * 0.003 + self._drift_offset()
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

    def _drift_offset(self) -> float:
        """경과 시간에 따라 0 → DRIFT_TEMP_DELTA로 선형 증가하는 기준선 이동량.

        "정상 자체가 서서히 변하는" 상황을 만든다. 이상값 주입과 달리 이건 이상이
        아니라 **새로운 정상**이므로, 잘 만든 감지기라면 재학습 후 알림이 원래
        수준으로 돌아와야 한다 — 재학습 주기가 적절한지 재는 기준이 된다.
        """
        if DRIFT_TEMP_DELTA == 0.0:
            return 0.0
        elapsed = time.time() - self._started_at
        if elapsed <= DRIFT_START_SECONDS:
            return 0.0
        if DRIFT_RAMP_SECONDS <= 0:
            return DRIFT_TEMP_DELTA
        progress = min(1.0, (elapsed - DRIFT_START_SECONDS) / DRIFT_RAMP_SECONDS)
        return DRIFT_TEMP_DELTA * progress

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

        payload = self._to_payload()
        self._log_ground_truth(payload, anomaly_type, "rule")
        return payload

    def inject_composite_anomaly(self) -> dict:
        """룰이 못 잡는 복합 패턴 주입 — 개별 필드는 전부 임계값 안에 둔다.

        각 케이스는 "필드 하나만 보면 정상인데 조합이 말이 안 되는" 상황이다.
        룰은 단일 필드 임계값만 보므로 원칙적으로 못 잡고, 다변량 이상치를 보는
        ML만 잡을 수 있어야 한다 — ML의 존재 이유를 검증하는 정답 데이터다.
        """
        anomaly_type = random.choice([
            "clutch_slip", "alternator_degrading",
            "overheat_at_idle", "throttle_no_response",
        ])

        if anomaly_type == "clutch_slip":
            # 거의 정지 상태인데 RPM만 높다(정상이면 speed 10km/h에서 rpm ≈ 1,020).
            self.speed = random.uniform(5.0, 15.0)
            self.rpm = random.randint(4000, 4500)       # < 6000
            self.throttle = random.uniform(60.0, 80.0)
        elif anomaly_type == "alternator_degrading":
            # 엔진이 도는데 충전 전압이 안 나온다. LOW 임계(11.5) 위라 룰엔 안 걸린다.
            self.battery_voltage = random.uniform(11.6, 12.2)
            self.rpm = random.randint(2500, 4000)
        elif anomaly_type == "overheat_at_idle":
            # 공회전인데 냉각이 안 된다. HIGH 임계(105) 아래로 유지한다.
            self.speed = random.uniform(0.0, 3.0)
            self.rpm = random.randint(800, 1000)
            self.engine_temp = random.uniform(100.0, 103.0)
        elif anomaly_type == "throttle_no_response":
            # 스로틀을 밟는데 차가 안 나간다.
            self.throttle = random.uniform(80.0, 100.0)
            self.speed = random.uniform(0.0, 10.0)
            self.rpm = random.randint(800, 1100)

        payload = self._to_payload()
        self._log_ground_truth(payload, anomaly_type, "composite")
        return payload

    def _log_ground_truth(self, payload: dict, label: str, kind: str) -> None:
        """채점용 정답 한 줄. 텔레메트리 페이로드에는 라벨을 넣지 않는다 —
        넣으면 감지기가 정답을 볼 수 있게 되고 운영 스키마도 오염된다.
        (vehicle_id, timestamp)로 알림과 조인할 수 있게 둘 다 남긴다."""
        logger.warning(
            f"{GROUND_TRUTH_PREFIX} vehicle={payload['vehicle_id']} "
            f"ts={payload['timestamp']} label={label} kind={kind}"
        )

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

    # 운영 프로파일은 vehicle_id와 CN이 같은 차량별 인증서를 사용한다.
    vehicle_cert = os.path.join(TLS_VEHICLE_CERT_DIR, f"{vehicle_id}.crt") if TLS_VEHICLE_CERT_DIR else TLS_CLIENT_CERT
    vehicle_key = os.path.join(TLS_VEHICLE_CERT_DIR, f"{vehicle_id}.key") if TLS_VEHICLE_CERT_DIR else TLS_CLIENT_KEY
    if TLS_CA_CERT and vehicle_cert and vehicle_key:
        import ssl
        client.tls_set(
            ca_certs=TLS_CA_CERT,
            certfile=vehicle_cert,
            keyfile=vehicle_key,
            tls_version=ssl.PROTOCOL_TLSv1_2,
        )
        log.info(f"TLS mTLS 활성화됨 (CN={vehicle_id})")

    try:
        client.connect(MQTT_HOST, MQTT_PORT, keepalive=60)
        client.loop_start()

        while not stop_event.is_set():
            # 룰 이상 → 복합 이상 → 정상 순으로 판정한다. 둘 다 0이면(기본값)
            # 기존과 동일하게 ANOMALY_RATE만 적용된다.
            roll = random.random()
            if roll < ANOMALY_RATE:
                payload = state.inject_anomaly()
            elif roll < ANOMALY_RATE + COMPOSITE_ANOMALY_RATE:
                payload = state.inject_composite_anomaly()
            else:
                payload = state.next()

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
    logger.info(f"  차량 수      : {VEHICLE_COUNT}대 (ID 오프셋 {VEHICLE_ID_OFFSET})")
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
        vehicle_id = f"SIM-{VEHICLE_ID_OFFSET + i:03d}"
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
