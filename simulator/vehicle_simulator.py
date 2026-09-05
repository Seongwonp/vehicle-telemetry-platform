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
        # 반드시 정상 상태에서 출발한다. 직전 틱이 룰 이상이었으면 그 값이 상태에 남아
        # 있어서(next()를 거치지 않으면 안 지워진다) 복합 이상 페이로드에 실려 나가고,
        # 룰이 잡아버려 "룰이 못 잡는 케이스"라는 전제가 깨진다 — 실측에서 복합 19,908건
        # 중 261건이 남은 dtc_code 때문에, 그 외에 battery_voltage도 같은 식으로 오염됐다.
        # 필드를 하나씩 되돌리면 새 룰 이상이 생길 때마다 또 놓치므로 통째로 정상화한다.
        self.next()

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


class PublishStats:
    """발행 결과를 단계별로 센다.

    MQTT 브로커 장애의 정합성을 대조하려면 **"몇 건을 실제로 발행했는가"라는
    정답 기준이 발행 측에 있어야 한다.** InfluxDB·Kafka 장애는 Kafka 토픽이나
    backend의 MQTT 수신 카운터를 기준으로 쓸 수 있었지만(`load-test/fault-injection/`),
    브로커가 죽으면 그 아래 단계가 전부 비어 있어 기준이 될 수 없다.

    문제는 `client.publish()`의 반환값이 그 기준이 **아니라는** 점이다.
    `rc == MQTT_ERR_SUCCESS`는 "paho 클라이언트의 송신 큐에 넣었다"는 뜻이지
    브로커가 받았다는 뜻이 아니다. QoS 1에서 브로커가 받았다는 증거는 PUBACK이고,
    그게 `on_publish` 콜백이다. 그래서 두 단계를 따로 센다:

    - ``queued``    : publish()가 성공을 반환 — 클라이언트가 받아들였다
    - ``confirmed`` : PUBACK 도착 — **브로커가 받았다. 이게 정답 기준이다**

    ``queued - confirmed``는 큐에는 들어갔는데 브로커까지 못 간 것으로,
    브로커 장애 구간에서 이 차이가 벌어진다.

    PUBACK이 유실되면 paho가 DUP로 재전송하는데 그때도 ``confirmed``는 1만 오른다.
    즉 ``confirmed``는 브로커가 실제로 받은 건수의 **하한**이다 —
    유실을 판정하는 기준으로는 이쪽이 안전하다(과소 추정은 유실을 놓치지 않는다).
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self.attempted = 0
        self.queued = 0
        self.rejected = 0
        self.confirmed = 0

    def attempt(self, accepted: bool) -> None:
        with self._lock:
            self.attempted += 1
            if accepted:
                self.queued += 1
            else:
                self.rejected += 1

    def confirm(self) -> None:
        with self._lock:
            self.confirmed += 1

    def snapshot(self) -> dict:
        with self._lock:
            return {
                "attempted": self.attempted,
                "queued": self.queued,
                "rejected": self.rejected,
                "confirmed": self.confirmed,
            }

    def line(self) -> str:
        """장애 주입 스크립트가 파싱하는 한 줄. 접두사를 고정한다."""
        s = self.snapshot()
        return ("[STATS] attempted={attempted} queued={queued} "
                "rejected={rejected} confirmed={confirmed}".format(**s))


STATS = PublishStats()
STATS_INTERVAL = float(os.getenv("STATS_INTERVAL", "5"))
# 종료 시 미확인 메시지를 몇 초까지 flush할지. 장애 주입 실험에서 정답 기준을
# 확정하려면 필요하다 — 자세한 이유는 run_vehicle의 finally 절 주석 참고.
SHUTDOWN_FLUSH_SECONDS = float(os.getenv("SHUTDOWN_FLUSH_SECONDS", "45"))


def _report_stats(stop_event: threading.Event) -> None:
    """주기적으로 `[STATS]` 한 줄을 남긴다.

    컨테이너를 `--rm`으로 띄우면 정지와 동시에 로그가 사라져서, 종료 시점에만
    찍으면 읽을 기회가 없다. 주기적으로 남겨두면 컨테이너를 지우기 전 아무 때나
    마지막 줄을 집어갈 수 있다.
    """
    while not stop_event.wait(STATS_INTERVAL):
        logger.info(STATS.line())


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

    # 이 클라이언트가 아직 PUBACK을 못 받은 건수. 종료 시 flush를 기다리는 데 쓴다.
    pending = {"sent": 0, "acked": 0}
    pending_lock = threading.Lock()

    def on_publish(c, userdata, mid):
        # QoS 1에서 이 콜백은 PUBACK 수신을 뜻한다 — 브로커가 실제로 받았다는
        # 유일한 증거다. publish()의 반환값은 큐에 넣은 것까지만 보장한다.
        STATS.confirm()
        with pending_lock:
            pending["acked"] += 1

    client.on_connect = on_connect
    client.on_disconnect = on_disconnect
    client.on_publish = on_publish

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
            STATS.attempt(result.rc == mqtt.MQTT_ERR_SUCCESS)
            # QoS 1은 rc가 NO_CONN이어도 paho의 송신 큐에 남아 재연결 때 재전송된다
            # (`_messages_reconnect_reset_out`가 DUP로 표시해 다시 큐에 넣는다).
            # 그래서 "미확인 건수"는 rc와 무관하게 시도 전부를 세야 한다.
            with pending_lock:
                pending["sent"] += 1

            if result.rc == mqtt.MQTT_ERR_SUCCESS:
                log.info(
                    f"speed={payload['speed']:>6.1f}km/h  "
                    f"rpm={payload['rpm']:>4d}  "
                    f"temp={payload['engine_temp']:>5.1f}°C  "
                    f"bat={payload['battery_voltage']:.2f}V  "
                    f"fuel={payload['fuel_level']:.1f}%"
                )
            else:
                # rc=4(MQTT_ERR_NO_CONN)가 대부분이다 — 브로커와 연결이 끊긴 상태.
                # 유실 확정은 아니다. QoS 1이라 재연결 시 재전송된다.
                log.warning(f"publish() 실패 (rc={result.rc}) — 재연결 시 재전송 대상")

            stop_event.wait(PUBLISH_INTERVAL)

    except ConnectionRefusedError:
        log.error(f"MQTT 브로커에 연결할 수 없습니다 ({MQTT_HOST}:{MQTT_PORT}). docker-compose가 실행 중인지 확인하세요.")
    except Exception as e:
        log.error(f"오류 발생: {e}", exc_info=True)
    finally:
        # 끊기 전에 미확인 메시지를 flush한다.
        #
        # 이게 없으면 정답 기준이 확정되지 않는다. 브로커 장애 뒤에는 paho의 송신
        # 큐에 수백 건씩 밀려 있는데, 바로 disconnect하면 그 건들이 PUBACK을 받기
        # 전에 버려진다. 그러면 `confirmed`가 실제보다 작게 나와, **정답 기준이
        # backend 수신량보다 작은** 말이 안 되는 결과가 된다(실측으로 겪었다:
        # 기준 46,142 < 수신 49,060).
        #
        # 무한정 기다리지는 않는다. 여기서 시간이 다 지나도 안 빠지는 건은 진짜로
        # 브로커에 못 간 것이고, `attempted - confirmed`가 그 값이다.
        deadline = time.monotonic() + SHUTDOWN_FLUSH_SECONDS
        while time.monotonic() < deadline:
            with pending_lock:
                if pending["sent"] <= pending["acked"]:
                    break
            time.sleep(0.2)
        with pending_lock:
            unflushed = pending["sent"] - pending["acked"]
        if unflushed > 0:
            log.warning(f"종료 시 미확인 {unflushed}건 — 브로커까지 못 간 것으로 집계된다")
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

    reporter = threading.Thread(
        target=_report_stats, args=(stop_event,), name="stats-reporter", daemon=True)
    reporter.start()

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

    final = STATS.snapshot()
    logger.info("=" * 60)
    logger.info(STATS.line())
    logger.info(f"  publish() 시도       : {final['attempted']:,}")
    logger.info(f"  클라이언트 큐 적재   : {final['queued']:,}")
    logger.info(f"  큐 적재 실패         : {final['rejected']:,}  (브로커 미연결 — 재전송 안 됨)")
    logger.info(f"  브로커 확인(PUBACK)  : {final['confirmed']:,}  ← 정합성 대조의 정답 기준")
    logger.info("=" * 60)
    logger.info("전체 시뮬레이터 종료 완료")


if __name__ == "__main__":
    main()
