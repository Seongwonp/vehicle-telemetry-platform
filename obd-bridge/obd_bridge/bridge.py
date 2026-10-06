"""폴링 루프 + CLI. 설정은 CLI 인자 또는 환경변수(이름만 README에 적는다)."""
from __future__ import annotations

import argparse
import json
import logging
import os
import signal
import threading
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Callable, Optional

from .mapping import build_payload, check_vehicle_id, format_timestamp, topic_for
from .publisher import SpoolPublisher, TlsConfig, make_paho_client
from .reader import PidReader
from .spool import Spool

log = logging.getLogger("obd_bridge")


@dataclass
class CycleStats:
    cycles: int = 0
    spooled: int = 0
    skipped_missing: int = 0       # 필수 PID가 None — 보내지 않았다
    skipped_out_of_range: int = 0  # 계약 범위 밖 — 보내지 않았다
    missing_by_field: dict = field(default_factory=dict)


class Bridge:
    def __init__(self, reader: PidReader, spool: Spool, publisher: SpoolPublisher,
                 vehicle_id: str, topic: str,
                 now: Callable[[], datetime] = lambda: datetime.now(timezone.utc)):
        self.reader = reader
        self.spool = spool
        self.publisher = publisher
        self.vehicle_id = check_vehicle_id(vehicle_id)
        self.topic = topic
        self._now = now
        self.stats = CycleStats()

    def cycle(self) -> Optional[dict]:
        """한 주기: timestamp를 찍고 PID 6개를 읽고, 계약을 채우면 spool에 쓴다.

        timestamp는 **주기 시작 시각**이다. PID 6개는 그 뒤 순차로 읽히므로 실제 측정 시각은
        한 주기 길이만큼 퍼진다(README의 폴링 측정 참고).
        """
        dt = self._now()
        ts = format_timestamp(dt)
        readings = self.reader.read()
        self.stats.cycles += 1
        result = build_payload(readings, self.vehicle_id, ts)
        if result.missing:
            self.stats.skipped_missing += 1
            for f in result.missing:
                self.stats.missing_by_field[f] = self.stats.missing_by_field.get(f, 0) + 1
            log.warning("필수 PID 없음 %s — 0으로 채우지 않고 이 주기는 보내지 않는다", result.missing)
            return None
        if result.out_of_range:
            self.stats.skipped_out_of_range += 1
            log.warning("계약 범위 밖 %s (원값 %s) — 보내지 않는다",
                        result.out_of_range, {f: readings[f] for f in result.out_of_range})
            return None
        self.spool.append(self.topic, json.dumps(result.payload, separators=(",", ":")),
                          ts, int(dt.timestamp() * 1000))
        self.stats.spooled += 1
        return result.payload

    def run(self, interval_s: float, stop: threading.Event, pump_every_s: float = 0.05) -> None:
        next_at = time.monotonic()
        while not stop.is_set():
            self.cycle()
            self.publisher.pump()
            next_at += interval_s
            # 다음 주기까지 PUBACK 처리와 재전송을 돌린다.
            while not stop.is_set():
                remaining = next_at - time.monotonic()
                if remaining <= 0:
                    break
                stop.wait(min(pump_every_s, remaining))
                self.publisher.pump()
            if time.monotonic() - next_at > interval_s:
                next_at = time.monotonic()  # 밀렸으면 따라잡으려 연사하지 않는다


def _env(name: str, default: Optional[str] = None) -> Optional[str]:
    v = os.environ.get(name)
    return v if v not in (None, "") else default


def parse_args(argv=None) -> argparse.Namespace:
    p = argparse.ArgumentParser(prog="obd_bridge", description="OBD-II → MQTT 브리지(프로토타입)")
    p.add_argument("--vehicle-id", default=_env("VEHICLE_ID"))
    p.add_argument("--obd-port", default=_env("OBD_PORT"),
                   help="예: COM5, /dev/ttyUSB0, socket://127.0.0.1:35000")
    p.add_argument("--obd-baudrate", type=int, default=int(_env("OBD_BAUDRATE", "0")),
                   help="0=자동. socket:// 은 자동 탐지가 안 되므로 지정해야 한다")
    p.add_argument("--mqtt-host", default=_env("MQTT_HOST", "localhost"))
    p.add_argument("--mqtt-port", type=int, default=int(_env("MQTT_PORT", "8883")))
    p.add_argument("--mqtt-protocol", choices=["3.1.1", "5"], default=_env("MQTT_PROTOCOL", "3.1.1"))
    p.add_argument("--topic-prefix", default=_env("MQTT_TOPIC_PREFIX", "vehicle/telemetry"))
    p.add_argument("--tls-ca-cert", default=_env("TLS_CA_CERT"))
    p.add_argument("--tls-client-cert", default=_env("TLS_CLIENT_CERT"))
    p.add_argument("--tls-client-key", default=_env("TLS_CLIENT_KEY"))
    p.add_argument("--insecure-plaintext", action="store_true",
                   default=_env("MQTT_INSECURE_PLAINTEXT") == "1",
                   help="dev override 전용. 기본은 mTLS")
    p.add_argument("--spool-dir", default=_env("SPOOL_DIR", "./spool"))
    p.add_argument("--interval", type=float, default=float(_env("POLL_INTERVAL", "1.0")))
    return p.parse_args(argv)


def main(argv=None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    a = parse_args(argv)
    if not a.vehicle_id or not a.obd_port:
        log.error("VEHICLE_ID와 OBD_PORT가 필요하다")
        return 2
    vehicle_id = check_vehicle_id(a.vehicle_id)
    tls = TlsConfig(a.tls_ca_cert, a.tls_client_cert, a.tls_client_key, a.insecure_plaintext)
    if not tls.insecure_plaintext and not (tls.ca_cert and tls.client_cert and tls.client_key):
        log.error("mTLS 인증서 경로(TLS_CA_CERT/TLS_CLIENT_CERT/TLS_CLIENT_KEY)가 없다. "
                  "평문은 --insecure-plaintext(dev 전용)")
        return 2

    import obd
    import paho.mqtt.client as mqtt
    conn = obd.OBD(a.obd_port, baudrate=a.obd_baudrate or None, fast=False)
    if not conn.is_connected():
        log.error("OBD 연결 실패: %s", conn.status())
        return 1
    protocol = mqtt.MQTTv5 if a.mqtt_protocol == "5" else mqtt.MQTTv311
    spool = Spool(a.spool_dir)
    log.info("spool 미전송 %d건으로 시작", len(spool))
    publisher = SpoolPublisher(
        spool, host=a.mqtt_host, port=a.mqtt_port, client_id=f"obd-bridge-{vehicle_id}",
        tls=tls, client_factory=lambda: make_paho_client(f"obd-bridge-{vehicle_id}", protocol))
    bridge = Bridge(PidReader(conn), spool, publisher, vehicle_id,
                    topic_for(a.topic_prefix, vehicle_id))

    stop = threading.Event()
    signal.signal(signal.SIGINT, lambda *_: stop.set())
    try:
        bridge.run(a.interval, stop)
    finally:
        publisher.close()
        conn.close()
        log.info("종료 — stats=%s publisher=%s spool 미전송=%d",
                 bridge.stats, publisher.stats, len(spool))
    return 0
