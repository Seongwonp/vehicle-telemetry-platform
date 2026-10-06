"""하드웨어 없이 돌린다 — 가짜 OBD 연결, 가짜 MQTT 클라이언트."""
from __future__ import annotations

import importlib.util
import os
import sys
from dataclasses import dataclass
from types import SimpleNamespace

import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
REPO = os.path.dirname(ROOT)
sys.path.insert(0, ROOT)


@pytest.fixture(scope="session")
def contract():
    """anomaly-detector/contract.py를 **경로로** 불러온다 — 사본을 두지 않는다."""
    path = os.path.join(REPO, "anomaly-detector", "contract.py")
    spec = importlib.util.spec_from_file_location("telemetry_contract", path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


# ── 가짜 OBD ─────────────────────────────────────────────
class FakeResponse:
    def __init__(self, value):
        self.value = value

    def is_null(self):
        return self.value is None


class FakeObdConnection:
    """values: 명령 이름 → 값(Pint 수량, 숫자, 또는 None). unsupported: 명령 이름 집합."""

    def __init__(self, values, unsupported=()):
        self.values = dict(values)
        self.unsupported = set(unsupported)
        self.queries = []

    def supports(self, cmd):
        return cmd.name not in self.unsupported

    def query(self, cmd):
        self.queries.append(cmd.name)
        return FakeResponse(self.values.get(cmd.name))


def fake_lookup(name):
    return SimpleNamespace(name=name)


# ── 가짜 MQTT ────────────────────────────────────────────
@dataclass
class FakeInfo:
    rc: int
    mid: int


class FakeMqttClient:
    """paho Client의 쓰는 부분만. 콜백은 테스트가 직접 부른다(브로커 역할)."""

    def __init__(self):
        self.on_connect = self.on_disconnect = self.on_publish = self.on_connect_fail = None
        self.calls = []  # 폐기 순서 확인용: "disconnect", "loop_stop"
        self.logger = None
        self.reconnect_delay = None
        self.published = []  # (mid, topic, payload)
        self.tls = None
        self.started = False
        self.stopped = False
        self.publish_rc = 0
        self._mid = 0

    def tls_set(self, **kw):
        self.tls = kw

    def connect_async(self, host, port, keepalive=60):
        self.target = (host, port)

    def loop_start(self):
        self.started = True

    def loop_stop(self):
        self.calls.append("loop_stop")
        self.stopped = True

    def disconnect(self):
        self.calls.append("disconnect")

    def enable_logger(self, logger=None):
        self.logger = logger

    def reconnect_delay_set(self, min_delay=1, max_delay=120):
        self.reconnect_delay = (min_delay, max_delay)

    def publish(self, topic, payload, qos=0):
        assert qos == 1
        self._mid += 1
        if self.publish_rc == 0:
            self.published.append((self._mid, topic, payload))
        return FakeInfo(self.publish_rc, self._mid)

    # 브로커 흉내
    def fire_connect(self, rc=0):
        self.on_connect(self, None, None, rc, None)

    def fire_connect_fail(self):
        self.on_connect_fail(self, None)

    def fire_disconnect(self):
        self.on_disconnect(self, None, None, 0, None)

    def fire_puback(self, mid, rc=0):
        self.on_publish(self, None, mid, rc, None)


class ClientFactory:
    def __init__(self):
        self.clients = []

    def __call__(self):
        c = FakeMqttClient()
        self.clients.append(c)
        return c

    @property
    def last(self):
        return self.clients[-1]


class FakeClock:
    def __init__(self):
        self.t = 1000.0

    def __call__(self):
        return self.t
