"""실제 paho-mqtt 2.1.0 클라이언트로 폐기·연결 실패 경로를 잰다. 브로커는 순수 Python 소켓 stub.

stub은 CONNACK과 PINGRESP만 보내고 **PUBACK은 절대 보내지 않는다** — TCP는 살아 있는데 확인이 안 오는 상태.
paho 2.1.0은 이때 loop_stop()을 먼저 부르면 _out_messages가 비지 않아 join이 끝나지 않는다.
"""
import socket
import threading
import time

import paho.mqtt.client as mqtt
import pytest

from obd_bridge.publisher import SpoolPublisher, TlsConfig, make_paho_client
from obd_bridge.spool import Spool

BOUND_S = 5.0


class NoPubackBroker:
    def __init__(self):
        self.srv = socket.socket()
        self.srv.bind(("127.0.0.1", 0))
        self.srv.listen(4)
        self.port = self.srv.getsockname()[1]
        self.packets = []  # 받은 패킷 타입(상위 4비트)
        self._conns = []
        threading.Thread(target=self._accept, daemon=True).start()

    def _accept(self):
        while True:
            try:
                c, _ = self.srv.accept()
            except OSError:
                return
            self._conns.append(c)
            threading.Thread(target=self._serve, args=(c,), daemon=True).start()

    @staticmethod
    def _read_exact(c, n):
        buf = b""
        while len(buf) < n:
            chunk = c.recv(n - len(buf))
            if not chunk:
                raise ConnectionError
            buf += chunk
        return buf

    def _serve(self, c):
        try:
            while True:
                head = self._read_exact(c, 1)[0]
                length, mult = 0, 1
                while True:
                    b = self._read_exact(c, 1)[0]
                    length += (b & 0x7F) * mult
                    mult *= 128
                    if not b & 0x80:
                        break
                self._read_exact(c, length)
                kind = head & 0xF0
                self.packets.append(kind)
                if kind == 0x10:    # CONNECT → CONNACK 성공
                    c.sendall(b"\x20\x02\x00\x00")
                elif kind == 0xC0:  # PINGREQ → PINGRESP
                    c.sendall(b"\xd0\x00")
                elif kind == 0xE0:  # DISCONNECT
                    return
                # PUBLISH(0x30) → PUBACK을 보내지 않는다
        except OSError:
            return
        finally:
            c.close()

    def close(self):
        self.srv.close()
        for c in self._conns:
            try:
                c.close()
            except OSError:
                pass


def paho_threads(client_id):
    return [t for t in threading.enumerate()
            if t.name == f"paho-mqtt-client-{client_id}" and t.is_alive()]


def wait_until(cond, timeout):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if cond():
            return True
        time.sleep(0.02)
    return cond()


def make_real(tmp_path, port, client_id, **kw):
    spool = Spool(str(tmp_path / "spool"))
    created = []

    def factory():
        c = make_paho_client(client_id, mqtt.MQTTv311)
        created.append(c)
        return c

    pub = SpoolPublisher(spool, host="127.0.0.1", port=port, client_id=client_id,
                         tls=TlsConfig(insecure_plaintext=True), client_factory=factory, **kw)
    return spool, pub, created


def run_pump_until(pub, cond, timeout):
    """pump를 별도 스레드에서 돌린다 — 회귀로 무기한 멈춰도 테스트 스위트는 멈추지 않는다."""
    worst = [0.0]

    def loop():
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline and not cond():
            t0 = time.monotonic()
            pub.pump()
            worst[0] = max(worst[0], time.monotonic() - t0)
            time.sleep(0.02)

    t = threading.Thread(target=loop, daemon=True)
    t.start()
    t.join(timeout + BOUND_S)
    return not t.is_alive(), worst[0]


@pytest.fixture
def broker():
    b = NoPubackBroker()
    yield b
    b.close()


def test_puback_timeout_teardown_is_bounded_on_live_link(tmp_path, broker):
    cid = "obd-bridge-test-timeout"
    spool, pub, _ = make_real(tmp_path, broker.port, cid, ack_timeout_s=0.5,
                              reconnect_min_s=60.0)  # 테스트 중 재접속하지 않게
    for i in range(3):
        spool.append("vehicle/telemetry/OBD-TEST-01", f'{{"m":{i}}}', f"ts{i}", i)

    finished, worst = run_pump_until(pub, lambda: pub.stats.ack_timeouts >= 1, timeout=10.0)
    assert finished, "pump가 끝나지 않았다 — 폴링 스레드가 폐기 중에 막혔다"
    assert pub.stats.ack_timeouts == 1
    assert worst < BOUND_S, f"pump 한 번이 {worst:.2f}s 걸렸다"
    assert broker.packets.count(0x30) == 3  # 3건 다 보냈지만 PUBACK은 없었다
    # PUBACK을 못 받았으므로 메모리·디스크 모두에 남는다
    assert len(spool) == 3 and len(Spool(spool.dir)) == 3
    # paho 네트워크 스레드도 제한 시간 안에 끝나고, 브로커는 DISCONNECT를 받았다
    assert wait_until(lambda: not paho_threads(cid), BOUND_S)
    assert 0xE0 in broker.packets


def test_close_with_unacked_messages_is_bounded(tmp_path, broker):
    cid = "obd-bridge-test-close"
    spool, pub, _ = make_real(tmp_path, broker.port, cid, ack_timeout_s=60.0)
    spool.append("vehicle/telemetry/OBD-TEST-01", '{"m":0}', "ts0", 0)
    finished, _ = run_pump_until(pub, lambda: broker.packets.count(0x30) >= 1, timeout=10.0)
    assert finished and broker.packets.count(0x30) == 1

    t0 = time.monotonic()
    pub.close(timeout_s=2.0)
    assert time.monotonic() - t0 < BOUND_S
    assert wait_until(lambda: not paho_threads(cid), BOUND_S)
    assert len(Spool(spool.dir)) == 1


def test_connect_failure_goes_through_bridge_backoff_not_paho_retry(tmp_path, caplog):
    """닫힌 포트: on_connect_fail → 폐기 → 우리 백오프 → 새 클라이언트. 옛 클라이언트 스레드는 남지 않는다."""
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()  # 아무도 듣지 않는 포트

    cid = "obd-bridge-test-refused"
    caplog.set_level("DEBUG", logger="obd_bridge.paho")
    spool, pub, created = make_real(tmp_path, port, cid, reconnect_min_s=0.2, reconnect_max_s=0.4)
    spool.append("t", "A", "a", 1)

    finished, worst = run_pump_until(pub, lambda: pub.stats.connect_failures >= 2, timeout=20.0)
    assert finished and pub.stats.connect_failures >= 2
    assert worst < BOUND_S
    # 실패할 때마다 우리 쪽에서 새 클라이언트를 만들었다(paho가 같은 클라이언트로 조용히 재시도한 것이 아니다)
    assert len(created) >= 2
    pub.close()
    assert wait_until(lambda: not paho_threads(cid), BOUND_S)
    assert len(spool) == 1
    # paho 로그가 우리 logger로 나온다
    assert any(r.name == "obd_bridge.paho" for r in caplog.records)
