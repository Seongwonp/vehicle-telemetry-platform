"""폴링 주기가 밀리면 따라잡기 연사는 하지 않되, 건너뛴 예정 주기를 센다."""
import threading
import time

from conftest import FakeObdConnection, fake_lookup
from obd_bridge.bridge import Bridge
from obd_bridge.reader import PidReader
from obd_bridge.spool import Spool

GOOD = {"SPEED": 50.0, "RPM": 2000.25, "COOLANT_TEMP": 90.0, "THROTTLE_POS": 20.0,
        "FUEL_LEVEL": 60.0, "CONTROL_MODULE_VOLTAGE": 13.9}


class StallingPublisher:
    """첫 pump에서 한 번 오래 멈춘다(예: 폐기 중 막힘)."""

    def __init__(self, stall_s):
        self.stall_s = stall_s
        self.calls = 0

    def pump(self):
        self.calls += 1
        if self.calls == 1:
            time.sleep(self.stall_s)


def test_skipped_cycles_are_counted(tmp_path):
    stop = threading.Event()
    spool = Spool(str(tmp_path / "spool"))
    bridge = Bridge(PidReader(FakeObdConnection(GOOD), command_lookup=fake_lookup), spool,
                    StallingPublisher(stall_s=0.55), "OBD-TEST-01", "vehicle/telemetry/OBD-TEST-01")
    orig = bridge.cycle

    def cycle():
        if bridge.stats.cycles >= 3:
            stop.set()
            return None
        return orig()

    bridge.cycle = cycle
    t = threading.Thread(target=bridge.run, args=(0.1, stop), daemon=True)
    t.start()
    t.join(5)
    assert not t.is_alive()
    # 0.55s 멈춤 동안 예정 주기 0.1~0.5가 지나갔다 → floor(0.45/0.1)=4개 건너뜀(타이밍 여유로 3 이상)
    assert bridge.stats.skipped_overrun >= 3
    assert bridge.stats.skipped_overrun <= 5
