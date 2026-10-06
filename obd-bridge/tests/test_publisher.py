"""PUBACK 뒤에만 지우고, 재연결 시 timestamp 순서로 다시 보낸다."""
import pytest
from paho.mqtt.packettypes import PacketTypes
from paho.mqtt.reasoncodes import ReasonCode

from conftest import ClientFactory, FakeClock
from obd_bridge.publisher import SpoolPublisher, TlsConfig, _is_failure, paho_log
from obd_bridge.spool import Spool


def make(tmp_path, **kw):
    spool = Spool(str(tmp_path / "spool"))
    factory = ClientFactory()
    clock = FakeClock()
    kw.setdefault("tls", TlsConfig(insecure_plaintext=True))
    pub = SpoolPublisher(spool, host="broker", port=8883, client_id="c",
                         client_factory=factory, clock=clock, reconnect_min_s=1.0, **kw)
    return spool, pub, factory, clock


def connect(pub, factory):
    pub.pump()                  # 클라이언트 생성 + connect_async
    factory.last.fire_connect()
    pub.pump()                  # CONNACK 처리 + 전송


def payloads(client):
    return [p for _, _, p in client.published]


def test_requires_mtls_unless_plaintext_override(tmp_path):
    spool, pub, factory, _ = make(tmp_path, tls=TlsConfig())
    with pytest.raises(ValueError):
        pub.pump()


def test_tls_options_are_passed(tmp_path):
    tls = TlsConfig(ca_cert="ca.crt", client_cert="v.crt", client_key="v.key")
    _, pub, factory, _ = make(tmp_path, tls=tls)
    pub.pump()
    assert factory.last.tls["ca_certs"] == "ca.crt"
    assert factory.last.tls["certfile"] == "v.crt"
    assert factory.last.tls["keyfile"] == "v.key"
    assert factory.last.target == ("broker", 8883)


def test_not_removed_until_puback(tmp_path):
    spool, pub, factory, _ = make(tmp_path)
    spool.append("t", "A", "a", 1)
    connect(pub, factory)
    assert payloads(factory.last) == ["A"]
    # publish()는 성공했지만 PUBACK 전 — 메모리와 디스크 모두에 남아 있다
    assert len(spool) == 1
    assert len(Spool(spool.dir)) == 1
    mid = factory.last.published[0][0]
    factory.last.fire_puback(mid)
    pub.pump()
    assert len(spool) == 0 and len(Spool(spool.dir)) == 0
    assert pub.stats.acked == 1


def test_disconnected_messages_resent_in_timestamp_order(tmp_path):
    spool, pub, factory, clock = make(tmp_path)
    # 연결 없이 쌓인다 — 삽입 순서와 timestamp 순서를 일부러 다르게
    spool.append("t", "t3", "c", 3000)
    spool.append("t", "t1", "a", 1000)
    spool.append("t", "t2", "b", 2000)
    connect(pub, factory)
    assert payloads(factory.last) == ["t1", "t2", "t3"]


def test_unacked_resent_after_reconnect_and_only_acked_removed(tmp_path):
    spool, pub, factory, clock = make(tmp_path)
    for i in range(3):
        spool.append("t", f"m{i}", f"ts{i}", i)
    connect(pub, factory)
    first = factory.last
    first.fire_puback(first.published[0][0])  # m0만 PUBACK
    first.fire_disconnect()                     # m1, m2는 PUBACK 없이 끊김
    pub.pump()
    assert [r.payload for r in spool.pending()] == ["m1", "m2"]
    # 옛 클라이언트는 버렸다(paho 자체 재전송에 맡기지 않는다). disconnect가 먼저다.
    assert first.calls[0] == "disconnect"

    clock.t += 1.0  # backoff
    connect(pub, factory)
    second = factory.last
    assert second is not first
    assert payloads(second) == ["m1", "m2"]


def test_late_puback_from_old_client_is_ignored(tmp_path):
    """옛 클라이언트의 mid가 새 클라이언트 mid와 겹쳐도 잘못 지우지 않는다."""
    spool, pub, factory, clock = make(tmp_path)
    spool.append("t", "A", "a", 1)
    connect(pub, factory)
    old = factory.last
    old.fire_disconnect()
    pub.pump()
    clock.t += 1.0
    spool.append("t", "B", "b", 2)
    connect(pub, factory)
    new = factory.last
    assert payloads(new) == ["A", "B"]
    old.fire_puback(1)   # 옛 연결의 mid=1 — 새 연결에서도 mid=1은 "A"
    pub.pump()
    assert len(spool) == 2  # 무시됨


def test_new_messages_queue_behind_backlog(tmp_path):
    """재전송 중 새로 들어온 것은 backlog 뒤에 간다(timestamp가 더 크므로)."""
    spool, pub, factory, _ = make(tmp_path, max_inflight=2)
    for i in range(4):
        spool.append("t", f"old{i}", "x", i)
    connect(pub, factory)
    c = factory.last
    assert payloads(c) == ["old0", "old1"]  # inflight 창 2
    spool.append("t", "new", "y", 100)
    c.fire_puback(1)
    c.fire_puback(2)
    pub.pump()
    assert payloads(c) == ["old0", "old1", "old2", "old3"]
    c.fire_puback(3)
    pub.pump()
    assert payloads(c)[-1] == "new"


def test_publish_error_tears_down_and_keeps_spool(tmp_path):
    spool, pub, factory, clock = make(tmp_path)
    spool.append("t", "A", "a", 1)
    pub.pump()
    factory.last.publish_rc = 4  # MQTT_ERR_NO_CONN
    factory.last.fire_connect()
    pub.pump()
    assert not pub.connected and len(spool) == 1


def test_puback_timeout_reconnects_and_resends(tmp_path):
    spool, pub, factory, clock = make(tmp_path, ack_timeout_s=30)
    spool.append("t", "A", "a", 1)
    connect(pub, factory)
    clock.t += 31
    pub.pump()
    assert pub.stats.ack_timeouts == 1 and len(spool) == 1
    clock.t += 1
    connect(pub, factory)
    assert payloads(factory.last) == ["A"]


def test_broker_rejection_in_puback_is_not_an_ack(tmp_path):
    """MQTT v5에서 PUBACK reason code가 실패면(예: ACL) 지우지 않는다."""
    spool, pub, factory, _ = make(tmp_path)
    spool.append("t", "A", "a", 1)
    connect(pub, factory)

    class Rc:
        is_failure = True

    factory.last.fire_puback(1, rc=Rc())
    pub.pump()
    assert len(spool) == 1 and pub.stats.rejected_by_broker == 1
    pub.pump()
    assert payloads(factory.last) == ["A"]  # 이번 세션에선 재전송하지 않는다


# ── 세대·연결 실패·reason code ─────────────────────────────────
def test_second_disconnect_event_does_not_double_teardown(tmp_path):
    """같은 클라이언트의 disconnect가 두 번 와도(폐기 중 paho가 한 번 더 알림) 폐기·백오프는 한 번만."""
    spool, pub, factory, clock = make(tmp_path)
    spool.append("t", "A", "a", 1)
    connect(pub, factory)
    first = factory.last
    first.fire_disconnect()
    first.fire_disconnect()
    pub.pump()
    assert first.calls.count("disconnect") == 1
    clock.t += 1.0  # reconnect_min_s — 이중 폐기였다면 2.0을 기다려야 한다
    pub.pump()
    assert len(factory.clients) == 2


def test_connect_fail_tears_down_and_uses_own_backoff(tmp_path):
    """TCP/TLS 연결 실패(on_connect_fail)는 paho 재시도에 맡기지 않고 폐기 → 우리 백오프로 새 클라이언트."""
    spool, pub, factory, clock = make(tmp_path)
    spool.append("t", "A", "a", 1)
    pub.pump()
    first = factory.last
    first.fire_connect_fail()
    pub.pump()
    assert pub.stats.connect_failures == 1 and not pub.connected
    assert first.calls[0] == "disconnect"
    clock.t += 1.0
    pub.pump()
    assert len(factory.clients) == 2 and len(spool) == 1


def test_client_configured_for_logging_and_no_early_paho_retry(tmp_path):
    _, pub, factory, _ = make(tmp_path, reconnect_max_s=30.0)
    pub.pump()
    assert factory.last.logger is paho_log
    assert factory.last.reconnect_delay == (30, 30)
    assert factory.last.on_connect_fail is not None


@pytest.mark.parametrize("value,failed", [(0x00, False), (0x10, False), (0x80, True), (0x87, True)])
def test_is_failure_uses_0x80_boundary_with_real_reason_code(value, failed):
    rc = ReasonCode(PacketTypes.PUBACK, identifier=value)
    assert _is_failure(rc) is failed
    assert _is_failure(value) is failed  # int fallback도 같은 기준


def test_v5_no_matching_subscribers_puback_is_an_ack(tmp_path):
    """0x10 No matching subscribers는 브로커가 받은 것이다 — 지운다."""
    spool, pub, factory, _ = make(tmp_path)
    spool.append("t", "A", "a", 1)
    connect(pub, factory)
    factory.last.fire_puback(1, rc=ReasonCode(PacketTypes.PUBACK, identifier=0x10))
    pub.pump()
    assert len(spool) == 0 and pub.stats.acked == 1 and pub.stats.rejected_by_broker == 0
