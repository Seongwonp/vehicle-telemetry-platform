"""PUBACK 뒤에만 지우고, 재연결 시 timestamp 순서로 다시 보낸다."""
import pytest

from conftest import ClientFactory, FakeClock
from obd_bridge.publisher import SpoolPublisher, TlsConfig
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
    assert first.stopped  # 옛 클라이언트는 버렸다(paho 자체 재전송에 맡기지 않는다)

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
