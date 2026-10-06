"""spool에서 꺼내 MQTT QoS 1로 보내고, PUBACK을 받은 것만 spool에서 지운다.

불변식 — 지우면 안 되는 이유:
- ``publish()``의 반환 성공은 "paho 송신 큐에 넣었다"일 뿐이다. **PUBACK(on_publish)만이**
  브로커가 받았다는 증거다(simulator `PublishStats`와 같은 구분).
- on_publish(PUBACK)는 paho 네트워크 스레드에서 **paho의 ``_out_message_mutex``를 쥔 채**
  불린다(paho-mqtt 2.1.0 ``_handle_pubackcomp`` → ``_do_on_publish``). ``publish()``도 같은
  mutex를 잡는다. 콜백에서 우리 lock을 잡으면, 우리 lock을 쥐고 publish()를 부르는 스레드와
  교착할 수 있어 콜백은 큐에 넣기만 하고 폴링 스레드의 ``pump()``가 처리한다.
- 클라이언트를 버릴 때 **``disconnect()``를 먼저, ``loop_stop()``(join)은 폴링 스레드 밖에서** 한다.
  paho 2.1.0의 ``loop_forever``는 terminate 요청을 받아도 ``_out_messages``(미확인 QoS 1)가
  빌 때까지 돌고 ``loop_stop``은 timeout 없이 join한다 — PUBACK이 안 오는데 TCP는 살아 있으면
  폴링 스레드가 무기한 멈춘다. ``disconnect()``가 DISCONNECT를 쓰면 paho가 소켓을 닫고 루프가
  끝난다. 쓰지 못하는 경우(송신 버퍼가 막힘)에도 상태가 DISCONNECTING이라 keepalive 경과 시
  소켓을 닫는다. 어느 쪽이든 폴링 스레드는 기다리지 않는다.
- paho 자체 재접속은 쓰지 않는다(``reconnect_on_failure=False`` + ``on_connect_fail`` → 폐기).
  재시도 간격은 이 클래스의 백오프만 정한다.
- 연결이 끊기면 그 클라이언트는 **버린다**. paho는 미확인 QoS 1을 자체 큐에 들고 있다가
  재연결 때 다시 보내는데, 우리도 spool에서 다시 보내면 순서와 중복을 둘 다 통제할 수 없다.
  새 클라이언트로 spool을 timestamp 순서대로 다시 보낸다. 재전송이므로 at-least-once —
  같은 메시지가 두 번 갈 수 있다(InfluxDB는 같은 vehicle_id+timestamp를 덮어쓴다).
- 옛 클라이언트의 늦은 PUBACK은 세대(generation)로 걸러낸다 — 새 클라이언트의 mid와
  번호가 겹칠 수 있다.
"""
from __future__ import annotations

import logging
import queue
import threading
import time
from dataclasses import dataclass
from typing import Any, Callable, Optional

from .spool import Spool

log = logging.getLogger("obd_bridge.publisher")
paho_log = logging.getLogger("obd_bridge.paho")

MQTT_ERR_SUCCESS = 0


@dataclass
class TlsConfig:
    ca_cert: Optional[str] = None
    client_cert: Optional[str] = None
    client_key: Optional[str] = None
    insecure_plaintext: bool = False  # dev override에서만. 기본은 mTLS


@dataclass
class PublisherStats:
    published: int = 0          # publish() 성공 반환(큐 투입)
    acked: int = 0              # PUBACK → spool 삭제
    rejected_by_broker: int = 0  # MQTT v5 PUBACK 실패 reason code
    reconnects: int = 0
    ack_timeouts: int = 0
    connect_failures: int = 0    # TCP/TLS 연결 실패(on_connect_fail) — paho가 조용히 재시도하지 않는다


def make_paho_client(client_id: str, protocol: int):
    import paho.mqtt.client as mqtt
    # reconnect_on_failure=False — 끊긴 뒤 paho가 같은 클라이언트로 재접속하지 않는다(모듈 docstring).
    return mqtt.Client(callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
                       client_id=client_id, protocol=protocol, reconnect_on_failure=False,
                       **({"clean_session": True} if protocol != mqtt.MQTTv5 else {}))


class SpoolPublisher:
    def __init__(self, spool: Spool, *, host: str, port: int, client_id: str,
                 tls: TlsConfig, client_factory: Callable[[], Any],
                 max_inflight: int = 20, ack_timeout_s: float = 30.0,
                 reconnect_min_s: float = 1.0, reconnect_max_s: float = 30.0,
                 clock: Callable[[], float] = time.monotonic):
        self.spool = spool
        self.host, self.port, self.client_id = host, port, client_id
        self.tls = tls
        self._factory = client_factory
        self.max_inflight = max_inflight
        self.ack_timeout_s = ack_timeout_s
        self._backoff_min, self._backoff_max = reconnect_min_s, reconnect_max_s
        self._backoff = reconnect_min_s
        self._clock = clock

        self.stats = PublisherStats()
        self._events: "queue.Queue[tuple]" = queue.Queue()
        self._client: Any = None
        self._gen = 0
        self._connected = False
        self._next_attempt = 0.0
        self._inflight: dict[int, tuple[int, float]] = {}  # mid → (seq, 보낸 시각)
        self._rejected: set[int] = set()  # 브로커가 거부한 seq — 이번 세션엔 재전송 안 함, 디스크엔 남김

    # ── 콜백(paho 네트워크 스레드) — 큐에만 넣는다 ─────────────────
    def _bind(self, client: Any, gen: int) -> None:
        def on_connect(c, userdata, flags, reason_code, properties=None):
            self._events.put(("connect", gen, _is_failure(reason_code)))

        def on_disconnect(c, userdata, flags, reason_code=None, properties=None):
            self._events.put(("disconnect", gen, None))

        def on_publish(c, userdata, mid, reason_code=None, properties=None):
            self._events.put(("puback", gen, (mid, _is_failure(reason_code))))

        def on_connect_fail(c, userdata):
            self._events.put(("connect_fail", gen, None))

        client.on_connect = on_connect
        client.on_disconnect = on_disconnect
        client.on_publish = on_publish
        client.on_connect_fail = on_connect_fail

    # ── 메인 스레드 ─────────────────────────────────────
    @property
    def connected(self) -> bool:
        return self._connected

    def inflight_seqs(self) -> list[int]:
        return [seq for seq, _ in self._inflight.values()]

    def _start_client(self) -> None:
        self._gen += 1
        client = self._factory()
        self._bind(client, self._gen)
        if not self.tls.insecure_plaintext:
            if not (self.tls.ca_cert and self.tls.client_cert and self.tls.client_key):
                raise ValueError("mTLS 설정이 없다 — 평문은 명시적 dev override에서만 허용한다")
            import ssl
            client.tls_set(ca_certs=self.tls.ca_cert, certfile=self.tls.client_cert,
                           keyfile=self.tls.client_key, tls_version=ssl.PROTOCOL_TLSv1_2)
        client.enable_logger(paho_log)
        # 첫 연결 재시도(loop_forever(retry_first_connection=True))는 reconnect_on_failure와 무관하다.
        # on_connect_fail로 곧바로 폐기하지만, 그 전에 paho가 먼저 재시도하지 않도록 대기를 최대로 둔다.
        delay = max(1, int(self._backoff_max))
        client.reconnect_delay_set(min_delay=delay, max_delay=delay)
        self._client = client
        client.connect_async(self.host, self.port, keepalive=30)
        client.loop_start()

    def _discard(self, client: Any) -> threading.Thread:
        """폴링 스레드를 막지 않고 클라이언트를 버린다. 반환한 reaper 스레드가 join을 맡는다."""
        try:
            client.disconnect()  # 먼저 — 이것이 paho 루프를 끝낸다(모듈 docstring)
        except Exception:  # 이미 끊긴 소켓 정리 실패는 무시한다
            pass

        def reap():
            try:
                client.loop_stop()  # timeout 없는 join — 그래서 폴링 스레드에서 부르지 않는다
            except Exception:
                pass

        t = threading.Thread(target=reap, name="obd-bridge-mqtt-reaper", daemon=True)
        t.start()
        return t

    def _teardown(self, reason: str) -> None:
        # 세대를 올린다 — 버린 클라이언트가 뒤늦게 보내는 disconnect 등이 두 번째 폐기·백오프를 일으키지 않게.
        self._gen += 1
        if self._client is not None:
            log.warning("MQTT 클라이언트 폐기(%s) — 미확인 %d건은 spool에 남아 재전송된다",
                        reason, len(self._inflight))
            self._discard(self._client)
        self._client = None
        self._connected = False
        self._inflight.clear()
        self._next_attempt = self._clock() + self._backoff
        self._backoff = min(self._backoff * 2, self._backoff_max)

    def _drain_events(self) -> None:
        while True:
            try:
                kind, gen, data = self._events.get_nowait()
            except queue.Empty:
                return
            if gen != self._gen:
                continue  # 옛 클라이언트의 늦은 이벤트
            if kind == "connect":
                if data:
                    self._teardown("CONNACK 실패")
                else:
                    self._connected = True
                    self._backoff = self._backoff_min
                    self.stats.reconnects += 1
            elif kind == "disconnect":
                self._teardown("연결 끊김")
            elif kind == "connect_fail":
                self.stats.connect_failures += 1
                self._teardown("연결 실패(TCP/TLS)")
            elif kind == "puback":
                mid, failed = data
                entry = self._inflight.pop(mid, None)
                if entry is None:
                    continue
                seq, _ = entry
                if failed:
                    # MQTT v5 브로커가 PUBACK에 실패 사유를 실었다(예: ACL 거부).
                    # 받은 게 아니므로 지우지 않는다. 처리 방식은 결정 대기(README).
                    self.stats.rejected_by_broker += 1
                    self._rejected.add(seq)
                    log.error("브로커가 seq=%d를 거부했다 — spool에 남긴다", seq)
                    continue
                if self.spool.ack(seq):
                    self.stats.acked += 1

    def pump(self) -> None:
        """이벤트를 처리하고, 연결돼 있으면 spool의 미전송분을 timestamp 순서대로 보낸다."""
        self._drain_events()
        now = self._clock()

        if self._client is None:
            if now >= self._next_attempt:
                try:
                    self._start_client()
                except ValueError:
                    raise
                except Exception as e:
                    log.warning("MQTT 연결 시작 실패: %s", e)
                    self._client = None
                    self._next_attempt = now + self._backoff
                    self._backoff = min(self._backoff * 2, self._backoff_max)
            return
        if not self._connected:
            return

        # PUBACK이 너무 오래 안 오면 연결을 버리고 처음부터 다시 보낸다.
        if any(now - sent > self.ack_timeout_s for _, sent in self._inflight.values()):
            self.stats.ack_timeouts += 1
            self._teardown("PUBACK timeout")
            return

        busy = set(self.inflight_seqs()) | self._rejected
        for rec in self.spool.pending():
            if len(self._inflight) >= self.max_inflight:
                break
            if rec.seq in busy:
                continue
            info = self._client.publish(rec.topic, rec.payload, qos=1)
            if info.rc != MQTT_ERR_SUCCESS:
                self._teardown(f"publish rc={info.rc}")
                return
            self._inflight[info.mid] = (rec.seq, now)
            self.stats.published += 1

    def close(self, timeout_s: float = 2.0) -> None:
        """종료. DISCONNECT를 보낼 시간을 최대 ``timeout_s`` 주고, 넘으면 기다리지 않는다
        (paho 스레드는 daemon이다). 미확인분은 spool에 남는다."""
        self._gen += 1
        if self._client is not None:
            reaper = self._discard(self._client)
            reaper.join(timeout_s)
            if reaper.is_alive():
                log.warning("MQTT 네트워크 스레드가 %.1fs 안에 끝나지 않았다 — 기다리지 않고 종료한다",
                            timeout_s)
        self._client = None
        self._connected = False


def _is_failure(reason_code: Any) -> bool:
    if reason_code is None:
        return False
    if hasattr(reason_code, "is_failure"):
        return bool(reason_code.is_failure)
    try:
        # ReasonCode.is_failure와 같은 기준. v5 0x10(no matching subscribers)은 성공이다.
        return int(reason_code) >= 0x80
    except (TypeError, ValueError):
        return False
