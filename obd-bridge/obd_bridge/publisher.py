"""spool에서 꺼내 MQTT QoS 1로 보내고, PUBACK을 받은 것만 spool에서 지운다.

불변식 — 지우면 안 되는 이유:
- ``publish()``의 반환 성공은 "paho 송신 큐에 넣었다"일 뿐이다. **PUBACK(on_publish)만이**
  브로커가 받았다는 증거다(simulator `PublishStats`와 같은 구분).
- on_publish는 paho 네트워크 스레드에서 **paho 내부 mutex를 쥔 채** 불린다. 여기서 우리 lock을
  잡으면 publish()를 부르는 메인 스레드와 교착할 수 있어, 콜백은 큐에 넣기만 하고
  메인 스레드 ``pump()``가 처리한다.
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
import time
from dataclasses import dataclass
from typing import Any, Callable, Optional

from .spool import Spool

log = logging.getLogger("obd_bridge.publisher")

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


def make_paho_client(client_id: str, protocol: int):
    import paho.mqtt.client as mqtt
    return mqtt.Client(callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
                       client_id=client_id, protocol=protocol,
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

        client.on_connect = on_connect
        client.on_disconnect = on_disconnect
        client.on_publish = on_publish

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
        self._client = client
        client.connect_async(self.host, self.port, keepalive=30)
        client.loop_start()

    def _teardown(self, reason: str) -> None:
        if self._client is not None:
            log.warning("MQTT 클라이언트 폐기(%s) — 미확인 %d건은 spool에 남아 재전송된다",
                        reason, len(self._inflight))
            try:
                self._client.loop_stop()
                self._client.disconnect()
            except Exception:  # 이미 끊긴 소켓 정리 실패는 무시한다
                pass
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

    def close(self) -> None:
        if self._client is not None:
            try:
                self._client.loop_stop()
                self._client.disconnect()
            except Exception:
                pass
        self._client = None
        self._connected = False


def _is_failure(reason_code: Any) -> bool:
    if reason_code is None:
        return False
    if hasattr(reason_code, "is_failure"):
        return bool(reason_code.is_failure)
    try:
        return int(reason_code) != 0
    except (TypeError, ValueError):
        return False
