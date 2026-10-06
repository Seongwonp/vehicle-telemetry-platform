"""PUBACK 전 메시지를 디스크에 남기는 append-only spool.

파일 두 개를 쓴다. 둘 다 **추가만** 한다.

- ``records.log`` — 한 줄에 레코드 하나 ``{"seq", "ts_ms", "ts", "topic", "payload"}``
- ``acks.log``    — 한 줄에 PUBACK 받은 seq 하나

미전송 = records − acks. **acks.log에는 PUBACK 콜백에서만 쓴다** — 그 전에는 지우지 않는다.

내구성 순서(지우면 안 되는 이유):
1. 새 파일을 만들면 내용 fsync 뒤 **디렉터리도 fsync**한다. 안 하면 전원 차단에서 파일 내용은
   있어도 디렉터리 엔트리가 없어 파일째 사라질 수 있다(POSIX).
2. 매 추가는 write → flush → ``os.fsync``. 반환되면 디스크에 있다고 본다.
3. compaction은 임시 파일에 미전송분을 쓰고 fsync → ``os.replace`` → 디렉터리 fsync →
   그 뒤에야 acks.log를 비운다. 순서를 뒤집으면 acks만 비고 records는 옛 것이 남아
   **이미 받은 메시지를 다시 보낸다**(중복). 지금 순서에서 중간 크래시의 결과는
   "acks에 없는 seq가 남는 것"뿐이라 무해하다.
4. 마지막 줄이 잘려 있으면(쓰는 중 크래시) 그 줄만 잘라내고 fsync한다. 중간 줄이 깨진 것은
   잘림이 아니라 손상이라 조용히 버리지 않고 예외를 던진다.

Windows는 디렉터리 핸들 fsync를 지원하지 않는다 — ``fsync_dir``이 False를 돌려주고
그 사실을 README에 적었다. NTFS 메타데이터 저널에 기댄다(검증하지 않았다).
"""
from __future__ import annotations

import json
import os
import threading
from dataclasses import dataclass
from typing import Callable, Optional

RECORDS = "records.log"
ACKS = "acks.log"


def _real_fsync_dir(path: str) -> bool:
    if os.name == "nt":
        return False
    fd = os.open(path, os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)
    return True


@dataclass(frozen=True)
class SpoolRecord:
    seq: int
    ts_ms: int
    ts: str
    topic: str
    payload: str


class SpoolCorrupted(RuntimeError):
    pass


class Spool:
    def __init__(self, directory: str, *,
                 fsync: Callable[[int], None] = os.fsync,
                 fsync_dir: Callable[[str], bool] = _real_fsync_dir,
                 compact_min_acked: int = 1000):
        self.dir = directory
        self._fsync = fsync
        self._fsync_dir = fsync_dir
        self._compact_min_acked = compact_min_acked
        self._lock = threading.Lock()
        os.makedirs(directory, exist_ok=True)
        self._pending: dict[int, SpoolRecord] = {}
        self._acked_since_compact = 0
        self._next_seq = 0
        self._load()

    # ── 경로 ─────────────────────────────────────────────
    def _p(self, name: str) -> str:
        return os.path.join(self.dir, name)

    # ── 적재 ─────────────────────────────────────────────
    def _read_lines(self, name: str) -> list[str]:
        path = self._p(name)
        if not os.path.exists(path):
            return []
        with open(path, "rb") as f:
            data = f.read()
        good_end = data.rfind(b"\n") + 1
        lines = data[:good_end].decode("utf-8").splitlines()
        if good_end < len(data):
            # 마지막 줄이 개행 없이 끝났다 = 쓰는 중 크래시. 그 줄은 fsync가 끝나지 않았으므로
            # 호출자에게 "저장됐다"고 돌려준 적이 없다. 잘라내도 약속을 어기지 않는다.
            with open(path, "r+b") as f:
                f.truncate(good_end)
                f.flush()
                self._fsync(f.fileno())
        return lines

    def _load(self) -> None:
        records: dict[int, SpoolRecord] = {}
        max_seq = -1
        for i, line in enumerate(self._read_lines(RECORDS)):
            try:
                d = json.loads(line)
                rec = SpoolRecord(int(d["seq"]), int(d["ts_ms"]), d["ts"], d["topic"], d["payload"])
            except (ValueError, KeyError, TypeError) as e:
                raise SpoolCorrupted(f"{RECORDS} line {i + 1}: {e}") from None
            records[rec.seq] = rec
            max_seq = max(max_seq, rec.seq)
        for i, line in enumerate(self._read_lines(ACKS)):
            try:
                seq = int(line)
            except ValueError:
                raise SpoolCorrupted(f"{ACKS} line {i + 1}") from None
            records.pop(seq, None)
            # compaction 중간 크래시로 acks에 옛 seq가 남았을 수 있다. seq를 그보다 크게 잡아야
            # 새 레코드가 옛 ack에 잘못 지워지지 않는다.
            max_seq = max(max_seq, seq)
        self._pending = records
        self._next_seq = max_seq + 1

    # ── 쓰기 ─────────────────────────────────────────────
    def _append_line(self, name: str, line: str) -> None:
        path = self._p(name)
        created = not os.path.exists(path)
        with open(path, "ab") as f:
            f.write(line.encode("utf-8") + b"\n")
            f.flush()
            self._fsync(f.fileno())
        if created:
            self._fsync_dir(self.dir)

    def append(self, topic: str, payload: str, ts: str, ts_ms: int) -> SpoolRecord:
        """디스크에 내구성 있게 쓴 뒤에 돌려준다. 돌려받기 전에는 publish하지 않는다."""
        with self._lock:
            rec = SpoolRecord(self._next_seq, ts_ms, ts, topic, payload)
            self._append_line(RECORDS, json.dumps(
                {"seq": rec.seq, "ts_ms": rec.ts_ms, "ts": rec.ts,
                 "topic": rec.topic, "payload": rec.payload},
                ensure_ascii=False, separators=(",", ":")))
            self._next_seq += 1
            self._pending[rec.seq] = rec
            return rec

    def ack(self, seq: int) -> bool:
        """PUBACK을 받은 seq를 지운다. **PUBACK 콜백 경로에서만 부른다.**"""
        with self._lock:
            if seq not in self._pending:
                return False
            self._append_line(ACKS, str(seq))
            del self._pending[seq]
            self._acked_since_compact += 1
            if self._acked_since_compact >= self._compact_min_acked:
                self._compact_locked()
            return True

    def compact(self) -> None:
        with self._lock:
            self._compact_locked()

    def _compact_locked(self) -> None:
        tmp = self._p(RECORDS + ".tmp")
        with open(tmp, "wb") as f:
            for rec in sorted(self._pending.values(), key=lambda r: r.seq):
                f.write(json.dumps(
                    {"seq": rec.seq, "ts_ms": rec.ts_ms, "ts": rec.ts,
                     "topic": rec.topic, "payload": rec.payload},
                    ensure_ascii=False, separators=(",", ":")).encode("utf-8") + b"\n")
            f.flush()
            self._fsync(f.fileno())
        os.replace(tmp, self._p(RECORDS))
        self._fsync_dir(self.dir)
        # records가 확정된 **뒤에** acks를 비운다(모듈 docstring 3번).
        acks_tmp = self._p(ACKS + ".tmp")
        with open(acks_tmp, "wb") as f:
            f.flush()
            self._fsync(f.fileno())
        os.replace(acks_tmp, self._p(ACKS))
        self._fsync_dir(self.dir)
        self._acked_since_compact = 0

    # ── 읽기 ─────────────────────────────────────────────
    def pending(self) -> list[SpoolRecord]:
        """미전송분을 **원래 timestamp 순서**로(같으면 seq 순)."""
        with self._lock:
            return sorted(self._pending.values(), key=lambda r: (r.ts_ms, r.seq))

    def __len__(self) -> int:
        with self._lock:
            return len(self._pending)

    def get(self, seq: int) -> Optional[SpoolRecord]:
        with self._lock:
            return self._pending.get(seq)
