"""spool의 fsync 순서, 재시작 후 보존, 잘린 꼬리 복구."""
import os

import pytest

from obd_bridge.spool import ACKS, RECORDS, Spool, SpoolCorrupted


class OpLog:
    """fsync 호출을 파일 이름과 함께 기록한다. 쓰기 순서는 fsync 시점의 파일 크기로 확인한다."""

    def __init__(self):
        self.events = []

    def fsync(self, fd):
        path = _fd_path(fd)
        self.events.append(("fsync", os.path.basename(path), os.fstat(fd).st_size))
        os.fsync(fd)

    def fsync_dir(self, path):
        self.events.append(("fsync_dir", os.path.basename(path)))
        return True


def _fd_path(fd):
    # 테스트용: 열린 fd의 경로를 찾는다(spool 디렉터리 안 파일만 대상)
    for name in OpLog.watch_dir_files():
        if os.path.samestat(os.fstat(fd), os.stat(name)):
            return name
    return "?"


def _make(tmp_path, **kw):
    ops = OpLog()
    d = str(tmp_path / "spool")
    os.makedirs(d, exist_ok=True)
    OpLog.watch_dir_files = staticmethod(lambda: [os.path.join(d, n) for n in os.listdir(d)])
    return Spool(d, fsync=ops.fsync, fsync_dir=ops.fsync_dir, **kw), ops, d


def test_first_append_fsyncs_file_then_directory(tmp_path):
    spool, ops, d = _make(tmp_path)
    spool.append("t", '{"a":1}', "2026-10-06T00:00:00.000Z", 1)
    size = os.path.getsize(os.path.join(d, RECORDS))
    # 파일 fsync 시점에 이미 내용 전체가 들어가 있고(write+flush 뒤), 그 다음 디렉터리 fsync
    assert ops.events == [("fsync", RECORDS, size), ("fsync_dir", "spool")]


def test_later_appends_fsync_file_only(tmp_path):
    spool, ops, _ = _make(tmp_path)
    spool.append("t", "1", "a", 1)
    ops.events.clear()
    spool.append("t", "2", "b", 2)
    assert [e[0] for e in ops.events] == ["fsync"]


def test_append_returns_only_after_fsync(tmp_path):
    """append가 돌려준 시점에 이미 fsync가 끝났다 — 돌려받은 뒤에만 publish한다."""
    spool, ops, _ = _make(tmp_path)
    rec = spool.append("t", "1", "a", 1)
    assert ops.events and ops.events[0][0] == "fsync"
    assert rec.seq == 0


def test_ack_is_appended_and_fsynced(tmp_path):
    spool, ops, d = _make(tmp_path)
    r = spool.append("t", "1", "a", 1)
    ops.events.clear()
    assert spool.ack(r.seq)
    assert ops.events == [("fsync", ACKS, os.path.getsize(os.path.join(d, ACKS))),
                          ("fsync_dir", "spool")]
    assert open(os.path.join(d, ACKS)).read() == "0\n"


def test_compaction_order_records_before_acks(tmp_path):
    """임시 파일 fsync → rename → dir fsync → 그 다음에야 acks를 비운다."""
    spool, ops, d = _make(tmp_path, compact_min_acked=10**9)
    for i in range(3):
        spool.append("t", str(i), f"ts{i}", i)
    spool.ack(0)
    spool.ack(1)
    ops.events.clear()
    spool.compact()
    kinds = [(e[0], e[1]) for e in ops.events]
    assert kinds == [("fsync", RECORDS + ".tmp"),  # 미전송분을 임시 파일에 쓰고 fsync
                     ("fsync_dir", "spool"),        # records.log로 rename 확정
                     ("fsync", ACKS + ".tmp"),      # 빈 acks
                     ("fsync_dir", "spool")]        # acks.log 교체 확정
    assert [r.seq for r in Spool(d).pending()] == [2]


def test_records_survive_restart_until_acked(tmp_path):
    spool, _, d = _make(tmp_path)
    a = spool.append("t", "A", "x", 10)
    spool.append("t", "B", "y", 20)
    again = Spool(d)
    assert [r.payload for r in again.pending()] == ["A", "B"]
    again.ack(a.seq)
    assert [r.payload for r in Spool(d).pending()] == ["B"]


def test_pending_is_in_timestamp_order_not_insert_order(tmp_path):
    spool, _, _ = _make(tmp_path)
    spool.append("t", "late", "c", 300)
    spool.append("t", "early", "a", 100)
    spool.append("t", "mid", "b", 200)
    spool.append("t", "mid2", "b2", 200)  # 같은 timestamp는 seq(삽입) 순
    assert [r.payload for r in spool.pending()] == ["early", "mid", "mid2", "late"]


def test_torn_tail_is_truncated_not_kept(tmp_path):
    spool, _, d = _make(tmp_path)
    spool.append("t", "ok", "a", 1)
    with open(os.path.join(d, RECORDS), "ab") as f:
        f.write(b'{"seq":1,"ts_ms":2,"ts":"b","to')  # 쓰다 만 줄(개행 없음)
    again = Spool(d)
    assert [r.payload for r in again.pending()] == ["ok"]
    # 잘린 꼬리를 지웠으므로 다음 추가가 깨진 줄에 붙지 않는다
    again.append("t", "next", "c", 3)
    assert [r.payload for r in Spool(d).pending()] == ["ok", "next"]


def test_corrupt_middle_line_raises(tmp_path):
    spool, _, d = _make(tmp_path)
    spool.append("t", "ok", "a", 1)
    with open(os.path.join(d, RECORDS), "ab") as f:
        f.write(b"garbage\n")
    spool2 = None
    with pytest.raises(SpoolCorrupted):
        spool2 = Spool(d)
    assert spool2 is None


def test_seq_not_reused_after_crash_mid_compaction(tmp_path):
    """records는 비웠는데 acks를 비우기 전에 죽은 상황 — 옛 ack가 새 레코드를 지우면 안 된다."""
    d = tmp_path / "spool"
    d.mkdir()
    (d / RECORDS).write_text("")
    (d / ACKS).write_text("0\n1\n2\n")
    spool = Spool(str(d))
    rec = spool.append("t", "new", "a", 1)
    assert rec.seq == 3
    assert [r.payload for r in Spool(str(d)).pending()] == ["new"]
