"""自测：写盘期间取消上传的两种时序，以及同片号重试的一致性。

覆盖修复点：
- 取消发生在覆盖提交前：旧片与旧游标仍然有效；同片号重试立即成功且无需等待
  前一次写盘收尾；前次迟到的提交被作废，回看读到的始终是重试的字节；
  临时文件被回收。
- 取消发生在提交中（shield 内）：覆盖完整生效，最新片号同步推进，旧游标
  收到 410 断档；同片号重试得到 409（片已入库）。
- 迟到写入绝不会改掉重试成功后的回看内容（本 bug 的核心回归用例）。
- 临时文件在取消、写盘失败、迟到提交作废、进程重启等路径下均被回收。

本模块不依赖 pytest-asyncio：每个用例在函数体内用 asyncio.run 驱动协程。
"""
from __future__ import annotations

import asyncio

import pytest

from app.store import (
    Cursor,
    Gap,
    NonContiguous,
    ReadOk,
    SegmentStore,
    StaleAttempt,
    Wait,
)

SLOT_COUNT = 4
SEG = 100 * 1024


def make_store(tmp_path, **kwargs) -> SegmentStore:
    return SegmentStore(
        data_dir=str(tmp_path / "data"),
        slot_count=SLOT_COUNT,
        max_segment_bytes=16 * 1024 * 1024,
        max_read_bytes=64 * 1024,
        **kwargs,
    )


def seg_bytes(seq: int, size: int = SEG) -> bytes:
    return bytes([seq % 256]) * size


def list_files(store: SegmentStore) -> list[str]:
    return sorted(p.name for p in store.data_dir.iterdir())


def slot_files_only(store: SegmentStore) -> bool:
    return all(name.endswith(".seg") for name in list_files(store))


async def _read_full_segment(store: SegmentStore, seq: int) -> bytes:
    """按片号起播并精确读完 seq 这一片，返回该片拼起来的原始字节。"""
    result = await store.start(seq)
    assert not isinstance(result, (Gap, Wait))
    cursor = result.cursor
    buf = bytearray()
    while True:
        r = await store.read(cursor.encode(), 64 * 1024)
        assert isinstance(r, ReadOk)
        buf += r.data
        cursor = r.next_cursor
        if r.offset + len(r.data) >= r.segment_size:
            return bytes(buf)


def read_full_segment(store: SegmentStore, seq: int) -> bytes:
    return asyncio.run(_read_full_segment(store, seq))


# ---- 时序 A：取消发生在提交前（临时文件写盘阶段） ----


def test_cancel_before_commit_keeps_old_segment_and_cursor(tmp_path):
    async def scenario():
        store = make_store(tmp_path, put_write_delay=0.3)
        # 写满 4 槽，片 1..4，片 1 独立可解码。
        for s in range(1, 5):
            await store.put(s, independent=(s == 1), data=seg_bytes(s))
        # 旧游标：片 1（槽 0）读了一半。
        start = await store.start(1)
        half = await store.read(start.cursor.encode(), 64 * 1024)
        half_cursor = half.next_cursor

        # 片 5 将覆盖槽 0 的片 1；在临时文件写盘阶段取消。
        task = asyncio.create_task(store.put(5, False, seg_bytes(5)))
        await asyncio.sleep(0.1)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

        # 给僵尸写盘线程一点时间"迟到"完成——它只能写临时文件，碰不到槽文件。
        await asyncio.sleep(0.5)
        await store.drain_background()

        # 覆盖未提交：最新片号仍是 4，槽 0 仍是片 1。
        snap = await store.snapshot()
        assert snap["newest_seq"] == 4
        assert snap["slots"][0]["seq"] == 1
        # 旧游标仍然有效，读回的还是片 1 的字节（绝不是片 5 的）。
        r = await store.read(half_cursor.encode(), 64 * 1024)
        assert not isinstance(r, Gap)
        assert r.data == seg_bytes(1)[64 * 1024 : 64 * 1024 + len(r.data)]
        # 临时文件已回收，磁盘上只剩槽位正式文件。
        assert slot_files_only(store)
        assert len(list_files(store)) == SLOT_COUNT

    asyncio.run(scenario())


def test_retry_after_cancel_before_commit_succeeds_and_wins(tmp_path):
    async def scenario():
        store = make_store(tmp_path, put_write_delay=0.3)
        for s in range(1, 5):
            await store.put(s, independent=(s == 1), data=seg_bytes(s))

        # 第一次上传片 5：写盘阶段取消（旧片 1 仍在槽 0）。
        task = asyncio.create_task(store.put(5, False, seg_bytes(5)))
        await asyncio.sleep(0.1)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

        # 同片号重试：无需等待前一次写盘收尾，立即成功并完成覆盖。
        result = await store.put(5, True, seg_bytes(5))
        assert result.seq == 5 and result.slot == 0
        snap = await store.snapshot()
        assert snap["newest_seq"] == 5 and snap["oldest_seq"] == 2

        # 等第一次尝试的僵尸线程彻底结束，确认它没有改掉任何东西。
        await asyncio.sleep(0.5)
        await store.drain_background()
        snap = await store.snapshot()
        assert snap["slots"][0]["seq"] == 5
        assert await _read_full_segment(store, 5) == seg_bytes(5)

        # 片号已推进：同片号再传一次 -> 409 不连续。
        with pytest.raises(NonContiguous):
            await store.put(5, False, seg_bytes(5))
        # 旧游标（片 1，槽 0 旧代次）-> 断档，且读不到片 5 的字节。
        old_cursor = Cursor(slot=0, generation=1, seq=1, offset=0)
        r = await store.read(old_cursor.encode(), 64 * 1024)
        assert isinstance(r, Gap)
        assert r.earliest_playable == 5
        assert slot_files_only(store)

    asyncio.run(scenario())


def test_late_write_never_clobbers_successful_retry(tmp_path):
    """核心回归：重试成功后，前次被取消的写盘迟到，也改不掉回看字节。"""

    async def scenario():
        store = make_store(tmp_path, put_write_delay=0.5)
        for s in range(1, 5):
            await store.put(s, independent=(s == 1), data=seg_bytes(s))

        task = asyncio.create_task(store.put(5, False, seg_bytes(5)))
        await asyncio.sleep(0.1)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

        # 重试成功（不等待前一次写盘）。
        await store.put(5, True, seg_bytes(5))
        # 此时前一次的写盘线程可能还在跑；等它结束并完成回收。
        await asyncio.sleep(1.0)
        await store.drain_background()

        # 槽 0 内容必须始终是重试入库的片 5。
        assert await _read_full_segment(store, 5) == seg_bytes(5)
        snap = await store.snapshot()
        assert snap["slots"][0]["seq"] == 5
        assert snap["slots"][0]["independent"] is True
        assert slot_files_only(store)

    asyncio.run(scenario())


# ---- 时序 B：取消发生在提交中（shield 保护，提交完整生效） ----


def test_cancel_during_commit_takes_effect_and_gaps_old_cursor(tmp_path):
    async def scenario():
        store = make_store(tmp_path, put_commit_delay=0.5)
        # 片 1、4 标为独立片，便于断档后 earliest_playable 的断言。
        for s in range(1, 5):
            await store.put(s, independent=(s in (1, 4)), data=seg_bytes(s))
        start = await store.start(1)
        half = await store.read(start.cursor.encode(), 64 * 1024)
        half_cursor = half.next_cursor

        # 片 5 覆盖槽 0 的片 1；提交在 shield 内进行时取消等待协程。
        task = asyncio.create_task(store.put(5, True, seg_bytes(5)))
        await asyncio.sleep(0.2)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

        # 提交已完整生效：最新片号同步推进到 5。
        snap = await store.snapshot()
        assert snap["newest_seq"] == 5
        assert snap["slots"][0]["seq"] == 5
        assert snap["slots"][0]["generation"] == 2
        # 旧游标立刻收到断档（代次不匹配），不会读到片 5 的字节。
        r = await store.read(half_cursor.encode(), 64 * 1024)
        assert isinstance(r, Gap)
        assert r.earliest_playable == 4
        # 片 5 已入库：同片号重试 -> 409；片 6 可以正常续写。
        with pytest.raises(NonContiguous):
            await store.put(5, False, seg_bytes(5))
        await store.put(6, False, seg_bytes(6))
        # 片 5 的回看字节就是这次上传的内容。
        assert await _read_full_segment(store, 5) == seg_bytes(5)
        assert slot_files_only(store)

    asyncio.run(scenario())


# ---- 僵尸提交作废与临时文件回收 ----


def test_stale_commit_is_discarded_and_tmp_reclaimed(tmp_path):
    async def scenario():
        store = make_store(tmp_path)
        for s in range(1, 5):
            await store.put(s, independent=True, data=seg_bytes(s))

        # 尝试 A 在写盘阶段被取消（领取了槽 0 的代次令牌 2），尝试 B 同片号
        # 重试成功并领到令牌 3、完成覆盖。
        store.put_write_delay = 0.3
        t_a = asyncio.create_task(store.put(5, False, seg_bytes(5)))
        await asyncio.sleep(0.1)
        t_a.cancel()
        with pytest.raises(asyncio.CancelledError):
            await t_a
        store.put_write_delay = 0.0
        result_b = await store.put(5, True, seg_bytes(5))
        assert result_b.generation == 3

        # 模拟 A 的迟到提交（其临时文件已被回收，先放占位文件模拟迟到线程
        # 刚刚写完）：代次令牌 2 已过期，提交必须作废并回收临时文件。
        stale_tmp = store._tmp_path(0, 2)
        stale_tmp.write_bytes(b"stale")
        with pytest.raises(StaleAttempt):
            await store._commit_attempt(0, 2, 5, False, 5)
        assert not stale_tmp.exists()
        snap = await store.snapshot()
        assert snap["slots"][0]["seq"] == 5
        assert snap["slots"][0]["generation"] == 3
        assert slot_files_only(store)

    asyncio.run(scenario())


def test_tmp_reclaimed_on_write_failure(tmp_path):
    async def scenario():
        store = make_store(tmp_path)
        real_write = store._write_tmp_file

        def boom(slot, generation, data):
            real_write(slot, generation, data)
            raise OSError("simulated disk failure")

        store._write_tmp_file = boom  # type: ignore[method-assign]
        with pytest.raises(OSError):
            await store.put(1, True, seg_bytes(1))
        # 写盘失败：临时文件被清理，且不应产生任何槽位正式文件。
        assert list_files(store) == []

    asyncio.run(scenario())


def test_wipe_dir_recovers_crash_leftovers(tmp_path):
    data_dir = tmp_path / "data"
    data_dir.mkdir(parents=True)
    (data_dir / "slot_0000.seg").write_bytes(b"old")
    (data_dir / "slot_0001.g00000007.tmp").write_bytes(b"orphan")
    store = make_store(tmp_path)
    assert list_files(store) == []
