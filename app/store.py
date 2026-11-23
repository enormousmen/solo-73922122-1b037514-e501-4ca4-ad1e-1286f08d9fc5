"""录像分片环形存储（支持写盘期间取消上传，且取消不破坏回看一致性）。

模型：
- 固定 slot_count 个槽位，每槽最多一片，分片字节落盘为每槽一个文件。
- 编码器按连续片号上传；写满后新片覆盖"最旧片"所在槽。
- 每次覆盖使槽位代次(generation) +1。游标里带着 (slot, generation, seq, offset)，
  旧游标对不上当前槽位元数据即判为断档，绝不会读到同槽新片的字节。

上传分两阶段，保证"写盘时取消"的安全性：
1. 锁外把字节写入该槽该代的*临时文件*。此阶段可被取消，且任何情况下都不会
   触碰槽位正式文件——磁盘上的旧片对旧游标始终有效。
2. 提交阶段在 asyncio.shield 内、持同一把锁完成 os.replace 与内存元数据更新，
   对外是原子的，且不可被取消打断：
   - 取消发生在提交前 -> 覆盖未提交，旧片与旧游标照旧有效，编码器用同片号
     重试无需等待前一次写盘收尾（前一次的线程只会写自己的临时文件）；
   - 取消发生在提交中 -> 提交要么完整生效（最新片号同步推进，旧游标收到
     断档 410），要么完全不生效，不存在半提交状态。
- 每次尝试在进入时领取一个代次令牌；提交时令牌与槽位当前代次不符，说明期间
  已有更新的尝试占用该槽，本次为迟到的僵尸提交，直接作废（StaleAttempt），
  绝不允许它替换掉更新的片。因此同片号重试成功后，前次被取消的写盘即使迟到
  也改不掉回看内容。
- 取消/失败留下的临时文件由后台回收任务（或提交失败时就地）删除；进程重启
  时数据目录整体清空，崩溃残留的临时文件同样被回收。

元数据全在内存，由一把 asyncio.Lock 保护；文件读写经 to_thread 挂到锁内，
因此"校验游标 -> 读字节"与"覆盖提交 -> 改元数据"互斥，不会读到半新半旧。

读路径只在单个请求内短暂持锁（最多读 64 KiB），请求之间不持有任何资源；
写路径不等待任何读者。所以慢终端既拖不住写入，也不会多占磁盘槽。
"""
from __future__ import annotations

import asyncio
import base64
import binascii
import json
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Awaitable, Callable, Optional, Union


class StoreError(Exception):
    """存储层错误基类。"""


class NonContiguous(StoreError):
    """片号不连续（不是 last_seq + 1）。"""

    def __init__(self, expected: int, got: int):
        super().__init__(f"expected seq {expected}, got {got}")
        self.expected = expected
        self.got = got


class StaleAttempt(StoreError):
    """迟到的提交：领取代次令牌后该槽已被更新的尝试占用，本次提交作废。"""


class UploadAborted(StoreError):
    """上传在覆盖提交前被放弃（典型：客户端在写盘期间断连）。

    覆盖确定未提交，旧片与旧游标保持有效；编码器可用同片号立即重试。
    与 asyncio.CancelledError 区分开，便于 Web 层静默处理而不打印错误堆栈。
    """


class EmptySegment(StoreError):
    """空分片体。"""


class SegmentTooLarge(StoreError):
    """超过单片上限。"""


class BadCursor(StoreError):
    """游标无法解析或自相矛盾。"""


@dataclass
class SlotMeta:
    seq: int
    independent: bool
    generation: int
    size: int


@dataclass
class Cursor:
    """续读游标：定位到某槽某代某片的某字节偏移。"""

    slot: int
    generation: int
    seq: int
    offset: int

    def encode(self) -> str:
        payload = json.dumps(
            {"s": self.slot, "g": self.generation, "q": self.seq, "o": self.offset},
            separators=(",", ":"),
        ).encode()
        return base64.urlsafe_b64encode(payload).decode()

    @classmethod
    def decode(cls, token: str) -> "Cursor":
        try:
            raw = base64.urlsafe_b64decode(token.encode())
            obj = json.loads(raw)
            cur = cls(
                slot=int(obj["s"]),
                generation=int(obj["g"]),
                seq=int(obj["q"]),
                offset=int(obj["o"]),
            )
        except (binascii.Error, ValueError, KeyError, TypeError, UnicodeEncodeError) as exc:
            raise BadCursor(f"invalid cursor: {exc}") from exc
        if cur.slot < 0 or cur.generation < 0 or cur.seq < 0 or cur.offset < 0:
            raise BadCursor("cursor fields must be non-negative")
        return cur


# ---- 操作结果 ----


@dataclass
class PutResult:
    seq: int
    slot: int
    generation: int
    size: int
    independent: bool
    oldest_seq: int
    newest_seq: int


@dataclass
class StartOk:
    start_seq: int
    cursor: Cursor


@dataclass
class ReadOk:
    data: bytes
    seq: int
    offset: int
    segment_size: int
    independent: bool
    next_cursor: Cursor


@dataclass
class Gap:
    """断档：所要的片已被覆盖。给出最早仍可起播的片号（可能为 None）。"""

    earliest_playable: Optional[int]


@dataclass
class Wait:
    """等待：暂无可用起点，或下一片尚未写入。"""

    reason: str


StartResult = Union[StartOk, Gap, Wait]
ReadResult = Union[ReadOk, Gap, Wait]


class SegmentStore:
    def __init__(
        self,
        data_dir: str,
        slot_count: int,
        max_segment_bytes: int,
        max_read_bytes: int,
        put_write_delay: float = 0.0,
        put_commit_delay: float = 0.0,
    ):
        if slot_count < 1:
            raise ValueError("slot_count must be >= 1")
        self.data_dir = Path(data_dir)
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.slot_count = slot_count
        self.max_segment_bytes = max_segment_bytes
        self.max_read_bytes = max_read_bytes
        # 故障注入（默认关闭）：分别放慢"临时文件写盘"与"提交"阶段，
        # 供自测/演示稳定复现"写盘时取消上传"的两种时序。生产环境保持 0。
        self.put_write_delay = put_write_delay
        self.put_commit_delay = put_commit_delay
        self._slots: list[Optional[SlotMeta]] = [None] * slot_count
        self._generations: list[int] = [0] * slot_count
        self._index: dict[int, int] = {}  # seq -> slot
        self._last_seq: Optional[int] = None
        self._lock = asyncio.Lock()
        # 取消后负责回收临时文件的后台任务（取消时写盘线程可能还没写完，
        # 必须等它结束再删，否则会留下孤儿临时文件）。
        self._reapers: set[asyncio.Task] = set()
        self._wipe_dir()

    # ---- 文件层（同步，经 to_thread 调用） ----

    def _wipe_dir(self) -> None:
        # 元数据在内存，重启后无法恢复索引，落盘文件（含崩溃残留的临时文件）
        # 一并清空，从头开始。
        for p in self.data_dir.iterdir():
            if p.is_file():
                p.unlink()

    def _slot_path(self, slot: int) -> Path:
        return self.data_dir / f"slot_{slot:04d}.seg"

    def _tmp_path(self, slot: int, generation: int) -> Path:
        return self.data_dir / f"slot_{slot:04d}.g{generation:08d}.tmp"

    def _write_tmp_file(self, slot: int, generation: int, data: bytes) -> None:
        """只写临时文件并落盘，绝不替换槽位正式文件（取消安全的关键）。"""
        tmp = self._tmp_path(slot, generation)
        with open(tmp, "wb") as fh:
            fh.write(data)
            fh.flush()
            os.fsync(fh.fileno())

    def _commit_slot_file(self, slot: int, generation: int) -> None:
        """把临时文件原子替换为槽位正式文件。仅在持锁的提交阶段调用。"""
        os.replace(self._tmp_path(slot, generation), self._slot_path(slot))

    def _read_slot_file(self, slot: int, offset: int, length: int) -> bytes:
        with open(self._slot_path(slot), "rb") as fh:
            fh.seek(offset)
            return fh.read(length)

    def _unlink_tmp(self, slot: int, generation: int) -> None:
        """尽力删除临时文件；文件不存在不算错误。"""
        try:
            self._tmp_path(slot, generation).unlink()
        except FileNotFoundError:
            pass

    def _spawn_reaper(self, write_task: "asyncio.Task[None]", slot: int, generation: int) -> None:
        """取消路径：等写盘线程结束后回收其临时文件，避免孤儿文件。"""

        async def _reap() -> None:
            try:
                await write_task
            except BaseException:  # 写盘成功或失败都要回收
                pass
            await asyncio.to_thread(self._unlink_tmp, slot, generation)

        task = asyncio.create_task(_reap())
        self._reapers.add(task)
        task.add_done_callback(self._reapers.discard)

    async def drain_background(self) -> None:
        """等待全部临时文件回收任务结束（自测用，生产无需调用）。"""
        while self._reapers:
            await asyncio.gather(*list(self._reapers), return_exceptions=True)

    # ---- 元数据辅助（须在锁内调用） ----

    def _earliest_playable_locked(self) -> Optional[int]:
        seqs = [m.seq for m in self._slots if m is not None and m.independent]
        return min(seqs) if seqs else None

    def _oldest_seq_locked(self) -> Optional[int]:
        seqs = [m.seq for m in self._slots if m is not None]
        return min(seqs) if seqs else None

    def _pick_slot_locked(self) -> int:
        for i, meta in enumerate(self._slots):
            if meta is None:
                return i
        # 写满了：覆盖最旧片所在槽。
        return min(range(self.slot_count), key=lambda i: self._slots[i].seq)

    # ---- 对外操作 ----

    async def put(
        self,
        seq: int,
        independent: bool,
        data: bytes,
        is_disconnected: "Optional[Callable[[], Awaitable[bool]]]" = None,
    ) -> PutResult:
        """上传一片。

        is_disconnected: 可选的异步回调，返回 True 表示客户端已断连。
        ASGI 服务器不会因断连自动取消正在写盘的请求任务，必须由应用在
        提交边界主动探测：探测到断连即按"提交前取消"处理，绝不提交覆盖。
        """
        if not data:
            raise EmptySegment("empty segment body")
        if len(data) > self.max_segment_bytes:
            raise SegmentTooLarge(
                f"segment is {len(data)} bytes, limit is {self.max_segment_bytes}"
            )
        # 阶段 0：在锁内完成连续性校验、选槽并领取本次尝试的代次令牌。
        # 令牌只用于识别迟到的僵尸提交，元数据此刻保持不变。
        async with self._lock:
            if self._last_seq is not None and seq != self._last_seq + 1:
                raise NonContiguous(expected=self._last_seq + 1, got=seq)
            slot = self._pick_slot_locked()
            self._generations[slot] += 1
            generation = self._generations[slot]

        # 阶段 1：锁外写临时文件。此阶段可被取消，且不会改动任何槽位文件或
        # 元数据，旧片与旧游标完全不受影响；同片号重试也不必等待它收尾。
        write_task = asyncio.create_task(
            asyncio.to_thread(self._write_tmp_file, slot, generation, data)
        )
        try:
            await write_task
            if self.put_write_delay:  # 故障注入：仍属提交前窗口
                await asyncio.sleep(self.put_write_delay)
        except asyncio.CancelledError:
            # 取消时尚未创建提交任务：覆盖确定未生效。线程无法真正停止，但它
            # 只会写完自己的临时文件，由回收任务清理；槽位正式文件绝不会被改动。
            if not write_task.done():
                self._spawn_reaper(write_task, slot, generation)
            else:
                # 写盘线程已完成、取消打断的是延迟窗口：直接回收临时文件。
                self._unlink_tmp(slot, generation)
            raise
        except BaseException:
            # 写盘本身失败（磁盘满/IO 错误等）：清理临时文件后原样抛出。
            write_task.cancel()
            await asyncio.to_thread(self._unlink_tmp, slot, generation)
            raise

        # 提交边界：ASGI 不会因客户端断连自动取消本任务，这里主动探测一次。
        # 断连即按"提交前取消"处理——覆盖尚未提交，旧片与旧游标保持有效，
        # 编码器可用同片号立即重试，无需等待任何写盘收尾。
        if is_disconnected is not None and await is_disconnected():
            self._unlink_tmp(slot, generation)
            raise UploadAborted("client disconnected before segment commit")

        # 阶段 2：shield + 同一把锁内原子提交。提交任务一旦创建，外部取消只能
        # 作用在"等待"上：os.replace 与元数据更新在 shield 内要么都完成、要么
        # 都不发生。
        commit_task = asyncio.ensure_future(
            self._commit_attempt(slot, generation, seq, independent, len(data))
        )
        try:
            return await asyncio.shield(commit_task)
        except asyncio.CancelledError:
            if commit_task.done():
                # 提交已经结束：成功则覆盖完整生效（最新片号已推进，旧游标将
                # 凭代次不匹配收到断档）；失败则其内部已自行清理临时文件。
                # 取回异常，避免事件循环报"异常未回收"。
                if not commit_task.cancelled():
                    commit_task.exception()
            else:
                # 提交仍在 shield 内继续运行：临时文件的生命周期交给它自己
                # （提交成功则被 replace 消费；令牌过期则自行删除）。此处绝
                # 不能提前 unlink，否则会让正在进行的原子替换失败。
                commit_task.add_done_callback(
                    lambda t: self._after_commit_task(t, slot, generation)
                )
            raise

    def _after_commit_task(self, task: "asyncio.Task[PutResult]", slot: int, generation: int) -> None:
        """回收 shield 内迟到结束的提交任务：取回异常，必要时清理临时文件。"""
        if task.cancelled():
            return
        exc = task.exception()
        if exc is not None:
            # 提交失败（如 os.replace 的 IO 错误）：临时文件不会被消费，就地回收。
            self._unlink_tmp(slot, generation)

    async def _commit_attempt(
        self, slot: int, generation: int, seq: int, independent: bool, size: int
    ) -> PutResult:
        async with self._lock:
            if self.put_commit_delay:  # 故障注入：稳定复现"提交中被取消"
                await asyncio.sleep(self.put_commit_delay)
            # 代次令牌过期：期间已有更新的尝试占用该槽（典型：前次上传被取消后
            # 同片号重试成功，前次的提交此刻才迟到）。本次提交作废，绝不能替换
            # 掉更新的片——这是"重试成功后回看内容不被迟到写入改掉"的保证。
            if self._generations[slot] != generation:
                await asyncio.to_thread(self._unlink_tmp, slot, generation)
                raise StaleAttempt(
                    f"stale commit for slot {slot}: generation {generation} "
                    f"is no longer current"
                )
            # 原子替换与元数据更新在同一临界区内，读者要么看到旧片旧元数据，
            # 要么看到新片新元数据，不会读到半新半旧。
            await asyncio.to_thread(self._commit_slot_file, slot, generation)
            old = self._slots[slot]
            if old is not None:
                self._index.pop(old.seq, None)
            self._slots[slot] = SlotMeta(
                seq=seq, independent=independent, generation=generation, size=size
            )
            self._index[seq] = slot
            self._last_seq = seq
            return PutResult(
                seq=seq,
                slot=slot,
                generation=generation,
                size=size,
                independent=independent,
                oldest_seq=self._oldest_seq_locked(),
                newest_seq=self._last_seq,
            )

    async def start(self, seq: int) -> StartResult:
        """从"不晚于 seq、仍留存的最近独立片"起播。"""
        async with self._lock:
            best: Optional[int] = None
            for meta in self._slots:
                if meta is None or not meta.independent or meta.seq > seq:
                    continue
                if best is None or meta.seq > best:
                    best = meta.seq
            if best is not None:
                slot = self._index[best]
                meta = self._slots[slot]
                return StartOk(
                    start_seq=best,
                    cursor=Cursor(slot=slot, generation=meta.generation, seq=best, offset=0),
                )
            earliest = self._earliest_playable_locked()
            if earliest is not None:
                # 还有可起播的片，但都比所求片号新 -> 所求起点已被覆盖。
                return Gap(earliest_playable=earliest)
            # 库里一片可独立解码的都没有 -> 等编码器继续写。
            return Wait(reason="no-independent-start")

    async def read(self, token: str, max_bytes: int) -> ReadResult:
        cursor = Cursor.decode(token)
        if cursor.slot >= self.slot_count:
            raise BadCursor("cursor slot out of range")
        max_bytes = max(1, min(max_bytes, self.max_read_bytes))
        async with self._lock:
            slot = cursor.slot
            generation = cursor.generation
            seq = cursor.seq
            offset = cursor.offset
            while True:
                meta = self._slots[slot]
                if meta is None or meta.seq != seq or meta.generation != generation:
                    # 槽位被覆盖（或重启清空）：旧游标读不到同槽新片。
                    return Gap(earliest_playable=self._earliest_playable_locked())
                if offset > meta.size:
                    raise BadCursor("cursor offset beyond segment size")
                if offset < meta.size:
                    length = min(max_bytes, meta.size - offset)
                    data = await asyncio.to_thread(
                        self._read_slot_file, slot, offset, length
                    )
                    return ReadOk(
                        data=data,
                        seq=seq,
                        offset=offset,
                        segment_size=meta.size,
                        independent=meta.independent,
                        next_cursor=Cursor(
                            slot=slot,
                            generation=generation,
                            seq=seq,
                            offset=offset + length,
                        ),
                    )
                # 本片已读完，顺势滚到下一片。
                next_slot = self._index.get(seq + 1)
                if next_slot is None:
                    if self._last_seq is not None and seq + 1 <= self._last_seq:
                        # 片号曾写入过却不在库里了 -> 被覆盖。
                        return Gap(earliest_playable=self._earliest_playable_locked())
                    return Wait(reason="next-segment-not-written")
                slot = next_slot
                seq = seq + 1
                offset = 0
                generation = self._slots[slot].generation

    async def snapshot(self) -> dict:
        async with self._lock:
            return {
                "slot_count": self.slot_count,
                "max_segment_bytes": self.max_segment_bytes,
                "max_read_bytes": self.max_read_bytes,
                "newest_seq": self._last_seq,
                "oldest_seq": self._oldest_seq_locked(),
                "earliest_playable": self._earliest_playable_locked(),
                "slots": [
                    (
                        {
                            "slot": i,
                            "empty": False,
                            "seq": meta.seq,
                            "independent": meta.independent,
                            "generation": meta.generation,
                            "size": meta.size,
                        }
                        if meta is not None
                        else {"slot": i, "empty": True}
                    )
                    for i, meta in enumerate(self._slots)
                ],
            }
