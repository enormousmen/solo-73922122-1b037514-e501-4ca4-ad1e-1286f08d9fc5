"""FastAPI 入口：录像分片接收 + 游标续读回看。

接口一览：
  POST /v1/segments/{seq}?independent=true|false   上传一片（原始字节，<=16 MiB）
  GET  /v1/playback/start?seq=N                    起播，返回游标
  GET  /v1/playback/read?cursor=...&max_bytes=...  凭游标续读，每次 <=64 KiB 原始字节
  GET  /v1/state                                   槽位/片号现状（演示与排障用）
  GET  /healthz                                    健康检查

状态语义：
  200 正常（续读时响应体即原始字节，下一游标在 X-Next-Cursor 响应头）
  204 等待（无可用起点 / 下一片尚未写入，原因在 X-Reason 响应头）
  410 断档（起点或续读位置已被覆盖，JSON 里给出 earliest_playable）
"""
from __future__ import annotations

import asyncio

from fastapi import FastAPI, Path, Query, Request, Response
from fastapi.responses import JSONResponse

from .config import Settings
from .store import (
    BadCursor,
    Gap,
    NonContiguous,
    ReadOk,
    SegmentStore,
    EmptySegment,
    StaleAttempt,
    StartOk,
    UploadAborted,
)

REASON_NO_START = "no-independent-start"
REASON_NO_NEXT = "next-segment-not-written"
# 非标准但常用的"客户端断连"状态码，仅用于连接仍在时的可观测响应；
# 真正断连时响应发不出去，处理器至少保证不打印错误堆栈。
STATUS_CLIENT_CLOSED = 499


def create_app(settings: Settings) -> FastAPI:
    store = SegmentStore(
        data_dir=settings.data_dir,
        slot_count=settings.slot_count,
        max_segment_bytes=settings.max_segment_bytes,
        max_read_bytes=settings.max_read_bytes,
        put_write_delay=settings.put_write_delay,
        put_commit_delay=settings.put_commit_delay,
    )
    app = FastAPI(title="segment-recorder", version="0.1.0")
    app.state.store = store

    @app.get("/healthz")
    async def healthz():
        return {"status": "ok"}

    @app.post("/v1/segments/{seq}", status_code=201)
    async def upload_segment(
        request: Request,
        seq: int = Path(ge=0),
        independent: bool = Query(False),
    ):
        # 先信 Content-Length 快速拒绝，再边读边限长，避免超限片占内存。
        declared = request.headers.get("content-length")
        if declared is not None and declared.isdigit():
            if int(declared) > store.max_segment_bytes:
                return _too_large(store.max_segment_bytes)
        body = bytearray()
        async for chunk in request.stream():
            body.extend(chunk)
            if len(body) > store.max_segment_bytes:
                return _too_large(store.max_segment_bytes)

        # ASGI 服务器不会在客户端断连时自动取消正在写盘的请求任务，需要应用
        # 自己在提交边界探测 http.disconnect；探测到断连即按"提交前取消"处理。
        # 监听任务预先创建并阻塞在 receive() 上：真实 uvicorn 在断连时投递
        # http.disconnect；内存型测试传输在请求结束前不会投递任何消息，
        # 因此用 0 超时轮询——无消息即视为"仍连接"，不会被挂住。
        async def _watch_disconnect() -> bool:
            message = await request.receive()
            return message.get("type") == "http.disconnect"

        disconnect_task = asyncio.ensure_future(_watch_disconnect())

        async def _is_disconnected() -> bool:
            if not disconnect_task.done():
                return False
            try:
                return disconnect_task.result()
            except asyncio.CancelledError:
                return False

        try:
            result = await store.put(
                seq=seq,
                independent=independent,
                data=bytes(body),
                is_disconnected=_is_disconnected,
            )
        except asyncio.CancelledError:
            # 写盘期间协程被事件循环取消：覆盖未提交（或已在 shield 内完整
            # 提交），连接既已断开，无需也无法再写 HTTP 响应，结束请求即可。
            raise
        except UploadAborted:
            # 写盘期间探测到客户端断连、覆盖尚未提交。连接多半已不可写；
            # 即便仍可写，也返回 499 表明上传被放弃，库内状态未变。
            return Response(status_code=STATUS_CLIENT_CLOSED)
        except EmptySegment:
            return JSONResponse(status_code=400, content={"detail": "empty segment body"})
        except NonContiguous as exc:
            return JSONResponse(
                status_code=409,
                content={
                    "detail": str(exc),
                    "expected_seq": exc.expected,
                    "got_seq": exc.got,
                },
            )
        except StaleAttempt as exc:
            # 迟到的覆盖提交被作废：槽位已被更新的尝试占用，本次结果以库里
            # 现有内容为准，客户端无需处理（重试成功的那一次已经生效）。
            return JSONResponse(
                status_code=409,
                content={"detail": str(exc), "reason": "stale-commit-discarded"},
            )
        finally:
            # 请求离开写盘路径后回收断连监听任务，避免悬挂的 receive() 协程。
            disconnect_task.cancel()
        return {
            "seq": result.seq,
            "slot": result.slot,
            "generation": result.generation,
            "size": result.size,
            "independent": result.independent,
            "oldest_seq": result.oldest_seq,
            "newest_seq": result.newest_seq,
        }

    @app.get("/v1/playback/start")
    async def playback_start(seq: int = Query(ge=0)):
        result = await store.start(seq)
        if isinstance(result, StartOk):
            return {
                "status": "ok",
                "start_seq": result.start_seq,
                "cursor": result.cursor.encode(),
            }
        if isinstance(result, Gap):
            return JSONResponse(
                status_code=410,
                content={
                    "status": "gap",
                    "detail": "requested start point has been overwritten",
                    "earliest_playable": result.earliest_playable,
                },
            )
        return Response(status_code=204, headers={"X-Reason": REASON_NO_START})

    @app.get("/v1/playback/read")
    async def playback_read(
        cursor: str = Query(...),
        max_bytes: int = Query(
            default=settings.max_read_bytes, ge=1, le=settings.max_read_bytes
        ),
    ):
        try:
            result = await store.read(cursor, max_bytes)
        except BadCursor as exc:
            return JSONResponse(status_code=400, content={"detail": str(exc)})
        if isinstance(result, ReadOk):
            end_offset = result.offset + len(result.data)
            headers = {
                "X-Next-Cursor": result.next_cursor.encode(),
                "X-Seq": str(result.seq),
                "X-Offset": str(result.offset),
                "X-Segment-Size": str(result.segment_size),
                "X-Independent": "true" if result.independent else "false",
                "X-End-Of-Segment": "true" if end_offset >= result.segment_size else "false",
            }
            return Response(
                content=result.data,
                media_type="application/octet-stream",
                headers=headers,
            )
        if isinstance(result, Gap):
            return JSONResponse(
                status_code=410,
                content={
                    "status": "gap",
                    "detail": "segment was overwritten before it could be read",
                    "earliest_playable": result.earliest_playable,
                },
            )
        return Response(status_code=204, headers={"X-Reason": REASON_NO_NEXT})

    @app.get("/v1/state")
    async def state():
        return await store.snapshot()

    @app.get("/")
    async def root():
        return {
            "service": "segment-recorder",
            "endpoints": [
                "POST /v1/segments/{seq}?independent=",
                "GET /v1/playback/start?seq=",
                "GET /v1/playback/read?cursor=&max_bytes=",
                "GET /v1/state",
                "GET /healthz",
            ],
        }

    return app


def _too_large(limit: int) -> JSONResponse:
    return JSONResponse(
        status_code=413,
        content={"detail": f"segment exceeds {limit} bytes (16 MiB) limit"},
    )


# uvicorn app.main:app 使用的模块级实例（测试走 create_app 自定义配置）。
app = create_app(Settings.from_env())
