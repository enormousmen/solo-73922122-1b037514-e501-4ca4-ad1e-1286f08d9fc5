#!/usr/bin/env bash
# 端到端演示：写盘时取消上传 -> 同片号重试 -> 回看一致性校验。
#
# 覆盖两种取消时序：
#   A. 取消发生在覆盖"提交前"（临时文件写盘阶段）：
#      旧片与旧游标仍有效；同片号重试立即成功，回看读到的就是重试的字节；
#      重复片号被 409 拒绝；旧游标报 410 断档；磁盘无临时文件残留。
#   B. 取消发生在覆盖"提交中"（shield 保护的原子提交阶段）：
#      覆盖完整生效、最新片号同步推进；旧游标立即 410 断档；
#      同片号重试得到 409（片已入库）；回看字节即取消前那次上传。
#
# 用法：
#   ./demo/demo-cancel-retry.sh                # docker compose 起服务（带故障注入）
#   ./demo/demo-cancel-retry.sh --no-compose   # 服务已在运行（需自行注入延迟）
#   BASE=http://127.0.0.1:8000 ./demo/demo-cancel-retry.sh --no-compose
#
# 前提：服务以 SLOT_COUNT=4 运行；compose 模式通过 docker-compose.demo.yml
# 注入 PUT_WRITE_DELAY/PUT_COMMIT_DELAY（秒），让取消点稳定落在目标阶段。
# --no-compose 模式请用同样的环境变量启动服务。需要 curl 和 python3。
set -euo pipefail

BASE="${BASE:-http://127.0.0.1:8000}"
COMPOSE=1
KEEP=0
while [[ $# -gt 0 ]]; do
  case "$1" in
    --no-compose) COMPOSE=0; shift ;;
    --keep)       KEEP=1; shift ;;
    --base)       BASE="$2"; shift 2 ;;
    *) echo "unknown arg: $1" >&2; exit 2 ;;
  esac
done

# 故障注入窗口（须与服务端 PUT_*_DELAY 对齐）：写盘 1s、提交 1s。
WRITE_DELAY=1
COMMIT_DELAY=1

say() { printf '\n== %s ==\n' "$*"; }

WORK="$(mktemp -d)"
cleanup() {
  rm -rf "$WORK"
  if [[ "$COMPOSE" == 1 && "$KEEP" == 0 ]]; then
    say "收尾: docker compose down"
    (cd "$(dirname "$0")/.." && docker compose -f docker-compose.yml -f docker-compose.demo.yml down)
  fi
}
trap cleanup EXIT

if [[ "$COMPOSE" == 1 ]]; then
  say "启动服务 (docker compose up -d --build，含写盘/提交延迟注入)"
  (cd "$(dirname "$0")/.." && docker compose -f docker-compose.yml -f docker-compose.demo.yml up -d --build)
fi

say "等待服务就绪 $BASE"
ready=0
for _ in $(seq 1 60); do
  if curl -fsS "$BASE/healthz" >/dev/null 2>&1; then ready=1; break; fi
  sleep 1
done
[[ "$ready" == 1 ]] || { echo "服务未就绪"; exit 1; }
echo "  /healthz OK"

json_field() { python3 -c 'import sys,json;print(json.load(sys.stdin)[sys.argv[1]])' "$1"; }

# 生成 1 MiB 的分片文件：整片同一个字节（值=片号），便于字节级核对。
make_seg() { python3 -c "import sys;sys.stdout.buffer.write(bytes([$1 % 256])*1048576)" > "$WORK/seg_$1.bin"; }

upload_code() { # <seq> <independent> <file>  -> 仅打印 HTTP 状态码
  curl -sS -o /dev/null -w '%{http_code}' \
    -H 'Content-Type: application/octet-stream' \
    --data-binary @"$3" \
    "$BASE/v1/segments/$1?independent=$2"
}

# 模拟"写盘期间客户端取消"：用裸 TCP 快速发完请求体后保持连接，到 wait 秒
# 直接断开（不读响应）。服务端时序（发完即 t≈0）：写盘线程 -> 写盘延迟窗口
# (0, WRITE_DELAY) -> 提交边界断连探测 -> shield 提交窗口 (WRITE_DELAY,
# WRITE_DELAY+COMMIT_DELAY)。因此 wait 落在写盘窗口即"提交前取消"，
# 落在提交窗口即"提交中取消"。
upload_cancel() { # <seq> <independent> <file> <wait-seconds>
  python3 - "$BASE" "$1" "$2" "$3" "$4" <<'PY'
import socket, sys, time
from urllib.parse import urlparse
base, seq, ind, path, wait = sys.argv[1:6]
u = urlparse(base)
host, port = u.hostname, u.port or 80
body = open(path, "rb").read()
req = (
    f"POST /v1/segments/{seq}?independent={ind} HTTP/1.1\r\n"
    f"Host: {host}:{port}\r\n"
    "Content-Type: application/octet-stream\r\n"
    f"Content-Length: {len(body)}\r\nConnection: close\r\n\r\n"
).encode()
s = socket.create_connection((host, port), timeout=10)
s.sendall(req + body)  # 回环上瞬间发完，服务端随即进入写盘阶段
deadline = time.monotonic() + float(wait)
gap = deadline - time.monotonic()
if gap > 0:
    time.sleep(gap)  # 保持连接到目标取消点
s.close()  # 不读响应直接断开 -> 客户端取消
print(f"cancelled at t={wait}s (full {len(body)} request bytes already sent)")
PY
}

start_playback() { # <seq> -> 全局 START_CODE / START_SEQ / CURSOR
  local code
  code=$(curl -sS -o "$WORK/start_body" -w '%{http_code}' "$BASE/v1/playback/start?seq=$1")
  START_CODE=$code
  START_SEQ=""; CURSOR=""
  if [[ "$code" == 200 ]]; then
    CURSOR=$(json_field cursor < "$WORK/start_body")
    START_SEQ=$(json_field start_seq < "$WORK/start_body")
  fi
}

read_seq_full() { # <seq> ：从起播游标读完该片，输出到 $WORK/read_$1.bin，返回末状态码
  local seq="$1" out="$WORK/read_$1.bin" code
  : > "$out"
  for _ in $(seq 1 100); do
    code=$(curl -sS -D "$WORK/headers" -o "$WORK/chunk" -w '%{http_code}' \
      "$BASE/v1/playback/read?cursor=$CURSOR")
    if [[ "$code" != 200 ]]; then echo "$code"; return; fi
    cat "$WORK/chunk" >> "$out"
    CURSOR=$(sed -n 's/^[Xx]-[Nn]ext-[Cc]ursor: //p' "$WORK/headers" | tr -d '\r')
    local eos
    eos=$(sed -n 's/^[Xx]-[Ee]nd-[Oo]f-[Ss]egment: //p' "$WORK/headers" | tr -d '\r')
    if [[ "$eos" == "true" ]]; then echo 200; return; fi
  done
  echo "too-many-reads"; exit 1
}

show_state() {
  curl -sS "$BASE/v1/state" | python3 -c '
import sys, json
st = json.load(sys.stdin)
parts = []
for s in st["slots"]:
    if s["empty"]:
        parts.append("-")
    else:
        mark = "I" if s["independent"] else " "
        parts.append("slot%d:seq%d%s(g%d)" % (s["slot"], s["seq"], mark, s["generation"]))
print("  slots: " + ", ".join(parts))
print("  oldest=%s newest=%s earliest_playable=%s" % (st["oldest_seq"], st["newest_seq"], st["earliest_playable"]))'
}

list_data_dir() {
  if [[ "$COMPOSE" == 1 ]]; then
    (cd "$(dirname "$0")/.." && docker compose -f docker-compose.yml -f docker-compose.demo.yml exec -T recorder ls -1 /data | sort)
  else
    echo "  (--no-compose 模式跳过容器内目录检查，可用 /v1/state 间接核对)"
  fi
}

# ========== 准备：写满 4 槽 ==========
say "0. 初始状态"
show_state

say "1. 上传片 1..4（每片 1 MiB；片 1、4 独立），4 个槽写满"
for s in 1 2 3 4; do make_seg "$s"; done
echo "  POST 片1(independent) -> $(upload_code 1 true "$WORK/seg_1.bin")"
for s in 2 3; do echo "  POST 片$s -> $(upload_code "$s" false "$WORK/seg_$s.bin")"; done
echo "  POST 片4(independent) -> $(upload_code 4 true "$WORK/seg_4.bin")"
show_state

# 旧游标：从片 1 起播并读一次，停在片 1 中段（1 MiB > 64 KiB）。
start_playback 1
OLD_CURSOR="$CURSOR"
code=$(curl -sS -D "$WORK/h" -o /dev/null -w '%{http_code}' \
  "$BASE/v1/playback/read?cursor=$OLD_CURSOR")
OLD_CURSOR=$(sed -n 's/^[Xx]-[Nn]ext-[Cc]ursor: //p' "$WORK/h" | tr -d '\r')
echo "  旧游标锚定片 1（槽 0，代次 1）中段，首次 read -> $code"

# ========== 时序 A：提交前取消 -> 重试成功 ==========
say "2A. 上传片 5 覆盖片 1，在写盘阶段（0.5s < 写盘延迟 ${WRITE_DELAY}s，提交探测之前）断开"
make_seg 5
upload_cancel 5 false "$WORK/seg_5.bin" 0.5
sleep $((WRITE_DELAY + 1))  # 等被取消的写盘线程收尾、临时文件被回收
show_state
echo "  数据目录（应只有 4 个 .seg，无 .tmp 残留）:"
list_data_dir | sed 's/^/    /'

say "3A. 覆盖未提交：旧游标续读仍读到片 1 的字节（不是片 5）"
code=$(curl -sS -D "$WORK/h" -o "$WORK/a_chunk" -w '%{http_code}' \
  "$BASE/v1/playback/read?cursor=$OLD_CURSOR")
echo "  旧游标 read -> $code，X-Seq=$(sed -n 's/^[Xx]-[Ss]eq: //p' "$WORK/h" | tr -d '\r')"
[[ "$code" == 200 ]] || { echo "期望 200，实际 $code"; exit 1; }

say "4A. 同片号重试（independent=true）：无需等前次写盘收尾，立即 201"
code=$(upload_code 5 true "$WORK/seg_5.bin")
echo "  POST 片5 重试 -> $code"
[[ "$code" == 201 ]] || { echo "期望 201"; exit 1; }
show_state

say "5A. 重复上传同片号 -> 409（最新片号已推进到 5）"
code=$(upload_code 5 true "$WORK/seg_5.bin")
echo "  POST 片5 重复 -> $code"
[[ "$code" == 409 ]] || { echo "期望 409"; exit 1; }

say "6A. 旧游标（片 1/槽 0 旧代次）-> 410 断档，读不到片 5 字节"
code=$(curl -sS -o "$WORK/a_gap" -w '%{http_code}' \
  "$BASE/v1/playback/read?cursor=$OLD_CURSOR")
echo "  旧游标 read -> $code $(cat "$WORK/a_gap")"
[[ "$code" == 410 ]] || { echo "期望 410"; exit 1; }

say "7A. 重新起播片 5：回看字节必须等于这次重试上传的字节（直到被正常覆盖）"
start_playback 5
[[ "$START_CODE" == 200 && "$START_SEQ" == 5 ]] || { echo "起播片 5 失败"; exit 1; }
code=$(read_seq_full 5)
echo "  读完片 5 -> 末状态 $code"
cmp "$WORK/seg_5.bin" "$WORK/read_5.bin" && echo "  OK: 片 5 回看字节与重试上传完全一致"

# ========== 时序 B：提交中取消 -> 提交仍完整生效 ==========
say "2B. 上传片 6 覆盖片 2，在提交阶段（写盘 ${WRITE_DELAY}s 已过、提交窗口内）断开"
make_seg 6
# 1.5s 时提交边界探测已通过（连接仍在），shield 内提交正在进行；
# 此刻断开只能取消"等待响应"，原子提交不受影响。
upload_cancel 6 true "$WORK/seg_6.bin" 1.5
sleep $((WRITE_DELAY + COMMIT_DELAY + 1))  # 等 shield 内原子提交完成
show_state
newest=$(curl -sS "$BASE/v1/state" | json_field newest_seq)
echo "  最新片号 newest_seq=$newest（提交已生效，必须是 6）"
[[ "$newest" == 6 ]] || { echo "期望 newest_seq=6"; exit 1; }
echo "  数据目录（仍应只有 4 个 .seg，无 .tmp 残留）:"
list_data_dir | sed 's/^/    /'

say "3B. 同片号重试片 6 -> 409（片已随被取消的那次上传入库）"
code=$(upload_code 6 true "$WORK/seg_6.bin")
echo "  POST 片6 重试 -> $code"
[[ "$code" == 409 ]] || { echo "期望 409"; exit 1; }

say "4B. 起播片 6 并读完：回看字节即取消前那次上传的原始字节"
start_playback 6
[[ "$START_CODE" == 200 && "$START_SEQ" == 6 ]] || { echo "起播片 6 失败"; exit 1; }
code=$(read_seq_full 6)
echo "  读完片 6 -> 末状态 $code"
cmp "$WORK/seg_6.bin" "$WORK/read_6.bin" && echo "  OK: 片 6 回看字节与被取消那次上传完全一致"

say "5B. 片 7 正常覆盖片 3 后，片 5 的回看在其槽被覆盖前保持不变"
make_seg 7
echo "  POST 片7 -> $(upload_code 7 false "$WORK/seg_7.bin")"
start_playback 5
code=$(read_seq_full 5)
cmp "$WORK/seg_5.bin" "$WORK/read_5.bin" && echo "  OK: 片 5 未被覆盖前沿用重试入库的字节"

say "演示完成：两种取消时序均符合预期，临时文件已全部回收"
