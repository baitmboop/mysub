#!/bin/bash
# 每日刷新: 采集 → 测速 → 交给网关并入历史池
#
# 设计要点:
#   1. 先探测本机代理(默认 127.0.0.1:7897)是否在线。采集要翻墙, 代理挂了就跳过采集,
#      但仍然重测已有节点 —— 测速本身不需要代理, 不要因为代理断了就整轮报废。
#   2. 任何一步失败都不删旧数据。probe.py 产出为空时保持原文件不动,
#      网关的历史并集会兜住上一轮的结果。
#
# 用法:
#   ./refresh.sh              # 完整流程
#   ./refresh.sh --no-collect # 只重测已有池子, 不采集
#   ./refresh.sh --dry-run    # 只打印将要做什么

set -uo pipefail
cd "$(dirname "$0")" || exit 1

ROOT="$(pwd)"
PY="${PYTHON:-$(command -v python3)}"
PROXY="${NODEPOOL_PROXY:-http://127.0.0.1:7897}"
PROXY_HOST="${PROXY#*://}"
PROXY_HOST="${PROXY_HOST%%:*}"
PROXY_PORT="${PROXY##*:}"
LOG_DIR="$ROOT/logs"
mkdir -p "$LOG_DIR" "$ROOT/data" "$ROOT/out"
LOG="$LOG_DIR/refresh-$(date +%Y%m%d).log"

NO_COLLECT=0
DRY=0
for a in "$@"; do
  case "$a" in
    --no-collect) NO_COLLECT=1 ;;
    --dry-run) DRY=1 ;;
    *) echo "未知参数: $a"; exit 2 ;;
  esac
done

say() { echo "[$(date '+%F %T')] $*" | tee -a "$LOG"; }

# macOS 默认没有 GNU timeout。优先用现成的，否则自己实现（子进程 wait 住，不是脱离）。
if command -v timeout >/dev/null 2>&1; then
  TO() { local s=$1; shift; timeout "$s" "$@"; }
elif command -v gtimeout >/dev/null 2>&1; then
  TO() { local s=$1; shift; gtimeout "$s" "$@"; }
else
  TO() {
    local s=$1; shift
    "$@" & local p=$!
    ( sleep "$s"; kill -TERM "$p" 2>/dev/null ) >/dev/null 2>&1 & local w=$!
    wait "$p"; local rc=$?
    kill -TERM "$w" 2>/dev/null; wait "$w" 2>/dev/null
    return $rc
  }
fi

say "========== 开始刷新 =========="
say "python: $PY"
say "代理:   $PROXY"

if [ "$DRY" = "1" ]; then
  say "dry-run: 代理在线=$([ "$(nc -z -G 2 "$PROXY_HOST" "$PROXY_PORT" 2>/dev/null; echo $?)" = "0" ] && echo yes || echo no)"
  say "dry-run: 结束"
  exit 0
fi

# ---------- 1. 采集 ----------
if [ "$NO_COLLECT" = "1" ]; then
  say "跳过采集 (--no-collect)"
else
  if nc -z -G 2 "$PROXY_HOST" "$PROXY_PORT" 2>/dev/null; then
    say "代理在线, 开始采集"
    cp -f "$ROOT/data/nodes.json" "$ROOT/data/nodes.prev.json" 2>/dev/null
    if TO 900 "$PY" collect.py --proxy "$PROXY" >>"$LOG" 2>&1; then
      N=$(python3 -c "import json;print(len(json.load(open('data/nodes.json'))))" 2>/dev/null || echo 0)
      say "采集完成: $N 个节点"
      if [ "$N" -lt 50 ]; then
        say "警告: 节点数异常偏少($N), 保留上一轮数据"
        [ -f "$ROOT/data/nodes.prev.json" ] && cp -f "$ROOT/data/nodes.prev.json" "$ROOT/data/nodes.json"
      fi
    else
      say "采集失败(退出码 $?), 保留上一轮数据"
    fi
  else
    say "代理 $PROXY 不在线, 跳过采集 (节点池仍会被重新测速)"
  fi
fi

# ---------- 2. 测速 ----------
say "开始测速"
cp -f "$ROOT/out/nodes_best.json" "$ROOT/out/nodes_best.prev.json" 2>/dev/null
if TO 1800 "$PY" probe.py --max-speed 100 --keep 30 --delay-limit 3000 \
      >>"$LOG" 2>&1; then
  K=$(python3 -c "import json;print(len(json.load(open('out/nodes_best.json'))))" 2>/dev/null || echo 0)
  P=$(python3 -c "import json;print(len(json.load(open('out/nodes_best.prev.json'))))" 2>/dev/null || echo 0)
  say "测速完成: 本轮可用 $K 个 (上轮 $P 个)"
  if [ "$K" = "0" ]; then
    say "警告: 本轮 0 个可用, 保留上一轮结果供网关并集使用"
    [ -f "$ROOT/out/nodes_best.prev.json" ] && cp -f "$ROOT/out/nodes_best.prev.json" "$ROOT/out/nodes_best.json"
  fi
else
  say "测速失败, 保留上一轮结果"
fi

# ---------- 3. 通知网关 ----------
# 网关有 watcher 盯 mtime, 这里再主动踢一下更快生效
TOKEN=$(python3 -c "import json;print(json.load(open('data/gateway.json')).get('token',''))" 2>/dev/null || true)
if [ -n "${TOKEN:-}" ] && nc -z -G 1 127.0.0.1 8899 2>/dev/null; then
  curl -s --noproxy '*' -m 5 "http://127.0.0.1:8899/reload?token=$TOKEN" -o /dev/null \
    && say "网关已重载"
else
  say "网关未运行, 启动后会自动读取最新结果"
fi

# ---------- 4. 清理旧日志 ----------
find "$LOG_DIR" -name 'refresh-*.log' -mtime +14 -delete 2>/dev/null

say "========== 刷新结束 =========="
