#!/bin/bash
# 一键流水线:采集 → 测速 → 导出
set -e
cd "$(dirname "$0")"

PROXY="${PROXY:-http://127.0.0.1:7897}"
MAX_AGE="${MAX_AGE:-72}"
KEEP="${KEEP:-30}"

echo "=========================================="
echo " 节点池流水线  $(date '+%Y-%m-%d %H:%M:%S')"
echo " 代理: $PROXY   帖子时限: ${MAX_AGE}h"
echo "=========================================="

python3 collect.py --proxy "$PROXY" --max-age "$MAX_AGE"

echo
python3 probe.py --max-speed 100 --keep "$KEEP" --delay-limit 3000

echo
echo "完成。结果:"
ls -lh out/
