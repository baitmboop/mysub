#!/bin/sh
# 双击或终端运行: 用你正在跑的 Clash 实测一轮, 把结果推回仓库
cd "$(dirname "$0")" || exit 1
exec python3 sync.py --push "$@"
