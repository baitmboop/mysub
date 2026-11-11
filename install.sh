#!/bin/bash
# 安装/卸载 macOS launchd 守护
#
#   ./install.sh              安装: 网关常驻 + 每日 04:00 自动刷新
#   ./install.sh --status     查看两个服务的运行状态
#   ./install.sh --uninstall   全部卸载
#   ./install.sh --refresh-now 立即跑一次刷新(不等待 04:00)
#
# 说明: launchd 启动的进程没有你的 shell 环境, 所以 python 路径、工作目录、
# 日志路径全部写死成绝对路径。代理地址由 refresh.sh 内部指定(默认 7897)。

set -uo pipefail
cd "$(dirname "$0")" || exit 1
ROOT="$(pwd)"
AGENTS="$HOME/Library/LaunchAgents"
LABEL_GW="com.nodepool.gateway"
LABEL_RF="com.nodepool.refresh"
PY="$(command -v python3)"
UID_N="$(id -u)"

mkdir -p "$AGENTS" "$ROOT/logs" "$ROOT/data"

bootout() {
  launchctl bootout "gui/$UID_N/$1" 2>/dev/null \
    || launchctl unload "$AGENTS/$1.plist" 2>/dev/null \
    || true
}

bootstrap() {
  launchctl bootstrap "gui/$UID_N" "$AGENTS/$1.plist" 2>/dev/null \
    || launchctl load -w "$AGENTS/$1.plist" 2>/dev/null \
    || true
}

write_gateway_plist() {
  cat > "$AGENTS/$LABEL_GW.plist" <<EOF
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
  <key>Label</key><string>$LABEL_GW</string>
  <key>ProgramArguments</key>
  <array>
    <string>$PY</string>
    <string>$ROOT/gateway.py</string>
    <string>--port</string><string>8899</string>
  </array>
  <key>WorkingDirectory</key><string>$ROOT</string>
  <key>RunAtLoad</key><true/>
  <key>KeepAlive</key><true/>
  <key>ThrottleInterval</key><integer>10</integer>
  <key>StandardOutPath</key><string>$ROOT/logs/gateway.log</string>
  <key>StandardErrorPath</key><string>$ROOT/logs/gateway.err.log</string>
</dict>
</plist>
EOF
}

write_refresh_plist() {
  cat > "$AGENTS/$LABEL_RF.plist" <<EOF
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
  <key>Label</key><string>$LABEL_RF</string>
  <key>ProgramArguments</key>
  <array>
    <string>/bin/bash</string>
    <string>$ROOT/refresh.sh</string>
  </array>
  <key>WorkingDirectory</key><string>$ROOT</string>
  <key>RunAtLoad</key><false/>
  <key>StartCalendarInterval</key>
  <dict><key>Hour</key><integer>4</integer><key>Minute</key><integer>0</integer></dict>
  <key>StandardOutPath</key><string>$ROOT/logs/refresh.out.log</string>
  <key>StandardErrorPath</key><string>$ROOT/logs/refresh.err.log</string>
</dict>
</plist>
EOF
}

case "${1:-}" in
  --uninstall)
    bootout "$LABEL_GW"; bootout "$LABEL_RF"
    rm -f "$AGENTS/$LABEL_GW.plist" "$AGENTS/$LABEL_RF.plist"
    echo "已卸载网关与定时刷新。数据保留在 $ROOT/data 与 $ROOT/out。"
    exit 0
    ;;
  --status)
    echo "=== $LABEL_GW (网关) ==="
    launchctl list | awk -v L="$LABEL_GW" '$3==L {printf "  pid=%s  上次退出码=%s\n",$1,$2}' || true
    nc -z -G 1 127.0.0.1 8899 2>/dev/null && echo "  8899 监听中 ✓" || echo "  8899 未监听 ✗"
    echo "=== $LABEL_RF (定时) ==="
    launchctl list | awk -v L="$LABEL_RF" '$3==L {printf "  pid=%s  上次退出码=%s\n",$1,$2}' || true
    echo "=== 最近日志 ==="
    tail -5 "$ROOT/logs/gateway.log" 2>/dev/null
    exit 0
    ;;
  --refresh-now)
    echo "立即执行刷新..."
    bash "$ROOT/refresh.sh"
    exit $?
    ;;
  "" ) : ;;
  *) echo "未知参数: $1"; exit 2 ;;
esac

echo "python : $PY"
echo "目录   : $ROOT"
[ -x "$PY" ] || { echo "找不到 python3"; exit 1; }
chmod +x "$ROOT/refresh.sh"

for L in "$LABEL_GW" "$LABEL_RF"; do bootout "$L"; done
write_gateway_plist
write_refresh_plist

for L in "$LABEL_GW" "$LABEL_RF"; do
  bootstrap "$L"
  if launchctl list | grep -q "$L"; then
    echo "✓ $L 已加载"
  else
    echo "✗ $L 加载失败, 试着手动执行: launchctl bootstrap gui/$UID_N $AGENTS/$L.plist"
  fi
done

echo
sleep 2
TOKEN="$(python3 -c "import json;print(json.load(open('$ROOT/data/gateway.json')).get('token',''))" 2>/dev/null || true)"
if nc -z -G 1 127.0.0.1 8899 2>/dev/null; then
  echo "网关已就绪, 把这个地址填进客户端:"
  echo "  http://127.0.0.1:8899/sub/clash?token=$TOKEN"
else
  echo "网关尚未监听, 查看 $ROOT/logs/gateway.err.log"
fi
echo
echo "其余命令: ./install.sh --status | --refresh-now | --uninstall"
