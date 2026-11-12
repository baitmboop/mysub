#!/bin/zsh
# 一次性安装: 把国内普查挂到 launchd, 每 7 天跑一次。
#
# 必须在**你自己的 Terminal 窗口**里跑(或双击运行)。原因: launchctl 只能在
# 登录会话(Aqua)里注册 LaunchAgent, 从非交互/沙箱进程里执行 bootstrap 会报
# "Bootstrap failed: 5: Input/output error" —— 实测连一个最小 plist 都装不上。
#
# 装完可以这样确认:
#   launchctl print gui/$(id -u)/com.nodepool.cn-refresh | grep -A4 calendar
set -u

REPO="/Users/ty-yc/WorkBuddy/2026-11-11-11-17-09/nodepool"
LABEL="com.nodepool.cn-refresh"
SRC="$REPO/launchd/$LABEL.plist"
DST="$HOME/Library/LaunchAgents/$LABEL.plist"
LOG="$REPO/logs/schedule-install.log"

cd "$REPO" || exit 1
mkdir -p "$HOME/Library/LaunchAgents" logs
: >"$LOG"

say() { echo "$*" | tee -a "$LOG"; }

if [ ! -f "$SRC" ]; then
    say "找不到 $SRC"
    exit 1
fi
chmod +x "$REPO/cn_refresh.sh"

cp -f "$SRC" "$DST"
say "已复制 $DST"

# 已经装过就先卸掉, 否则 bootstrap 会因为 label 重复而失败
launchctl bootout "gui/$(id -u)/$LABEL" 2>/dev/null

if launchctl bootstrap "gui/$(id -u)" "$DST" 2>>"$LOG"; then
    say "装载成功"
else
    say "装载失败。把上面的报错发我, 或改用 cron 方案。"
    exit 1
fi

if launchctl print "gui/$(id -u)/$LABEL" >>"$LOG" 2>&1; then
    say "已确认在 launchd 里:"
    launchctl print "gui/$(id -u)/$LABEL" | grep -E "state|path|run interval|calendar" | head -6 | tee -a "$LOG"
fi

say ""
say "下一次运行: 每周四 04:30(错过则在开机/唤醒后补跑)"
say "手动跑一次:  $REPO/cn_refresh.sh"
say "看日志:      $REPO/logs/cn-refresh.log"
say "卸载:        launchctl bootout gui/$(id -u)/$LABEL"
echo
echo "你可以关掉这个窗口了。"
