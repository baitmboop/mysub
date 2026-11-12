#!/bin/zsh
# 双击这个文件即可安装(它会在 Terminal 里跑 install_schedule.sh)。
#
# 为什么要双击: launchctl 只能在登录会话里注册 LaunchAgent, 从脚本/沙箱进程里
# 调 bootstrap 会报 "Bootstrap failed: 5: Input/output error"。
#
# 补充: 即使不双击, plist 已经放在 ~/Library/LaunchAgents 了, macOS 会在下次
# 登录时自动加载它 —— 双击只是让它立刻生效, 不用等到下次登录。
cd "$(dirname "$0")" || exit 1
exec ./install_schedule.sh
