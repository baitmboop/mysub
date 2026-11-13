#!/bin/zsh
# 国内普查的自动刷新。由 launchd 每 7 天拉起一次(关机则开机后补跑)。
#
# 与 refresh.sh 的区别: 那个是旧的"本机采集+测速+网关"流程(需要本机代理);
# 这个是**只做国内实测**的那一段 —— 它决定订阅里有多少节点是真的能用。
#
# 为什么需要它: 订阅能用多少节点, 完全取决于 cn_pref.json 里"国内实测 ok=1"
# 的存量。而 verify.py 不在 CI 里(Azure runner 在美国, 测不出国内可用性),
# 只能在这台机器上跑。cn_pref 有 7 天有效期, 过期就整体失效, 订阅会退回
# "只按云端延迟排"的状态, 可用率明显下降。
#
# 跑法(手动): ./cn_refresh.sh
# 日志: logs/cn-refresh.log
set -u

REPO="/Users/ty-yc/WorkBuddy/2026-11-11-11-17-09/nodepool"
OUT="/tmp/nodepool-verify.json"
LOCK="/tmp/nodepool-cn-refresh.lock"

cd "$REPO" || exit 1
mkdir -p logs data
LOG="$REPO/logs/cn-refresh.log"
exec >>"$LOG" 2>&1

# launchd 给的 PATH 只有 /usr/bin:/bin:/usr/sbin:/sbin, 而 /usr/bin/python3
# 没有 pyyaml(会 import 失败)。所以必须写死能用的解释器, 不能靠 PATH。
PY="/Users/ty-yc/.workbuddy/binaries/python/versions/3.13.12/bin/python3"
[ -x "$PY" ] || PY="$(command -v python3)"
if ! "$PY" -c "import yaml" 2>/dev/null; then
    echo "找不到带 pyyaml 的 python3 (试过 $PY), 中止"
    exit 3
fi

echo "=== $(date '+%F %T') 开始国内普查 ==="
echo "python: $PY"

# 上一轮没跑完就别叠着跑: 两个 kernel 实例并行会把测速打偏(实测 551→0.7KB/s)
if ! mkdir "$LOCK" 2>/dev/null; then
    echo "已有实例在跑 ($LOCK), 跳过"
    exit 0
fi
trap 'rmdir "$LOCK" 2>/dev/null' EXIT

# 上一次可能死在 rebase 中间(云端同时在推 pool_history, 撞上就冲突)。
# 残留状态会让之后每次 git pull/push 全部失败, 而且每小时都撞同一堵墙 ——
# 2026-11-12 就发生过一次: 普查数据提交留在本地, 后面所有推送都被挡住。
if [ -d .git/rebase-merge ] || [ -d .git/rebase-apply ]; then
    echo "发现残留的 rebase 状态, 先回滚"
    git rebase --abort 2>/dev/null || true
fi

# 候选列表由云端每 3 小时提交一次, 不拉就会拿旧候选普查。
# --autostash: 本机可能有未提交的实验改动, 别让它们挡住 rebase。
git pull --rebase --autostash -q origin main || echo "拉取失败, 用本地已有候选继续"

START=$(date +%s)
"$PY" verify.py --file data/nodes.json --refresh --cull --apply data/nodes.json \
    --out "$OUT"
rc=$?
echo "普查结束 rc=$rc, 用时 $(( $(date +%s) - START ))s"
[ $rc -ne 0 ] && exit $rc

# 只提交实测证据。产物 out/ 由云端生成, 这里绝不碰。
git add data/cn_pref.json data/pool_history.json
if git diff --cached --quiet; then
    echo "数据无变化, 不提交"
    exit 0
fi

"$PY" - <<'PY'
import json
p = json.load(open("data/cn_pref.json"))
ok = sum(1 for v in p.values() if v.get("ok") == 1)
fast = sum(1 for v in p.values()
           if v.get("ok") == 1 and (v.get("speed") or 0) > 0.05)
print(f"cn_pref {len(p)} 条, 国内可用 {ok} 个 (其中能传数据 {fast} 个)")
PY

git -c user.name="nodepool" -c user.email="bot@users.noreply.github.com" \
    commit -q -m "chore: 国内普查更新 ($(date +%F))" || exit 0

# 云端每 3 小时会自己提交一次产物, 直接推会被拒 → 变基后重试。
#
# 冲突几乎必然出在 data/*.json 上: 云端刚提交了一版 pool_history, 本机这边
# 也改了同一份, 而这两个文件都是单行/不可逐行合并的 JSON, git 无法自动合。
#
# 取舍: 冲突时**以本机这次普查为准**(rebase 里 --theirs 才是我方)。理由是本机
# 刚跑完实测, 数据比云端那份旧快照准; 云端 pool_history 里的 verify_ok(复检
# 次数)会在下一轮 build 重新填上, 丢了能自己长回来。
#
# 之前这里是 `git pull --rebase || break` —— 冲突直接把 rebase 半途留在工作区,
# 然后 exit 1, 之后所有推送全被这堵墙挡住, 只有人工 git rebase --abort 才解开。
for i in 1 2 3; do
    if git push -q origin main; then
        echo "已推送, 云端会自动重建订阅"
        exit 0
    fi
    echo "第 $i 次推送被拒, 变基后重试"
    git fetch -q origin main || { echo "fetch 失败, 放弃推送"; exit 1; }
    if git rebase -q origin/main; then
        continue
    fi
    echo "  rebase 冲突, 数据文件以本机普查为准"
    git checkout --theirs data/cn_pref.json data/pool_history.json 2>/dev/null || true
    git add data/cn_pref.json data/pool_history.json
    if ! git -c core.editor=true rebase --continue; then
        echo "  冲突解不掉, 回滚 rebase(本地提交保留, 需人工推)"
        git rebase --abort 2>/dev/null || true
        break
    fi
done
echo "推送失败, 本地已提交但未推送。下次运行会先 rebase 再试, 也可以手动 git push"
exit 1
