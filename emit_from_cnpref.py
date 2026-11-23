#!/usr/bin/env python3
"""紧急重建订阅: 直接用已有的国内实测结论, 不跑任何探测。

为什么需要这个脚本
    正常路径是 ci_build.py: 采集 → 云端复检 → 并入历史池 → 按 cn_pref 排序。
    但历史池只保留 "7 天内进过订阅的" 节点(约 300 个), 和 cn_pref 里
    "实测能传数据"的节点(42 个)交集很小 —— 2026-11-23 实测只有 12 个。

    普查(verify.py)跑一轮要 2 小时, 期间用户的订阅是坏的。这个脚本绕开
    历史池, 直接从 data/nodes.json(全量配置) ∩ data/cn_pref.json(实测结论)
    取交集, 秒级产出可用的订阅。

    代价要说清楚: cn_pref 是 10 天前的结论, 节点可能已经死了。但"10 天前
    能传数据"远好过"从没测过的彩票"。这是一份过渡产物, 普查完成后会被
    ci_build.py 的正常产出覆盖。

用法:
    python3 emit_from_cnpref.py              # 只保留实测能传数据的
    python3 emit_from_cnpref.py --include-ok # 连"连得上但传不动"的也保留
"""
import argparse
import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import gateway  # noqa: E402
from ci_build import write_outputs  # noqa: E402

NODES = os.path.join(gateway.DATA, "nodes.json")


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--include-ok", action="store_true",
                    help="把 ok=1 但没测出速度的也带上(排最后)")
    ap.add_argument("--min-speed", type=float, default=0.05,
                    help="算作'能传数据'的最低速度 MB/s")
    args = ap.parse_args()

    nodes = gateway.load_json(NODES, [])
    cn = gateway.load_json(gateway.CN_PREF, {})
    if not isinstance(nodes, list) or not nodes:
        print(f"读不出 {NODES} 或为空, 中止")
        return 1

    # 并上"普查时的配置清单"。
    #
    # data/nodes.json 是云端每 3 小时按当前源重建的; 源里下架了的节点配置就没了,
    # 但它在 cn_pref 里的实测结论还留着 7 天。只读 nodes.json 会让"刚测通、
    # 配置已被源下架"的节点彻底消失 —— 2026-11-23 实测这一项就丢了 28 个。
    #
    # 清单由 cn_refresh.sh 在开测前写, 缺了就跳过(老版本没有这个文件)。
    seen = {(n.get("server"), n.get("port")) for n in nodes if isinstance(n, dict)}
    snap = os.path.join(gateway.DATA, "nodes.at-census.json")
    if os.path.exists(snap):
        extra = gateway.load_json(snap, [])
        added = 0
        for n in extra if isinstance(extra, list) else []:
            if not isinstance(n, dict) or not n.get("server") or not n.get("port"):
                continue
            if (n["server"], n["port"]) in seen:
                continue
            seen.add((n["server"], n["port"]))
            nodes.append(n)
            added += 1
        if added:
            print(f"   从普查清单补回 {added} 个已下架节点的配置")
    else:
        print("   (无 data/nodes.at-census.json, 只用当前 nodes.json)")
    if not isinstance(cn, dict) or not cn:
        print(f"读不出 {gateway.CN_PREF} 或为空, 中止")
        return 1

    # 同一个节点可能在 nodes.json 里出现多次(不同来源采到同一个 server:port),
    # 按 fp 去重, 保留实测速度最高的那条。
    best: dict[str, dict] = {}
    for n in nodes:
        if not isinstance(n, dict) or not n.get("server") or not n.get("port"):
            continue
        key = f"{n['server']}:{n['port']}"
        v = cn.get(key)
        if not isinstance(v, dict) or v.get("ok") != 1:
            continue
        speed = v.get("speed") or 0.0
        if speed < args.min_speed and not args.include_ok:
            continue

        n = dict(n)
        n["cn_ok"] = True
        n["cn_speed"] = speed
        n["cn_latency"] = v.get("latency")
        n["cn_at"] = v.get("at")
        fp = n.get("fp") or gateway.fingerprint(n)
        n["fp"] = fp
        old = best.get(fp)
        if old is None or (n["cn_speed"] or 0) > (old["cn_speed"] or 0):
            best[fp] = n

    pool = sorted(best.values(), key=lambda n: -(n.get("cn_speed") or 0))

    # cn_pref 的 key 只有 "server:port", 不带 uuid —— 同一台服务器上的多个
    # 账号变体在实测结论里是同一个结论, 无法区分"通的到底是哪个 uuid"。
    # 所以不能按 server:port 硬去重(可能删掉唯一有效的那个), 但要限制每台
    # 服务器的条目数: 2026-11-23 实测, 一个 vmess 服务器占了 5 条, 26 个节点
    # 里有 5 个是同一个 IP, 订阅看起来像坏了。
    per_server_cap = 2
    seen_server: dict[str, int] = {}
    kept = []
    for n in pool:
        sk = f"{n['server']}:{n['port']}"
        if seen_server.get(sk, 0) >= per_server_cap:
            continue
        seen_server[sk] = seen_server.get(sk, 0) + 1
        kept.append(n)
    pool = kept

    if not pool:
        print("没有任何节点同时满足 [在 nodes.json] 且 [实测能传数据], 中止")
        return 1

    newest = max((n.get("cn_at") or 0) for n in pool)
    import time
    age_d = (time.time() - newest) / 86400 if newest else -1

    write_outputs(pool, [], [], n_collected=len(nodes), n_delay=0,
                  n_geo=0, n_verified=0, n_picked=0, n_history=len(pool))

    # write_outputs 的报告按 cn_pref 的 7 天窗口算"有效条数", 会把这里算成 0
    # 并打出"实测数据已全部过期"—— 但这份产物恰恰是用那份超期数据做的。
    # 报告必须说清它到底是什么, 不能自相矛盾。
    import time as _t
    stamp = _t.strftime("%Y-%m-%d %H:%M", _t.localtime(newest)) if newest else "未知"

    # 订阅里真实有几个节点 —— 必须去基数, 不能沿用 pool 的长度。
    #
    # 2026-11-23 实测: pool 63 个, base64 里只有 53 个。差的那 10 个是
    # gateway.build_base64 按 (server, port) 去重掉的, 报告却照抄 63。
    # 用户拿 63 去核对客户端看到 53, 只会以为是客户端出错。
    import base64 as _b64
    import ci_build
    sub_path = os.path.join(ci_build.OUT, "sub-base64.txt")
    n_sub = len(pool)
    try:
        with open(sub_path, encoding="utf-8") as f:
            raw = f.read().strip()
        text = _b64.b64decode(raw + "=" * (-len(raw) % 4)).decode("utf-8", "replace")
        n_sub = len([x for x in text.strip().splitlines() if x.strip()])
    except Exception:
        pass

    # 过期与否要按真实天数说, 不能写死。
    if age_d > 7:
        fresh_note = (f"已超出 7 天有效期 —— 结论可能过时, 节点可能已死。")
        age_warn = "⚠️ "
    else:
        fresh_note = "仍在 7 天有效期内。"
        age_warn = ""

    lines = [
        "# 节点池日报（应急重建）", "",
        f"- 生成时间: {_t.strftime('%Y-%m-%d %H:%M:%S')} (UTC)",
        f"- ⚠️ **这是一份应急产物**: 国内普查正在重跑, 本文件由"
        f" `emit_from_cnpref.py` 直接从「全量节点配置 ∩ 历史实测结论」生成,"
        f" 未做任何新的探测。",
        f"- 国内实测来源: {stamp} (**{age_d:.1f} 天前**), {age_warn}{fresh_note}",
        f"- 最终订阅: **{n_sub}** 个节点"
        + (f"（配置池 {len(pool)} 个，按 server:port 去重后 {n_sub} 个）"
           if n_sub != len(pool) else "")
        + ", 全部是当时实测能传数据的",
        f"- 下一轮 `ci_build.py` 会用新的普查结果整体覆盖本文件", "",
        "| # | 协议 | 服务器 | 当时速度 | 来源 |",
        "|---|------|--------|----------|------|",
    ]
    for i, n in enumerate(pool, 1):
        sp = n.get("cn_speed") or 0
        lines.append(f"| {i} | {n.get('proto')} | {n['server']}:{n['port']} | "
                     f"{sp * 1024:.0f} KB/s | {n.get('source') or '-'} |")
    age_txt = "今天" if age_d < 1 else f"{age_d:.0f} 天前"
    lines += ["", f"> 「当时速度」是{age_txt}国内实测的结果, 不代表现在还能跑。"]
    import ci_build
    os.makedirs(ci_build.OUT, exist_ok=True)
    with open(os.path.join(ci_build.OUT, "report.md"), "w", encoding="utf-8") as f:
        f.write("\n".join(lines) + "\n")

    print(f"✅ 生成 {len(pool)} 个节点 (实测数据 {age_d:.1f} 天新)")
    print(f"   其中能传数据 {sum(1 for n in pool if (n.get('cn_speed') or 0) > 0)} 个")
    for n in pool[:8]:
        print(f"   {n.get('proto'):<7} {n['server']}:{n['port']}  "
              f"{n['cn_speed'] * 1024:.0f} KB/s  {n.get('source') or '-'}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
