#!/usr/bin/env python3
"""维护"已知可用节点的完整配置"库 —— data/known_good.json

解决什么问题
    data/nodes.json 是云端每 3 小时按**当前源**重建的, 源里下架了的节点,
    配置(uuid / path / sni / ws-host)就没了。但 verify.py 写进 cn_pref 的
    实测结论还保留 7 天 —— 结果是"6 小时前刚测出 2.5MB/s 的节点, 现在
    发不进订阅", 而且从任何报告上都看不出来。

    2026-11-23 实测: cn_pref 里 83 个能传数据, 只有 34 个的配置还在
    nodes.json 里; 补回配置后订阅从 18/39 涨到 62。

为什么不是直接提交 nodes.json 快照
    那份 2.3MB, 每天一份, 一年会把 git 仓库撑到 800MB。这里只存
    "实测能传数据"的那几十个节点, 约 40KB, 而且会按 cn_pref 的有效期
    自动修剪 —— 大小基本恒定。

用法
    python3 archive_known_good.py            # 更新已知可用库
    python3 archive_known_good.py --stats    # 只看现状
"""
import argparse
import json
import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import gateway  # noqa: E402

OUT = os.path.join(gateway.DATA, "known_good.json")
MIN_SPEED = 0.05          # MB/s, 与 ci_build / emit_from_cnpref 保持一致
KEEP_DAYS = 7             # 与 gateway.load_cn_pref 的窗口一致


def node_key(n: dict) -> str:
    return f"{n.get('server')}:{n.get('port')}"


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--stats", action="store_true", help="只打印统计, 不写文件")
    ap.add_argument("--nodes", default=os.path.join(gateway.DATA, "nodes.json"))
    args = ap.parse_args()

    cn = gateway.load_json(gateway.CN_PREF, {})
    nodes = gateway.load_json(args.nodes, [])
    if not isinstance(cn, dict) or not isinstance(nodes, list):
        print("读不出 cn_pref / nodes.json, 中止")
        return 1

    # 并上普查时的配置快照: 有些节点的配置已经被源下架, 但它是本轮真测通过的,
    # 正是这个库存在的意义。快照由 cn_refresh.sh 写, 不存在就跳过。
    snap = os.path.join(gateway.DATA, "nodes.at-census.json")
    if os.path.exists(snap):
        extra_nodes = gateway.load_json(snap, [])
        have = {(n.get("server"), n.get("port")) for n in nodes if isinstance(n, dict)}
        n_add = 0
        for n in extra_nodes if isinstance(extra_nodes, list) else []:
            if not isinstance(n, dict) or not n.get("server") or not n.get("port"):
                continue
            if (n["server"], n["port"]) in have:
                continue
            have.add((n["server"], n["port"]))
            nodes.append(n)
            n_add += 1
        if n_add:
            print(f"并入普查快照 {n_add} 个配置")

    cutoff = time.time() - KEEP_DAYS * 86400
    known = gateway.load_json(OUT, {})
    if not isinstance(known, dict):
        known = {}
    before = len(known)

    # 1. 把本轮配置存在、且实测能传数据的节点收进库里
    by_key = {}
    for n in nodes:
        if isinstance(n, dict) and n.get("server") and n.get("port"):
            by_key.setdefault(node_key(n), n)

    added = updated = 0
    for key, v in cn.items():
        if not isinstance(v, dict) or v.get("ok") != 1:
            continue
        if (v.get("speed") or 0) < MIN_SPEED or v.get("at", 0) < cutoff:
            continue
        n = by_key.get(key)
        if not n:
            continue                       # 本轮配置里没有, 只能靠库里原有的
        rec = dict(n)
        rec["cn_speed"] = v.get("speed")
        rec["cn_latency"] = v.get("latency")
        rec["cn_at"] = v.get("at")
        rec["archived_at"] = int(time.time())
        if key in known:
            updated += 1
        else:
            added += 1
        known[key] = rec

    # 2. 修剪: cn_pref 过期或已判定不通的, 从库里删掉
    dropped = 0
    for key in list(known):
        v = cn.get(key) or {}
        fresh = isinstance(v, dict) and v.get("at", 0) >= cutoff
        ok = isinstance(v, dict) and v.get("ok") == 1 and (v.get("speed") or 0) >= MIN_SPEED
        if not (fresh and ok):
            known.pop(key, None)
            dropped += 1

    n_ok_cn = sum(1 for v in cn.values()
                  if isinstance(v, dict) and v.get("ok") == 1
                  and (v.get("speed") or 0) >= MIN_SPEED and v.get("at", 0) >= cutoff)
    n_in_nodes = sum(1 for k in cn if k in by_key
                     and isinstance(cn[k], dict) and cn[k].get("ok") == 1
                     and (cn[k].get("speed") or 0) >= MIN_SPEED
                     and cn[k].get("at", 0) >= cutoff)

    print(f"cn_pref 中能传数据(7 天内) {n_ok_cn} 个, 其中 {n_in_nodes} 个的配置仍在 nodes.json")
    print(f"known_good: {before} → {len(known)} 个 (新增 {added} / 更新 {updated} / 修剪 {dropped})")

    if args.stats:
        return 0

    os.makedirs(gateway.DATA, exist_ok=True)
    tmp = OUT + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(known, f, ensure_ascii=False, indent=0, sort_keys=True)
    os.replace(tmp, OUT)
    print(f"已写入 {OUT} ({os.path.getsize(OUT)/1024:.1f} KB)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
