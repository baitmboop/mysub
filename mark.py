#!/usr/bin/env python3
"""把"某节点连不上"的反馈写进中国实测记录, 让构建时把它摘掉。

用途: 你人在外面、电脑关着, 手机测出某几个节点不通时, 提一个 GitHub issue
写下节点名或 IP 即可。云端 workflow 会跑这个脚本, 不用我在场。

用法:
    python3 mark.py --text "N05 N07 连不上"
    python3 mark.py --text "202.50.253.72 很慢" --dry-run
    python3 mark.py --text "@issue_body_file" --rebuild

匹配方式: 文本里出现的 IP 或节点名(N05)都会去当前订阅里反查。
"""
from __future__ import annotations

import argparse
import os
import re
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import gateway  # noqa: E402

OUT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "out")
SUB = os.path.join(OUT, "sub.yaml")

IP_RE = re.compile(r"\b(?:\d{1,3}\.){3}\d{1,3}\b")
NAME_RE = re.compile(r"\bN\s?(\d{1,3})\b", re.I)


def load_sub_map() -> dict:
    """节点名 → server, 用来把 "N05" 反查成具体地址。

    注释格式是 "N05 1.2.3.4", 顺手兼容只有序号的旧订阅。
    """
    if not os.path.exists(SUB):
        return {}
    try:
        import yaml
        d = yaml.safe_load(open(SUB, encoding="utf-8")) or {}
    except Exception:
        return {}
    m = {}
    for p in d.get("proxies") or []:
        name = str(p.get("name") or "")
        srv = p.get("server")
        if not name or not srv:
            continue
        m[name] = srv
        hit = NAME_RE.match(name)
        if hit:
            m[f"N{int(hit.group(1)):02d}"] = srv
    return m


def resolve(text: str, sub_map: dict) -> tuple[set, list]:
    """从反馈文本里解析出目标服务器地址。返回 (地址集合, 未识别的记号)。"""
    targets = set()
    missed = []
    for ip in IP_RE.findall(text or ""):
        targets.add(ip)
    for num in NAME_RE.findall(text or ""):
        key = f"N{int(num):02d}"
        if key in sub_map:
            targets.add(sub_map[key])
        else:
            missed.append(key)
    return targets, missed


def main() -> int:
    ap = argparse.ArgumentParser(description="把节点不可用反馈写进实测记录")
    ap.add_argument("--text", required=True,
                    help="反馈文本, 直接传字符串; 以 @ 开头则当作文件路径")
    ap.add_argument("--dry-run", action="store_true", help="只打印, 不写文件")
    ap.add_argument("--rebuild", action="store_true", help="写完顺便重建订阅")
    args = ap.parse_args()

    text = args.text
    if text.startswith("@"):
        p = text[1:]
        text = open(p, encoding="utf-8").read() if os.path.exists(p) else ""

    sub_map = load_sub_map()
    targets, missed = resolve(text, sub_map)
    print(f"从反馈里解析到 {len(targets)} 个地址: {sorted(targets)}")
    if missed:
        print(f"这些名字在当前订阅里找不到, 跳过: {missed}")
    if not targets:
        print("没有可识别的节点 —— 反馈里请写上节点名(如 N05)或 IP")
        return 2

    # 在历史池里找对应节点, 拿到 server:port 这个 cn_pref 用的键
    history = gateway.load_json(gateway.HISTORY, {})
    hits = []
    for fp, rec in history.items():
        n = rec.get("node") or {}
        if n.get("server") in targets and n.get("port"):
            hits.append((fp, n))

    if not hits:
        print("这些地址不在当前历史池里(可能已被淘汰), 无需处理")
        return 0

    pref = gateway.load_json(gateway.CN_PREF, {})
    now = int(time.time())
    for fp, n in hits:
        key = f"{n['server']}:{n['port']}"
        pref[key] = {"ok": 0, "at": now, "by": "user_report"}
        print(f"  标记不可用: {key}  ({n.get('proto')})")

    if args.dry_run:
        print("--dry-run: 未写入")
        return 0

    gateway.save_json(gateway.CN_PREF, pref)
    print(f"已写入 {len(hits)} 条 → data/cn_pref.json (7 天后自动失效)")
    print("下一轮构建会把这些节点摘掉。若反馈有误, 删掉对应条目即可恢复。")

    if args.rebuild:
        import subprocess
        print("\n重建订阅…")
        rc = subprocess.call([sys.executable,
                              os.path.join(os.path.dirname(SUB), "..", "ci_build.py"),
                              "--keep", "40", "--delay-limit", "3000"])
        return rc
    return 0


if __name__ == "__main__":
    sys.exit(main())
