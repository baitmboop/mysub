#!/usr/bin/env python3
"""把"某节点连不上"的反馈写进中国实测记录, 让构建时把它摘掉。

用途: 你人在外面、电脑关着, 手机发现某几个节点不通时, 提一个 issue
写下节点名或 IP 即可。云端 workflow 会跑这个脚本, 不用我在场。

用法:
    python3 mark.py --text "N05 N07 连不上"
    python3 mark.py --text "5号 和第7个 不行"
    python3 mark.py --text "全部都不行"            # 不标记, 只请求重新筛选
    python3 mark.py --text "202.50.253.72 很慢" --dry-run
    python3 mark.py --text "@issue_body_file" --rebuild

能识别的写法（混着写也行）:
    节点序号   N05 / n5 / 第5个 / 5号
    地址       202.50.253.72
    整体      全部 / 全都 / 所有 / 都不行 / 都很慢 / 重测 / 换一批

在 CI 里跑时会把结果写进 $GITHUB_OUTPUT, 供后续步骤读取。
"""
from __future__ import annotations

import argparse
import os
import re
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import gateway  # noqa: E402

HERE = os.path.dirname(os.path.abspath(__file__))
OUT = os.path.join(HERE, "out")
SUB = os.path.join(OUT, "sub.yaml")
SUMMARY = "/tmp/mark_summary.txt"

IP_RE = re.compile(r"\b(?:\d{1,3}\.){3}\d{1,3}\b")
# 三种写法都要认: N05 / 第 5 个 / 5 号。位数放到 4 位 —— 手机端(base64 订阅)
# 曾经的节点名是 "n0158" 这种四位编号, 1-3 位会直接漏掉。
NAME_RE = re.compile(r"(?<![A-Za-z0-9])N\s?(\d{1,4})(?![\d.])", re.I)
ORD_RE = re.compile(r"第\s*(\d{1,4})\s*[个条]|(?<![\d.])(\d{1,4})\s*号")

# 整体性描述 —— 命中就不用逐个点名, 直接让云端重新筛一批
ALL_WORDS = ("全部", "全都", "所有", "整体", "普遍", "都不行", "都不能",
             "都连不上", "都很慢", "都是慢", "重测", "重新筛", "重新抓",
             "换一批", "换个批")


def strip_form_preamble(text: str) -> str:
    """issue 表单会把模板说明文字也写进正文, 只解析用户填的部分。

    表单渲染出来的正文形如:

        <模板说明>

        ### 范围
        某几个节点（在下面写）

        ### 哪些节点
        N05 N07

    模板说明里可能出现"第 5 个"这类举例, 不切掉就会被当成真实反馈,
    导致每次提 issue 都误标某几个节点。所以从第一个 "### " 开始读。
    """
    i = (text or "").find("### ")
    return text[i:] if i >= 0 else (text or "")


def wants_full_rebuild(text: str) -> bool:
    """用户是否在说"整体都不行", 需要重新采集筛选而不是摘掉某几个。"""
    t = strip_form_preamble(text)
    return any(w in t for w in ALL_WORDS)


def _read_yaml_map(m: dict) -> None:
    if not os.path.exists(SUB):
        return
    try:
        import yaml
        d = yaml.safe_load(open(SUB, encoding="utf-8")) or {}
    except Exception:
        return
    for p in d.get("proxies") or []:
        name = str(p.get("name") or "")
        srv = p.get("server")
        if not name or not srv:
            continue
        m[name.lower()] = srv
        hit = re.match(r"N\s?(\d{1,4})", name, re.I)
        if hit:
            m[f"N{int(hit.group(1)):02d}"] = srv
            m[f"n{int(hit.group(1)):04d}"] = srv
        m[srv] = srv


def _read_base64_map(m: dict) -> None:
    """base64 订阅里也可能有手机端显示的名字, 一并收进来。

    正常情况下两个订阅已经同名(都叫 "N01 1.2.3.4"), 但手机还留着旧订阅时
    显示的是 "n0158" 这种源站原名 —— 用户照着念出来的就是旧名字, 得能认。
    """
    p = os.path.join(OUT, "sub-base64.txt")
    if not os.path.exists(p):
        return
    try:
        import base64
        import collect
        raw = base64.b64decode(open(p, encoding="utf-8").read().strip()).decode()
    except Exception:
        return
    for line in raw.splitlines():
        line = line.strip()
        if not line or "://" not in line:
            continue
        try:
            n = collect.parse_uri(line, "sub")
        except Exception:
            continue
        if not n or not n.get("server"):
            continue
        name = str(n.get("name") or "").lower()
        if name:
            m.setdefault(name, n["server"])
            m.setdefault(name.lstrip("n0") or name, n["server"])
        m.setdefault(n["server"], n["server"])


def load_sub_map() -> dict:
    """节点名 → server。两种订阅格式都读, 无论手机显示哪个名字都能反查。"""
    m: dict = {}
    _read_yaml_map(m)
    _read_base64_map(m)
    return m


def resolve(text: str, sub_map: dict) -> tuple[set, list]:
    """从反馈文本里解析出目标服务器地址。返回 (地址集合, 未识别的序号)。"""
    text = strip_form_preamble(text)
    targets, missed = set(), []

    for ip in IP_RE.findall(text):
        targets.add(ip)

    nums = [int(m.group(1)) for m in NAME_RE.finditer(text)]
    for m in ORD_RE.finditer(text):
        nums.append(int(m.group(1) or m.group(2)))

    for num in nums:
        key = f"N{num:02d}"
        if key in sub_map:
            targets.add(sub_map[key])
            continue
        # 手机端(base64)历史上显示的是 n0158 这种四位编号, 按名字再试一次
        alt = f"n{num:04d}"
        if alt in sub_map:
            targets.add(sub_map[alt])
        elif key not in missed:
            missed.append(key)
    return targets, missed


def _write_output(**kv) -> None:
    """把结果写给 GitHub Actions 后续步骤用。本地跑时静默跳过。"""
    path = os.environ.get("GITHUB_OUTPUT")
    if not path:
        return
    try:
        with open(path, "a", encoding="utf-8") as f:
            for k, v in kv.items():
                f.write(f"{k}={v}\n")
    except OSError:
        pass


def _say(lines: list) -> None:
    for ln in lines:
        print(ln)
    try:
        with open(SUMMARY, "w", encoding="utf-8") as f:
            f.write("\n".join(lines) + "\n")
    except OSError:
        pass


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

    force = wants_full_rebuild(text)
    sub_map = load_sub_map()
    targets, missed = resolve(text, sub_map)

    says = []
    if targets:
        says.append(f"从反馈里解析到 {len(targets)} 个地址：{'、'.join(sorted(targets))}")
    if missed:
        says.append(f"这些序号在当前订阅里找不到，已跳过：{'、'.join(missed)}")

    # ---- 情况一: 整体都不行 → 不点名, 直接要求重新采集筛选 ----
    if force and not targets:
        says.append("判定为「整体不可用」，不做逐个标记，改为重新采集一批候选节点重新筛选。")
        _say(says)
        _write_output(marked="0", rebuild="true")
        return 0

    # ---- 情况二: 没有识别到任何目标 ----
    if not targets:
        says.append("没有识别到节点。请写节点序号（如 N05、第5个、5号）或 IP。")
        if force:
            says.append("若意思是「全部都不行」，写「全部都不行」四个字即可。")
        _say(says)
        _write_output(marked="0", rebuild="false")
        return 2

    # ---- 在历史池里找对应节点, 拿到 cn_pref 用的 server:port 键 ----
    history = gateway.load_json(gateway.HISTORY, {})
    hits = []
    for fp, rec in history.items():
        n = rec.get("node") or {}
        if n.get("server") in targets and n.get("port"):
            hits.append((fp, n))

    if not hits:
        says.append("这些地址已经不在历史池里（可能早被淘汰），无需处理。")
        says.append("正在重新采集一批候选节点来补充。")
        _say(says)
        _write_output(marked="0", rebuild="true")
        return 0

    pref = gateway.load_json(gateway.CN_PREF, {})
    now = int(time.time())
    for fp, n in hits:
        key = f"{n['server']}:{n['port']}"
        pref[key] = {"ok": 0, "at": now, "by": "user_report"}
        says.append(f"・标记不可用：{key}（{n.get('proto')}）")

    if args.dry_run:
        says.append("--dry-run：未写入")
        _say(says)
        _write_output(marked=str(len(hits)), rebuild="true")
        return 0

    gateway.save_json(gateway.CN_PREF, pref)
    says.append(f"已写入 {len(hits)} 条，7 天后自动失效（若是临时抽风会自己回到候选池）。")
    _say(says)
    _write_output(marked=str(len(hits)), rebuild="true")

    if args.rebuild:
        import subprocess
        print("\n重建订阅…")
        return subprocess.call([sys.executable, os.path.join(HERE, "ci_build.py"),
                                "--keep", "40", "--delay-limit", "3000"])
    return 0


if __name__ == "__main__":
    sys.exit(main())
