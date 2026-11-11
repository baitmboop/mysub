#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
云端构建 —— 给 GitHub Actions 用（也能在本地跑，方便调试）

和本地 refresh.sh 的四个区别
--------------------------
1. **不做带宽测试**。Actions 机器在美国/欧洲，测出来的速度对国内用户不作数，
   白花五分钟还会误导排序。速度交给手机上的客户端自己测（它在你本地，才是真的）。
2. **不起 HTTP 服务**，直接产出静态文件，提交回仓库就是订阅。
3. 历史池走仓库里的 data/pool_history.json，每次 commit 回去。
4. **不需要代理** —— runner 本身就在境外，采集直连即可。

真正保留的核心筛选：延迟粗筛 + 多站点复检。
死节点在哪个机房测都是死的，这一层云端做得比本机还准。

用法
----
    python3 ci_build.py                 # 默认
    python3 ci_build.py --no-verify     # 跳过复检
    python3 ci_build.py --keep 60       # 保留更多
"""
from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import time
from concurrent.futures import ThreadPoolExecutor

ROOT = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, ROOT)

import probe                                    # noqa: E402
import gateway                                  # noqa: E402

DATA = os.path.join(ROOT, "data")
OUT = os.path.join(ROOT, "out")
HISTORY = os.path.join(DATA, "pool_history.json")
PRIVATE = os.path.join(DATA, "private.yaml")
FALLBACK = os.path.join(DATA, "fallback.yaml")


def log(m: str) -> None:
    print(f"[{time.strftime('%H:%M:%S')}] {m}", flush=True)


def cloud_namer(n: dict, i: int) -> str:
    """订阅里的节点名。用纯序号, 避免延迟波动导致名字天天变。"""
    return f"N{i:02d}"


def main() -> int:
    ap = argparse.ArgumentParser(description="云端构建节点订阅")
    ap.add_argument("--nodes", default=os.path.join(DATA, "nodes.json"))
    ap.add_argument("--keep", type=int, default=40, help="最终保留节点数")
    ap.add_argument("--delay-limit", type=int, default=2500, help="延迟准入 ms")
    ap.add_argument("--workers", type=int, default=64)
    ap.add_argument("--verify-workers", type=int, default=16)
    ap.add_argument("--verify-min", type=int, default=2)
    ap.add_argument("--no-verify", action="store_true")
    ap.add_argument("--keep-days", type=float, default=7, help="历史池保留天数")
    args = ap.parse_args()

    if not os.path.exists(args.nodes):
        log(f"✗ 找不到 {args.nodes}, 先跑 collect.py")
        return 1

    nodes = json.load(open(args.nodes))
    log(f"载入 {len(nodes)} 个节点")

    # ---------- 1. 生成内核配置(会自动剔除非法节点) ----------
    log("生成内核配置...")
    name_map, n_ok = probe.build_config(nodes)
    log(f"可转换 {n_ok} 个, 丢弃 {len(nodes) - n_ok} 个")

    proc = subprocess.Popen(
        [probe.MIHOMO, "-f", probe.CFG],
        stdout=subprocess.DEVNULL, stderr=subprocess.STDOUT, cwd=ROOT,
        preexec_fn=os.setsid if hasattr(os, "setsid") else None,
    )

    try:
        if not probe.wait_ready(proc):
            log("✗ 内核启动失败")
            return 1

        # ---------- 2. 延迟粗筛 ----------
        log(f"延迟测试 {len(name_map)} 个 ({args.workers} 并发)...")
        t0 = time.time()
        cand = []
        with ThreadPoolExecutor(max_workers=args.workers) as ex:
            futs = {ex.submit(probe.delay_test, nm): nm for nm in name_map}
            for f in futs:
                nm = futs[f]
                try:
                    med, jitter = f.result()
                except Exception:
                    med, jitter = None, None
                if med and med <= args.delay_limit:
                    n = dict(name_map[nm])
                    n.update(name=nm, latency=med, jitter=jitter)
                    cand.append(n)
        log(f"✓ 存活 {len(cand)} 个 (≤{args.delay_limit}ms), 耗时 {time.time()-t0:.0f}s")

        if not cand:
            log("✗ 本轮没有存活节点, 保留上一轮产物")
            return 1

        # ---------- 3. 多站点复检 ----------
        if args.no_verify:
            for n in cand:
                n["verify_ok"] = None
        else:
            cand.sort(key=lambda x: x["latency"])
            cand = cand[: max(args.keep * 3, 60)]
            log(f"多站点复检 {len(cand)} 个 × {len(probe.VERIFY_SITES)} 站...")
            t0 = time.time()

            def _v(n):
                try:
                    return probe.verify_multi(n["name"])
                except Exception:
                    return 0, {}

            with ThreadPoolExecutor(max_workers=args.verify_workers) as ex:
                for n, (ok, detail) in zip(cand, ex.map(_v, cand)):
                    n["verify_ok"] = ok
                    n["verify"] = detail

            good = [n for n in cand if n["verify_ok"] >= args.verify_min]
            if len(good) < 3:
                relax = [n for n in cand if n["verify_ok"] >= 1]
                log(f"! 严格标准仅 {len(good)} 个, 放宽到 ≥1 站 → {len(relax)} 个")
                good = relax or cand
            cand = good
            log(f"✓ 复检后 {len(cand)} 个, 耗时 {time.time()-t0:.0f}s")

        # ---------- 4. 排序 + 评分 ----------
        mx = max((n["latency"] for n in cand), default=1) or 1
        for n in cand:
            v = n.get("verify_ok") or 0
            n["score"] = round(v / len(probe.VERIFY_SITES) * 0.8
                               + (1 - n["latency"] / mx) * 0.2, 4)
            n["speed"] = 0.0  # 云端不测速, 占位保持字段一致

        cand.sort(key=lambda n: (-(n.get("verify_ok") or 0), n.get("score") or 0))
        picked = cand[: args.keep]

        # ---------- 5. 并入历史池 ----------
        history = gateway.load_json(HISTORY, {})
        before = len(history)
        history = gateway.merge_history(history, picked, keep_days=args.keep_days)
        gateway.save_json(HISTORY, history)
        log(f"历史池 {before} → {len(history)}")

        pool = gateway.pool_nodes(history)
        private = gateway.parse_private(PRIVATE)
        fallback = gateway.parse_private(FALLBACK)
        log(f"订阅池 {len(pool)} 个 (自建 {len(private)} / 额度 {len(fallback)})")

        # ---------- 6. 产出 ----------
        os.makedirs(OUT, exist_ok=True)
        sub_yaml = gateway.build_config(pool, private, fallback,
                                       namer=cloud_namer, runtime=False)
        sub_b64 = gateway.build_base64(pool, private, fallback)
        with open(os.path.join(OUT, "sub.yaml"), "w", encoding="utf-8") as f:
            f.write(sub_yaml)
        with open(os.path.join(OUT, "sub-base64.txt"), "w", encoding="utf-8") as f:
            f.write(sub_b64)
        with open(os.path.join(OUT, "nodes_best.json"), "w", encoding="utf-8") as f:
            json.dump(picked, f, ensure_ascii=False, indent=1)

        # 报告
        lines = [
            "# 节点池日报", "",
            f"- 生成时间: {time.strftime('%Y-%m-%d %H:%M:%S')} (UTC)",
            f"- 采集去重: **{len(nodes)}** 个",
            f"- 延迟存活: **{len([n for n in cand])}** 个 (≤{args.delay_limit}ms)",
            f"- 复检合格: **{len(picked)}** 个",
            f"- 历史池: **{len(history)}** 个 (含 7 天内可用过的)",
            f"- 最终订阅: **{len(pool)}** 个节点", "",
            "| # | 协议 | 服务器 | 延迟 | 复检 | 评分 | 来源 |",
            "|---|------|--------|------|------|------|------|",
        ]
        for i, n in enumerate(pool[:60], 1):
            v = n.get("verify_ok")
            vs = f"{v}/{len(probe.VERIFY_SITES)}" if v is not None else "-"
            lines.append(
                f"| {i} | {n.get('proto')} | {n.get('server')}:{n.get('port')} | "
                f"{n.get('latency') or '-'}ms | {vs} | {n.get('score') or 0:.3f} | "
                f"{n.get('source') or '-'} |")
        lines += ["", "> 速度未在本机测 —— 客户端会自动选最快的，不用我替你判断。"]
        with open(os.path.join(OUT, "report.md"), "w", encoding="utf-8") as f:
            f.write("\n".join(lines) + "\n")

        log(f"✅ 完成: sub.yaml / sub-base64.txt / report.md ({len(pool)} 个节点)")
        return 0
    finally:
        try:
            if hasattr(os, "killpg"):
                os.killpg(os.getpgid(proc.pid), 15)
            else:
                proc.terminate()
        except Exception:
            pass


if __name__ == "__main__":
    sys.exit(main())
