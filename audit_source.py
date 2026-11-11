#!/usr/bin/env python3
"""源体检器 —— 给一个新源地址, 就地判断"值不值得转正"。

    python3 audit_source.py <url> [<url> ...] [--kind auto|http|socks5]
    python3 audit_source.py --file data/candidates.txt --sample 40 --workers 4

为什么需要它
------------
现有 collect.py 只按"抽到多少条"算贡献, 但抽到 1800 条全是死节点的源
(实测 anaer/Sub 就是) 对订阅毫无意义。真正要回答的是三件事:

    raw   —— 这个源能抽出多少节点
    new   —— 其中多少个是本池没有的 (独立贡献, 转贴源在这里归零)
    alive —— 抽出来的节点, 在你现在这条网络上真能用几个

只看前两个会挑出一堆镜像仓库, 只看第三个会被"一次抖动"骗。三个一起看,
再叠加实测速度, 才能判断"加它能不能让订阅变厚"。

支持四种源形态
--------------
1. URI 列表 / 整份 base64 订阅   —— vmess|vless|ss|trojan:// 正则
2. Clash YAML (proxies:)         —— 走 collect.nodes_from_yaml
3. 纯 ip:port 列表               —— 开放代理池, 按 --kind 生成 http/socks5 节点
   (开放代理池没有"协议"标记, 而 http 和 socks5 的报文完全不同, 猜错就全灭,
    所以默认两种都试一遍, 各取一半样本, 谁活率高信谁)
4. tg 频道名 (@xxx)              —— 走 collect 的网页预览抓取

安全约定: 只读源, 不改 data/ 下任何文件。要并入必须显式 --save。
"""
from __future__ import annotations

import argparse
import json
import os
import random
import re
import sys
import time
from concurrent.futures import ThreadPoolExecutor

ROOT = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, ROOT)

import collect  # noqa: E402
import gateway   # noqa: E402
import sweep_cn  # noqa: E402

IPPORT = re.compile(r"^\s*(\d{1,3}(?:\.\d{1,3}){3}):(\d{1,5})\s*$")
REPORT = "/tmp/source_audit.json"


def log(m: str = "") -> None:
    print(m, flush=True)


def fetch_text(url: str, proxy: str | None) -> str | None:
    """抓一份源。直连不通就换 gh 镜像 —— raw.githubusercontent.com 在国内
    是"时通时断"的, 一次 TLS 握手超时会让整轮体检得出"源已失效"的假结论。"""
    tried = [url]
    if "raw.githubusercontent.com" in url:
        tried.append(url.replace("https://raw.githubusercontent.com",
                                 "https://gh-proxy.com/https://raw.githubusercontent.com"))
    for u in tried:
        data = collect.fetch(u, collect.build_opener(proxy), timeout=30, retries=2)
        if data:
            return collect.maybe_b64(data.decode("utf-8", "ignore"))
    return None


def parse_any(text: str, source: str, kind: str) -> list[dict]:
    """把一份内容解析成节点列表, 覆盖 URI / YAML / ip:port 三种形态。"""
    nodes = collect.nodes_from_yaml(text, source)
    if nodes:
        return nodes

    for m in collect.VG_REGEX.finditer(text):
        n = collect.parse_uri(m.group(0), source)
        if n and n.get("server") and n.get("port"):
            nodes.append(n)

    if nodes:
        return nodes

    # ip:port 形态。tls 一律关掉: 开放代理池里绝大多数是明文 CONNECT,
    # 少数支持 TLS 的写 tls=true 也连不上, 反而让 TLS 握手白白多花几秒。
    kinds = ["http", "socks5"] if kind == "auto" else [kind]
    for line in text.splitlines():
        m = IPPORT.match(line)
        if not m:
            continue
        for k in kinds:
            nodes.append({"proto": k, "server": m.group(1), "port": int(m.group(2)),
                          "tls": False, "name": "", "source": source})
    return nodes


def dedup_against_pool(nodes: list[dict]) -> tuple[list[dict], int]:
    """和现有历史池比, 返回 (新节点, 重复数)。"""
    hist = gateway.load_json(gateway.HISTORY, {})
    known = {f"{v['node'].get('server')}:{v['node'].get('port')}" for v in hist.values()}
    seen, fresh, dup = set(), [], 0
    for n in nodes:
        key = f"{n['server']}:{n['port']}"
        if key in seen:
            continue
        seen.add(key)
        if key in known:
            dup += 1
            continue
        fresh.append(n)
    return fresh, dup


def test_batch(nodes: list[dict], workers: int) -> list[dict]:
    """逐个隔离实测, 返回带实测结果的节点。"""
    out = []

    def one(n):
        pr = gateway.to_mihomo(n, "T")
        if not pr:
            return n, None
        return n, sweep_cn.measure(pr)

    with ThreadPoolExecutor(max_workers=workers) as ex:
        for i, (n, r) in enumerate(ex.map(one, nodes), 1):
            n["_test"] = r
            out.append(n)
            if r and r.get("ok"):
                spd = round((r.get("speed") or 0) * 1024)
                log(f"  [{i:3}/{len(nodes)}] ✓ {n['proto']:9} {n['server']}:{n['port']} "
                    f"{spd} KB/s  {r.get('latency')}ms")
            elif i % 10 == 0 or i == len(nodes):
                log(f"  [{i:3}/{len(nodes)}] ...")
    return out


def audit(url: str, kind: str, sample: int, workers: int, proxy: str | None,
          seed: int) -> dict:
    log(f"\n=== {url}")
    text = fetch_text(url, proxy)
    if not text:
        log("  ! 抓不到内容")
        return {"url": url, "ok": False, "reason": "fetch failed"}

    nodes = parse_any(text, url, kind)
    log(f"  抽出 {len(nodes)} 条")
    if not nodes:
        log("  ! 解析不出节点")
        return {"url": url, "ok": False, "reason": "no nodes parsed", "raw": 0}

    fresh, dup = dedup_against_pool(nodes)
    log(f"  本池没有的 {len(fresh)} 条 (与现有池重复 {dup} 条)")
    if not fresh:
        return {"url": url, "ok": True, "raw": len(nodes), "new": 0,
                "tested": 0, "alive": 0, "fast": 0, "samples": []}

    # 采样: 开放代理池动辄上万条, 全测要几小时; 随机采样才不会有顺序偏差
    # (列表常按国家/延迟排序, 取前 N 个等于只测了同一批机房)。
    random.seed(seed)
    todo = fresh if (sample <= 0 or len(fresh) <= sample) else random.sample(fresh, sample)
    log(f"  实测 {len(todo)} 条 (并发 {workers})")

    t0 = time.time()
    tested = test_batch(todo, workers)

    alive = [n for n in tested if n.get("_test") and n["_test"].get("ok")]
    fast = [n for n in alive if (n["_test"].get("speed") or 0) >= 0.05]
    fast.sort(key=lambda n: -(n["_test"].get("speed") or 0))

    log(f"  → 连通 {len(alive)}/{len(todo)}, 其中能传数据(≥50KB/s) {len(fast)} 个"
        f"  用时 {time.time() - t0:.0f}s")
    for n in fast[:10]:
        log(f"      {round((n['_test']['speed']) * 1024):5d} KB/s  {n['proto']:8} "
            f"{n['server']}:{n['port']}")

    return {
        "url": url, "ok": True,
        "raw": len(nodes), "new": len(fresh), "tested": len(todo),
        "alive": len(alive), "fast": len(fast),
        # 完整的节点结构(带实测结果), --save 靠它收编; 报告里只留够用的字段。
        "alive_nodes": [dict(n) for n in alive],
        "samples": [{"proto": n["proto"], "server": n["server"], "port": n["port"],
                     **{k: v for k, v in n.items()
                        if k not in ("_test", "source", "name")},
                     "speed": n["_test"].get("speed"),
                     "latency": n["_test"].get("latency")} for n in fast[:50]],
    }


def admit(items: list[dict]) -> int:
    """把实测通过的节点正式并入历史池 + 国内实测表。

    这是 "源 → 体检 → 收编" 的最后一环。只写实测证据, 不写"我觉得它能用":
    能走到这里的节点都已经在本机连通过、并且跑出过下载速度。

    verify_ok 故意留空 —— 那是"云端三站复检"的字段, 这里没做那件事。
    pool_nodes 只在 verify_ok 非空时用它过滤, 留空就不会被误判成不合格。
    """
    hist = gateway.load_json(gateway.HISTORY, {})
    pref = gateway.load_json(gateway.CN_PREF, {})
    now = int(time.time())
    n_add = 0
    for it in items:
        t = it.get("_test") or {}
        if not (t.get("ok") and (t.get("speed") or 0) > 0):
            continue
        node = {k: v for k, v in it.items()
                if k not in ("_test", "speed", "latency", "fp")}
        node["fp"] = gateway.fp_key(node)
        rec = hist.get(node["fp"]) or {"first_seen": now, "ok_hits": 0}
        rec["node"] = node
        rec["last_ok"] = now
        rec["ok_hits"] = rec.get("ok_hits", 0) + 1
        rec["latency"] = t.get("latency")
        rec["speed"] = t.get("speed")
        hist[node["fp"]] = rec

        key = f"{node['server']}:{node['port']}"
        old = pref.get(key) or {}
        pref[key] = {"ok": 1, "latency": t.get("latency"),
                     "speed": max(t.get("speed") or 0, old.get("speed") or 0),
                     "at": now, "by": "auto"}
        n_add += 1
    gateway.save_json(gateway.HISTORY, hist)
    gateway.save_json(gateway.CN_PREF, pref)
    return n_add


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("urls", nargs="*", help="源地址, 可以给多个")
    ap.add_argument("--file", help="一行一个地址的文件")
    ap.add_argument("--kind", default="auto", choices=["auto", "http", "socks5"],
                    help="纯 ip:port 列表按哪种协议测 (auto = 两种都测)")
    ap.add_argument("--sample", type=int, default=30,
                    help="每个源最多实测多少条 (0 = 全部)")
    ap.add_argument("--workers", type=int, default=4)
    ap.add_argument("--proxy", default=None, help="抓取走这个代理")
    ap.add_argument("--seed", type=int, default=11)
    ap.add_argument("--save", action="store_true",
                    help="把本轮实测能传数据的节点并入池子(历史池 + 国内实测表)")
    ap.add_argument("--admit", metavar="JSON",
                    help="把一份已测好的结果(含 _test 字段)并入池子, 不重测")
    args = ap.parse_args()

    urls = list(args.urls)
    if args.file:
        urls += [ln.strip() for ln in open(args.file, encoding="utf-8")
                 if ln.strip() and not ln.strip().startswith("#")]
    if not urls and not args.admit:
        log("给个地址: audit_source.py <url> [--kind http]")
        return 2

    reports = [audit(u, args.kind, args.sample, args.workers, args.proxy, args.seed)
               for u in urls]
    gateway.save_json(REPORT, reports)

    log("\n=== 汇总 ===")
    log(f"{'源':<62} {'raw':>5} {'new':>5} {'测':>4} {'通':>4} {'快':>4}")
    for r in reports:
        name = r["url"][:60]
        if not r.get("ok"):
            log(f"{name:<62} {r.get('raw', 0):>5} {'-':>5} {'-':>4} {'-':>4} {'-':>4}  ✗")
            continue
        log(f"{name:<62} {r['raw']:>5} {r['new']:>5} {r['tested']:>4} "
            f"{r['alive']:>4} {r['fast']:>4}")

    if args.admit:
        items = json.load(open(args.admit, encoding="utf-8"))
        n = admit(items)
        log(f"\n已并入池子 {n} 个节点 (来源: {args.admit})")

    if args.save:
        # 只有真跑出过下载速度的才算数; 单纯"连得上"的留给下一轮复测。
        pool_items = []
        for u in urls:
            r = next((x for x in reports if x["url"] == u), None)
            pool_items += (r or {}).get("alive_nodes", [])
        n = admit(pool_items)
        log(f"\n已并入池子 {n} 个节点")
    log(f"\n明细: {REPORT}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
