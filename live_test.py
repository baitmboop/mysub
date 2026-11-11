#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""线上订阅端到端体检。

拉一次线上 sub.yaml, 在本机起一个临时 mihomo, 用"你所在网络"的真实视角
测每个节点, 再走代理访问几个真实站点。

这是唯一能回答"这份订阅在我这儿到底能不能用"的方法 ——
云端 runner 在美国, 它说通不代表你通。

    python3 live_test.py                 # 用主地址
    python3 live_test.py --url <地址>
    python3 live_test.py --top 5         # 顺带测前 5 名真实下载速度
"""
from __future__ import annotations

import argparse
import json
import os
import shutil
import socket
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.parse
import urllib.request
from concurrent.futures import ThreadPoolExecutor

ROOT = os.path.dirname(os.path.abspath(__file__))
MIHOMO = os.path.join(ROOT, "bin", "mihomo")

DEFAULT_URL = ("https://gh-proxy.com/https://raw.githubusercontent.com/"
               "baitmboop/mysub/main/out/sub.yaml")
TEST_URL = "https://www.gstatic.com/generate_204"
SITES = [
    ("gstatic", "https://www.gstatic.com/generate_204"),
    ("github", "https://github.com"),
    ("youtube", "https://www.youtube.com"),
    ("x", "https://x.com"),
]


def free_port(start: int) -> int:
    for p in range(start, start + 200):
        with socket.socket() as s:
            try:
                s.bind(("127.0.0.1", p))
                return p
            except OSError:
                continue
    raise RuntimeError("找不到空闲端口")


def fetch(url: str) -> str:
    req = urllib.request.Request(url, headers={"User-Agent": "clash-meta/1.0"})
    # 拉订阅本身不能走系统代理, 否则代理挂了就拉不到订阅
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
    with opener.open(req, timeout=30) as r:
        return r.read().decode("utf-8")


def api(port: int, path: str, method: str = "GET", timeout: float = 12.0):
    req = urllib.request.Request(f"http://127.0.0.1:{port}{path}", method=method)
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
    with opener.open(req, timeout=timeout) as r:
        body = r.read()
        return json.loads(body) if body else {}


def wait_api(port: int, limit: float = 15.0) -> bool:
    end = time.time() + limit
    while time.time() < end:
        try:
            api(port, "/version", timeout=1.5)
            return True
        except Exception:
            time.sleep(0.3)
    return False


def http_probe(proxy_port: int, url: str, timeout: float = 20.0,
               want_body: bool = False):
    """走本地 mixed-port 访问 url。返回 (状态码/耗时, 错误)。"""
    handler = urllib.request.ProxyHandler({
        "http": f"http://127.0.0.1:{proxy_port}",
        "https": f"http://127.0.0.1:{proxy_port}",
    })
    opener = urllib.request.build_opener(handler)
    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
    t0 = time.time()
    try:
        with opener.open(req, timeout=timeout) as r:
            if want_body:
                r.read(200000)
            else:
                r.read(1024)
            return round(time.time() - t0, 2), r.status, None
    except urllib.error.HTTPError as e:
        return round(time.time() - t0, 2), e.code, None
    except Exception as e:
        return round(time.time() - t0, 2), None, type(e).__name__


def speed_test(proxy_port: int, mb: int = 3) -> float:
    """下载固定体积, 返回 MB/s。

    端点选两个取最大值: 国内到 Cloudflare 本身就被限速 (实测直连也只有
    0.22MB/s), 单用它会冤枉所有节点。ovh 通常更宽松。
    """
    urls = [
        f"https://speed.cloudflare.com/__down?bytes={mb * 1024 * 1024}",
        "https://proof.ovh.net/files/10Mb.dat",
    ]
    best = 0.0
    for url in urls:
        handler = urllib.request.ProxyHandler({
            "http": f"http://127.0.0.1:{proxy_port}",
            "https": f"http://127.0.0.1:{proxy_port}",
        })
        opener = urllib.request.build_opener(handler)
        req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
        t0 = time.time()
        got = 0
        try:
            with opener.open(req, timeout=20) as r:
                while True:
                    chunk = r.read(65536)
                    if not chunk:
                        break
                    got += len(chunk)
                    if time.time() - t0 > 8:      # 8 秒封顶, 别让慢节点拖死
                        break
        except Exception:
            continue
        dur = max(time.time() - t0, 0.01)
        best = max(best, got / dur / 1024 / 1024)
    return best


def load_proxies(raw: str) -> dict:
    """从订阅里取出 {名称: (server, port)}。"""
    try:
        import yaml
    except ImportError:
        return {}
    try:
        doc = yaml.safe_load(raw) or {}
    except Exception:
        return {}
    out = {}
    for p in doc.get("proxies") or []:
        if isinstance(p, dict) and p.get("name") and p.get("server"):
            out[p["name"]] = (str(p["server"]), str(p.get("port", "")))
    return out


def save_cn_pref(results: list, proxies: dict, path: str) -> int:
    """把本轮实测写成 data/cn_pref.json, 供云端排序时参考。

    键是 server:port (历史池的 node 里能直接拼出来), 值是延迟和是否通。
    """
    old = {}
    if os.path.exists(path):
        try:
            with open(path, encoding="utf-8") as f:
                old = json.load(f) or {}
        except Exception:
            old = {}
    now = time.time()
    n_new = 0
    for nm, delay, err in results:
        sp = proxies.get(nm)
        if not sp:
            continue
        key = f"{sp[0]}:{sp[1]}"
        old[key] = {"ok": 1 if delay else 0, "at": int(now)}
        if delay:
            old[key]["latency"] = delay
        n_new += 1
    cutoff = now - 7 * 86400
    old = {k: v for k, v in old.items()
           if isinstance(v, dict) and v.get("at", 0) >= cutoff}
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(dict(sorted(old.items())), f, ensure_ascii=False, indent=1)
    return n_new


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--url", default=DEFAULT_URL)
    ap.add_argument("--top", type=int, default=3, help="额外测几个真实下载速度")
    ap.add_argument("--workers", type=int, default=24)
    ap.add_argument("--timeout", type=int, default=6000, help="单节点延迟上限 ms")
    ap.add_argument("--save", action="store_true",
                    help="把结果写入 data/cn_pref.json, 供云端排序参考")
    args = ap.parse_args()

    if not os.path.exists(MIHOMO):
        print("找不到 bin/mihomo")
        return 1

    # ---------- 1. 拉线上配置 ----------
    print(f"拉取 {args.url}")
    t0 = time.time()
    raw = fetch(args.url)
    print(f"  ok  {len(raw)}B  {time.time() - t0:.1f}s")
    head = [ln for ln in raw.splitlines() if ln.startswith("#")][:3]
    for ln in head:
        print(f"  {ln}")

    work = tempfile.mkdtemp(prefix="live-")
    cfg_path = os.path.join(work, "config.yaml")
    mixed = free_port(17890)
    ctl = free_port(19090)
    # 客户端产物不带端口参数(避免覆盖用户自己的设置), 体检时补上
    with open(cfg_path, "w", encoding="utf-8") as f:
        f.write(f"mixed-port: {mixed}\n"
                f"external-controller: 127.0.0.1:{ctl}\n"
                f"log-level: silent\n")
        f.write(raw)

    # ---------- 2. 校验 + 启动 ----------
    chk = subprocess.run([MIHOMO, "-t", "-d", work, "-f", cfg_path],
                         capture_output=True, text=True)
    if chk.returncode != 0:
        print("配置校验失败:")
        print((chk.stdout + chk.stderr)[-800:])
        return 1

    proc = subprocess.Popen([MIHOMO, "-d", work, "-f", cfg_path],
                            stdout=subprocess.DEVNULL,
                            stderr=subprocess.DEVNULL)
    try:
        if not wait_api(ctl):
            print("内核没起来")
            return 1

        proxies = api(ctl, "/proxies")["proxies"]
        pool_group = "③ 免费池"
        names = list(proxies.get(pool_group, {}).get("all", []))
        if not names:
            names = [k for k, v in proxies.items()
                     if v.get("type") not in ("Fallback", "Selector",
                                              "URLTest", "Direct", "Reject",
                                              "Compatible", "Pass")]
        print(f"\n订阅内节点 {len(names)} 个, 开始测延迟 (并发 {args.workers})")

        # ---------- 3. 逐节点延迟 ----------
        def one(nm: str):
            q = (f"/proxies/{urllib.parse.quote(nm)}/delay"
                 f"?url={urllib.parse.quote(TEST_URL, safe='')}"
                 f"&timeout={args.timeout}")
            try:
                d = api(ctl, q, timeout=args.timeout / 1000 + 6)
                return nm, d.get("delay"), None
            except Exception as e:
                return nm, None, type(e).__name__

        t1 = time.time()
        results = []
        with ThreadPoolExecutor(max_workers=args.workers) as ex:
            for nm, delay, err in ex.map(one, names):
                results.append((nm, delay, err))
        alive = [r for r in results if r[1]]
        alive.sort(key=lambda r: r[1])
        print(f"  完成 {time.time() - t1:.1f}s")
        print(f"  可用 {len(alive)}/{len(names)}  "
              f"({len(alive) / max(len(names), 1) * 100:.0f}%)")
        if alive:
            print(f"  最快 {alive[0][1]}ms / 中位 "
                  f"{alive[len(alive) // 2][1]}ms / 最慢 {alive[-1][1]}ms")
            print("\n  延迟前 10:")
            for nm, d, _ in alive[:10]:
                print(f"    {nm:<10} {d:>6}ms")

        if not alive:
            print("\n没有节点可用 —— 订阅对你的网络无效, 需要改筛选策略")
            if args.save:
                cnt = save_cn_pref(results, load_proxies(raw),
                                   os.path.join(ROOT, "data", "cn_pref.json"))
                print(f"  已记录 {cnt} 条实测到 data/cn_pref.json")
            return 2

        # ---------- 4. 真实站点 ----------
        print("\n走总出口(fallback 自动选)访问真实站点:")
        for label, url in SITES:
            sec, code, err = http_probe(mixed, url, timeout=25)
            mark = "ok " if code and code < 400 else "FAIL"
            print(f"  [{mark}] {label:<8} {str(code or err):<18} {sec}s")

        # ---------- 5. 前 N 名真实下载速度 ----------
        if args.top > 0:
            print(f"\n强制切换前 {args.top} 名, 测真实下载速度:")
            grp = urllib.parse.quote(pool_group)
            for nm, d, _ in alive[: args.top]:
                try:
                    req = urllib.request.Request(
                        f"http://127.0.0.1:{ctl}/proxies/{grp}",
                        data=json.dumps({"name": nm}).encode(),
                        headers={"Content-Type": "application/json"},
                        method="PUT")
                    urllib.request.build_opener(
                        urllib.request.ProxyHandler({})).open(req, timeout=8)
                except Exception as e:
                    print(f"    {nm}: 切换失败 {type(e).__name__}")
                    continue
                time.sleep(0.6)
                mbps = speed_test(mixed)
                print(f"    {nm:<10} {d:>6}ms  {mbps:>5.2f} MB/s")

        # ---------- 6. 出口 IP ----------
        sec, code, err = http_probe(mixed, "https://www.cloudflare.com/cdn-cgi/trace",
                                    timeout=20, want_body=True)
        print(f"\n出口探针: {code or err}  {sec}s")

        if args.save:
            cnt = save_cn_pref(results, load_proxies(raw),
                               os.path.join(ROOT, "data", "cn_pref.json"))
            print(f"已记录 {cnt} 条实测 → data/cn_pref.json "
                  f"(可用 {len(alive)} 条, 云端会优先排它们)")
        return 0
    finally:
        proc.terminate()
        try:
            proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            proc.kill()
        shutil.rmtree(work, ignore_errors=True)


if __name__ == "__main__":
    sys.exit(main())
