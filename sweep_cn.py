#!/usr/bin/env python3
"""订阅池逐节点实测 —— 从"你这台机器所在的网络"出发, 量每个节点到底能不能用。

为什么要单节点独立内核
----------------------
一次挂多个节点共用一个内核 + 切组会互相干扰, 之前就是这么测出"全灭"的假数据。
所以每个节点单独起一个 mihomo(独立 mixed-port、独立 -d 工作目录), 用完就杀。

为什么要多个测速目标
--------------------
只用一个目标(比如 speed.cloudflare.com)会被限流, 返回 429 后速度记成 0,
一堆好节点被误判成死的 —— 这个坑真踩过, 一次把 109 个节点全标成不可用。
所以命中 429/403/502/503 只算"这次没测准", 换目标重来, 绝不写 ok=0。

判定
----
先花 5 秒看能不能连通(204 探测), 连不上才算死;
连通了再下 10MB 量速度: >=50KB/s 记 3 档(好用), 更低记 1 档(连得上但传不动)。
结果先写 /tmp/cn_sweep.json 核对, 加 --apply 才并入 data/cn_pref.json。
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
from concurrent.futures import ThreadPoolExecutor

ROOT = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, ROOT)

import gateway  # noqa: E402

MIHOMO = os.path.join(ROOT, "bin", "mihomo")
WORK = "/tmp/cn_sweep.json"
CLEAN = {**os.environ, "HTTP_PROXY": "", "HTTPS_PROXY": "", "ALL_PROXY": ""}

PING_URL = "https://www.gstatic.com/generate_204"
# 三个不同机房的测试文件: 哪个被限流就换下一个
SPEED_URLS = [
    "https://proof.ovh.net/files/10Mb.dat",
    "https://speed.cloudflare.com/__down?bytes=10000000",
    "http://ipv4.download.thinkbroadband.com/10MB.zip",
]
THROTTLED = {"429", "403", "503", "502"}


def log(m: str) -> None:
    print(m, flush=True)


def free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def curl_via(port: int, url: str, timeout: int):
    out = subprocess.run(
        ["curl", "-s", "-o", "/dev/null", "-m", str(timeout),
         "-x", f"http://127.0.0.1:{port}",
         "-w", "%{speed_download} %{time_starttransfer} %{http_code}", url],
        capture_output=True, text=True, env=CLEAN).stdout.split()
    if len(out) < 3:
        return None
    try:
        return float(out[0]) / 1024, float(out[1]) * 1000, out[2]
    except ValueError:
        return None


def with_kernel(proxy: dict, fn):
    """起一个只含该节点的内核, 执行 fn(port), 无论如何都收拾干净。"""
    port = free_port()
    wd = tempfile.mkdtemp(prefix="mh_")
    with tempfile.NamedTemporaryFile("w", suffix=".yaml", delete=False) as f:
        json.dump({"mixed-port": port, "allow-lan": False, "mode": "rule",
                   "log-level": "silent", "proxies": [proxy],
                   "proxy-groups": [{"name": "G", "type": "select",
                                     "proxies": [proxy["name"]]}],
                   "rules": ["MATCH,G"]}, f)
        cfg = f.name
    p = subprocess.Popen([MIHOMO, "-d", wd, "-f", cfg],
                         stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    try:
        for _ in range(50):
            try:
                socket.create_connection(("127.0.0.1", port), timeout=0.3).close()
                return fn(port)
            except OSError:
                time.sleep(0.1)
        return None
    finally:
        p.terminate()
        try:
            p.wait(3)
        except Exception:
            p.kill()
        os.unlink(cfg)
        shutil.rmtree(wd, ignore_errors=True)


def measure(proxy: dict):
    """返回 {ok, latency, speed}; 目标限流导致测不出结论时返回 None。"""
    def fn(port):
        # 5 秒对 CloudFront 前置的节点太紧: 冷启动要 TLS + ws 握手, 实测一个
        # 59ms 的 vless(ws) 首包要 6~8 秒才回来, 5 秒一刀切会把它误判成死的。
        # 所以先等 8 秒, 失败再用 12 秒复一次, 两次都不通才算死。
        ping = curl_via(port, PING_URL, 8) or curl_via(port, PING_URL, 12)
        if not ping or ping[2] not in ("204", "200"):
            return {"ok": 0, "latency": None, "speed": None}
        lat = int(ping[1])
        best, bad = 0.0, 0
        for url in SPEED_URLS:
            r = curl_via(port, url, 10)
            if not r:
                bad += 1
                continue
            spd, _, code = r
            if code in THROTTLED:
                bad += 1          # 测速站限流, 不是节点的问题
                continue
            if spd > 0:
                # 拿到真实速度就够了, 不必再试别的源。
                # 只有"被限流/报错"才需要换源重来; 慢节点(10~40KB/s)如果也跑三个源,
                # 每个节点要多花 20 秒, 一批 150 个就是 10 分钟纯等待 —— 实测踩过。
                return {"ok": 1, "latency": lat, "speed": round(spd / 1024, 4)}
            best = max(best, spd)
        if best == 0 and bad >= len(SPEED_URLS):
            return None
        return {"ok": 1, "latency": lat, "speed": round(best / 1024, 4)}

    return with_kernel(proxy, fn)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--workers", type=int, default=5)
    ap.add_argument("--apply", action="store_true", help="并入 data/cn_pref.json")
    ap.add_argument("--redo", action="store_true", help="忽略上次结果, 全部重测")
    ap.add_argument("--retest-dead", action="store_true",
                    help="只复测上一轮被自己判死的节点(超时门槛放宽后再验一遍)")
    args = ap.parse_args()

    history = gateway.load_json(gateway.HISTORY, {})
    if args.retest_dead:
        pref_now = gateway.load_json(gateway.CN_PREF, {})
        dead = {k for k, v in pref_now.items()
                if v.get("ok") == 0 and v.get("by") == "auto"}
        todo, seen = [], set()
        for fp, rec in history.items():
            n = dict(rec.get("node") or {})
            key = f"{n.get('server')}:{n.get('port')}"
            if not n.get("server") or key not in dead or key in seen:
                continue
            n["fp"] = fp
            todo.append(n)
            seen.add(key)
        prev = {}
    else:
        pool = gateway.pool_nodes(history)
        prev = {} if args.redo else gateway.load_json(WORK, {})
        todo = [n for n in pool if f"{n['server']}:{n['port']}" not in prev]
    log(f"待测 {len(todo)} 个 / 并发 {args.workers}")

    res = dict(prev)
    now, stat, unknown = int(time.time()), {}, []

    def one(n):
        pr = gateway.to_mihomo(n, "T")
        return n, (measure(pr) if pr else None)

    with ThreadPoolExecutor(max_workers=args.workers) as ex:
        for i, (n, r) in enumerate(ex.map(one, todo), 1):
            key = f"{n['server']}:{n['port']}"
            proto = str(n.get("proto"))
            s = stat.setdefault(proto, [0, 0])
            if r is None:
                unknown.append(key)
                mark, spd = "?", 0
            else:
                r["at"], r["by"] = now, "auto"
                res[key] = r
                s[0] += 1
                spd = round((r["speed"] or 0) * 1024)
                if r["ok"] and (r["speed"] or 0) >= 0.05:
                    s[1] += 1
                mark = "✓" if (r["ok"] and (r["speed"] or 0) >= 0.05) else \
                       ("慢" if r["ok"] else "✗")
            log(f"[{i:3}/{len(todo)}] {mark} {proto:9} {key} {spd} KB/s")

    gateway.save_json(WORK, res)
    log("\n按协议 (可用/测过):")
    for proto, (total, good) in sorted(stat.items(), key=lambda kv: -kv[1][1]):
        log(f"  {proto:10} {good}/{total}")
    if unknown:
        log(f"  测不准(限流/超时, 未写结论): {len(unknown)} 个")

    if args.apply:
        pref = gateway.load_json(gateway.CN_PREF, {})
        now = int(time.time())
        kept = 0
        for k, v in res.items():
            old = pref.get(k) or {}
            # 一次没测通就推翻"能用"的结论太脆: 免费节点本来就时好时坏,
            # 同一个节点前后两次能差到 20 倍(实测 11KB/s <-> 854KB/s)。
            # 所以只要历史结论是"能用", 这次没测通就不写, 下次再失败也还有机会。
            if old.get("ok") == 1 and v.get("ok") == 0:
                kept += 1
                continue
            if old.get("ok") == 1 and v.get("ok") == 1 and old.get("at", 0) > now - 86400:
                # 两边都能用: 速度取 24 小时内见过的最好值, 延迟取最低值。
                # 排序本来就是给"哪台更值得先试"用的, 手机端的 url-test 会当场
                # 再校准一次; 拿某一次的抖动值当结论, 会把 854KB/s 的节点排到末尾。
                v["speed"] = max(old.get("speed") or 0, v.get("speed") or 0)
                lats = [x for x in (old.get("latency"), v.get("latency")) if x]
                v["latency"] = min(lats) if lats else None
            pref[k] = v
        gateway.save_json(gateway.CN_PREF, pref)
        log(f"\n已并入 data/cn_pref.json, 共 {len(pref)} 条 (保留原判 {kept} 条)")
    else:
        log(f"\n结果在 {WORK} (加 --apply 才写进 cn_pref)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
