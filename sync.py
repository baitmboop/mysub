#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""把"你机器上的真实测速结果"同步回仓库。

为什么需要它
------------
云端 runner 在新加坡/美国, 它测出的延迟和"国内能不能连上"相关度极低 ——
实测云端判活 40 个, 到国内只剩 13 个能连。而测速这件事没有别的捷径:
只有从你所在网络出发测出来的才算数。

脚本不去另起内核, 而是直接让**你正在用的 Clash 客户端**去测 —— 它本来就
跑在你的网络里, 视角 100% 准确, 且不额外消耗流量(只拉 204 空响应)。

结果写进 data/cn_pref.json, 云端构建时优先录用这些实测通过的节点。

用法
----
    python3 sync.py              # 测一轮, 写入文件
    python3 sync.py --push       # 测完顺手提交并推到 GitHub
    python3 sync.py --timeout 5000 --workers 16

前提: Clash Verge 正在运行(订阅已导入)。
"""
from __future__ import annotations

import argparse
import concurrent.futures as cf
import glob
import http.client
import json
import os
import socket
import subprocess
import sys
import time
import urllib.parse

ROOT = os.path.dirname(os.path.abspath(__file__))
PREF = os.path.join(ROOT, "data", "cn_pref.json")
PROBE_URL = "https://www.gstatic.com/generate_204"
GROUPS = ("③ 免费池", "自动选择", "节点选择")


# ---------------------------------------------------------------- 定位客户端
def verge_dir() -> str | None:
    """Clash Verge 的配置目录(macOS / Linux)。"""
    home = os.path.expanduser("~")
    cands = [
        os.path.join(home, "Library/Application Support/io.github.clash-verge-rev.clash-verge-rev"),
        os.path.join(home, ".config/io.github.clash-verge-rev.clash-verge-rev"),
        os.path.join(home, ".local/share/io.github.clash-verge-rev.clash-verge-rev"),
    ]
    for d in cands:
        if os.path.isdir(d):
            return d
    return None


def small_yaml(path: str) -> dict:
    """只读顶层的简单 key: value, 不引入 yaml 依赖。"""
    out = {}
    try:
        with open(path, encoding="utf-8") as f:
            for line in f:
                if line.startswith((" ", "-", "#")):
                    continue
                if ":" in line:
                    k, v = line.split(":", 1)
                    out[k.strip()] = v.strip().strip("'\"")
    except OSError:
        pass
    return out


def find_controller() -> tuple[str, str, str]:
    """返回 (方式, 地址, secret)。方式为 unix / tcp。"""
    d = verge_dir()
    if not d:
        raise SystemExit("找不到 Clash Verge 配置目录")
    cfg = {}
    for name in ("clash-verge.yaml", "config.yaml"):
        cfg.update(small_yaml(os.path.join(d, name)))
    secret = cfg.get("secret", "")
    unix = cfg.get("external-controller-unix", "")
    tcp = cfg.get("external-controller", "")
    if unix and os.path.exists(unix):
        return "unix", unix, secret
    if tcp:
        return "tcp", tcp, secret
    raise SystemExit("Clash 控制器未启用(既没有 unix socket, 也没有 tcp 端口)")


def active_profile(d: str) -> str | None:
    """当前激活的订阅文件路径。"""
    meta = small_yaml(os.path.join(d, "profiles.yaml"))
    uid = meta.get("current")
    if uid:
        p = os.path.join(d, "profiles", f"{uid}.yaml")
        if os.path.exists(p):
            return p
    files = sorted(glob.glob(os.path.join(d, "profiles", "*.yaml")))
    return files[-1] if files else None


def name_map(path: str) -> dict:
    """订阅里的 名字 -> "server:port"。"""
    try:
        import yaml
    except ImportError:
        return {}
    try:
        doc = yaml.safe_load(open(path, encoding="utf-8")) or {}
    except Exception:
        return {}
    out = {}
    for p in doc.get("proxies") or []:
        if isinstance(p, dict) and p.get("name") and p.get("server"):
            out[p["name"]] = f"{p['server']}:{p.get('port', '')}"
    return out


# ---------------------------------------------------------------- HTTP 客户端
class Client:
    def __init__(self, kind: str, addr: str, secret: str):
        self.kind, self.addr, self.secret = kind, addr, secret

    def _conn(self, timeout: float):
        if self.kind == "unix":
            class UConn(http.client.HTTPConnection):
                def __init__(s):
                    super().__init__("localhost", timeout=timeout)

                def connect(s):
                    s.sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
                    s.sock.settimeout(timeout)
                    s.sock.connect(self.addr)
            return UConn()
        host, _, port = self.addr.rpartition(":")
        return http.client.HTTPConnection(host or "127.0.0.1", int(port), timeout=timeout)

    def get(self, path: str, timeout: float = 10.0):
        c = self._conn(timeout)
        try:
            c.request("GET", path, headers={"Authorization": f"Bearer {self.secret}"})
            r = c.getresponse()
            body = r.read()
            if r.status >= 400:
                raise RuntimeError(f"HTTP {r.status}: {body[:80]!r}")
            return json.loads(body or b"{}")
        finally:
            c.close()


# ---------------------------------------------------------------- 主流程
def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--push", action="store_true", help="写完后提交并推送")
    ap.add_argument("--timeout", type=int, default=5000, help="单节点超时 ms")
    ap.add_argument("--workers", type=int, default=16)
    args = ap.parse_args()

    d = verge_dir()
    kind, addr, secret = find_controller()
    cli = Client(kind, addr, secret)
    try:
        ver = cli.get("/version", timeout=5)
    except Exception as e:
        raise SystemExit(f"连不上 Clash 控制器({kind} {addr}): {type(e).__name__} {e}")
    print(f"已连上 Clash {ver.get('version')} ({kind})")

    prof = active_profile(d)
    if not prof:
        raise SystemExit("找不到激活的订阅文件")
    nmap = name_map(prof)
    print(f"激活订阅: {os.path.basename(prof)}  节点 {len(nmap)} 个")

    proxies = cli.get("/proxies")["proxies"]
    group = next((g for g in GROUPS if g in proxies and proxies[g].get("all")), None)
    if not group:
        raise SystemExit("订阅里没有找到分组, 请确认已导入")
    names = list(proxies[group]["all"])
    print(f"分组「{group}」{len(names)} 个节点, 开始让客户端逐个测 (并发 {args.workers})")

    q_url = urllib.parse.quote(PROBE_URL, safe="")

    def one(nm: str):
        q = (f"/proxies/{urllib.parse.quote(nm)}/delay?url={q_url}"
             f"&timeout={args.timeout}")
        try:
            return nm, cli.get(q, timeout=args.timeout / 1000 + 6).get("delay"), None
        except Exception as e:
            return nm, None, type(e).__name__

    t0 = time.time()
    results = []
    with cf.ThreadPoolExecutor(max_workers=args.workers) as ex:
        for r in ex.map(one, names):
            results.append(r)
    alive = [r for r in results if r[1]]
    print(f"完成 {time.time() - t0:.1f}s  可用 {len(alive)}/{len(names)}")

    # ---- 写入 cn_pref.json ----
    old = {}
    if os.path.exists(PREF):
        try:
            old = json.load(open(PREF, encoding="utf-8")) or {}
        except Exception:
            old = {}
    now = int(time.time())
    n_new = 0
    for nm, delay, err in results:
        key = nmap.get(nm)
        if not key:
            continue          # 不是我们订阅里的节点(比如 DIRECT), 跳过
        rec = {"ok": 1 if delay else 0, "at": now}
        if delay:
            rec["latency"] = delay
        # 之前测过的带宽数据仍有效就保留, 免得每次同步把速度信息抹掉
        prev = old.get(key) or {}
        if prev.get("speed") is not None and now - prev.get("at", 0) < 3 * 86400:
            rec["speed"] = prev["speed"]
        old[key] = rec
        n_new += 1

    cutoff = now - 7 * 86400
    old = {k: v for k, v in old.items()
           if isinstance(v, dict) and v.get("at", 0) >= cutoff}
    os.makedirs(os.path.dirname(PREF), exist_ok=True)
    with open(PREF, "w", encoding="utf-8") as f:
        json.dump(dict(sorted(old.items())), f, ensure_ascii=False, indent=1)
    print(f"已写入 {n_new} 条 → data/cn_pref.json (累计 {len(old)} 条)")

    if alive:
        lat = sorted(r[1] for r in alive)
        print(f"延迟 最快 {lat[0]}ms / 中位 {lat[len(lat) // 2]}ms / 最慢 {lat[-1]}ms")

    if not args.push:
        return 0

    # ---- 提交推送 ----
    def git(*a):
        return subprocess.run(["git", *a], cwd=ROOT, capture_output=True, text=True)

    if not git("status", "--porcelain", "data/cn_pref.json").stdout.strip():
        print("内容无变化, 不提交")
        return 0
    key = os.path.join(ROOT, ".ssh", "id_ed25519")
    env = dict(os.environ)
    if os.path.exists(key):
        env["GIT_SSH_COMMAND"] = f"ssh -i {key} -o StrictHostKeyChecking=accept-new"
    git("add", "data/cn_pref.json")
    c = subprocess.run(
        ["git", "commit", "-q", "-m",
         f"chore: 同步本机实测 {len(alive)}/{len(names)} 可用"],
        cwd=ROOT, capture_output=True, text=True)
    if c.returncode != 0:
        print("提交失败:", c.stderr.strip()[:200])
        return 1
    p = subprocess.run(["git", "push", "-q", "origin", "HEAD:main"],
                       cwd=ROOT, capture_output=True, text=True, env=env)
    if p.returncode != 0:
        print("推送失败(可能需要先 pull):", p.stderr.strip()[:300])
        return 1
    print("已推送到仓库, 下次云端构建会优先用这批数据")
    return 0


if __name__ == "__main__":
    sys.exit(main())
