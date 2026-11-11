#!/usr/bin/env python3
"""
测速引擎 —— 用 mihomo 内核驱动真实链路测试。
两级筛选: 延迟/抖动 → 下载带宽,最后按综合评分排序。
用法: python3 probe.py --max-speed 120
输出: out/best.yaml, out/report.md, out/nodes_best.json
"""
import argparse
import json
import os
import re
import shutil
import signal
import subprocess
import sys
import time
import urllib.parse
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed

ROOT = os.path.dirname(os.path.abspath(__file__))
MIHOMO = os.path.join(ROOT, "bin", "mihomo")
CFG = os.path.join(ROOT, "data", "generated.yaml")
API = "http://127.0.0.1:19090"
MIXED = 17890

TEST_URL = "https://www.gstatic.com/generate_204"
SPEED_URL = "https://speed.cloudflare.com/__down?bytes=5000000"
SS_CIPHERS = {
    "aes-128-gcm", "aes-192-gcm", "aes-256-gcm", "aes-128-cfb", "aes-192-cfb",
    "aes-256-cfb", "aes-128-ctr", "aes-192-ctr", "aes-256-ctr", "rc4-md5",
    "chacha20-ietf", "xchacha20", "chacha20-ietf-poly1305", "xchacha20-poly1305",
    "2022-blake3-aes-128-gcm", "2022-blake3-aes-256-gcm", "2022-blake3-chacha20-poly1305",
    "none", "plain",
}


def yaml_scalar(v):
    if isinstance(v, bool):
        return "true" if v else "false"
    if isinstance(v, (int, float)):
        return str(v)
    s = str(v)
    if s == "" or re.search(r'[:#\[\]{},&*!|>%@`"\'\n]', s) or s.strip() != s:
        return '"' + s.replace('\\', '\\\\').replace('"', '\\"') + '"'
    return s


def emit(obj, indent=0):
    """极简 YAML 输出,避免依赖 pyyaml 的格式差异。"""
    pad = " " * indent
    out = []
    if isinstance(obj, dict):
        for k, v in obj.items():
            if isinstance(v, (dict, list)) and v:
                out.append(f"{pad}{k}:")
                out.append(emit(v, indent + 2))
            elif isinstance(v, (dict, list)):
                out.append(f"{pad}{k}: {'{}' if isinstance(v, dict) else '[]'}")
            else:
                out.append(f"{pad}{k}: {yaml_scalar(v)}")
    elif isinstance(obj, list):
        for item in obj:
            if isinstance(item, dict):
                lines = emit(item, indent + 2).split("\n")
                first = lines[0].lstrip()
                out.append(f"{pad}- {first}")
                out.extend(lines[1:])
            else:
                out.append(f"{pad}- {yaml_scalar(item)}")
    return "\n".join(out)


def to_mihomo(n: dict, name: str) -> dict | None:
    """统一结构 → mihomo proxy。不支持的直接丢弃。"""
    p = {"name": name, "type": n["proto"], "server": n["server"], "port": n["port"]}
    proto = n["proto"]

    if proto == "ss":
        if (n.get("cipher") or "").lower() not in SS_CIPHERS:
            return None
        p.update(cipher=n["cipher"].lower(), password=n["password"], udp=True)

    elif proto == "vmess":
        p.update(uuid=n["uuid"], alterId=n.get("alterId", 0),
                 cipher=n.get("cipher") or "auto", udp=True)
        if n.get("tls"):
            p.update(tls=True, servername=n.get("sni") or n["server"], skipCertVerify=True)

    elif proto == "vless":
        p.update(uuid=n["uuid"], udp=True, skipCertVerify=True)
        if n.get("flow"):
            p["flow"] = n["flow"]
        if n.get("tls"):
            p["tls"] = True
            p["servername"] = n.get("sni") or n.get("host") or n["server"]
        if n.get("reality") and n.get("pbk"):
            sid = n.get("sid") or ""
            if not (re.fullmatch(r"[0-9a-fA-F]{0,16}", sid) and len(sid) % 2 == 0):
                sid = ""
            if not re.fullmatch(r"[A-Za-z0-9_-]{43}", n["pbk"]):
                return None
            p["reality-opts"] = {"public-key": n["pbk"], "short-id": sid}
            p["client-fingerprint"] = n.get("fp") or "chrome"

    elif proto == "trojan":
        p.update(password=n["password"], udp=True, skipCertVerify=True)
        if n.get("sni"):
            p["sni"] = n["sni"]

    elif proto == "hysteria2":
        p.update(password=n["password"], skipCertVerify=True)
        if n.get("sni"):
            p["sni"] = n["sni"]

    elif proto == "tuic":
        p.update(uuid=n["uuid"], password=n["password"], skipCertVerify=True, udp=True)
        if n.get("sni"):
            p["sni"] = n["sni"]
        if n.get("alpn"):
            p["alpn"] = n["alpn"]
    else:
        return None

    net = n.get("network") or "tcp"
    if proto in ("vmess", "vless", "trojan") and net in ("ws", "grpc", "h2", "http"):
        if net == "ws":
            opts = {}
            if n.get("path"):
                opts["path"] = n["path"]
            if n.get("host"):
                opts["headers"] = {"Host": n["host"]}
            if opts:
                p["ws-opts"] = opts
                p["network"] = "ws"
            else:
                p["network"] = "ws"
        elif net == "grpc":
            p["network"] = "grpc"
            p["grpc-opts"] = {"grpc-service-name": n.get("path") or n.get("host") or ""}
        elif net in ("h2", "http"):
            p["network"] = "h2"
            if n.get("host"):
                p["h2-opts"] = {"host": [n["host"]], "path": n.get("path") or "/"}
    return p


def validate_config(path):
    """mihomo -t 校验;返回 (出错索引, 原因) 或 None 表示通过。"""
    r = subprocess.run([MIHOMO, "-f", path, "-t"], capture_output=True, text=True, timeout=120)
    if r.returncode == 0:
        return None
    blob = (r.stderr or "") + (r.stdout or "")
    m = re.search(r"proxy (\d+):\s*(.+)", blob)
    if m:
        return int(m.group(1)), m.group(2).strip()
    return -1, blob.strip().splitlines()[-1][:120] if blob.strip() else "unknown"


def build_config(nodes):
    proxies = []
    for i, n in enumerate(nodes):
        p = to_mihomo(n, f"n{i:04d}")
        if p:
            proxies.append(p)

    cfg = {
        "mixed-port": MIXED,
        "allow-lan": False,
        "mode": "rule",
        "log-level": "silent",
        "external-controller": "127.0.0.1:19090",
        "unified-delay": True,
        "tcp-concurrent": True,
        "find-process-mode": "off",
        "dns": {
            "enable": True,
            "ipv6": False,
            "enhanced-mode": "normal",
            "nameserver": ["223.5.5.5", "119.29.29.29"],
        },
        "rules": ["MATCH,POOL"],
    }
    os.makedirs(os.path.dirname(CFG), exist_ok=True)

    dropped = []
    for _ in range(80):
        cfg["proxies"] = proxies
        cfg["proxy-groups"] = [{
            "name": "POOL", "type": "select",
            "proxies": [p["name"] for p in proxies],
        }]
        with open(CFG, "w") as f:
            f.write(emit(cfg) + "\n")
        err = validate_config(CFG)
        if err is None:
            break
        idx, msg = err
        if 0 <= idx < len(proxies):
            dropped.append((proxies[idx]["name"], msg))
            proxies.pop(idx)
        else:
            print(f"      ! 无法定位的配置错误: {msg}")
            break
    else:
        print("      ! 剔除次数达到上限")

    if dropped:
        print(f"      逐条校验剔除 {len(dropped)} 个非法节点 (示例: {dropped[0][1][:50]})")

    by_name = {p["name"]: p for p in proxies}
    nodes_by_name = {}
    for i, n in enumerate(nodes):
        nm = f"n{i:04d}"
        if nm in by_name:
            nodes_by_name[nm] = n
    return nodes_by_name, len(proxies)


def api(path, method="GET", body=None, timeout=10):
    req = urllib.request.Request(f"{API}{path}", method=method,
                                 data=json.dumps(body).encode() if body else None,
                                 headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        raw = r.read()
        return json.loads(raw) if raw else None


def wait_ready(proc, limit=40):
    for _ in range(limit * 2):
        if proc.poll() is not None:
            return False
        try:
            api("/version", timeout=2)
            return True
        except Exception:
            time.sleep(0.5)
    return False


def delay_test(name, timeout_ms=4000, retries=2):
    """取多次探测的中位数,顺带算抖动。"""
    vals = []
    for _ in range(retries + 1):
        try:
            d = api(f"/proxies/{name}/delay?url={TEST_URL}&timeout={timeout_ms}", timeout=(timeout_ms / 1000) + 4)
            if d and d.get("delay"):
                vals.append(int(d["delay"]))
        except Exception:
            vals.append(None)
    ok = [v for v in vals if v]
    if not ok:
        return None, None
    ok.sort()
    med = ok[len(ok) // 2]
    jitter = (max(ok) - min(ok)) if len(ok) > 1 else 0
    return med, jitter


def speed_test(name, seconds=8):
    """切到该节点,通过 mixed-port 下载测带宽。"""
    try:
        api("/proxies/POOL", "PUT", {"name": name})
    except Exception:
        return 0.0
    time.sleep(0.15)
    cmd = ["curl", "-s", "--noproxy", "*", "-x", f"http://127.0.0.1:{MIXED}",
           "-o", "/dev/null", "--max-time", str(seconds),
           "-w", "%{speed_download}", SPEED_URL]
    try:
        out = subprocess.run(cmd, capture_output=True, text=True, timeout=seconds + 5)
        bps = float(out.stdout.strip() or 0)
        return round(bps / 1024 / 1024, 2)  # MB/s
    except Exception:
        return 0.0


VERIFY_SITES = [
    ("gstatic", "https://www.gstatic.com/generate_204"),
    ("github", "https://github.com"),
    ("cf-trace", "https://www.cloudflare.com/cdn-cgi/trace"),
]


def verify_multi(name, timeout_ms=6000):
    """多站点复检。

    为什么要这一步: 实测过一个 134ms、0.7MB/s 的节点, 延迟最低所以被 url-test 选中,
    但它打不开 github/youtube —— 只能过 generate_204。只看单点延迟会挑出"假好"节点。

    用 /proxies/{name}/delay 而不是切 POOL, 因为前者不影响当前选中, 可以并发跑。
    """
    ok, detail = 0, {}
    for label, url in VERIFY_SITES:
        try:
            q = urllib.parse.quote(url, safe="")
            d = api(f"/proxies/{name}/delay?url={q}&timeout={timeout_ms}",
                    timeout=timeout_ms / 1000 + 4)
            good = bool(d and d.get("delay"))
        except Exception:
            good = False
        detail[label] = "ok" if good else "x"
        ok += 1 if good else 0
    return ok, detail


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--nodes", default="data/nodes.json")
    ap.add_argument("--max-speed", type=int, default=80, help="进入带宽测试的节点上限")
    ap.add_argument("--delay-limit", type=int, default=2500, help="延迟准入阈值 ms")
    ap.add_argument("--workers", type=int, default=64)
    ap.add_argument("--keep", type=int, default=30, help="最终保留节点数")
    ap.add_argument("--verify", dest="verify", action="store_true", default=True,
                    help="对入选节点做多站点复检(默认开)")
    ap.add_argument("--no-verify", dest="verify", action="store_false")
    ap.add_argument("--verify-min", type=int, default=2, help="至少通过的站点数")
    ap.add_argument("--verify-workers", type=int, default=16)
    args = ap.parse_args()

    nodes = json.load(open(os.path.join(ROOT, args.nodes)))
    print(f"[1/4] 载入 {len(nodes)} 个节点,生成内核配置...")
    name_map, n_ok = build_config(nodes)
    print(f"      可转换 {n_ok} 个,丢弃 {len(nodes) - n_ok} 个(不支持的加密/协议)")

    print("[2/4] 启动 mihomo 内核...")
    proc = subprocess.Popen([MIHOMO, "-f", CFG], stdout=subprocess.DEVNULL,
                            stderr=subprocess.STDOUT, cwd=ROOT,
                            preexec_fn=os.setsid)
    try:
        if not wait_ready(proc):
            print("      ✗ 内核启动失败,查看 data/generated.yaml")
            return 1
        print("      ✓ 内核就绪")

        print(f"[3/4] 延迟测试 ({len(name_map)} 个, {args.workers} 并发)...")
        results = []
        t0 = time.time()
        with ThreadPoolExecutor(max_workers=args.workers) as ex:
            futs = {ex.submit(delay_test, nm): nm for nm in name_map}
            done = 0
            for f in as_completed(futs):
                nm = futs[f]
                done += 1
                if done % 50 == 0:
                    print(f"      {done}/{len(name_map)} ...", flush=True)
                try:
                    med, jitter = f.result()
                except Exception:
                    med, jitter = None, None
                if med and med <= args.delay_limit:
                    n = dict(name_map[nm])
                    n.update(name=nm, latency=med, jitter=jitter)
                    results.append(n)
        alive = len(results)
        print(f"      ✓ 存活 {alive} 个 (延迟≤{args.delay_limit}ms),耗时 {time.time()-t0:.0f}s")
        if not results:
            print("      ✗ 无可用节点")
            return 1

        results.sort(key=lambda x: x["latency"])
        cand = results[:args.max_speed]
        print(f"[4/4] 带宽测试 (前 {len(cand)} 个,每个 8s)...")
        for i, n in enumerate(cand, 1):
            n["speed"] = speed_test(n["name"])
            print(f"      {i}/{len(cand)}  {n['name']}  {n['latency']}ms  {n['speed']}MB/s", flush=True)

        # 综合评分: 带宽 60% + 延迟 30% + 稳定性 10%
        sp = [n["speed"] for n in cand] or [0]
        lt = [n["latency"] for n in cand] or [1]
        jt = [n["jitter"] or 0 for n in cand] or [0]
        ms, ml, mj = max(sp) or 1, max(lt) or 1, (max(jt) or 1)
        for n in cand:
            n["score"] = round(
                0.60 * (n["speed"] / ms)
                + 0.30 * (1 - n["latency"] / ml)
                + 0.10 * (1 - (n["jitter"] or 0) / mj), 4)

        best = sorted([n for n in cand if n["speed"] > 0],
                      key=lambda x: -x["score"])[:args.keep]
        print(f"      ✓ 到达可用终点 {len(best)} 个")

        # [5] 多站点复检 —— 淘汰"能下载但打不开常用站"的假好节点
        if args.verify and best:
            print(f"[5/5] 多站点复检 ({len(best)} 个 × {len(VERIFY_SITES)} 站)...")
            t0 = time.time()

            def _v(n):
                try:
                    return verify_multi(n["name"])
                except Exception:
                    return 0, {}

            with ThreadPoolExecutor(max_workers=args.verify_workers) as ex:
                for n, res in zip(best, ex.map(_v, best)):
                    ok, detail = res
                    n["verify_ok"] = ok
                    n["verify"] = detail
                    print(f"      {n['name']:<28s} {ok}/{len(VERIFY_SITES)}  "
                          f"{' '.join(k for k, v in detail.items() if v == 'ok')}", flush=True)

            kept = [n for n in best if n.get("verify_ok", 0) >= args.verify_min]
            if len(kept) < 3:
                relaxed = [n for n in best if n.get("verify_ok", 0) >= 1]
                if len(relaxed) > len(kept):
                    print(f"      ! 严格标准仅剩 {len(kept)} 个, 放宽到「至少通 1 站」"
                          f" → {len(relaxed)} 个")
                    kept = relaxed
            if kept:
                best = kept
            else:
                print("      ! 复检全灭 —— 大概率是本机网络问题, 保留复检前结果")
            print(f"      ✓ 复检后 {len(best)} 个, 耗时 {time.time()-t0:.0f}s")

        outdir = os.path.join(ROOT, "out")
        os.makedirs(outdir, exist_ok=True)
        json.dump(best, open(os.path.join(outdir, "nodes_best.json"), "w"),
                  ensure_ascii=False, indent=1)

        # 导出可直接使用的 mihomo/clash 订阅
        proxies = [to_mihomo(n, f"{n['proto'].upper()}-{n['server']}-{n['speed']}MB") for n in best]
        proxies = [p for p in proxies if p]
        sub = {"proxies": proxies}
        with open(os.path.join(outdir, "best.yaml"), "w") as f:
            f.write(emit(sub) + "\n")

        # 报告
        lines = ["# 节点池实测报告", "",
                 f"- 采集去重: **{len(nodes)}** 个",
                 f"- 延迟存活: **{alive}** 个 (≤{args.delay_limit}ms)",
                 f"- 完成测速: **{len([n for n in cand if n['speed'] > 0])}** 个",
                 f"- 最终入选: **{len(best)}** 个", "",
                 "| # | 协议 | 服务器 | 延迟 | 抖动 | 下载 | 复检 | 评分 | 来源 |",
                 "|---|------|--------|------|------|------|------|------|------|"]
        for i, n in enumerate(best, 1):
            v = n.get("verify_ok")
            vs = f"{v}/{len(VERIFY_SITES)}" if v is not None else "-"
            lines.append(f"| {i} | {n['proto']} | {n['server']}:{n['port']} | "
                         f"{n['latency']}ms | {n['jitter']}ms | {n['speed']}MB/s | "
                         f"{vs} | {n['score']:.3f} | {n['source']} |")
        open(os.path.join(outdir, "report.md"), "w").write("\n".join(lines) + "\n")
        print(f"\n✅ 结果已写入 out/ (best.yaml / report.md / nodes_best.json)")
        if best:
            avg = sum(n["speed"] for n in best) / len(best)
            print(f"   入选节点平均带宽 {avg:.2f} MB/s,最快 {max(n['speed'] for n in best)} MB/s,"
                  f"最低延迟 {min(n['latency'] for n in best)}ms")
    finally:
        try:
            os.killpg(os.getpgid(proc.pid), signal.SIGTERM)
        except Exception:
            pass
    return 0


if __name__ == "__main__":
    sys.exit(main())
