#!/usr/bin/env python3
"""CF 优选 IP 扫描器 —— 没有域名也能跑。

为什么这件事必须先做
--------------------
Cloudflare 的 IP 段在国内普遍被限速, 但这不等于"全都不行"。同一段里,
有的 IP 在这条宽带上能跑几 MB/s, 有的只有 20KB/s。**选哪个 IP 比选哪个
协议影响大得多** —— 这也是为什么别人的 CF 节点教程永远在强调"优选 IP"。

探测对象用 CF 自己的域名, 所以不需要你有域名:
    cloudflare.com        → 判断这是不是真的 CF 边缘(要 200 + trace 文本)
    speed.cloudflare.com  → 下载 1MB, 测真实吞吐

等自己的域名就位, 用 --host 换成自己的域名复扫一次会更准(按你自己的
zone 路由, 而不是 cloudflare.com 的 zone)。

测量纪律(全部踩过, 写死在这里)
------------------------------
1. 必须 `--noproxy '*'`。本机环境挂着 http_proxy, 不清掉的话 curl 会走
   本机 Clash, 测的是 Clash 的线路, 不是这个 IP 的线路。
2. 但**绝不**同时给 `-x`。`--noproxy` 会把 `-x` 一起作废, 测出来是直连
   —— 2026-11-11 那轮"179 个可用节点"假数据的根因就是这个组合。
3. 测速阶段低并发。同一个 ss 节点 4 并发测出 0.7KB/s, 串行 551KB/s,
   差 800 倍。所以 TCP 阶段可以 64 并发, 测速阶段只给 4。

用法
----
    python3 cf_scan.py                          # 扫 400 个, 出 top 20
    python3 cf_scan.py --sample 800 -j 64
    python3 cf_scan.py --host node.example.com  # 有域名后按自己 zone 复扫
"""
from __future__ import annotations

import argparse
import json
import os
import random
import socket
import subprocess
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

ROOT = Path(__file__).parent
OUT = ROOT / "data" / "cf_ips.json"

# 环境里的小写 http_proxy 会劫持 curl, 大小写两套都要清。
CLEAN_ENV = {k: v for k, v in os.environ.items()
             if k.lower() not in ("http_proxy", "https_proxy", "all_proxy")}

# 常被用来做优选 IP 的 CF 段(网段, 前缀长度)。
CF_BLOCKS = [
    ("104.16.0.0", 13),      # 104.16~23   最主流
    ("104.24.0.0", 14),      # 104.24~27
    ("172.64.0.0", 13),      # 172.64~71
    ("162.159.192.0", 24),   # WARP 段, 常常很快
    ("188.114.96.0", 20),
    ("190.93.240.0", 20),
    ("197.234.240.0", 22),
    ("198.41.128.0", 17),
]

SNI_JUDGE = "cloudflare.com"
SNI_SPEED = "speed.cloudflare.com"
SPEED_URL = f"https://{SNI_SPEED}/__down?bytes=1000000"
CANARY_IP = "104.16.132.229"      # CF 的 anycast 边缘, 长期稳定, 用来验证测量链


def log(msg: str) -> None:
    print(msg, flush=True)


def _ip_to_int(ip: str) -> int:
    a, b, c, d = (int(x) for x in ip.split("."))
    return (a << 24) | (b << 16) | (c << 8) | d


def _int_to_ip(v: int) -> str:
    return ".".join(str((v >> s) & 255) for s in (24, 16, 8, 0))


def sample_ips(n: int, seed: int | None = None) -> list[str]:
    """在每个段里均匀取样。不是顺序扫 —— 顺序扫会集中在一两个段,
    得出"这段全好/这段全坏"的假结论。"""
    rnd = random.Random(seed)
    ranges = [(_ip_to_int(ip), 32 - plen) for ip, plen in CF_BLOCKS]
    weights = [2 ** bits for _, bits in ranges]
    total = sum(weights)
    out: set[str] = set()
    while len(out) < n:
        pick = rnd.randrange(total)
        for (base, bits), w in zip(ranges, weights):
            if pick < w:
                # 段内随机偏移, 但避开网络号/广播号
                off = rnd.randrange(1, 2 ** bits - 1) if bits > 1 else 0
                out.add(_int_to_ip(base + off))
                break
            pick -= w
    return sorted(out)


def _curl(args: list[str], timeout: int) -> tuple[float, float, str]:
    """返回 (建连毫秒, 首包毫秒, http_code)。

    只用 `--noproxy '*'`, 绝不带 `-x` —— 这里的每个请求本来就该是直连。
    """
    r = subprocess.run(
        ["curl", "-s", "-o", "/dev/null", "-m", str(timeout), "--noproxy", "*", *args,
         "-w", "%{time_appconnect} %{time_starttransfer} %{http_code}"],
        capture_output=True, text=True, env=CLEAN_ENV)
    parts = (r.stdout or "").split()
    if len(parts) != 3:
        return 0.0, 0.0, "000"
    try:
        return float(parts[0]) * 1000, float(parts[1]) * 1000, parts[2]
    except ValueError:
        return 0.0, 0.0, "000"


def tcp_connect(ip: str, timeout: float) -> tuple[str, bool, float]:
    t0 = time.time()
    try:
        with socket.create_connection((ip, 443), timeout=timeout):
            return ip, True, (time.time() - t0) * 1000
    except OSError:
        return ip, False, 0.0


def tls_judge(ip: str, host: str, timeout: int = 5) -> dict | None:
    """确认这个 IP 真的在提供 CF 边缘服务, 并记录 TLS 与首包耗时。"""
    tls_ms, ttfb_ms, code = _curl(
        ["--resolve", f"{host}:443:{ip}", f"https://{host}/cdn-cgi/trace"], timeout)
    if code != "200":
        return None
    return {"ip": ip, "tls_ms": round(tls_ms), "ttfb_ms": round(ttfb_ms)}


def measure_speed(ip: str, timeout: int = 12) -> float:
    """下 1MB, 返回 KB/s。走 speed.cloudflare.com 的 zone, 不需要自己的域名。"""
    r = subprocess.run(
        ["curl", "-s", "-o", "/dev/null", "-m", str(timeout), "--noproxy", "*",
         "--resolve", f"{SNI_SPEED}:443:{ip}", SPEED_URL,
         "-w", "%{speed_download} %{http_code}"],
        capture_output=True, text=True, env=CLEAN_ENV)
    parts = (r.stdout or "").split()
    if len(parts) != 2 or parts[1] != "200":
        return 0.0
    try:
        return float(parts[0]) / 1024          # B/s → KB/s
    except ValueError:
        return 0.0


def canary(host: str) -> None:
    """开跑前的入口自检: 一个已知可用的 CF IP 必须能测通。
    测不通说明是测量链坏了(代理没绕开 / 网络断了), 不是 IP 不好。"""
    log("入口自检...")
    one = tls_judge(CANARY_IP, host)
    if not one:
        raise SystemExit(f"自检失败: 已知可用的 {CANARY_IP} 都测不通, "
                         f"先查本机代理和网络, 不要相信后面的任何数据")
    spd = measure_speed(CANARY_IP)
    # 下载测不出来不算失败: 自检的目的是确认"请求真的直连到了 CF 边缘",
    # TLS 校验已经回答了这个问题。测速走的是另一个 zone, 偶发超时不该中止。
    extra = f"下载 {spd:.0f}KB/s" if spd else "下载未测出(不影响判定)"
    log(f"  ✓ {CANARY_IP} 通过: tls {one['tls_ms']}ms, 首包 {one['ttfb_ms']}ms, {extra}")


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--sample", type=int, default=400, help="采样多少个 IP")
    ap.add_argument("-j", "--workers", type=int, default=64, help="TCP 阶段并发")
    ap.add_argument("--judge", type=int, default=80, help="进入 TLS 校验的数量上限")
    ap.add_argument("--speed", type=int, default=24, help="进入测速的数量上限")
    ap.add_argument("--speed-workers", type=int, default=4,
                    help="测速并发。别调高: 并发会让速度失真几十倍")
    ap.add_argument("--host", default=SNI_JUDGE, help="SNI 域名, 有自己域名后换成它")
    ap.add_argument("--seed", type=int, default=None)
    ap.add_argument("--out", default=str(OUT))
    args = ap.parse_args()

    canary(args.host)

    ips = sample_ips(args.sample, args.seed)
    log(f"采样 {len(ips)} 个 IP (来源 {len(CF_BLOCKS)} 个段)")

    t0 = time.time()
    alive: list[tuple[str, float]] = []
    with ThreadPoolExecutor(max_workers=args.workers) as ex:
        for ip, ok, ms in ex.map(lambda i: tcp_connect(i, 1.5), ips):
            if ok:
                alive.append((ip, ms))
    alive.sort(key=lambda x: x[1])
    log(f"  TCP 443 连上 {len(alive)}/{len(ips)} ({len(alive)/len(ips)*100:.0f}%), "
        f"用时 {time.time()-t0:.0f}s")

    cand = [ip for ip, _ in alive[:args.judge]]
    judged: list[dict] = []
    with ThreadPoolExecutor(max_workers=16) as ex:
        for r in ex.map(lambda i: tls_judge(i, args.host), cand):
            if r:
                judged.append(r)
    log(f"  TLS + trace 校验通过 {len(judged)}/{len(cand)}")
    if not judged:
        log("没有一个通过校验 —— 这些 IP 可能不是 CF 边缘, 或链路被完全阻断")
        return 1

    judged.sort(key=lambda r: r["ttfb_ms"])
    final = judged[:args.speed]
    log(f"  对最快的 {len(final)} 个测 1MB 下载 (并发 {args.speed_workers}, 必须低)...")
    with ThreadPoolExecutor(max_workers=args.speed_workers) as ex:
        speeds = list(ex.map(lambda r: measure_speed(r["ip"]), final))
    for r, s in zip(final, speeds):
        r["speed_kbps"] = round(s)

    final = [r for r in final if r["speed_kbps"] > 0]
    final.sort(key=lambda r: (-r["speed_kbps"], r["ttfb_ms"]))

    log(f"\n总用时 {time.time()-t0:.0f}s。测到速度的 {len(final)} 个, 前 15:")
    for r in final[:15]:
        log(f"   {r['ip']:16} {r['speed_kbps']:7}KB/s   首包 {r['ttfb_ms']:5}ms   "
            f"TLS {r['tls_ms']:4}ms")

    payload = {"at": int(time.time()), "host": args.host,
               "sampled": len(ips), "tcp_ok": len(alive), "results": final}
    Path(args.out).parent.mkdir(parents=True, exist_ok=True)
    Path(args.out).write_text(json.dumps(payload, ensure_ascii=False, indent=1))
    log(f"\n已写入 {args.out} ({len(final)} 条)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
