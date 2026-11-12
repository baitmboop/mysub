#!/usr/bin/env python3
"""
CF 中转节点生成器 —— 模仿频道那套"给 IP 清单 + 换成节点"的做法。

思路
----
频道发的不是节点，是 **CF 边缘 IP:端口 清单**。单看没用，因为一个 IP 只是
"任意门", 真正决定能不能上网的是门后面的后端: 域名 + UUID + path(ws)。
所以完整的链路是三段:

    IP 清单  →  验它是真 CF 边缘  →  套到活的后端上  →  实测

这个脚本把三段都做了。前两段不需要任何账号、任何域名, 现在就能跑;
第三段需要一个**活的后端**, 由 `--template` 提供(频道里给的 vless:// 链接,
或池子里某个节点)。

用法
----
    # 1) 只处理 IP: 给一段频道原文(或文件), 验证 + 测速
    python3 cf_relay.py --posts data/tg_posts.txt
    python3 cf_relay.py --text "104.16.1.1:443 104.17.2.2:2053 ..."

    # 2) 直接抓频道网页预览(需要能访问 t.me)
    python3 cf_relay.py --channel wxgqlfx

    # 3) 套模板: 用频道给的 vless 链接当后端, 换成上面的 IP
    python3 cf_relay.py --posts data/tg_posts.txt \\
        --template "vless://uuid@old.example.com:443?security=tls&type=ws&path=/&host=old.example.com#demo"

    # 4) 用池子里某个节点的指纹当后端(按 server:port 找)
    python3 cf_relay.py --posts data/tg_posts.txt --template-node hk3.wawojk.dpdns.org:51314

为什么不用现成的"IP 清单"直接当节点
------------------------------------
因为它们只是入口。同一批 IP 配 A 后端能上网、配 B 后端 404, 差别全在后端。
这一点在 2026-11-12 的实验里被证实: 62 个 CF 前置域名换好 IP 后,
只有 9 个 zone 还响应, 9 个里 0 个协议层能通 —— 后端死了, 换 IP 救不回来。
"""
import argparse
import json
import os
import re
import socket
import subprocess
import sys
import time
import urllib.parse
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

ROOT = Path(__file__).parent
OUT = ROOT / "data" / "cf_relay_ips.json"
UA = ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36")

# 大小写两套代理都要清: 小写那套会劫持 curl, 把测量导去本机 VPN。
CLEAN_ENV = {k: v for k, v in os.environ.items()
             if k.lower() not in ("http_proxy", "https_proxy", "all_proxy")}

# 判断"是不是真 CF 边缘"用的 zone。用 CF 自己的域名, 不需要自己的。
SNI_JUDGE = "cloudflare.com"
SNI_SPEED = "speed.cloudflare.com"
SPEED_URL = f"https://{SNI_SPEED}/__down?bytes=1000000"
CANARY_IP = "104.16.132.229"

# CF 免费/Pro 计划允许回源的端口。别的端口连上了也不是 CF 边缘服务。
CF_PORTS = {443, 2052, 2053, 2082, 2083, 2086, 2087, 2095, 2096, 8080, 8443, 8880}

IP_PORT_RE = re.compile(
    r"(?<![\d.])(\d{1,3}(?:\.\d{1,3}){3})(?::(\d{1,5}))?(?![\d.])")
LINK_RE = re.compile(
    r"(?:vless|vmess|trojan|ss|hysteria2|hy2|tuic)://[^\s\"'<>]+", re.I)


def log(msg: str) -> None:
    print(msg, flush=True)


# ---------------------------------------------------------------- 抓文本

def fetch_channel(name: str, timeout: int = 30) -> str:
    """抓公开频道的网页预览 t.me/s/<name>。

    只对公开频道有效; 群组(t.me/+xxx)拿回来是空页。
    走环境里的代理(如果配了) —— 这一步和测速不一样, 测速必须直连。
    """
    url = f"https://t.me/s/{name.lstrip('@')}"
    req = urllib.request.Request(url, headers={"User-Agent": UA})
    raw = urllib.request.urlopen(req, timeout=timeout).read()
    html = raw.decode("utf-8", "replace")
    # 只取消息正文, 免得把侧栏、推荐文章里的 IP 也吃进来。
    bodies = re.findall(
        r'<div class="tgme_widget_message_text[^"]*"[^>]*>(.*?)</div>',
        html, re.S)
    if not bodies:
        return html            # 结构变了就退化成全文, 至少不空手
    text = []
    for b in bodies:
        b = re.sub(r"<br\s*/?>", "\n", b)
        b = re.sub(r"<[^>]+>", "", b)
        text.append(b)
    return "\n".join(text)


def read_posts(args) -> str:
    parts = []
    if args.channel:
        for ch in args.channel:
            log(f"抓频道 {ch} ...")
            try:
                t = fetch_channel(ch, args.fetch_timeout)
                log(f"  {len(t)} 字符")
                parts.append(t)
            except Exception as e:
                log(f"  失败: {type(e).__name__}: {e}")
    if args.posts:
        parts.append(Path(args.posts).read_text(encoding="utf-8", errors="replace"))
    if args.text:
        parts.append(args.text)
    return "\n".join(parts)


def extract_ips(text: str) -> list[tuple[str, int]]:
    """从任意文本里抠 IP:端口。没写端口按 443 算。"""
    out: dict[tuple[str, int], int] = {}
    for m in IP_PORT_RE.finditer(text):
        ip, port = m.group(1), m.group(2)
        # 排除版本号、日期之类: 四段都必须 0~255
        try:
            if any(not (0 <= int(x) <= 255) for x in ip.split(".")):
                continue
        except ValueError:
            continue
        p = int(port) if port else 443
        if not (1 <= p <= 65535):
            continue
        out[(ip, p)] = out.get((ip, p), 0) + 1
    # 同一 IP 出现多次说明是频道反复推的, 排前面
    return sorted(out, key=lambda k: -out[k])


def extract_links(text: str) -> list[str]:
    return [l.rstrip(".,);") for l in LINK_RE.findall(text)]


# ---------------------------------------------------------------- 验 IP

def _curl(args: list[str], timeout: int) -> tuple[str, float, float]:
    """返回 (http_code, 建连毫秒, 首包毫秒)。

    只用 `--noproxy '*'`, **绝不带 -x** —— 这里每个请求本来就该直连。
    (带 -x 又与 --noproxy 同时给, --noproxy 会把 -x 一起作废, 变成测本机直连。
     2026-11-12 那次"179 个可用节点"的假数据就是这么来的。)
    """
    r = subprocess.run(
        ["curl", "-s", "-o", "/dev/null", "-m", str(timeout), "--noproxy", "*", *args,
         "-w", "%{http_code} %{time_appconnect} %{time_starttransfer}"],
        capture_output=True, text=True, env=CLEAN_ENV)
    parts = (r.stdout or "").split()
    if len(parts) != 3:
        return "000", 0.0, 0.0
    try:
        return parts[0], float(parts[1]) * 1000, float(parts[2]) * 1000
    except ValueError:
        return "000", 0.0, 0.0


def judge_edge(ip: str, port: int, timeout: int = 6) -> dict | None:
    """这个 IP:端口 是不是真的在提供 CF 边缘服务。

    判据是 `/cdn-cgi/trace` 返回 200 且正文里有 `colo=` 和 `fl=` ——
    这两个字段只有 CF 边缘会吐。只 ping 通、只 TCP 连上都不算。
    """
    url = f"https://{SNI_JUDGE}:{port}/cdn-cgi/trace" if port != 443 \
        else f"https://{SNI_JUDGE}/cdn-cgi/trace"
    r = subprocess.run(
        ["curl", "-s", "-m", str(timeout), "--noproxy", "*",
         "--resolve", f"{SNI_JUDGE}:{port}:{ip}", url,
         "-w", "\n%{http_code} %{time_appconnect} %{time_starttransfer}"],
        capture_output=True, text=True, env=CLEAN_ENV)
    body, _, tail = (r.stdout or "").rpartition("\n")
    parts = tail.split()
    if len(parts) != 3 or parts[0] != "200":
        return None
    if "colo=" not in body or "fl=" not in body:
        return None
    colo = re.search(r"colo=(\w+)", body)
    loc = re.search(r"loc=(\w+)", body)
    return {"ip": ip, "port": port,
            "tls_ms": round(float(parts[1]) * 1000),
            "ttfb_ms": round(float(parts[2]) * 1000),
            "colo": colo.group(1) if colo else "?",
            "loc": loc.group(1) if loc else "?"}


def measure_speed(ip: str, port: int, timeout: int = 12) -> float:
    """下 1MB, 返回 KB/s。走 speed.cloudflare.com, 不需要自己的域名。"""
    r = subprocess.run(
        ["curl", "-s", "-o", "/dev/null", "-m", str(timeout), "--noproxy", "*",
         "--resolve", f"{SNI_SPEED}:{port}:{ip}", SPEED_URL,
         "-w", "%{speed_download} %{http_code}"],
        capture_output=True, text=True, env=CLEAN_ENV)
    parts = (r.stdout or "").split()
    if len(parts) != 2 or parts[1] != "200":
        return 0.0
    try:
        return float(parts[0]) / 1024
    except ValueError:
        return 0.0


def canary() -> None:
    """入口自检: 已知可用的 CF IP 必须验得过, 否则中止。

    这一步是那次假数据之后加的 —— 测量链坏掉时, 结果会"变得更好"而不是更差,
    光看数字发现不了。
    """
    log("入口自检...")
    one = judge_edge(CANARY_IP, 443)
    if not one:
        raise SystemExit(
            f"自检失败: 已知可用的 {CANARY_IP} 都验不过。\n"
            f"这不是 IP 的问题, 是测量链断了 —— 先检查本机网络/代理是否把请求劫走。")
    log(f"  ✓ {CANARY_IP} 通过: tls {one['tls_ms']}ms, 首包 {one['ttfb_ms']}ms, "
        f"colo={one['colo']}")


# ---------------------------------------------------------------- 套模板

def node_from_link(link: str) -> dict | None:
    """把 vless:// / trojan:// 链接拆成池子内部结构(proto/server/port/...)。"""
    link = link.strip()
    if link.startswith("vless://"):
        body = link[len("vless://"):].split("#")[0]
        cred, _, rest = body.partition("@")
        hostport, _, query = rest.partition("?")
        host, _, port = hostport.rpartition(":")
        q = {k: v[0] for k, v in urllib.parse.parse_qs(query).items()}
        n = {"proto": "vless", "server": host, "port": int(port or 443), "uuid": cred}
        if q.get("sni"): n["sni"] = q["sni"]
        if q.get("type"): n["network"] = q["type"]
        if q.get("path"): n["path"] = q["path"]
        if q.get("host"): n["host"] = q["host"]
        if q.get("flow"): n["flow"] = q["flow"]
        if q.get("fp"): n["client-fingerprint"] = q["fp"]
        if q.get("security") in ("tls", "reality"): n["tls"] = True
        return n
    if link.startswith("trojan://"):
        body = link[len("trojan://"):].split("#")[0]
        cred, _, rest = body.partition("@")
        hostport, _, query = rest.partition("?")
        host, _, port = hostport.rpartition(":")
        q = {k: v[0] for k, v in urllib.parse.parse_qs(query).items()}
        n = {"proto": "trojan", "server": host, "port": int(port or 443), "password": cred}
        if q.get("sni"): n["sni"] = q["sni"]
        if q.get("type"): n["network"] = q["type"]
        if q.get("path"): n["path"] = q["path"]
        if q.get("host"): n["host"] = q["host"]
        n["tls"] = True
        return n
    return None


def node_from_pool(key: str) -> dict | None:
    """按 server:port 从 pool_history 里取一个节点的完整配置。"""
    import gateway
    hist = gateway.load_json(gateway.HISTORY, {})
    host, _, port = key.rpartition(":")
    for rec in hist.values():
        n = dict(rec.get("node") or {})
        if not n.get("server"):
            continue
        if not n.get("proto") and n.get("type"):
            n["proto"] = n["type"]
        if str(n["server"]) == host and str(n.get("port")) == port:
            return n
    return None


def swap_endpoint(node: dict, ip: str, port: int) -> dict:
    """只换入口地址, 其余(sni/host/path/uuid)一个字节都不动。

    这是整套做法的核心: 后端决定"能不能上网", 入口决定"连得上连不上"。
    换掉入口, 后端保持不变, 就得到了同一个后端的多个可用入口。
    """
    m = dict(node)
    m["server"] = ip
    m["port"] = port
    m["name"] = f"{node.get('server')}@{ip}:{port}"
    return m


def probe_nodes(nodes: list[dict], timeout: float = 8.0) -> list[dict]:
    """用 verify 的内核通道逐个实测。必须串行 —— 并发会让测速失真上百倍。"""
    import verify
    alive = []
    for i, n in enumerate(nodes, 1):
        r = verify.probe_kernel(n, timeout, verify.PROBE_URLS[:1])
        ok = r.get("verdict") == "alive"
        spd = r.get("speed") or 0
        log(f"  [{i}/{len(nodes)}] {'OK' if ok else 'XX'} {n['server']}:{n['port']}"
            f"  {spd:7.1f}KB/s {str(r.get('latency') or '-'):>5}ms"
            f"{'' if ok else '  stage=' + str(r.get('stage'))}")
        if ok:
            n = dict(n)
            n["_speed"] = spd
            n["_latency"] = r.get("latency")
            alive.append(n)
    return alive


# ---------------------------------------------------------------- 主流程

def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--channel", action="append", metavar="NAME",
                    help="抓公开频道 t.me/s/<NAME>, 可重复")
    ap.add_argument("--posts", metavar="FILE", help="频道原文存成的本地文件")
    ap.add_argument("--text", metavar="STR", help="直接给一段文本")
    ap.add_argument("--fetch-timeout", type=int, default=30)
    ap.add_argument("--limit", type=int, default=30, help="最多验证多少个 IP")
    ap.add_argument("--speed", type=int, default=8, help="最多测速多少个")
    ap.add_argument("--speed-workers", type=int, default=4,
                    help="测速并发, 别调高(并发会失真)")
    ap.add_argument("--template", metavar="LINK",
                    help="用一个 vless:// / trojan:// 链接当后端")
    ap.add_argument("--template-node", metavar="SERVER:PORT",
                    help="用池子里某个节点的配置当后端")
    ap.add_argument("--probe-timeout", type=float, default=8.0)
    ap.add_argument("--out", default=str(OUT))
    args = ap.parse_args()

    text = read_posts(args)
    if not text.strip():
        log("没有拿到任何文本。用 --posts 给一段频道原文, 或 --channel 抓网页预览。")
        return 2

    pairs = extract_ips(text)
    links = extract_links(text)
    log(f"\n从文本里抠到 {len(pairs)} 个 IP:端口, {len(links)} 个节点链接")
    if not pairs:
        return 2
    odd = [p for _, p in pairs if p not in CF_PORTS]
    if odd:
        log(f"  注意: {len(odd)} 个端口不在 CF 回源端口表里, 大概率不是 CF: "
            f"{sorted(set(odd))[:6]}")

    canary()

    # --- 第一步: 验哪些真的是 CF 边缘
    todo = pairs[:args.limit]
    log(f"\n验证 {len(todo)} 个 IP:端口是不是真 CF 边缘...")
    edges: list[dict] = []
    with ThreadPoolExecutor(max_workers=8) as ex:
        for res in ex.map(lambda p: judge_edge(*p), todo):
            if res:
                edges.append(res)
    log(f"  真 CF 边缘: {len(edges)}/{len(todo)}")
    if not edges:
        log("  一个都不是。这批 IP 要么过期, 要么根本不是 CF。")
        return 1

    # --- 第二步: 测速, 慢的没必要留
    edges.sort(key=lambda r: r["ttfb_ms"])
    top = edges[:args.speed]
    log(f"\n给最快的 {len(top)} 个测速 (并发 {args.speed_workers})...")
    with ThreadPoolExecutor(max_workers=args.speed_workers) as ex:
        speeds = list(ex.map(lambda r: measure_speed(r["ip"], r["port"]), top))
    for r, s in zip(top, speeds):
        r["speed_kbps"] = round(s)
    edges = [r for r in edges if "speed_kbps" not in r] + top
    edges.sort(key=lambda r: -(r.get("speed_kbps") or 0))

    log(f"\n{'ip':16} {'port':>5} {'colo':>5} {'tls':>6} {'首包':>7} {'速度':>9}")
    for r in edges[:20]:
        sp = f"{r['speed_kbps']}KB/s" if r.get("speed_kbps") is not None else "-"
        log(f"{r['ip']:16} {r['port']:>5} {r['colo']:>5} {r['tls_ms']:>5}ms "
            f"{r['ttfb_ms']:>6}ms {sp:>9}")

    Path(args.out).write_text(json.dumps(
        {"at": int(time.time()), "source": args.channel or args.posts,
         "results": edges}, ensure_ascii=False, indent=1))
    log(f"\nIP 结果已写入 {args.out}")

    # --- 第三步: 有后端才谈得上"变成节点"
    backend = None
    if args.template:
        backend = node_from_link(args.template)
        if not backend:
            log(f"模板解析失败: 只支持 vless:// 和 trojan://")
    elif args.template_node:
        backend = node_from_pool(args.template_node)
        if not backend:
            log(f"池子里找不到 {args.template_node}")

    if not backend:
        log("\n没有 --template / --template-node, 到这里就是终点。")
        log("IP 只是入口: 同一个 IP 配不同的后端, 一个能上网一个 404。")
        log("要变成能用的节点, 还需要一个**活的后端**(域名 + UUID + ws path)。")
        return 0

    net = (backend.get("network") or "").lower()
    log(f"\n后端: {backend.get('proto')} {backend.get('server')}:{backend.get('port')}"
        f"  sni={backend.get('sni') or backend.get('servername')}  net={net}")
    if net != "ws":
        log("  警告: 后端不是 ws。CF 只能承载 HTTP/WebSocket, 非 ws 后端"
            "换成 CF IP 后必定连不上 —— 这条路对它是无效的。")

    cand = [swap_endpoint(backend, r["ip"], r["port"]) for r in edges[:args.speed]]
    log(f"\n把后端套到这 {len(cand)} 个入口上, 逐个实测...")
    alive = probe_nodes(cand, args.probe_timeout)
    log(f"\n可用: {len(alive)}/{len(cand)}")

    if alive:
        import gateway
        out = ROOT / "out" / "cf_relay.yaml"
        out.parent.mkdir(exist_ok=True)
        proxies = []
        for i, n in enumerate(alive, 1):
            p = gateway.to_mihomo(n, f"CF-relay-{i}")
            if p:
                proxies.append(p)
        if proxies:
            out.write_text(json.dumps(
                {"proxies": proxies,
                 "proxy-groups": [{"name": "CF",
                                   "type": "select",
                                   "proxies": [p["name"] for p in proxies]}],
                 "rules": ["MATCH,CF"]}, ensure_ascii=False, indent=1))
            log(f"已写入 {out} ({len(proxies)} 个节点)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
