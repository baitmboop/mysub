#!/usr/bin/env python3
"""把「自己部署的 CF Worker」生成成订阅。

和 cf_scan.py 的关系
--------------------
cf_scan 负责找"从你这条宽带上最快的 CF 入口 IP" —— 那一步不需要域名。
本文件负责把域名 + UUID + 路径 + 那些 IP 拼成客户端能吃的节点。

为什么参数要单独放一个文件
--------------------------
UUID 和 path 是凭据, 等于密码。写进 .py 会被 git 记住, 一旦推上去就等于
公开。所以它们在 data/cf_self.json, 这个文件已经 .gitignore。

用法
----
    python3 cf_sub.py --init          # 生成 UUID / path 的模板, 之后手填 domain
    python3 cf_sub.py --worker        # 把 UUID/path 灌进 worker.js, 出可直接粘贴的副本
    python3 cf_sub.py --emit          # 生成 out/sub_self.yaml (需要先有 domain)
    python3 cf_sub.py --emit --top 5  # 只取最快的 5 个 IP
    python3 cf_sub.py --emit --probe  # 生成后立刻用 verify 实跑一遍

domain 不需要自己买: 注册 CF 免费账号后 xxx.workers.dev 就是白送的子域,
免绑卡免备案。TLS 握手必须带 SNI, 而 SNI 只能是域名 —— 所以"零域名"不存在,
但"零成本域名"存在。

注意: CF 免费版只放行标准端口, 443 之外的端口一律不通。所以所有节点
固定 443, 不要试 8080/2053 之类, 那是付费版的待遇。
"""
from __future__ import annotations

import argparse
import json
import secrets
import subprocess
import sys
import time
import uuid as uuidlib
from pathlib import Path
from urllib.parse import quote

ROOT = Path(__file__).parent
CFG = ROOT / "data" / "cf_self.json"
IPS = ROOT / "data" / "cf_ips.json"
OUT = ROOT / "out" / "sub_self.yaml"
WORKER_TPL = ROOT / "worker" / "worker.js"
WORKER_OUT = ROOT / "out" / "worker.js"
MIHOMO = ROOT / "bin" / "mihomo"

DEFAULT_PORT = 443


def log(msg: str) -> None:
    print(msg, flush=True)


def load_cfg() -> dict:
    if not CFG.exists():
        raise SystemExit(f"没有配置文件 {CFG}\n先跑: python3 cf_sub.py --init")
    return json.loads(CFG.read_text())


def init_cfg() -> int:
    if CFG.exists():
        log(f"{CFG} 已存在, 不覆盖(里面可能已经有你的 UUID)")
        return 0
    cfg = {
        "uuid": str(uuidlib.uuid4()),
        "domain": "",
        "path": "/" + secrets.token_hex(6),
        "ips": [],
        "note": "domain 填你绑到 Worker 的域名; path 要和 Worker 里配的一致",
    }
    CFG.parent.mkdir(parents=True, exist_ok=True)
    CFG.write_text(json.dumps(cfg, ensure_ascii=False, indent=1))
    CFG.chmod(0o600)
    log(f"已生成 {CFG}")
    log(f"  UUID : {cfg['uuid']}")
    log(f"  path : {cfg['path']}")
    log("这两个值要填进 Worker 的设置里, 两边必须完全一致, 否则握手直接失败。")
    return 0


def pick_ips(cfg: dict, top: int) -> list[str]:
    if cfg.get("ips"):
        return list(cfg["ips"])[:top]
    if not IPS.exists():
        raise SystemExit(f"没有 IP 列表。先跑 python3 cf_scan.py 生成 {IPS}")
    data = json.loads(IPS.read_text())
    rows = sorted(data.get("results") or [],
                  key=lambda r: (-(r.get("speed_kbps") or 0), r.get("ttfb_ms") or 9999))
    return [r["ip"] for r in rows][:top]


def build_node(cfg: dict, ip: str, idx: int) -> dict:
    domain = cfg["domain"]
    path = cfg.get("path") or "/"
    return {
        "name": f"CF自建 {idx:02d} {ip}",
        "type": "vless",
        "server": ip,
        "port": DEFAULT_PORT,
        "uuid": cfg["uuid"],
        # Worker 只能出站 TCP(cloudflare:sockets 没有 UDP), 写 true 会让
        # DNS 走 cmd=2 被后端拒掉, 表现为"节点通但网页打不开"。
        "udp": False,
        "tls": True,
        "servername": domain,
        "network": "ws",
        "ws-opts": {"path": path, "headers": {"Host": domain}},
        "client-fingerprint": "chrome",
        "skip-cert-verify": False,
    }


def vless_url(cfg: dict, ip: str) -> str:
    domain = cfg["domain"]
    q = (f"encryption=none&security=tls&sni={domain}&fp=chrome"
         # safe="" 让 "/" 编码成 %2F。虽然多数客户端两种都能解析, 但 %2F
         # 是确定的写法; 不编码时 path 里的 & 或 ? 会直接把 query 切断。
         f"&type=ws&host={domain}&path={quote(cfg.get('path') or '/', safe='')}")
    return f"vless://{cfg['uuid']}@{ip}:{DEFAULT_PORT}?{q}#CF-{ip}"


def emit(cfg: dict, top: int, probe: bool) -> int:
    domain = (cfg.get("domain") or "").strip()
    if not domain:
        raise SystemExit("domain 还是空的。\n"
                         "先把域名绑到 Worker 上(或者先用 xxx.workers.dev 试), "
                         f"再填进 {CFG} 的 domain 字段。\n"
                         "没有域名也能先跑 cf_scan.py 挑 IP —— 那一步不依赖域名。")

    ips = pick_ips(cfg, top)
    if not ips:
        raise SystemExit("一个 IP 都没有, 先跑 cf_scan.py")

    proxies = [build_node(cfg, ip, i + 1) for i, ip in enumerate(ips)]
    names = [p["name"] for p in proxies]
    groups = [
        {"name": "自建", "type": "select", "proxies": names},
        {"name": "自建-自动", "type": "url-test", "proxies": names,
         "url": "https://www.gstatic.com/generate_204", "interval": 300},
    ]
    doc = {"mixed-port": 7890, "allow-lan": False, "mode": "rule",
           "log-level": "info", "proxies": proxies,
           "proxy-groups": groups, "rules": ["MATCH,自建"]}

    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(_dump_yaml(doc))
    log(f"已写入 {OUT} ({len(proxies)} 个节点, 全部指向 {domain})")

    links = [vless_url(cfg, ip) for ip in ips]
    (OUT.with_suffix(".links.txt")).write_text("\n".join(links) + "\n")
    log(f"同时写入 {OUT.with_suffix('.links.txt')} ({len(links)} 条 vless:// 链接)")

    if MIHOMO.exists():
        r = subprocess.run([str(MIHOMO), "-t", "-f", str(OUT), "-d", "/tmp/mh_cf"],
                           capture_output=True, text=True)
        ok = "successful" in (r.stdout or "") + (r.stderr or "")
        log(f"内核配置校验: {'通过' if ok else '失败'}\n{(r.stdout or r.stderr).strip()[-300:]}")
        if not ok:
            return 1

    if probe:
        log("\n用 verify 实测(这才是唯一能证明它真能用的证据)...")
        import verify                                      # noqa: PLC0415
        hit = 0
        for p in proxies[:3]:
            node = {"proto": "vless", "server": p["server"], "port": p["port"],
                    "uuid": p["uuid"], "tls": True, "servername": p["servername"],
                    "network": "ws", "ws-opts": p["ws-opts"],
                    "client-fingerprint": "chrome"}
            r = verify.probe_kernel(node, 8.0, verify.PROBE_URLS[:1])
            ok = r.get("verdict") == "alive"
            hit += ok
            log(f"  {'OK' if ok else 'XX'} {p['server']:16} {r}")
        log(f"\n{min(3, len(proxies))} 个里可用 {hit} 个")
        if hit == 0:
            log("一个都不通。按这个顺序查: 1) Worker 是否部署成功 2) UUID/path 两边是否一致 "
                "3) 域名是否已 Active 4) IP 是否被断")
            return 1
    return 0


def emit_worker(cfg: dict) -> int:
    """把 UUID / path 灌进模板。省得手改 —— 手改最容易多一个空格或少一个斜杠,
    而这两种错都只表现为"握手失败", 看不出原因。"""
    if not WORKER_TPL.exists():
        raise SystemExit(f"模板不存在: {WORKER_TPL}")
    src = WORKER_TPL.read_text()
    if "__UUID__" not in src or "__PATH__" not in src:
        raise SystemExit("模板里的占位符被人改过了, 检查 worker/worker.js")
    out = src.replace("__UUID__", cfg["uuid"]).replace("__PATH__", cfg["path"])
    WORKER_OUT.parent.mkdir(parents=True, exist_ok=True)
    WORKER_OUT.write_text(out)
    log(f"已写入 {WORKER_OUT}")
    log("  接下来: CF 控制台 → Workers & Pages → Create → Worker → Deploy")
    log("          → Edit code → 整份粘贴 → Deploy")
    log("          → 访问 https://<名字>.<账号>.workers.dev 看到 ok")
    log(f"          → 把该域名填进 {CFG} 的 domain 字段")
    log(f"  然后: python3 cf_sub.py --emit --probe")
    return 0


def _dump_yaml(doc: dict) -> str:
    try:
        import yaml                                        # noqa: PLC0415
        return yaml.safe_dump(doc, allow_unicode=True, sort_keys=False)
    except ImportError:
        return json.dumps(doc, ensure_ascii=False, indent=1)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--init", action="store_true", help="生成 UUID / path 模板")
    ap.add_argument("--worker", action="store_true", help="生成填好凭据的 Worker 代码")
    ap.add_argument("--emit", action="store_true", help="生成订阅")
    ap.add_argument("--top", type=int, default=5, help="用最快的几个 IP")
    ap.add_argument("--probe", action="store_true", help="生成后用 verify 实跑")
    args = ap.parse_args()

    if args.init:
        return init_cfg()
    if args.worker:
        return emit_worker(load_cfg())
    if args.emit:
        return emit(load_cfg(), args.top, args.probe)
    ap.print_help()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
