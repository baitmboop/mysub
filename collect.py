#!/usr/bin/env python3
"""
节点采集器 —— 从 Telegram 频道 + 公开订阅源抓取节点,归一化去重。
用法: python3 collect.py [--proxy http://127.0.0.1:7897]
输出: data/nodes.json
"""
import argparse
import base64
import binascii
import datetime
import hashlib
import json
import os
import re
import sys
import time
import urllib.parse
import urllib.request

UA = ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36")

VG_REGEX = re.compile(
    r'(?:vmess|vless|trojan|ss|ssr|hysteria2|hy2|tuic)://[^\s"\'<>\\]+', re.I)

TG_CHANNELS = [
    "oneclickvpnkeys", "ripaojiedian", "freefq", "v2ray_configs_pool",
    "clashnodes", "nodefree", "freev2rayconfig", "getfreev2ray",
    "Jsnzkpg", "proxy_config", "vpnfail_v2ray", "ssrsub",
]

RAW_SOURCES = [
    ("NoMoreWalls", "https://raw.githubusercontent.com/peasoft/NoMoreWalls/master/list.txt"),
    ("ermaozi", "https://raw.githubusercontent.com/ermaozi/get_subscribe/main/subscribe/v2ray.txt"),
    ("ermaozi_ss", "https://raw.githubusercontent.com/ermaozi/get_subscribe/main/subscribe/ss.txt"),
    ("ermaozi_trojan", "https://raw.githubusercontent.com/ermaozi/get_subscribe/main/subscribe/trojan.txt"),
    ("ripaojiedian", "https://raw.githubusercontent.com/ripaojiedian/freenode/main/sub"),
    ("freefq", "https://raw.githubusercontent.com/freefq/free/master/v2"),
]


def build_opener(proxy: str | None):
    if not proxy:
        return urllib.request.build_opener()
    handler = urllib.request.ProxyHandler({"http": proxy, "https": proxy})
    return urllib.request.build_opener(handler)


def fetch(url: str, opener, timeout=25, retries=3) -> bytes | None:
    req = urllib.request.Request(url, headers={
        "User-Agent": UA,
        "Accept": "*/*",
        "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
    })
    last = ""
    for i in range(retries):
        try:
            with opener.open(req, timeout=timeout) as r:
                data = r.read()
                if data:
                    return data
                last = "空响应"
        except Exception as e:
            last = f"{type(e).__name__}: {str(e)[:60]}"
        if i < retries - 1:
            time.sleep(1.5 * (i + 1))
    print(f"    ! {last}", file=sys.stderr)
    return None


def maybe_b64(text: str) -> str:
    """订阅体常是整体 base64,尝试解码。"""
    s = re.sub(r"\s+", "", text)
    if not s or not re.fullmatch(r"[A-Za-z0-9+/=_-]+", s):
        return text
    pad = s + "=" * (-len(s) % 4)
    for dec in (base64.b64decode, base64.urlsafe_b64decode):
        try:
            out = dec(pad).decode("utf-8")
            if "://" in out:
                return out
        except (binascii.Error, UnicodeDecodeError, ValueError):
            continue
    return text


def html_to_text(html: str) -> str:
    html = re.sub(r"<br\s*/?>", "\n", html, flags=re.I)
    html = re.sub(r"<[^>]+>", "", html)
    html = (html.replace("&amp;", "&").replace("&quot;", '"')
                .replace("&#39;", "'").replace("&lt;", "<").replace("&gt;", ">"))
    return html


def tg_messages(html: str):
    """按消息块切分,产出 (发布时间, 纯文本)。"""
    blocks = re.split(r'(?=<div class="tgme_widget_message[ "])', html)
    for b in blocks:
        m = re.search(r'datetime="([^"]+)"', b)
        yield (m.group(1) if m else None), html_to_text(b)


def age_hours(ts: str | None) -> float:
    if not ts:
        return 99999.0
    try:
        d = datetime.datetime.fromisoformat(ts.replace("Z", "+00:00"))
        if d.tzinfo is None:
            d = d.replace(tzinfo=datetime.timezone.utc)
        return (datetime.datetime.now(datetime.timezone.utc) - d).total_seconds() / 3600
    except Exception:
        return 99999.0


def b64d(s: str) -> str:
    s = s.strip().replace("-", "+").replace("_", "/")
    return base64.b64decode(s + "=" * (-len(s) % 4)).decode("utf-8", "ignore")


def qs(url: str) -> dict:
    return {k: v[0] for k, v in urllib.parse.parse_qs(urllib.parse.urlparse(url).query).items()}


def truthy(v) -> bool:
    return str(v).lower() in ("1", "true", "yes")


def parse_uri(uri: str, source: str) -> dict | None:
    """把各类分享链接归一化成统一结构。"""
    try:
        scheme = uri.split("://", 1)[0].lower()
        body = uri.split("://", 1)[1]

        if scheme == "vmess":
            j = json.loads(b64d(body.split("#")[0]))
            return {
                "proto": "vmess",
                "server": j.get("add", ""),
                "port": int(j.get("port", 0) or 0),
                "uuid": j.get("id", ""),
                "alterId": int(j.get("aid", 0) or 0),
                "cipher": j.get("scy") or "auto",
                "tls": truthy(j.get("tls")),
                "sni": j.get("sni") or j.get("host") or "",
                "network": j.get("net") or "tcp",
                "path": j.get("path") or "",
                "host": j.get("host") or "",
                "name": urllib.parse.unquote(j.get("ps") or ""),
                "source": source,
            }

        # 其余都是 URL 形式
        name = urllib.parse.unquote(body.split("#", 1)[1]) if "#" in body else ""
        base = body.split("#")[0]
        p = qs(uri)
        netloc = base.split("?", 1)[0]
        netloc = netloc.split("@", 1)[1] if "@" in netloc else netloc.split("://")[-1]
        host, _, port = netloc.rpartition(":")
        port = int(re.sub(r"\D", "", port) or 0)

        if scheme == "vless":
            return {
                "proto": "vless",
                "server": host.strip("[]"),
                "port": port,
                "uuid": base.split("@")[0],
                "tls": p.get("security") in ("tls", "reality"),
                "sni": p.get("sni") or p.get("peer") or "",
                "network": p.get("type") or "tcp",
                "path": urllib.parse.unquote(p.get("path", "")),
                "host": p.get("host", ""),
                "flow": p.get("flow", ""),
                "pbk": p.get("pbk", ""),
                "sid": p.get("sid", ""),
                # 注意: 这是 URI 里的 uTLS 指纹(chrome/firefox/randomized...),
                # 不是节点指纹。必须叫 utls —— 曾经叫 fp, 结果被 ingest() 里的
                # 节点哈希覆盖掉, 所有节点都退回默认指纹, reality 节点握手会失败。
                "utls": p.get("fp", ""),
                "reality": p.get("security") == "reality",
                "name": name,
                "source": source,
            }

        if scheme == "trojan":
            return {
                "proto": "trojan",
                "server": host.strip("[]"),
                "port": port,
                "password": urllib.parse.unquote(base.split("@")[0]),
                "sni": p.get("sni") or "",
                "network": p.get("type") or "tcp",
                "path": urllib.parse.unquote(p.get("path", "")),
                "host": p.get("host", ""),
                "skipCertVerify": truthy(p.get("allowInsecure", 0)),
                "name": name,
                "source": source,
            }

        if scheme == "ss":
            if "@" in base:  # ss://b64(method:pass)@host:port
                user = base.split("@")[0]
                method, _, password = b64d(user).partition(":")
                netloc = base.split("@", 1)[1]
                host, _, port = netloc.rpartition(":")
            else:            # ss://b64(method:pass@host:port)
                decoded = b64d(base)
                user, _, netloc = decoded.partition("@")
                method, _, password = user.partition(":")
                host, _, port = netloc.rpartition(":")
            return {
                "proto": "ss",
                "server": host.strip("[]"),
                "port": int(re.sub(r"\D", "", port) or 0),
                "cipher": method,
                "password": password,
                "name": name,
                "source": source,
            }

        if scheme in ("hysteria2", "hy2"):
            return {
                "proto": "hysteria2",
                "server": host.strip("[]"),
                "port": port,
                "password": urllib.parse.unquote(base.split("@")[0]),
                "sni": p.get("sni") or "",
                "skipCertVerify": truthy(p.get("insecure", 0)),
                "name": name,
                "source": source,
            }

        if scheme == "tuic":
            cred, _, _ = base.partition("@")
            uuid, _, pwd = cred.partition(":")
            return {
                "proto": "tuic",
                "server": host.strip("[]"),
                "port": port,
                "uuid": uuid,
                "password": urllib.parse.unquote(pwd),
                "sni": p.get("sni") or "",
                "alpn": p.get("alpn", "").split(",") if p.get("alpn") else [],
                "skipCertVerify": truthy(p.get("allow_insecure", 0)),
                "name": name,
                "source": source,
            }
    except Exception:
        return None
    return None


def fingerprint(n: dict) -> str:
    key = f"{n['proto']}|{n['server']}|{n['port']}|{n.get('uuid') or n.get('password') or ''}"
    return hashlib.sha1(key.encode()).hexdigest()[:16]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--proxy", default=os.environ.get("HTTPS_PROXY") or None)
    ap.add_argument("--out", default="data/nodes.json")
    ap.add_argument("--openai-only", action="store_true", help="仅采集 AI 友好来源(默认全采)")
    ap.add_argument("--max-age", type=float, default=72, help="只采最近 N 小时内的 TG 帖子")
    args = ap.parse_args()

    opener = build_opener(args.proxy)
    pool: dict[str, dict] = {}
    stats: dict[str, int] = {}

    def ingest(text: str, source: str):
        text = maybe_b64(text)
        found = VG_REGEX.findall(text)
        for uri in found:
            n = parse_uri(uri, source)
            if not n or not n.get("server") or not n.get("port"):
                continue
            fp = fingerprint(n)
            if fp in pool:
                srcs = pool[fp]["sources"]
                if source not in srcs:
                    srcs.append(source)
                continue
            n["fp"] = fp
            n["sources"] = [source]
            pool[fp] = n
        stats[source] = stats.get(source, 0) + len(found)
        print(f"    → 抽出 {len(found):4d} 条, 去重后累计 {len(pool)}")

    print("[1/2] 公开订阅源")
    for name, url in RAW_SOURCES:
        print(f"  * {name}: {url}")
        body = fetch(url, opener)
        if body:
            ingest(body.decode("utf-8", "ignore"), name)
            for m in list(set(re.findall(r"https?://[^\s\"']+", body.decode("utf-8", "ignore"))))[:3]:
                if any(k in m for k in ("sub", "token", "clash", "v2ray")) and "github.com" not in m:
                    sub = fetch(m, opener)
                    if sub:
                        ingest(sub.decode("utf-8", "ignore"), f"{name}→child")

    print("[2/2] Telegram 频道 (t.me/s/ 网页预览)")
    for ch in TG_CHANNELS:
        body = fetch(f"https://t.me/s/{ch}", opener)
        if not body:
            print(f"  * @{ch}: 无响应")
            continue
        html = body.decode("utf-8", "ignore")
        fresh, stale = [], 0
        for ts, msg in tg_messages(html):
            if not msg.strip():
                continue
            if age_hours(ts) > args.max_age:
                if VG_REGEX.search(msg):
                    stale += 1
                continue
            fresh.append(msg)
        joined = "\n".join(fresh)
        tag = f"最新 {len(fresh)} 帖" + (f", 跳过 {stale} 条过期" if stale else "")
        if not VG_REGEX.search(joined):
            print(f"  * @{ch}: 页面 {len(body)//1024}KB, 无直出节点 ({tag})")
        else:
            print(f"  * @{ch}: 页面 {len(body)//1024}KB ({tag})")
        ingest(joined, f"tg:@{ch}")
        # 频道里贴的订阅链接也抓
        for u in set(re.findall(r"https?://[^\s\"'<>]+", joined)):
            if any(k in u for k in ("/sub", "token=", "/api/v1/client/subscribe")) and "t.me" not in u:
                sub = fetch(u, opener)
                if sub:
                    ingest(sub.decode("utf-8", "ignore"), f"tg:@{ch}→sub")

    nodes = sorted(pool.values(), key=lambda x: (x["proto"], x["server"]))
    os.makedirs(os.path.dirname(args.out) or ".", exist_ok=True)
    with open(args.out, "w") as f:
        json.dump(nodes, f, ensure_ascii=False, indent=1)

    by_proto: dict[str, int] = {}
    by_src: dict[str, int] = {}
    for n in nodes:
        by_proto[n["proto"]] = by_proto.get(n["proto"], 0) + 1
        by_src[n["source"]] = by_src.get(n["source"], 0) + 1
    print(f"\n✅ 去重后 {len(nodes)} 个节点 → {args.out}")
    print("   协议:", by_proto)
    print("   来源:", dict(sorted(by_src.items(), key=lambda x: -x[1])[:8]))


if __name__ == "__main__":
    main()
