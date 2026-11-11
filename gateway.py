#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
统一出口网关 (nodepool gateway)

职责
----
1. 历史池并集   每轮测速结果 merge 进 data/pool_history.json。近 N 天可用过、
                但本轮没测到的节点依然保留 —— 修掉"一轮全挂就把池子清空"。
2. 分层供给     ① 自建(私有)  ② 正规免费额度  ③ 免费池
3. 轻量健康探测 对池内节点做 TCP 粗筛, 剔除明显死掉的
4. 固定订阅地址 客户端只填一个地址; 用 fallback 组实现
                "自建优先 → 挂了自动降级到免费额度 → 再降级到免费池"

分工 (重要)
-----------
网关只做 TCP 粗筛。真正的延迟/可用性判断交给客户端的 url-test 组 ——
"TCP 连得上" 不等于 "能代理", 而客户端本来就会持续探测, 没必要重复做。
这样网关很轻 (纯标准库, 无常驻子进程), 也不和客户端抢网络。

用法
----
    python3 gateway.py                    # 前台, 监听 127.0.0.1:8899
    python3 gateway.py --host 0.0.0.0     # 允许局域网 (手机用), 强制要求 token
    python3 gateway.py --port 8899

订阅地址 (启动时会打印)
    http://127.0.0.1:8899/sub/clash?token=xxx   Clash / mihomo / Stash
    http://127.0.0.1:8899/sub/base64?token=xxx  v2rayN / Shadowrocket 等
    http://127.0.0.1:8899/                      状态页
"""
from __future__ import annotations

import argparse
import base64
import json
import os
import re
import secrets
import socket
import sys
import threading
import time
import urllib.parse
from concurrent.futures import ThreadPoolExecutor
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

ROOT = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, ROOT)

from probe import emit, to_mihomo  # noqa: E402  复用已验证的转换与序列化

DATA = os.path.join(ROOT, "data")
OUT = os.path.join(ROOT, "out")
HISTORY = os.path.join(DATA, "pool_history.json")
BEST = os.path.join(OUT, "nodes_best.json")
PRIVATE = os.path.join(DATA, "private.yaml")      # 自建节点, 原样 mihomo proxies 片段
FALLBACK = os.path.join(DATA, "fallback.yaml")    # 正规免费额度/WARP, 同上
STATE = os.path.join(DATA, "gateway.json")
CN_PREF = os.path.join(DATA, "cn_pref.json")      # 中国网络实测结果

TEST_URL = "https://www.gstatic.com/generate_204"
# 自动选择组的健康检查间隔(秒)。云端一天只测一次, 客户端才是真正的实时测速,
# 所以这个值宁小勿大 —— 120 秒重探一轮, 挂了 2 分钟内就能换走。
AUTO_INTERVAL = 120
KEEP_DAYS = 7          # 历史池保留天数
MAX_POOL = 300         # 历史池上限, 防文件无限膨胀
MIN_KEEP = 5           # 剔除"中国实测不通"后至少要剩这么多个, 否则全留
MIN_SPEED = 0.05       # 低于这个 MB/s 视为传不动数据
PROBE_WORKERS = 64
SWEEP_INTERVAL = 600   # 后台保活扫描间隔(秒)

_lock = threading.RLock()
_state = {
    "config": None,        # 缓存的订阅文本
    "config_mtime": 0.0,   # 生成时的 BEST mtime
    "built_at": 0.0,
    "alive": {},           # fp -> bool (最近一次 TCP 探测结果)
    "stats": {},
    "sweeping": False,
}


def log(msg: str) -> None:
    print(f"[{time.strftime('%H:%M:%S')}] {msg}", flush=True)


# ---------------------------------------------------------------- 文件工具
def load_json(path: str, default):
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return default


def save_json(path: str, obj) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False)
    os.replace(tmp, path)


# ---------------------------------------------------------------- 历史池
def fp_key(n: dict) -> str:
    """节点在历史池里的唯一键。

    所有写池/查池的地方必须用这一个函数。曾经 ci_build 用 "server:port",
    merge_history 用 "server:port:proto" —— 没有 fp 字段的老节点会被存成两份,
    而且"本轮测到死的要摘掉"会因为键对不上而静默失效。
    """
    return n.get("fp") or f"{n.get('server')}:{n.get('port')}:{n.get('proto')}"


def merge_history(history: dict, best: list, keep_days: int = KEEP_DAYS) -> dict:
    """把本轮测速结果并入历史池。返回更新后的 history。

    合并语义:
      - 本轮测到的节点: 刷新 node 数据 / last_ok / 统计
      - 本轮没测到的节点: 原样保留 (靠 last_ok 判断是否过期)
    """
    now = time.time()
    for n in best:
        fp = fp_key(n)
        rec = history.get(fp)
        if not rec:
            rec = {"first_seen": now, "ok_hits": 0}
        rec["node"] = n
        rec["last_ok"] = now
        rec["latency"] = n.get("latency")
        rec["speed"] = n.get("speed")
        rec["score"] = n.get("score", 0)
        rec["ok_hits"] = rec.get("ok_hits", 0) + 1
        history[fp] = rec

    # 过期清理
    cutoff = now - keep_days * 86400
    stale = [k for k, v in history.items() if v.get("last_ok", 0) < cutoff]
    for k in stale:
        history.pop(k, None)

    # 数量上限: 按分数截断
    if len(history) > MAX_POOL:
        ranked = sorted(history.items(), key=lambda kv: kv[1].get("score", 0), reverse=True)
        history = dict(ranked[:MAX_POOL])
    return history


def prune_history(history: dict, keep_days: int = KEEP_DAYS) -> dict:
    """只做过期清理, 不累加统计。rebuild 在未遇到新测速结果时用它。"""
    cutoff = time.time() - keep_days * 86400
    return {k: v for k, v in history.items() if v.get("last_ok", 0) >= cutoff}


def load_cn_pref(keep_days: int = 7) -> dict:
    """中国网络实测过的节点: {"server:port": {"ok": 1|0, "latency": ms, "at": ts}}。

    为什么需要这个文件: 云端 runner 在美国, 它测出的延迟和"国内能不能连上"
    相关度很低 —— 实测云端判活 40 个, 到国内只剩 13 个能连。免费节点的生死
    很大程度取决于链路, 而链路只有从国内出发才测得准。

    由 live_test.py --save 写入, 只在国内的机器上跑才会更新。超过 keep_days
    自动失效, 避免拿半年前的结论指导今天的排序。
    """
    raw = load_json(CN_PREF, {})
    cutoff = time.time() - keep_days * 86400
    return {k: v for k, v in raw.items()
            if isinstance(v, dict) and v.get("at", 0) >= cutoff}


def _cn_rank(n: dict) -> int:
    """中国实测优先级。

    3 = 能传数据 (实测下载速度达标) —— 只有这一档是真的好用
    2 = 连得上但没测速度
    1 = 连得上但传不动数据 (延迟低、带宽 0, 典型陷阱节点)
    0 = 没测过
    -1 = 测过但不通
    """
    v = n.get("cn_ok")
    if v is None:
        return 0
    if not v:
        return -1
    sp = n.get("cn_speed")
    if sp is None:
        return 2
    return 3 if sp > MIN_SPEED else 1


def pool_nodes(history: dict, alive: dict | None = None,
               cn: dict | None = None) -> list:
    """取出当前可用节点, 按 (中国实测, 复检通过数, 评分) 降序。

    排序把 verify_ok 放在 score 前面: 复检 3/3 的永远排在只有 1/3 的前面。
    历史遗留节点没有 verify_ok 字段, 视为 0 排到最后 —— 它们还留着是为了
    防止新数据全灭, 但轮不到它们被优先使用。

    cn 是来自中国网络的实测证据, 权重最高: 云端复检 3/3 但国内连不上的节点,
    对使用者毫无价值, 必须排在"国内实测能通"的后面。
    """
    cn = load_cn_pref() if cn is None else cn
    out = []
    for fp, rec in history.items():
        if alive is not None and alive.get(fp) is False:
            continue
        n = dict(rec.get("node") or {})
        if not n.get("server"):
            continue
        # 明确复检不合格的不进订阅, 免得好节点挂了它顶上来
        if n.get("verify_ok") is not None and n["verify_ok"] < 2:
            continue
        n["fp"] = fp
        pref = cn.get(f"{n['server']}:{n['port']}")
        n["cn_ok"] = None if pref is None else int(bool(pref.get("ok")))
        if pref and pref.get("by"):
            n["cn_by"] = pref["by"]
        if pref and pref.get("latency"):
            n["cn_latency"] = pref["latency"]
        if pref and pref.get("speed") is not None:
            n["cn_speed"] = pref["speed"]
        out.append(n)

    # 中国实测明确不通的, 强制踢出订阅 —— 不设下限。
    # 之前用 MIN_KEEP 兜底过, 结果适得其反: 20 个节点里 18 个连不上时,
    # 兜底会把那 18 个又塞回订阅, 用户看到的就是"大半连不上"。
    # 唯一例外: 通不通的比例太悬殊(>80%)时更像测量环境出了问题
    # (比如换了个网络测), 这时不信这批数据, 保留原样等 7 天过期自愈。
    dead = [n for n in out if n.get("cn_ok") == 0]
    auto_dead = [n for n in dead if n.get("cn_by") != "user_report"]
    if out and auto_dead and len(auto_dead) > 0.8 * len(out):
        # 自动测出来大面积不通, 更像测量环境有问题(比如换了网络测)。
        # 但用户亲口说"这个不行"的要照办 —— 那是人的判断, 比脚本可信。
        out = [n for n in out
               if not (n.get("cn_ok") == 0 and n.get("cn_by") == "user_report")]
    else:
        out = [n for n in out if n.get("cn_ok") != 0]
    # "连得上但传不动数据"只是当下拥塞, 允许兜底: 踢完不够 MIN_KEEP 就留着候补。
    keep = [n for n in out if _cn_rank(n) != 1]
    if len(keep) >= MIN_KEEP:
        out = keep

    # 排序: 能传数据的优先, 同等条件按实测下载速度降序 —— 那才是实际体验。
    out.sort(key=lambda n: (-_cn_rank(n),
                            -(n.get("cn_speed") or 0),
                            n.get("cn_latency") or 99999,
                            -(n.get("verify_ok") or 0),
                            -(n.get("score") or 0)))
    return out


# ---------------------------------------------------------------- 健康探测
def tcp_probe(server: str, port: int, timeout: float = 4.0) -> bool:
    try:
        with socket.create_connection((server, int(port)), timeout=timeout):
            return True
    except Exception:
        return False


def sweep(nodes: list, timeout: float = 4.0) -> dict:
    """并发 TCP 粗筛, 返回 {fp: bool}。"""
    result: dict = {}

    def one(n):
        return n["fp"], tcp_probe(n["server"], n["port"], timeout)

    with ThreadPoolExecutor(max_workers=PROBE_WORKERS) as ex:
        for fp, ok in ex.map(one, nodes):
            result[fp] = ok
    return result


# ---------------------------------------------------------------- 私有层
def parse_private(path: str) -> list:
    """读取用户手写的 mihomo proxies 片段。

    支持两种写法:
      proxies:
        - {name: my-vps, type: vless, ...}
      - {name: my-vps, type: vless, ...}
    """
    if not os.path.exists(path):
        return []
    try:
        with open(path, "r", encoding="utf-8") as f:
            text = f.read()
    except Exception:
        return []
    try:
        import yaml  # 有 pyyaml 就用, 更稳
        doc = yaml.safe_load(text)
        if isinstance(doc, dict):
            items = doc.get("proxies") or []
        elif isinstance(doc, list):
            items = doc
        else:
            items = []
        return [p for p in items if isinstance(p, dict) and p.get("type")]
    except Exception:
        pass
    # 没有 pyyaml: 极简解析 "- {...}" 行内 JSON 风格
    items = []
    for line in text.splitlines():
        s = line.strip()
        if not s.startswith("- "):
            continue
        body = s[2:].strip()
        if not body.startswith("{"):
            continue
        try:
            items.append(json.loads(body))
        except Exception:
            continue
    return [p for p in items if isinstance(p, dict) and p.get("type")]


# ---------------------------------------------------------------- 订阅生成
def to_uri(n: dict) -> str | None:
    """节点结构 → 分享链接。base64 订阅用。"""
    proto = n.get("proto")
    srv, port = n.get("server"), n.get("port")
    name = urllib.parse.quote(n.get("name") or f"{srv}:{port}", safe="")
    if not srv or not port:
        return None

    if proto == "ss":
        if not n.get("cipher") or not n.get("password"):
            return None
        userinfo = base64.urlsafe_b64encode(
            f"{n['cipher']}:{n['password']}".encode()
        ).decode().rstrip("=")
        return f"ss://{userinfo}@{srv}:{port}#{name}"

    if proto == "vmess":
        cfg = {
            "v": "2", "ps": n.get("name") or srv, "add": srv, "port": str(port),
            "id": n.get("uuid", ""), "aid": str(n.get("alterId") or 0),
            "scy": n.get("cipher") or "auto", "net": n.get("network") or "tcp",
            "type": "none", "host": n.get("host") or "", "path": n.get("path") or "",
            "tls": "tls" if n.get("tls") else "", "sni": n.get("sni") or "",
        }
        raw = json.dumps(cfg, ensure_ascii=False, separators=(",", ":"))
        return "vmess://" + base64.b64encode(raw.encode()).decode()

    if proto in ("vless", "trojan"):
        q = []
        if proto == "vless":
            q.append("encryption=none")
            sec = "reality" if n.get("reality") else ("tls" if n.get("tls") else "none")
            q.append(f"security={sec}")
        else:
            q.append("security=tls")
        if n.get("sni"):
            q.append("sni=" + urllib.parse.quote(str(n["sni"]), safe=""))
        if n.get("reality") and n.get("pbk"):
            q.append("pbk=" + urllib.parse.quote(str(n["pbk"]), safe=""))
            if n.get("sid"):
                q.append("sid=" + str(n["sid"]))
            q.append("fp=" + (n.get("utls") or "chrome"))
        elif n.get("utls"):
            q.append("fp=" + str(n["utls"]))
        if n.get("network") and n["network"] != "tcp":
            q.append("type=" + str(n["network"]))
        if n.get("path"):
            q.append("path=" + urllib.parse.quote(str(n["path"]), safe=""))
        if n.get("host"):
            q.append("host=" + urllib.parse.quote(str(n["host"]), safe=""))
        if n.get("flow"):
            q.append("flow=" + str(n["flow"]))
        if n.get("skipCertVerify"):
            q.append("allowInsecure=1")
        user = n.get("uuid") if proto == "vless" else n.get("password")
        if not user:
            return None
        return f"{proto}://{user}@{srv}:{port}?{'&'.join(q)}#{name}"

    if proto == "hysteria2":
        if not n.get("password"):
            return None
        q = []
        if n.get("sni"):
            q.append("sni=" + urllib.parse.quote(str(n["sni"]), safe=""))
        if n.get("skipCertVerify"):
            q.append("insecure=1")
        tail = f"?{'&'.join(q)}" if q else "?"
        return f"hysteria2://{n['password']}@{srv}:{port}{tail}#{name}"

    if proto == "tuic":
        if not (n.get("uuid") and n.get("password")):
            return None
        q = []
        if n.get("sni"):
            q.append("sni=" + urllib.parse.quote(str(n["sni"]), safe=""))
        if n.get("alpn"):
            q.append("alpn=" + str(n["alpn"]))
        if n.get("skipCertVerify"):
            q.append("allow_insecure=1")
        tail = f"?{'&'.join(q)}" if q else ""
        return f"tuic://{n['uuid']}:{n['password']}@{srv}:{port}{tail}#{name}"
    return None


def tier_label(n: dict, idx: int) -> str:
    spd = n.get("speed") or 0
    return f"T2-{idx:02d} · {spd:.1f}MB/s"


def build_config(pool: list, private: list, fallback: list,
                 namer=None, runtime: bool = True) -> str:
    """生成 mihomo 订阅。分层 + fallback 实现自动降级。

    runtime=True  → 本地网关用: 带 mixed-port / allow-lan 等运行参数
    runtime=False → 云端产物用: 只出 proxies / proxy-groups / rules,
                    避免客户端的端口、DNS 被你覆盖
    """
    namer = namer or tier_label
    proxies: list = []
    groups: list = []
    tier_names: list = []

    tiers = [
        ("① 自建", private, 120),
        ("② 免费额度", fallback, 300),
        ("③ 免费池", None, 600),
    ]

    for label, items, interval in tiers:
        if items is None:  # 免费池: 从节点结构转换
            converted = []
            for i, n in enumerate(pool, 1):
                p = to_mihomo(n, namer(n, i))
                if p:
                    converted.append(p)
            items = converted
        else:
            # 私有的原样保留, 只保证名字不冲突
            seen = {p["name"] for p in proxies}
            normalized = []
            for i, p in enumerate(items, 1):
                p = dict(p)
                if p.get("name") in seen:
                    p["name"] = f"{label}-{i:02d}"
                seen.add(p["name"])
                normalized.append(p)
            items = normalized

        if not items:
            continue
        names = [p["name"] for p in items]
        proxies.extend(items)
        # 一律用 fallback 而不是 url-test:
        #   url-test 选"延迟最低", 实测会挑中 134ms 但打不开 github 的节点;
        #   fallback 从列表第一个开始, 第一个健康就用它 —— 而列表顺序由
        #   probe.py 的真实下载速度评分决定, 所以等于"优先用实测最好的, 挂了才换下一个"。
        groups.append({
            "name": label, "type": "fallback", "url": TEST_URL,
            "interval": interval, "proxies": names,
        })
        tier_names.append(label)

    if not proxies:
        # 一个节点都没有: 至少给个能上网的 DIRECT, 避免客户端导入即报错
        cfg = {
            "proxy-groups": [{"name": "节点选择", "type": "select",
                              "proxies": ["DIRECT"]}],
            "rules": ["MATCH,节点选择"],
        }
        return "# 池内暂无可用节点\n" + emit(cfg) + "\n"

    # 层级顺序展平成一维: 自建 → 额度 → 免费池。fallback 从列表第一个开始试,
    # 所以"列表顺序"本身就是优先级, 不需要再套一层组去表达降级。
    flat = [p["name"] for p in proxies]
    groups.insert(0, {
        "name": "自动选择", "type": "fallback", "url": TEST_URL,
        "interval": AUTO_INTERVAL, "proxies": flat,
    })
    # 顶层必须是 select。Clash Verge 的"当前节点"卡片只认 select 组里选中的东西,
    # 出口本身若是 fallback(尤其是指向另一个组的 fallback), 它解析不出具体节点,
    # 就会显示"暂无激活的代理节点" —— 机场订阅有"自动切换"效果就是靠这层 select。
    groups.insert(0, {
        "name": "节点选择", "type": "select",
        "proxies": ["自动选择"] + tier_names + flat,
    })
    if runtime:
        cfg = {
            "mixed-port": 7890,
            "allow-lan": True,
            "mode": "rule",
            "log-level": "warning",
            "unified-delay": True,
            "tcp-concurrent": True,
            "proxies": proxies,
            "proxy-groups": groups,
            "rules": ["MATCH,节点选择"],
        }
    else:
        cfg = {
            "proxies": proxies,
            "proxy-groups": groups,
            "rules": ["MATCH,节点选择"],
        }
    # 云端 runner 是 UTC，直接打本地时间会让人看糊涂（显示 20:05 而北京是次日 04:05）。
    # 统一按北京时间标注。
    bj = time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime(time.time() + 8 * 3600))
    header = (
        f"# nodepool  生成于 {bj} (北京时间)\n"
        f"# 节点数 {len(proxies)}  "
        f"(自建 {len(private)} / 免费额度 {len(fallback)} / 免费池 {len(pool)})\n"
        f"# 节点选择 = select, 下设\"自动选择\"(fallback, 每 {AUTO_INTERVAL}s 复检)\n"
        f"# 自动选择按 ① 自建 → ② 免费额度 → ③ 免费池 的顺序降级\n"
    )
    return header + emit(cfg) + "\n"


def build_base64(pool: list, private: list, fallback: list, namer=None) -> str:
    """生成通用分享链接列表 (base64)。

    namer 必须和 build_config 用同一个 —— 否则同一次构建里, Clash 订阅显示
    "N01 202.50.253.72", 而 base64 订阅显示源站原始名 "n0091"。手机端多半走
    base64, 用户看到的名字就跟说明对不上, 报"哪个不通"时谁也不知道是哪个。
    """
    uris = []
    for p in private + fallback:  # 私有层通常是 mihomo 格式, 尽力转
        u = _proxy_to_uri(p)
        if u:
            uris.append(u)
    for i, n in enumerate(pool, 1):
        item = n
        if namer is not None:
            item = dict(n)
            item["name"] = namer(n, i)
        u = to_uri(item)
        if u:
            uris.append(u)
    return base64.b64encode("\n".join(uris).encode()).decode()


def _proxy_to_uri(p: dict) -> str | None:
    """把 mihomo proxy dict 粗略转成分享链接 (仅覆盖常见类型)。"""
    t = p.get("type")
    n = {
        "proto": t, "server": p.get("server"), "port": p.get("port"),
        "name": p.get("name"), "cipher": p.get("cipher"),
        "password": p.get("password"), "uuid": p.get("uuid"),
        "sni": p.get("servername") or p.get("sni"),
        "network": p.get("network"), "host": p.get("servername"),
        "path": (p.get("ws-opts") or {}).get("path"),
        "reality": bool(p.get("reality-opts")),
        "pbk": (p.get("reality-opts") or {}).get("public-key"),
        "sid": (p.get("reality-opts") or {}).get("short-id"),
        "fp": p.get("client-fingerprint"),
        "tls": p.get("tls"), "flow": p.get("flow"),
        "skipCertVerify": p.get("skip-cert-verify"),
        "alpn": p.get("alpn"),
    }
    if n["reality"] and n["pbk"]:
        n["tls"] = True
    return to_uri(n) if t in ("ss", "vmess", "vless", "trojan", "hysteria2", "tuic") else None


# ---------------------------------------------------------------- 构建 & 缓存
def rebuild(force: bool = False) -> dict:
    """重建订阅缓存。返回统计信息。"""
    with _lock:
        best_mtime = os.path.getmtime(BEST) if os.path.exists(BEST) else 0.0
        if not force and _state["config"] and best_mtime <= _state["config_mtime"]:
            return _state["stats"]

        history = load_json(HISTORY, {})
        best = load_json(BEST, [])
        # 只有测速结果真的换了才并池。否则后台保活重建会把 ok_hits 反复 +1,
        # 让"通过次数"变成"重建次数", 统计彻底失真。
        if best and best_mtime != _state.get("merged_mtime"):
            history = merge_history(history, best)
            save_json(HISTORY, history)
            _state["merged_mtime"] = best_mtime
        else:
            pruned = prune_history(history)
            if len(pruned) != len(history):
                history = pruned
                save_json(HISTORY, history)

        nodes = pool_nodes(history, _state["alive"])
        private = parse_private(PRIVATE)
        fallback = parse_private(FALLBACK)

        cfg = build_config(nodes, private, fallback)
        b64 = build_base64(nodes, private, fallback)

        _state["config"] = cfg
        _state["base64"] = b64
        _state["config_mtime"] = best_mtime
        _state["built_at"] = time.time()
        _state["stats"] = {
            "history_total": len(history),
            "alive_filtered": len(nodes),
            "private": len(private),
            "fallback": len(fallback),
            "built_at": time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()),
            "best_mtime": best_mtime,
        }
        log(f"订阅已重建: 历史 {len(history)} 存活 {len(nodes)} "
            f"自建 {len(private)} 额度 {len(fallback)}")
        return _state["stats"]


# ---------------------------------------------------------------- HTTP
class Handler(BaseHTTPRequestHandler):
    server_version = "nodepool/1.0"
    token = ""

    def log_message(self, fmt, *args):  # 静音默认访问日志
        pass

    # --- 鉴权 ---
    def _authed(self) -> bool:
        if not self.token:
            return True
        q = urllib.parse.parse_qs(urllib.parse.urlparse(self.path).query)
        if q.get("token", [""])[0] == self.token:
            return True
        auth = self.headers.get("Authorization", "")
        return auth == f"Bearer {self.token}"

    def _send(self, code: int, body: bytes, ctype: str = "text/plain; charset=utf-8",
              extra: dict | None = None):
        self.send_response(code)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        for k, v in (extra or {}).items():
            self.send_header(k, v)
        self.end_headers()
        try:
            self.wfile.write(body)
        except Exception:
            pass

    def do_GET(self):
        path = urllib.parse.urlparse(self.path).path.rstrip("/") or "/"

        if path in ("/", "/status"):
            if path == "/status" and not self._authed():
                return self._send(403, b'{"error":"token required"}', "application/json")
            stats = rebuild()
            if path == "/status":
                body = json.dumps({
                    **stats,
                    "sweeping": _state["sweeping"],
                    "uptime_s": int(time.time() - START),
                }, ensure_ascii=False, indent=1).encode()
                return self._send(200, body, "application/json; charset=utf-8")
            return self._send(200, self._html(stats).encode(), "text/html; charset=utf-8")

        if not self._authed():
            return self._send(403, b"403 token required\n")

        if path in ("/sub/clash", "/sub", "/clash"):
            rebuild()
            body = (_state.get("config") or "").encode()
            return self._send(200, body, "text/yaml; charset=utf-8", {
                "Content-Disposition": 'attachment; filename="nodepool.yaml"',
                "Profile-Update-Interval": "6",
            })

        if path in ("/sub/base64", "/sub/v2ray"):
            rebuild()
            body = (_state.get("base64") or "").encode()
            return self._send(200, body, "text/plain; charset=utf-8", {
                "Profile-Update-Interval": "6",
            })

        if path == "/reload":
            rebuild(force=True)
            return self._send(200, b"ok\n")

        return self._send(404, b"404\n")

    def _html(self, stats: dict) -> str:
        t = self.token
        rows = "".join(
            f"<tr><td>{k}</td><td>{v}</td></tr>" for k, v in stats.items()
        )
        return f"""<!doctype html><meta charset="utf-8">
<title>nodepool gateway</title>
<style>
 body{{font:13px/1.6 -apple-system,sans-serif;margin:32px;max-width:720px;color:#222}}
 h1{{font-size:15px;font-weight:500}}
 code{{background:#f4f4f2;padding:2px 6px;border-radius:4px;font-size:12px;word-break:break-all}}
 table{{border-collapse:collapse;margin:12px 0}}
 td{{padding:3px 16px 3px 0;border-bottom:1px solid #eee}}
 .k{{color:#666}}
</style>
<h1>nodepool 统一出口</h1>
<p class="k">把下面任一地址填进客户端, 之后不用再管节点。</p>
<p>Clash / mihomo:<br><code>http://127.0.0.1:8899/sub/clash?token={t}</code></p>
<p>v2rayN / Shadowrocket:<br><code>http://127.0.0.1:8899/sub/base64?token={t}</code></p>
<table>{rows}</table>
<p class="k">"节点选择"下设"自动选择"组, 按 ① 自建 → ② 免费额度 → ③ 免费池 自动降级。</p>
"""


# ---------------------------------------------------------------- 后台线程
def watcher():
    """盯 nodes_best.json 变化, 自动重载。"""
    last = 0.0
    while True:
        try:
            m = os.path.getmtime(BEST) if os.path.exists(BEST) else 0.0
            if m != last:
                last = m
                rebuild(force=True)
        except Exception as e:
            log(f"watcher: {type(e).__name__}: {e}")
        time.sleep(30)


def keeper():
    """定期保活扫描: 只扫池子前 200 个, 更新 alive 标记。"""
    while True:
        time.sleep(SWEEP_INTERVAL)
        try:
            with _lock:
                history = load_json(HISTORY, {})
                nodes = pool_nodes(history)[:200]
            if not nodes:
                continue
            _state["sweeping"] = True
            t0 = time.time()
            res = sweep(nodes)
            with _lock:
                _state["alive"].update(res)
            dead = sum(1 for v in res.values() if not v)
            log(f"保活扫描 {len(nodes)} 个, 死 {dead} 个, 耗时 {time.time()-t0:.1f}s")
            rebuild(force=True)
        except Exception as e:
            log(f"keeper: {type(e).__name__}: {e}")
        finally:
            _state["sweeping"] = False


START = time.time()


def load_token(host: str, port: int) -> str:
    st = load_json(STATE, {})
    token = st.get("token")
    if not token:
        token = secrets.token_urlsafe(12)
        save_json(STATE, {**st, "token": token})
    return token


def main():
    ap = argparse.ArgumentParser(description="nodepool 统一出口网关")
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--port", type=int, default=8899)
    ap.add_argument("--no-token", action="store_true", help="关闭订阅鉴权(仅本机监听时建议)")
    ap.add_argument("--no-keepalive", action="store_true", help="关闭后台保活扫描")
    args = ap.parse_args()

    token = "" if args.no_token else load_token(args.host, args.port)
    Handler.token = token

    os.makedirs(DATA, exist_ok=True)
    rebuild()

    threading.Thread(target=watcher, daemon=True).start()
    if not args.no_keepalive:
        threading.Thread(target=keeper, daemon=True).start()

    srv = ThreadingHTTPServer((args.host, args.port), Handler)
    base = f"http://{'127.0.0.1' if args.host == '0.0.0.0' else args.host}:{args.port}"
    log(f"监听 {args.host}:{args.port}  (Ctrl+C 退出)")
    print()
    print(f"  Clash / mihomo : {base}/sub/clash" + (f"?token={token}" if token else ""))
    print(f"  v2rayN / SR    : {base}/sub/base64" + (f"?token={token}" if token else ""))
    print(f"  状态页         : {base}/" + (f"?token={token}" if token else ""))
    print()
    try:
        srv.serve_forever()
    except KeyboardInterrupt:
        log("退出")
    finally:
        srv.server_close()


if __name__ == "__main__":
    main()
