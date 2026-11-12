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
# url-test 的切换阈值(毫秒)。延迟差不到这个数就不换节点, 防止两个节点
# 数值来回抖时客户端跟着反复切, 每次切换都要重建连接。
AUTO_TOLERANCE = 100
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

    # 数量上限: 截断要保住"真的能用"的节点。
    #
    # 原来只按 score 排, 而 score 来自云端复检 —— 于是"中国实测 2214KB/s、但云端连不上"
    # 的节点 score=0, 池子一满第一个被砍, 留下的是云端复检漂亮、国内打不开的那些。
    # 现在按 (国内实测能通?, 实测速度, score) 排, 实测证据永远优先于云端观感。
    if len(history) > MAX_POOL:
        cn = load_cn_pref()

        def weight(kv) -> tuple:
            node = kv[1].get("node") or {}
            p = cn.get(f"{node.get('server')}:{node.get('port')}") or {}
            return (int(bool(p.get("ok"))), p.get("speed") or 0.0,
                    kv[1].get("score") or 0)

        ranked = sorted(history.items(), key=weight, reverse=True)
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
        # pool_history 里的节点本体有两种历史写法: 一种带 proto, 一种只有 type
        # (apply_results 早期写的是 type)。统一成 proto, 否则 to_mihomo 认不出类型,
        # 节点会静默地从订阅里消失 —— 13 个池子只出 4 个就是这么来的。
        if not n.get("proto") and n.get("type"):
            n["proto"] = n["type"]
        # 明确复检不合格的不进订阅, 免得好节点挂了它顶上来
        if n.get("verify_ok") is not None and n["verify_ok"] < 2:
            continue
        n["fp"] = fp
        pref = cn.get(f"{n['server']}:{n['port']}")
        # ok 有三种状态, 不能压成两种:
        #   1     实测能通
        #   0     实测不通 → 踢出订阅
        #   None  只记了一笔失败(timeout 首轮)还没定论 → 当作没测过, 不许踢
        # 之前写成 int(bool(pref.get("ok"))), None 会被 bool() 变成 False(即 0),
        # 于是"首轮只记账不判死"的节点反而被立刻踢掉了 —— 和意图完全相反。
        n["cn_ok"] = (None if (pref is None or pref.get("ok") is None)
                      else int(bool(pref["ok"])))
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
    #
    # 但"实测不通"有两种来源, 期望值完全相反, 必须分开算:
    #   by=auto    订阅小样本实测。20 个死 16 个 → 大概率是本机网络换了,
    #              不是节点同时全挂, 所以用比例兜底: 不信这批数据。
    #   by=verify  全池普查。5922 个公开免费节点死掉八成是常态, 这个"异常比例"
    #              恰恰就是要剔掉的东西。
    # 混在一个阈值里的后果很具体: 通宵扫完全池, 因为存活率 18% < 20%,
    # 一个节点都没被剔, 等于白跑。所以先把普查结论摘出来, 一律照办。
    swept_dead = [n for n in out
                  if n.get("cn_ok") == 0 and n.get("cn_by") == "verify"]
    rest = [n for n in out
            if not (n.get("cn_ok") == 0 and n.get("cn_by") == "verify")]

    dead = [n for n in rest if n.get("cn_ok") == 0]
    auto_dead = [n for n in dead if n.get("cn_by") != "user_report"]
    if rest and auto_dead and len(auto_dead) > 0.8 * len(rest):
        # 自动测出来大面积不通, 更像测量环境有问题(比如换了网络测)。
        # 但用户亲口说"这个不行"的要照办 —— 那是人的判断, 比脚本可信。
        rest = [n for n in rest
                if not (n.get("cn_ok") == 0 and n.get("cn_by") == "user_report")]
    else:
        rest = [n for n in rest if n.get("cn_ok") != 0]

    # 剔完不够 MIN_KEEP 就说明判得太狠了, 把普查判死的按速度捞一半回来当候补。
    # 宁可留几个死节点, 也不能让订阅悄悄变成空的 —— 那个错误没人会发现。
    if len(rest) >= MIN_KEEP or not swept_dead:
        out = rest
    else:
        swept_dead.sort(key=lambda n: -(n.get("cn_speed") or 0))
        back = swept_dead[len(swept_dead) // 2:]
        log(f"  ! 普查判死 {len(swept_dead)} 个后只剩 {len(rest)} 个(低于 "
            f"MIN_KEEP={MIN_KEEP}), 捞回 {len(back)} 个当候补")
        out = rest + back
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

    # 同一个 server:port 只留一条。
    # 历史池是按指纹(fp)去重的, 而 fp 里含协议/凭据 —— 同一个出口换了密码或换了
    # 传输参数, 就算两条记录。表现为订阅里两个不同编号指向同一个地址, 客户端会
    # 同时对同一个端口发起两倍连接, 只会互相挤占。排序完再去重, 留下的是最优那条。
    seen, uniq = set(), []
    for n in out:
        key = f"{n.get('server')}:{n.get('port')}"
        if key in seen:
            continue
        seen.add(key)
        uniq.append(n)
    return uniq


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

    if proto in ("http", "socks5"):
        # base64 订阅只装得下 URI。普通代理用标准 userinfo 形式表达:
        #   http://user:pass@host:port#name
        #
        # 但**带 TLS 的 http 代理不能进 base64**: 它没有公认的分享格式。
        # 曾经写成 https://host:port, 结果是客户端当成网页地址 —— 小火箭里
        # 显示成一个标着 ⚠️ 的 HTTPS 条目(AUTO 分组里), 看着有, 其实连不上。
        # 这种"假装存在"比缺席更坏: 用户会以为节点坏了, 实际是订阅写错了。
        # 这类节点只走 sub.conf 的 Surge 写法(https, host, port), 那是 iOS
        # 端唯一有明确定义的表达。
        if n.get("tls"):
            return None
        cred = ""
        if n.get("username") or n.get("password"):
            cred = urllib.parse.quote(str(n.get("username") or ""), safe="")
            cred += ":" + urllib.parse.quote(str(n.get("password") or ""), safe="")
            cred += "@"
        scheme = "socks5" if proto == "socks5" else "http"
        return f"{scheme}://{cred}{srv}:{port}#{name}"
    return None


def _surge_safe(v) -> bool:
    """Surge 配置是逗号分隔的纯文本, 没有转义机制。

    值里出现逗号或换行就会把这一行拆成两个字段, 客户端要么解析报错、要么悄悄
    丢掉后面全部参数(比如 password 被截断, 表现为"能导入但连不上")。宁可不收,
    也不要写出一行看起来正常实际是错的内容。
    """
    s = str(v or "")
    return "," not in s and "\n" not in s and "\r" not in s


def _surge_line(p: dict, name: str) -> str | None:
    """mihomo 节点 → Surge/Shadowrocket 的 [Proxy] 一行。

    为什么要这份而不只是 base64: 普通 HTTP 代理没有公认的分享链接格式。
    base64 里只能写成 https://host:port, 小火箭不认这种行, 于是"显示不可用"。
    Surge 格式里 http/https 代理有明确写法, 是 iOS 端唯一可靠的表达方式。
    """
    t = p.get("type")
    srv, port = p.get("server"), p.get("port")
    if not t or not srv or not port:
        return None
    if not _surge_safe(p.get("password")) or not _surge_safe(p.get("username")):
        return None
    if not _surge_safe(srv) or not _surge_safe(name):
        return None
    head = f"{name} = {t}, {srv}, {port}"

    if t == "ss":
        if not p.get("cipher") or not p.get("password"):
            return None
        if not _surge_safe(p["cipher"]):
            return None
        return f"{head}, encrypt-method={p['cipher']}, password={p['password']}, udp-relay=false"

    if t == "vmess":
        if not p.get("uuid"):
            return None
        opts = [f"username={p['uuid']}", "vmess-aead=true"]
        if p.get("tls"):
            opts.append("tls=true")
        if p.get("servername") or p.get("sni"):
            opts.append(f"sni={p.get('servername') or p.get('sni')}")
        if p.get("network") == "ws":
            opts.append("ws=true")
            if p.get("ws-opts"):
                wo = p["ws-opts"] or {}
                if wo.get("path"):
                    opts.append(f"ws-path={wo['path']}")
                hd = (wo.get("headers") or {})
                host = hd.get("Host") or hd.get("host")
                if host:
                    opts.append(f"ws-headers=Host:{host}")
        if p.get("skip-cert-verify"):
            opts.append("skip-cert-verify=true")
        return head + ", " + ", ".join(opts)

    if t == "vless":
        if not p.get("uuid"):
            return None
        opts = [f"username={p['uuid']}"]
        if p.get("tls"):
            opts.append("tls=true")
        if p.get("servername") or p.get("sni"):
            opts.append(f"sni={p.get('servername') or p.get('sni')}")
        if p.get("flow"):
            opts.append(f"flow={p['flow']}")
        if p.get("network") == "ws":
            opts.append("ws=true")
            if p.get("ws-opts"):
                wo = p["ws-opts"] or {}
                if wo.get("path"):
                    opts.append(f"ws-path={wo['path']}")
                hd = (wo.get("headers") or {})
                host = hd.get("Host") or hd.get("host")
                if host:
                    opts.append(f"ws-headers=Host:{host}")
        # reality 参数 Surge 表达不了, 但 tls=true + sni 仍可能连通; 不通就靠测速淘汰
        if p.get("skip-cert-verify"):
            opts.append("skip-cert-verify=true")
        return head + ", " + ", ".join(opts)

    if t == "trojan":
        if not p.get("password"):
            return None
        opts = [f"password={p['password']}"]
        if p.get("sni"):
            opts.append(f"sni={p['sni']}")
        if p.get("skip-cert-verify"):
            opts.append("skip-cert-verify=true")
        return head + ", " + ", ".join(opts)

    if t == "hysteria2":
        if not p.get("password"):
            return None
        opts = [f"password={p['password']}"]
        if p.get("sni"):
            opts.append(f"sni={p['sni']}")
        if p.get("skip-cert-verify"):
            opts.append("skip-cert-verify=true")
        return head + ", " + ", ".join(opts)

    if t in ("http", "socks5"):
        # 关键点: Surge 里 https 代理靠 "https" 这个类型名表达, 不是靠 tls=true。
        #
        # 为什么无认证也要写占位账号密码: 小火箭对"http/https 类型 + 用户名密码为空"
        # 的节点会打一个黄色警告三角(列表里那个"!"), 用户看到以为节点坏了。实测
        # 这些开放代理会直接忽略 Proxy-Authorization 头(带 nodepool/nodepool 依然
        # 204, 速度 1.37MB/s), 所以填占位值既能消掉警告, 又不会影响连通。
        kind = t
        if t == "http" and p.get("tls"):
            kind = "https"
        user = p.get("username") or "nodepool"
        pw = p.get("password") or "nodepool"
        cred = f", {user}, {pw}"
        opts = ""
        if p.get("skip-cert-verify") and kind == "https":
            opts = ", skip-cert-verify=true"
        return f"{name} = {kind}, {srv}, {port}{cred}{opts}"

    return None


def build_surge(pool: list, private: list, fallback: list, namer=None) -> str:
    """生成 Surge / Shadowrocket 配置 (.conf)。

    为什么单独出这一份:
      iOS 的 Shadowrocket 吃 base64 订阅时只认节点链接。普通 HTTP 代理没有统一的
      分享链接格式, 只能退化成 https://host:port —— 小火箭会把它当成网页地址,
      结果就是"列表里有、点了不通"。Surge 格式对 http/https 代理有明确写法,
      又能带 url-test 组, 所以手机端这条路必须走 conf 而不是 base64。

    节点名在这里用 N01/N02 这种纯 ASCII 短名: Surge 的 [Proxy Group] 是逗号分隔,
    名字里带空格和点虽然多数版本能认, 但一旦解析器抠字眼就会整组失效, 不值得赌。
    """
    # 名字里带上 IP: 客户端列表里只显示名字, 如果这里只有 N22, 用户报"N22 不通"
    # 谁也说不清是哪个服务器 —— 而编号每轮重建都会变(按测速重排)。带上 IP 之后,
    # 一个名字就是唯一标识, 不需要再回查任何对照表。
    # 用 "-" 而不是空格分隔: Surge 的组是逗号分隔的裸文本, 名字里带空格多数版本
    # 能认, 但一旦解析器抠字眼整组就失效, 不值得赌。
    namer = namer or (lambda n, i: f"N{i:02d}-{n.get('server')}")

    lines_proxy: list[str] = ["DIRECT = direct"]
    legend: list[str] = []
    names: list[str] = []
    proven: list[str] = []   # 国内实测能传数据的, 只有这些进"自动选择"

    def add(mihomo_proxy: dict, nm: str, info: str, good: bool = True) -> None:
        if nm in names:      # 重名会让客户端只认后者, 前面那个凭空消失
            return
        line = _surge_line(mihomo_proxy, nm)
        if not line:
            return
        lines_proxy.append(line)
        names.append(nm)
        if good:
            proven.append(nm)
        legend.append(f"# {nm:<5} {info}")

    # 私有层通常是 mihomo 原样结构, 直接转
    for i, p in enumerate(list(private) + list(fallback), 1):
        nm = namer(p, i) if namer else f"P{i:02d}"
        add(p, nm, f"{p.get('type')} {p.get('server')}:{p.get('port')}")

    for i, n in enumerate(pool, 1):
        p = to_mihomo(n, namer(n, i))
        if not p:
            continue
        add(p, p["name"], f"{n.get('proto')} {n.get('server')}:{n.get('port')}",
            good=_cn_rank(n) >= 2)

    if not names:
        names = ["DIRECT"]

    members = ", ".join(names)
    group_lines = []
    if names != ["DIRECT"]:
        # 「自动选择」只放国内实测真能传数据的节点。实测过 94 个"没在国内测过"
        # 的候选, 能连上的是 0 个 —— 让 url-test 在彩票里挑, 挑出来的还是不通,
        # 于是"自动选择"等于随机。没实测的放在「备用节点」里, 手动想试才点。
        auto = ", ".join(proven) if proven else members
        group_lines.append(
            f"自动选择 = url-test, {auto}, "
            f"url = {TEST_URL}, interval = 300, tolerance = {AUTO_TOLERANCE}"
        )
        if proven and len(proven) < len(names):
            group_lines.append(f"备用节点 = select, {members}")
            group_lines.append("节点选择 = select, 自动选择, 备用节点, DIRECT")
        else:
            group_lines.append(f"节点选择 = select, 自动选择, {members}, DIRECT")
        final = "节点选择"
    else:
        group_lines.append("节点选择 = select, DIRECT")
        final = "节点选择"

    bj = time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime(time.time() + 8 * 3600))
    head = [
        f"# nodepool · Shadowrocket / Surge 配置",
        f"# 生成于 {bj} (北京时间)",
        f"# 节点数 {len(names)}"
        + (f" (自建 {len(private)} / 免费额度 {len(fallback)} / 免费池 {len(pool)})"
           if names != ["DIRECT"] else ""),
        "# 「自动选择」= url-test, 由手机当场测速择优, 不用手动点。",
        "# 编号对照:",
    ]
    if names != ["DIRECT"]:
        head += legend
    body = (
        head
        + ["", "[General]", "loglevel = notify", "bypass-system = true",
           "skip-proxy = 127.0.0.1, 192.168.0.0/16, 10.0.0.0/8, 172.16.0.0/12, "
           "localhost, *.local, 100.64.0.0/10",
           "dns-server = 223.5.5.5, 119.29.29.29, 1.1.1.1",
           "",
           "[Proxy]"]
        + lines_proxy
        + ["", "[Proxy Group]"]
        + group_lines
        + ["", "[Rule]", f"FINAL,{final}", ""]
    )
    return "\n".join(body)


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
    # 「自动选择」用 url-test: 客户端每 120 秒把成员全测一遍, 选延迟最低的。
    # 和原来的 fallback 相比, 差别是"谁来决定用哪个":
    #   fallback = 按列表顺序取第一个能通的 —— 顺序由云端(美国机房)的下载速度排,
    #              它决定了你优先用哪个, 你只是被动接受;
    #   url-test = 每个成员都测一遍, 谁快用谁 —— 测的是"从你当前位置过去"的延迟,
    #              判断发生在你自己的网络上, 云端排序只影响同延迟时的兜底顺序。
    #
    # 以前不敢用 url-test, 是因为它会挑中"134ms 却打不开 github"的假好节点:
    # 那种节点只对 generate_204 有响应, 真实流量传不动。现在成员在入库前都过了
    # probe.py 的真实下载测速(MIN_SPEED 以下直接淘汰), 假好节点根本进不到这里,
    # 所以那个风险已经在更上游被消掉了。
    #
    # 想回到"按顺序降级"的老行为, 在节点选择里改选「③ 免费池」即可 —— 它还是
    # fallback, 成员完全一样。
    groups.insert(0, {
        "name": "自动选择", "type": "url-test", "url": TEST_URL,
        "interval": AUTO_INTERVAL, "tolerance": AUTO_TOLERANCE, "proxies": flat,
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
        f"# 节点选择 = select, 下设\"自动选择\"(url-test, 每 {AUTO_INTERVAL}s 全测一遍, 取延迟最低)\n"
        f"# 想改成按顺序降级, 在节点选择里切到「③ 免费池」(fallback, 成员相同)\n"
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
    # 编号只在"真的进了列表"时才前进。
    # 带 TLS 的 http 代理没有公认的分享链接格式(to_uri 返回 None), 如果照旧
    # 按 pool 下标编号, 手机端就会看到 N01..N10 之后直接跳到 N16 —— 中间十
    # 一个空号, 用户以为订阅缺了一截。实测 30 个池子出 19 条, 空号正好是
    # 那 11 个带 TLS 的 http 代理。
    # 代价: yaml/conf 是完整订阅, 编号仍是池内排名(1..30), 这里按 base64
    # 自己的输出位置重编(1..19), 两边编号会不同。所以名字里都带 server ——
    # 报"某个节点不通"时按 IP 对号, 不依赖编号。
    seq = 0
    for n in pool:
        # 能否转出 URI 与名字无关, 先用占位名判一次, 免得白占一个编号
        if not to_uri(dict(n, name="-")):
            continue
        seq += 1
        item = dict(n) if namer is None else dict(n, name=namer(n, seq))
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
        "username": p.get("username"), "tls": p.get("tls"),
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
    # http / socks5 是后加的: 从"机场试用订阅"里挖出来的类型, 现网其它节点都没有。
    return to_uri(n) if t in ("ss", "vmess", "vless", "trojan", "hysteria2",
                              "tuic", "http", "socks5") else None


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
