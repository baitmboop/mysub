#!/usr/bin/env python3
"""自动化流程的边界自检。

不需要网络, 不写真实 data/ 目录 —— 只调纯函数和临时文件, 验证异常输入
不会让每天的定时任务崩掉。跑法: python3 selfcheck.py
"""
from __future__ import annotations

import json
import os
import subprocess
import sys
import tempfile
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import gateway  # noqa: E402
import probe    # noqa: E402
import verify   # noqa: E402

FAIL = []
OK = []


def check(name: str, fn):
    try:
        fn()
        OK.append(name)
        print(f"  ✓ {name}")
    except Exception as e:
        FAIL.append((name, e))
        print(f"  ✗ {name}  →  {type(e).__name__}: {e}")


def node(server="1.2.3.4", port=443, proto="vless", **kw):
    n = {"server": server, "port": port, "proto": proto,
         "uuid": "11111111-2222-3333-4444-555555555555"}
    n.update(kw)
    return n


# ---------------------------------------------------------------- 数据读取
def t_load_json_missing():
    assert gateway.load_json("/nonexistent/x.json", {}) == {}


def t_load_json_corrupt():
    with tempfile.NamedTemporaryFile("w", suffix=".json", delete=False) as f:
        f.write("{ this is not json")
        p = f.name
    try:
        assert gateway.load_json(p, {"d": 1}) == {"d": 1}
    finally:
        os.unlink(p)


def t_load_json_empty_file():
    with tempfile.NamedTemporaryFile("w", suffix=".json", delete=False) as f:
        p = f.name
    try:
        assert gateway.load_json(p, []) == []
    finally:
        os.unlink(p)


# ---------------------------------------------------------------- 历史池
def t_merge_empty():
    assert gateway.merge_history({}, []) == {}


def t_merge_skips_node_without_server():
    h = gateway.merge_history({}, [{"proto": "ss", "port": 1}])
    assert len(h) == 1, "无 server 的节点也应该进池(由 pool_nodes 过滤)"


def t_merge_same_node_twice_counts_hits():
    n = node(fp="aaa")
    h = gateway.merge_history({}, [n])
    h = gateway.merge_history(h, [n])
    assert h["aaa"]["ok_hits"] == 2, h["aaa"]["ok_hits"]


def t_merge_expires_old():
    old = time.time() - 8 * 86400
    h = {"x": {"node": node(), "last_ok": old, "ok_hits": 1}}
    h = gateway.merge_history(h, [], keep_days=7)
    assert "x" not in h


def t_merge_caps_pool():
    big = {f"k{i}": {"node": node(), "last_ok": time.time(),
                     "ok_hits": 1, "score": i / 1000} for i in range(400)}
    h = gateway.merge_history(big, [])
    assert len(h) <= gateway.MAX_POOL, len(h)


# ---------------------------------------------------------------- 订阅池
def t_pool_empty():
    assert gateway.pool_nodes({}) == []


def t_pool_skips_bad_record():
    h = {"a": {"node": None}, "b": {"node": {"proto": "ss"}}}
    assert gateway.pool_nodes(h, cn={}) == []


def t_pool_alive_true_keeps():
    h = {"a": {"node": node(fp="a"), "last_ok": time.time(), "ok_hits": 1}}
    out = gateway.pool_nodes(h, alive={"a": True}, cn={})
    assert len(out) == 1


def t_pool_alive_false_drops():
    h = {"a": {"node": node(fp="a"), "last_ok": time.time(), "ok_hits": 1}}
    out = gateway.pool_nodes(h, alive={"a": False}, cn={})
    assert out == [], "本轮测到死的必须摘掉"


def t_pool_alive_missing_keeps():
    """本轮没测到的(不在 alive 里)要保留, 不能因为没测就删。"""
    h = {"a": {"node": node(fp="a"), "last_ok": time.time(), "ok_hits": 1}}
    out = gateway.pool_nodes(h, alive={"b": False}, cn={})
    assert len(out) == 1


def t_pool_all_dead_keeps_nothing():
    """全灭时返回空 —— 上层据此判断是否测量异常, 这里只保证不抛异常。"""
    h = {str(i): {"node": node(server=f"10.0.0.{i}", fp=str(i)),
                  "last_ok": time.time(), "ok_hits": 1} for i in range(6)}
    out = gateway.pool_nodes(h, alive={k: False for k in h}, cn={})
    assert out == []


def t_pool_verify_below_two_dropped():
    h = {"a": {"node": node(fp="a", verify_ok=1), "last_ok": time.time()}}
    assert gateway.pool_nodes(h, cn={}) == []


def t_pool_cn_pref_filters():
    """中国实测不通的要被踢掉。"""
    many = {f"n{i}": {"node": node(server=f"10.1.0.{i}", fp=f"n{i}",
                                   verify_ok=3), "last_ok": time.time()}
            for i in range(20)}
    cn = {f"10.1.0.{i}:443": {"ok": 0, "at": time.time()} for i in range(10)}
    out = gateway.pool_nodes(many, cn=cn)
    assert len(out) == 10, f"应剩 10 个, 实际 {len(out)}"


def t_pool_cn_pref_mass_dead_not_trusted():
    """死掉比例 >80% 更像测量环境出了问题, 这时不该信这批数据把池子清空。"""
    many = {f"n{i}": {"node": node(server=f"10.3.0.{i}", fp=f"n{i}",
                                   verify_ok=3), "last_ok": time.time()}
            for i in range(20)}
    cn = {f"10.3.0.{i}:443": {"ok": 0, "at": time.time()} for i in range(18)}
    out = gateway.pool_nodes(many, cn=cn)
    assert len(out) == 20, f"不该据此清空, 实际剩 {len(out)}"


def t_cull_never_kills_alive_endpoint_sibling():
    """同一端口上还有别的协议活着时, 不许按死掉的那个写 ok=0。"""
    a, rec = verify.cull_decide({}, "refused",
                                endpoint="1.2.3.4:443",
                                alive_eps={"1.2.3.4:443"}, ts=1)
    assert a == "skip" and rec is None, (a, rec)


def t_cull_hard_dead_one_strike():
    """refused 是铁证, 一次就封, 不等第二轮。"""
    a, rec = verify.cull_decide({}, "refused", endpoint="1.2.3.4:443",
                                alive_eps=set(), ts=1)
    assert a == "kill" and rec["ok"] == 0, (a, rec)


def t_cull_timeout_needs_two_rounds():
    """timeout 可能只是本机抖动, 第一轮只记账。"""
    a1, r1 = verify.cull_decide({}, "timeout", endpoint="1.2.3.4:443",
                                alive_eps=set(), ts=1)
    assert a1 == "note", a1
    assert r1["ok"] is None and r1["strikes"] == 1, r1
    a2, r2 = verify.cull_decide(r1, "timeout", endpoint="1.2.3.4:443",
                                alive_eps=set(), ts=2)
    assert a2 == "kill" and r2["ok"] == 0 and r2["strikes"] == 2, (a2, r2)


def t_cull_timeout_then_refused_escalates():
    """第一轮 timeout, 第二轮 refused —— 拿到铁证就立刻封, 不必等满两轮 timeout。"""
    _, r1 = verify.cull_decide({}, "timeout", endpoint="1.2.3.4:443",
                               alive_eps=set(), ts=1)
    a, r = verify.cull_decide(r1, "refused", endpoint="1.2.3.4:443",
                              alive_eps=set(), ts=2)
    assert a == "kill", a


def t_cull_user_report_not_overwritten():
    """人报过不行的节点, 脚本不许改。"""
    old = {"ok": 0, "by": "user_report", "at": 1}
    a, rec = verify.cull_decide(old, "refused", endpoint="1.2.3.4:443",
                                alive_eps=set(), ts=2)
    assert a == "skip" and rec is None, (a, rec)


def t_cull_keeps_old_fields_when_noting():
    """只记账不封的时候, 原有的 ok/speed 不能被抹掉。"""
    old = {"ok": 1, "speed": 1.5, "latency": 120, "by": "verify", "at": 1}
    a, rec = verify.cull_decide(old, "timeout", endpoint="1.2.3.4:443",
                                alive_eps=set(), ts=2)
    assert a == "note" and rec["ok"] == 1 and rec["speed"] == 1.5, (a, rec)


def t_select_skips_tested_and_dedups_endpoint():
    """已经测过的端点不再测; 同一端点上的第二个协议也不重复测。

    不这么做的话, 一轮普查要重扫 5177 个, 其中 5096 个的结论还在 7 天效期
    内 —— 真正没测过的只有新采到的那批, 而重扫要多花几小时。
    """
    nodes = [
        node(server="1.1.1.1", port=443, proto="http"),
        node(server="1.1.1.1", port=443, proto="vless", uuid="u"),
        node(server="2.2.2.2", port=8443, proto="http"),
    ]
    out, skip_old, skip_ss = verify.select_targets(nodes, {"2.2.2.2:8443"}, 1.0)
    assert len(out) == 1, [n["server"] for n in out]
    assert out[0]["server"] == "1.1.1.1", out[0]
    assert skip_old == 1 and skip_ss == 0, (skip_old, skip_ss)


def t_select_dup_endpoint_keeps_http():
    """同一端点有多个协议时, 留命中最高的那个。

    cn_pref 只按 server:port 记账, 测哪个协议都不会多出一条结论, 但 kernel
    组要串行跑 10 秒。所以顺序必须是"先按协议排, 再去重" —— 反过来就会
    留下列表里第一个碰到的 vless, 把 http 丢掉。
    """
    nodes = [
        node(server="3.3.3.3", port=443, proto="vless", uuid="u"),
        node(server="3.3.3.3", port=443, proto="http"),
    ]
    out, _, _ = verify.select_targets(nodes, set(), 1.0)
    assert len(out) == 1, len(out)
    assert out[0]["proto"] == "http", out[0]["proto"]


def t_select_ss_sample_is_stable():
    """ss 抽样必须是稳定的: 同一端点每轮结论一致, 否则测了哪些不可复现。"""
    nodes = [node(server=f"10.0.{i // 250}.{i % 250}", port=443, proto="ss",
                  cipher="aes-256-gcm", password="p") for i in range(400)]
    a, _, skip_a = verify.select_targets(nodes, set(), 0.1)
    b, _, skip_b = verify.select_targets(nodes, set(), 0.1)
    eps_a = [verify.ep_of(n) for n in a]
    eps_b = [verify.ep_of(n) for n in b]
    assert eps_a == eps_b, "两次抽样结果不同, 抽样不稳定"
    assert skip_a == skip_b == 400 - len(a)
    assert 20 <= len(a) <= 70, f"10% 抽样应落在 40 上下, 实际 {len(a)}"
    # 0 和 1 是边界, 必须精确
    assert len(verify.select_targets(nodes, set(), 0.0)[0]) == 0
    assert len(verify.select_targets(nodes, set(), 1.0)[0]) == 400


def t_select_priority_http_before_ss():
    """http/socks5 排最前, ss 排最后 —— 时间不够时先测命中率高的。"""
    nodes = [
        node(server="9.9.9.1", port=1, proto="ss", cipher="aes-256-gcm", password="p"),
        node(server="9.9.9.2", port=2, proto="vless", uuid="u"),
        node(server="9.9.9.3", port=3, proto="socks5"),
        node(server="9.9.9.4", port=4, proto="http"),
    ]
    out, _, _ = verify.select_targets(nodes, set(), 1.0)
    assert {n["proto"] for n in out[:2]} == {"http", "socks5"}, [n["proto"] for n in out]
    assert out[-1]["proto"] == "ss", out[-1]["proto"]


def t_report_labels_match_what_they_count():
    """日报的标签必须和它统计的东西对得上。

    2026-11-12 复盘: 旧报表写"延迟存活 656 / 复检合格 597", 实际前者是
    候选+免筛的合计, 后者是最终候选数(含 557 个根本没送复检的免筛节点)。
    我和第三方复审都照着标签读, 各自算错一轮 —— 标签错比数字错更难发现,
    因为它看起来总是合理的。
    """
    import ci_build
    tmp = tempfile.mkdtemp()
    old = ci_build.OUT
    try:
        ci_build.OUT = tmp
        fast = node(server="8.8.8.8", port=443, proto="vless", uuid="u")
        fast.update(cn_ok=1, cn_speed=0.5, verify_ok=3)
        back = node(server="9.9.9.9", port=443, proto="vless", uuid="u")
        back.update(cn_ok=None)
        ci_build.write_outputs([fast, back], [], [],
                               n_collected=100, n_delay=7, n_geo=93,
                               n_verified=7, n_picked=100, n_history=200)
        with open(os.path.join(tmp, "report.md"), encoding="utf-8") as f:
            txt = f.read()
        assert "订阅内可用率" in txt, "缺可用率"
        assert "50.0%" in txt, f"1/2 能传数据, 可用率应是 50.0%\n{txt[:400]}"
        assert "云端延迟存活" in txt and "免筛放行" in txt, txt[:400]
        assert "复检合格" not in txt, "旧标签还在 —— 它统计的是最终候选数"
    finally:
        ci_build.OUT = old


def t_kernel_proxy_keeps_ws_opts():
    """ws 节点的 path/host 必须还原成 ws-opts, 否则内核拿不到 Host 头。

    2026-11-12 的实测代价: verify.py 曾经自己抄字段传给内核, 漏了这一步,
    所有 CF 前置的 vless/trojan 一律 404 被判死, 内核类命中率掉到 0.7%。
    采集侧把 ws-opts 扁平化成顶层 path/host, 转换时必须还原 —— 两边约定不同,
    中间那一层少一次映射就全军覆没。
    """
    n = node(server="1.2.3.4", port=2083, proto="vless", tls=True,
             sni="a.pages.dev", network="ws",
             path="/", host="a.pages.dev")
    p = gateway.to_mihomo(n, "x")
    assert p and p.get("ws-opts"), p
    assert p["ws-opts"]["path"] == "/", p
    assert p["ws-opts"]["headers"]["Host"] == "a.pages.dev", p
    assert p.get("network") == "ws", p
    # 顶层不能再残留 path/host, 内核不认这两个键
    assert "path" not in p and "host" not in p, p


def t_kernel_proxy_rejects_unconvertible():
    """缺必需字段的节点转换应返回 None, 由调用方按"用户也用不了"处理。"""
    assert gateway.to_mihomo({"proto": "vless", "server": "1.2.3.4",
                              "port": 443}, "x") is None
    assert gateway.to_mihomo({"proto": "ss", "server": "1.2.3.4", "port": 443,
                              "cipher": "not-a-cipher", "password": "x"}, "x") is None


def t_pool_ok_none_means_undecided_not_dead():
    """ok=None 是"只记了一笔失败, 还没定论", 不能被当成不通踢掉。

    这条差点酿成误杀: verify 的 timeout 首轮只写 strikes/ok=None, 而
    pool_nodes 原来用 int(bool(ok)) 解析, None → 0 → 立刻踢出订阅。
    等于"为了不误杀而只记账", 结果记账本身就把节点杀了。
    """
    h = {f"n{i}": {"node": node(server=f"10.6.0.{i}", fp=f"n{i}",
                                verify_ok=3), "last_ok": time.time()}
         for i in range(6)}
    cn = {f"10.6.0.{i}:443": {"ok": None, "strikes": 1, "by": "verify",
                              "at": time.time()}
          for i in range(6)}
    out = gateway.pool_nodes(h, cn=cn)
    assert len(out) == 6, f"未定论的不该被踢, 应剩 6 个, 实际 {len(out)}"
    assert all(n["cn_ok"] is None for n in out), [n.get("cn_ok") for n in out]


def t_pool_sweep_dead_culled_despite_low_survival():
    """全池普查(by=verify)判死的不受 80% 兜底保护。

    这是 2026-11-12 通宵全池扫描前发现的坑: 存活率本来就该低于 20%,
    如果套用小样本的比例兜底, 扫一整夜一个都剔不掉, 等于白跑。
    """
    many = {f"n{i}": {"node": node(server=f"10.4.0.{i}", fp=f"n{i}",
                                   verify_ok=3), "last_ok": time.time()}
            for i in range(100)}
    cn = {f"10.4.0.{i}:443": {"ok": 0, "by": "verify", "at": time.time()}
          for i in range(85)}
    out = gateway.pool_nodes(many, cn=cn)
    assert len(out) == 15, f"普查判死的应该照剔, 应剩 15 个, 实际 {len(out)}"


def t_pool_sweep_dead_never_empties_pool():
    """全判死也不能把订阅清空 —— 留一半当候补, 但绝不能是原来的全量。"""
    h = {f"n{i}": {"node": node(server=f"10.5.0.{i}", fp=f"n{i}",
                                verify_ok=3), "last_ok": time.time()}
         for i in range(6)}
    cn = {f"10.5.0.{i}:443": {"ok": 0, "by": "verify", "at": time.time()}
          for i in range(6)}
    out = gateway.pool_nodes(h, cn=cn)
    assert 0 < len(out) < 6, f"应捞回部分候补, 实际 {len(out)}"


def t_pool_min_keep_fallback():
    """只剩少数"能连但传不动"的, 低于 MIN_KEEP 时应留作候补, 不能清空。"""
    h = {f"n{i}": {"node": node(server=f"10.2.0.{i}", fp=f"n{i}",
                                verify_ok=3), "last_ok": time.time()}
         for i in range(6)}
    # 全部"通但传不动"(_cn_rank=1 : ok=1 且 speed 低于阈值)
    cn = {f"10.2.0.{i}:443": {"ok": 1, "speed": 0.0, "at": time.time()}
          for i in range(6)}
    out = gateway.pool_nodes(h, cn=cn)
    assert len(out) == 6, f"应留作候补, 实际 {len(out)}"


def t_pool_cn_pref_expired_ignored():
    h = {"a": {"node": node(fp="a", verify_ok=3), "last_ok": time.time()}}
    old = {"1.2.3.4:443": {"ok": 0, "at": time.time() - 30 * 86400}}
    out = gateway.pool_nodes(h, cn=old)
    assert len(out) == 1, "过期实测数据不该继续生效"


# ---------------------------------------------------------------- 产物生成
def t_build_empty_pool():
    cfg = gateway.build_config([], [], [], runtime=False)
    assert "DIRECT" in cfg, "空池要给能上网的兜底, 不能生成非法配置"


def t_build_one_node():
    cfg = gateway.build_config([node(fp="a", name="N01")], [], [], 
                               namer=lambda n, i: f"N{i:02d}", runtime=False)
    assert "N01" in cfg


def t_build_base64_empty():
    s = gateway.build_base64([], [], [])
    assert isinstance(s, str)


def t_build_base64_numbering_contiguous():
    """转不出 URI 的节点不许在编号里留空号。

    带 TLS 的 http 代理没有公认分享格式, to_uri 返回 None。如果编号按 pool
    下标走, 手机端就会看到 N01..N10 之后跳到 N16 —— 30 个池子出 19 条, 空号
    正好 11 个, 用户以为订阅缺了一截。编号必须跟着实际输出位置走。
    """
    import base64 as _b64
    import re as _re

    pool = [
        node("1.1.1.1", proto="vless"),
        {"server": "2.2.2.2", "port": 443, "proto": "http",
         "username": "u", "password": "p", "tls": True},
        node("3.3.3.3", proto="ss", cipher="aes-256-gcm", password="p"),
        {"server": "4.4.4.4", "port": 443, "proto": "http",
         "username": "u", "password": "p", "tls": True},
        {"server": "5.5.5.5", "port": 8080, "proto": "http",
         "username": "u", "password": "p"},
    ]
    s = gateway.build_base64(pool, [], [],
                             namer=lambda n, i: f"N{i:02d} {n['server']}")
    raw = _b64.b64decode(s).decode()
    nums = [int(m.group(1)) for m in _re.finditer(r"#N(\d{2})", raw)]
    assert nums == [1, 2, 3], f"编号必须 1..3 连续, 实际 {nums}"
    assert "2.2.2.2" not in raw and "4.4.4.4" not in raw, "带 TLS 的 http 不该出现在 base64"


def t_build_handles_missing_fields():
    """节点字段残缺时不应该抛异常, 顶多这一条生成不出来。"""
    n = {"server": "1.2.3.4", "port": 443, "proto": "vless", "fp": "a"}
    gateway.build_config([n], [], [], runtime=False)


# ---------------------------------------------------------------- 协议转换
def t_to_mihomo_bad_shortid():
    """REALITY short-id 必须偶数长度 hex, 奇数长度要能被挡住。"""
    n = node(reality=True, pbk="AQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQE",
             sid="abc", fp="chrome")
    p = probe.to_mihomo(n, "x")
    # 要么拒绝(返回 None), 要么交出的 sid 合法
    if p is not None:
        sid = (p.get("reality-opts") or {}).get("short-id", "")
        assert len(sid) % 2 == 0, f"short-id 长度 {len(sid)} 是奇数"


def t_to_mihomo_missing_uuid():
    p = probe.to_mihomo({"server": "1.2.3.4", "port": 1, "proto": "vless"}, "x")
    assert p is None, "缺 uuid 应返回 None 而不是抛异常"


# ---------------------------------------------------------------- 一致性
def t_fp_key_consistency():
    """去重键必须全项目统一, 否则同一节点存两份、"测到死"的剔除会静默失效。"""
    n = node(port=8443)
    n["fp"] = "deadbeefdeadbeef"
    assert gateway.fp_key(n) == "deadbeefdeadbeef"

    m = node(port=8443)          # 没有 fp 的退化情况
    assert gateway.fp_key(m) == "1.2.3.4:8443:vless", gateway.fp_key(m)


def t_all_protocols_survive_missing_fields():
    """任何协议字段残缺都只能丢这一个节点, 不能抛异常。"""
    for proto in ("ss", "vmess", "vless", "trojan", "hysteria2", "tuic", "weird"):
        probe.to_mihomo({"server": "1.2.3.4", "port": 1, "proto": proto}, "x")
    probe.to_mihomo({}, "x")
    probe.to_mihomo(None, "x")


def t_tls_fingerprint_not_overwritten():
    """uTLS 指纹不能被节点哈希覆盖, 否则 reality 节点握手会失败。"""
    root = os.path.dirname(os.path.abspath(__file__))
    src = open(os.path.join(root, "collect.py"), encoding="utf-8").read()
    assert '"utls": p.get("fp", "")' in src, "解析 URI 时应把 uTLS 指纹存到 utls"
    assert '"fp": p.get("fp", "")' not in src, "fp 已被节点哈希占用, 不能再用作 uTLS 指纹"
    for f in ("probe.py", "gateway.py"):
        body = open(os.path.join(root, f), encoding="utf-8").read()
        assert 'get("utls")' in body, f"{f} 应读 utls 字段"


def t_pool_user_report_overrides_mass_dead_guard():
    """自动测大面积不通时不轻信; 但用户亲口说"这个不行"的必须照办。"""
    many = {f"n{i}": {"node": node(server=f"10.4.0.{i}", fp=f"n{i}",
                                   verify_ok=3), "last_ok": time.time()}
            for i in range(20)}
    cn = {f"10.4.0.{i}:443": {"ok": 0, "at": time.time()} for i in range(19)}
    cn["10.4.0.0:443"] = {"ok": 0, "at": time.time(), "by": "user_report"}
    out = gateway.pool_nodes(many, cn=cn)
    assert len(out) == 19, f"应只摘掉用户报的那个, 实际 {len(out)}"


# ---------------------------------------------------------------- 反馈解析
def t_mark_resolve_by_ip():
    import mark
    t, missed = mark.resolve("202.50.253.72 打不开", {})
    assert t == {"202.50.253.72"}, t
    assert missed == []


def t_mark_resolve_by_name():
    import mark
    t, missed = mark.resolve("N05 和 N07 连不上", {"N05": "1.1.1.1", "N07": "2.2.2.2"})
    assert t == {"1.1.1.1", "2.2.2.2"}, t


def t_mark_unknown_name():
    import mark
    t, missed = mark.resolve("N99 不行", {})
    assert t == set() and missed == ["N99"], (t, missed)


def t_mark_nothing_recognized():
    import mark
    t, missed = mark.resolve("今天天气不错", {})
    assert not t and not missed


def t_mark_empty_text():
    import mark
    t, missed = mark.resolve("", {})
    assert not t


def t_mark_ordinal_forms():
    """「第5个」「5号」和「N05」要等价, 手机上没人会写 N05。"""
    import mark
    m = {"N05": "1.1.1.1", "N12": "2.2.2.2"}
    for txt in ("第5个不行", "5号不行", "第 5 个不行", "N5 不行", "n05 不行"):
        t, _ = mark.resolve(txt, m)
        assert t == {"1.1.1.1"}, f"{txt!r} → {t}"


def t_mark_ordinal_no_false_positive():
    """IP 里的数字不能被当成序号: 202.0.0.5 不该命中 N05。"""
    import mark
    t, missed = mark.resolve("202.0.0.5 打不开", {"N05": "1.1.1.1"})
    assert t == {"202.0.0.5"}, t
    assert missed == [], missed


def t_mark_form_preamble_ignored():
    """issue 表单会把模板说明写进正文, 里面的举例不能被当成真实反馈。"""
    import mark
    body = ("客户端里每个节点都显示成「序号 + IP」的形式，比如排第五的那个，\n"
            "名字就是「N05」后面跟一串地址。\n\n"
            "### 范围\n\n某几个节点（在下面写）\n\n"
            "### 哪些节点\n\nN07 连不上\n")
    t, _ = mark.resolve(body, {"N05": "1.1.1.1", "N07": "2.2.2.2"})
    assert t == {"2.2.2.2"}, f"模板里的 N05 被误解析: {t}"
    assert not mark.wants_full_rebuild(body)


def t_mark_all_keyword():
    """「全部都不行」这类整体性描述要触发重新筛选, 而不是逐个点名。"""
    import mark
    for txt in ("全部都不行", "全都连不上", "整体都很慢", "重测一下", "换一批吧"):
        assert mark.wants_full_rebuild(txt), txt
    assert not mark.wants_full_rebuild("N05 不行")


def t_mark_form_all_option():
    """表单里选「全部都不行」时, 正文要能被识别成整体重筛。"""
    import mark
    body = "### 范围\n\n全部都不行 / 整体都很慢（重新筛选一批）\n\n### 哪些节点\n\n"
    assert mark.wants_full_rebuild(body)


def t_issue_template_shape():
    """issue 表单的字段名和选项必须齐, 否则 GitHub 上点进去是空白页。"""
    import yaml
    p = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                     ".github", "ISSUE_TEMPLATE", "node-report.yml")
    d = yaml.safe_load(open(p, encoding="utf-8"))
    assert d.get("name") and d.get("description"), "缺少 name/description"
    body = d.get("body") or []
    kinds = [b.get("type") for b in body]
    assert "dropdown" in kinds and "textarea" in kinds, kinds
    for b in body:
        if b["type"] in ("dropdown", "textarea"):
            assert b.get("id"), f"{b['type']} 缺 id"
            assert (b.get("attributes") or {}).get("label"), f"{b['type']} 缺 label"
    # 选项文字会原样进正文, 不能出现会被解析成节点的字样, 否则每次提交都误标
    import re as _re
    for b in body:
        if b["type"] != "dropdown":
            continue
        for opt in (b.get("attributes") or {}).get("options") or []:
            assert not _re.search(r"N\s?\d", str(opt)), f"选项里出现节点序号: {opt}"
    # 模板顶部的说明文字也会进正文, 但它永远排在第一行, mark 会整体忽略;
    # 这里只保证「说明在选项之前」这个前提成立。
    assert body[0]["type"] == "markdown", "示例文字必须放在最前面, 才能被整体跳过"


def t_mark_legacy_four_digit_name():
    """手机端(base64 旧订阅)显示的是 n0158 这种四位编号, 必须认得。"""
    import mark
    m = {"n0158": "1.1.1.1"}
    t, missed = mark.resolve("n0158 超时", m)
    assert t == {"1.1.1.1"}, (t, missed)
    assert missed == [], missed


def t_mark_name_and_ip_same_target():
    """用户照抄客户端里整条名字（序号 + IP）时不该重复计数。"""
    import mark
    t, missed = mark.resolve("N05 202.50.253.72", {"N05": "202.50.253.72"})
    assert t == {"202.50.253.72"}, t
    assert missed == [], missed


def t_base64_names_match_yaml():
    """两种订阅格式必须同名 —— 否则手机看到的名字和说明里的对不上。"""
    import base64
    import urllib.parse
    import yaml
    pool = [node(server=f"10.9.0.{i}", fp=f"x{i}") for i in range(1, 4)]

    def namer(n, i):
        return f"N{i:02d} {n['server']}"

    y = gateway.build_config(pool, [], [], namer=namer, runtime=False)
    b = gateway.build_base64(pool, [], [], namer=namer)
    ys = [p["name"] for p in (yaml.safe_load(y) or {}).get("proxies") or []]
    raw = base64.b64decode(b).decode()
    bs = []
    for line in raw.splitlines():
        if line.startswith("vmess://"):
            import collect
            bs.append((collect.parse_uri(line, "t") or {}).get("name"))
        elif "#" in line:
            bs.append(urllib.parse.unquote(line.split("#", 1)[1]))
    assert ys == bs, f"yaml={ys} base64={bs}"


# ---------------------------------------------------------------- YAML 订阅 & 普通代理
def t_yaml_sub_extracts_nodes():
    """Clash YAML 订阅必须能解析 —— 只认 URI 的正则会把它整份算成 0 条。"""
    import collect
    doc = """
proxies:
  - name: "A"
    type: ss
    server: 1.2.3.4
    port: 8388
    cipher: aes-256-gcm
    password: pw
  - name: "B"
    type: http
    server: 5.6.7.8
    port: 9002
"""
    ns = collect.nodes_from_yaml(doc, "test")
    assert len(ns) == 2, ns
    assert {n["proto"] for n in ns} == {"ss", "http"}, ns


def t_yaml_http_proxy_fields():
    """http 代理的关键字段不能丢: 用户名/密码/tls 少一个就连不上。"""
    import collect
    doc = """
proxies:
  - {name: H, type: http, server: h.example.com, port: 443,
     username: u, password: p, tls: true}
"""
    n = collect.nodes_from_yaml(doc, "t")[0]
    assert n["proto"] == "http", n
    assert n["server"] == "h.example.com" and n["port"] == 443, n
    assert n["username"] == "u" and n["password"] == "p", n
    assert n["tls"] is True, n


def t_yaml_broken_input():
    """畸形 / 空 / 非订阅内容只能返回空列表, 不能抛异常拖垮整轮采集。"""
    import collect
    for bad in ["", "{", "proxies: [", "hello world",
                "proxies:\n  - 1\n  - 2", "proxies: notalist", "[]", "null"]:
        assert collect.nodes_from_yaml(bad, "t") == [], repr(bad)


def t_yaml_unknown_type_dropped():
    """不认识的协议要丢掉, 不能带着 type 进池子让测速环节崩。"""
    import collect
    doc = """
proxies:
  - {name: X, type: wireguard, server: 1.1.1.1, port: 1}
  - {name: Y, type: vless, server: 2.2.2.2, port: 443, uuid: abc}
"""
    ns = collect.nodes_from_yaml(doc, "t")
    assert [n["proto"] for n in ns] == ["vless"], ns


def t_yaml_ws_opts_restored():
    """ws 的 path/host 在 YAML 里是嵌套结构, 不还原就连不上。"""
    import collect
    doc = """
proxies:
  - name: W
    type: vless
    server: w.example.com
    port: 443
    uuid: 11111111-2222-3333-4444-555555555555
    tls: true
    servername: w.example.com
    network: ws
    ws-opts:
      path: /ray
      headers:
        Host: w.example.com
"""
    n = collect.nodes_from_yaml(doc, "t")[0]
    assert n["network"] == "ws" and n["path"] == "/ray", n
    assert n["host"] == "w.example.com", n
    assert n["sni"] == "w.example.com", n


def t_http_proxy_roundtrip():
    """http 节点: 内部结构 → mihomo 配置, 字段一个都不能丢。"""
    n = {"proto": "http", "server": "p.example.com", "port": 8443,
         "username": "u", "password": "p", "tls": True, "name": "H1"}
    p = probe.to_mihomo(n, "H1")
    assert p and p["type"] == "http" and p["tls"] is True, p
    assert p["username"] == "u" and p["password"] == "p", p
    assert p["udp"] is False, "http 代理必须显式关掉 udp"


def t_http_proxy_to_uri():
    """base64 订阅里要能表达普通代理, 但**不能**表达带 TLS 的那种。

    https://host:port 没有公认的代理语义, 客户端会当成网页地址 —— 小火箭里
    就是一个带 ⚠️ 的 HTTPS 条目, 看着在, 点不通。宁可缺席, 也不要假装存在:
    缺席用户知道"少一个", 假存在用户会以为"节点全坏了"。
    """
    n = {"proto": "http", "server": "p.example.com", "port": 8443,
         "username": "u", "password": "p", "name": "H1"}
    uri = gateway.to_uri(n)
    assert uri and uri.startswith("http://u:p@p.example.com:8443"), uri
    # 带 TLS 的: base64 里没有合法写法, 必须返回 None 交给 sub.conf
    n["tls"] = True
    assert gateway.to_uri(n) is None, gateway.to_uri(n)
    n2 = {"proto": "http", "server": "q.example.com", "port": 8080, "name": "H2"}
    uri2 = gateway.to_uri(n2)
    assert uri2 and uri2.startswith("http://q.example.com:8080"), uri2


def t_base64_never_emits_https():
    """整个 base64 产物里一条 https:// 都不许有 —— 这是手机端 ⚠️ 的来源。"""
    pool = [
        {"proto": "http", "server": "a.example.com", "port": 443, "tls": True, "name": "H1"},
        {"proto": "http", "server": "b.example.com", "port": 8080, "name": "H2"},
        {"proto": "socks5", "server": "c.example.com", "port": 1080, "name": "S1"},
    ]
    import base64 as b64
    txt = b64.b64decode(gateway.build_base64(pool, [], [])).decode()
    assert "https://" not in txt, txt
    assert "http://b.example.com:8080" in txt
    assert "socks5://c.example.com:1080" in txt
    assert "a.example.com" not in txt, "带 TLS 的 http 代理不该出现在 base64 里"


def t_surge_http_no_auth():
    """iOS 端的关键: https 代理靠类型名表达, 不是靠 tls=true。

    小火箭读 base64 时只认节点链接, 而普通 HTTP 代理没有公认的分享格式,
    写成 https://host:port 会被当成网页地址 —— 这就是"列表里有、点了不通"。
    Surge 的 [Proxy] 段有明确写法, 所以手机端必须吃这份 conf。
    """
    p = {"type": "http", "server": "51.159.225.196", "port": 23856,
         "tls": True, "skip-cert-verify": True}
    line = gateway._surge_line(p, "N22")
    assert line == ("N22 = https, 51.159.225.196, 23856, "
                    "nodepool, nodepool, skip-cert-verify=true"), line
    # 绝不能留下空位参数 ", ," —— 客户端会把后面全部字段读错
    assert ", ," not in line, line
    # 占位账号密码是有意的: 小火箭对"账号密码为空的 http/https 代理"会打黄色
    # 警告三角, 用户看到以为节点坏了。这些开放代理会忽略 Proxy-Authorization
    # (实测带 nodepool/nodepool 依然 204、1.37MB/s), 所以填占位值不影响连通。
    p2 = {"type": "http", "server": "q.example.com", "port": 8080}
    assert (gateway._surge_line(p2, "N01")
            == "N01 = http, q.example.com, 8080, nodepool, nodepool")
    # 真有账号密码时必须用真的, 不能被占位值顶掉
    p3 = {"type": "http", "server": "q.example.com", "port": 8080,
          "username": "u1", "password": "p1"}
    assert gateway._surge_line(p3, "N02") == "N02 = http, q.example.com, 8080, u1, p1"


def t_surge_comma_guard():
    """密码里带逗号会把一行拆成两个字段, 客户端可能导入成功却永远连不上。
    输出端必须宁可跳过, 也不能写出一行看着正常实际是错的内容。"""
    p = {"type": "ss", "server": "1.1.1.1", "port": 443,
         "cipher": "aes-128-gcm", "password": "ab,cd"}
    assert gateway._surge_line(p, "N01") is None
    ok = {"type": "ss", "server": "1.1.1.1", "port": 443,
          "cipher": "aes-128-gcm", "password": "abcd"}
    assert gateway._surge_line(ok, "N01").startswith("N01 = ss, 1.1.1.1, 443")


def t_surge_group_refs_exist():
    """组里引用的名字必须都在 [Proxy] 里存在。

    少一个, Surge 会整份配置导入失败; 名字对不上是最难查的那类错误, 因为
    conf 看起来完全正常, 只有客户端知道它坏了。
    """
    pool = [
        {"proto": "ss", "server": "1.1.1.1", "port": 443,
         "cipher": "aes-128-gcm", "password": "pw"},
        {"proto": "http", "server": "2.2.2.2", "port": 8443, "tls": True},
    ]
    conf = gateway.build_surge(pool, [], [])
    txt = conf.splitlines()
    assert "[Proxy]" in txt and "[Proxy Group]" in txt and "[Rule]" in txt
    i, j = txt.index("[Proxy]"), txt.index("[Proxy Group]")
    defined = {l.split(" = ")[0].strip() for l in txt[i + 1:j] if " = " in l}
    groups = {}
    for l in txt[j + 1:]:
        if l.startswith("["):
            break
        if " = " in l:
            k, v = l.split(" = ", 1)
            groups[k.strip()] = [x.strip() for x in v.split(",")]
    assert "自动选择" in groups and groups["自动选择"][0] == "url-test"
    for gname, parts in groups.items():
        for ref in parts[1:]:
            if "=" in ref:      # url = ... / interval = ... 这类参数
                continue
            assert ref in defined or ref in groups, f"{gname} 引用了不存在的 {ref}"
    assert txt[-1] == "FINAL,节点选择"


def t_surge_empty_pool():
    """一个节点都没有时不能产出空 [Proxy], 否则客户端导入即报错。"""
    conf = gateway.build_surge([], [], [])
    assert "DIRECT = direct" in conf
    assert "节点选择 = select, DIRECT" in conf


def t_yaml_null_literals():
    """订阅里用字符串 'null' 表示"无认证"(go4sharing 那份就是这么写的)。
    不清掉会生成 https://null:null@host —— 每次连接都被拒, 但测速阶段看不出来。"""
    import collect
    doc = """
proxies:
  - {name: A, type: http, server: 1.1.1.1, port: 8080, username: 'null', password: 'null'}
  - {name: B, type: vless, server: 2.2.2.2, port: 443, uuid: 'null', sni: 'none'}
"""
    a, b = collect.nodes_from_yaml(doc, "t")
    assert a["username"] == "" and a["password"] == "", a
    assert b["uuid"] == "" and b["sni"] == "", b
    uri = gateway.to_uri(a)
    assert uri and "null" not in uri.split("#")[0], uri
    # uuid 被清洗成空之后, 测速环节必须把它丢掉, 而不是生成一个连不上的配置
    assert probe.to_mihomo(b, "B") is None


def t_sub_link_filter():
    """频道里的推广页/图片/站内链接不该占用抓取配额。"""
    import collect
    assert collect.is_sub_candidate("https://jikun.zmxoo.xyz/subapi?token=free_x")
    assert collect.is_sub_candidate("https://proxy.hongx.sbs/ip/3468e3a3")
    assert collect.is_sub_candidate("https://apirule.fastfly.qzz.io/s/f8e623")
    for bad in ["https://t.me/wxgqlfx", "https://github.com/a/b",
                "https://www.google.com/x", "https://a.com/x.png",
                "ftp://a.com/x", "not-a-url"]:
        assert not collect.is_sub_candidate(bad), bad


def t_looks_like_sub():
    """抓回来先验内容, 推广页不能被当成订阅灌进解析器刷日志。"""
    import collect
    assert collect.looks_like_sub("vmess://eyJhZGQiOiIxLjIuMy40In0=")
    assert collect.looks_like_sub("proxies:\n  - {server: 1.1.1.1, port: 1}")
    assert not collect.looks_like_sub("<html>注册送10G</html>")
    assert not collect.looks_like_sub("")


def t_archive_urls_from_api():
    """归档源: 目录接口能列出来时, 按文件名取最近的几份。"""
    import collect
    listing = json.dumps([
        {"name": "01-01.yaml"}, {"name": "README.md"},
        {"name": "08-09.yaml"}, {"name": "11-08.yaml"},
    ]).encode()

    class FakeOpener:
        def open(self, req, timeout=0):
            raise AssertionError("这个用例不该发网络请求")

    urls = collect.archive_urls  # 只验证纯逻辑那部分
    # 用 monkeypatch 替换 fetch, 避免自检依赖网络
    old = collect.fetch
    collect.fetch = lambda *a, **k: listing
    try:
        got = collect.archive_urls("a/b", "2026", 2, None)
    finally:
        collect.fetch = old
    assert got == ["https://raw.githubusercontent.com/a/b/main/2026/08-09.yaml",
                   "https://raw.githubusercontent.com/a/b/main/2026/11-08.yaml"], got


def t_archive_urls_fallback():
    """目录接口拿不到就按日期猜, 且不能因为文件缺失就返回空。"""
    import collect
    old = collect.fetch
    collect.fetch = lambda *a, **k: None
    try:
        got = collect.archive_urls("a/b", "2026", 3, None)
    finally:
        collect.fetch = old
    assert len(got) == 6, len(got)          # take*2: 空缺多, 多猜几天
    assert all(u.endswith(".yaml") for u in got)
    assert "/main/" in got[0]


def t_pool_nodes_dedup_by_endpoint():
    """同一 server:port 在订阅里只能出现一次。

    真实成因不是"凭据不同", 而是历史键格式不一致: 早期 ci_build 用 "server:port",
    后来统一成 "server:port:proto", 两种格式的记录会同时留在池子里。表现是订阅里
    两个编号指向同一个地址, 客户端对同一端口发起两倍连接。
    """
    n1 = node(server="5.5.5.5", port=443, proto="ss",
              cipher="aes-128-gcm", password="a")
    n2 = node(server="5.5.5.5", port=443, proto="ss",
              cipher="aes-128-gcm", password="b")
    n1.pop("fp", None)
    n2.pop("fp", None)
    n1["score"] = 0.2          # 排序读的是 node.score, 不是记录级 score
    n2["score"] = 0.9
    hist = {"5.5.5.5:443": {"node": n1, "score": 0.2, "last_ok": time.time()},
            "5.5.5.5:443:ss": {"node": n2, "score": 0.9, "last_ok": time.time()}}
    cn = {"5.5.5.5:443": {"ok": 1, "speed": 0.5, "latency": 100}}

    out = gateway.pool_nodes(hist, cn=cn)
    assert len(out) == 1, [x.get("password") for x in out]
    assert out[0]["password"] == "b", out[0]["password"]   # 排序靠前的那条胜出


def t_history_truncate_keeps_cn_alive():
    """池子满了要砍, 但绝不能砍掉国内实测能通的 —— 那是唯一真有用的证据。"""
    old_max, old_cn = gateway.MAX_POOL, gateway.load_cn_pref
    gateway.MAX_POOL = 2
    gateway.load_cn_pref = lambda: {
        "9.9.9.9:443": {"ok": 1, "speed": 9.5, "latency": 100}}
    try:
        hist = {}
        for i, srv in enumerate(["1.1.1.1", "2.2.2.2", "9.9.9.9"]):
            n = node(server=srv, port=443, proto="ss", cipher="aes-128-gcm",
                     password="p")
            n["score"] = 0.9 if srv != "9.9.9.9" else 0.0   # 实测那个云端评分最低
            hist[gateway.fp_key(n)] = {"node": n, "score": n["score"], "last_ok": time.time()}
        out = gateway.merge_history(hist, [])
        kept = {v["node"]["server"] for v in out.values()}
        assert "9.9.9.9" in kept, kept
        assert len(out) == 2, len(out)
    finally:
        gateway.MAX_POOL, gateway.load_cn_pref = old_max, old_cn


def t_cf_email_restore():
    """Cloudflare 邮箱混淆会把 ss://<b64>@host 的 @ 换成 <a data-cfemail>, 必须还原。

    构造方法与线上一致: 首字节放 key, 其余字节 key XOR 原文字符。
    """
    import collect
    # 线上 shape: ss:// 后面直接跟 <a>, CF 把整段 "base64@host:port" 当成邮箱混淆了
    plain = "YWJj@1.2.3.4:8443"
    key = 0x5A
    enc = f"{key:02x}" + "".join(f"{ord(c) ^ key:02x}" for c in plain)
    page = ('<pre>ss://<a href="/cdn-cgi/l/email-protection" class="__cf_email__" '
            f'data-cfemail="{enc}">[email protected]</a>#name</pre>')
    out = collect.restore_cf_email(page)
    assert "<a" not in out, out
    assert "ss://YWJj@1.2.3.4:8443#name" in out, out
    uris = collect.VG_REGEX.findall(out)
    assert len(uris) == 1 and uris[0].endswith(":8443#name"), uris


def t_cf_email_noop():
    """没有混淆的文本必须原样返回, 不能平白改写内容。"""
    import collect
    s = "vless://uuid@host:443?type=ws#n"
    assert collect.restore_cf_email(s) == s
    assert collect.cf_decode_email("") == ""
    assert collect.cf_decode_email("zz") == ""      # 非法 hex 不抛异常


def t_blog_urls_dedup_and_cap():
    """博客型源: 首页里同一篇文章出现多次只算一次, 且不超过 take 篇。"""
    import collect
    page = ('<a href="https://blog.x.icu/2026/11/11/a-free-proxies/">A</a>'
            '<a href="https://blog.x.icu/2026/11/11/a-free-proxies/">A again</a>'
            '<a href="https://blog.x.icu/2026/11/10/b-free-proxies/">B</a>'
            '<a href="https://blog.x.icu/2026/11/09/c-free-proxies/">C</a>'
            '<a href="https://blog.x.icu/about">about</a>')
    old = collect.fetch
    collect.fetch = lambda *a, **k: page.encode()
    try:
        pat = r"https://blog\.x\.icu/20\d\d/\d\d/\d\d/[^\s\"'<>]+"
        got = collect.blog_urls("https://blog.x.icu/", pat, 2, None)
    finally:
        collect.fetch = old
    assert len(got) == 2, got                     # take 生效
    assert len(set(got)) == 2, got                # 重复链接被去掉
    assert got[0].endswith("a-free-proxies/"), got  # 保持首页顺序(最近的在前)


def t_blog_urls_home_down():
    """首页抓不到就返回空, 不能抛异常把整轮采集带崩。"""
    import collect
    old = collect.fetch
    collect.fetch = lambda *a, **k: None
    try:
        got = collect.blog_urls("https://nope.invalid/", r"https://x/(\d+)", 5, None)
    finally:
        collect.fetch = old
    assert got == [], got


def t_blog_sources_configured():
    """配了博客源就必须有可用的正则, 否则每天白抓一个首页。"""
    import re
    import collect
    for tag, home, pat, take in collect.BLOG_SOURCES:
        assert home.startswith("http"), (tag, home)
        assert take >= 1, (tag, take)
        re.compile(pat)          # 正则写错在这一步就炸, 不用等到线上


def t_dead_channels_not_active():
    """停更频道不能还挂在抓取名单里, 否则每天白请求一次。"""
    import collect
    assert not (set(collect.TG_CHANNELS) & set(collect.DEAD_CHANNELS))
    assert len(collect.TG_CHANNELS) >= 4


def t_dated_urls():
    """按日期猜地址: 今天在最前, 跨月/跨年都要算对, 否则回补会整天 404。"""
    import datetime
    import collect
    u = collect.dated_urls("https://a/{y}/{m}/{ymd}.yaml", 3,
                          today=datetime.date(2026, 11, 11))
    assert u[0] == "https://a/2026/11/20261111.yaml", u
    assert u[1] == "https://a/2026/11/20261110.yaml", u
    assert len(u) == 3
    # 跨月: 12-01 往前一天必须是 11-30, 不能变成 12-00 或 11-31
    v = collect.dated_urls("https://a/{y}/{m}/{ymd}.yaml", 2,
                          today=datetime.date(2026, 12, 1))
    assert v[1] == "https://a/2026/11/20261130.yaml", v
    # 跨年
    w = collect.dated_urls("https://a/{y}/{m}/{ymd}.yaml", 2,
                          today=datetime.date(2027, 1, 1))
    assert w[1] == "https://a/2026/12/20261231.yaml", w
    # days 写 0 或负数也不能返回空列表
    assert len(collect.dated_urls("https://a/{ymd}", 0,
                                 today=datetime.date(2026, 11, 11))) == 1
    # 同日多份(带 {n}): 每天展开成 N 个地址, 顺序是天优先、序号次之
    s = collect.dated_urls("https://a/{y}/{m}/{n}-{ymd}.yaml", 2,
                           today=datetime.date(2026, 11, 11), seq=(0, 1))
    assert s == ["https://a/2026/11/0-20261111.yaml",
                 "https://a/2026/11/1-20261111.yaml",
                 "https://a/2026/11/0-20261110.yaml",
                 "https://a/2026/11/1-20261110.yaml"], s


def t_dated_sources_wellformed():
    """日期型源的表配错(模板没占位符/天数写成 0)会让每天的采集静默变空。"""
    import collect
    for entry in collect.DATED_SOURCES:
        tag, tpl, days = entry[0], entry[1], entry[2]
        seq = entry[3] if len(entry) > 3 else None
        assert tpl.startswith("http"), (tag, tpl)
        assert "{ymd}" in tpl, (tag, tpl)      # 至少要能按天变
        assert days >= 1, (tag, days)
        assert tpl.count("{") == tpl.count("}"), (tag, tpl)
        # 模板里有 {n} 就必须给序号, 否则 format 会 KeyError 让整段采集炸掉
        if "{n}" in tpl:
            assert seq, (tag, "模板含 {n} 但没配序号")
        else:
            assert seq is None, (tag, "模板没有 {n} 却配了序号")


def t_opener_ignores_env_proxy():
    """没指定代理时必须真的直连 —— 环境变量里的 http_proxy 常指向失效端口。"""
    import collect
    os.environ["http_proxy"] = "http://127.0.0.1:9"
    try:
        op = collect.build_opener(None)
        assert not [h for h in op.handlers if h.__class__.__name__ == "ProxyHandler"
                    and getattr(h, "proxies", {})]
    finally:
        os.environ.pop("http_proxy", None)


# ---------------------------------------------------------------- 自建 CF 节点
def t_cf_sample_ips_stay_inside_blocks():
    """优选 IP 的采样必须落在 CF 段内, 否则扫的是别人的 IP。"""
    import cf_scan
    ips = cf_scan.sample_ips(200, seed=7)
    assert len(ips) == 200, len(ips)
    assert len(set(ips)) == 200, "有重复 IP"
    ranges = [(cf_scan._ip_to_int(b), 32 - p) for b, p in cf_scan.CF_BLOCKS]
    for ip in ips:
        v = cf_scan._ip_to_int(ip)
        assert any(base <= v < base + 2 ** bits for base, bits in ranges), f"{ip} 不在 CF 段内"


def t_cf_node_keeps_ws_opts():
    """CF 只能出 ws, 节点必须带 ws-opts。

    这条是给 2026-11-11 那个 bug 上锁: 当时探测丢掉了 ws-opts, 导致所有
    CF 前置节点的 ws 握手拿不到 Host 头, 整类被判死(命中率 0.7% vs 同批 http 8.2%)。
    """
    import cf_sub
    cfg = {"uuid": "11111111-2222-3333-4444-555555555555",
           "domain": "n.example.com", "path": "/abc123"}
    n = cf_sub.build_node(cfg, "104.16.1.1", 1)
    assert n["network"] == "ws"
    assert n["ws-opts"]["path"] == "/abc123", n["ws-opts"]
    assert n["ws-opts"]["headers"]["Host"] == "n.example.com", n["ws-opts"]
    assert n["servername"] == "n.example.com" and n["tls"] is True
    assert n["port"] == 443, "CF 免费版只放行标准端口"


def t_cf_vless_url_complete():
    """vless:// 链接缺一个参数, 客户端就是连不上, 而且不报错。"""
    import cf_sub
    cfg = {"uuid": "11111111-2222-3333-4444-555555555555",
           "domain": "n.example.com", "path": "/abc123"}
    url = cf_sub.vless_url(cfg, "104.16.1.1")
    for must in ("security=tls", "type=ws", "sni=n.example.com",
                 "host=n.example.com", "path=%2Fabc123", "encryption=none"):
        assert must in url, f"缺 {must}: {url}"


def t_cf_emit_refuses_without_domain():
    """没域名就要报错, 不能生成一堆连不上的节点 —— 那种半成品最难查。"""
    import cf_sub
    try:
        cf_sub.emit({"uuid": "x", "domain": "", "path": "/p", "ips": ["1.1.1.1"]}, 1, False)
    except SystemExit:
        return
    raise AssertionError("没有域名却生成了订阅")


# ---------------------------------------------------------------- CF 中转
def t_cf_relay_extract_ips():
    """从频道原文里抠 IP:端口。要能容忍废话、版本号、日期。"""
    import cf_relay
    text = ("今日优选 104.16.1.1:443 104.17.2.2:2053\n"
            "v2.1.5 发布于 2026.11.12 16:30\n"
            "无效 999.1.1.1:443 和 104.18.3.3:99999\n"
            "单写 104.19.4.4 也要认(默认 443)")
    pairs = cf_relay.extract_ips(text)
    assert (("104.16.1.1", 443) in pairs), pairs
    assert (("104.17.2.2", 2053) in pairs), pairs
    assert (("104.19.4.4", 443) in pairs), pairs
    assert not any(ip.startswith(("999.", "2.1.", "2026.")) for ip, _ in pairs), pairs


def t_cf_relay_swap_only_endpoint():
    """换入口**只能**动 server/port。sni/host/path/uuid 动一个字节,
    节点就会静默失效 —— 这是整套做法唯一的危险操作。"""
    import cf_relay
    n = node(server="old.example.com", port=443, proto="vless",
             sni="backend.pages.dev", network="ws", path="/abc?ed=2048",
             host="backend.pages.dev")
    m = cf_relay.swap_endpoint(n, "104.16.1.1", 2053)
    assert m["server"] == "104.16.1.1" and m["port"] == 2053
    for k in ("sni", "host", "path", "uuid"):
        assert m[k] == n[k], (k, m[k], n[k])
    # 原节点不能被就地改掉
    assert n["server"] == "old.example.com", "swap 修改了原节点"
    assert n is not m


def t_cf_relay_parse_link_keeps_ws():
    """vless 链接解析要保住 ws 的 path/host —— 丢了就是 2026-11-11 那个坑。"""
    import cf_relay
    link = ("vless://11111111-2222-3333-4444-555555555555@old.example.com:443"
            "?security=tls&type=ws&path=%2Ftv%2Fis&sni=backend.pages.dev"
            "&host=backend.pages.dev#demo")
    n = cf_relay.node_from_link(link)
    assert n["proto"] == "vless" and n["server"] == "old.example.com"
    assert n["network"] == "ws", n
    assert n["path"] == "/tv/is", n["path"]
    assert n["host"] == "backend.pages.dev", n
    assert n["sni"] == "backend.pages.dev", n
    assert n["tls"] is True


def t_cf_relay_rejects_bad_link():
    import cf_relay
    assert cf_relay.node_from_link("http://a/b") is None
    assert cf_relay.node_from_link("ss://xxx") is None


def t_cf_relay_pool_lookup_missing():
    import cf_relay
    assert cf_relay.node_from_pool("no-such-host.invalid:443") is None


# ------------------------------------------------------------ CF 自建后端
def t_cf_worker_render_fills_credentials():
    """--worker 不该要求人手改代码。凭据要原样灌进去, 占位符一个不剩。"""
    import cf_sub
    cfg = {"uuid": "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee",
           "path": "/p%26q", "domain": "", "ips": []}
    old_tpl, old_out = cf_sub.WORKER_TPL, cf_sub.WORKER_OUT
    with tempfile.TemporaryDirectory() as d:
        try:
            cf_sub.WORKER_OUT = cf_sub.Path(d) / "worker.js"
            assert cf_sub.emit_worker(cfg) == 0
            out = cf_sub.WORKER_OUT.read_text()
        finally:
            cf_sub.WORKER_TPL, cf_sub.WORKER_OUT = old_tpl, old_out
    assert cfg["uuid"] in out, "UUID 没灌进去"
    assert "'/p%26q'" in out, "path 没灌进去"
    assert "__UUID__" not in out and "__PATH__" not in out, "还有占位符没替换"


def t_cf_worker_path_matches_node():
    """Worker 里的 PATH 和节点里的 ws-opts.path 必须逐字节相同。
    差一个斜杠就握手失败, 而失败信息里不会告诉你是哪边错了。"""
    import cf_sub
    cfg = {"uuid": "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee",
           "path": "/abc123", "domain": "x.workers.dev", "ips": []}
    n = cf_sub.build_node(cfg, "1.2.3.4", 1)
    assert n["ws-opts"]["path"] == cfg["path"]
    assert n["ws-opts"]["headers"]["Host"] == cfg["domain"]
    assert n["servername"] == cfg["domain"], "SNI 必须走后端域名, 不能是 IP"
    assert n["server"] == "1.2.3.4", "入口才是 IP"


def t_cf_worker_node_udp_off():
    """Worker 只能出站 TCP, 节点写 udp:true 会让 DNS 卡死到超时。"""
    import cf_sub
    n = cf_sub.build_node({"uuid": "u", "path": "/p", "domain": "x.workers.dev"}, "1.1.1.1", 1)
    assert n["udp"] is False, "udp 必须是 False"


def t_cf_worker_js_syntax():
    """node --check 有分辨力(故意写错会返回 1), 所以它通过是有意义的。"""
    here = os.path.dirname(os.path.abspath(__file__))
    r = subprocess.run(["node", "--check", os.path.join(here, "worker", "worker.js")],
                       capture_output=True, text=True)
    assert r.returncode == 0, (r.stderr or "")[-300:]


def t_cf_worker_header_parser():
    """vless 头的字节偏移错一位, 线上表现是「连上了就是不通」。本地先钉死。"""
    here = os.path.dirname(os.path.abspath(__file__))
    r = subprocess.run(["node", os.path.join(here, "worker", "test_worker.mjs")],
                       capture_output=True, text=True, timeout=60)
    assert r.returncode == 0, (r.stdout or "")[-500:] + (r.stderr or "")[-300:]


# ---------------------------------------------------------- 普通代理(HTTP/SOCKS)
def t_proxy_uri_needs_three():
    """单条 http://x:port 不收 —— 帖子正文里出现一条太正常了。"""
    import collect
    one = "http://1.2.3.4:8080"
    assert collect.proxy_uri_nodes(one, "t") == [], "单条不该收"
    three = "http://1.2.3.4:8080\nhttp://5.6.7.8:3128\nsocks5://9.9.9.9:1080"
    got = collect.proxy_uri_nodes(three, "t")
    assert len(got) == 3, got
    assert {n["proto"] for n in got} == {"http", "socks5"}, got


def t_proxy_uri_rejects_paths():
    """网页链接一定带路径; 带路径的绝不能被当成代理。"""
    import collect
    txt = ("https://a.com/path/1\nhttps://b.com/x?y=1\nhttp://c.com:8080/a\n"
           "http://1.2.3.4:8080")
    assert collect.proxy_uri_nodes(txt, "t") == [], collect.proxy_uri_nodes(txt, "t")


def t_proxy_list_needs_whole_document():
    """整篇都是地址才算清单; 一半正文一半地址的, 一条都不收。"""
    import collect
    good = "\n".join(f"{i}.{i}.{i}.{i}:8080" for i in range(2, 20))
    assert len(collect.proxy_list_nodes(good, "t")) >= 10
    mixed = "1.1.1.1:80\n2.2.2.2:80\n3.3.3.3:80\n以下是广告\n" * 3
    assert collect.proxy_list_nodes(mixed, "t") == [], "混排版式被误认成清单"


def t_proxy_list_drops_private_and_bad():
    """私网/保留地址/999.1.1.1 这些永远连不通, 不许进池。"""
    import collect
    bad = ["10.0.0.5:8080", "192.168.1.1:8080", "127.0.0.1:8080",
           "172.20.0.1:8080", "169.254.1.1:8080", "999.1.1.1:8080"]
    txt = "\n".join(bad)
    assert collect.proxy_list_nodes(txt, "t") == [], collect.proxy_list_nodes(txt, "t")
    n = collect._proxy_node("8.8.8.8", "99999", "http", None, None, "t")
    assert n is None, "端口越界还在"
    n = collect._proxy_node("8.8.8.8", "0", "http", None, None, "t")
    assert n is None, "端口 0 还在"


def t_proxy_list_cap_is_deterministic():
    """量大要控住, 但必须可复现 —— 随机抽的话今天涨了还是跌了说不清。"""
    import collect
    txt = "\n".join(f"{(i % 200) + 1}.{((i // 200) % 200) + 1}.{((i // 40000) % 200) + 1}."
                    f"{(i % 250) + 1}:{8000 + i % 1000}" for i in range(3000))
    a = [n["server"] + ":" + str(n["port"]) for n in collect.proxy_list_nodes(txt, "t")]
    b = [n["server"] + ":" + str(n["port"]) for n in collect.proxy_list_nodes(txt, "t")]
    assert len(a) == collect.PROXY_LIST_MAX, len(a)
    assert a == b, "两次调用结果不一致"


def t_looks_like_sub_ignores_web_links():
    """网页链接不该让采集器以为抓到了订阅。"""
    import collect
    assert not collect.looks_like_sub("https://t.me/abc/1\nhttps://example.com/a/b")
    assert collect.looks_like_sub("\n".join(f"{i}.{i}.{i}.{i}:8080" for i in range(2, 15)))


def t_derive_template_roundtrip_and_month_boundary():
    """反推的模板必须能原样渲染回去, 且月份/日期段是真变量。

    跨月是这类工具最典型的静默错误: 把 /2026/11/ 这段当成常量, 到了 12 月
    拼出来的还是 /2026/11/, 每天 404, 日志里看着像"源挂了"。
    """
    import datetime
    import discover_dated as dd
    t = dd.derive_template("https://node.clashnode.cc/uploads/2026/11/1-20261112.yaml")
    assert t, "带日期的 URL 必须能反推出模板"
    assert t["url"] == "https://node.clashnode.cc/uploads/{y}/{m}/{n}-{ymd}.yaml", t["url"]
    got = dd.render(t, datetime.date(2026, 10, 1), 1)
    assert got == "https://node.clashnode.cc/uploads/2026/10/1-20261001.yaml", got
    got = dd.render(t, datetime.date(2026, 3, 5), 0)
    assert got == "https://node.clashnode.cc/uploads/2026/03/0-20260305.yaml", got


def t_derive_template_keeps_width_and_ignores_query():
    """非补零的日期照抄宽度, query 里的日期不当变量。

    有的站是 /2026/9/1/, 补成 09 就连不上; 而 ?v=20261112 是缓存参数,
    把它当日期会让整条 URL 每天都变, 反补必定全 404。
    """
    import datetime
    import discover_dated as dd
    t = dd.derive_template("https://x.example.com/2026/9/1/nodes.yaml")
    assert t and t["width"]["m"] == 1 and t["width"]["d"] == 1, t
    assert dd.render(t, datetime.date(2026, 3, 5)) == "https://x.example.com/2026/3/5/nodes.yaml"

    q = dd.derive_template("https://x.example.com/a/20261112.txt?v=20261112")
    assert q and q["url"].endswith("?v=20261112"), q
    assert dd.render(q, datetime.date(2026, 1, 2)).endswith("a/20260102.txt?v=20261112")


def t_derive_template_rejects_what_it_cannot_replay():
    """没有日期、或日期只在域名里, 一律返回 None —— 不猜。"""
    import discover_dated as dd
    assert dd.derive_template("https://x.example.com/post/hello") is None
    assert dd.derive_template("https://2026.example.com/x") is None
    # 110 不是"11 号", 不能被 /{y}/{m}/{d} 吃掉
    t = dd.derive_template("https://x.example.com/2026/11/110/nodes.yaml")
    assert t and "{d}" not in t["url"], t


def t_derive_template_collapses_seq_variants():
    """同一天的多份文件必须收敛到同一个模板, 否则会被当成 N 个源各测一遍。"""
    import discover_dated as dd
    a = dd.derive_template("https://node.clashnode.cc/uploads/2026/11/0-20261112.yaml")
    b = dd.derive_template("https://node.clashnode.cc/uploads/2026/11/3-20261112.yaml")
    assert a and b and a["url"] == b["url"], (a, b)


def t_discover_verdict_gates():
    """闸门: 取不到 > 镜像 > 进管线。这三档决定了要不要写进 JSON。"""
    import discover_dated as dd
    assert dd.verdict({"files": 1, "raw": 500, "ratio": 0.9}) == "no"
    assert dd.verdict({"files": 3, "raw": 500, "ratio": 0.05}) == "mirror"
    assert dd.verdict({"files": 3, "raw": 500, "ratio": 0.50}) == "active"


def t_known_templates_covers_hardcoded():
    """硬编码的三条必须被认成"已在管线"。

    这条是防一个具体的误判: 一个源只要在采, 它的节点就在池子里, new/raw 必然
    接近 0, 光看闸门会判成"镜像"。把"已在管线"和"镜像"混为一谈, 结论就是
    建议删掉一个每天稳定供货的源。
    """
    import datetime
    import collect
    import discover_dated as dd
    known = dd.known_templates(datetime.date(2026, 11, 12))
    assert "https://xinshoucun.net/sub/clash-{ymd}.yaml" in known, known
    assert "https://node.clashnode.cc/uploads/{y}/{m}/{n}-{ymd}.yaml" in known, known


def t_load_dated_sources_merges_and_dedupes():
    """JSON 里只收 active 的; 与硬编码重复的模板不重复加; 硬编码排在最前。"""
    import collect
    d = tempfile.mkdtemp()
    p = os.path.join(d, "dated.json")
    old = collect.DATED_JSON
    try:
        with open(p, "w", encoding="utf-8") as f:
            json.dump({"sources": [
                {"name": "new1", "template": "https://a.example/x-{ymd}.yaml",
                 "days": 2, "status": "active", "seq": [0, 1]},
                {"name": "mirror", "template": "https://b.example/y-{ymd}.yaml",
                 "days": 3, "status": "mirror"},
                {"name": "dup", "template": collect.DATED_SOURCES[0][1],
                 "days": 5, "status": "active"},
                {"name": "bad", "template": "", "days": 3, "status": "active"},
            ]}, f)
        collect.DATED_JSON = p
        got = collect.load_dated_sources()
        heads = [e[1] for e in got[:len(collect.DATED_SOURCES)]]
        assert heads == [e[1] for e in collect.DATED_SOURCES], heads
        tpls = [e[1] for e in got]
        assert len(tpls) == len(set(tpls)), f"模板重复: {tpls}"
        assert "https://a.example/x-{ymd}.yaml" in tpls
        assert not any("b.example" in t for t in tpls), "mirror 不该进管线"
        entry = [e for e in got if e[1].startswith("https://a.example")][0]
        assert entry[2] == 2 and entry[3] == (0, 1), entry
    finally:
        collect.DATED_JSON = old


def t_load_dated_sources_survives_broken_json():
    """JSON 写坏/不存在时退回只用硬编码, 不能抛异常 —— 否则整轮采集全挂。"""
    import collect
    d = tempfile.mkdtemp()
    p = os.path.join(d, "bad.json")
    old = collect.DATED_JSON
    try:
        with open(p, "w", encoding="utf-8") as f:
            f.write("{ not json ")
        collect.DATED_JSON = p
        assert len(collect.load_dated_sources()) == len(collect.DATED_SOURCES)
        collect.DATED_JSON = os.path.join(d, "nope.json")
        assert len(collect.load_dated_sources()) == len(collect.DATED_SOURCES)
    finally:
        collect.DATED_JSON = old


def t_discover_filters_media_and_slug():
    """媒体文件和 slug 页必须挡在实测之前。

    这不是洁癖: 名额只有 10 个, 而一个 WordPress 站的首页能贡献 400 多条
    /2026/11/xxx.jpg。不挡的话名额全被头像吃掉, 真正像订阅的那条挤不进去 ——
    实测时就发生过, 前 10 个全是头像和裁剪图。
    """
    import discover_dated as dd
    assert dd.is_noise("https://b.example/wp-content/uploads/{y}/{m}/头像.jpg")
    assert dd.is_noise("https://b.example/{y}/{m}/{d}/2026-11-11%e8%8a%82%e7%82%b9-150%e4%b8%aa/")
    # 日期目录 + 纯数字文章 ID 也是永久链接, 回补只有当天那篇能取到
    assert dd.is_noise("https://b.example/{y}/{m}/{d}/1466/")
    assert not dd.is_noise("https://node.clashnode.cc/uploads/{y}/{m}/{n}-{ymd}.yaml")
    good = dd.score("https://x.example/sub/clash-{ymd}.yaml")
    bad = dd.score("https://x.example/wp-content/uploads/{y}/{m}/a.jpg")
    assert good > bad, (good, bad)


def t_hot_channels_subset_of_tg():
    """高频轮只跑 HOT_CHANNELS, 写错一个频道名就等于那一轮白跑且不报错。"""
    import collect
    hot, tg = set(collect.HOT_CHANNELS), set(collect.TG_CHANNELS)
    assert hot, "HOT_CHANNELS 空表 = 高频轮什么都不采"
    assert hot <= tg, f"HOT 里有不在 TG_CHANNELS 的: {hot - tg}"
    assert not (hot & set(collect.DEAD_CHANNELS)), "别把停更频道放进高频轮"


def t_merge_base_pool_handles_bad_input():
    """底池读不出来时返回 0, 不能抛。

    守的是一个具体的坑: 高频轮只采 5 个频道, 产出几百个。如果读不到现有
    nodes.json 却照写不误, 七千多个节点会被几百个覆盖 —— 订阅掉一大截,
    全流程不报任何错。
    """
    import collect
    d = tempfile.mkdtemp()
    pool: dict = {}
    assert collect.merge_base_pool(os.path.join(d, "nope.json"), pool) == 0
    assert pool == {}

    bad = os.path.join(d, "bad.json")
    with open(bad, "w", encoding="utf-8") as f:
        f.write("{ not json ")
    assert collect.merge_base_pool(bad, pool) == 0, "坏文件必须当作空池"
    assert pool == {}

    obj = os.path.join(d, "obj.json")
    with open(obj, "w", encoding="utf-8") as f:
        json.dump({"proxies": []}, f)          # 是 dict 不是 list
    assert collect.merge_base_pool(obj, pool) == 0
    assert pool == {}


def t_merge_base_pool_keeps_nodes_and_fills_fp():
    """正常列表要全部留住, 缺 fp 的补上, 已有 fp 的不重算。"""
    import collect
    d = tempfile.mkdtemp()
    p = os.path.join(d, "nodes.json")
    with open(p, "w", encoding="utf-8") as f:
        json.dump([
            {"server": "1.2.3.4", "port": 8080, "proto": "http"},
            {"server": "9.9.9.9", "port": 443, "proto": "vless", "fp": "FIXED"},
        ], f)
    pool: dict = {}
    assert collect.merge_base_pool(p, pool) == 2
    fps = set()
    for n in pool.values():
        assert n.get("fp"), "没 fp 的节点后续去重会失效, 同一节点会被反复计数"
        assert n.get("sources"), "缺 sources 会让这个源在报表里消失"
        fps.add(n["fp"])
    assert len(fps) == 2
    assert "FIXED" in fps, "已有 fp 不能被重算覆盖(会打乱历史指纹)"


def t_merge_base_pool_skips_junk():
    """空条目 / 缺 server 或 proto 的条目不能进池。

    缺 proto 尤其致命: fingerprint() 直接取 n['proto'], 会在载入底池这一步
    抛 KeyError, 表现像是网络故障, 排查方向全错。
    """
    import collect
    d = tempfile.mkdtemp()
    p = os.path.join(d, "nodes.json")
    with open(p, "w", encoding="utf-8") as f:
        json.dump([
            {}, {"server": ""}, {"port": 1}, "x", None,
            {"server": "5.6.7.8", "port": 80},                       # 缺 proto
            {"server": "5.6.7.9", "port": 80, "proto": "http"},      # 唯一好的
        ], f)
    pool: dict = {}
    assert collect.merge_base_pool(p, pool) == 1
    only = list(pool.values())[0]
    assert only["server"] == "5.6.7.9", only


TESTS = [(k[2:], v) for k, v in sorted(globals().items()) if k.startswith("t_")]
if __name__ == "__main__":
    print(f"边界自检: {len(TESTS)} 项\n")
    for name, fn in TESTS:
        check(name, fn)
    print(f"\n通过 {len(OK)} / {len(TESTS)}")
    if FAIL:
        print("\n失败项:")
        for name, e in FAIL:
            print(f"  - {name}: {type(e).__name__}: {e}")
        sys.exit(1)
    print("全部通过")
