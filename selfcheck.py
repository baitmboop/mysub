#!/usr/bin/env python3
"""自动化流程的边界自检。

不需要网络, 不写真实 data/ 目录 —— 只调纯函数和临时文件, 验证异常输入
不会让每天的定时任务崩掉。跑法: python3 selfcheck.py
"""
from __future__ import annotations

import json
import os
import sys
import tempfile
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import gateway  # noqa: E402
import probe    # noqa: E402

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
