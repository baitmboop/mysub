#!/usr/bin/env python3
"""
节点溯源与独占度分析 —— 回答"这个节点被多少人共用"。

思路: 单点无法知道真实并发人数,但可以测"扩散广度":
  一个节点被越多公开源同时收录 → 流传越广 → 越可能被万人骑。
反向, 只出现在 1 个源里的节点, 相对小众。

用法: python3 attribute.py [--nodes data/nodes.json] [--best out/nodes_best.json]
"""
import argparse
import collections
import ipaddress
import json
import os
import re

ROOT = os.path.dirname(os.path.abspath(__file__))

# 必然无效的地址段
JUNK_NETS = [ipaddress.ip_network(x) for x in (
    "127.0.0.0/8", "10.0.0.0/8", "172.16.0.0/12", "192.168.0.0/16",
    "169.254.0.0/16", "0.0.0.0/8", "::1/128", "fc00::/7", "fe80::/10",
)]
CF_PORTS = {2053, 2083, 2087, 2096, 8443}


def is_ip(s):
    try:
        ipaddress.ip_address(s)
        return True
    except ValueError:
        return False


def is_junk(server):
    try:
        ip = ipaddress.ip_address(server)
    except ValueError:
        return False
    return any(ip in n for n in JUNK_NETS)


def net24(server):
    try:
        return str(ipaddress.ip_network(server + "/24", strict=False))
    except ValueError:
        return None


def reg_domain(server):
    """近似取可注册域(不引第三方库, 处理常见双后缀)。"""
    if is_ip(server):
        return None
    parts = server.lower().split(".")
    two = {"co", "com", "net", "org", "gov", "edu", "ac"}
    if len(parts) >= 3 and parts[-2] in two:
        return ".".join(parts[-3:])
    return ".".join(parts[-2:])


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--nodes", default="data/nodes.json")
    ap.add_argument("--best", default="out/nodes_best.json")
    args = ap.parse_args()

    nodes = json.load(open(os.path.join(ROOT, args.nodes)))
    for n in nodes:
        n.setdefault("sources", [n.get("source", "?")])

    print(f"总节点 {len(nodes)}\n")

    # 1. 垃圾地址
    junk = [n for n in nodes if is_junk(n["server"])]
    print(f"1) 无效地址(回环/内网/保留): {len(junk)} 个")
    if junk:
        c = collections.Counter(n["server"] for n in junk)
        print(f"   {dict(c.most_common(5))}")
        print(f"   → 这些是别人机器上泄露的本地配置(如 127.0.0.53 是 systemd-resolved),"
              f"拿不到任何公网连接")

    clean = [n for n in nodes if not is_junk(n["server"])]

    # 2. 扩散广度: 跨源出现次数
    spread = collections.Counter(len(n["sources"]) for n in clean)
    print(f"\n2) 扩散广度(被几个公开源同时收录):")
    for k in sorted(spread):
        print(f"   出现在 {k} 个源: {spread[k]:4d} 个 ({spread[k]/len(clean)*100:.0f}%)")
    once = [n for n in clean if len(n["sources"]) == 1]
    print(f"   → 只在一处出现的「小众」节点占 {len(once)/len(clean)*100:.0f}%")

    # 3. 归属聚合
    seg = collections.Counter(net24(n["server"]) for n in clean if is_ip(n["server"]))
    dom = collections.Counter(reg_domain(n["server"]) for n in clean if not is_ip(n["server"]))
    seg.pop(None, None)
    dom.pop(None, None)
    print(f"\n3) 归属聚合: {len(seg)} 个 /24 网段 + {len(dom)} 个注册域 "
          f"承载 {len(clean)} 个节点")
    print(f"   平均每个网段 {len(clean)/max(len(seg),1):.2f} 个节点 "
          f"(值越接近 1 说明越分散, 越不像同一家机场批量开的小号节点)")
    top = seg.most_common(3)
    print(f"   最集中的网段: {[(s, c) for s, c in top]}")

    # 4. CDN 中转
    cf = [n for n in clean if n["port"] in CF_PORTS]
    print(f"\n4) Cloudflare 常用端口节点: {len(cf)} 个 ({len(cf)/len(clean)*100:.0f}%)")
    print(f"   → 端口落在 2053/2083/2087/2096/8443 的多为 CF 中转, 入口 IP 是共享的,"
          f"同一 IP 背后可能是成千上万个 node")

    # 5. 与实测结果交叉
    bestp = os.path.join(ROOT, args.best)
    if os.path.exists(bestp):
        best = json.load(open(bestp))
        print(f"\n5) 与实测入选项交叉 (n={len(best)}):")
        for n in best[:8]:
            s = n["server"]
            seg24 = net24(s) if is_ip(s) else reg_domain(s)
            share = sum(1 for x in clean if (net24(x["server"]) if is_ip(x["server"])
                                             else reg_domain(x["server"])) == seg24)
            print(f"   {n['proto']:9s} {s[:32]:32s} :{n['port']:<6d} "
                  f"同归属簇 {share:3d} 个  {n['speed']}MB/s")

    # 6. 推荐子集
    print(f"\n6) 推荐筛选口径:")
    print(f"   丢弃: 回环/内网地址 ({len(junk)} 个)")
    print(f"   降权: 出现在 ≥3 个公开源 ({sum(1 for n in clean if len(n['sources'])>=3)} 个)")
    print(f"   降权: CF 中转端口 ({len(cf)} 个)")
    print(f"   优先: 单源 + 非 CF 端口 + 独立网段 "
          f"({sum(1 for n in clean if len(n['sources'])==1 and n['port'] not in CF_PORTS)} 个候选)")


if __name__ == "__main__":
    main()
