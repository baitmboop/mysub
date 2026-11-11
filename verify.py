#!/usr/bin/env python3
"""节点验证器 —— 三级筛选, 按协议走正确的测量通道。

为什么不能只信 sweep_cn
------------------------
2026-11-11 深夜实测, 同一个 http 节点、同一分钟:

    curl -x 直测    → code=000  (连不上)
    mihomo 内核测   → ok=1, 2.27KB/s

反过来, 订阅里 41 个节点 mihomo 报「全 0」, 换 curl 直测发现 http 有 2 个
真能用(330KB/s)。两边都在错, 而且错的方向相反。原因是 http 出站经过内核
多了一层改写, 而 curl 走的是协议原生交互。

所以这里的规矩是硬的:
    http / https / socks5  → curl 直测, 不许过内核
    ss / ssr / vmess / vless / trojan / hysteria2 / tuic → 内核(必须, 没法绕)

另外 `curl --noproxy '*' -x ...` 要绝对避免: --noproxy 会把 -x 盖掉,
测出来的是本机直连速度。这个坑已经踩过一次, 差点把 253KB/s 的直连当成节点速度。

三级
----
L1 tcp   连 server:port, 默认 1.5s。绝大多数死节点在这一步就被干掉, 成本极低。
L2 probe 通过节点请求 generate_204, 只回答「能不能翻墙」, 不看速度。
L3 speed 只对 L2 通过的做, 下 2MB, 回答「快不快」。

分级的理由: 全量 5922 个节点如果每个都直接测速, 一条 15 秒, 4 并发要 6 小时,
而且 95% 的时间花在早就死掉的节点上。先 cheap 后 expensive。

用法
----
    python3 verify.py --sub                 # 只验订阅, 出真实存活率
    python3 verify.py --pool --limit 200    # 小批试跑
    python3 verify.py --pool --resume       # 全池续跑(通宵用)
    python3 verify.py --report              # 只看已有结果
"""
from __future__ import annotations

import argparse
import json
import os
import shutil
import socket
import subprocess
import sys
import tempfile
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))

import gateway  # noqa: E402
import sweep_cn  # noqa: E402

OUT = ROOT / "data" / "verify.json"
# 内核的常驻工作目录: 只为了让 cache.db 能在多次运行间复用, 省掉每次冷启动。
# 因为探测是串行的, 同一时刻只有一个内核实例, 共享这个目录不会打架。
KERNEL_DIR = ROOT / "data" / "kernel_cache"

# 清掉环境里的代理变量 —— 本机 shell 里挂着 HTTP_PROXY=127.0.0.1:49844,
# 那是不走 VPN 的通道, 留着会让所有直测变成超时。
CLEAN_ENV = {k: v for k, v in os.environ.items()
             if k.lower() not in ("http_proxy", "https_proxy", "all_proxy")}

# sweep_cn 自己的 CLEAN 只清了**大写**的 HTTP_PROXY, 而本机 shell 里同时挂着
# 小写的 http_proxy / https_proxy = 127.0.0.1:49844(那是不走 VPN 的内部通道)。
# curl 会优先读小写, 于是通过内核发的每个请求都被导去那个死端口, 全部 000。
# 症状极具迷惑性: 内核进程明明活着、端口也通, 却每个节点都"探测失败"。
# 这里直接把它的环境覆盖掉, 一劳永逸 —— 下面所有 curl_via 调用都受影响。
sweep_cn.CLEAN = CLEAN_ENV

# 探测目标: 只要求 204/200, 不下载内容。两个都试, 一个通就算通
# (gstatic 在部分线路上被单独处理, 只测它会冤枉一些能用的节点)。
PROBE_URLS = [
    "https://www.gstatic.com/generate_204",
    "https://cp.cloudflare.com/generate_204",
]

# 测速目标。2026-11-11 实测: hetzner / tele2 / cachefly / thinkbroadband 从本机
# 全部 000 或 403, 只有 proof.ovh 稳定(直连 85KB/s)。CF 的 __down 会被限流,
# 放第二位兜底。
SPEED_URLS = [
    "https://proof.ovh.net/files/10Mb.dat",
    "https://speed.cloudflare.com/__down?bytes=2000000",
]
SPEED_BYTES = 2 * 1024 * 1024      # 速度判定的下限, 低于此视为"通但没用"
L3_TIMEOUT = 12

DIRECT_PROTOS = {"http", "https", "socks5"}
KERNEL_PROTOS = {"ss", "ssr", "vmess", "vless", "trojan", "hysteria2", "hy2", "tuic"}


def log(msg: str) -> None:
    print(msg, flush=True)


def key_of(n: dict) -> str:
    return f"{n.get('server')}:{n.get('port')}:{n.get('proto') or n.get('type')}"


# --------------------------------------------------------------------------- L1

def l1_tcp(n: dict, timeout: float = 1.5) -> bool:
    """TCP 能不能连上。域名解析失败也算死。"""
    host, port = n.get("server"), n.get("port")
    if not host or not port:
        return False
    try:
        with socket.create_connection((str(host), int(port)), timeout=timeout):
            return True
    except (OSError, ValueError):
        return False


# --------------------------------------------------------------------------- L2/L3

def _curl(args: list[str], timeout: int) -> tuple[float, str, float] | None:
    """返回 (速度KB/s, http_code, 首包毫秒)。

    必须带 `--noproxy '*'`: 本机环境里挂着 http_proxy=127.0.0.1:49844(不走 VPN
    的内部通道), curl 会优先读它, 把所有经内核的请求导去死端口。清环境变量不够
    保险(大小写两套), 显式 --noproxy 才彻底。注意在 macOS curl 上 --noproxy 与
    -x 同时给时 **-x 仍然生效**, 所以不会变成测直连。
    """
    out = subprocess.run(
        ["curl", "-s", "-o", "/dev/null", "-m", str(timeout), "--noproxy", "*", *args,
         "-w", "%{speed_download} %{http_code} %{time_starttransfer}"],
        capture_output=True, text=True, env=CLEAN_ENV).stdout.split()
    if len(out) < 3:
        return None
    try:
        return float(out[0]) / 1024, out[1], float(out[2]) * 1000
    except ValueError:
        return None


def _direct_proxy_arg(n: dict) -> list[str] | None:
    """给 curl 拼 -x / --socks5-hostname。http 类节点必须带认证占位账号:
    订阅里导出的 http 节点大多要求 Basic 认证, 不带账号会直接 407/000,
    而 CI 侧的 _surge_line 也是填 nodepool/nodepool 占位。"""
    proto = (n.get("proto") or n.get("type") or "").lower()
    host, port = n.get("server"), n.get("port")
    if proto in ("http", "https"):
        user = n.get("username") or "nodepool"
        pwd = n.get("password") or "nodepool"
        scheme = "https" if n.get("tls") else "http"
        return ["-x", f"{scheme}://{user}:{pwd}@{host}:{port}"]
    if proto == "socks5":
        return ["--socks5-hostname", f"{host}:{port}"]
    return None


def probe_direct(n: dict) -> dict:
    """http/socks5 直测: 先探通, 通了再测速。"""
    arg = _direct_proxy_arg(n)
    if not arg:
        return {"verdict": "skip"}
    for url in PROBE_URLS:
        r = _curl([*arg, url], 10)
        if r and r[1] in ("204", "200"):
            break
    else:
        return {"verdict": "dead", "stage": "probe"}

    best = 0.0
    for url in SPEED_URLS:
        r = _curl([*arg, url], L3_TIMEOUT)
        if not r:
            continue
        spd, code, _ = r
        if code in ("403", "429", "503", "502"):
            continue                      # 测速站限流, 与节点无关
        best = max(best, spd)
        if best >= 300:                   # 够快就不必再试下一个源
            break
    return {"verdict": "alive", "speed": round(best, 1), "via": "curl"}


def _kernel_probe(port: int) -> dict:
    """跑在一个只有该节点的内核实例上。

    注意: 这个函数**不能并发调用**。2026-11-11 实测, 同一个 ss 节点:
        串行       → 551KB/s
        4 实例并发 → 0.7KB/s
    差 800 倍。多个 mihomo 实例同时测速会互相拖垮 CPU 和带宽, 测出来的
    全是假低速。所以 deep_workers 默认 1。

    所有请求都走本文件的 _curl(带 --noproxy), 不要用 sweep_cn.curl_via ——
    后者没带 --noproxy, 会被本机 49844 那个代理劫走, 导致每个节点都"探测失败"。
    """
    via = ["-x", f"http://127.0.0.1:{port}"]
    lat = None
    for url in PROBE_URLS:
        for timeout in (5, 9):
            r = _curl([*via, url], timeout)
            if r and r[1] in ("204", "200"):
                lat = int(r[2])
                break
        if lat is not None:
            break
    if lat is None:
        return {"verdict": "dead", "stage": "probe"}

    best = 0.0
    for url in SPEED_URLS:
        r = _curl([*via, url], L3_TIMEOUT)
        if not r:
            continue
        spd, code, _ = r
        if code in ("403", "429", "503", "502"):
            continue
        best = max(best, spd)
        if best >= 300:
            break
    return {"verdict": "alive", "latency": lat, "speed": round(best, 1), "via": "kernel"}


def probe_kernel(n: dict) -> dict:
    """自己管内核, 不用 sweep_cn.with_kernel。

    为什么不能用现成的: with_kernel 在"端口可连接"的瞬间就发请求, 但 mihomo
    监听混合端口时节点还没初始化完 —— ws / reality 前置的节点要再等 2 秒以上
    才真正通。实测同一个节点 (43.174.195.1):

        端口一通就测        → 探测失败, 记成 kernel-fail
        等 4 秒再测         → 204, 1571KB/s

    这一条把 120 个好节点整体判成了"测不了"。所以这里自己做: 共享一个常驻
    工作目录(缓存 cache.db, 省掉每次冷启动), 端口通了再补一段就绪等待。

    注意 `proto` 必须还原成 Clash 的 `type`, 否则内核解析失败 —— 同一个坑犯过两次。
    """
    proxy = {k: v for k, v in n.items() if k != "proto"}
    proto = (n.get("proto") or n.get("type") or "").lower()
    if proto:
        proxy["type"] = proto
    proxy.setdefault("name", "node")

    # 每个节点用一个全新的工作目录。
    #
    # 试过共享一个常驻目录(想让 cache.db 复用、省掉冷启动), 结果是**连续探测时
    # 内核全部立刻退出**: 上一实例刚被 terminate, cache.db 的锁还没放开, 下一个
    # 就打不开缓存直接 exit —— 从外面看又是"kernel-exit", 和端口冲突的症状一样。
    # 宁可每次多花 0.5 秒冷启动, 也不要在这种地方猜。
    wd = Path(tempfile.mkdtemp(prefix="np_k_"))
    cfg_fd, cfg_name = tempfile.mkstemp(prefix="np_cfg_", suffix=".yaml")
    cfg_path = Path(cfg_name)
    port = _free_port()
    with os.fdopen(cfg_fd, "w") as f:
        f.write(json.dumps({
            "mixed-port": port, "allow-lan": False, "mode": "rule",
            "log-level": "silent",
            "proxies": [proxy],
            "proxy-groups": [{"name": "G", "type": "select", "proxies": [proxy["name"]]}],
            "rules": ["MATCH,G"],
        }))
    proc = subprocess.Popen([str(sweep_cn.MIHOMO), "-d", str(wd), "-f", str(cfg_path)],
                            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    try:
        # 给内核一段固定的启动+就绪时间, 然后直接发请求。
        # 不做 poll() 的"进程是否已退出"判断: 一个小节内退出的情形实测无法区分
        # 是真空转还是启动中的瞬间, 一旦误判就把好节点记成 kernel-exit。
        # 真起不来的话, 下面 curl 会直接拿到 000, 自然会判 dead。
        time.sleep(3.0)
        return _kernel_probe(port)
    except Exception as e:                                    # noqa: BLE001
        return {"verdict": "unknown", "err": str(e)[:60]}
    finally:
        proc.terminate()
        try:
            proc.wait(3)
        except Exception:                                     # noqa: BLE001
            proc.kill()
        try:
            cfg_path.unlink()
        except OSError:
            pass
        shutil.rmtree(wd, ignore_errors=True)


def _free_port(start: int = 28900, span: int = 200) -> int:
    """找一个真的能用的本地端口。

    不能用 bind(("127.0.0.1", 0)) 那种"让系统分配再立刻释放"的写法: macOS 的
    临时端口段是 49152-65535, 而本机 49844 就挂着一个系统代理, 释放出来的端口
    转眼会被别人拿走 —— 表现是内核起来后立刻退出(exit 0, 无报错), 被记成
    kernel-exit。实测拿到过 49868 这种端口, 全部失败。

    因为探测是串行的, 固定区间逐个试就够了。
    """
    for p in range(start, start + span):
        with socket.socket() as s:
            try:
                s.bind(("127.0.0.1", p))
                return p
            except OSError:
                continue
    raise RuntimeError("no free port")


def classify(n: dict) -> dict:
    proto = (n.get("proto") or n.get("type") or "").lower()
    if not l1_tcp(n):
        return {"verdict": "dead", "stage": "tcp"}
    r = probe_direct(n) if proto in DIRECT_PROTOS else probe_kernel(n)
    r.setdefault("via", "curl" if proto in DIRECT_PROTOS else "kernel")
    return r


# --------------------------------------------------------------------------- 输入

def load_nodes(args) -> list[dict]:
    if args.sub:
        import yaml
        proxies = yaml.safe_load((ROOT / "out" / "sub.yaml").read_text())["proxies"]
        return [dict(p, proto=p.get("type")) for p in proxies]
    if args.file:
        return json.loads(Path(args.file).read_text())
    history = gateway.load_json(gateway.HISTORY, {})
    pool = gateway.pool_nodes(history)
    return pool


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--sub", action="store_true", help="验当前订阅里的节点")
    ap.add_argument("--pool", action="store_true", help="验采集池")
    ap.add_argument("--file", help="从 JSON 文件读节点列表")
    ap.add_argument("--limit", type=int, default=0)
    ap.add_argument("--workers", type=int, default=6, help="TCP 阶段并发")
    ap.add_argument("--deep-workers", type=int, default=1,
                    help="内核阶段并发。必须保持 1: 并发多个内核实例会让"
                         "测速失真几百倍(实测 551KB/s → 0.7KB/s)")
    ap.add_argument("--direct-workers", type=int, default=8,
                    help="http/socks5 直测并发。走 curl 无内核, 不受上面的限制")
    ap.add_argument("--resume", action="store_true", help="跳过已有结论的节点")
    ap.add_argument("--report", action="store_true", help="只打印已有结果")
    ap.add_argument("--out", default=str(OUT))
    ap.add_argument("--apply", metavar="NODES_JSON",
                    help="把本轮 alive 的节点并入 cn_pref + pool_history, "
                         "参数是这些节点的原始 JSON(verify 结果里不带完整配置)")
    args = ap.parse_args()

    done: dict = gateway.load_json(Path(args.out), {})
    if args.report:
        summarize(done)
        return 0

    nodes = load_nodes(args)
    if not nodes:
        log("没有节点可验")
        return 1
    if args.resume:
        nodes = [n for n in nodes if key_of(n) not in done]
    if args.limit:
        nodes = nodes[: args.limit]

    log(f"待验 {len(nodes)} 个; 阶段1 TCP 并发 {args.workers}, 阶段2 并发 {args.deep_workers}")
    if args.deep_workers > 1:
        log("  ! 警告: 阶段2 并发 >1 会让测速严重失真(实测 551→0.7KB/s), 结果只可信 '通/不通'")

    # ---- L1: TCP。高并发, 大部分节点死在这里, 成本只有 1.5 秒。
    t0 = time.time()
    alive: list[dict] = []
    with ThreadPoolExecutor(max_workers=max(4, args.workers * 4)) as ex:
        for n, ok in zip(nodes, ex.map(l1_tcp, nodes)):
            if ok:
                alive.append(n)
            else:
                done[key_of(n)] = {"verdict": "dead", "stage": "tcp", "at": now()}
    log(f"  L1 TCP 通过 {len(alive)}/{len(nodes)}, 用时 {time.time()-t0:.0f}s")

    # ---- L2+L3: 协议探测 + 测速
    #
    # 分两组跑, 因为成本完全不同:
    #   direct(http/socks5) 走 curl 直连代理, 没有内核也没有中间层, 可以开到 8 并发;
    #   kernel(ss 系)       每个节点一个内核实例, **必须串行**, 并发会让测速掉几百倍。
    direct = [n for n in alive
              if (n.get("proto") or n.get("type") or "").lower() in DIRECT_PROTOS]
    kernelled = [n for n in alive
                 if (n.get("proto") or n.get("type") or "").lower() in KERNEL_PROTOS]
    log(f"  L1 通过分两组: direct {len(direct)} / kernel {len(kernelled)}")

    t0 = time.time()
    if direct:
        with ThreadPoolExecutor(max_workers=args.direct_workers) as ex:
            for n, r in zip(direct, ex.map(probe_direct, direct)):
                r["at"] = now()
                r["proto"] = n.get("proto") or n.get("type")
                r["server"], r["port"] = n.get("server"), n.get("port")
                done[key_of(n)] = r
        save(Path(args.out), done)
        log(f"    direct 组完成 {len(direct)} 个, 用时 {time.time()-t0:.0f}s")

    t0 = time.time()
    n_done = 0
    for n in kernelled:                       # 串行, 不并发
        r = probe_kernel(n)
        r["at"] = now()
        r["proto"] = n.get("proto") or n.get("type")
        r["server"], r["port"] = n.get("server"), n.get("port")
        done[key_of(n)] = r
        n_done += 1
        if n_done % 10 == 0 or n_done == len(kernelled):
            save(Path(args.out), done)
            log(f"    kernel 进度 {n_done}/{len(kernelled)}  {time.time()-t0:.0f}s")
    save(Path(args.out), done)
    summarize(done)
    if args.apply:
        apply_results(Path(args.apply), done)
    return 0


def now() -> str:
    return time.strftime("%Y-%m-%d %H:%M:%S")


def apply_results(nodes_json: Path, done: dict) -> None:
    """把实测可用的节点写进 cn_pref(实测结论) 和 pool_history(节点本体)。

    两步都要做, 少一步都没用:
      - 只写 cn_pref → ci_build 在 pool_history 里找不到这个节点, 加不了;
      - 只写 history → 云端探测仍然会把它筛掉, 因为国内好用的节点往往在
        Azure 上连着都费劲(见 ci_build 里的 _cn_ok 注释)。

    单位注意: cn_pref 的 speed 是 **MB/s**(历史约定, sweep_cn 也这么写),
    verify 内部一律用 KB/s, 这里必须换算, 否则筛选阈值会差 1024 倍。
    """
    nodes = json.loads(nodes_json.read_text())
    by_key = {key_of(n): n for n in nodes}

    pref = gateway.load_json(gateway.CN_PREF, {})
    hist = gateway.load_json(gateway.HISTORY, {})
    ts = int(time.time())
    added = updated = 0

    for key, r in done.items():
        if r.get("verdict") != "alive":
            continue
        node = by_key.get(key)
        if not node:
            continue
        endpoint = f"{node.get('server')}:{node.get('port')}"
        pref[endpoint] = {
            "ok": 1,
            "latency": r.get("latency"),
            "speed": round((r.get("speed") or 0) / 1024, 4),   # KB/s → MB/s
            "at": ts,
            "by": "verify",
        }
        if key in hist:
            hist[key]["last_ok"] = ts
            hist[key]["ok_hits"] = (hist[key].get("ok_hits") or 0) + 1
            updated += 1
        else:
            clean = {k: v for k, v in node.items() if k != "proto"}
            clean["type"] = node.get("proto") or node.get("type")
            hist[key] = {"first_seen": ts, "last_ok": ts, "ok_hits": 1,
                         "node": clean}
            added += 1

    gateway.save_json(gateway.CN_PREF, pref)
    gateway.save_json(gateway.HISTORY, hist)
    log(f"\n已写入: cn_pref {len(pref)} 条 / pool_history {len(hist)} 条 "
        f"(新增 {added}, 刷新 {updated})")


def save(path: Path, done: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".tmp")
    tmp.write_text(json.dumps(done, ensure_ascii=False, indent=1))
    tmp.replace(path)


def summarize(done: dict) -> None:
    if not done:
        log("还没有结果")
        return
    from collections import Counter
    v = Counter(r.get("verdict") for r in done.values())
    log(f"\n总计 {len(done)} 条: " + ", ".join(f"{k}={c}" for k, c in v.most_common()))
    live = [(k, r) for k, r in done.items() if r.get("verdict") == "alive"]
    live.sort(key=lambda x: -(x[1].get("speed") or 0))
    if live:
        log(f"\n可用 {len(live)} 个, 按速度取前 15:")
        for k, r in live[:15]:
            log(f"  {r.get('speed'):>8.1f}KB/s  {r.get('proto','?'):<9}{k}")
    slow = [(k, r) for k, r in live if (r.get("speed") or 0) < 5]
    if slow:
        log(f"\n另有 {len(slow)} 个'通但几乎跑不动'(<5KB/s), 建议降级")


if __name__ == "__main__":
    raise SystemExit(main())
