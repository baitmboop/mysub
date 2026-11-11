#!/usr/bin/env python3
"""对照实验: 不同并发下同一批节点的存活率, 判断是否被并发误杀。"""
import json, os, random, signal, subprocess, sys, time
from concurrent.futures import ThreadPoolExecutor, as_completed
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import probe

nodes = json.load(open("data/nodes.json"))
tg = [n for n in nodes if n["source"].startswith("tg:")]
gh = [n for n in nodes if not n["source"].startswith("tg:")]
random.seed(11)
sample = random.sample(tg, min(60, len(tg))) + random.sample(gh, min(60, len(gh)))
random.shuffle(sample)
print(f"样本: TG来源 {min(60,len(tg))} + 订阅来源 {min(60,len(gh))} = {len(sample)}")

name_map, cnt = probe.build_config(sample)
print(f"内核可载入 {cnt} 个")

proc = subprocess.Popen([probe.MIHOMO, "-f", probe.CFG], stdout=subprocess.DEVNULL,
                        stderr=subprocess.STDOUT, preexec_fn=os.setsid)
try:
    assert probe.wait_ready(proc), "内核启动失败"
    for workers, retries in [(8, 1), (32, 2), (64, 2)]:
        alive, detail = 0, []
        t0 = time.time()
        with ThreadPoolExecutor(max_workers=workers) as ex:
            futs = {ex.submit(probe.delay_test, nm, 5000, retries): nm for nm in name_map}
            for f in as_completed(futs):
                nm = futs[f]
                med, jit = f.result()
                if med:
                    alive += 1
                    detail.append((nm, med))
        src = {}
        for nm, _ in detail:
            s = "TG" if name_map[nm]["source"].startswith("tg:") else "订阅"
            src[s] = src.get(s, 0) + 1
        print(f"workers={workers:2d} retries={retries}: 存活 {alive:3d}/{len(name_map)} "
              f"({alive/len(name_map)*100:.1f}%)  来源分布{src}  耗时{time.time()-t0:.0f}s")
        detail.sort(key=lambda x: x[1])
        print("        最快:", ", ".join(f"{n}={d}ms" for n, d in detail[:5]))
finally:
    try:
        os.killpg(os.getpgid(proc.pid), signal.SIGTERM)
    except Exception:
        pass
