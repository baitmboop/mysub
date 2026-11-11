#!/usr/bin/env python3
"""xsearch.py —— 不登录 X 的前提下, 找出"哪些 X 账号在发节点"。

## 为什么要这个工具

X 把内容全部关在登录墙后面: 直接请求 `x.com/<user>` 只拿到 46KB 的 JS 空壳,
`/search` 返回 307 跳登录, `api.x.com` 返回 401, syndication 接口按 IP 429。
实测记录见 RESEARCH.md 第五轮。

但**搜索引擎收录了 X 的正文摘要**, 于是可以绕开登录:
用 Brave 的 HTML 搜索 (服务端渲染, 不需要 key, 不拦脚本) 搜 `site:x.com <关键词>`,
从结果块里抠账号名和摘要。拿到账号之后, 再决定这个账号值不值得盯。

## 用法

    python3 xsearch.py                       # 跑内置的一组查询
    python3 xsearch.py -q "site:x.com 小火箭"  # 自定义查询
    python3 xsearch.py --json out.json        # 存原始结果
    NODEPOOL_PROXY=socks5://127.0.0.1:7897 python3 xsearch.py

代理默认取 `NODEPOOL_PROXY`, 没有就不走代理。
"""
import argparse
import html as htmlmod
import json
import os
import re
import subprocess
import sys
import time
import urllib.parse

UA = ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36")

# 内置查询: 覆盖中英两种表述, 以及"节点/订阅/机场试用"三类供给形态
QUERIES = [
    "site:x.com 免费节点",
    "site:x.com 小火箭 节点",
    "site:x.com clash 订阅 免费",
    "site:x.com v2ray 免费 节点",
    "site:x.com free v2ray nodes",
    "site:x.com 机场 免费试用 订阅",
]

# 这些路径是 X 的功能页, 不是账号
NOT_ACCOUNT = {
    "search", "i", "home", "explore", "settings", "login", "logout", "intent",
    "share", "hashtag", "status", "compose", "tos", "privacy", "signup",
    "messages", "notifications", "account", "about", "download", "jobs",
    "en",  # Brave 里 TOS/隐私政策的结果都挂在 x.com/en/ 下
}


def fetch(url, proxy=None, tries=3, timeout=20):
    """curl 拉页面。用 curl 而不是 urllib: 它原生支持 socks5 且能单独指定代理,
    不受环境里 HTTP_PROXY 的干扰(zsh 下变量分词也有坑, 所以参数一律明文写)。"""
    cmd = ["curl", "-sL", "--compressed", "-m", str(timeout),
           "-A", UA, "-w", "\n__CODE__%{http_code}", url]
    if proxy:
        scheme = proxy.split("://", 1)[0]
        flag = "--socks5-hostname" if scheme.startswith("socks") else "-x"
        cmd[1:1] = [flag, proxy]
    last = None
    for i in range(tries):
        try:
            out = subprocess.run(cmd, capture_output=True, timeout=timeout + 8).stdout.decode("utf-8", "ignore")
        except subprocess.TimeoutExpired:
            last = "timeout"
            continue
        body, _, code = out.rpartition("\n__CODE__")
        code = code.strip()
        if code == "200" and body:
            return body
        last = code or "empty"
        # 429 是"问得太勤", 立刻重试只会更糟; 其余情况(连接失败)1 秒后就够了
        time.sleep(20 if code == "429" else 1.2)
    print(f"    ! 拉取失败({last}) {url}", file=sys.stderr)
    return None


def strip_tags(s):
    s = re.sub(r"<script.*?</script>|<style.*?</style>", " ", s, flags=re.S)
    s = re.sub(r"<[^>]+>", " ", s)
    return re.sub(r"\s+", " ", htmlmod.unescape(s)).strip()


def parse_brave(page):
    """Brave 的结果块是 <div class="snippet ..." data-type="web">, 每块里第一个
    外链就是结果 URL, 文本部分是标题 + 摘要。按块切比全局正则准。"""
    out = []
    blocks = re.split(r'<div class="snippet[^"]*"', page)
    for b in blocks[1:]:
        b = b.split(">", 1)[1] if ">" in b else b  # 切点在 class 属性中间, 丢掉残尾
        m = re.search(r'href="(https?://(?:www\.)?(?:x|twitter)\.com/[^"]+)"', b)
        if not m:
            continue
        url = htmlmod.unescape(m.group(1)).split("?")[0]
        text = strip_tags(b)[:400]
        out.append({"url": url, "text": text})
    return out


def handle_of(url):
    m = re.match(r"https?://(?:www\.)?(?:x|twitter)\.com/([A-Za-z0-9_]{2,20})(?:/|$)", url)
    if not m:
        return None
    h = m.group(1)
    return None if h.lower() in NOT_ACCOUNT else h


# 一眼分辨"发节点的"和"卖机场的"。卖机场的也会带 v2ray/clash 这些词, 所以
# 只看后者会全判成供给; 用"推销话术"当判据反而准。
AD_WORDS = ("机场推荐", "低价", "性价比", "优惠", "折扣", "购买", "年付", "月付",
            "独家", "三块", "一元", "测评", "评测", "推荐")
SUPPLY_WORDS = ("免费节点", "节点更新", "订阅", "小火箭", "shadowrocket",
                "clash", "v2ray", "ssr", "trojan")


def classify(text):
    ad = sum(w in text for w in AD_WORDS)
    sup = sum(w in text for w in SUPPLY_WORDS)
    if ad > sup:
        return "广告/机场"
    if sup:
        return "疑似供给"
    return "不明"


def search(query, proxy, engine="brave"):
    q = urllib.parse.quote(query)
    url = {"brave": f"https://search.brave.com/search?q={q}",
           "bing": f"https://www.bing.com/search?q={q}"}[engine]
    page = fetch(url, proxy)
    if not page:
        return []
    rows = parse_brave(page) if engine == "brave" else []
    return rows


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("-q", "--query", action="append", help="自定义查询, 可重复")
    ap.add_argument("--engine", default="brave", choices=["brave", "bing"])
    ap.add_argument("--proxy", default=os.environ.get("NODEPOOL_PROXY", ""))
    ap.add_argument("--json", help="把原始结果写到这个文件")
    ap.add_argument("--min-len", type=int, default=0, help="摘要至少多少字才收")
    a = ap.parse_args()

    queries = a.query or QUERIES
    print(f"代理: {a.proxy or '直连'}   引擎: {a.engine}   查询 {len(queries)} 条\n")

    accounts = {}   # handle -> {hits, samples:[]}
    raw = []
    for q in queries:
        rows = search(q, a.proxy, a.engine)
        got = 0
        for r in rows:
            h = handle_of(r["url"])
            if not h:
                continue
            text = r["text"]
            if len(text) < a.min_len:
                continue
            got += 1
            key = h.lower()  # X 的账号名不区分大小写, RockHeavens 和 rockheavens 是同一个人
            e = accounts.setdefault(key, {"hits": 0, "samples": [], "kinds": [], "handle": h})
            e["hits"] += 1
            e["kinds"].append(classify(text))
            if len(e["samples"]) < 2:
                e["samples"].append(text[:160])
            raw.append({"q": q, **r, "handle": h})
        print(f"  {q:<42} 账号 {got}")
        time.sleep(1.5)  # 别把 Brave 打通了

    print(f"\n去重后候选账号 {len(accounts)} 个\n")
    supply = 0
    for h, e in sorted(accounts.items(), key=lambda kv: -kv[1]["hits"]):
        kind = max(set(e["kinds"]), key=e["kinds"].count) if e["kinds"] else "不明"
        if kind == "疑似供给":
            supply += 1
        print(f"  @{e.get('handle', h):<18} [{kind}]")
        for s in e["samples"][:1]:
            print(f"      {s[:140]}")
    print(f"\n其中疑似真供给 {supply} 个 / 广告机场 {len(accounts) - supply} 个")

    if a.json:
        with open(a.json, "w", encoding="utf-8") as f:
            json.dump(raw, f, ensure_ascii=False, indent=1)
        print(f"\n原始结果 -> {a.json}")


if __name__ == "__main__":
    main()
