#!/usr/bin/env python3
"""按日归档站点的指纹发现与回补实测。

为什么要单独一个工具: DATED_SOURCES 里那三条模板全是人肉挖出来的 ——
看到一篇帖子、点进站、抄下 URL、把日期换成占位符。这种人肉活没法扩:
池子 81% 的节点来自两个源, 想加第三个就得有人再挖一次。

工具把它拆成三步, 每步都能单独验:

    1) 抠 URL        从帖子正文 / 站点页面里挑出**带日期**的 URL
    2) 反推模板      把日期片段换回占位符, **必须能原样渲染回去**(round-trip)
                     才算数 —— 不能还原的就是猜的, 一律丢
    3) 回补实测      按模板往前取 N 天, 只量三个数: 几份能取到、抽出几条、
                     其中几条是池子里没有的

第 3 步的 new/raw 是唯一的准入闸门, 专治"姊妹站共用同一个后端" ——
四个域名给你四份一模一样的文件, raw 很好看, new 是 0。这种加进去只
增加抓取时间和 404 日志, 一个节点都不多。

只有通过闸门的才写进 data/dated_sources.json, 由 collect.py 读取。
硬编码那三条永远保留: JSON 丢了不至于整段采集失效。

用法:

    python3 discover_dated.py --probe-template https://node.clashnode.cc/uploads/2026/11/1-20261112.yaml
    python3 discover_dated.py --posts data/tg_posts.txt --days 3
    python3 discover_dated.py --host xinshoucun.net --days 3
    python3 discover_dated.py ... --write          # 落盘到 data/dated_sources.json

    --probe-template  已知一条就行, 从它反推模板 (最容易验证的一条路)
    --posts           文本语料, 每行/每段里出现 URL 都会被扫 (TG 导出、X 帖子)
    --host            结构性嗅探: 首页 + sitemap + 最多 N 个站内页
"""

from __future__ import annotations

import argparse
import datetime
import hashlib
import json
import os
import re
import sys
import urllib.parse
import urllib.request

ROOT = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, ROOT)

import collect  # noqa: E402

OUT_JSON = os.path.join(ROOT, "data", "dated_sources.json")
POOL_JSON = os.path.join(ROOT, "data", "nodes.json")

NEW_RATIO_MIN = 0.10      # 新节点占比低于这个数 → 是镜像, 不进管线
FILES_OK_MIN = 2          # 只取到 1 份文件不足以判断 (可能是残留)
MAX_TEMPLATES = 10        # 单次运行最多实测几个模板, 防止一个站刷出上百个
PER_HOST_MAX = 4          # 同一个站最多占几个名额, 否则一个图片多的站挤掉全部

# 媒体/静态资源。它们的路径里也带 /2026/11/, 是最容易混进来的噪声 ——
# 实测一个 WordPress 站的首页能贡献 400 多条这种 URL, 把名额全占满。
MEDIA_EXT = (".jpg", ".jpeg", ".png", ".gif", ".webp", ".svg", ".ico", ".bmp",
             ".css", ".js", ".woff", ".woff2", ".ttf", ".eot", ".mp4", ".zip")
SUB_WORDS = ("clash", "node", "nodes", "sub", "subscribe", "v2ray", "proxy",
             "proxies", "yaml", "yml", "txt", "json", "conf", "free")
SUB_EXT = (".yaml", ".yml", ".txt", ".json", ".conf")

# 日期形态。顺序即优先级, 长的在前 —— 否则 20261112 会被 2026 先吃掉。
# 每个的 lookahead 都要求日期后面是路径/扩展名/结束, 这条约束是必需的:
# 没有它, /uploads/2026/11/1-20261112.yaml 里的 "2026/11/1" 会被误读成
# 2026-11-01, 反推出 {y}/{m}/{d}-{ymd} 这种鬼模板, 跨月直接失效。
_DATE_YMD = re.compile(r"(?<!\d)(20\d{6})(?!\d)")
_DATE_DASH = re.compile(r"(?<!\d)(20\d{2})-(\d{1,2})-(\d{1,2})(?!\d)")
_DATE_SLASH_D = re.compile(r"(?<!\d)(20\d{2})/(\d{1,2})/(\d{1,2})(?=[/.?#]|$)")
_DATE_SLASH_M = re.compile(r"(?<!\d)(20\d{2})/(\d{1,2})(?=[/.?#]|$)")

URL_RE = re.compile(r"https?://[^\s\"'<>()\[\]{}]+")
URL_SPLIT = re.compile(r"^(https?://[^/?#]+)([^?#]*)(\?[^#]*)?$")

SUB_HINT = ("clash", "node", "sub", "v2ray", "free", "yaml", "txt", "proxy")


# ------------------------------------------------------------------ 模板

def has_date(url: str) -> bool:
    m = URL_SPLIT.match(url or "")
    if not m:
        return False
    path = m.group(2)
    return any(p.search(path) for p in (_DATE_YMD, _DATE_DASH, _DATE_SLASH_D, _DATE_SLASH_M))


def derive_template(url: str) -> dict | None:
    """从一条带日期的 URL 反推模板。反推不出来就返回 None, 不做任何猜测。

    只处理 path: 域名里的数字不是日期 (`2026.example.com` 不该被换),
    query 里的也不处理 —— 那通常是缓存参数, 每天变的是时间戳不是日期。

    返回 {"url","width","anchor"}。anchor 是这条 URL 指向的日期, 用来做
    round-trip 校验: 用反推出的模板重新渲染 anchor, 必须一个字节都不差。
    """
    m = URL_SPLIT.match(url or "")
    if not m:
        return None
    head, path, tail = m.group(1), m.group(2), m.group(3) or ""

    got: dict[str, int] = {}          # 占位符 -> 匹配到的原文宽度
    tpl = path

    def take(mo, kind):
        if kind == "ymd":
            s = mo.group(1)
            if not _valid(int(s[0:4]), int(s[4:6]), int(s[6:8])):
                return mo.group(0)
            got.update({"y": 4, "m": 2, "d": 2, "ymd": 8})
            got.setdefault("anchor", (int(s[0:4]), int(s[4:6]), int(s[6:8])))
            return "{ymd}"
        if kind == "ym":
            got["y"] = len(mo.group(1))
            got["m"] = len(mo.group(2))
            got.setdefault("anchor", (int(mo.group(1)), int(mo.group(2)), 1))
            return "{y}/{m}"
        y, mth, d = mo.group(1), mo.group(2), mo.group(3)
        if not _valid(int(y), int(mth), int(d)):
            return mo.group(0)
        got.setdefault("anchor", (int(y), int(mth), int(d)))
        return _join(mo.group(0), mo, got)

    # 逐形态替换。占位符里没有数字, 所以后面的 pass 不会重复匹配同一段。
    tpl = _DATE_YMD.sub(lambda mo: take(mo, "ymd"), tpl)
    tpl = _DATE_DASH.sub(lambda mo: take(mo, "sep"), tpl)
    tpl = _DATE_SLASH_D.sub(lambda mo: take(mo, "sep"), tpl)
    tpl = _DATE_SLASH_M.sub(lambda mo: take(mo, "ym"), tpl)

    if "anchor" not in got:
        return None
    y, mo_, d = got.pop("anchor")

    # 同日多份文件的站点 (clashnode 的 {n}-{ymd}.yaml)。只认"路径段开头的
    # 1~2 位数字 + 连字符"这一种形态 —— 别的位置的数字含义太多, 猜错就是白请求。
    seg = tpl.rsplit("/", 1)[-1]
    lead = re.match(r"^(\d{1,2})-", seg)
    if lead:
        got["n"] = len(lead.group(1))
        tpl = tpl[:len(tpl) - len(seg)] + "{n}-" + seg[lead.end():]

    out = {"url": head + tpl + tail, "width": got}
    # round-trip: 还原不回去就是推错了, 宁可不要
    back = render(out, datetime.date(y, mo_, d),
                  int(lead.group(1)) if lead else None)
    return out if back == url else None


def _valid(y: int, m: int, d: int) -> bool:
    try:
        datetime.date(y, m, d)
        return 2000 <= y <= 2099
    except ValueError:
        return False


def _join(whole: str, mo, got: dict) -> str:
    """把 y-m-d / y/m/d 这种带分隔符的日期换成 {y}-{m}-{d} / {y}/{m}/{d}。

    分隔符从原文抄, 不假设是 `-` 还是 `/`: 2026.11.12 这种也见过。
    """
    y, m, d = mo.group(1), mo.group(2), mo.group(3)
    sep1 = whole[len(y):len(y) + 1]
    sep2 = whole[len(y) + 1 + len(m):len(y) + 2 + len(m)]
    got["y"] = len(y)
    got["m"] = len(m)
    got["d"] = len(d)
    return "{y}" + sep1 + "{m}" + sep2 + "{d}"


def is_noise(template: str) -> bool:
    """这条模板值不值得花请求去测。宁可错杀, 因为名额有限。

    两类必须挡:
      * 媒体/静态资源 —— 路径里的 /2026/11/ 只是上传目录, 和日期归档没关系
      * 永久链接 —— 末段是百分号编码的标题 (「2026-11-11节点更新-150个」, 那个
        「150个」每天都变), 或者末段是纯数字的文章 ID (/2026/11/12/1466/)。
        这两种都只有当天那一篇能取到, 拿它回补必然 404。
    """
    path = URL_SPLIT.match(template)
    p = (path.group(2) if path else template).lower()
    if p.endswith(MEDIA_EXT):
        return True
    if "%" in p:
        return True
    seg = [x for x in p.split("/") if x]
    return bool(seg) and seg[-1].isdigit()


def score(template: str) -> int:
    """排序用: 越像订阅地址越靠前。名额只有 MAX_TEMPLATES 个, 顺序就是全部。"""
    p = (URL_SPLIT.match(template).group(2) if URL_SPLIT.match(template) else template).lower()
    s = 0
    if p.endswith(SUB_EXT):
        s += 3
    if any(w in p for w in SUB_WORDS):
        s += 2
    if "/sub" in p or "/uploads/" in p:
        s += 1
    return s


def render(tpl: dict, day: datetime.date, n: int | None = None) -> str:
    """把模板按某一天渲染成 URL。pad 宽度照抄原文, 不统一补零 ——
    有的站是 /2026/9/1/, 补成 09 就连不上。"""
    w = tpl["width"]
    out = tpl["url"]
    out = out.replace("{ymd}", day.strftime("%Y%m%d"))
    out = out.replace("{y}", str(day.year).zfill(w.get("y", 4)))
    out = out.replace("{m}", str(day.month).zfill(w.get("m", 2)))
    out = out.replace("{d}", str(day.day).zfill(w.get("d", 2)))
    if "{n}" in out:
        if n is None:
            return ""
        out = out.replace("{n}", str(n).zfill(w.get("n", 1)))
    return out


# ------------------------------------------------------------------ 取数

def get(url: str, opener, timeout: int = 20) -> bytes | None:
    """静默取一页。404 是常态(回补时缺那天), 不能像 collect.fetch 那样打日志。"""
    req = urllib.request.Request(url, headers={"User-Agent": collect.UA})
    try:
        with opener.open(req, timeout=timeout) as r:
            return r.read()
    except Exception:
        return None


def urls_in(text: str) -> list[str]:
    return list(dict.fromkeys(URL_RE.findall(text or "")))


def host_pages(host: str, opener) -> list[str]:
    """首页 + sitemap + sitemap 子文件。sitemap 是站方自己公布的全站索引,
    比爬首页可靠 —— 首页常常只有最近几篇, 而且有的站首页干脆是拦截页。"""
    base = host if host.startswith("http") else f"https://{host}"
    pages = [base + "/", base + "/sitemap.xml"]
    body = get(base + "/sitemap.xml", opener, 15)
    if body:
        subs = [u for u in urls_in(body.decode("utf-8", "ignore")) if "sitemap" in u]
        pages += subs[:3]
    return pages


def harvest(host: str, opener, follow: int, budget: list[int]) -> list[str]:
    """从站点里收带日期的 URL, 允许跳一层: 首页/sitemap 里的站内页再取一次。

    为什么要跳一层: 节点站的首页通常只给文章链接 (xinshoucun 是 /10651 这种
    纯数字 ID, 不含日期), 真正带日期的下载地址在文章正文里。只扫首页会得出
    "这个站没有指纹"的错误结论。
    """
    found: list[str] = []
    pages = host_pages(host, opener)
    seen = set()
    for page in pages:
        if budget[0] <= 0:
            break
        budget[0] -= 1
        body = get(page, opener, 20)
        if not body:
            continue
        html = body.decode("utf-8", "ignore")
        for u in urls_in(html):
            if has_date(u):
                found.append(u)
            else:
                seen.add(u)

    # 一层跟跳: 挑站内、且 URL 里带订阅类关键词的页
    root = urllib.parse.urlsplit(pages[0]).netloc
    inner = [u for u in seen
             if urllib.parse.urlsplit(u).netloc == root
             and any(k in u.lower() for k in SUB_HINT)]
    for page in inner[:follow]:
        if budget[0] <= 0:
            break
        budget[0] -= 1
        body = get(page, opener, 20)
        if not body:
            continue
        for u in urls_in(body.decode("utf-8", "ignore")):
            if has_date(u):
                found.append(u)
    return list(dict.fromkeys(found))


# ------------------------------------------------------------------ 实测

def count_nodes(text: str, tag: str) -> list[dict]:
    """按 collect 的真实口径数节点: 先当 YAML, 不是就按 URI / 代理清单。"""
    nodes = collect.nodes_from_yaml(text, tag)
    if nodes:
        return nodes
    t = collect.maybe_b64(text)
    nodes = [x for x in (collect.parse_uri(u, tag) for u in collect.VG_REGEX.findall(t)) if x]
    nodes += collect.proxy_uri_nodes(t, tag)
    nodes += collect.proxy_list_nodes(t, tag)
    return nodes


def pool_fingerprints() -> set[str]:
    try:
        d = json.loads(open(POOL_JSON, encoding="utf-8").read())
    except Exception:
        return set()
    nodes = d if isinstance(d, list) else (d.get("nodes") or [])
    return {collect.fingerprint(n) for n in nodes if isinstance(n, dict)}


def probe(tpl: dict, days: int, seq: list[int] | None, opener,
          today: datetime.date, pool_fps: set[str], budget: list[int]) -> dict:
    """按模板回补 days 天, 量 files / raw / new。"""
    seqs = seq if (seq and "{n}" in tpl["url"]) else [None]
    st = {"tried": 0, "files": 0, "miss": 0, "dup": 0, "raw": 0, "new": 0,
          "sizes": [], "hits": []}
    seen_hash: set[str] = set()
    seen_fp: set[str] = set()
    for i in range(max(1, days)):
        day = today - datetime.timedelta(days=i)
        for n in seqs:
            if budget[0] <= 0:
                break
            url = render(tpl, day, n)
            if not url:
                continue
            budget[0] -= 1
            st["tried"] += 1
            body = get(url, opener, 15)
            if not body:
                st["miss"] += 1
                continue
            h = hashlib.sha1(body).hexdigest()
            if h in seen_hash:
                # 同一份内容换了个日期/序号 —— 转贴号, 或者模板里的日期不是真变量
                st["dup"] += 1
                continue
            seen_hash.add(h)
            nodes = count_nodes(body.decode("utf-8", "ignore"), "probe")
            if not nodes:
                st["miss"] += 1          # 200 但不是订阅 (拦截页/HTML 首页)
                continue
            st["files"] += 1
            st["sizes"].append(len(body))
            st["hits"].append(url)
            for nd in nodes:
                fp = collect.fingerprint(nd)
                if fp in seen_fp:
                    continue
                seen_fp.add(fp)
                st["raw"] += 1
                if fp not in pool_fps:
                    st["new"] += 1
    st["ratio"] = (st["new"] / st["raw"]) if st["raw"] else 0.0
    return st


def known_templates(today: datetime.date) -> set[str]:
    """已经在管线里的模板。

    必须单独识别: 一个源只要在采, 它的节点就在池子里, 于是 new/raw 必然接近 0,
    光看闸门会把它判成"镜像" —— 而它其实是每天稳定供货的那条。把"已知"和
    "镜像"混为一谈, 后果是有人会照这个结论删掉个好源。
    """
    out: set[str] = set()
    for entry in collect.DATED_SOURCES:
        name, tpl, days = entry[0], entry[1], entry[2]
        seq = entry[3] if len(entry) > 3 else None
        for u in collect.dated_urls(tpl, 1, today, seq)[:1]:
            t = derive_template(u)
            if t:
                out.add(t["url"])
    for s in load_json(OUT_JSON).get("sources", []):
        if s.get("template"):
            out.add(s["template"])
    return out


def verdict(st: dict) -> str:
    if st["files"] < FILES_OK_MIN:
        return "no"            # 取不到文件 / 只有一份
    if st["ratio"] < NEW_RATIO_MIN:
        return "mirror"        # 同后端镜像, 加进去一个节点都不多
    return "active"


# ------------------------------------------------------------------ 落盘

def load_json(path: str) -> dict:
    try:
        doc = json.loads(open(path, encoding="utf-8").read())
        if isinstance(doc, dict) and isinstance(doc.get("sources"), list):
            return doc
    except Exception:
        pass
    return {"updated": "", "sources": []}


def save_json(path: str, doc: dict) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(doc, f, ensure_ascii=False, indent=1)


def main() -> int:
    ap = argparse.ArgumentParser(description="按日归档站点指纹发现")
    ap.add_argument("--probe-template", nargs="*", default=[],
                    help="已知一条带日期的 URL, 从它反推模板并回补实测")
    ap.add_argument("--posts", nargs="*", default=[], help="文本语料文件 (TG 导出 / 帖子)")
    ap.add_argument("--host", nargs="*", default=[], help="站点域名, 结构性嗅探")
    ap.add_argument("--days", type=int, default=3, help="回补几天")
    ap.add_argument("--seq", default="", help="同日多份的序号, 如 0,1,2,3")
    ap.add_argument("--follow", type=int, default=6, help="--host 模式下最多跟几层站内页")
    ap.add_argument("--max-fetch", type=int, default=80, help="单次运行请求上限")
    ap.add_argument("--write", action="store_true", help="把通过的写进 data/dated_sources.json")
    ap.add_argument("--proxy", default=None)
    args = ap.parse_args()

    if not (args.probe_template or args.posts or args.host):
        ap.print_help()
        return 2

    opener = collect.build_opener(args.proxy)
    today = datetime.date.today()
    pool = pool_fingerprints()
    budget = [args.max_fetch]
    seq = [int(x) for x in args.seq.split(",") if x.strip().isdigit()] or [0, 1, 2, 3]

    print(f"今日 {today}  池内指纹 {len(pool)}  回补 {args.days} 天")

    # 1) 收集候选 URL
    urls: list[str] = list(args.probe_template)
    for path in args.posts:
        try:
            text = open(path, encoding="utf-8", errors="ignore").read()
        except OSError as e:
            print(f"  ! 读不到 {path}: {e}")
            continue
        hit = [u for u in urls_in(text) if has_date(u)]
        print(f"  语料 {os.path.basename(path)}: {len(urls_in(text))} 个 URL, 其中带日期 {len(hit)}")
        urls += hit
    for host in args.host:
        got = harvest(host, opener, args.follow, budget)
        print(f"  站点 {host}: 收到带日期 URL {len(got)}")
        urls += got

    urls = list(dict.fromkeys(urls))
    print(f"候选 URL {len(urls)} 个")

    # 2) 反推模板 (同一个模板只测一次)
    tpls: dict[str, dict] = {}
    dropped = noise = 0
    for u in urls:
        t = derive_template(u)
        if not t:
            dropped += 1
            continue
        if is_noise(t["url"]):
            noise += 1
            continue
        tpls.setdefault(t["url"], t)
    print(f"反推出模板 {len(tpls)} 个 (无法还原 {dropped} 条, 噪声 {noise} 条)")

    if not tpls:
        print("没有可用模板。指纹只存在于帖子正文或文章页里, 首页往往什么都没有 ——"
              " 试试 --posts 喂一份帖子正文。")
        return 1

    # 名额有限, 顺序就是全部: 先按"像不像订阅地址"排, 再限制单站占比 ——
    # 否则一个图片多的 WordPress 站能用 400 条 /2026/11/xxx.jpg 挤掉所有别人。
    picked: list[tuple[str, dict]] = []
    per_host: dict[str, int] = {}
    for key in sorted(tpls, key=lambda k: (-score(k), k)):
        host = urllib.parse.urlsplit(key).netloc
        if per_host.get(host, 0) >= PER_HOST_MAX:
            continue
        per_host[host] = per_host.get(host, 0) + 1
        picked.append((key, tpls[key]))
        if len(picked) >= MAX_TEMPLATES:
            break
    if len(tpls) > len(picked):
        print(f"只实测前 {len(picked)} 个 (共 {len(tpls)} 个, 按订阅特征排序)")

    # 3) 实测
    known = known_templates(today)
    rows = []
    for i, (key, t) in enumerate(picked, 1):
        st = probe(t, args.days, seq if "{n}" in key else None, opener, today, pool, budget)
        v = "known" if key in known else verdict(st)
        flag = {"active": "✓ 进管线", "known": "· 已在管线",
                "mirror": "✗ 镜像", "no": "✗ 取不到"}[v]
        print(f"  [{i}] {flag:8} files {st['files']}/{st['tried']}  raw {st['raw']:5}"
              f"  new {st['new']:4} ({st['ratio']:.0%})")
        print(f"      {key}")
        if st["hits"]:
            print(f"      例: {st['hits'][0]}")
        if v == "known":
            continue          # 已在管线, 不重复记账
        rows.append({"name": _name(key), "template": key, "days": args.days,
                     "seq": seq if "{n}" in key else None,
                     "evidence": {k: st[k] for k in ("tried", "files", "miss", "dup", "raw", "new")}
                     | {"new_ratio": round(st["ratio"], 3)},
                     "status": v})

    if args.write:
        doc = load_json(OUT_JSON)
        old = {s.get("template"): s for s in doc["sources"]}
        for r in rows:
            old[r["template"]] = r
        doc["sources"] = list(old.values())
        doc["updated"] = datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
        save_json(OUT_JSON, doc)
        ok = sum(1 for r in rows if r["status"] == "active")
        print(f"\n写入 {OUT_JSON}: 本轮通过 {ok} 个, 累计 {len(doc['sources'])} 条")

    return 0


def _name(template: str) -> str:
    host = urllib.parse.urlsplit(template).netloc or template
    return host.replace("www.", "")


if __name__ == "__main__":
    sys.exit(main())
