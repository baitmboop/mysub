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

# YAML 订阅解析用。做成软依赖: 缺 pyyaml 时该分支直接跳过,
# 不让整个采集器因为一个可选格式而崩掉。
try:
    import yaml
except ImportError:  # pragma: no cover
    yaml = None

UA = ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36")

VG_REGEX = re.compile(
    r'(?:vmess|vless|trojan|ss|ssr|hysteria2|hy2|tuic)://[^\s"\'<>\\]+', re.I)

TG_CHANNELS = [
    # 还在更新的。
    "oneclickvpnkeys",   # 最新帖 2h, 87 个直出节点
    "vpnfail_v2ray",     # 最新帖 0.1h, 20 个
    "ssrsub",            # 最新帖 8h, 30 个
    "ripaojiedian",      # 最新帖 1.5h, 帖内是订阅地址
    "freefq",            # 频道停更但页面里挂的订阅地址仍在
    # 这两个和上面不是一类: 上面发"直出节点"(贴出来即死), 这两个发"机场免费试用订阅"。
    #   freeVPNjd  —— 每 1-2 小时一条新订阅, 供给不断, 是最稳定的新增来源。
    #                 token 一次性, 必须当轮用掉(实测十分钟后同一地址就 403)。
    #   go4sharing —— 频道已停更, 抓它主要是为了捞页面里的 GitHub 仓库(见 SUB_SOURCES)。
    "freeVPNjd", "go4sharing",
    # wxdy666 —— 2026-11-11 实测: 最新帖 4.7h, 直出 5 个节点 + 20 个链接,
    # 其中 8 份是有效订阅(最多一份 191 条), 一次贡献 221 个本池没有的节点。
    # 它同时发"直出节点"和"机场订阅", 两种都能吃, 是最肥的一类。
    "wxdy666",
]

# 已停更的频道, 不再请求。2026-11-11 逐个看了 t.me/s/ 页面里最新帖子的时间:
#   v2ray_configs_pool 792h / freev2rayconfig 28388h / getfreev2ray 27968h
#   proxy_config 35106h / clashnodes、nodefree、Jsnzkpg 连帖子块都抓不到
# 这 7 个每天仍被请求一次、每次返回"无直出节点", 纯噪音还拖慢整轮采集。
# 名单留着是因为频道可能复活 —— 一两个月后复查一次, 活着就挪回 TG_CHANNELS。
# 复查方法: t.me/s/<频道> 最新 datetime 距今 < 720h。
DEAD_CHANNELS = [
    "v2ray_configs_pool", "clashnodes", "nodefree", "freev2rayconfig",
    "getfreev2ray", "Jsnzkpg", "proxy_config",
]

# 频道里贴的"订阅地址"没有统一特征: `/sub`、`token=` 是常见形态, 但 `/s/xxx`、
# `/ip/xxx`、`:8001/xxx/yyy`、`?url=xxx` 同样指向整份订阅。
# 原来按关键词白名单过滤, freeVPNjd 里 16 个链接有 7 个就是这么被漏掉的。
# 改成"排除明显不是订阅的 + 抓回来验内容", 判断交给 looks_like_sub。
_NOT_SUB = (
    "t.me/", "telegram.me", "telegram.org", "telesco.pe",
    "github.com/", "githubusercontent", "gist.github",
    "youtube.com", "youtu.be", "twitter.com", "//x.com/", "facebook.com",
    "cdnjs", "jsdelivr", "unpkg.com", "bootcdn", "npmjs",
    "google.com", "gstatic", "googleapis", "cloudflare.com/ajax",
    ".png", ".jpg", ".jpeg", ".gif", ".svg", ".webp", ".css", ".js", ".ico",
)


def is_sub_candidate(u: str) -> bool:
    """这条链接值不值得抓一次。真正的判断在内容上, 见 looks_like_sub。"""
    if not u.lower().startswith("http"):
        return False
    return not any(k in u.lower() for k in _NOT_SUB)


# 外部追加清单。加源不用改代码 —— 用户随口报一个频道名或订阅地址,
# 往文件里追加一行, 下一轮采集就带上了, 也不会因为手误改坏主逻辑。
# 路径基于本文件定位, 不依赖运行时的工作目录。
_ROOT = os.path.dirname(os.path.abspath(__file__))
EXTRA_CHANNELS = os.path.join(_ROOT, "data", "extra_channels.txt")
EXTRA_SUBS = os.path.join(_ROOT, "data", "extra_subs.txt")


def load_extra_list(path: str) -> list[str]:
    """一行一个, # 开头是注释。文件不存在就返回空, 不报错。"""
    try:
        with open(path, encoding="utf-8") as f:
            return [ln.strip() for ln in f
                    if ln.strip() and not ln.strip().startswith("#")]
    except OSError:
        return []


def looks_like_sub(text: str) -> bool:
    """抓回来先验一眼, 免得把一堆推广页灌进解析器刷满日志。"""
    t = maybe_b64(text)
    return bool(VG_REGEX.search(t)) or ("proxies" in t and "server" in t)

RAW_SOURCES = [
    ("NoMoreWalls", "https://raw.githubusercontent.com/peasoft/NoMoreWalls/master/list.txt"),
    ("ermaozi", "https://raw.githubusercontent.com/ermaozi/get_subscribe/main/subscribe/v2ray.txt"),
    ("ermaozi_ss", "https://raw.githubusercontent.com/ermaozi/get_subscribe/main/subscribe/ss.txt"),
    ("ermaozi_trojan", "https://raw.githubusercontent.com/ermaozi/get_subscribe/main/subscribe/trojan.txt"),
    ("ripaojiedian", "https://raw.githubusercontent.com/ripaojiedian/freenode/main/sub"),
    ("freefq", "https://raw.githubusercontent.com/freefq/free/master/v2"),
    # 下面两个是体检转正的: 实测独立贡献 470 / 118, 和上面几个几乎不重合。
    # 原来的 18 个源全挤在同一个生态圈里互相转贴, 加多少都只是复制粘贴。
    ("Auto_proxy",
     "https://raw.githubusercontent.com/w1770946466/Auto_proxy/main/Long_term_subscription_num"),
    ("mahdibland",
     "https://raw.githubusercontent.com/mahdibland/ShadowsocksAggregator/master/Eternity"),
]

# 整份订阅型源 —— 一份地址背后是几十个节点, 而不是一条条 URI。
# 和 RAW_SOURCES 没有本质区别, 只是内容形态可能是 Clash YAML, 走 from_mihomo 转换。
SUB_SOURCES = [
    # 实测 23 个节点、国内 17 个能通(74%), 其中 14 个是现网没有的 http 代理类型。
    # 注意仓库最后更新停在 3 天前, 所以只当补充, 不当主力 —— 它哪天停了也不影响别人。
    ("go4sharing/sub", "https://raw.githubusercontent.com/go4sharing/sub/main/sub.yaml"),
]

# 归档型源 —— 仓库按天存一份, 旧的也还在。
#
# 为什么值得翻旧账: 这类源发的是**静态** http 代理(机场把出口用 http 反代出来),
# 不像 ss/vless 那样一天一换 IP。2026-11-11 实测: 只取最近 40 天归档, 得 155 个
# 不重复 http + 3 个 socks5; 把其中的 http 全测一遍, 45 个当场连通、8 个能传数据,
# 最快的 192.227.176.234 跑到 1221KB/s —— 比当时订阅里第一名(1130KB/s)还快。
# 只抓"今天那一份文件"等于每天扔掉 95% 的存量, 所以这里按天回补。
#
# {year}/{mm}/{dd} 由日期填, 路径不存在就 404, 当天空着不影响其它天。
#
# 注意这个仓库**不是每天都有文件**: 实测 2026 年 168 份 / 330 天, 而且有连续空缺。
# 所以不能只按"最近 N 天"回补(21 天只命中 5 份), 先问 GitHub 目录接口拿真实文件名,
# 拿到就按文件名取最近 take 份; 拿不到(限流/网络)才退回按日期猜。
#
# 2026-11-11 复盘: take 从 40 提到 200(全量 169 份)。
# 依据是"转化率"而不是"抽到多少条": 同一轮统计里 http 占采集池 6.4%,
# 却贡献了订阅里 27% 的可用节点(155 进池 → 11 个可用, 7.1%),
# 而 vless 占池 33% 只贡献 15%(0.7%), vmess 占池 12% 贡献 0%。
# 也就是说 http 这个协议的边际价值是 vless 的 10 倍,
# 而全池的 http 只来自这一个仓库 —— 它是唯一被验证过的"高转化"供给。
# 静态 http 代理不随日期失效, 三个月前那份今天照样能连, 所以全量回补是纯赚。
ARCHIVE_SOURCES = [
    ("go4sharing/arch", "go4sharing/sub", "2026", 200),
]

# 按日期命名的站点归档 —— 没有 GitHub 目录接口可问, 只能按模板猜。
#
# 来源: 2026-11-11 从 X 上一个日更账号(@RockHeavens)的帖子展开 t.co 落到两个站,
# 它们的订阅文件都带日期, 且**今天的地址可以推**:
#   xinshoucun.net/sub/clash-20261111.yaml   (WP 站, 每天一篇, 189~220 条, 与现池重合 119)
#   oss.oneclash.cc/2026/11/20261111.yaml    (14 条, 与现池重合 11, 属于转贴)
# 结构上是最值钱的那类(按天存盘、可回补), 所以按模板往前取几天。
#
# 元组: (标签, URL 模板, 往前取几天)。模板里 {ymd} {y} {m} 会被日期填上。
DATED_SOURCES = [
    ("xinshoucun", "https://xinshoucun.net/sub/clash-{ymd}.yaml", 3),
    ("oneclash", "https://oss.oneclash.cc/{y}/{m}/{ymd}.yaml", 2),
]


def dated_urls(template: str, days: int, today: datetime.date | None = None) -> list[str]:
    """把 URL 模板按"今天, 昨天, 前天…"展开。缺的那天会 404, 由调用方计 miss。

    `today` 只为测试注入, 生产走系统时间。
    """
    base = today or datetime.date.today()
    out = []
    for i in range(max(1, days)):
        d = base - datetime.timedelta(days=i)
        out.append(template.format(ymd=d.strftime("%Y%m%d"),
                                   y=d.strftime("%Y"), m=d.strftime("%m")))
    return out

# 博客型源 —— 节点不在 TG 帖子里, 在频道主自己的站上。
#
# 为什么需要这一类: t.me/s/ 页面对"文档附件"只给一个 https://t.me/<ch>/<id> 的
# 跳转链接, 文件本体要登录态才能下, 抓取侧拿不到。但同一个帖子正文里会带
# **站内文章 URL**, 而文章页是纯网页, 节点就写在里面。
#   @byxiaoxi 就是这种: 帖子只写"今日更新 150 个, 点击进入页面", 附件是 .txt,
#   正文链接指向 blog.banyunxiaoxi.icu/YYYY/MM/DD/...  —  实测文章页里直接有 URI。
#
# 元组: (标签, 首页地址, 文章 URL 正则, 最多取几篇)
# 只要首页能列出文章列表, 就不需要关心它哪天发、目录怎么组织。
BLOG_SOURCES = [
    ("tg:@byxiaoxi→blog",
     "https://blog.banyunxiaoxi.icu/",
     r"https://blog\.banyunxiaoxi\.icu/20\d\d/\d\d/\d\d/[^\s\"'<>]+",
     12),
]


def blog_urls(home: str, pattern: str, take: int, opener, timeout: int = 25) -> list[str]:
    """从一个站点的首页(或归档页)里列出最近 take 篇文章。

    首页通常按时间倒序排, 所以取前 take 个就是最近的。这里不做去重排序之外的
    任何假设 —— 有的站首页会把置顶/推荐混进来, 抓多了不影响结果(节点会去重)。
    """
    body = fetch(home, opener, timeout=timeout, retries=2)
    if not body:
        return []
    html = body.decode("utf-8", "ignore")
    urls = list(dict.fromkeys(re.findall(pattern, html)))
    return urls[:take]

# 候选源 —— 每轮都会采, 但只用来"体检", 决定要不要转成正式源。
# 判据是**独立贡献**(new): 抽到多少条节点里, 有多少是现有源没有的。
#   new ≈ 0  → 它和现有源高度重合, 或者只是同一个聚合器的镜像, 删掉;
#   new 可观  → 说明它有一手来源, 转正进 RAW_SOURCES。
# 只看 raw(抽出条数)会被骗: 一个转贴 NoMoreWalls 的仓库也能抽出几百条。
#
# 顺带一个坑: 这些地址大多托管在 raw.githubusercontent.com, 而国内直连它
# 经常是断的 —— 本地跑 collect.py 会整片超时, 不代表源有问题, 要在 CI 里看。
CANDIDATE_SOURCES = [
    ("aiboboxx/v2rayfree", "https://raw.githubusercontent.com/aiboboxx/v2rayfree/main/v2"),
    ("aiboboxx/clashfree", "https://raw.githubusercontent.com/aiboboxx/clashfree/main/clash.yml"),
    ("mfuu/v2ray", "https://raw.githubusercontent.com/mfuu/v2ray/master/v2ray"),
    ("Pawdroid/Free-servers", "https://raw.githubusercontent.com/Pawdroid/Free-servers/main/sub"),
    ("Barabama/FreeNodes", "https://raw.githubusercontent.com/Barabama/FreeNodes/main/nodes.txt"),
    # 这个源是 Clash YAML, 不是 URI 列表, 所以体检时"可达但抽出 0"。
    # 要吃到它得先给 ingest 加一条 YAML 解析分支, 光靠 VG_REGEX 抓不到。
    ("anaer/Sub", "https://raw.githubusercontent.com/anaer/Sub/main/clash.yaml"),
]


def cf_decode_email(hexstr: str) -> str:
    """解 Cloudflare 的邮箱混淆: 首字节是 XOR key, 其后逐字节异或还原。"""
    try:
        raw = bytes.fromhex(hexstr)
    except ValueError:
        return ""
    if not raw:
        return ""
    key = raw[0]
    return "".join(chr(c ^ key) for c in raw[1:])


def restore_cf_email(text: str) -> str:
    """把 <a data-cfemail="...">[email protected]</a> 换回原文。

    为什么必须做: Cloudflare 的 Email Obfuscation 会把页面里任何形如 a@b 的
    文本包成 <a class="__cf_email__" data-cfemail="…">[email protected]</a>。
    而节点 URI 的 `ss://<base64>@host:port`、`vless://<uuid>@host:port` 恰好含 @,
    于是 URL 被一个 <a> 标签拦腰截断, 正则只能匹配到协议头。
    实测 blog.banyunxiaoxi.icu 当天那篇"150 个节点": 不还原只抽出 29 个,
    还原后 150 个一个不少(29 个是不含 @ 的 vmess, 本来就没被混淆)。
    """
    if "data-cfemail=" not in text:
        return text
    return re.sub(
        r'<a[^>]*?data-cfemail="([0-9a-fA-F]+)"[^>]*>.*?</a>',
        lambda m: cf_decode_email(m.group(1)),
        text, flags=re.S)


def build_opener(proxy: str | None):
    if not proxy:
        # 必须显式传空 ProxyHandler: urllib 默认会读环境变量里的 http_proxy /
        # HTTPS_PROXY, 而这类环境变量常指向一个只在某种网络下有效的本地端口。
        # 踩过: 环境里 http_proxy=127.0.0.1:62694 失效, 所有抓取都变成
        # "Tunnel connection failed: 502", 看起来像源站挂了, 实际一个字节都没出去。
        return urllib.request.build_opener(urllib.request.ProxyHandler({}))
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


# 订阅作者常用字符串 'null' / 'none' 表示"没这个字段"(实测 go4sharing 那份就是
# username: 'null')。不清掉的话, 生成的链接会变成 https://null:null@host:port,
# 认证必然失败 —— 而且节点在测速时会"能用", 只是每次连接都被拒, 很难查。
_NULLISH = {"", "null", "none", "nil", "undefined", "-"}


def _clean(v) -> str:
    s = str(v if v is not None else "").strip()
    return "" if s.lower() in _NULLISH else s


def from_mihomo(p: dict, source: str) -> dict | None:
    """Clash / mihomo 的 proxy 条目 → 内部统一结构。

    免费订阅有一半是 YAML(`proxies:` 列表)而不是 URI 列表, 只认 `vmess://` 的
    正则会把整份订阅算成 0 条。go4sharing 那份实测 23 个节点、国内 17 个能通,
    在旧逻辑下就是这么被整份漏掉的。这里做反向转换, 让两种格式共用同一条
    去重 / 测速流水线 —— 谁上线只看测速名次, 跟来自哪种格式无关。
    """
    if not isinstance(p, dict):
        return None
    t = str(p.get("type") or "").lower()
    if t == "hy2":
        t = "hysteria2"
    server = str(p.get("server") or "").strip()
    try:
        port = int(p.get("port") or 0)
    except (TypeError, ValueError):
        return None
    if not server or not port:
        return None

    n: dict = {"proto": t, "server": server, "port": port,
               "name": str(p.get("name") or ""), "source": source}

    if t == "ss":
        n.update(cipher=_clean(p.get("cipher")).lower(),
                 password=_clean(p.get("password")))
    elif t == "vmess":
        n.update(uuid=_clean(p.get("uuid")),
                 alterId=int(p.get("alterId") or 0),
                 cipher=_clean(p.get("cipher")) or "auto",
                 tls=bool(p.get("tls")),
                 sni=_clean(p.get("servername")) or _clean(p.get("sni")),
                 network=_clean(p.get("network")) or "tcp")
    elif t == "vless":
        n.update(uuid=_clean(p.get("uuid")),
                 tls=bool(p.get("tls")),
                 sni=_clean(p.get("servername")) or _clean(p.get("sni")),
                 network=_clean(p.get("network")) or "tcp",
                 flow=_clean(p.get("flow")),
                 # 同 parse_uri: 这里是 uTLS 指纹, 不是节点指纹, 字段名不能叫 fp。
                 utls=_clean(p.get("client-fingerprint")))
        ro = p.get("reality-opts")
        if isinstance(ro, dict) and ro.get("public-key"):
            n["reality"] = True
            n["pbk"] = ro["public-key"]
            n["sid"] = ro.get("short-id") or ""
    elif t == "trojan":
        n.update(password=_clean(p.get("password")),
                 sni=_clean(p.get("sni")),
                 network=_clean(p.get("network")) or "tcp")
    elif t == "hysteria2":
        n.update(password=_clean(p.get("password")),
                 sni=_clean(p.get("sni")),
                 skipCertVerify=bool(p.get("skip-cert-verify")))
    elif t == "tuic":
        n.update(uuid=_clean(p.get("uuid")),
                 password=_clean(p.get("password")),
                 sni=_clean(p.get("sni")),
                 alpn=p.get("alpn") or [])
    elif t in ("http", "socks5"):
        # 普通代理。允许无认证, 所以这里不校验 username/password。
        n.update(username=_clean(p.get("username")),
                 password=_clean(p.get("password")),
                 tls=bool(p.get("tls")),
                 sni=_clean(p.get("sni")),
                 skipCertVerify=bool(p.get("skip-cert-verify")))
    else:
        return None

    # 传输层参数。只对 ws / grpc 还原, 其余靠 network 默认值。
    ws = p.get("ws-opts")
    if isinstance(ws, dict):
        n["path"] = _clean(ws.get("path"))
        hdr = ws.get("headers")
        if isinstance(hdr, dict):
            n["host"] = _clean(hdr.get("Host") or hdr.get("host"))
    gr = p.get("grpc-opts")
    if isinstance(gr, dict) and gr.get("grpc-service-name"):
        n["network"] = "grpc"
        n["path"] = gr["grpc-service-name"]
    return n


def nodes_from_yaml(text: str, source: str) -> list[dict]:
    """把 Clash YAML 订阅解析成节点列表(未去重)。

    单独成函数而不是塞进 ingest 的闭包里 —— 这样自检可以直接喂畸形输入,
    验证格式错乱时是"返回空列表"而不是抛异常。自动流程里宁可少一批节点,
    不能整个采集任务挂掉。
    """
    # 先做廉价排除, 否则每份几千行的 base64 文本都要跑一遍 safe_load。
    if yaml is None or "proxies" not in text or "server" not in text:
        return []
    try:
        doc = yaml.safe_load(text)
    except Exception:
        return []
    if not isinstance(doc, dict):
        return []
    items = doc.get("proxies")
    if not isinstance(items, list):
        return []
    out = []
    for p in items:
        n = from_mihomo(p, source)
        if n:
            out.append(n)
    return out


def fingerprint(n: dict) -> str:
    key = f"{n['proto']}|{n['server']}|{n['port']}|{n.get('uuid') or n.get('password') or ''}"
    return hashlib.sha1(key.encode()).hexdigest()[:16]


def archive_urls(repo: str, subdir: str, take: int, opener) -> list[str]:
    """列出归档目录里的文件, 返回最近 take 个的下载地址。

    优先用 GitHub 目录接口: 一次请求就能拿到真实文件名, 而按日期猜要发几十次
    404 —— 这个仓库一年只有 168 份文件, 空缺比文件还多。
    接口失败(限流/被墙)就退回按日期逐天猜, 效果差些但不至于整段失效。
    """
    base = f"https://raw.githubusercontent.com/{repo}/main"
    api = f"https://api.github.com/repos/{repo}/contents/{subdir}"
    body = fetch(api, opener, timeout=20, retries=1)
    names: list[str] = []
    if body:
        try:
            items = json.loads(body.decode("utf-8", "ignore"))
            if isinstance(items, list):
                names = sorted(x["name"] for x in items
                               if str(x.get("name", "")).endswith((".yaml", ".yml", ".txt")))
        except (ValueError, KeyError, TypeError):
            names = []
    if names:
        return [f"{base}/{subdir}/{n}" for n in names[-take:]]

    out = []
    for d in range(take * 2):          # 空缺多, 多猜几天才凑得够
        day = datetime.date.today() - datetime.timedelta(days=d)
        out.append(f"{base}/{day.year}/{day.month:02d}-{day.day:02d}.yaml")
    return out


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

    def _put(n: dict, source: str, tgt: dict) -> bool:
        """入池或补记来源。返回是否算新节点。"""
        if not n or not n.get("server") or not n.get("port"):
            return False
        fp = fingerprint(n)
        if fp in tgt:
            srcs = tgt[fp]["sources"]
            if source not in srcs:
                srcs.append(source)
            return False
        n["fp"] = fp
        n["sources"] = [source]
        tgt[fp] = n
        return True

    def _ingest_yaml(text: str, source: str, tgt: dict) -> int:
        """YAML 订阅分支: proxies: [...] → 内部结构。"""
        items = nodes_from_yaml(text, source)
        for n in items:
            _put(n, source, tgt)
        return len(items)

    def ingest(text: str, source: str, target: dict | None = None, quiet: bool = False):
        # target 给候选源体检用: 传一份临时池进去, 节点只参与"是不是新的"这个判断,
        # 不会真的混进正式池 —— 否则每多一个候选源, 当天要测速的节点就多一批,
        # 而候选源里绝大多数是重复的。
        tgt = pool if target is None else target
        # 顺序很重要: 先解整体的 base64, 再还原 CF 邮箱混淆。
        # 反过来的话 base64 文本里不会有 data-cfemail, 但 HTML 页面解 base64 会失败。
        text = restore_cf_email(maybe_b64(text))
        found = VG_REGEX.findall(text)
        for uri in found:
            _put(parse_uri(uri, source), source, tgt)
        # 一条 URI 都没有 → 可能是 Clash YAML 订阅。
        # 有 URI 就不试 YAML: 那种文本跑 safe_load 又慢又必然不是 dict。
        extra = _ingest_yaml(text, source, tgt) if not found else 0
        stats[source] = stats.get(source, 0) + len(found) + extra
        if not quiet:
            print(f"    → 抽出 {len(found) + extra:4d} 条, 去重后累计 {len(tgt)}")
        return len(found) + extra

    print("[1/6] 公开订阅源")
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

    print("[2/6] 整份订阅 (含 Clash YAML)")
    # 外部清单: 用户随时追加, 不用改代码。
    extra_subs = []
    for u in load_extra_list(EXTRA_SUBS):
        try:
            tag = u.split("//", 1)[1].split("/", 1)[0]
        except IndexError:
            continue
        extra_subs.append((f"extra:{tag}", u))
    for name, url in SUB_SOURCES + extra_subs:
        print(f"  * {name}: {url}")
        body = fetch(url, opener, timeout=25, retries=2)
        if not body:
            print("    × 无响应")
            continue
        ingest(body.decode("utf-8", "ignore"), name)

    print("[3/6] 归档型源 (按天回补, 旧的静态代理照样活着)")
    for name, repo, subdir, take in ARCHIVE_SOURCES:
        urls = archive_urls(repo, subdir, take, opener)
        got = miss = 0
        for url in urls:
            body = fetch(url, opener, timeout=12, retries=1)
            if not body:
                miss += 1
                continue
            got += ingest(body.decode("utf-8", "ignore"), name, quiet=True)
        print(f"  * {name}: 取 {len(urls)} 份 (缺 {miss}), 共抽出 {got} 条")

    for name, tpl, days in DATED_SOURCES:
        urls = dated_urls(tpl, days)
        got = miss = 0
        for url in urls:
            body = fetch(url, opener, timeout=20, retries=1)
            if not body:
                miss += 1
                continue
            got += ingest(body.decode("utf-8", "ignore"), name, quiet=True)
        print(f"  * {name}: 取 {len(urls)} 份 (缺 {miss}), 共抽出 {got} 条")

    print("[4/6] Telegram 频道 (t.me/s/ 网页预览)")
    channels = list(dict.fromkeys(TG_CHANNELS + load_extra_list(EXTRA_CHANNELS)))
    for ch in channels:
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
        # 频道里贴的订阅链接也抓。机场试用订阅的 token 是一次性的 ——
        # 实测同一批 443 个节点的订阅, 十分钟后再抓就 403, 所以必须当轮用掉,
        # 不能存下来"以后再说"。
        links = [u for u in dict.fromkeys(re.findall(r"https?://[^\s\"'<>]+", joined))
                 if is_sub_candidate(u)][:12]
        hit = 0
        for u in links:
            sub = fetch(u, opener, timeout=15, retries=1)
            if not sub:
                continue
            body = sub.decode("utf-8", "ignore")
            if not looks_like_sub(body):
                continue
            hit += 1
            ingest(body, f"tg:@{ch}→sub")
        if links:
            print(f"    · 帖内链接 {len(links)} 个, 其中 {hit} 份是有效订阅")

    print("[5/6] 博客型源 (帖子里只有链接, 节点在频道主自己的站上)")
    for tag, home, pat, take in BLOG_SOURCES:
        urls = blog_urls(home, pat, take, opener)
        got = miss = 0
        for url in urls:
            body = fetch(url, opener, timeout=20, retries=1)
            if not body:
                miss += 1
                continue
            got += ingest(body.decode("utf-8", "ignore"), tag, quiet=True)
        print(f"  * {tag}: {len(urls)} 篇 (缺 {miss}), 共抽出 {got} 条")

    print("[6/6] 候选源体检 (只看独立贡献, 不看抽出条数)")
    cand: dict[str, dict] = {}
    for name, url in CANDIDATE_SOURCES:
        # 浅拷贝一份池子做沙盘: 新节点的 fp 只在沙盘里累积, 正式池不受影响。
        tmp = dict(pool)
        before = len(tmp)
        # 超时和重试都比正式源短: 候选源不该拖慢每天的正式采集。
        body = fetch(url, opener, timeout=12, retries=1)
        if not body:
            cand[name] = {"ok": False, "raw": 0, "new": 0}
            print(f"  * {name}: 无响应")
            continue
        got = ingest(body.decode("utf-8", "ignore"), name, tmp) or 0
        new = len(tmp) - before
        cand[name] = {"ok": True, "raw": got, "new": new}
        print(f"  * {name}: 抽出 {got} 条, 其中全新 {new} 个")

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

    # 源体检报告: 每次采集都留一份, 用来回答"哪个源还值得留着"。
    # 单独成文件而不是塞进 nodes.json —— 后者是构建的输入, 不该混进统计信息。
    report = {
        "generated_at": time.strftime("%Y-%m-%dT%H:%M:%S+08:00",
                                      time.gmtime(time.time() + 8 * 3600)),
        "total_nodes": len(nodes),
        "by_source": dict(sorted(by_src.items(), key=lambda x: -x[1])),
        "raw_hits": dict(sorted(stats.items(), key=lambda x: -x[1])),
        "candidates": cand,
    }
    rp = os.path.join(os.path.dirname(args.out) or ".", "sources_report.json")
    with open(rp, "w") as f:
        json.dump(report, f, ensure_ascii=False, indent=1)
    print(f"   源体检 → {rp}")


if __name__ == "__main__":
    main()
