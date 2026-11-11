# 已部署完成 —— 你只需要填一个订阅地址

**状态：云端已跑通。** 仓库 `baitmboop/mysub`，Actions 每天北京时间 04:00 自动运行，
结果提交回仓库。你的电脑和手机填同一个地址即可，跟本机开不开机无关。

---

## 你的订阅地址（复制这条）

**Clash / mihomo / Clash Verge / Stash / 小火箭（Clash 模式）**

```
https://gh-proxy.com/https://raw.githubusercontent.com/baitmboop/mysub/main/out/sub.yaml
```

**v2rayN / v2rayNG / Shadowrocket（Base64 通用格式）**

```
https://gh-proxy.com/https://raw.githubusercontent.com/baitmboop/mysub/main/out/sub-base64.txt
```

两个地址内容一样，只是格式不同。**按你的客户端挑一个，所有设备都填它。**

### 万一上面这个打不开，换这几个（实测都可直连）

| 顺序 | 地址前缀 | 实测 | 新鲜度 |
|---|---|---|---|
| ① 主用 | `https://gh-proxy.com/https://raw.githubusercontent.com/baitmboop/mysub/main/` | 1.0s / 200 | 分钟级（几小时内必新） |
| ② 备用 | `https://ghfast.top/https://raw.githubusercontent.com/baitmboop/mysub/main/` | 1.7s / 200 | 分钟级 |
| ③ 备用 | `https://cdn.jsdelivr.net/gh/baitmboop/mysub@main/` | 1.8s / 200 | 可能滞后 ≤12h |
| ④ 有代理时 | `https://raw.githubusercontent.com/baitmboop/mysub/main/` | 直连超时 | 分钟级 |

用的时候把前缀接上 `out/sub.yaml`（Clash）或 `out/sub-base64.txt`（Base64）。
比如备用 ② 就是 `https://ghfast.top/https://raw.githubusercontent.com/baitmboop/mysub/main/out/sub.yaml`。

> **为什么不用 github 原始地址？** `raw.githubusercontent.com` 在国内直连实测 **12 秒超时**
> （被墙）。而拉订阅这一步本身不能依赖代理——否则就是死锁（没订阅就连不上，连不上就拉不到订阅）。
> 所以必须走一个国内能通的转发。
>
> **为什么不主用 jsDelivr？** 它是正规 CDN、最稳定，但它把 `@main` 缓存最长 12 小时，
> 而且清缓存接口对同一文件约 1 小时只放 1 次（实测返回 `throttled`）。
> 主用它的后果是：**你可能拿到昨天那份**。上面 ①② 是实时回源的 GitHub 反代，
> 每次拉都是最新，代价是它们是社区服务、理论上可能失效——所以备着 4 个地址。

---

## 填进客户端

**电脑（Clash Verge / ClashX / mihomo 系）**

配置 → 新建 / 导入 → 粘贴上面的 URL → 下载 → 启用

**手机（Shadowrocket / Clash for Android / Stash）**

添加订阅 → 粘贴同一个 URL → 更新

导入后你会看到这几组：

| 组名 | 作用 |
|---|---|
| 总出口 | 实际生效的组，按 ①→②→③ 顺序自动降级 |
| ① 自建 | 现在是空的（你没填 VPS） |
| ② 免费额度 | 现在是空的（没填 WARP） |
| ③ 免费池 | 当前 55 个免费节点，按云端实测排序 |

**客户端自己会在 ③ 组里测速切换**，你不用手动挑。

---

## 之后每天会发生什么

```
北京时间 04:00   GitHub 启动一台 Linux 机器（你电脑关着也照跑）
      ↓
  采集 500~800 个公开节点（境外机房直连，不用翻墙）
      ↓
  延迟粗筛 + 三站复检（gstatic / github / cloudflare）
      ↓
  与历史池合并（7 天内可用过的都留着，防某天全灭）
      ↓
  提交 out/sub.yaml 回仓库 + 刷新 CDN 缓存
      ↓
  你的设备下次刷新订阅时自动拿到新节点
```

一次约 **2 分钟**。公开仓库的 Actions 额度**完全免费不限量**（私有仓库才有 2000 分钟/月限制，
这也是选公开的一个原因）。

> GitHub 的定时任务不保证准点，高峰期可能晚几十分钟。不影响使用——客户端一般几小时才拉一次订阅。

---

## 首次部署实测记录（2026-11-11）

| 项目 | 结果 |
|---|---|
| Actions 运行 | Run #1~#6 全部 **success**，稳定后每次约 **94 秒** |
| 节点数量 | 55 → 65 → 74 → 83 → 88 → **89 个**（历史池逐轮累积；本机同时段只测出 12 个） |
| 配置校验 | `mihomo -t` 通过 |
| 端到端 | gstatic 204 / GitHub 200 / YouTube 200 / x.com 200 |
| 出口 IP | 80.174.118.151 |

**为什么云端比本机多出 6 倍节点？** 因为 Actions 机器在美国机房，能直连的节点远多于国内网络。
"筛掉死的"这一项，机房环境比本机更准——这也是当初决定"云端只筛活、不测速"的另一个理由。

---

## 想改点什么

| 想做什么 | 怎么做 |
|---|---|
| 改运行时间 | `.github/workflows/daily.yml` 里 `cron: "0 20 * * *"`，注意是 **UTC**（北京 = UTC+8） |
| 多留节点 | 改工作流里 `ci_build.py --keep 40` 的 40，比如 60 |
| 加自建 VPS 节点 | 写进 `data/private.yaml`（参考 `data/private.yaml.example`），优先级最高 |
| 加 WARP / 机场 | 写进 `data/fallback.yaml`（参考 `data/fallback.yaml.example`） |
| 立刻更新一次 | Actions 页面 →「每日更新节点订阅」→ Run workflow |
| 只改文档不想跑构建 | `out/`、`data/`、`*.md` 的改动不会触发构建 |

改代码 push 后会自动跑一次（工作流里有 `push` 触发，但排除了产物路径，防止机器人提交导致无限循环）。

---

## 常见问题

**Q：拉不到订阅 / 地址打不开？**

先用浏览器打开那个 jsDelivr 地址。打不开就换 4G 网络再试，确认是本地 DNS 问题还是 CDN 节点问题。
真打不开就改用 raw 地址（需要代理）。

**Q：节点为什么老变？**

免费节点的本质。实测存活率 2~3%，每天换一批。订阅里的 55 个是"云端复检过、至少能通 2 个常用站"的，
但仍会持续死亡。客户端会自动切，切完就等明天更新。

**Q：什么时候能稳定？**

买一台 ¥10~15/月的 VPS，写进 `data/private.yaml`。之后总出口永远优先走它，免费池退化成第三道保险。
这是唯一真正的解法——免费节点的稳定性天花板就在这。

**Q：订阅里为什么没有端口 / 分流规则？**

订阅只提供节点和分组，端口、分流规则由你的客户端自己管理。这是标准做法，不是缺配置。

**Q：订阅会不会拿到旧的？**

主用的 gh-proxy / ghfast 是实时回源的 GitHub 反代，只有几分钟的分发延迟——
按天更新的场景完全感觉不到。

如果你换成了 jsDelivr，它有最长 12 小时缓存。工作流里加了"清缓存 + 回验"，
但 jsDelivr 对同一文件约 1 小时只允许清 1 次（实测会返回 `throttled`），
所以它只当兜底用。

**Q：怎么确认拿到的是最新版？**

打开订阅地址，第一行就是生成时间（北京时间）：

```
# nodepool  生成于 2026-11-11 12:52:31 (北京时间)
```

定时任务是每天北京 04:00 跑，所以正常情况下这里应该是**今天凌晨 4 点左右**。
如果还是前一天的，说明 CDN 滞后或者任务没跑成。

**Q：仓库是公开的，安全吗？**

订阅里全是公开免费节点，本来就在 GitHub 上人人可见。仓库里只有脚本，没有你的任何账号信息。
但如果以后要把自己 VPS 的节点加进 `data/private.yaml`，那就该改成私有仓库 + 令牌访问——
那时候告诉我，我给你换方案。

---

## 你需要保管的东西

- 仓库地址：`https://github.com/baitmboop/mysub`
- 订阅地址（主用）：
  `https://gh-proxy.com/https://raw.githubusercontent.com/baitmboop/mysub/main/out/sub.yaml`
- 订阅地址（备用）：
  `https://ghfast.top/https://raw.githubusercontent.com/baitmboop/mysub/main/out/sub.yaml`
- 本机推送用的 SSH 私钥：`nodepool/.ssh/id_ed25519`（已 gitignore，**别删**，删了要重新配）
