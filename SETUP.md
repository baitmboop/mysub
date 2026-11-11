# 三步搭好：让云端每天自动给你出订阅

目标：**电脑关机也不影响，手机和电脑填同一个地址。**

整套逻辑跑在 GitHub 的免费机器上，每天北京时间凌晨 4 点自动跑一次，
把筛好的订阅提交回仓库。你只需要做下面三步。

---

## 第 1 步：建仓库并推上去（3 分钟）

### 1.1 在 GitHub 建仓库

打开 https://github.com/new

- **Repository name**：随便起，比如 `mysub`
- **Private** ← 一定选这个（里面有你的节点密码）
- **不要**勾 "Add a README file"（会和推送冲突）

点 Create repository，页面会给你一段命令，先别管。

### 1.2 在本机终端跑这几行

```bash
cd /Users/ty-yc/WorkBuddy/2026-11-11-11-17-09/nodepool

git init
git add .
git commit -m "初始版本"
git branch -M main
git remote add origin https://github.com/你的用户名/mysub.git
git push -u origin main
```

把 `你的用户名` 换成你的 GitHub 用户名。

> **push 要密码时**：密码框里填的是 **Personal Access Token**，不是登录密码。
> 建法见第 3 步，也可以先用它。

> `bin/mihomo`（54MB 的内核）和临时文件已经被 `.gitignore` 排除了，
> 云端会自己下载 Linux 版，不会冲突。

---

## 第 2 步：给 Actions 写入权限（1 分钟）

私有仓库默认禁止 Actions 改文件，要开一下：

1. 进入你的仓库 → **Settings**
2. 左侧 **Actions** → **General**
3. 拉到最下面 **Workflow permissions**
4. 选 **Read and write permissions** → 点 **Save**

### 手动跑一次验证

1. 仓库顶部 **Actions** 标签
2. 左侧点「每日更新节点订阅」→ 右侧 **Run workflow** → 绿色按钮
3. 等 2~3 分钟，看是不是绿勾

绿了之后，仓库里的 `out/sub.yaml` 会变成新内容。

> 第一次跑如果报错，多半是第 2 步的权限没开，或者仓库没推送成功（`.github/workflows/daily.yml` 没上去）。

---

## 第 3 步：建一个令牌，拿到订阅地址（2 分钟）

仓库是私有的，直接访问文件需要凭证。

1. 打开 https://github.com/settings/tokens?type=beta
2. **Generate new token**
   - Token name：`sub-read`
   - Expiration：建议 90 天或 No expiration
   - **Repository access**：选 `Only select repositories` → 勾上你刚建的仓库
   - **Permissions** → 展开 `Repository permissions` → 找到 **Contents** → 改成 **Read-only**
3. 点 Generate，**复制那串 `github_pat_...`**（只显示一次）

### 你的订阅地址

Clash / mihomo / Stash：

```
https://你的GitHub用户名:github_pat_拿到的令牌@raw.githubusercontent.com/你的用户名/mysub/main/out/sub.yaml
```

v2rayN / Shadowrocket / v2rayNG（Base64 格式）：

```
https://你的GitHub用户名:github_pat_拿到的令牌@raw.githubusercontent.com/你的用户名/mysub/main/out/sub-base64.txt
```

把这两处占位符替换掉，然后：

- **电脑**：Clash 系客户端 → 配置 → 添加订阅 → 粘贴
- **手机**：同样粘贴这一个地址

**所有设备填同一个地址。** 以后换手机、出差、重装系统，还是填它。

---

## 之后每天会发生什么

```
北京时间 04:00   GitHub 自动启动一台 Linux 机器
      ↓
  采集 500~800 个公开节点（境外机房直连，不用翻墙）
      ↓
  延迟粗筛 + 三站点复检（gstatic / github / cloudflare）
      ↓
  与历史池合并（7 天内可用过的都留着，防某天全灭）
      ↓
  提交 out/sub.yaml 回仓库
      ↓
  你的手机、电脑下次刷新订阅时自动拿到新节点
```

耗时约 **2 分钟**。免费额度每月 2000 分钟，够跑 600 多次。

> GitHub 的定时任务**不保证准点**，高峰期可能晚几十分钟。这不影响使用——
> 客户端一般每 6~24 小时才拉一次订阅。

---

## 想改点什么

| 想做什么 | 怎么做 |
|---|---|
| 改运行时间 | `.github/workflows/daily.yml` 里的 `cron: "0 20 * * *"`，注意是 **UTC**（北京 = UTC+8） |
| 多留些节点 | 改 `ci_build.py --keep 40` 的 40，比如 60 |
| 加自建 VPS 节点 | 把节点写进 `data/private.yaml`（参考同目录 `.example`），它优先级最高 |
| 加 WARP / 机场 | 写进 `data/fallback.yaml` |
| 立刻更新一次 | Actions 页面点 Run workflow |

---

## 常见问题

**Q：客户端不认带令牌的地址怎么办？**

改用 Secret Gist：地址更干净（`https://gist.githubusercontent.com/...`），
但要以 gist 方式上传。告诉 WorkBuddy「改成 gist 模式」，会给你改好的方案。

**Q：为什么不测速了？**

云端机器在美国，它测出来的 1.5MB/s 对你在国内不作数。
**筛掉死的**由云端做（这一项云端比本机还准），**选最快的**交给你手机上的客户端
——它就在你本地，测的才是真实速度。这叫各干各的擅长的事。

**Q：节点还是不稳定的？**

这是免费节点的本质，不是配置问题。免费池实测存活率 2~3%，每天换一批。
要稳定就买台 ¥10~15/月的 VPS 写进 `data/private.yaml`，免费池自动退化成第三道保险。

**Q：令牌过期了怎么办？**

订阅拉不下来时重新生成一个，换掉地址里的令牌即可。建议设 90 天并记个提醒。

**Q：能把仓库改成公开的吗？**

可以，但 `out/sub.yaml` 里是节点密码，公开等于送人。真要公开，把令牌那套去掉、
改用随机文件名（`out/a7f3d9e2.yaml`）当"地址即密钥"，安全性靠不可猜。
