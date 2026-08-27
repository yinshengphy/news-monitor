# News Monitor (全球重大突发新闻监测服务)

基于多信源并发抓取、多级大模型智能研判（Gemini / Codex / DeepSeek）与可靠微信分发的自动化突发灾难雷达系统。

## ✨ 核心特性

- **多源并发采集**：内置 15 个全球主流通讯社、突发雷达与 USGS 地震源，采用多线程并发极速拉取（单轮耗时 < 3s）。
- **轻量前置拦截**：突发关键词快速过滤，非紧急资讯自动归档，极大降低大模型 Token 开销。
- **多级大模型研判链**：
  - L1: Gemini 3.7 Flash High
  - L2: OpenAI Codex (gpt-5.6-luna)
  - L3: Gemini 3.1 Pro Low
  - L4: DeepSeek V4 Flash (降级兜底)
- **同事件跨媒体防刷冷却**：基于地点与事件实体的 4 小时冷却窗口，杜绝不同媒体同质跟进发稿造成的消息轰炸。
- **Outbox 可靠投递队列**：结合 SQLite 状态机与指数退避重试，保证微信消息不因限流或网络抖动而丢失。
- **精美纯文本排版**：去除原始链接并统一标注来源时间，避免微信 Markdown 渲染异常。
- **存活健康监测**：支持 Watchdog 心跳文件与 Kubernetes `livenessProbe` 自动自愈。

## 🚀 部署运行

### 本地 / 容器运行
```bash
export CLIPROXY_API_KEY="your-cliproxy-key"
export DEEPSEEK_API_KEY="your-deepseek-key"
export WEIXIN_RECEIVER="your-wechat-id"

python3 monitor.py
```

### Kubernetes (K3s) 部署
参考 [k3s-infra](https://github.com/yinshengphy/k3s-infra) 仓库中的 `apps/news-monitor/` 配置。
