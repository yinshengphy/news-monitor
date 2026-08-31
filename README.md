# News Monitor (全球重大突发新闻监测服务)

基于多信源并发抓取、Gemini/Codex 主备研判与持久化微信队列的新闻和每日内容服务。

## ✨ 核心特性

- **多源并发采集**：内置 15 个全球主流通讯社、突发雷达与 USGS 地震源，采用多线程并发极速拉取（单轮耗时 < 3s）。
- **轻量前置拦截**：突发关键词快速过滤，非紧急资讯自动归档，极大降低大模型 Token 开销。
- **双模型研判链**：Gemini 3.7 Flash High 为主，OpenAI Codex `gpt-5.6-luna` 为备用；主切备、主备均失败及恢复均按状态变化告警。
- **同事件跨媒体防刷冷却**：基于地点与事件实体的 4 小时冷却窗口，杜绝不同媒体同质跟进发稿造成的消息轰炸。
- **中心化可靠投递队列**：SQLite WAL 持久化、单消费者严格串行、稳定 `client_id` 幂等重试。`-2` 会先排除过期上下文，再按 75 秒、3 分钟、10 分钟、30 分钟、最长 1 小时退避。
- **完整内容分块**：生成器显式输出消息边界；新闻条目、面试题章节和代码块不会从中间截断，每条消息保留在微信 2000 字限制以内。
- **每日内容调度**：北京时间 07:00 每日热点早报、07:30 高级面试题与 AI 应用层 GitHub 项目。
- **双通道新闻**：重大突发继续实时监控；全球热议在 08:00–22:00 的偶数整点聚合，X 仅作为发现信号，至少两个独立新闻发布者核验后才可推送，每日最多 3 次。
- **精美纯文本排版**：去除原始链接并统一标注来源时间，避免微信 Markdown 渲染异常。
- **存活健康监测**：支持 Watchdog 心跳文件与 Kubernetes `livenessProbe` 自动自愈。

## 🚀 部署运行

### 本地 / 容器运行
```bash
export CLIPROXY_API_KEY="your-cliproxy-key"
export WEIXIN_RECEIVER="your-wechat-id"
export DELIVERY_QUEUE_TOKEN="a-random-internal-api-token"

python3 delivery_service.py
```

`monitor.py` 通过 `DELIVERY_QUEUE_URL` 和 `DELIVERY_QUEUE_TOKEN` 把突发快讯交给投递服务，不再直接持有微信发送权限。

### Kubernetes (K3s) 部署
参考 [k3s-infra](https://github.com/yinshengphy/k3s-infra) 仓库中的 `clusters/home/apps/news-monitor.yaml`。
