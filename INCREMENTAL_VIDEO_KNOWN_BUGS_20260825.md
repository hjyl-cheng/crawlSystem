# Incremental Video Known Bugs

记录日期：2026-08-25

修复前基线：`agent/incremental`，`1732210`

状态：问题 1 已在当前改动中修复；问题 2 仍待修复。

## 1. Unlisted 可能在来源合并后变成 Public（已修复）

优先级：P0

### 已复现现象

YouTube.js 已从 microformat 明确得到：

```text
access_status = unlisted
access_status_source = youtubejs_microformat
is_unlisted = true
```

真实视频 `yOz82lts4zc` 的 yt-dlp 原始结果同时包含：

```text
availability = unlisted
playability_status = OK
```

旧转换代码把 `OK` 当成 `public`，覆盖了更明确的 `availability=unlisted`。增量来源合并
还会把访问状态和来源字段分开选择，可能进一步拼出：

```text
access_status = public
access_status_source = youtubejs_microformat
is_unlisted = true
```

这既是字段自相矛盾，也丢失了 YouTube 的真实可见性事实。

### 根因与修复

- yt-dlp 转换现在优先采用明确的 `availability`，`playability_status=OK` 只表示能播放，
  不再代表一定是 Public。
- 增量合并把访问状态、来源、availability、privacy 和 `is_unlisted` 作为一组证据选择；
  Unlisted 与 Public 冲突时保留 Unlisted。
- 迁移详情使用 yt-dlp 补字段时采用相同的整组选择规则，不让后续 Public 覆盖 YouTube.js
  已明确识别的 Unlisted。
- Business Publication 的业务规则已改为：Unlisted 正常进入 Current 和业务快照；Private
  与 Unavailable 继续排除；Unknown 与 Login Required 继续视为未证实。
- 新 Revision 不再生成 `source_unlisted` 撤回；历史记录仍可读取，避免破坏恢复兼容性。
- 运行时 Projection Schema 和全新 Business Bootstrap 都允许保存 Unlisted。

### 回归覆盖

- yt-dlp 的 `availability=unlisted` 与 `playability_status=OK` 最终仍为 Unlisted。
- 增量与迁移真实入口都覆盖 YouTube.js=Unlisted、yt-dlp patch=Public；最终状态和来源整组
  保留 Unlisted，同时仍采用 yt-dlp 补齐的非访问字段。
- 增量首次发现将 Unlisted 按 Unlisted 写入 Crawler。
- Publication Current、Business Contract、Projection Adapter、Projection Schema、Reconciler
  和 Dead-letter Recovery 都允许 Unlisted 正常分发。
- Private 与 Unavailable 的排除行为有独立回归，未被本次规则修改。

### 上线数据要求

历史 Unlisted 行过去没有 `publication_item_hash`。部署代码和 Schema 后，需要通过现有的
`services/qybullmq/scripts/backfillVideoPublicationItemHashes.mjs` 安全脚本重算 hash，之后这些存量行才能进入
Business Current；不能伪造 hash 或直接修改历史 Revision。

## 2. AbortSignal 没有真正终止底层 yt-dlp

优先级：P1

### 现象

增量 Worker 已经把取消信号传给详情抓取和 Uploads 兜底，但共享 yt-dlp 适配器没有接收
或消费该信号：

- `fetchChannelUploads()` 只读取 `language`，忽略调用方传入的 `signal`。
- `fetchVideoYtDlpDetail()` 同样只读取 `language`。
- `ytdlpSession.js` 的持久会话请求只支持超时，不支持按信号取消正在执行的命令。

所以 Worker 丢失租约或任务被取消后，上层虽然会在请求返回时丢弃结果，底层 yt-dlp
仍可能继续运行：视频详情最长约 90 秒，Uploads 最长约 180 秒。

### 影响

- 通常不会让旧 Worker 写入数据，因为返回后还有取消检查和 fencing 门禁。
- 会继续占用 Worker、代理、持久 yt-dlp 会话和抓取吞吐量。
- 大量取消或租约丢失时，旧请求可能挤压新任务。

### 当前代码证据

- `services/qybullmq/src/incrementalVideo.js` 已向详情和 Uploads 调用传入 `signal`。
- `services/qybullmq/src/youtube.js` 的 `fetchChannelUploads()` 与
  `fetchVideoYtDlpDetail()` 尚未接收该参数。
- `services/qybullmq/src/ytdlpSession.js` 的 `request()` 尚无取消处理。

### 修复要求

- 给共享适配器增加可选 `signal`；迁移调用不传信号时，行为必须保持不变。
- 一次性 Python 子进程在取消时必须及时终止并返回原始取消原因。
- 持久 yt-dlp 请求取消时必须清理 pending、timer 和相关进程状态，不能留下悬挂请求。
- 取消后不得启动新的 fallback 或重试。
- 保留返回后的 fencing 检查，不能把底层取消当作唯一数据安全门禁。

### 必补测试

- `fetchChannelUploads()` 的真实适配路径收到取消后能及时结束，而不是等 180 秒超时。
- `fetchVideoYtDlpDetail()` 的真实适配路径收到取消后能及时结束。
- 持久会话取消后 pending 和 timer 被清理，下一次请求可以正常建立新会话。
- Worker 丢失租约后不再发起 fallback、重试或持久化结果。

## 已完成但不要混淆的问题

评论来源冲突已经修复：

- `aff97d6`：增量链路复核 YouTube.js 的“评论关闭”结论，并整组选择评论证据。
- `1732210`：迁移链路应用相同策略。

本文继续跟踪尚未修复的 AbortSignal 问题，并保留问题 1 的根因和验收记录。
