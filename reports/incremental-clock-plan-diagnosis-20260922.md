# 增量 Clock Plan 失败与历史执行中诊断

诊断日期：2026-09-22。主要数据库采样 08:05–08:10 UTC（北京时间 16:05–16:10），随后完成离线事件校验和回执核对。全部生产访问为只读；没有重试、重放、修改数据、修改线上配置或部署。

## 结论与范围

- 历史累计 `failed` Plan：1,943。该数包含 9 月 10 日起的历史记录，不是今日失败数，也不是去重频道数。
- 这 1,943 个 Plan 对应的 BullMQ job 全部为 `failed`，没有 `active/waiting/delayed`；其中 Run 为 `running` 的 1,567 条是状态残留，不能据此判断仍有 Worker 执行。
- `plan_day < 2026-09-22` 且 Plan 为 `running/dispatched`：335 个，均为 active 且当前到期的频道。
- 其中 Plan `running` 306 个：Run 已完成 301 个、Run 已失败 5 个。
- 其中 Plan `dispatched` 29 个：Run 已完成 28 个，Run 残留 running 1 个、队列已失败。
- 若只统计用户所说的“多天前”（9 月 17–20 日）且 Plan 为 running，则为 210 个：Run 完成 207、失败 3。

## 1. 334 个旧 Plan 等待缺失前序事件

完整链路已通过数据库、队列与已部署解析器交叉验证：

1. 早期全量抓取生成 video baseline observation。生产者将 discovery 标为 `partial`，但字段中保留了 `list_end` 等会被消费者解释为完整覆盖的证据。
2. Feature 接收端拒绝该事件，报 `Discovery outcome disagrees with scan coverage`。校验发生在插入 inbox 之前，因此 inbox 中没有这个序号。
3. outbox `published` 只表示事件已入 BullMQ，不表示 Feature 已应用。转发器将 HTTP 422 视为 `UnrecoverableError`，不会继续自动重试。
4. 后续增量视频事件虽然完成采集，却进入 inbox `waiting_gap/sequence_gap`。Plan reducer 只统计 `applied` 事件，故 Plan 保持 running 或 dispatched。
5. 相同频道已有活动 Plan 时，调度器不创建新的活动 Plan。因此会阻塞后续日期的采集，超出 UI 显示问题的范围。

批量离线校验：334 个缺失前序事件，0 个通过，334 个全部报同一错误。

这些旧 Plan 的等待事件结果为：314 complete、15 partial、5 failed。修复必须按事实分别收尾，不能全部标为成功。

可核对的最小样本：

- Plan `0b63bce5-134c-5fdf-8254-3fe1dbb43152`，频道 `UCa-1jg37usOqsYbBBoEkJoQ`。
- 9 月 17 日 10:03:17 UTC 开始，10:03:51 Run 已 done；Plan 仍 running。
- video sequence 2 已到达，但等待 sequence 1。
- 缺失事件 `1ad04def-694c-4ec4-8128-3e65e9d16eb0` 生成于 9 月 9 日；Feature job failed，实际尝试 1 次，配置上限 8 次，错误为上述协议不一致。

生产源码对应：

- `services/qybullmq/src/baselineBundle.js:128`：baseline 完成判定依赖 `terminal_condition`，不完整时仍可能保留 `stop_reason=list_end`。
- `services/feature-engine/src/feature_engine/events.py:1117`：消费者按扫描覆盖字段判断 complete，与历史生产者语义不一致。
- `services/qybullmq/src/runFeatureRecalcRelay.js:22`：永久性校验错误直接转不可重试。
- `services/feature-engine/src/feature_engine/applier.py:235`：缺序事件等待；`:688`：仅 applied 结果参与 Plan 收尾。

修复方案：先统一生产/消费契约，对历史 baseline 提供有范围限制、保留真实 partial 语义的兼容处理，并用上述 334 个原始事件做回归。随后通过原接收链路重放原始缺序事件，让等待事件按顺序应用，重新计算 Clock 与 Plan。不要跳过 sequence、伪造成功或仅修改前端展示。

注意：现有 `daily_plan_status_repair.py` 只检查 failed/partial Plan，且只根据已 applied 事件推导状态，不能直接解决这批 waiting_gap。

## 2. 另 1 个历史重试没有最终收尾

Plan `28fd0319-380d-51f1-8e06-e73f4b447f1f`，日期 9 月 16 日：

- 原 video failed 事件已于当天 04:06 UTC applied。
- Plan 最后更新于当天 12:39 UTC，目前为 dispatched。
- 此 Plan 在 `runtime/bug0916-retry/manifest.json` 的历史恢复名单中；该目录的恢复脚本会将 Plan 改回 dispatched 并增加一次队列尝试。
- 当前队列为 failed，实际/允许尝试为 6/6，最终错误 `timeout exceeded when trying to connect`，Run 仍为 running。

结论：是历史重试后的收尾遗漏，目前没有继续重试。应在确认无活动执行、无待完成 API/网络交接后，按已有失败事实一致地关闭 Plan 和 Run。

## 3. 1,943 个失败 Plan 的原因

按 Run/Domain 中持久化错误做互斥分类：

| 分类 | Plan 数 | 其中 Run 残留 running |
|---|---:|---:|
| 网络/TLS/代理连接或连接超时 | 816 | 775 |
| WHOLE_CHANNEL_DETAIL_MISSING | 760 | 697 |
| 频道删除、终止或封禁，已有 terminal_channel 证据 | 134 | 0 |
| content_candidates_run_id_position_key 冲突 | 128 | 0 |
| 其他错误或缺少更具体 Domain 错误证据 | 105 | 95 |
| 合计 | 1,943 | 1,567 |

上述是记录中的失败分类，不代表各类根因完全独立。例如详情缺失样本也经历了网络错误。

### 远程详情续跑（760）

错误在 `incrementalCoordinator.js:197` 触发：中心需要某视频的详情时，已有 whole-channel 回执中既无 captured detail、也无可抛出的 item.error，直接抛 `WHOLE_CHANNEL_DETAIL_MISSING`。

代表样本 Plan `e65aeaa1-0625-576a-b1ba-14065502b3a4` 的实际回执显示：

- 已完成的 first_seen/recent 项保留 `already_checkpointed`，数据库中对应 captured。
- 一个 recent 项经历代理/TLS 错误后进入 `api_pending`，之后数据库中已 captured。
- 后面的 5 个 recent 项仍为 pending，中心尝试继续读取这些未返回的详情时失败。

这是“远程部分结果/API 回补后，还存在需要网络采集的 pending 项”的续跑缺口。应显式区分 captured、api_pending、retryable、pending，将需要网络的剩余项交回合法的新执行租约继续采集，复用已捕获的 checkpoint；不能将未返回的详情视作全体已采集完成。该回执证明一个具体触发模式，不声称已经逐条证明全部 760 个 Plan 的触发模式完全相同。

### 候选位置唯一键冲突（128）

数据库错误已确认是 `(run_id, position)` 唯一性冲突。`incrementalYoutubeJsVideo.js:872` 的 upsert 按 `(run_id, source_content_id)` 处理冲突，却同时写入/更新 `position`；不同视频占用同一位置时仍会抛错。

修复应覆盖同一 Run 中发现、复查、断点恢复时的位置分配，确保位置稳定、互不冲突，或者在明确 position 业务含义后将扫描位置与稳定候选身份分开。需要真实冻结目标列表的回归测试；当前只证实约束冲突及受影响写入路径，未证明每一例具体由重排、追加复查还是重复输入造成。不建议直接删除约束或清空候选数据。

### 网络错误（816）

主要有 `SSLError curl_code=35`、`ProxyError curl_code=56`，以及连接超时；还有少量挑战/登录要求。应核对失败路线与代理健康，根据错误类型换合格网络身份、有界重试，并保留已经采集的进度。不能将永久频道删除混入网络重试。

此次未按具体代理、路由或握手链进一步归因，因此这里是已证实的失败类别，不是某一代理服务故障的完整根因结论。

### 永久频道状态（134）

这批已有 terminal_channel 证据的频道均在 Clock 中为 removed。属于有效终止，保留失败历史、停止继续调度即可，不应批量恢复抓取。

## 4. 自动重试实际行为与 running 残留

- 增量 Plan 投递默认最多 5 次尝试，指数退避起点 5 秒，带 jitter。原始实现见 `services/feature-dispatch/src/dispatchTransport.js:226`。
- 历史恢复操作对部分任务额外放开一次，因此存在 6/6。数字不是一直递增的无限重试机制。
- 频道删除等不可重试错误会提前结束。
- Rota 另有限制单 Business Run/Route 的执行预算。1,576 个 failed job 的最终失败原因是 `Rota Business Run budget exhausted`；这是最终预算状态，最初触发失败的细节仍需看 Domain/attempt 记录。
- 本次 1,943 个 failed Plan 对应队列全部 failed，不在重试等待中。
- `IncrementalRunStore.claim()` 会将非 done/waiting_agent 的旧 Run 改回 running；`proxyBusinessRun.js:273` 在网络执行准备之前调用它。随后在 Rota 预算等准备阶段失败，可能没有进入业务 runner 的正常 fail 收尾路径。
- `IncrementalChannelRunner` 对失败收尾写入使用 `.catch(() => {})`；远程事务又受 business fence 约束。因此必须把“已获得执行权但采集失败”和“准备/准入失败、没有执行权”分别正确收尾，不能放宽业务 fence 来掩盖状态不一致。

另一种正常情况是：旧 Plan 失败，但频道在之后日期重新生成了新 Plan。本次失败历史中有 1,574 条已经存在后续日期 Plan；这不改变旧 Plan 的历史结果，也不代表旧 job 仍在重试。

## 5. 建议实施顺序与验收

1. **优先修复事件契约与 334 个缺序事件**：契约回归通过后，经标准接收器重放前序事件；验证 waiting_gap 被顺序清空、Plan 按 complete/partial/failed 收尾、频道可以生成后续计划。单独关闭剩余 1 个旧恢复遗留。
2. **补齐失败终态一致性**：覆盖准入失败、Rota 预算耗尽、业务 fence 拒绝收尾、队列不可重试失败等路径；只在无活动执行和待处理交接时按持久证据修复 1,567 条 running Run。增加“队列终态与 Run/Plan 不一致”巡检。
3. **修复详情续跑和候选位置冲突**：分别用部分回执/API 回补/剩余 pending，以及同 Run 候选重放的位置冲突做集成回归；通过后再恢复受影响且尚未被新 Plan 覆盖的工作。
4. **处理可重试网络失败**：按健康网络身份、小批量有界恢复；永久删除频道保持 removed。面板增加真实失败原因和“等待事件/等待 API/重试等待”状态，避免笼统显示执行中。

验收不能只检查页面标签：需同时核对队列、远程 task、执行 attempt、Run、event inbox、Plan、Clock 后续调度。

## 取证文件

`runtime/clock-plan-diagnosis-20260922/`：

- `probe.mjs` / `probe.txt`：Plan/Run 初始统计与症状复现。
- `details.jsonl`、`forensics.jsonl`、`followup.jsonl`：事件缺序、错误分类、任务与执行尝试等只读证据。个别探索查询的超时/字段错误保留在原始记录中，后续成功查询已替代；报告不使用失败查询结果。
- `queues.mjs` / `queues.json`：异常 job 的实时状态、尝试次数及 Feature 失败样本。
- `missing-events.json`、`validate-events.py`、`event-validation.json`：334 个原始事件及已部署解析器的纯内存验证；334/334 拒绝同一契约错误。
- `sample-evidence.mjs` / `sample-evidence.json`：代表性详情缺失任务的已保存远程回执与 checkpoint 摘要。

本次完成诊断与修复方案；生产问题尚未实施修复。
