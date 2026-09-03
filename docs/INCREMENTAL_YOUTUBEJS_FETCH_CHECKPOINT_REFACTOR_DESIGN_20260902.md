# Incremental YouTubeJS 新抓取脚本设计

## 1. 文档状态

- 状态：已实现并完成本地及隔离 PostgreSQL 验证；默认仍走 Legacy，待单 Worker canary
- 初版日期：2026-09-02
- 本次收缩：2026-09-03
- 实现日期：2026-09-03
- 目标代码：`services/qybullmq/src/incrementalYoutubeJsVideo.js`
- 替换对象：仅替换 Incremental Video executor，不修改旧 `incrementalVideo.js`

本稿取代此前把 Fetch、Publication Unit、Agent、Terminal Settlement、全局 Fact Freshness Fence 和 Rota Runtime 生命周期一起上线的方案。

本轮只增加一条新的 Incremental Video 抓取实现。Clock、Plan、Runner、About、Agent、Publication、业务数据库投递、Rota 和现有恢复控制器继续沿用当前行为。

### 1.1 当前实现快照

已落地内容：

1. 新增 `incrementalYoutubeJsVideo.js`，独立实现 YouTubeJS Scan、目标固化、串行 Detail、逐 Item 检查点和原子 Finalize；
2. 新增 Batch/Item 两张 PostgreSQL 表以及带显式目标库确认和现有 Incremental Run 数量门禁的迁移脚本；
3. YouTubeJS Detail 新增 opt-in 的 required-surface 严格模式，旧调用方默认行为不变；
4. Worker 新增 `INCREMENTAL_VIDEO_EXECUTOR` 装配开关，仅支持 `legacy` 和 `youtubejs_checkpoint_v1`，默认值为 `legacy`；
5. Runner 增加最小 lifecycle 续跑兼容：已提交 Video 的 dormant 结论可以恢复，合法 incomplete Partial 没有 lifecycle 结论时继续原有 Agent 行为；Runner 不感知 Batch/Item；
6. 新增纯逻辑、Schema、严格 Detail、检查点 PostgreSQL 和端到端 PostgreSQL 测试。

本地和隔离 PostgreSQL 已验证：Item CAS/过期 token、Phase 门禁、慢请求 claim 心跳、Parser gap、线路错误保持 `pending`、只重入 `pending`、Phase A/Phase B、Content Enrich reservation/fence、原子 Finalize，以及 finalized 重放不发网络请求且不重复写 Observation。

尚未完成：生产 Schema preflight/apply、排空旧 active Incremental Job、启用 `youtubejs_checkpoint_v1` 的单 Worker canary、完整 Clock 周期观测和扩大流量。因此本文不宣称已部署或已完成生产验收。

## 2. 一句话决定

新增一个与旧实现并存的 `incrementalYoutubeJsVideo.js`：它使用 YouTubeJS 完成 Uploads Scan 和 Video Detail，按视频把结果持久化到 PostgreSQL，线路异常时交回现有 Rota，恢复后只继续未完成视频，最后仍按当前契约完成 `recordVideoCycle` 等价收口。

旧 `services/qybullmq/src/incrementalVideo.js` 不修改、不导入、不包装，也不在本轮删除。

## 3. 本轮目标

1. Incremental About、Uploads Scan 和 Video Detail 的实际 YouTube 数据请求只来自 YouTubeJS；About 已经使用 YouTubeJS，本轮保持原实现；
2. 新增全新的 Incremental Video executor，不在旧脚本上继续叠加判断或 fallback；
3. 保留现有 Clock、Plan `task_mask`、Runner 顺序和 Task Domain 到期语义；
4. 保留现有 Uploads 分页、Anchor、Gap Abandonment、First-Seen、Disposition、Recent Sampling、Capacity 和 Lifecycle 算法；
5. Video Detail 先保持串行，一次只处理一个视频；
6. 每个视频形成可信结果后立即写入 PostgreSQL 检查点；
7. 普通 BullMQ retry、进程重启和换 IP 恢复同一个抓取周期，只继续 `pending` Item；
8. 现有受控恢复提交新 marker 后进入新的抓取周期，不能被旧周期结果短路；
9. 所有视频完成后，从数据库检查点重建现有 Finalization 输入；
10. Finalization 保留当前 Observation、Cursor、Crawler Outbox、Content、Lifecycle 和 Publication 行为；
11. YouTubeJS 有值时不得因为 Parser、线路或子请求错误被写成“字段为空”；
12. 新旧实现可以并存，Cutover 只修改 Worker 的 executor 装配。

## 4. 明确非目标

本轮不实现、也不得顺带接入以下能力：

- 频道级 Publication Unit；
- TaskPatchSnapshot、TaskComponent、DomainCandidateSnapshot 或 UnitVersion；
- About、Video、Agent 同 Plan 完成后合并为一次业务请求；
- 多频道批量发布；
- Publication command sequence 或 Fact Freshness Fence；
- Business `acceptUnit()`；
- Agent Request Fence、Agent cohort 或 Agent fan-out 重构；
- Channel/Agent TerminalSettlementIntent 新协议；
- Runner Execution Epoch；
- Rota Runtime Instance、Route Quiescence Set 或跨进程 drain 新协议；
- Video Detail 并发 10；
- 连续 3 次 Route Failure breaker；
- raw evidence 对象存储、WORM、Object Lock 或 evidence GC；
- Full Crawl、Migration、Query、Content Enrich 抓取链路改造；
- 当前业务库表结构、Revision 语义或投递协议改造；
- 清理或重构旧 `incrementalVideo.js`。

频道级合并发布仍是有价值的后续项目，但它不再是本轮新抓取脚本的依赖或验收条件。

## 5. 冻结的现有契约

以下能力是调用方已经依赖的契约，新脚本必须兼容，不能重新解释。

| 现有能力 | 本轮约束 |
|---|---|
| Feature Clock 计算到期 | 新脚本不判断 About、Video、Agent 是否到期 |
| Plan 携带 `task_mask` | Runner 仍只执行 `task_mask=true` 的 Domain |
| Runner 执行 `about -> video -> agent` | 顺序和 Agent gate 不变 |
| Uploads Anchor/Gap | 算法和结果解释不变 |
| First-Seen/recheck | 目标范围、ledger 和 disposition 语义不变 |
| Recent Sampling Planner | 输入字段、容量和选择算法不变 |
| incomplete Scan | 仍写现有 Partial，且不推进 Cursor |
| `recordVideoCycle()` | 最终写入集合和业务解释保持等价 |
| Publication Reconcile | 继续当前 per-domain reconcile |
| Agent | 请求、批处理和结果写入完全不变 |
| Rota | 继续消费当前失败类型并负责换线路 |
| 受控恢复脚本 | 三个现有脚本不修改，继续先写 marker 再 retry |
| 独立 Content Enrich | 继续使用当前 executor 和抓取实现 |

本轮所说的“保留”，是指相同输入下产生相同业务事实、相同 Cursor 结果、相同 Lifecycle 结论和相同 Publication 调用。检查点只改变中间结果存放位置，不增加新的业务结论。

## 6. 允许和禁止的代码改动

### 6.1 必须新增

- `services/qybullmq/src/incrementalYoutubeJsVideo.js`
- 新脚本对应的测试文件与 YouTubeJS fixture
- Incremental YouTubeJS Batch/Item 的 additive Schema migration

### 6.2 允许最小修改

- `services/qybullmq/src/worker.js`
  - 只增加新 executor import、配置选择和构造注入；
  - 不修改 Queue、Runner、Agent 或 Publication 流程。
- `services/qybullmq/src/incrementalChannelRunner.js`
  - 只允许保存和恢复 Video executor 已返回的 lifecycle 摘要；
  - 明确允许 incomplete Partial 没有 lifecycle 结论，并保持原有 Agent gate；
  - 不允许理解 Batch/Item、改变 Domain 顺序或重新判断 Clock 到期。
- `services/qybullmq/src/schema.sql`
  - 只追加本文件第 11 节定义的两张检查点表和约束。
- `database/bootstrap/crawler.sql`
  - fresh bootstrap 必须包含与运行时 DDL 完全一致的两张检查点表。
- `services/qybullmq/src/youtubeJs.js`
  - 只允许增加供新脚本使用的 strict required-surface 选项或新导出；
  - 旧调用方的默认行为必须保持不变；
  - 不加入 yt-dlp fallback。

### 6.3 明确禁止修改

- `services/qybullmq/src/incrementalVideo.js`
- `services/qybullmq/src/incrementalRunStore.js`
- `services/qybullmq/src/incrementalAbout.js`
- 所有 Incremental Agent 文件
- `services/qybullmq/src/publicationReconciler.js` 及业务投递代码
- 三个现有 Video 受控恢复脚本
- Rota 控制面和 BullMQ Job 生命周期协议

实现 PR 必须单独检查上述禁止文件没有 diff。

## 7. 新脚本的 Interface

新脚本是一个深模块。Runner 只知道一个与旧 executor 兼容的 Interface；Batch、Item、cycle key、恢复判断和 YouTubeJS 严格校验全部留在实现内部。

生产导出：

```js
export async function executeIncrementalYoutubeJsVideo({
  plan,
  runId,
  getChannelSnapshot,
  query,
  withTransaction,
  startedAt,
})
```

返回值必须保持旧 Runner 已消费的形状：

```js
{
  outcome: "complete" | "partial",
  observation_id: string | null,
  event_id: string | null,
  kind_sequence: number | null,
  first_seen_count: number,
  selected_count: number,
  lifecycle_status: string | null,
  dormant_recheck_day: string | null,
  duplicate?: boolean,
  reservation_cleanup_deferred?: boolean,
}
```

约束：

1. 不改变 `IncrementalChannelRunner` 的构造或执行协议；
2. 不把 Batch/Item 状态返回给 Runner；
3. 不要求 Job payload 增加字段；
4. 不读取 BullMQ progress 作为正确性事实；
5. 不导入 `./incrementalVideo.js`；
6. 允许继续调用已有稳定模块，例如 Planner、Observation Store、Publication Reconciler、YouTube failure policy 和 YouTubeJS Adapter；
7. 旧脚本中的私有业务逻辑由新脚本按现有测试和数据库行为重新实现，不能通过临时导出旧私有函数绕回旧实现。

测试可通过同文件导出的 factory 注入 YouTubeJS 和 PostgreSQL adapter；生产 Runner 仍只调用上面的 executor。

## 8. 总体流程

```text
Feature Clock / Plan / BullMQ / Rota ------------------------ 不变
                       |
                       v
IncrementalChannelRunner 按 task_mask 执行 ------------------ 不变
                       |
          +------------+-------------+
          |                          |
          v                          v
       About                      Video
  现有实现不变          executeIncrementalYoutubeJsVideo()
                                     |
                                     v
                     读取当前稳定 cycle_key
                                     |
                                     v
                     加载 PostgreSQL Batch/Item
                         |                    |
                   Batch 已存在          Batch 不存在
                         |                    |
                         |                    v
                         |          YouTubeJS Uploads Scan
                         |                    |
                         |          Anchor / Gap / 目标选择
                         |                    |
                         |                    v
                         +---------- 固化目标 Batch/Item
                                              |
                                              v
                          Phase A: First-Seen / recheck
                                              |
                                串行 YouTubeJS Detail
                                              |
                           每个结果一个短事务写检查点
                                              |
                              First-Seen ledger checkpoint
                                              |
                                              v
                               Phase B: Recent Sampling
                                              |
                                串行 YouTubeJS Detail
                                              |
                           每个结果一个短事务写检查点
                                              |
                                              v
                              全部可 Finalize 的 Item 齐备
                                              |
                              从数据库重建现有输入
                                              |
                                              v
                          现有 recordVideoCycle 等价原子收口
                                              |
                       Observation / Cursor / Outbox / Lifecycle
                       + 当前 per-domain Publication Reconcile
                                              |
                                              v
                                Runner 继续现有 Agent 流程
```

## 9. 逻辑抓取周期 `cycle_key`

### 9.1 为什么需要

BullMQ attempt、Worker 进程和 Route 都是传输执行身份，不是一次逻辑 Video 抓取周期的身份。

同一个逻辑周期内，IP 切换、BullMQ retry、Worker 重启和 Job stalled 后重投都必须命中同一个 Batch，并复用已提交的 Item。

但现有受控恢复脚本会显式要求同一 Run 再执行一轮 Video。新一轮不能复用上一轮已 finalized 的 Batch。

### 9.2 本轮的最小实现

本轮不增加全局 Video Recovery Generation，也不修改现有恢复脚本。新脚本只读取已经提交在 `channel_runs.result_json.controlled_recoveries` 下的 marker key。

```text
marker_keys = sort(Object.keys(controlled_recoveries || {}))

marker_keys 为空：
  cycle_key = "base"

marker_keys 非空：
  cycle_key = "recovery:" + sha256(canonical_json(marker_keys))
```

硬约束：

1. 只使用 marker 的对象 key；不得把 dispatch 状态、时间、BullMQ attempt 或错误消息放入 hash；
2. 每个可见 marker 都已经随恢复 prepare 事务提交；未提交数据对新脚本不可见；
3. 普通 retry 不添加 marker，因此 cycle key 不变；
4. 现有任一恢复脚本首次写入自己的稳定 operation marker 后，cycle key 改变；
5. Batch 一旦创建，保存自己的 cycle key，后续恢复只读该值；
6. `target_hash` 只用于校验目标集合，不是周期身份；
7. marker 结构缺失、不是对象或 key 与 marker 内的 operation identity 冲突时 fail closed；
8. 如果未来允许同一种受控恢复在一个 Run 内重复多次，必须另立设计升级 cycle identity；本轮三个恢复操作仍保持当前“一种操作每 Run 至多一次”的契约。

Observation idempotency key 固定为：

```text
video:${runId}:youtubejs:${cycleKey}
```

禁止使用 `executionAttemptId`、Route、Worker、时间或 target hash。

## 10. 详细执行流程

### 10.1 入口恢复

新脚本开始时：

1. 从 `channel_runs` 读取并验证 Run、Plan 和 committed recovery marker keys；
2. 计算当前 cycle key；
3. 加载 `(run_id,cycle_key)` Batch；
4. 若 Batch=`finalized`，返回其 `final_result_json`，不扫描、不请求视频；
5. 若 Batch=`fetching|ready`，使用其固化 Scan、目标和 Item 恢复；
6. 若 Batch 不存在，才进入 Uploads Scan；
7. 同一 key 出现多个 Batch、finalized Batch 缺 Observation ref、Item 集合与 target hash 不一致时 fail closed。

### 10.2 YouTubeJS Uploads Scan

仍通过 Runner 提供的 `getChannelSnapshot()` 调用 YouTubeJS `scanUploads()`。

扫描规则不改：

- 分页规则不改；
- Anchor 顺序和匹配不改；
- Catch-up 限额不改；
- Gap Abandonment 不改；
- Scan complete/Partial 的业务定义不改；
- 不调用 `fetchChannelUploads()`；
- 不调用 yt-dlp；
- 不允许 Item 级或页面级 fallback。

Scan 抛错，或返回 `stop_reason='pagination_error'` 时，先使用现有 `selectYoutubeFailure()` 语义分类：

```text
确认 Route Failure
  -> 不创建 Partial Observation
  -> 不推进 Cursor
  -> 不把线路失败写成“频道 Uploads 不完整”
  -> 原错误交给现有 Managed Worker / Rota

返回了可信 partial entries 的非线路 pagination incomplete
  -> 应用现有 Gap Abandonment
  -> 强制 complete=false，忽略冲突的 Adapter complete 标志
  -> 最终仍 incomplete 时走现有 Partial 语义
  -> 不推进 Cursor
```

如果 Scan 直接抛错且没有携带可验证的 normalized partial entries，则不创建 Batch 或 Partial Observation，原错误向外抛出；不能凭异常文本伪造一次 Uploads 扫描结果。

最终仍 incomplete 时不进入完整扫描的详情流程：只固化一个 `target_count=0` 的 ready Batch，保留本次 `scannedWork`、deferred 和 Partial Finalization 输入，不创建 Phase A/Phase B Item，也不执行 Recent Sampling。这必须与当前 `executeIncrementalVideo()` 的 incomplete 分支一致。

### 10.3 目标选择

新脚本必须在第一条 Video Detail 请求之前确定并固化两组目标：

```text
Phase A
  = 新发现视频
  + 到期的 disposition recheck

Phase B
  = 现有 Recent Sampling Planner 选中的存量视频
  - Phase A 已包含的视频
```

当前已经存在的 pending First-Seen ledger 是恢复输入，不是新的请求目标。新脚本加载它并在 Finalization 中认领，但不得因为恢复 ledger 再请求同一个视频。

必须保留 known video、due recheck、live/upcoming、player/next capacity、sampling score/window、Content Enrich reservation 和 First-Seen 优先级等现有规则。相同目标不能同时存在于两个 Phase。

Recent Sampling 的数据库读取、Planner 计算和 reservation claim 使用短事务。事务提交后才开始网络请求，禁止像当前实现一样持有 Content Enrich mode 锁等待全部 HTTP 完成。

同一 `(run_id,cycle_key)` 并发创建 Batch 时，数据库唯一约束决定唯一赢家。输家重新加载赢家 Batch，不能把自己计算的另一套目标混入。

### 10.4 固化 Batch 和 Item

Scan 和目标选择完成后，使用一个短事务：

1. 插入 Batch；
2. 插入 Phase A、Phase B 全部 Item；
3. 保存 Scan、Anchor、Planner 配置与结果；
4. 保存稳定 `cycle_observed_at`；
5. 保存 target hash；
6. 提交事务。

此事务不发网络请求。Batch 提交后不再重新 Scan、不再重新运行 Planner；恢复只读取 Item 状态并处理 `pending` Item。

### 10.5 串行详情抓取

本轮固定：

```text
detail_concurrency = 1
```

处理顺序固定为 Phase A、First-Seen ledger checkpoint、Phase B。不跨 Phase 并发，也不为每个视频创建 BullMQ Job。

单 Item 流程：

```text
短事务 claim pending Item
        |
        v
事务外调用 strict YouTubeJS Detail
        |
        +--可信 Detail / 明确内容状态
        |      -> 短事务写 captured
        |
        +--现有业务允许提交的非线路 Detail failure
        |      -> 短事务写 settled_error
        |
        +--Route Failure
        |      -> Item 保持 pending
        |      -> 等当前请求已经结束
        |      -> 向外抛原始分类错误
        |
        +--取消、进程退出或结果不确定
               -> Item 保持 pending
```

Item claim 返回不可复用的 `claim_token`。settlement 必须以 `(run_id,cycle_key,phase,video_id,claim_token,status='claimed')` 做 CAS；lease 过期后旧 Worker 即使晚到，也不得覆盖新 claim 或已提交结果。

网络请求期间以短事务续租当前 Item claim；续租同时维持这一 Batch 已持有的 Content Enrich reservation。续租失败或 claim 已丢失时立即取消当前请求并向外抛错，禁止继续用失效 token 提交结果。心跳间隔必须小于 Item lease，默认取 lease 的三分之一且不超过 60 秒。

Worker 重入时若发现同一 Batch 仍有未过期的 Item claim，不得把它记成一次 Job 失败并消耗 BullMQ retry。执行器在事务外做可取消的有界轮询：旧 Worker 已提交则直接跳过该 Item，claim 到期则用新 token 认领；只有既没有可认领 Item、也没有 active claim、Phase 又尚未完成时才按状态不变量破坏失败。

只有 `content_terminal` 和 `parser_runtime` 属于本 cycle 可结算的 Detail failure。`proxy_transport`、`youtube_rate_limited`、`youtube_challenge`、`token_or_client`、`upstream_transient`、`unknown`、数据库/contract 错误及主动取消都释放当前 claim、保持 Item 为 `pending` 并向外抛出；其中只有前三类触发现有 Rota 换线，其余按现有 Worker 决策在原线路、刷新客户端或人工修复后重试。

因为本轮串行，确认的 Route Failure 直接交给现有 Rota；不引入连续三次 breaker。以后若要并发，必须作为独立设计增加完成顺序协调、取消和 drain，不能在本脚本里提前埋半套状态机。

### 10.6 First-Seen ledger

Phase A 中新视频即使 Detail 最终形成现有 `settled_error`，仍按当前业务规则形成 First-Seen checkpoint，使后续 Run 能认领，不能静默消失。

- Phase A 未全部 settled 前不得开始 Phase B；
- First-Seen ledger checkpoint 未提交前不得开始 Phase B；
- checkpoint 重放必须幂等；
- 已由现有 ledger 认领的目标不得再次创建重复 ledger 记录；
- Item Capture 不直接推进 Cursor，也不直接调用 Publication。

### 10.7 从检查点重建 Finalization 输入

只有以下条件同时满足，Batch 才从 `fetching` 进入 `ready`：

- 全部 Phase A Item 为 `captured|settled_error`；
- First-Seen ledger checkpoint 已完成或明确不适用；
- 全部 Phase B Item 为 `captured|settled_error`；
- Batch target hash 与 Item 集合一致；
- 没有 `pending|claimed` Item。

新脚本随后只从 Batch/Item 重建旧 Finalization 所需的 `scan`、`discoveryEntries`、`discoveryCaptures`、`checkpointedFirstSeen`、`pendingDeferredVideoIds`、`preparedSampling`、`samplingPlanInput`、`anchors` 和 `observedAt`。

禁止重新请求 YouTube，也禁止从当前数据库状态重新运行 Planner。

### 10.8 最终收口

旧 `recordVideoCycle()` 是私有函数，且旧脚本不可修改。因此新脚本需要按现有行为重新实现等价的私有 Finalization block，并继续调用已有稳定 Store/Publication 接口。

最终事务仍然完成：

```text
Contents / Candidates / Disposition / Lifecycle
+ Video Observation
+ Cursor
+ Crawler Outbox
+ 当前 Publication Reconcile（仅完整 Scan）
+ Batch.status=finalized
+ Batch.final_observation_id
+ Batch.final_result_json
```

要求：

1. 上述写入与 Batch finalized 在同一事务；
2. 不逐 Item 写 Observation；
3. 不逐 Item调用 Publication；
4. incomplete Scan 保持现有 Partial 和 Cursor 语义；
5. Finalization 重放先读 finalized Batch，直接返回相同结果；
6. Observation 已存在但 Batch 未 finalized，或 Batch finalized 但 Observation 不存在，属于事务不变量破坏，必须停止自动覆盖；
7. `extractor_versions` 只写 YouTubeJS，不出现 yt-dlp 或 fallback；
8. `final_result_json` 必须保存第 7 节定义的完整 Runner 返回字段。

## 11. PostgreSQL 检查点模型

本轮只新增两张脚本私有表，不新增 Unit、Agent 或全局 Job 状态表。

### 11.1 `crawler.incremental_youtubejs_video_batches`

| 字段 | 语义 |
|---|---|
| `run_id` | 当前 Incremental Run |
| `cycle_key` | 本轮稳定抓取周期 |
| `plan_id/channel_id` | 与 Run/Plan 一致 |
| `status` | `fetching/ready/finalized` |
| `cycle_observed_at` | Scan 前冻结的观察时间 |
| `started_at` | Runner 传入的开始时间 |
| `scan_json` | 现有 normalized Scan 结果 |
| `anchors_json` | 本轮 Anchors |
| `discovery_entries_json` | Phase A 业务输入 |
| `pending_deferred_video_ids` | 当前 deferred 集合 |
| `sampling_plan_json` | 固化的 Planner 结果 |
| `sampling_config_json` | Planner 配置快照 |
| `target_hash` | 两个 Phase 的有序目标 hash |
| `first_seen_checkpoint_status` | `pending/complete/not_applicable` |
| `first_seen_checkpoints_json` | Batch 创建时已存在及 Phase A 后新写入的 First-Seen ledger 快照 |
| `final_observation_id` | finalized 时必填 |
| `final_result_json` | finalized 时完整保存 Runner 返回 |
| `created_at/updated_at/finalized_at` | 审计时间 |

```text
PRIMARY KEY (run_id, cycle_key)
```

Batch 对 Run 使用 `ON DELETE RESTRICT`，避免检查点存在时 Run 被普通清理删除。

### 11.2 `crawler.incremental_youtubejs_video_items`

| 字段 | 语义 |
|---|---|
| `run_id/cycle_key` | 所属 Batch |
| `phase` | `first_seen/recent` |
| `ordinal` | Phase 内稳定顺序 |
| `video_id` | YouTube Video ID |
| `target_json` | 固化的 Entry/row 输入 |
| `status` | `pending/claimed/captured/settled_error` |
| `claim_token/claim_expires_at` | 崩溃后可回收的短 lease |
| `detail_json` | 本次已观察到的 normalized Detail；`settled_error` 可保存现有语义允许的 partial detail |
| `field_status_json` | 本次字段观察状态 |
| `error_json` | 非线路、可 Finalize 的现有错误事实 |
| `attempt_count` | 诊断计数，不参与身份 |
| `captured_at/updated_at` | 审计时间；Recent Sampling 重建 retry/outcome 时间时使用已持久化的 `captured_at` |

```text
PRIMARY KEY (run_id, cycle_key, phase, video_id)
UNIQUE (run_id, cycle_key, phase, ordinal)
```

相同视频不得同时出现在 Phase A 和 Phase B。该约束由 Batch 创建事务验证，并用数据库约束兜底。

### 11.3 状态写入规则

- `pending -> claimed -> captured|settled_error`；
- claim 超时只能回到 `pending`；
- `captured/settled_error` 是本 cycle 内终态；
- Route Failure、取消和未知结果不得写 `settled_error`；
- `captured` 必须有完整可信 `detail_json`，且 `error_json` 为空；
- `settled_error` 必须有 `error_json`，并可同时保存本次已经可信观察到的 partial `detail_json`；
- partial detail 中 `unobserved/parser_gap` 字段不能伪装为 empty，也不能跨请求拼成完整 Detail；
- settlement 必须匹配当前 `claim_token`；stale token 零 canonical 写入；
- Item 终态后相同 hash 重放为 no-op，不同 hash fail closed；
- Batch finalized 后禁止修改任何 Item。

Batch 和 Item 本轮不设置自动 TTL，也不接入通用 Cleaner。它们至少保留到对应 Run、Observation 和所有受控恢复窗口均已按现有流程结束；没有独立审计和删除设计前不得物理删除 finalized Batch。

## 12. 数据完整性契约

### 12.1 本轮“完整”的准确含义

本轮完整性范围是当前 Incremental Video 业务契约中的字段和 required YouTubeJS surfaces，不承诺永久复制 YouTube 内部所有未建模、随版本变化的私有响应字段。

对于契约中的字段：

> YouTubeJS 本次可信响应明确给出值，新脚本必须保存该值；明确为空才保存空；没有观察到、Parser 不认识或子请求失败时，不得伪装成空值。

至少覆盖当前 Video 逻辑消费的 video identity、title、description、thumbnail、published time、类型信号、duration、view/like/comment count、comments disabled、comments first page、hashtags、keywords、access/playability status、live times、extractor version 和 source。

### 12.2 字段状态

| 状态 | 含义 | 能否当作空值覆盖 |
|---|---|---|
| `exact` | YouTubeJS 明确返回值 | 按值写入 |
| `empty` | 权威响应明确为空 | 可以 |
| `disabled` | 功能明确关闭，例如评论关闭 | 按业务规则写入 |
| `unavailable` | 内容不可访问且原因明确 | 按现有 access 规则 |
| `unobserved` | 本次没有观察到 | 不可以 |
| `parser_gap` | 响应存在但当前 Parser 无法提取 | 不可以 |

`null` 本身不能证明 `empty`。只有相应 status 才决定能否覆盖当前值。

### 12.3 Required surfaces

- 主详情成功、评论 Route Failure：整个 Item 保持 pending并交 Rota；
- 主详情成功、评论 Parser error：不得写“评论为 0”或“评论关闭”；按现有非线路失败语义形成 `settled_error` 或明确的 unobserved 状态；
- 明确 comments disabled 与明确 comment count=0 必须可区分；
- bot challenge 即使 HTTP 200，也按现有 YouTube failure policy 识别；
- private/deleted/members-only/region restricted 等明确 playability 结论是内容事实，不是线路失败；
- 不允许 YouTubeJS 主详情字段和 yt-dlp 字段合并成一条 Capture。

现有 `fetchYoutubeJsVideoDetail()` 会把评论错误压成字符串。为了让新脚本保留原始 cause/evidence，允许在 `youtubeJs.js` 增加 opt-in strict 模式；默认模式和所有旧调用方行为必须不变。

### 12.4 本轮不做 raw evidence 平台

本轮通过固定版本真实响应 fixture、字段状态测试、normalizer/检查点一致性测试、canary 数据对比和 extractor version 来验证字段覆盖。

不引入对象存储、WORM 或长期 raw body retention。若未来要求对任意未知 YouTube 字段也能事后证明没有遗漏，应单独设计 raw evidence 项目，不能隐含在本轮范围内。

## 13. 失败分类与 IP 切换

新脚本复用当前 `youtubeFailurePolicy` 的可信分类，不另建策略层。

交给 Rota 的错误只有 `proxy_transport`、`youtube_rate_limited` 和 `youtube_challenge`。明确内容不可用和 Parser gap 可以按 Item 规则结算；PostgreSQL 错误、本地 contract 错误、主动取消、`token_or_client`、`upstream_transient` 及没有可信证据的普通错误不交给 Rota，但同样不得结算成内容事实，Item 保持 `pending` 并向外抛出。

```text
确认 Route Failure
  -> 当前 Item 不落终态
  -> 等当前 YouTubeJS promise 已 reject/cancel
  -> 原错误上抛
  -> 现有 Managed Worker/Rota 决定换线与 BullMQ retry
  -> 新 attempt 读取同一 Batch
  -> 跳过 captured/settled_error
  -> 从第一个 pending Item 继续
```

本轮不修改 Rota 决策，不在脚本内调用切 IP 接口，也不从历史请求列表重新推断是否换线。

## 14. 事务要求

网络调用全部发生在数据库事务之外。

允许的短事务只有 cycle读取、Batch/Item创建、Item claim、单Item settlement、First-Seen checkpoint、Batch ready、最终收口和reservation释放。

明确禁止：

- 持有 `loadContentEnrichMode(... lock:true)` 的事务执行 HTTP；
- 持有 Run、Content、Cursor 或 Publication 行锁等待 YouTube；
- 一次事务覆盖整批 Video Detail；
- Capture 提交后依赖进程内 `Map` 才能恢复；
- 以 BullMQ progress 代替 PostgreSQL 状态。

## 15. 崩溃恢复矩阵

| 崩溃位置 | 恢复行为 |
|---|---|
| Scan 完成前 | 允许重新 Scan；此时尚无 Batch |
| Batch 提交后、第一个 Item 前 | 读取原 Batch，不重跑 Scan/Planner |
| Item 请求发出、响应未确定 | lease 到期后回 pending，允许重请求该 Item |
| YouTube 成功响应、Capture 提交前 | 最多重复请求当前 Item一次 |
| Capture 已提交、下一个 Item 前 | 已提交 Item永久跳过 |
| Phase A 完成、ledger checkpoint 前 | 重放幂等 ledger checkpoint |
| Phase A ledger 完成、Phase B 前 | 直接从 Phase B pending 开始 |
| 全部 Item 完成、Finalization 前 | 从 PostgreSQL 重建输入，不请求 YouTube |
| Finalization 事务提交后、函数返回前 | Batch finalized，恢复相同 `final_result_json` |
| BullMQ ACK 前 | 同上，不产生第二条同 cycle Observation |
| Route 切换 | cycle key不变，只继续 pending |
| 受控恢复 marker 提交后 | cycle key改变，允许创建新 Batch和新 Observation |

检查点保证的是“已提交结果不重抓”。如果当前请求成功后、数据库提交前进程立即死亡，数据库没有可恢复事实，因此当前视频最多再请求一次；依靠同 Item/cycle 幂等写入避免重复数据。

## 16. 与现有链路的关系

### 16.1 About

About 继续由现有 `executeIncrementalAbout()` 完成。它已经使用 Runner 的 YouTubeJS Channel snapshot，本轮不重写。

### 16.2 Agent

Agent 是否到期、如何入 Backlog、如何批处理和如何写回 Run全部保持当前实现。Runner 只补齐 Video 已提交后重入时的 lifecycle 摘要恢复：明确 dormant 继续跳过 Agent；incomplete Partial 没有 lifecycle 结论时继续执行到期 Agent，不能让 Run 永久失败。

### 16.3 Publication

新脚本在完整 Video Finalization 中继续调用当前：

```text
reconcilePublication(channel, ["channel", "video"])
```

因此本轮仍可能在 About、Video、Agent 各自完成时形成分域发布。它是当前行为，不是新脚本引入的变化。

“同一个 Plan 的 About+Video+Agent 全部完成后只发一次”明确推迟到后续 Publication Unit 项目。

### 16.4 Content Enrich

独立 `contentEnrichExecutor` 继续使用当前 `incrementalVideo.js` 导出的 helper和当前抓取器。本轮的 YouTubeJS-only承诺只覆盖 Incremental Channel Job中的 About/Video抓取，不扩展到独立 Content Enrich Queue。

### 16.5 Rota 和 BullMQ

BullMQ 只负责 retry、stalled recovery 和调度。PostgreSQL Batch/Item 才是恢复现场。

Rota 继续负责 Route 分配和换线。新脚本只上抛现有 policy 已确认的错误，不修改 Rota 状态机。

## 17. 新旧实现装配与 Cutover

Worker 增加单一 executor 配置，并要求新路径启用完整 YouTubeJS Detail：

```text
INCREMENTAL_VIDEO_EXECUTOR=legacy|youtubejs_checkpoint_v1
YOUTUBEJS_EXTRACTOR_MODE=channel|full
```

默认组合固定为 `legacy + channel`；canary 组合固定为
`youtubejs_checkpoint_v1 + full`。新 executor 与非 `full` 模式的组合必须在
Worker 启动时失败，不能静默退回旧抓取器。

装配示意：

```js
const videoExecutor = config.incrementalVideoExecutor === "youtubejs_checkpoint_v1"
  ? executeIncrementalYoutubeJsVideo
  : executeIncrementalVideo;

const incrementalChannelRunner = new IncrementalChannelRunner({
  runStore,
  agentBacklog,
  query,
  withTransaction,
  video: videoExecutor,
});
```

只允许修改装配，不允许把 mode 判断散布进 Runner、旧脚本或 Publication。

### 17.1 上线顺序

1. 添加两张检查点表和约束，不启用新 writer；
2. 部署新脚本、strict YouTubeJS opt-in和测试，配置仍为 `legacy`；
3. 使用 fixture 和 PostgreSQL集成测试验证；
4. 暂停领取新的Incremental Job；
5. 排空旧executor的active Job，并处置所有准备立即retry的旧Job；
6. 停止全部 legacy Incremental Worker，只启动使用 `youtubejs_checkpoint_v1` 的 canary Worker；
7. 在没有 legacy Worker 竞争同一 Queue 的前提下，只恢复少量受控频道流量，观察至少一个完整Clock周期；
8. 验证字段、Partial、Cursor、First-Seen、Lifecycle和Publication结果；
9. 扩大Worker范围；
10. 旧脚本继续保留，不在本轮删除。

### 17.2 禁止同一 Run 混用

一个 Run一旦创建新 Batch，就只能由新 executor继续。

- 不允许新 Batch抓到一半切回旧 executor；
- 不允许旧 executor忽略新 Batch重新 Scan；
- 不允许两个 executor并发处理同一 Run；
- 同一个 Incremental Queue 上禁止同时运行 legacy 和新 executor Worker；BullMQ 不保证某个频道 Job 被哪一类 Worker领取；
- Canary 使用“暂停普通流量、停止全部 legacy Worker、只启动新 executor Worker、受控恢复少量 Job”的方式；本轮不为 Canary 新增 Queue或修改Producer路由；
- 回滚时，已经存在未 finalized新 Batch的 Run由新 executor排空或停机修复，不能交给旧 executor。

## 18. 可观测性

新脚本至少记录 Batch created/resumed/finalized、cycle类型、Scan结果、两Phase目标数、Item各状态计数、First-Seen checkpoint、final Observation ID，以及每个Item的checkpoint hit、请求耗时、字段状态摘要、失败分类和retry次数。

指标必须能直接回答：

- 本次是否调用过yt-dlp；
- 为什么某个字段为空；
- 某个视频是否被重复请求；
- 重复请求发生在Capture前还是后；
- 换IP后从哪个Item恢复；
- Scan为什么成为Partial；
- 本Run为什么创建了新cycle；
- Finalization是否发生过重放。

## 19. 测试计划

### 19.1 静态范围测试

- 旧 `incrementalVideo.js` 无 diff；
- 新脚本不 import `incrementalVideo.js`；
- 新脚本不引用 `fetchChannelUploads`、`fetchVideoYtDlpDetail`、`yt_dlp` 或 fallback merge；
- Runner 不感知 Batch/Item，除 lifecycle 摘要续跑兼容外不改变执行顺序和 Agent gate；
- Agent、Publication和恢复脚本无行为修改；
- Worker diff只包含import/config/injection。

### 19.2 旧行为等价测试

使用相同Plan、数据库seed和完整YouTubeJS fixture，对比旧/新executor的complete/incomplete Scan、Anchor、Gap、新视频、recheck、recent refresh、live/upcoming、unavailable、no-change、dormant、First-Seen ledger、Cursor、Observation summary和Publication domains。

允许差异只有：source只剩YouTubeJS、不再有fallback/merge、中间Capture来自PostgreSQL，以及Observation key使用cycle key。

### 19.3 Checkpoint 测试

- Batch唯一创建；
- 并发创建输家加载赢家；
- captured Item重入不请求；
- claimed lease过期回pending；
- 重入遇到未过期 claim 时不消耗 BullMQ retry，等待提交或到期后继续；
- 响应后、提交前崩溃只重复当前Item；
- Phase A checkpoint后崩溃不重抓Phase A；
- Phase B中途崩溃只继续剩余Item；
- 全部Item完成后崩溃直接Finalize；
- finalized后重放返回相同结果；
- Batch/Observation单边状态fail closed。

### 19.4 Cycle identity 测试

- base cycle稳定；
- BullMQ attempt、Route、Worker和stalled变化不改变cycle；
- 三类committed recovery marker分别改变cycle；
- marker可变内容变化不改变cycle；
- marker结构冲突fail closed。

### 19.5 错误分类与完整性测试

- 429、HTTP 200 challenge和proxy transport上抛Route Failure；
- Scan Route Failure不写Partial；
- 非线路incomplete仍写现有Partial；
- private/deleted不换IP；
- Parser gap不写empty；
- comments子请求Route Failure使Item保持pending；
- cancellation使Item保持pending；
- 每个契约字段有present和authoritative empty fixture；
- comments disabled与comment count=0分开；
- YouTubeJS有值时Capture值相同；
- `field_status_json`与Capture一致；
- `extractor_versions`不含yt-dlp。

### 19.6 事务测试

- 网络promise pending期间没有数据库事务保持打开；
- Content Enrich mode锁在网络前释放；
- 每个Item只使用短claim/settlement事务；
- 慢请求期间 Item lease 心跳使用独立短事务，心跳失败会取消请求且 stale token 不能提交；
- Finalization与Batch finalized原子；
- Item提交不推进Cursor、不写Observation、不调用Publication。

## 20. 验收条件

全部满足后，才允许新 executor扩大流量：

1. 新文件 `incrementalYoutubeJsVideo.js` 存在并独立实现；
2. 旧 `incrementalVideo.js` 未修改；
3. Runner 仅包含 lifecycle 摘要续跑兼容且不感知 Batch/Item；Run Store、About、Agent、Publication、Recovery和Rota行为未修改；
4. Incremental About/Uploads/Video实际数据请求只使用YouTubeJS；
5. 新Video路径的yt-dlp调用数为0；
6. Uploads Scan不再fallback；
7. Video Detail不再fallback或逐字段merge；
8. 目标选择结果与现有算法一致；
9. Video Detail固定串行；
10. 每个可信结果立即写PostgreSQL检查点；
11. 换IP和普通retry只继续pending Item；
12. 已提交Item不重复请求；
13. 当前Item响应后、提交前崩溃最多重请求当前Item一次；
14. 受控恢复marker产生新cycle，允许合法新Observation；
15. 同一cycle只产生一条Video Observation；
16. Route Failure不写成字段为空、视频不存在或Scan Partial；
17. incomplete Scan仍不推进Cursor；
18. First-Seen ledger语义保持一致；
19. 网络请求期间没有长事务或数据库锁；
20. Finalization继续原子写现有Observation、Cursor、Outbox、Lifecycle和Publication结果；
21. 完整Runner返回保存在Batch并可重复恢复；
22. 当前Publication调用次数、Domain和payload语义不因检查点改变；
23. About+Video+Agent频道级合并发布未被偷偷引入；
24. 一个完整Clock周期canary无静默字段丢失、重复Observation或Cursor越界；
25. 旧/new executor没有处理同一Run。

## 21. 明确拒绝的实现

- 在旧脚本中删除fallback后继续改；
- 新脚本包装旧`executeIncrementalVideo()`；
- 修改Runner来理解Batch/Item；
- 把检查点写进BullMQ progress/job.data；
- 每个视频一个BullMQ Job；
- 第一版直接并发10；
- 把线路错误写成`settled_error`；
- Item成功后立即发布；
- 用BullMQ attempt作为Batch或Observation身份；
- 为本轮引入Publication Unit。

## 22. 实现顺序

1. 给现有Video路径补齐characterization fixture和最终数据库快照测试；
2. 添加Batch/Item additive Schema与Store集成测试；
3. 新建`incrementalYoutubeJsVideo.js`，先实现相同Scan和目标选择；
4. 实现cycle key和Batch固化；
5. 实现Phase A串行YouTubeJS Detail与逐Item检查点；
6. 接回First-Seen ledger checkpoint；
7. 实现Phase B串行Detail与稳定reservation owner；
8. 从Batch/Item重建Finalization输入；
9. 实现`recordVideoCycle`等价原子收口；
10. 增加strict YouTubeJS required-surface opt-in；
11. 完成错误分类、崩溃注入和字段完整性测试；
12. 在Worker增加单点executor装配开关；
13. 配置保持legacy，完成fixture对比；
14. 排空旧active Job后进行单Worker canary；
15. 一个完整Clock周期稳定后扩大流量。

## 23. 最终字符图

```text
旧实现（完整保留）
  incrementalVideo.js
        |
        +--旧Worker配置仍可选择
        +--本轮零修改

新实现
  incrementalYoutubeJsVideo.js
        |
        +--YouTubeJS Uploads Scan
        +--现有Anchor/Gap/目标选择
        +--固化Batch和全部目标
        +--Phase A串行Detail
        |     +--每个结果立即写PostgreSQL
        |     +--First-Seen ledger checkpoint
        +--Phase B串行Detail
        |     +--每个结果立即写PostgreSQL
        +--线路错误保持pending并交现有Rota
        +--换IP后只继续pending
        +--全部完成后执行现有语义的原子Finalize
        +--继续当前per-domain Publication

没有进入本轮
  Publication Unit
  Agent重构
  Terminal协议重构
  并发10/三连breaker
  多频道批量发布
  WORM/raw evidence平台
```

最终边界是：只替换 Incremental Video 的抓取实现和它私有的恢复现场；所有上游调度与下游业务语义继续使用当前系统。
