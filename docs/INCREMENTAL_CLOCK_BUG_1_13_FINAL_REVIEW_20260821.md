# Incremental Clock BUG-1 至 BUG-13 最终复核与解决方案

## 1. 文档目的

本文对 `DATA_COMPLETENESS_AUDIT_20260821.md` 及其后续 Claude 评审中提出的 BUG-1 至 BUG-13 逐项复核。

复核目标不是证明原评审“整体对或整体错”，而是把四种不同性质的问题分开：

1. 当前代码会持续制造数据缺口的真实缺陷；
2. 历史坏数据仍未修复，但当前写入逻辑已经正确；
3. 容量、积压或部署治理问题；
4. 既定业务契约或保护机制，被误判成 BUG。

本文最初只做评审和方案设计。BUG-8 根因确认后，唯一源码已完成最小修复与上线验证。2026-08-24，BUG-6/BUG-7 的 Enrich Drain 修正候选及后续边界修正已在隔离 Worktree 完成实现和隔离验收；该变更尚未部署，未连接或修改生产数据库、生产队列和业务数据。这里的“隔离验收通过”只表示代码候选具备上线条件，不表示生产积压已经下降或 BUG 已在生产关闭。

## 2. 复核边界与证据

- 唯一源码：`/root/workspace/agent/pachongsys`
- 源码提交：`cbca43bd22fffa71386dc712c04c61b7b323eb35`
- 数据库：`bullmq_crawler_migration`
- 数据查询：全部使用 `REPEATABLE READ READ ONLY` 或 `READ ONLY` 事务
- 最终动态快照：2026-08-21 09:20:35 UTC，即北京时间 17:20:35
- 今日范围：`plan_day = 2026-08-21`
- 本文没有把 Migration、Query、Full Crawl 的历史结果混入今日 Incremental Clock 统计

最终快照时，今日计划状态为：

| 状态 | 频道计划数 |
|---|---:|
| succeeded | 10,963 |
| partial | 744 |
| failed | 4 |
| cancelled | 71 |
| running | 18 |
| dispatched | 27 |
| planned | 1,173 |
| **总计** | **13,000** |

Clock 仍在执行，因此今日累计数量会继续变化。代码语义、历史数据形态和部署版本结论不受该变化影响。

## 3. 总结结论

| 编号 | 最终结论 | 性质 | 处理优先级 |
|---|---|---|---|
| BUG-1 | **部分成立，核心缺陷成立，但 1a/1b/1c 不是已证实根因** | 当前数据正确性缺陷 | P0 |
| BUG-2 | **现象成立，但不是独立 BUG** | BUG-1 下游保护行为 | 随 BUG-1 处理 |
| BUG-3 | **305 条异常数据成立，当前代码根因不成立** | 历史数据修复 | P1 |
| BUG-4 | **不成立** | 既定发布契约 | 不修改，只增强可观测性 |
| BUG-5 | **5 条当日异常成立，写入端根因尚未证明** | 历史数据/溯源修复 | P1 |
| BUG-6 | **积压成立；根因是补全任务没有独立、有界且公平的调度通路** | 调度/吞吐缺口；修正候选已通过隔离验收、待生产验收 | P0/P1 |
| BUG-7 | **历史未补全成立，是 BUG-6 的存量结果，不是第二套恢复问题** | 随 BUG-6 通过正常补全与发布链处理 | 随 BUG-6 处理 |
| BUG-8 | **成立，根因已确认是外部审计脚本污染共享 PgBouncer 后端** | 数据库连接状态污染 | P0 |
| BUG-9 | **已修复并由唯一源码接管** | 源码/部署治理 | P2 |
| BUG-10 | **镜像漂移与新契约缺口均已修复、上线并验证** | Feature Ingest 契约 | 已关闭 |
| BUG-11 | **容器残留成立，不是业务功能 BUG** | 运行态垃圾 | P2 |
| BUG-12 | **版本差异成立，48 个角色当前功能落后不成立** | 组件版本治理 | P2 |
| BUG-13 | **全域漏排不成立，但存在 1 个冷启动恢复缺口** | Agent Clock 边界状态缺陷 | P1（单频道修复 + 不变量） |

## 4. BUG-1：详情对象非空，但新视频未入库

### 4.1 最终结论

**核心问题成立。**

最终快照的恒等式为：

```text
detail_success_count = 35,718
first_seen_count     = 31,932
unresolved_count     =  3,786
detail_failure_count =      0

35,718 = 31,932 + 3,786
```

受影响 740 个频道。

但 `detail_success_count` 的准确含义是“`capture.detail` 是非空对象”，不是“标题、访问状态、权威类型和全部字段均已成功取得”。详情对象可能只有部分字段，也可能缺少足以判断 `video / short / live` 的权威 Watch 信号。

### 4.2 原评审中正确的部分

- `upsertFirstSeenContent()` 在 `storageAction.kind !== "upsert"` 时直接返回 `null`；
- 调用方只增加 `unresolved_count`，没有保存每个视频被拒绝的具体原因；
- 没有生成 `content_candidates` 或 `content_enrich_tasks` 恢复入口；
- 详情请求成本已经发生，但结果没有形成可恢复的数据闭环。

### 4.3 原评审中需要修正的部分

#### 1a：`candidate: {}` 不是当前丢弃问题的已证实根因

该函数只处理已经通过 `knownVideoIds()` 确认为“数据库尚不存在”的 `firstSeenEntries`。因此在这个调用点没有 existing content key，`update_access` 分支本来就不应命中。

这里复用一个同时支持“新建”和“更新访问状态”的函数，接口不够清晰，但不能据此认定它造成了 3,786 条丢弃。

#### 1b/1c：Uploads 不权威认定普通 video 是事实边界，不应直接改掉

Uploads 列表不能稳定地区分普通视频和 Shorts。把 Uploads 中看似普通的条目直接当作 authoritative video，会重新引入曾经发生过的 Shorts 错分问题。

`resolveFromUpload()` 只把明确的 short/live 信号作为非权威 fallback，并固定 `authoritative: false`，符合“最终类型必须由 Watch/Player 权威信号确认”的既定设计。

正确修复方向是补强 Watch 详情信号及恢复闭环，不是把 Uploads 的普通 video 强行升级为权威事实。

#### 1d：视频 ID 并未完全消失

`crawler.crawl_observations.result_summary_json` 只保存计数，确实没有 ID；但完整 Observation 事件会把 `unresolved_video_ids` 写入 `crawler.crawler_outbox.payload_json`。

最终快照中：

| 指标 | 数量 |
|---|---:|
| unresolved 出现次数 | 3,786 |
| distinct video_id | 3,786 |
| 带 unresolved IDs 的 Observation | 740 |

因此准确结论是：**ID 可恢复，逐 ID 的拒绝原因和详情证据不可恢复。**

### 4.4 真实设计缺口

当前路径把多种情况压成同一个 `null`：

```text
Uploads 发现 ID
  -> 获取非空 detail 对象
  -> 权威类型不足 / 访问状态不允许新建
  -> storageAction != upsert
  -> return null
  -> 只累计 unresolved_count
```

系统无法区分：

- Watch 类型信号缺失，但视频公开，可换客户端重试；
- Challenge/网络身份导致详情不完整，可换 Rota 线路重试；
- 视频 private/unavailable，本次发现周期可以闭环，不应在当前任务中无限重试；
- 视频 members-only/unlisted，已有明确访问结论；
- 解析器漏掉了一个本来存在的权威信号。

### 4.5 具体解决方案

#### A. 用结构化 disposition 替代 `null`

`upsertFirstSeenContent()` 必须返回下列三类结果之一：

```text
stored
  - content_key
  - content_type
  - evidence_source

deferred
  - video_id
  - reason_code
  - missing_fields
  - access_status
  - type_signal_summary
  - extractor/client
  - retry_class

terminal_excluded
  - video_id
  - reason_code
  - access_status
  - evidence_source
```

禁止再用裸 `null` 表示所有失败形态。

#### B. 复用 `crawler.content_candidates` 保存恢复证据

对 `deferred` 和 `terminal_excluded` 均写入候选记录，至少保存：

- `run_id / channel_id / source_content_id / position`；
- `detail_status / type_status / missing_fields`；
- `result_json.disposition_reason`；
- `result_json.access`；
- `result_json.content_type_signals`；
- extractor、客户端和尝试次数；
- terminal 与 retryable 的明确标志。

不要再新增另一张语义重复的“gap 表”。现有 candidates 已具备状态、缺失字段、尝试次数和结果 JSON。

#### C. 按原因选择恢复方式

| 原因 | 恢复动作 |
|---|---|
| public + Watch 类型信号缺失 | 使用备用 Player/YouTube.js/yt-dlp 客户端重新获取权威信号 |
| Challenge / bot check | 通过 Rota 更换网络身份后做有上限重试 |
| access unknown | 保持 deferred，进入独立补全队列 |
| private / unavailable 且证据明确 | 本次周期记为 terminal_excluded，不继续烧当前任务请求；另排低频访问状态复查 |
| unlisted / members-only 且类型明确 | 按现有 crawler 存储契约处理；业务发布仍遵守自己的可见性规则 |

YouTube Data API 可以补标题、日期等字段，但不能可靠证明 Shorts 类型，不能把它作为类型权威来源。

这里的 `terminal_excluded` 只表示“当前 Observation 已有明确结论”，不是“这个视频永远不再检查”。private 视频以后可能重新公开，因此系统应建立独立、低频、带退避的 access recheck；它不阻塞当前增量游标，也不与正常新视频争抢高优先级 Player 预算。

#### D. 增加按原因的指标

至少输出：

- `stored_count`；
- `deferred_type_signal_count`；
- `deferred_access_count`；
- `terminal_private_count`；
- `terminal_unavailable_count`；
- `challenge_count`；
- `recovered_count`。

### 4.6 回归测试与验收

必须覆盖：

1. 普通 video、Shorts、live 三种 Watch 权威信号；
2. public、unlisted、members-only、private、unavailable、unknown；
3. detail 非空但类型信号不足时生成 candidate，而不是静默返回；
4. candidate 重放后可幂等转为 stored 或 terminal_excluded；
5. 每个 unresolved ID 均有 reason_code；
6. `detail_success = stored + deferred + terminal_excluded`；
7. private/unavailable 可关闭本次 Observation，但会生成低频 access recheck；
8. 连续 24 小时 `silent_drop_count = 0`。

## 5. BUG-2：增量游标不前进

### 5.1 最终结论

**现象成立，但不是应该单独修掉的 BUG。**

当前只有 discovery complete 才更新锚点和 source cursor。只要存在 unresolved，游标不前进。这会造成下一次 Clock 重扫，但也阻止系统把未处理视频永久越过。

如果现在直接允许 partial 更新游标，3,786 个视频会从“重复请求但仍有机会恢复”变成“永久静默丢失”。

### 5.2 具体解决方案

游标完成条件应从“所有视频都已入库”改为“所有发现 ID 都有明确 disposition”：

```text
stored            -> 已闭环
terminal_excluded -> 已闭环
deferred          -> 未闭环，游标不得前进
```

BUG-1 修复后：

- private/unavailable 等“本周期终态”不会阻塞游标，但由低频任务继续复查访问状态；
- 需要恢复的 public/unknown 视频仍阻止游标越过；
- recovery 完成后重新计算 Observation closure，游标自然前进；
- 不需要重新抓整个频道来找回已经保存在 Outbox/candidate 中的 ID。

### 5.3 回归测试与验收

- 全部 stored：游标前进；
- stored + terminal_excluded：游标前进；
- 任意 deferred：游标不前进；
- deferred 恢复为终态后：游标只前进一次；
- 重放同一 Observation：游标和锚点幂等。

## 6. BUG-3：disabled 评论状态被拒绝

### 6.1 最终结论

**2026-08-24 更新：产品契约已明确，评论关闭必须发布为数值 `0`，并由
`comments_disabled=true` 保留“关闭”语义。原先的 `disabled + NULL` 结论已废止。**

数据库证据：

| 指标 | 数值 |
|---|---:|
| `comment_count_status='disabled'` 且 count 非 NULL | 305 |
| 频道 | 37 |
| `publication_item_hash IS NULL` | 305 |
| 最早首次写入 | 2026-07-18 05:00 UTC |
| 最晚首次写入 | 2026-07-18 06:31 UTC |
| 最晚被更新 | 2026-07-26 06:00 UTC |

这批数据是在旧契约下集中产生的历史数据；按当前契约均需要规范化。

当前所有视频详情入口必须执行：

```text
comments_disabled=true
  -> comment_count=0
  -> comment_count_status='disabled'
```

这里的 `0` 是系统对外使用的权威业务值，不代表 YouTube 返回了一个原始评论总数。
`comments_disabled=true` 负责把“作者关闭评论”和“评论已开启但当前确实为 0”区分开。
Crawler 保留 `comment_count_status='disabled'`；Business Projection 将其投影为
`comment_count=0`、`comment_count_status='exact'`，同时继续保留
`comments_disabled=true`。

### 6.2 具体解决方案

1. YouTube.js、yt-dlp、YouTube Data API fallback 统一产生 `0/disabled/true`；
2. Full Crawl、Migration、Query、Incremental First-Seen、Incremental Recent Sampling
   和 Content Enrich 写入同一状态三元组；
3. `videoPublicationCurrent.js` 只接受
   `comment_count=0 AND comment_count_status='disabled' AND comments_disabled=true`；
4. Business Projection v4 将 Crawler 的 `disabled` 映射为业务库的
   `0/exact/true`，旧 v3 快照继续只读兼容；
5. 使用 `npm run repair:disabled-comment-counts` 先只读 Plan，再用 Plan 输出的
   精确行数、证据哈希和确认值 Apply；
6. 修复事务同时刷新 `publication_item_hash`，随后通过正式 Repair Revision 和
   Publication Reconciler 更新业务 Current，不修改历史 Revision；
7. Crawler 约束为
   `comments_disabled=true -> comment_count=0 AND comment_count_status='disabled'`；
   运行时先以 `NOT VALID` 安装，历史修复后再 `VALIDATE`。

### 6.3 验收

- Plan 与 Apply 的数据库身份、频道数、内容数、目标行数和证据哈希完全一致；
- 所有 `comments_disabled=true` 行均为 `comment_count=0` 且状态为 `disabled`；
- 其他评论状态不变，受影响行重新生成合法 item hash；
- 增量首次发现、存量刷新和 Content Enrich 的 PostgreSQL 回归均通过；
- Publication Current 发布 `0/disabled/true`，Business Current 最终为
  `0/exact/true`；
- 新写入任何不一致组合均在数据库层直接拒绝。

## 7. BUG-4：unlisted 存得进但发不出

### 7.1 最终结论

**不是 BUG，是明确的业务发布契约。**

证据：

- crawler 允许保存 unlisted，以保留来源事实；
- `videoPublicationCurrent.js` 明确把它映射为 `source_unlisted`；
- 单元测试明确要求 unlisted 不进入业务窗口；
- Business Publication Contract 只允许 public 和 members-only；
- 当前全库有 237 个 unlisted 视频/104 个频道，它们没有 publication item hash，符合该契约。

这不是“无告警漏白名单”，而是“爬虫保存事实，业务端不公开分发”的边界。

### 7.2 解决方案

不修改白名单。只增强可观测性：

- Dashboard 展示 `source_unlisted` 排除数量；
- Publication 报告区分正常 retraction 与 item contract failure；
- 不把 unlisted 计入发布硬失败。

如果未来产品决定在业务库展示 unlisted，需要单独 ADR，并同步修改：业务契约、数据库 check、Projection Adapter、API 权限、前端可见性和测试。不能只加一行白名单。

### 7.3 验收

- crawler 继续保存 unlisted 来源事实；
- Business Current 继续不包含 unlisted；
- Dashboard 将 `source_unlisted` 显示为正常排除，而不是发布失败；
- Publication 告警不把正常 unlisted 排除计入 contract failure。

## 8. BUG-5：resolved 数值缺少 `_source`

### 8.1 最终结论

**数据缺陷成立，当前写入端仍在制造该缺陷的证据不足。**

今日触达且影响门禁的是：

- `duration_status='exact'`、有 duration、无 `duration_source`：4 条；
- like 已解析、有值、无 `like_count_source`：1 条。

全库范围：

- 同类 duration 历史数据 30,185 条；
- 同类 like 历史数据 2 条。

当前增量 Writer 会从详情来源补齐这些 source。大量 duration 缺口更像历史导入/旧写入路径遗留，不能只凭 5 条断言当前 Writer 漏字段。

### 8.2 具体解决方案

1. 先按 5 条记录检查 `raw_json`、extractor version、run_id 和原始证据；
2. 只有原始证据能证明来源时才补 `_source`；
3. 若无证据，不得猜测来源，应把状态和值降级为 `unresolved/NULL`；
4. 按来源批次修复全库 30,185 条 duration 历史数据；
5. 刷新 item hash，并通过 Repair Revision 分发；
6. 增加写入契约：resolved 必须同时具有 value、source、observed_at；
7. 增加 merge 回归测试，防止新详情缺 source 时覆盖已有有效 source；
8. 历史清理后增加 `NOT VALID` 数据库约束并最终验证。

### 8.3 验收

- 新增 7 天数据中 resolved-without-source 为 0；
- 今日 5 条有证据修复或诚实降级；
- 无任何伪造 source；
- Publication Item 校验通过。

## 9. BUG-6：`content_enrich_tasks` 积压

### 9.1 最终结论

**严重积压成立，“没有消费者”不准确；准确根因是没有独立、有界且公平的补全调度通路。**

2026-08-24 最终修正又关闭了两个 P1 阻断项：Controller 不再无限等待 Enrich
全表监控聚合；Clock 也不再先提交成功数据和游标、再单独执行 Publication。这里的
“关闭”只指代码候选和隔离事务验收，生产 BUG 仍必须等金丝雀实际消化积压后才能关闭。

同日后续复审又关闭两个事务边界缺口：First-Seen 不完整详情的失败证据不再被
Publication 回滚；已提交 Clock 周期也不再因 reservation cleanup 短暂失败而被控制面
误判为失败。这里仍只代表代码候选通过隔离验收，不代表生产积压已经修复。

同一轮最终复审继续关闭 First-Seen checkpoint 的重放缺口：同一 Run 在 Publication
失败后重试时，即使该视频已经离开当前 Uploads 页面，也会从 Candidate 和尚未关联
Observation 的占位 Content 恢复一次 First-Seen ledger；不能因 Content 已存在就静默
漏掉 `first_seen`、disposition 和 crawler outbox 后继续推进游标。

后续并发复审确认 `Content.last_observation_id` 只能表达“最近被哪个 Observation 看见”，
不能同时表达 First-Seen ledger 是否已经消费。partial Observation 合法更新该字段时，旧
恢复条件会永久漏账；两个重叠 Clock Job 也可能同时读取同一 checkpoint。修正候选因此
在 Candidate 上引入显式 `not_applicable`、`pending`、`consumed` 状态，在完整 Observation
事务内用条件 `UPDATE ... RETURNING` 原子取得唯一消费权。partial Observation 不消费
checkpoint；Publication 回滚也会把消费权一起回滚为 pending。

最后一轮跨 Run 复审进一步确认：partial 扫描会正常结束旧 Run，下一次 Clock 使用新的
`run_id`，因此恢复条件不能绑定产生 checkpoint 的 Run。当前实现按频道读取所有历史
Incremental Run 的 pending ledger，以稳定的 `channel_id + content_key` 关联 Content，并按
`channel_id + candidate_id + pending` 原子消费；后续 Full Crawl 即使把同一 Content 的
`run_id` 更新为新 Run，也不会让旧 ledger 隐身。Candidate 保留原 Run 身份，成功
Observation、crawler outbox、游标与 Publication 则归当前 Run。共享 Observation writer
同时在幂等键和域 cursor 前显式取得 Channel `FOR NO KEY UPDATE`，不再依赖外键
`KEY SHARE` 偶然形成锁序。

2026-08-21 原始核对时：

| 状态 | 任务数 | 频道数 | 说明 |
|---|---:|---:|---|
| done / player-refresh | 18,477 | 1,189 | 今天仍在持续完成 |
| queued / player-refresh | 47,127 | 373 | 最老创建于 7 月 23 日 |

Incremental Clock 会读取 queued/failed task，把它们放到最近视频采样候选最前面，并在成功后更新为 done。因此 Clock 本身是消费者。

2026-08-23 的后续证据为：

| 指标 | 数量 |
|---|---:|
| queued / player-refresh | 约 46,103 |
| 涉及频道 | 约 324 |
| `last_enriched_at IS NULL` 视频 | 约 45,770 |
| 上述未补全视频中无 Enrich Task | 0 |

根因链为：

- 只有频道下一次 Video Clock 到期时才有机会处理；
- Enrich Task 虽然在采样计划中优先，仍受每个计划的 `player_cap` 限制；
- 休眠频道没有近期 Clock，等待时间显著更长；
- 单频道可能有上百个任务；
- 失败任务重新进入现有 upsert 时会把 `attempts` 重置为 0，重试历史和退避依据失真；
- `youtube-content-detail` 是 Full Crawl 的旧阶段队列，只接受 `run_id + content_candidates`，且内联详情模式会暂停它，不能承载本任务契约；
- 所以任务持续生成，但处理能力受频道 Clock 频率和正常增量预算约束，低于积压形成速度。

### 9.2 本地修正候选：有界 Enrich Drain

本地实现采用数据库单一事实来源、Controller 派发和专用 Worker，不允许 Worker 无限轮询数据库：

```text
content_enrich_tasks
  -> Controller 在 High Water 以下有界、公平派发
  -> youtube-content-enrich
  -> 专用 channel-identity Enrich Worker
  -> 复用 fetchIncrementalVideoDetail + applyIncrementalVideoDetail
  -> contents + Video Item Hash
  -> Publication Reconciler
  -> done / terminal(next_retry_at) / failed(next_retry_at) / dead_letter
```

状态机和 fencing：

```text
queued / failed / 到期的 terminal
  -> leased     (lease_owner, lease_expires_at, dispatch_generation + 1)
  -> running    (Worker 再读 DB，并校验 Job、频道、generation 和有效 lease)
  -> done       (完整 public 详情成功)
  -> terminal   (private / unavailable 等权威终态；关闭本轮并低频复查)
  -> failed     (attempts + 1, next_retry_at 使用有上限指数退避)
  -> dead_letter (累计 attempts 达到上限，停止自动重试)
```

框架完成后的复核还发现了第二层根因：专用 Enrich Worker 与 Incremental Clock 的 Recent Sampling、First-Seen 路径曾各自维护一套 Task 状态转换。结果是 Queue 有完整性门禁，而 Clock 仍可能把缺标题、播放量或时长的详情写入 `last_enriched_at` 并关闭任务；Clock 失败也不累计 `attempts`。本轮把“详情是否完整、错误是否为权威终态、累计预算是否耗尽、退避到何时”抽成共享策略，两个消费者只负责所有权和持久化，不再各自解释业务终态。

- BullMQ Job ID 由频道和排序后的 `task_id:dispatch_generation` 哈希确定，Controller 重放不会产生第二个 Job；
- `dispatch_generation` 是 fencing token。过期 running 任务重新派发时推进 generation，旧 Worker 不能写 Content 或关闭新任务；
- Worker 对已完成、过期、错误 generation、错误 lease owner 和重复 Job 均幂等跳过；
- Queue Worker、Clock Recent Sampling 和 Clock First-Seen 共用完整 Video surface 契约；缺标题、发布时间、播放量或时长等关键事实时进入 retryable，不能写 `last_enriched_at` 或假装 done；
- Clock 与 Queue 都按同一开放任务的累计 `attempts` 消耗预算；默认第 8 次失败进入 `dead_letter`。只有已关闭任务开始明确的新一轮时才从 0 计数，默认 Clock 模式和回滚模式不能成为无限重试旁路；
- Clock 在 Recent Sampling 候选 SQL 和取得 Task `FOR UPDATE` 行锁后各执行一次 retry eligibility 门禁；`queued/failed` 且 `next_retry_at` 未到期的近期视频不能因“仍在最近窗口”提前抓取或消耗 attempts；
- 只有失败策略给出 `content_terminal` 的权威 private/members-only/unavailable 证据才能写 `terminal` 和访问状态；解析器、数据库契约等 `retry_mode=none` 工程故障进入 `dead_letter`，不得伪造视频终态；
- retryable 的 `next_retry_at` 从每条详情请求实际结束的时刻计算，并受指数退避上限约束；慢请求不能在刚失败时就因批次开始时间过早而立即重试；
- 新建 failed/dead-letter Task 的 `last_success_at` 必须为 NULL；First-Seen 收到非空但不完整的 public detail 时，Candidate 写 `detail_status='failed'` 和缺失详情证据，不能把“收到对象”伪装成“详情完成”；
- 已是 `dead_letter` 的视频若被后续正常 Incremental 取得完整详情，可以按真实成功结果转为 `done`；历史 attempts 保留作审计，避免出现数据已完整但任务仍显示死信的矛盾状态；
- Worker 在抓取期间按不超过 lease 一半的间隔续租；任何续租异常或未续满整批都立即触发 `AbortSignal`、停止后续抓取并丢弃未结算结果，不能把失去所有权后的详情写入 Content；
- lease 的创建、刷新、claim、续租、过期回收和 retry 到期判断都以 PostgreSQL `clock_timestamp()` 为准，不能混用 Controller/Worker 进程时钟；可能等待行锁的 claim/renew/settle 使用“先锁行、后用 DB 当前时间判 lease”的两阶段 fencing，等待前求出的时间戳不能复活已过期 lease；
- Rota 换身份前的 retry checkpoint 只有在本批 retryable/dead-letter outcome 全部实际结算后才标记为已持久化；lease 过期或 generation fencing 导致任何 outcome 跳过时必须 fail closed，不能虚报 attempts 已记账；
- 开放任务再次 upsert 时保留 `attempts`、`next_retry_at` 和有效 lease；只有 done/terminal/skipped 的已关闭任务开始新一轮补抓时才把 attempts 重置为 0。

有界、公平和崩溃恢复：

- 生产入口是 transaction-mode PgBouncer，session advisory lock 不能表达跨事务所有权；同时也不能为持有 xact advisory lock 而把 `queue.add()` 放在租约事务内，否则 Worker 可能先消费 Job、却看不到尚未 COMMIT 的 lease；
- Controller 因此使用已提交到 `crawler.settings` 的可过期 mutex（owner、expiry、heartbeat）串行化完整 High Water 周期。mutex 获取事务先提交，任务 lease 在独立短事务中提交，之后才允许 BullMQ 投递；
- 领取新批次和刷新未投递 lease 都会在各自数据库事务内重新锁定并校验 mutex owner、expiry 和 mode；仅靠进程内 heartbeat 状态不足以阻止 TTL 后的旧 Controller 继续派发；
- 需要等待 mutex 行锁的 owner 校验和模式切换先取得行锁，再调用 `clock_timestamp()` 判断 expiry，不能在等待前提前求值；Controller 丢失 mutex heartbeat 或数据库 owner fencing 后均 fail closed。进程崩溃后 mutex 自动过期，下一 Controller 可接管；这不依赖 PgBouncer 后端 session 粘性；
- 每轮按持久化频道游标环形选择，每个频道最多一个可配置小批次，单 Job 只包含同一频道的少量 task ID；
- 派发优先级为：活跃频道最近 90 天或日期未知的视频、活跃频道历史视频、非活跃/休眠频道；每层仍遵守 task priority 和频道公平游标；
- 领取使用 `FOR UPDATE SKIP LOCKED`，第一版只选择已有证据确认的 `player-refresh`；
- DB 已提交 lease、BullMQ 尚未投递时崩溃，下一轮按原 lease owner 和确定性 Job ID 恢复；
- 恢复投递的频道占用本轮该频道唯一的公平名额，后续新领取 SQL 显式排除这些频道，不能让一个恢复中的大频道在同一轮再次挤掉其他频道；
- Queue 已有 waiting/active/delayed Job 时，即使达到 High Water 仍维护其 lease；未投递 lease 的恢复失败会消耗本轮容量，不会在 Redis 故障时继续扩大租约集合；
- BullMQ 已完成或失败但 DB 仍为 leased 时，Controller 释放旧 lease，再以新 generation 派发。
- Enrich 派发异常写结构化 Controller action 和日志后即隔离返回，不能阻断同一 tick 后续 Query、Full Crawl、Migration 或 Incremental 工作。

抓取、Rota 和发布：

- Worker 复用现有视频详情抓取、类型判断、访问状态和增量落库函数，没有复制第二套视频规则；
- private/unavailable 权威证据更新 `contents.access_status/access_status_source`、关闭当前任务并写 7 天后的 `next_retry_at`；到期后才开启新一轮，复查仍为终态则再次低频排期；
- retryable/dead-letter checkpoint 和未执行任务释放先在独立短事务提交；成功或 terminal 的 Content、Task、变化视频 Item Hash 与按频道 Publication Reconciler 保持在另一个原子事务；
- 因而 Publication 失败会回滚对应 Content/Hash/terminal/done 写入，但不能抹掉同批已经记录的 retry attempts；未变化的 retryable/dead-letter 不刷新 Hash 或调用 Publication；
- Clock 第一事务只提交真实 retryable/dead-letter checkpoint；对 success/terminal 则把已有 Task 变为带 `lease_owner`、expiry 和新 `dispatch_generation` 的 `running` reservation，不提前提交 Content、Hash、Observation、游标或 crawler outbox；
- First-Seen 仅在复用现有类型、访问状态与 Enrich outcome 状态机确认结果为 retryable/dead-letter 后，才用独立短事务保存 Candidate、未补全 Content 和累计 attempts；成功或权威 terminal 不提前提交。Publication 失败时这些真实失败证据保留，而 Hash、Observation、游标和 crawler outbox 仍全部回滚；
- 完整扫描按频道跨 Run 恢复显式标记为 `first_seen_ledger_status=pending` 的 First-Seen checkpoint，不依赖 `Content.last_observation_id`、原 Run 继续执行或当前 Uploads 页面再次返回该 ID，也不绕过 `next_retry_at` 重抓详情。partial 扫描可以正常结束但不能消费 ledger；下一 Run 在成功事务内按 `candidate_id` 原子改为 `consumed` 并关联唯一 Observation，竞争失败的并发事务不能生成第二份 `first_seen`/disposition。恢复事务同时刷新 Hash 并进入 Publication；事务失败时 claim 回滚，原 Candidate 不重复写入，Task attempts 和退避时间不重置；
- Crawler Observation 在共享 writer 入口显式锁定 Channel `FOR NO KEY UPDATE`，随后才领取幂等键并锁定频道/域 cursor；Migration、Incremental 和其他 Observation 调用者因此统一遵守 Channel-before-cursor 顺序。同频道重叠事务仍按 cursor 串行取得 sequence，Video lifecycle 后续取得 Channel 锁只是同一事务重入；
- First-Seen ledger 的运行时 schema 只在约束缺失时添加 `NOT VALID` CHECK/FK，不再每次启动 drop/re-add 已验证约束，也不在启动事务中普通创建大表索引。受控 CLI 必须使用专用 `FIRST_SEEN_LEDGER_ADMIN_DATABASE_URL(_FILE)` 直连 PostgreSQL，明确拒绝 PgBouncer 主机，并在执行任何 session 设置或 advisory lock 前核对数据库名和后端端口；随后在显式 Candidate 最低行数和 pending 数量确认后，以 `CREATE INDEX CONCURRENTLY` 建立 `(channel_id,candidate_id)` pending 索引，再 `VALIDATE CONSTRAINT`。preflight/postflight 核对 CHECK 表达式、FK 列/目标/动作/延迟属性和完整索引 predicate，而不是只看对象名称；fresh bootstrap 直接包含最终约束和索引；
- Clock 第二事务重新锁定并验证 reservation，随后把 success/terminal Task、Content、Video Item Hash、Observation、游标、crawler outbox 和 `channel + video` Publication Reconciler 一起提交。Publication 失败时第二事务全部回滚，前置短事务已记录的失败 attempts 保留；失败路径按 fence 尽力恢复未消费 reservation，进程崩溃或 cleanup 故障则由现有 expired-running 回收路径接管；
- 主事务一旦提交，cleanup 只是缩短未消费 reservation 恢复时间的补偿优化，不能反转 Job、Domain 或 Run 的成功结果。cleanup 故障返回并持久化 `reservation_cleanup_deferred=true`，已消费 Task 保持 done/terminal，未消费 reservation 等待 lease 到期后按新 generation 接管；
- 拆分事务不能把 Recent Sampling 从 `post_discovery_current` 偷换成旧快照；规划查询用本轮 Uploads 日期证据对既有 Content 做只读 overlay，因此刚补到 `published_at` 的已知视频仍可在同一轮采样，而真正的 Discovery/Content/游标写入继续等第二事务与 Publication 原子提交；
- Queue Publication 部分失败时，异常只携带已确认提交的 Enrich outcome；`leased -> running` claim 与 `claimed` 事件同事务，fenced Task 状态迁移与 `checkpointed` outcome 事件同事务。监控只统计这两类已提交事件，不再依赖 Rota 最后一条 Job 事件，因此换身份前已落库的 claim/retry/dead-letter 不会在后续换线成功时消失，也不会把已回滚的 done/terminal 误报为成功；
- Rota 增加真实 `content_enrich` task kind，并限定为 channel role；没有伪装成 Full Crawl 或 Incremental；
- Enrich 使用独立 Worker 服务、独立 Queue concurrency 和新增的 channel Slot 容量。示例配置为 2 个 Enrich Worker、每实例并发 1、总 channel Slots 从 40 增至 42。

Clock 互斥与安全切换：

- 数据库设置 `content_enrich_dispatch.mode` 默认为 `clock`；只允许 `clock` 或 `queue`；
- Clock 在自己的写事务内以共享锁读取 mode。`queue` 模式下，它不消费开放任务或已到期 terminal；任何模式下都跳过有效 Enrich lease 和尚未到期的 terminal；只有 `clock` 模式可消费已到期 terminal；
- Controller 只在 gate 已启用且 mode=`queue` 时派发；模式更新在短事务内按统一顺序取得 xact advisory lock、锁定并校验已提交 mutex，再锁 mode 行，因此切换不会落在派发或 Clock 写事务中间，也不会形成反向锁序；
- 回滚先切回 `clock`。已领取的 Worker Job 可以排空；Clock 会等待有效 lease 完成或过期后再接管，不能同时消费同一任务。

private/unavailable 的低频复查继续使用同一条 `content_enrich_tasks` 事实和同一个 Clock/Controller 所有权开关，不新增表、Worker 轮询器或第二套调度器。第一版不为其他历史 `job_type` 扩大范围。

运行可观测性已经接入 Controller 和 Dashboard：

- Controller 按独立、可配置的数据库采样周期统计 `player-refresh` 的 queued/leased/running/failed/terminal/dead_letter/done、最老 queued 年龄，以及已提交 outcome 的 claim/success/retry/terminal/dead-letter 数量、每分钟速率和比例；默认每 60 秒采样一次，不再随 15 秒 Controller tick 全表统计；
- PostgreSQL 聚合在独立事务中设置 transaction-local `statement_timeout`，Controller 侧另有 wall-clock deadline；两者默认均为 5 秒并由 `CONTENT_ENRICH_METRICS_QUERY_TIMEOUT_SECONDS` 配置，既限制数据库执行，也覆盖连接池或事务入口停滞；
- 监控刷新使用 single-flight。超时或失败时继续返回旧数据库快照，并只更新本 tick 的 BullMQ/mutex/dispatch 字段；启动时尚无旧快照则在 deadline 后由现有 Controller 故障隔离记录错误并继续后续调度，不会无限卡住主循环；
- 缓存周期内数据库指标和 `observed_at` 保持稳定，BullMQ 开放 Job 数、mutex contention 和派发结果仍逐 tick 更新，避免仅因时间戳变化每 15 秒写一条等价 `controller_ticks`；
- 当前派发是否遇到 mutex contention、BullMQ 开放 Job 数和派发是否成功与同一快照一起写入 `crawler.controller_ticks`；
- backlog 和 queued age 超阈值时产生带 raised/reminder/resolved 状态的结构化告警日志，并隔离指标查询或派发异常，不能阻断 Controller 后续正常工作；
- Dashboard `/health` 和只读 `/api/content-enrich/operational` 返回最近快照，并区分 ok、alerting、stale 和 unavailable。

### 9.3 本地验证

- qybullmq 全量按文件执行时沙箱内 220/224 通过；Build Images、Business Publication HTTP、Fingerprint Gateway 和 yt-dlp Session 四个环境受限文件解除本地进程/监听限制，并使用现有测试 Python 虚拟环境后共 5/5 子测试通过。因此 224 个测试文件均已实际验证，无业务断言失败；
- Content Enrich 真实 PostgreSQL 16 生命周期 2/2 通过、0 skip，覆盖已提交 mutex 争用和过期接管、`SKIP LOCKED`、Worker 在 `queue.add()` 内即时 claim 的事务可见性、投递崩溃恢复、attempts/实际失败时间退避、generation fencing、heartbeat、行锁等待后 lease 过期、Rota checkpoint fencing、terminal、工程 dead-letter、Content、Item Hash、真实 Publication revision/outbox、部分失败指标和重复 Job；
- runtime schema 和 fresh `database/bootstrap/crawler.sql` 均在空 PostgreSQL 16 测试库完整应用；bootstrap 默认 mode、cursor 和 mutex 已读取验证；First-Seen 在线迁移另在临时数据库中 1/1 通过，覆盖旧索引的并发替换、CHECK/FK 完整定义验证、错误同名约束拒绝、行数守恒和重复执行；
- Incremental Video PostgreSQL 9/9 通过、0 skip；除存量 public success、authoritative terminal 与 retry 混合批次外，还覆盖重复 First-Seen 不完整详情第一次 Publication 失败后只累计一次 attempts、保留 Candidate/占位 Content 且不提交发布数据；旧 Run 随后以 partial 正常结束，中间 Full Crawl 将同一 Content 改写为新的 `run_id`，再由下一 Clock Run 在 Uploads 已不再返回该视频时按频道恢复原 Candidate，并恰好一次提交 `first_seen`/disposition、Observation、crawler outbox、Hash、游标以及真实 Publication revision/current/outbox；另覆盖两个并发恢复事务只有一个原子消费 ledger，以及主事务成功后 cleanup 事务入口故障仍返回 complete、保留 Task/Content/Hash/游标/outbox；
- Enrich、Clock、Controller 接线的针对性单元回归全部通过；Migration、Query、Full Crawl、Content Enrich 和正常 Incremental 聚焦回归 25/25 文件通过。共享 Observation writer 的显式 Channel-before-key-before-cursor 契约和真实 Migration/Incremental 双事务 PostgreSQL 验收 1/1、Full Crawl PostgreSQL 1/1、Content Enrich PostgreSQL 2/2 通过；Dashboard 前序 3/3 通过；
- 本次最终修正未修改 Rota；前序分支验收已有 `internal/proxycontrol` 通过记录，但当前宿主没有 `go` 可执行文件，因此本次未独立重跑 Go 测试；
- Node 语法检查、`git diff --check` 和隔离 PostgreSQL 测试均通过。

这些是隔离测试证据，不代表生产积压已经下降。

### 9.4 上线、监控与回滚

上线顺序必须保持：

1. 应用 crawler 运行时兼容 schema，确认 mode 仍为 `clock`；该步骤只添加缺失列和 `NOT VALID` 约束，不创建 First-Seen 大表索引；
2. 准备只用于本次 DDL、直连 PostgreSQL（生产拓扑为 `crawler-postgres:5432`）的 `FIRST_SEEN_LEDGER_ADMIN_DATABASE_URL` 或 `_FILE`；禁止填写 `crawler-pgbouncer:6432`。只读确认目标数据库名、PostgreSQL 后端端口、`content_candidates` 最低行数和 pending ledger 数量，设置 `EXPECTED_FIRST_SEEN_LEDGER_POSTGRES_SERVER_PORT`、`CONFIRM_FIRST_SEEN_LEDGER_SCHEMA_APPLY`、`EXPECTED_CRAWLER_CANDIDATE_MIN_ROWS`、`EXPECTED_FIRST_SEEN_LEDGER_PENDING_COUNT` 后执行 `npm run schema:first-seen-ledger-online`；确认两个约束完整定义正确且 validated、pending 索引为 `(channel_id,candidate_id)` 且 predicate 精确为 `first_seen_ledger_status='pending'`；超时则保持 `clock` 并安全重跑，不能跳过；
3. 先部署支持 `content_enrich` task kind 的 Rota；
4. 确认实际提供至少 42 个 ready channel Slots/代理，而不只是修改期望值；
5. 部署 Queue、Controller 和 Worker 代码，保持 `CONTENT_ENRICH_DISPATCH_ENABLED=false`，并显式确认监控查询 deadline（默认 `CONTENT_ENRICH_METRICS_QUERY_TIMEOUT_SECONDS=5`）；
6. 启动 Enrich Worker，确认独立并发和 Rota 身份正常；
7. 开启 Controller gate，但 mode 仍保持 `clock`；
8. 先执行 `npm run content-enrich:mode -- queue` dry-run，再提供 operator、reason 和精确确认值执行 `--apply`；
9. 小流量观察后再调整 High Water、refill 或 Worker 数量，不能一次性放大。

代码已提供 Queue 开放 Job 数、queued/leased/running/failed/terminal/dead_letter/done、最老 queued 年龄、claim/success/retry/terminal/dead-letter rate、当前 dispatch mutex contention、积压和年龄阈值告警。上线仍需把结构化告警日志接入现有告警平台，并联合监控 High Water、mutex expiry/heartbeat、Worker lease renewal/loss、恢复投递失败、过期 lease、`reservation_cleanup_deferred`、attempts 与 next retry、terminal access source、`last_enriched_at`、Item Hash、Publication revision/outbox，以及 Rota channel ready/claimed 和 `content_enrich` Task 结果。

回滚时先把 mode 切为 `clock`，再关闭 Controller gate；保留 Worker 让已领取 Job 排空，或接受它们在 lease 到期后由 Clock 接管。不得先强停 Worker 后直接让 Clock 忽略有效 lease，也不得直接批量改 BUG-7 数据。

剩余风险是生产代理容量和单条详情延迟尚未用真实积压压测，结构化告警日志也尚未在生产告警平台验证送达。在线约束验证和并发索引只在隔离 PostgreSQL 验证过；生产仍须在 `clock` 模式下观察执行时长、锁等待和磁盘空间，任何 guard 不一致或 timeout 都必须停止发布并重跑 preflight。监控全表聚合若持续超过 deadline，Controller 会保持运行但 Dashboard 将显示旧快照；金丝雀必须同时观察 stale 状态和数据库查询耗时，必要时再基于生产 `EXPLAIN` 优化索引或改为增量指标。heartbeat 能覆盖正常长抓取；若数据库不可续租，Worker 会 fail closed，数据不会被旧 Worker 覆盖，但当前底层抓取适配器可能要等正在执行的单次上游调用返回后才能完全退出，仍可能浪费一次请求。`incrementalVideo.js` 当前已超过 2,600 行，Clock reservation/First-Seen checkpoint 状态机宜在本次行为稳定后拆成独立深模块；为避免在上线热修中同时改变接口和事务行为，本次不做该重构。上线后必须用 p95/p99 单条和批次时长校准 batch size、lease 与 heartbeat，并观察失租率。

### 9.5 生产验收

- 新任务进入队列后无需等待下一次频道 Clock；
- 同一任务不会被 Clock 和 Drain 同时执行；
- queued 总量和最老年龄连续下降；
- 正常 Incremental Clock 吞吐不下降超过预设阈值；
- retryable 任务保留真实 attempts 并按有上限退避重试；预算耗尽进入 dead_letter，权威终态进入 terminal 并按低频计划复查，不永久无解释 queued；
- Migration、Query、Full Crawl、正常 Incremental 和 Publication 的吞吐及错误率不回归。

## 10. BUG-7：历史未 enrich 视频

### 10.1 最终结论

**历史缺口成立；它是 BUG-6 的存量结果，不是 BUG-1 的结果，也不需要第二套恢复系统。代码候选已通过隔离验收，但未直接修改这些数据；只有生产 Drain 经正常抓取、写入、Hash 和 Publication 链实际消化后，BUG-7 才能关闭。**

2026-08-21 原始查询结果：

| 指标 | 数量 |
|---|---:|
| `last_enriched_at IS NULL` | 46,626 视频 |
| 频道 | 321 |
| 有 queued enrich task | 46,626 |
| 无 task | 0 |
| 最早首次写入 | 2026-07-23 |
| 最晚首次写入 | 2026-08-12 |

它们全部来自 Incremental `clock_due` runs，并且都有恢复任务。

这些记录不可能是 BUG-1 直接产生的残留：BUG-1 的问题路径在 INSERT 前返回 `null`，不会生成 `crawler.contents` 行；而 BUG-7 统计的前提是已经存在 contents 行。

BUG-7 是 BUG-6 积压中的历史数据集合，不应建立第二套恢复系统。

2026-08-23 后续核对约有 45,770 个视频 `last_enriched_at IS NULL`，这些视频全部已有 Enrich Task；对应 `queued/player-refresh` 总量约 46,103、涉及约 324 个频道。数量会随 Clock 继续运行而变化，但“未补全视频全部已有任务”这一归因不变。

### 10.2 具体解决方案

只由 BUG-6 的 Enrich Drain 消化：

1. P0：活跃频道最近 90 天或发布时间未知的视频；
2. P1：活跃频道的更老历史视频；
3. P2：非活跃/休眠频道视频；
4. 各层继续按频道公平批量补抓已有 content ID，不重新跑整个频道；
5. 通过正常详情抓取和共享 Writer 更新 `contents` 与 `last_enriched_at`；
6. private/unavailable 权威证据更新访问状态、关闭本轮并进入同一任务的低频复查；
7. 刷新 Item Hash，再经 Publication Reconciler 更新发布链；
8. 不直接批量 UPDATE 这 45,770 行，也不为它们创建第二张恢复表或第二个调度器。

### 10.3 验收

- 当前存量任务最终进入 done/terminal/dead_letter 之一；处理中 retryable 必须保留真实 attempts 和 `next_retry_at`，不能无限重试；
- 不存在无任务的 `last_enriched_at IS NULL` 行；
- queued 总量和最老年龄持续下降，失败与 terminal 比例可解释；
- P0 层先于 P1/P2 清空，且单个大频道不能霸占 Worker；
- 成功任务的 Content、`last_enriched_at`、Item Hash 和 Publication 同步完成；
- 不增加全频道重复扫描量。

## 11. BUG-8：写事务偶发进入 read-only

### 11.1 最终结论

**真实系统性 P0，且原评审的“一个 Worker”结论已经过时。**

最终快照共有 4 个失败计划：

- 1 个频道被 YouTube Community Guidelines 移除，属于正常终态；
- 3 个频道报 `cannot execute INSERT in a read-only transaction`；
- 3 个失败分布在 4 个不同 Incremental Worker 执行尝试中；
- 部分 BullMQ Job 已达到第 5 次尝试；
- 失败 execution attempt 仍残留为 running，说明失败收尾也有缺口。

运行态事实：

- PgBouncer 使用 `pool_mode = transaction`；
- PostgreSQL 只有一个主库，`pg_is_in_recovery() = false`；
- 服务器、数据库和 `bullmq` 角色的默认事务只读设置均为 `off`；
- 外部审计脚本 `/tmp/claude-0/.../scratchpad/q.sh` 执行了 session 级 `SET default_transaction_read_only=on`；
- 脚本修改时间为 `2026-08-21 06:59:12.097 UTC`，第一条只读写入错误发生在 `06:59:12.371 UTC`，间隔约 0.27 秒；
- transaction pooling 会把 PostgreSQL 后端连接交给不同客户端复用，因此被审计脚本污染的后端随后随机交给 Worker；
- 成功与失败交错、多个 Worker 同时受影响，正是“少量后端连接被污染”的表现。

因此，根因不是 PostgreSQL 权限不足、只读副本路由或 Worker 自身事务逻辑，而是审计脚本修改了共享后端连接的 session 状态。

### 11.2 具体解决方案

#### 第一层：消除污染来源

- Worker 继续使用 PostgreSQL 角色的默认读写权限和普通 `BEGIN`，不增加额外 SQL；
- 正式源码、运行配置和审计脚本禁止启用 session 级默认事务只读；
- 只读对账使用已有的只读数据库角色，并把 `READ ONLY` 限定在当前事务；
- 普通人工审计只执行 `SELECT`；需要防误写时使用 `BEGIN ... READ ONLY`，结束后 `ROLLBACK`；
- 增加源码门禁测试，禁止该危险设置再次进入生产代码。

#### 第二层：一次性清理受污染连接

- 在 Worker 切换窗口等待正在执行的事务结束；
- 让 PgBouncer 重新建立目标数据库的后端连接，清除遗留 session 状态；
- 不修改数据库角色权限，不修改业务数据；
- 不启用每事务 `DISCARD ALL`，避免给所有正常事务增加额外往返。

#### 受影响镜像与运行角色

| 镜像 | 缺陷位置 | 必须更新的持久角色 |
|---|---|---|
| `qy-allpachong/dashboard` | Dashboard 的 Business 对账连接请求 session 级默认只读 | `qy-newcrawler-dashboard-1` |
| `qy-allpachong/qybullmq` | `scripts/cloneMigrationRuntimeConfig.js` 被复制进统一镜像 | API、Controller、全部 Queue Worker、Feature Relay、Crawler Outbox Publisher、Publication Ingress/Reconciler/Projector/Publisher |

`feature-engine`、`feature-dispatch`、`auth`、`rota-core`、`rota-dashboard` 和 Nginx 的源码与运行镜像均未发现该设置，不需要为了 BUG-8 重建。PostgreSQL 与 PgBouncer 也不需要换镜像，只需要在应用切换窗口清理一次现存后端连接。

#### 第三层：修复失败收尾（独立缺口）

- failed/aborted execution attempt 必须设置 finished_at；
- plan retry 完成后旧 attempt 不得继续保持 running；
- 修复后重放 3 个失败频道；
- 频道移除仍按 terminal removed 处理，不进入重试。

### 11.3 回归测试与验收

1. 源码门禁扫描生产目录，发现 session 级默认只读设置即失败；
2. 默认 Worker 连接完成 `SELECT / INSERT / UPDATE / DELETE` 冒烟测试；
3. 同一 PgBouncer 后端在事务级只读审计结束后可被 Writer 正常复用；
4. 清理连接池后进行 1000 次事务压力测试，必须没有只读写入错误；
5. 3 个失败频道重放成功；
6. execution attempts 没有无期限 running 残留。

### 11.4 当前实施状态

- 已移除 Dashboard 审计连接和迁移配置复制脚本中的 session 级默认只读设置；
- Worker 的普通 `BEGIN` 和 PostgreSQL 默认读写权限保持不变；
- 已增加生产源码、数据库初始化、部署、运维脚本和运行配置门禁测试；
- Dashboard 测试通过；
- QYBullMQ 完整测试共 969 项，911 项通过、58 项按环境条件跳过、0 项失败；
- Dashboard、53 个运行中的 QYBullMQ 角色均已部署固定镜像
  `pachongsys-3e65734-pgbouncer-readonly`，全部 0 重启；
- Publication Ingress/Reconciler/Projector/Publisher 已由唯一源码仓库中的
  `deploy/compose.qy-publication-runtime.yml` 接管；
- 已执行 PgBouncer `RECONNECT bullmq_crawler_migration`；
- 五个实际数据库运行角色均通过正式表零行 `UPDATE` + `ROLLBACK` 探针；
- 1,000 次并发事务覆盖 17 个后端 PID，0 个只读错误、0 行持久变更；
- 最终确认 12 个受影响 Run，全部 Run=`done`、Plan=`succeeded`，0 条
  affected attempt 仍为 `running`；
- `UCLGNJYRIp1fY2l7KQq-uAxA` 使用原 BullMQ Job 和原 Plan/Run 恢复成功，
  没有创建重复 Run；
- Reconciler 的 Business PostgreSQL `/dev/shm` 错误是独立存量事故，记录为
  `INC-20260821-009`，不属于本 BUG 的回归。

## 12. BUG-9：Publisher/Relay 仍归属旧 Compose

### 12.1 最终结论

**部署归属违规成立，当前数据链路故障未出现。**

诊断时，两个容器仍由 `/root/workspace/FeatureEngine/docker-compose.yml` 管理：

- `qy-crawler-outbox-publisher`；
- `qy-feature-relay`。

诊断时镜像为无 revision label 的旧 `bullmq-crawler-qy:latest`。

进一步哈希核对发现：两个入口文件、Publisher 主实现和 Feature Transport 与唯一源码一致，但旧镜像的 `db.js`、`queues.js` 等依赖并不完全一致，且缺少当前数据库连接模块。因此不能仅凭入口文件一致就宣称整条执行闭包一致。

诊断时的功能证据：

- crawler outbox：313,031 published，仅 1 条刚创建的 pending；
- feature inbox：313,031 applied；
- 没有 Publisher/Relay 积压。

所以这是需要接管的部署债务，不是当前 P0 数据故障。

### 12.2 具体解决方案

1. 把两个服务的 Compose 定义迁入 `pachongsys/deploy`；
2. 从唯一源码构建带 revision label 的 qybullmq 镜像；
3. 固定同一 crawler PostgreSQL、Feature PostgreSQL、Redis 和队列名；
4. 先等待 pending 接近 0；
5. 停止旧 Publisher/Relay；
6. 启动新角色并验证 claim lease、幂等 job id 和 Feature Inbox；
7. 观察至少一个完整 Clock 批次；
8. 从旧 FeatureEngine Compose 删除这两个角色，防止双启动；
9. 发布门禁增加“生产容器必须有源码 revision label”。

### 12.3 当前实施状态

- 唯一源码新增 `deploy/compose.qy-feature-bridge-runtime.yml` 和固定项目名的
  `scripts/feature-bridge-compose.sh`；
- `qy-crawler-outbox-publisher`、`qy-feature-relay` 已由该 Compose 接管；
- 两个容器均运行不可变 `pachongsys` QYBullMQ 镜像，Compose labels 指向
  `/root/workspace/agent/pachongsys`，0 次重启；
- 当前镜像为 `pachongsys-2e6fc73-query-closure`，完整 Git revision 为
  `2e6fc73a3e417de9343394f666b65b85b26b1b36`；全套 53 个 QYBullMQ 运行角色
  均为该 revision、0 次重启；
- 旧 `/root/workspace/FeatureEngine` 容器被保留为已停止回滚证据，不再消费
  生产队列；
- 编排回归测试和 `docker compose config --quiet` 均通过；真实 Observation
  已经经 Publisher、Relay 到达 Feature Inbox 并变成 `applied`。

## 13. BUG-10：Feature Ingest 运行旧镜像

### 13.1 最终结论

**成立。**

诊断时运行中的 `qy-feature-ingest` 使用：

```text
qy-feature-engine:video-contract-compat-20260815-v2
```

其 `contracts.py` 和 `events.py` 哈希与唯一源码不同。正在运行的新版 Scheduler 镜像中，同三个关键文件与唯一源码逐字节一致，证明正确构建已经存在。

诊断时 Inbox 尚未形成可见积压；但契约和事件解析版本漂移具有真实风险。

### 13.2 具体解决方案

1. 从当前唯一源码构建专用 Feature Ingest 镜像标签；
2. 不因为 Scheduler 镜像内容相同就直接复用其标签，先验证 entrypoint、healthcheck 和环境契约；
3. 运行 Feature Engine contracts/events/ingest tests；
4. 停止旧 ingest，保持数据库和 endpoint 不变，单实例切换；
5. 用一条真实 Observation 做 canary；
6. 核对 crawler outbox published、feature inbox applied、channel feature state 更新；
7. 观察无 rejected/waiting_gap 增长后完成接管；
8. Compose 归属统一回 `pachongsys/deploy`。

### 13.3 当前实施状态

- `qy-feature-ingest` 已由唯一源码 Feature Bridge Compose 接管，容器健康且
  0 次重启；
- 旧镜像漂移问题已消除；运行镜像为
  `qy-allpachong/feature-engine:pachongsys-8b919ed-feature-ledger`，完整 Git
  revision 为 `8b919ed41874a77f897c94d7dca75ca3b2ef03b9`；
- Video disposition ledger 契约缺口 `INC-20260822-013` 已修复，183 项 Feature
  Engine 回归和完整仓库测试通过；
- 原失败事件 `136780fa-f93f-4bf5-acd1-d9c5d3712abf` 已受控重试成功，Feature
  Inbox 记录 Video sequence 12、`complete/applied`；
- Crawler Observation Outbox 的 324,439 条 `published` 与 Feature Inbox 的
  324,438 条 `applied` 加 1 条历史 `rejected` 完全对账；历史拒绝是已被
  sequence 13 覆盖的 `QY-BUG-012`，不属于当前回归。

## 14. BUG-11：`fervent_mccarthy` 游离容器

### 14.1 最终结论

**遗留容器成立，但不是业务 Worker。**

其启动命令只是动态 import 若干模块并打印 `module-import-ok`。因为被 import 的模块保留了事件循环句柄，容器持续运行；它没有 Worker 启动命令、没有 Compose 归属，也没有证据表明它消费生产队列。

### 14.2 具体解决方案

1. 保存只读 inspect 证据和创建时间；
2. 确认无队列注册、无正在执行 job；
3. 停止并删除该容器；
4. 增加运行态清单检查：生产容器必须具有 Compose project、service 和 revision label；
5. 冒烟测试使用 `--rm`，测试完成后自动退出。

## 15. BUG-12：48 个 qybullmq 容器落后 HEAD

### 15.1 最终结论

**版本号事实成立，但不能据此判定 48 个角色存在功能 BUG。**

48 个常规 qybullmq 容器运行 revision `08437c4`。从该 revision 到 HEAD，qybullmq 生产源码只变更：

- `businessPublicationProjector.js`；
- `publicationCurrentReconciliation.js`。

常规 Channel/Incremental/Query/Agent/Data API Worker 不 import 这两个文件。真正的 Business Projector 已单独运行 `f055471` 镜像；Reconciliation 文件只由管理脚本调用。

因此：

- 48 个容器的全仓库 commit label 落后是真的；
- 它们执行的抓取逻辑并未因此少两个修复；
- 不能为了“所有镜像等于全局 HEAD”无差别重启正在跑 Clock 的 48 个角色。

### 15.2 具体解决方案

1. 继续保留单一 qybullmq 源码，不复制角色代码；
2. 构建产物记录 `git_revision` 和 `component_source_digest`；
3. 建立 role -> runtime entrypoint -> transitive source files 清单；
4. 发布门禁比较组件 digest，而不是简单要求所有组件等于仓库 HEAD；
5. 共享基础文件变化时，才要求所有 qybullmq 角色滚动升级；
6. 仅 Projector 文件变化时，只升级 Projector；
7. 下次常规 qybullmq 发布时把 48 个角色滚动到当前构建，作为治理收敛，不作为紧急故障修复。

## 16. BUG-13：Agent Clock 今日为 0

### 16.1 最终结论

**不存在全域调度漏排，但存在 1 个冷启动恢复缺口。**

数据库实际状态：

| 指标 | 数值 |
|---|---:|
| 活跃频道 | 23,455 |
| `agent_due_day <= 2026-08-21` | 0 |
| 今日计划 `run_agent=true` | 0 |
| 最早 Agent 到期日 | 2026-10-06 |
| 最晚 Agent 到期日 | 2027-09-16 |
| 从未完成 Agent 的活跃频道 | 1 |
| 最近一次 Agent 完成时间 | 2026-08-20 11:34 UTC |

Agent semantic policy 的稳定区间为 60 至 365 天。就今日全域排程而言，`0 due -> 0 planned` 符合调度器规则。

但唯一一个从未成功完成 Agent 的活跃频道不能被这个总体结论掩盖：

```text
channel_id              = UCvt-54g-zNs6jmt8uie48TA
agent_last_complete_at  = NULL
agent observations      = 0
finalized profile       = ready_partial
current_output_hash     = NULL
agent_due_day           = 2026-11-05
agent_tier              = 90
```

该频道历史 Agent profile 为 failed，没有 observation、output hash、input content hash 或 input content IDs，却被推迟到 90 天后。这不是“今天所有 Agent 都漏排”，而是失败且从未产出结果的频道被错误套用了正常稳定频道的到期策略。

### 16.2 具体解决方案

不对 13,000 个今日计划做全量补排。增加调度不变量与 Dashboard：

```text
agent_due_now
agent_planned_today
next_agent_due_day
last_agent_complete_at
never_completed_agent_count
```

告警条件应是：

```text
agent_due_now > 0 AND agent_planned_today < agent_due_now
```

同时增加冷启动不变量：

```text
active channel
AND agent_last_complete_at IS NULL
AND current_output_hash IS NULL
  -> 必须立即进入 baseline recovery，或写入明确 terminal reason
```

先对上述 1 个频道执行可审计的 Agent recovery；成功后按正常策略计算下次 due，失败则记录明确原因和有上限的退避，不能直接进入 90 天稳定周期。

因此不能简单看到 `run_agent=0` 就告警，也不能只看 `agent_due_now=0` 就断言没有任何 Agent 边界缺口。

如果产品要求“每次出现重要新视频都立即运行本地 Agent”，这是策略变更：应基于 evidence/content hash 变化把 Agent clock 提前，而不是把所有 Incremental Clock 都强制设为 `run_agent=true`。

## 17. 正确修复顺序

| 批次 | 内容 | 原因 |
|---|---|---|
| 第一批 | BUG-1 + BUG-2 | 当前每天持续丢失新视频并重复抓取，直接影响完整性和性能 |
| 第一批 | BUG-8 | 已扩散到多个 Worker，会随机使任何 About/Video 写入失败 |
| 第二批 | BUG-6 + BUG-7 | 约 46,103 queued、45,770 未 enrich，现有 Clock 消化能力不足 |
| 第三批 | BUG-5 | 修复溯源契约和历史 source 缺口，避免发布门禁拒绝 |
| 第三批 | BUG-3 | 精确修复 305 条历史 disabled 数据并增加约束 |
| 第四批 | BUG-10 | 用唯一源码接管 Feature Ingest |
| 第四批 | BUG-9 | 接管 Publisher/Relay 的 Compose 归属 |
| 清理批 | BUG-11 | 删除无业务作用的游离容器 |
| 治理批 | BUG-12 | 建立组件级 source digest 和角色发布门禁 |
| 单点恢复 | BUG-13 | 不全量补排；修复 1 个无成功输出频道，并增加冷启动不变量 |
| 不修改 | BUG-4 | 当前行为符合既定契约，只补监控和说明 |

BUG-1 与 BUG-8 可以并行开发，但上线应分别做小流量 canary，避免把视频入库语义和数据库事务改动绑成一个难以回滚的大版本。

## 18. 明确禁止的“快速修法”

以下改法看起来简单，但会制造更严重的数据错误：

1. 不得把 Uploads 中未明确为 short/live 的条目直接权威认定为 video；
2. 不得在存在 retryable deferred 视频时强行推进增量游标；
3. 不得只把 comments disabled 写成数值 0 而丢失 `comments_disabled=true`；
4. 不得只给发布白名单增加 unlisted，而不修改整个业务权限契约；
5. 不得给缺失 `_source` 的历史数值伪造来源；
6. 不得同时启动两个无协调接管过程的 Publisher/Relay；
7. 不得因为仓库 HEAD 变化就无差别重启所有角色；
8. 不得通过无限提高 Player 重试次数来掩盖状态机缺口。

## 19. 上线总验收

修复完成后必须同时满足：

- 新发现视频 `silent_drop_count = 0`；
- 每个未入库 ID 都有 structured disposition 和 reason；
- cursor 只在所有 ID closed 时前进；
- 生产连接不存在 session 级默认只读污染；
- 无 read-only INSERT 失败；
- enrich queued 数量和最老年龄持续下降；
- disabled/count、resolved/source 契约数据库层可验证；
- unlisted 继续按 `source_unlisted` 正常排除；
- Outbox/Relay/Feature Ingest 均由唯一源码构建和管理；
- 运行容器具备 project、service、revision/component digest；
- Agent due 与 daily plan mask 的不变量成立；
- Incremental、Migration、Query、Publication、Business Projection 分别完成回归，不用一个链路的成功代替另一个链路的验证。
