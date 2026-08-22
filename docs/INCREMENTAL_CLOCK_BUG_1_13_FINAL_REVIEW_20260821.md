# Incremental Clock BUG-1 至 BUG-13 最终复核与解决方案

## 1. 文档目的

本文对 `DATA_COMPLETENESS_AUDIT_20260821.md` 及其后续 Claude 评审中提出的 BUG-1 至 BUG-13 逐项复核。

复核目标不是证明原评审“整体对或整体错”，而是把四种不同性质的问题分开：

1. 当前代码会持续制造数据缺口的真实缺陷；
2. 历史坏数据仍未修复，但当前写入逻辑已经正确；
3. 容量、积压或部署治理问题；
4. 既定业务契约或保护机制，被误判成 BUG。

本文最初只做评审和方案设计。BUG-8 根因确认后，唯一源码已完成最小修复与本地回归测试；截至本文最后更新，尚未部署该修复，也未修改生产数据库或业务数据。

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
| BUG-6 | **积压成立，“没有消费者”不成立** | 补全吞吐不足 | P0/P1 |
| BUG-7 | **历史未补全成立，归因 BUG-1 错误** | BUG-6 的存量结果 | 随 BUG-6 处理 |
| BUG-8 | **成立，根因已确认是外部审计脚本污染共享 PgBouncer 后端** | 数据库连接状态污染 | P0 |
| BUG-9 | **已修复并由唯一源码接管** | 源码/部署治理 | P2 |
| BUG-10 | **镜像漂移已修复；新契约缺口另记 INC-20260822-013** | Feature Ingest 契约 | P1 |
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

**305 条异常数据真实存在，但“disabled 放错桶”这个代码根因不成立。**

数据库证据：

| 指标 | 数值 |
|---|---:|
| `comment_count_status='disabled'` 且 count 非 NULL | 305 |
| 频道 | 37 |
| `publication_item_hash IS NULL` | 305 |
| 最早首次写入 | 2026-07-18 05:00 UTC |
| 最晚首次写入 | 2026-07-18 06:31 UTC |
| 最晚被更新 | 2026-07-26 06:00 UTC |

这批数据是集中产生的历史坏数据。

当前写入代码已经执行：

```text
comments_disabled=true
  -> comment_count=NULL
  -> comment_count_status='disabled'
```

`disabled` 表示作者关闭评论，评论总数不可得，不表示评论数为 0。因此它应属于“无数值但已终态”的集合，而不是 resolved numeric 集合。

### 6.2 具体解决方案

1. 不修改 `videoPublicationCurrent.js` 的 disabled 分桶；
2. 编写一次性、可审计的数据修复脚本，只处理精确谓词：
   `comment_count_status='disabled' AND comment_count IS NOT NULL`；
3. 把 `comment_count` 设为 NULL，保留 `comments_disabled=true` 和已有来源；
4. 刷新 `publication_item_hash`；
5. 通过正式 Repair Revision 和 Publication Reconciler 更新业务 Current，不修改历史 Revision；
6. 修复后增加数据库约束：
   `comment_count_status='disabled' -> comment_count IS NULL AND comments_disabled=true`；
7. 约束先 `NOT VALID` 上线，历史修复完再 `VALIDATE`，避免长时间锁表。

### 6.3 验收

- 精确 305 条被修复；
- 其他 comment 状态不变；
- 305 条重新生成合法 item hash；
- 当前 Writer 的 disabled 测试继续通过；
- 新写入 disabled + count 非 NULL 在数据库层直接拒绝。

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

**严重积压成立，“没有消费者”不成立。**

最终核对时：

| 状态 | 任务数 | 频道数 | 说明 |
|---|---:|---:|---|
| done / player-refresh | 18,477 | 1,189 | 今天仍在持续完成 |
| queued / player-refresh | 47,127 | 373 | 最老创建于 7 月 23 日 |

Incremental Clock 会读取 queued/failed task，把它们放到最近视频采样候选最前面，并在成功后更新为 done。因此 Clock 本身是消费者。

真实问题是：

- 只有频道下一次 Video Clock 到期时才有机会处理；
- 每个计划仍受 player cap 限制；
- 单频道可能有上百个任务；
- 任务表具有 lease 字段和 claim 索引，但没有独立常驻 drain worker；
- 处理能力低于历史积压形成速度。

### 9.2 推荐解决方案：增加独立 Enrich Drain 角色

继续保留 Clock 的顺带补全能力，同时增加专用消费者：

```text
content_enrich_tasks
  -> FOR UPDATE SKIP LOCKED 原子 claim
  -> 按 channel 公平分片
  -> 调用现有 fetchIncrementalVideoDetail / RotaSlotAdapter
  -> 成功更新 contents + item hash
  -> done / terminal / retryable_failed
  -> Publication Reconciler
```

实现要求：

1. 复用现有 qybullmq 源码与镜像，只增加角色入口，不复制一套抓取代码；
2. claim 必须使用 lease owner、lease expiry 和 `SKIP LOCKED`；
3. Clock 读取任务时必须跳过有效 lease，避免重复抓；
4. 每个频道设置公平配额，不能让大频道吃满所有 Worker；
5. 最近 90 天、业务窗口内、活跃频道优先；
6. 历史归档视频使用低优先级；
7. private/unavailable 等权威状态关闭当前 enrich 任务，并转入低频 access recheck；
8. retryable challenge 才允许换线重试，并设置累计预算；
9. 暴露 queued age、claim rate、success rate、terminal rate、retry rate；
10. 当 backlog 或最老年龄超过阈值时告警。

若不新增独立角色，至少应把每个频道的 pending 数量纳入计划成本，并动态提高该频道 player cap；但这会挤压正常增量任务，长期不如独立 drain 清晰。

### 9.3 验收

- 新任务进入队列后无需等待下一次频道 Clock；
- 同一任务不会被 Clock 和 Drain 同时执行；
- queued 总量和最老年龄连续下降；
- 正常 Incremental Clock 吞吐不下降超过预设阈值；
- 失败任务最终进入明确 terminal 或 dead-letter，不永久 queued。

## 10. BUG-7：历史未 enrich 视频

### 10.1 最终结论

**历史缺口成立，但数量和归因需要修正。**

最终查询结果：

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

### 10.2 具体解决方案

由 BUG-6 的 Enrich Drain 消化，并按优先级分层：

1. P0：当前 90 天业务窗口内、活跃频道、尚未发布完整数据；
2. P1：活跃频道的更老视频；
3. P2：休眠频道和纯历史归档视频；
4. 已明确 private/unavailable 的任务关闭本轮 enrich，并转入低频 access recheck；
5. 每批完成后刷新 item hash 和 Publication Current；
6. 不重新跑整个频道，只按已有 content ID 补详情。

### 10.3 验收

- 46,626 条全部进入 done/terminal/retry-dead-letter 之一；
- 不存在无任务的 `last_enriched_at IS NULL` 行；
- P0 队列先清零；
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

两个容器仍由 `/root/workspace/FeatureEngine/docker-compose.yml` 管理：

- `qy-crawler-outbox-publisher`；
- `qy-feature-relay`。

镜像为无 revision label 的旧 `bullmq-crawler-qy:latest`。

进一步哈希核对发现：两个入口文件、Publisher 主实现和 Feature Transport 与唯一源码一致，但旧镜像的 `db.js`、`queues.js` 等依赖并不完全一致，且缺少当前数据库连接模块。因此不能仅凭入口文件一致就宣称整条执行闭包一致。

当前功能证据：

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
- 旧 `/root/workspace/FeatureEngine` 容器被保留为已停止回滚证据，不再消费
  生产队列；
- 编排回归测试和 `docker compose config --quiet` 均通过。

## 13. BUG-10：Feature Ingest 运行旧镜像

### 13.1 最终结论

**成立。**

运行中的 `qy-feature-ingest` 使用：

```text
qy-feature-engine:video-contract-compat-20260815-v2
```

其 `contracts.py` 和 `events.py` 哈希与唯一源码不同。正在运行的新版 Scheduler 镜像中，同三个关键文件与唯一源码逐字节一致，证明正确构建已经存在。

当前 Inbox 全部正常 applied，说明尚未形成可见积压；但契约和事件解析版本漂移具有真实风险。

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
- 旧镜像漂移问题已消除，运行镜像来自 `pachongsys` 的不可变构建；
- 接管后的真实事件复验发现新版 Video disposition ledger 尚未进入 Feature
  Engine 契约，该独立兼容故障已记录为 `INC-20260822-013`；
- disposition 契约修复和 183 项 Feature Engine 回归已在源码完成，生产镜像
  切换与失败事件重放仍是关闭该新事故的最后门禁。

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
| 第二批 | BUG-6 + BUG-7 | 47,127 queued、46,626 未 enrich，现有 Clock 消化能力不足 |
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
3. 不得把 comments disabled 当作 resolved numeric 0；
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
