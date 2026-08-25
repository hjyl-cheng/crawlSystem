# 当前系统优化方案

## 1. 目标与范围

本方案只优化当前正在运行的系统，不假设正在进行数据迁移，也不使用老库数据外推当前系统。

目标：

1. 消除 `creator_search_current` 每次发布复制全部 Creator 的 O(N) 写放大。
2. 消除 Business Inbox 与 Business Revision 之间已经确认的 payload 重复。
3. 建立当前系统自己的容量基线，再决定 Snapshot、raw payload 和索引是否需要继续优化。
4. 保留现有跨库隔离、Revision、Outbox、顺序/Hash 校验、灰度和回滚能力。

本阶段不做：

- 不合并爬虫数据库和业务数据库。
- 不重写 Event Ledger 或发布子系统。
- 不删除 `result.*_current.payload_json`。
- 不提前拆分 `content_snapshots`。
- 不仅根据 `idx_scan=0` 删除索引。

## 2. 当前系统的读写链路

Projector 是后台写入模块，不在 KOL 请求链路中。写链路与读链路必须分开理解。

### 2.1 后台写入链路

```text
Business Revision
        ↓
Reconciler / Activator
        ↓
result.*_current
        ↓
projection_outbox
        ↓
Projector（后台异步 worker）
        ↓
public.channel_snapshots / content_snapshots / facts / metrics
        ↓
creator_search_live（Creator 搜索当前表）
```

### 2.2 KOL 查询链路

```text
KOL 前端
    ↓ HTTP
KOL Backend / API
    ↓ SQL，直接查询业务数据库
creator_search_live / Snapshot 相关表
    ↓
返回查询结果
```

KOL 请求不会经过 Projector。Projector 负责提前生成查询数据，KOL Backend/API 负责直接读取这些数据。

## 3. 当前已确认的问题

### 3.1 Creator Search 发布写放大

当前实测：

| 指标 | 数值 |
| --- | ---: |
| `creator_search_releases.changed_channel_count` 累计 | 208 |
| `publication.creator_search_changes` | 208 行 |
| `creator_search_live` | 197 行 |
| `creator_search_current` | 5,280 行 |
| 当前累计写放大 | 25.4 倍 |

正常发布由 Projector 调用 `refresh_creator_search_release_v9`。在 `shadow` 模式下，
该函数继续调用 Legacy 刷新链路，根据上一 active watermark 构建完整的 Creator Search release：

```text
本次变化 k 个 Creator
        ↓
从上一 active watermark 复制未变化的 N-k 行
        +
根据最新 Snapshot 重新生成变化的 k 行
        ↓
creator_search_current 新增约 N 行
        ↓
替换 creator_search_live 中变化的 k 行
        ↓
追加 k 行 creator_search_changes
```

随着 Creator 数量增加，单次发布成本按 N 增长；当发布次数也随 Creator 数量增长时，累计成本接近 O(N²)。

系统已经实现增量模式。`refresh_creator_search_release_v9` 会先保存 prior watermark，
再临时移除 active watermark，使 Legacy 刷新链路无法复制上一完整 release：

```text
本次变化 k 个 Creator
        ↓
creator_search_current 只临时生成变化的 k 行
        ↓
DELETE + INSERT creator_search_live 中对应的 k 行
        ↓
记录 k 行 creator_search_changes
        ↓
删除 creator_search_current 中本次临时行
```

增量模式把单次发布从 O(N) 降为 O(k)。它不是零写入：Live 表的 k 行
`DELETE + INSERT` 需要维护 16 个索引，`creator_search_changes` 追加 k 行，
临时 Current 行也会产生 O(k) 的 WAL、死元组和索引写入。因此仍需监控
autovacuum、WAL 和表/索引膨胀，但这些成本不再随总 Creator 数量 N 增长。

需要特别区分正常发布和回滚回放：`replay_creator_search_release_v9` 在
`shadow` 模式下会从 `creator_search_live` 全表重建指定 watermark 的 Legacy release；
这是 rollback/replay 路径，不是 Projector 的正常发布路径。

### 3.2 Inbox 与 Revision 重复保存 payload

当前实测 615/615 行：

```text
inbox.received_envelope->'payload'
        =
revision.payload_json
```

`publication.inbox` 当前约 5,336 kB，其中约 4,928 kB 是 envelope 对应的 TOAST 数据。这是当前系统已经确认的线性冗余。

### 3.3 容量趋势缺少当前系统自己的长期数据

当前系统只运行了约 1～2 轮，尚不能证明 `content_snapshots` 已经出现多轮放大。老库数据只作为风险提示，不作为本方案的优化依据。

### 3.4 当前切换就绪度快照

2026-08-25 的实测状态如下；这些值说明当前具备切换条件，但执行时仍必须重新读取，
不能把文档中的快照当作 cutover 参数：

| 检查项 | 当前观测值 |
| --- | ---: |
| `creator_search_live` | 197 行 |
| active Legacy release | 197 行 |
| Legacy/Live parity diff | 0 |
| `channel_ownership` | 197 个，全部 `online/active` |
| `projection_outbox` | 208 条，全部 `delivered` |
| 数据盘 | 299 GB 总量 / 254 GB 已用 / 45 GB 剩余（86% 已用） |

Creator Search 存储 cutover 与按频道执行的投影 cutover 是两件事。前者切换
`creator_search_storage_state`，不使用 `held_shadow`，也不写
`publication.projection_cutover`。

## 4. 优化一：完成 Creator Search 增量切换

这是第一优先级，使用系统已有的 `shadow/legacy -> incremental/live` 能力，不改 Projector 的外部接口。

### 4.1 切换函数的事务内保证

`activate_creator_search_incremental_v1` 会在持有发布 advisory lock 的同一事务内强制检查：

- active watermark 必须等于调用方提供的 `p_expected_watermark`。
- 当前状态必须是 `write_mode='shadow'`、`read_mode='legacy'`。
- Live、active Legacy 和 `p_expected_live_count` 三者行数必须相同。
- 除 `watermark` 外，Legacy 与 Live 必须逐行完全一致。

任一条件不满足都会抛异常并回滚。人工查询用于提前发现问题和生成调用参数，
最终安全保证由函数内部的事务、锁和强制 parity 检查提供。

### 4.2 运行前置条件

1. 记录磁盘总量、已用量、剩余量、WAL 目录余量和生产告警阈值。当前已用 86%，
   在执行历史清理或任何表重写前必须先清理可安全删除的运行产物或扩容；不得把删除业务数据作为临时腾挪手段。
2. 核查 KOL Backend/API 的真实 SQL，并对搜索、筛选、排序、分页和 Creator 详情
   分别执行 Legacy/Live 结果对账。
3. 安排短静默窗口：临时暂停 Projector 发布，确认没有活跃发布批次，且
   `projection_outbox` 没有 `pending/retry_wait/leased` 行。
4. 在静默窗口内重新读取 active watermark、Live 行数和 Legacy 行数，并立即将
   watermark 与 Live 行数作为 cutover 的 expected 参数。
5. 保存 active watermark、Live 行数、Legacy 行数、磁盘基线和完整回滚参数。

发布 advisory lock 可以保证并发时不会产生不一致状态，但并发发布可能先拿到锁并改变
active watermark，导致 cutover 因 expected watermark 失效而回滚。静默窗口的目的，是避免
这种可预期的失败和重试，不是替代函数内部的一致性检查。

`read_mode` 只是数据库状态，不会自动改写外部 SQL。如果 KOL Backend/API 仍直接查询 `creator_search_current`，必须先让其通过一个稳定的读取接口选择 Legacy 或 Live；存储表切换的知识不能分散到多个查询调用方。

### 4.3 执行切换

```text
[磁盘余量与告警状态确认]
        ↓
[进入短静默窗口并确认无在途 Projection]
        ↓
[读取 expected watermark / Live count]
        ↓
[调用现有 guarded cutover]
        ↓
write_mode: shadow      → incremental
read_mode:  legacy      → live
        ↓
[发布一个单 Creator 变更作为 Canary]
```

函数会在事务内再次执行全量 parity；无需用人工检查替代该保证。

### 4.4 验收条件

Canary 变化一个 Creator 时必须满足：

- `changed_channel_count = 1`。
- 对应 watermark 的 `creator_search_changes = 1`。
- `creator_search_live` 只有该 Creator 发生变化，总行数符合新增/删除语义。
- `creator_search_current` 净行数不增长，但每次发布仍有 k 行 INSERT + DELETE 周转；
  对应的死元组、WAL 和 autovacuum 工作量必须保持 O(k)，不能重新退化为 O(N)。
- KOL 搜索、筛选、排序和分页结果正确。
- 单次发布耗时、WAL 增量和磁盘增量不再与总 Creator 数量 N 成正比。

### 4.5 回滚与清理

切换后先保留现有 5,280 行 Legacy 数据作为回滚基线，不立即 prune。只有在 Live 读取稳定、回滚演练通过并超过约定回滚窗口后，才执行已有的 Legacy prune 流程。

`creator_search_changes` 是增量回滚和 replay 的硬依赖。回滚会使用
`before_document`，向前 replay 会使用 `after_document`，并对恢复后的 Live 行做精确校验。
因此必须在 cutover 时同时确定以下策略：

```text
creator_search_changes 最短保留范围
  = 最老允许回滚的 watermark
    到当前 active watermark 的完整连续变更链
```

Changes 保留窗口不得短于回滚窗口。在创建新的完整 checkpoint 并验证可恢复之前，
不得删除这条连续链中的 release 或 change。缩短 changes 保留范围，就等价于缩短可回滚距离。

切换只是停止 O(N) 复制继续发生，不会立即释放现有表文件空间。

## 5. 优化二：Business Inbox 指针化

目标是让成功接收的 Inbox 只记录“收到什么”，完整业务 payload 只由 Business Revision 保存。

### 5.1 目标存储模型

```text
publication.inbox
  revision_id
  publication_stream_id
  channel_id / domain / data_sequence
  payload_hash / envelope_hash
  receipt_id / receive_status
  error_code / timestamps / receive_count
  received_envelope = NULL（正常接收）
        ↓ revision_id
publication.revision
  完整 revision metadata
  source_json
  payload_json
```

保留规则：

- `accepted`、`waiting_gap`、`waiting_ownership`：Inbox 不保存完整 envelope，Revision 保存完整数据。
- `rejected`、`conflict`：继续保存完整 envelope 或等价隔离证据，因为这些记录可能没有可引用的 Revision。

### 5.2 实施步骤

```text
[允许 received_envelope 为空并增加状态约束]
        ↓
[验证可由 Inbox 元数据 + Revision 重建接收证据]
        ↓
[停止为正常接收写入完整 envelope]
        ↓
[观察重复接收、Gap、Ownership、冲突和隔离路径]
        ↓
[超过回滚窗口后清理历史正常 envelope]
```

Schema 兼容、停止新写和历史清理必须分成独立发布，不能在一次变更中同时完成。

### 5.3 验收条件

- 正常接收仍能完成幂等判断、顺序判断、Hash 校验和 Revision 激活。
- 相同 `revision_id` 的重复请求仍返回原 receipt。
- 相同 `revision_id`、不同 envelope 的请求仍进入 conflict/quarantine。
- 拒绝和冲突记录保留完整故障证据。
- 新正常 Inbox 行不再产生 envelope TOAST 数据。

## 6. 优化三：建立当前系统容量基线

完成前两项后，让当前系统自然运行 3～5 个完整抓取轮次，再重新测量。

每轮至少记录：

- Creator、视频、Revision 和 Snapshot 新增行数。
- 各主要表的 heap、TOAST、索引增量。
- `content_snapshots` 每视频版本数分布及 p50/p95/p99。
- `raw_item`、`raw_channel` 的逻辑字节和物理 TOAST 字节。
- `creator_search_changes` 和 releases 的增长速度。
- `pg_stat_user_indexes.idx_scan`、`idx_tup_read`、`idx_tup_fetch` 的绝对值和每轮增量。
- `pg_stat_database.stats_reset`，避免把统计重置后的 0 次扫描误判为长期未使用。
- Projector 每批耗时、WAL 增量、重试和 dead letter。
- autovacuum 运行频率、死元组和表膨胀。

容量模型必须拆成固定成本和可变成本：

```text
总容量
  = 固定 Schema/索引成本
  + 每 Creator 成本
  + 每视频成本
  + 每 Revision 成本
  + 每抓取轮次历史成本
  + 每次 Search 变化日志成本
```

不能再使用“当前总量 / 197 个频道”直接线性外推全部规模，因为其中混有固定页、固定索引和批次级成本。

## 7. 后续动作的触发条件

以下项目不立即实施，只在当前系统数据达到触发条件后进行：

| 触发条件 | 后续动作 |
| --- | --- |
| 同一视频出现多轮语义相同 Snapshot | 增加写时判重或 Snapshot 保留窗口 |
| `raw_item/raw_channel` 持续占 Snapshot 主要字节 | 改为 Revision/Hash 引用；无 Revision 来源的历史数据再考虑 MinIO |
| 某索引在代表性统计周期内无扫描且不是约束依赖 | 通过查询计划验证后删除 |
| 已超过 §4.5 固化的回滚窗口，且已有验证通过的新 checkpoint | 按连续 watermark 链裁剪过期 changes/releases |
| KOL 读取不再依赖完整 `result.*_current.payload_json` | 再评估 Current 规范化，不提前删除 |

## 8. 最终实施顺序

```text
[磁盘余量、WAL 余量和统计基线检查]
        ↓
[KOL 读取 SQL 与 Legacy/Live parity 审计]
        ↓
[进入短静默窗口并确认无在途 Projection]
        ↓
[重新读取 expected watermark / Live count]
        ↓
[Creator Search incremental/live cutover]
        ↓
[单 Creator Canary + 回滚演练]
        ↓
[Business Inbox 指针化]
        ↓
[运行 3～5 轮并建立容量基线]
        ↓
[按触发条件处理 Snapshot/raw/index/changes retention]
        ↓
[稳定期结束后 prune Legacy Search 历史]
```

每一步都必须能够独立验收和独立回滚。流量/读取切换、历史数据删除和 Schema 移除不得合并在同一个发布中。

## 9. 最终结论

当前系统不需要全面重构。最合理的优化顺序是：

1. 打开已经实现的 Creator Search 增量模式，消除当前最严重的 O(N) 单次发布写放大。
2. 将正常 Business Inbox 改为 Revision 指针，删除已经字节级确认的 payload 副本。
3. 用当前系统运行 3～5 轮后的真实数据决定是否继续优化 Snapshot、raw payload、索引和历史保留。

这三步只改变模块内部存储实现，不改变爬虫到业务数据库的分发接口，也不改变 KOL Backend/API 直接查询业务数据库的使用方式。

## 10. 当前分支已落地的实现

当前实现只提供可审计的 Schema、管理命令和测量工具，没有对运行中的数据库执行
cutover、历史清理或数据删除。

### 10.1 Creator Search 存储管理

命令：

```text
npm run publication:creator-search-storage
npm run publication:creator-search-storage -- --apply
npm run publication:creator-search-storage -- --rollback
npm run publication:creator-search-storage -- --rollback --apply
```

默认和 `--rollback` 均只生成只读计划。写操作必须显式传入 `--apply`，并要求
`CONFIRM_BUSINESS_CREATOR_SEARCH_STORAGE` 与计划输出的确认串完全一致。确认串绑定：

- Business 数据库名和预期 Channel 数。
- 操作类型、操作人和原因。
- 当前 active watermark、Live/Legacy 行数、parity 和存储模式。
- rollback target 及其 Legacy 行数（回滚时）。

Apply 会在 `SERIALIZABLE` 事务中获取 Creator Search 发布 advisory lock，重新读取全部
状态，阻断在途 Projection、模式漂移、watermark 漂移、行数漂移和 parity 差异，然后才
调用数据库已有的 guarded cutover/rollback 函数。

该命令只能证明数据库内条件。KOL Backend/API 不在本仓库中，因此其真实 SQL、
Legacy/Live 查询结果和查询计划仍必须按 §4.2 在上线前独立审计；不得仅凭命令输出
`ready=true` 省略这个外部检查。

### 10.2 Inbox 指针化发布顺序

已实现兼容迁移：

1. `businessPublicationSchema.sql` 先将 `received_envelope` 改为可空，并增加状态约束。
2. 旧版 Ingress 在兼容 Schema 上仍可继续写完整 object。
3. 新版 Ingress 启动时强制确认列可空且新约束已验证，否则拒绝启动。
4. 新版 Ingress 为 `accepted`、`waiting_gap`、`waiting_ownership` 写 SQL `NULL`，并将完整
   payload 写入 Revision；`rejected`、新建 `conflict` 和 `inbox_conflict` 继续保留完整证据。

本次没有实现或执行历史 envelope 清理。旧正常行继续保留 object，不影响新旧版本滚动
发布；历史清理必须等观察期和回滚窗口结束后再单独设计、预览和执行。

### 10.3 容量基线

命令：

```text
npm run publication:storage-baseline
npm run publication:storage-baseline -- --output <new-file.json>
```

该命令没有 apply 模式，在单个 `REPEATABLE READ READ ONLY` 快照中记录：

- 数据库身份、Channel 数、数据库字节、WAL LSN 和 `stats_reset`。
- public/publication/result 下各表 heap、TOAST、索引、总字节和 vacuum/analyze 状态。
- 每个索引的字节、`idx_scan`、`idx_tup_read`、`idx_tup_fetch`、唯一/主键属性和定义。
- Creator Search 模式、Live/Legacy/Change/Release 行数和累计变化数。
- Channel、Content identity、Revision、Snapshot、Current 和 Projection Outbox 的精确行数。
- Content Snapshot 每视频版本 p50/p95/p99/max，以及 raw Item/Channel 逻辑和存储字节。
- Projection Outbox 各状态行数与最大 attempts，以及 Projector batch 耗时分位数。

应在每个完整抓取轮次后保存一份新文件，以 `stats_reset` 为共同起点计算增量；报告本身
不会给出“删除索引”的结论。
