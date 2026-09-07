# Full Crawl YouTubeJS 独立抓取与检查点重构设计

## 1. 文档状态

- 状态：主体实现、队列关闭及 Query / Migration 抓取中断与重复投递集成验证完成；默认保持 legacy，待其余故障点、Rota 换线及真实 canary 验收
- 初版日期：2026-09-04
- 实施日期：2026-09-04
- 验证更新：2026-09-07
- 灰度准备补充：迁移专用队列路由及恢复保护已加入工作区并通过验证，但尚未部署；当前 Rota 40 个频道槽位全部占用，需先确认资源安排。最新验证结果和部署范围见 [单 Worker 迁移灰度准备](FULL_CRAWL_MIGRATION_CANARY_20260907.md)。下方 36 项定向 / 1653 项全量统计是加入灰度路由之前的阶段记录，最新为 52 项定向全过、1657 项全量中 1504 通过 / 153 跳过 / 0 失败。
- 目标仓库：`pachongsys-query-incremental-refactor`
- 目标链路：Migration / Query 共用的 Full Crawl 抓取阶段
- 参考实现：`pachongsys-incremental-refactor @ 89052c7`
- 参考文档：`INCREMENTAL_YOUTUBEJS_FETCH_CHECKPOINT_REFACTOR_DESIGN_20260902.md`

本文用于冻结开发范围、流程、Interface、检查点、错误语义、灰度方式和验收标准，并记录实际实施结果。

当前实现没有新增数据库表，复用 `channel_runs`、`content_candidates` 和 JSONB receipt。新建 Run 的默认抓取契约仍是 `legacy_full_v2`；只有显式配置 canary 后，新建的普通 `channel-snapshot` 才会选择 `youtubejs_full_v1`。历史 Run、Recovery 和 Repair 均继续使用 legacy。

当前验证状态：纯模型、执行器恢复、契约冻结、Store 不变量、Runtime 能力选择、Finalize 确定性投递和静态范围测试已通过。2026-09-07 在隔离 PostgreSQL 16 与 Redis 7 实例完成定向测试：`36 passed / 0 skipped / 0 failed`，包括原有 30 项 Full Crawl 相关测试、2 项共享 Redis 能力测试，以及新增 4 项新执行器跨进程恢复测试。

原有 PostgreSQL Store 测试使用单连接外层回滚事务，覆盖准入、Uploads 固化、目标冲突、Data API 状态冲突、Detail 提交与 Close Fetch 恢复。新增集成测试直接调用生产入口 `executeFullCrawlYoutubeJs()`，使用真实数据库事务、真实 BullMQ Worker 子进程及真实 Finalize 投递，只在 YouTubeJS Adapter 边界返回固定资料。Query 与 Migration（启用近期活跃度判断）各运行两个 `SIGKILL` 场景：第 5 个视频请求开始后崩溃，以及抓取和 Handoff 已提交但任务尚未向队列确认完成时崩溃；四项全部通过。

重启后 BullMQ 实际产生 stalled 恢复事件；同一 Run 和目标 hash 保持不变，已提交的前 4 个视频不再抓取，仅处理第 5～10 个。已完成 Fetch 的恢复和显式重放均零 YouTube 请求；相同 Job ID 在 active/completed 状态重复投递不新增任务，Finalize 始终只有一份且 revision 不变。正常退出还验证了真实下游队列关闭、重复关闭及进程自然退出。测试未启动完整生产 `worker.js` 的 Rota 管理链路，也未运行 Agent / Finalize 消费者或 Publication，因此不能替代 Rota 换线、其余崩溃位置和真实 YouTube 全链路 canary 验收。验收未修改业务数据库、线上队列或默认抓取契约。

2026-09-07 全量回归：`1653 tests / 1500 passed / 153 skipped / 0 failed`。在 `services/qybullmq` 运行 `node --test --test-skip-pattern='real curl_cffi|persistent yt-dlp' test/*.test.js`，全量回归不设置集成测试连接变量，因此新增 4 项集成用例在该命令下跳过，但已在前述隔离定向运行中实际通过。两个 Python 依赖相关测试按名称明确跳过，其余跳过项依照测试自身的环境条件处理。队列关闭异常注入尚未覆盖。`git diff --check` 通过。

### 1.1 阅读约定

为避免开发时对中英文状态词产生不同理解，本文固定使用以下含义：

| 文中名称 | 中文含义 |
|---|---|
| Full Crawl | 一个频道的一次完整抓取 |
| Business Run | 任务在进入 Worker 前就冻结的业务执行身份 |
| Channel Run / Run | `crawler.channel_runs` 中的一次完整抓取记录 |
| Channel Candidate | `crawler.channel_candidates` 中等待准入的频道候选 |
| Video Candidate / Candidate | `crawler.content_candidates` 中等待详情抓取的视频候选；没有前缀时由上下文区分 |
| executor | 执行完整抓取流程的执行器 |
| Interface | 调用方必须遵守的最小调用契约，不只是函数参数 |
| checkpoint | 已经提交到 PostgreSQL、可用于恢复的权威事实 |
| Fence | 判断当前 Job 是否仍拥有写入权的执行权凭证 |
| Admission | 抓取频道资料、判断准入、注册频道并创建 Run |
| Uploads | 从频道内容页抓取并固化本 Run 的视频目标列表 |
| Video Detail | 逐条抓取视频详情及评论第一页 |
| Close Fetch | 不再访问 YouTube，只从数据库校验并收口抓取结果 |
| Handoff | 把已完成的抓取结果交给现有 Agent / Finalize 链路 |
| Route Failure | 确认由当前网络线路引起、允许处罚 Proxy 或换线的失败 |
| canonical | 数据库中决定恢复和最终业务结果的权威事实 |

## 2. 一句话决定

新增独立的 `fullCrawlYoutubeJs.js`，使用 YouTubeJS 完成频道资料、视频列表、视频详情和评论抓取；每条可信结果及时写入 PostgreSQL，重试时只恢复未完成阶段；旧 `processChannelCrawlV2()` 完整保留并负责排空旧 Run。

新脚本不得导入、包装或回调旧 `processChannelCrawlV2()`，也不得调用旧 HTML、yt-dlp 或 YouTube Data API 作为抓取回退。

恢复模型固定为：BullMQ 和 Rota 负责让同一个执行器再次进入；PostgreSQL 负责冻结 Business Run、目标集合、单条结果和完成状态。BullMQ 不是 Full Crawl 业务现场的事实来源。

## 3. 正确的重构范围

### 3.1 Full Crawl 的起点和终点

本设计中的 Full Crawl 从一个已经建立持久化身份的 Channel Candidate 开始，到抓取事实完成并交回现有 Agent / Finalize 链路为止。

```text
Query Discover 产生频道候选 --------+
                                  |
Migration 产生频道候选 ------------+--> Full Crawl 抓取
                                            |
                                            +--> 现有智能分析
                                            +--> 现有最终汇总
                                            +--> 现有发布链路
```

Query Quality 和 Discover Search 是 Query 的上游，不属于本轮 Full Crawl 重构。

Agent、Finalize、Publication 是 Full Crawl 的下游。本轮只保持交接契约，不重构它们的内部实现。

### 3.2 Query 和 Migration 的关系

Query 和 Migration 不拥有两套 Full Crawl：

```text
Query Channel Candidate -----+
                             +--> 同一个 Full Crawl YouTubeJS executor
Migration Channel Candidate -+
```

两者的抓取顺序、YouTubeJS 解析、检查点和错误语义完全相同。

唯一业务差异是：Migration 开启“近期内容活跃度判断”，Query 不开启。

### 3.3 本轮只替换抓取实现

本轮保留以下现有业务语义：

- Candidate Attempt Fence；
- Business Run identity；
- 频道订阅数准入；
- Channel Registry promotion；
- 内容类型判定；
- 发布时间窗口；
- Video disposition；
- Migration Activity Gate；
- Agent 调度；
- Finalize 和 Publication；
- Rota 的线路选择和处罚策略；
- BullMQ 的投递、重试和 stalled recovery。

本轮改变的是这些业务规则之间的编排方式，以及所有 YouTube 数据的获取方式。

## 4. 为什么不能继续修改旧 pipeline

当前 `processChannelCrawlV2()` 同时承担：

- Candidate 和 Run 身份校验；
- Channel Snapshot；
- 多抓取器回退；
- 频道准入；
- Registry promotion；
- About Observation；
- Uploads；
- Migration Activity Gate；
- `content_candidates` 固化；
- 内联或队列化 Video Detail；
- yt-dlp 和 Data API 补字段；
- Detail 汇总；
- Finalize 派发；
- 多种 Repair 特例。

当前抓取层是：

```text
频道资料：YouTubeJS -> HTML -> yt-dlp -> Data API
视频列表：YouTubeJS -> yt-dlp
视频详情：YouTubeJS -> yt-dlp -> Data API
评论数据：YouTubeJS / yt-dlp / Data API 交叉补充
```

这会导致一次 Full Crawl 中出现多种来源拼接、失败语义漂移，以及重试后重新执行已经完成的网络阶段。

继续在旧函数内删除回退，只会让旧函数保留全部历史分支和 Repair 耦合，不能形成像 Incremental 新脚本一样清楚的恢复路径。

因此采用 replace-and-drain：新增完整实现，旧实现不动，新旧并存，按 Run 的不可变抓取契约选择。

## 5. 目标结构

### 5.1 新旧实现并存

```text
Full Crawl Job
      |
      v
读取 Business Run 固定的抓取契约
      |
  +---+---------------------------+
  |                               |
  v                               v
legacy_full_v2               youtubejs_full_v1
  |                               |
  v                               v
processChannelCrawlV2()      executeFullCrawlYoutubeJs()
旧实现完整保留               新实现只使用 YouTubeJS
```

环境变量 `FULL_CRAWL_FETCH_CONTRACT_DEFAULT` 只能决定“新建 Run 默认选择哪个契约”，不能决定一个已有 Run 在本次重试时使用哪个 executor。未配置时固定为 `legacy_full_v2`。

### 5.2 新模块的唯一生产 Interface

新模块的生产导出固定为：

```js
export async function executeFullCrawlYoutubeJs(
  job,
  { resumeMode = "initial" } = {},
)
```

调用方只需要知道：

1. Job 已经经过现有 Channel Runtime 和 Business Run preparation；
2. Job 对应的不可变抓取契约是 `youtubejs_full_v1`；
3. 函数成功返回时，当前可完成的 Full Crawl 抓取工作已经提交；
4. 函数抛出线路错误时，现有 Managed Worker / Rota 负责换线和重试；
5. 函数不会把内部阶段状态、YouTubeJS object 或 continuation object 返回给调用方。

建议返回兼容当前 Channel Crawl 日志和调用方的形状：

```js
{
  ok: true,
  channel_id: string,
  candidate_id: number | null,
  run_id: string | null,
  resumed: boolean,
  skipped: boolean,
  skip_reason: string | null,
  candidate_count: number,
  detail_processed: number,
  detail_status: "pending" | "running" | "done" | "failed",
  migration_activity_gate: object | null,
  fetch_contract: "youtubejs_full_v1",
  executed_phases: string[],
  phase_timings_ms: object,
}
```

具体阶段、SQL、client ladder 和恢复判断全部属于模块 Implementation，不进入生产 Interface。

测试可以通过 `fullCrawlYoutubeJsFactory.js` 的 factory 注入 YouTubeJS Adapter、PostgreSQL adapter、时钟和队列派发函数；生产 Worker 仍只调用一个 executor。

模块另导出生命周期方法 `closeFullCrawlYoutubeJsQueues()`，仅负责关闭懒创建的下游队列连接。Worker 必须先停止接单并等待正在运行的任务结束，再关闭这些队列，最后关闭数据库。关闭方法等待所有队列关闭结果，保留失败连接供再次关闭，并向调用方汇总错误；它不是第二个抓取入口。

## 6. 不可变抓取契约

### 6.1 为什么必须冻结

BullMQ attempt、Worker 进程、Proxy Route 和部署环境都可能变化，但同一个 Business Run 不能从旧抓取器切到新抓取器，也不能在 YouTubeJS 抓到一半后回退 yt-dlp。

因此每个 Full Crawl Run 必须绑定一个不可变抓取契约：

```json
{
  "executor_id": "youtubejs_full",
  "executor_version": 1,
  "contract_hash": "sha256:..."
}
```

稳定标识写作：

```text
youtubejs_full_v1
```

### 6.2 冻结位置

Channel Run 尚未 materialize 前，唯一可用的稳定身份是 `crawler.business_run_bindings`。

新建 binding 时，把抓取契约写入已经参与 `intent_hash` 的 `intent_json.intent.fetch_contract`：

```json
{
  "intent": {
    "fetch_contract": {
      "executor_id": "youtubejs_full",
      "executor_version": 1,
      "contract_hash": "sha256:..."
    }
  }
}
```

Channel Run materialize 时，再把同一份契约镜像到：

```text
channel_runs.result_json.fetch_contract
```

两处值必须一致。镜像只用于 Run 侧审计和快速校验，Business Run immutable intent 是 Run 创建前的身份事实。

### 6.3 兼容规则

- 已存在但没有 `fetch_contract` 的 binding 和 Run 一律解释为 `legacy_full_v2`；
- 新建 Business Run 必须先选择契约，再调用 `BusinessRunBindingStore.resolve()`，并把显式契约写入新 binding 的 immutable intent；
- 恢复已有 binding 时，先读取它已经保存的 immutable intent，再计算有效契约；部署后的默认值不得参与历史 intent 的重新计算；
- 历史 binding 的“缺少字段”等价于 legacy，只能接受 legacy 请求；若请求以 YouTubeJS 契约认领它，必须报 contract mismatch；
- 不回填或原地修改历史 `intent_json`、`intent_hash`；历史 Run 的镜像字段也不为了格式统一而补写；
- 不允许部署默认值把历史记录自动解释为 YouTubeJS；
- Job payload 可以携带 contract hash 作为冲突校验，但不能成为唯一事实；
- retry、stalled recovery、换 IP 和进程重启必须继承原契约；
- 回滚只改变之后创建的 Business Run，不能改变已存在 Run；
- contract mismatch 必须 fail closed，不能猜测或回退。

### 6.4 Contract manifest 和版本规则

`contract_hash` 不是对包含自身的 `fetch_contract` JSON 求哈希，而是对仓库内固定的 canonical manifest 求哈希。首版 manifest 至少冻结：

```json
{
  "executor_id": "youtubejs_full",
  "executor_version": 1,
  "channel_source": "youtubejs",
  "uploads_source": "youtubejs",
  "detail_source": "youtubejs",
  "comments_source": "youtubejs",
  "fallback": "forbidden",
  "detail_concurrency": 1
}
```

规则如下：

- manifest 使用 canonical JSON 和稳定字段顺序计算 SHA-256；
- 实际 YouTubeJS 包版本、client、Proxy 和部署版本写 telemetry，不写进语义契约；
- 改变数据来源、required surfaces、fallback、阶段语义或提交粒度时，必须发布新 executor version 和新 hash；
- 普通缺陷修复若不改变上述语义，可以继续实现 v1；
- Worker 必须保留所有尚未排空版本的实现，未知 version/hash 必须拒绝执行；
- Run 镜像只允许“原来不存在时写入”或“与 binding 完全相同时重放”，不得覆盖成另一个契约。

2026-09-07 评论策略调整使用 `youtubejs_full_v2`：在 v1 manifest 基础上增加 `comments_required=false`、`comments_fallback=empty_top_to_newest_once`，executor_version 改为 2；`fallback=forbidden` 仍表示禁止跨抓取引擎兜底。v2 hash 为 `sha256:cdd2d1843c89057feb0638f6d6fc4b801dd76832c6effde0a551ca3103fcf911`。v1 manifest/hash 原样保留，已冻结 v1 的 Run 仍按原必需评论规则续跑；新灰度 Run 使用 v2。不得改写旧 binding/Run 为 v2。

## 7. 新 Full Crawl 完整流程

### 7.1 Migration 主流程

```text
旧 results.db 中的频道
          |
          v
     创建迁移任务
          |
          v
     创建频道候选记录
          |
          v
  使用 YouTubeJS 抓取
  频道资料和简介
          |
          v
  判断频道是否符合要求
          |
     +----+---------------------+
     |                          |
     v                          v
频道不存在、已迁移          频道符合要求
或订阅数不合格                  |
     |                          v
     v                  保存频道资料
结束该频道的迁移            创建完整抓取记录
                                |
                                v
                     使用 YouTubeJS 抓取视频列表
                                |
                                v
                     第一次判断频道近期是否活跃
                                |
               +----------------+----------------+
               |                                 |
               v                                 v
      明确超过 90 天没有内容              有近期内容或暂时无法确定
               |                                 |
               v                                 v
        标记为休眠频道                    保存待抓取的视频列表
        不再抓取视频详情                         |
        不进入智能分析                           v
               |                       逐个抓取视频详情和评论
               |                       每完成一个立即保存
               |                                 |
               |                                 v
               |                       再次判断频道是否活跃
               |                                 |
               |                    +------------+------------+
               |                    |                         |
               |                    v                         v
               |               确认已经休眠             活跃或仍无法确定
               |                    |                         |
               |                    |                         v
               |                    |                    进入智能分析
               |                    |                         |
               +--------------------+-------------------------+
                                    |
                                    v
                              汇总最终结果
                                    |
                                    v
                              发布到业务系统
```

### 7.2 固定内部阶段

新 executor 内部阶段固定为：

```text
Claim / Restore
    -> Admission
    -> Uploads
    -> Video Detail
    -> Close Fetch
    -> Handoff
```

中文含义：

```text
认领并恢复
    -> 抓取频道资料并完成准入
    -> 抓取并固化视频列表
    -> 逐个抓取视频详情
    -> 从数据库汇总抓取结果
    -> 交给现有下游
```

阶段顺序不能由调用方选择，也不能通过 Job payload 跳过。恢复位置只能由 PostgreSQL 已提交事实推导。

### 7.3 谁负责冻结，谁负责恢复

本文所说的“冻结和恢复”不是把 Full Crawl 阶段或视频结果保存在 BullMQ 中，而是分成以下职责：

| 模块 | 负责 | 不负责 |
|---|---|---|
| BullMQ | 任务持久化、投递、失败重试、卡死任务恢复和完成确认 | 决定当前业务阶段、保存抓取目标或视频结果 |
| Rota | 分配线路、确认换线条件、在新线路上重新进入执行器 | 改变 Run 身份、目标集合或业务终态 |
| Business Run binding | 冻结一次业务执行的身份和抓取契约 | 保存逐视频抓取进度 |
| `channel_runs` | 保存 Full Crawl 阶段提交凭据、目标摘要和抓取完成状态 | 代替 BullMQ 调度任务 |
| `content_candidates` | 保存每条视频目标、详情结果和处置结果 | 决定下一次任务何时投递 |
| `executeFullCrawlYoutubeJs()` | 每次进入时读取 PostgreSQL，验证不变量并推导下一阶段 | 相信进程内缓存或任务进度决定恢复位置 |

固定恢复协议如下：

1. 初次投递、BullMQ 失败重试、卡死任务恢复、Worker 重启和 Rota 换线都重新调用同一个生产 Interface；
2. 执行器先读取 Business Run binding、`channel_runs` 和 `content_candidates`，再决定是否需要访问 YouTube；
3. 已经提交的阶段和 Candidate 永久跳过，只处理 PostgreSQL 中尚未完成的工作；
4. 任务编号、BullMQ 执行次数、Worker 进程和线路可以变化，但 `run_id`、抓取契约、`uploads_hash` 和 `target_hash` 不变；
5. `job.data` 可以缓存 `run_id`、`business_run_key` 和契约 hash，用于传输及冲突校验，但不能覆盖 PostgreSQL；两者冲突时必须停止执行并明确报错；
6. BullMQ `progress` 只用于界面和诊断，清空或缺失 progress 不得改变恢复结果；
7. 若业务明确要求重新执行一轮完整抓取，必须创建新的受控 Business Run，不能通过增加 BullMQ 执行次数伪造新一轮。

一句话概括：BullMQ 保证任务会再次执行，PostgreSQL 保证再次执行时知道从哪里继续。

## 8. 阶段一：认领并恢复

### 8.1 输入校验

入口必须验证：

- `channel_id` 存在；
- `candidate_id` 对 Query / Migration 正常 Full Crawl 必须存在；
- `run_id` 和 `business_run_key` 已由现有 Runtime prepare；
- Business Run binding 的 channel、candidate、run kind 和 identity policy 一致；
- `fetch_contract` 是 `youtubejs_full_v1`；
- Candidate Attempt Fence 仍属于当前 Job attempt；
- Channel Registry promotion Run 与请求 Run 不冲突。

### 8.2 入口终态

以下状态直接返回，不访问 YouTube：

- Channel 已标记 removed；
- Candidate 已 rejected；
- Candidate 已 existing；
- Business Run binding 已 terminal；
- Run 已完成 Full Crawl 抓取且下游派发可以幂等重放；
- 当前 Job 的 Candidate Attempt Fence 已被后续 attempt 取代。

Fence 丢失属于 superseded execution，不属于业务失败，不处罚 Proxy。

### 8.3 下一阶段推导

Runner 不保存一个可以任意改写的 `current_phase` 字段。它从已提交事实推导下一阶段：

```text
Candidate 尚未 accepted，或没有匹配的 Run
  -> Admission

Run 已存在，但没有 Uploads receipt
  -> Uploads

Uploads receipt 已存在，且有未完成 content_candidates
  -> Video Detail

Uploads receipt 已存在，且全部 content_candidates 已终结
  -> Close Fetch

Fetch completion 已提交
  -> Handoff / Duplicate return
```

任何互相矛盾的组合都属于不变量破坏，例如：

- Run 声称 Uploads 已提交，但 target hash 与 Candidate 集合不一致；
- YouTubeJS Run 出现新的 `api_pending` Candidate；
- Run 声称 Fetch 完成，但仍有 queued/running/failed Candidate；
- Candidate accepted，但 Channel promotion 指向另一个 Run；
- Business Run contract 与 Channel Run contract 不一致。

这些情况必须停止自动覆盖并进入人工或受控 Repair。

## 9. 阶段二：频道资料与准入

### 9.1 网络步骤

新 executor 调用 YouTubeJS 获取：

- Channel root；
- About；
- 标题、handle、频道 URL；
- 描述；
- 国家信息；
- 订阅数及其原始文本；
- Videos / Shorts / Live tab 可用性；
- Channel removed 的明确证据；
- 请求数、客户端和 extractor version。

同一次 attempt 内可以缓存 Channel root，供随后 Uploads 使用。缓存只用于性能，不是恢复事实；进程重启后只凭 `channel_id` 重建。

### 9.2 禁止回退

这一阶段禁止调用：

- `fetchChannelInitial()`；
- HTML header parser；
- `fetchChannelYtDlpMetadata()`；
- Channel Data API fallback；
- 任何逐字段跨 extractor merge。

### 9.3 准入判断

频道抓取成功后继续使用现有 `evaluateChannelQualification()` 语义：

- 需要最低订阅数时，订阅数必须有可信观察；
- 明确低于阈值时拒绝；
- 订阅数未观察到不能当作 0；
- required subscriber surface 出现 Parser gap 时显式失败，不能借用 Search 结果或旧库值偷偷通过。

### 9.4 Admission 提交

网络请求完成后，使用一个短事务提交：

1. 重新锁定 Candidate Attempt Fence；
2. 校验 Candidate 仍允许准入；
3. 写 `channel_candidates.snapshot_json`；
4. qualified 时执行现有 Channel Registry promotion/update；
5. 写 Candidate accepted；
6. 创建或校验 `channel_runs`；
7. materialize Business Run binding；
8. 写入不可变 `fetch_contract`；
9. 把 About 事实保存为现有 `pending_initial_about_observation` command，仍由现有 Finalize 事务生成 Initial About Observation；
10. 保持 `ready_for_agent=false`，直到 Full Crawl Fetch 真正完成。

以上提交构成 Admission checkpoint。

如果准入失败，则在一个短事务中写 Candidate rejected 和证据，并终止尚未 materialize 的 Business Run binding。Rejected 流程不创建一个伪造的空 Channel Run。

## 10. 阶段三：视频列表与目标固化

### 10.1 YouTubeJS Uploads

Admission checkpoint 已存在后，抓取有界 Uploads：

```text
channel_id
+ content_limit
+ locale
+ 本 Run 稳定 observed_at
```

首版沿用 Full Crawl 的 bounded selection，不引入 Incremental Anchor、Gap、Recent Sampling 或 Phase A/B。

结果必须包含：

- playlist identity；
- 有序 entries；
- 每条 entry 的 video id、position、URL、标题和缩略图；
- Uploads 上实际观察到的内容类型证据；
- 发布时间证据四元组；
- pages、inspected count、parse gap count；
- complete、stop reason、terminal reason；
- activity evidence completeness；
- request count 和 extractor version。

这里需要区分两个稳定摘要：

- `uploads_hash`：覆盖本次有界扫描实际观察到的有序 entries、完整性和停止原因，用于证明活跃度判断基于哪份列表；
- `target_hash`：只覆盖本 Run 最终选中、需要由 `content_candidates` 表示的 Detail 目标，用于恢复时校验目标集合没有变化。

通常 active 分支的两份集合高度重合，但它们不是同一个概念。明确 dormant 时可能有很多旧 Uploads entry，却有零个 Detail target。

### 10.2 Uploads 的提交粒度

首版不做页级 checkpoint。

原因：Full Crawl 的 Uploads 由 `content_limit` 严格限制，目标集合必须在第一条 Detail 请求前完整冻结。页级 checkpoint 会引入 continuation identity、跨页去重和目标截断恢复，超出本轮需要。

因此：

- Uploads 成功并提交前崩溃，重试整段有界扫描；
- Uploads receipt 提交后，永不重新扫描或重新选择目标；
- 如果生产测量证明 Uploads 重扫成本不可接受，再单独设计页级 checkpoint。

### 10.3 完整、部分和无效结果

- complete scan：正常进入目标固化；
- 有可信 normalized entries 的非线路 incomplete scan：保存为 partial，不得声称 evidence complete；
- incomplete scan 不能提前把 Migration 判为 dormant；
- Route Failure：不创建 partial receipt，原错误向外抛出；
- 异常没有携带可验证 entries：不创建 receipt；
- 合法空频道：只有 YouTubeJS 明确证明列表为空或频道无内容 tab 时才是可信 empty。

### 10.4 Migration 第一次活跃度判断

仅当 Run 的 Migration Activity Gate 为 required 时执行。

使用 Uploads 发布时间证据判断：

```text
完整证据 + 近期内容数=0 + 不确定内容数=0
  -> conclusive dormant

存在近期内容
  -> 继续 Video Detail

存在不确定内容或扫描不完整
  -> 不能提前 dormant，继续 Video Detail
```

Query Run 跳过这个业务判断，但仍使用相同 Uploads collector 和目标固化逻辑。

### 10.5 原子固化

Uploads 抓取和判断在事务外完成，然后使用一个短事务提交二选一结果。

分支 A，明确 dormant：

```text
保存 Uploads receipt、uploads_hash 和活跃度证据
保存空 Detail 目标的 target_hash 和 selected_count=0
写 Migration Activity Gate=dormant
写 Channel dormant lifecycle
写 Run detail_status=done
保持 ready_for_agent=false
不创建 content_candidates
```

分支 B，需要详情：

```text
保存 Uploads receipt 和 uploads_hash
创建完整且有序的 content_candidates
保存 target_hash 和 selected_count
写 Run status=waiting_detail
写 Run detail_status=queued 或 done
```

Uploads receipt 和 `content_candidates` 必须在同一事务提交。

同一 Run 重放时：

- 相同 target hash 是 no-op；
- 不同 target hash 必须 fail closed；
- 不允许 `ON CONFLICT` 重置已经完成 Candidate 的 detail/disposition；
- 不允许第二次扫描产生的新列表覆盖第一次已提交列表。

## 11. 阶段四：逐视频详情

### 11.1 第一版固定串行

```text
detail_concurrency = 1
```

按 `content_candidates.position` 从小到大处理。

不为每条视频创建 BullMQ Job，也不预取后续视频。串行可以让 Route Failure、取消和恢复位置保持确定。

### 11.2 单条 Candidate 流程

```text
短事务认领下一条未完成 Candidate
              |
              v
事务外调用严格 YouTubeJS Video Detail
              |
       +------+-------------------+
       |                          |
       v                          v
可信详情或明确内容状态       线路错误、取消、执行权失效
       |                          |
       v                          v
短事务 CAS 提交 Candidate      保持可重试，不写业务空值
       |                          |
       v                          v
处理下一条                    向外抛出或安静停止
```

认领和提交必须复用现有 Candidate Attempt Fence 与 Content Detail execution fence。

旧 attempt 晚到时，如果 Run-level fence 已改变，其结果不得写入 canonical Candidate。

### 11.3 YouTubeJS Detail 契约

每条 Video Detail 至少处理：

- playability；
- content type；
- title；
- description、hashtags、keywords；
- published time；
- duration 或 live evidence；
- view count；
- like count；
- comment count；
- comments disabled 状态；
- comments first page；
- thumbnail；
- live/upcoming 状态；
- access status；
- 字段观察状态；
- 实际 YouTube client 和请求计数。

新路径使用从 Incremental 分支移植的有限客户端顺序：

```text
WEB -> IOS -> ANDROID -> 最终状态探测
```

客户端切换发生在同一个 Route Fence 内，不等于换 IP。

禁止跨客户端或跨请求拼出一条看似完整的 Detail。只能选择第一条满足当前 strict contract 的完整观察、明确 terminal observation，或返回显式失败。

### 11.4 Candidate 提交结果

可信公开内容：

- 应用现有内容类型和时间窗口规则；
- 调用现有 Full Video content store；
- 写 `content_candidates.detail_status=done`；
- 写 `disposition=stored` 或现有合法排除 disposition；
- 保存完整 evidence 和 field status。

明确的 private、members-only、unavailable 或 uploader removed：

- 作为内容 terminal observation；
- 写现有 `unavailable` 或 `terminal_excluded` 语义；
- 不处罚 Proxy；
- 不调用 Data API 验证。

upcoming、live in progress 或超出内容窗口：

- 按现有 Full Crawl scope/disposition 规则提交；
- 不把它们伪装成抓取失败。

### 11.5 不再有 Data API 阶段

`youtubejs_full_v1` 不创建新的 YouTube Data API task。

因此新 Run 中出现以下状态属于不变量破坏：

```text
content_candidates.detail_status = api_pending
content_candidates.api_status IN (pending, queued, running)
```

历史 legacy Run 的 Data API task 继续由旧 Worker 排空，不能全局删除相关表和 Worker。

## 12. 阶段五：从数据库收口抓取

只有全部目标 Candidate 已进入合法 terminal disposition 后，才能 Close Fetch。

Close Fetch 不访问 YouTube，只从 PostgreSQL 重建和校验：

- Uploads receipt 和 `uploads_hash`；
- Detail `target_hash`；
- selected count；
- Candidate 有序集合；
- stored、deferred、terminal excluded 数量；
- partial 和 missing fields；
- 内容窗口排除数量；
- live/upcoming 排除数量；
- Migration Activity evidence；
- Run 和 Candidate Fence。

### 12.1 最终 Migration Activity Gate

Migration Run 使用已经提交的 Candidate / Content 事实再次判断：

- 有近期公开内容：passed，Channel active；
- 完整证据确认无近期内容：dormant；
- 仍有无法消除的不确定证据：inconclusive，保持 active 并明确记录；
- Channel 已 removed：rejected。

Query Run 的 Gate 结果为 not required，不执行 Migration dormant 业务。

### 12.2 Fetch completion 提交

一个短事务完成：

1. 锁定 Run 和 Channel；
2. 再次验证 `uploads_hash`、`target_hash` 和 Candidate terminal 状态；
3. 执行现有 `reconcileRunDetailStatus()` 等价逻辑；
4. 执行最终 Migration Activity Gate；
5. 写 `channel_runs.detail_status=done`；
6. 保存 `result_json.full_crawl.fetch` completion receipt；
7. active/inconclusive Channel 才设置 `ready_for_agent=true`；
8. dormant Channel 保持 `ready_for_agent=false`；
9. active/inconclusive Run 写 `status=waiting_agent`，等待现有 Agent / Finalize 推进；
10. dormant/rejected Run 按现有 Activity Gate 终态写 `status=done` 或 `skipped`；
11. 保存最终计数和完成时间。

不能在 Admission 成功时就设置 `ready_for_agent=true`。通过订阅数准入只证明频道可以继续抓取，不证明 Video Detail 已经完成。

这里的 Fetch completion 只表示抓取阶段结束。对于 active/inconclusive Run，它不等于整个业务 Run 已经 `done`；Run 的最终终态仍由现有 Agent / Finalize 链路决定。

## 13. 阶段六：交回现有下游

Fetch completion 已提交后：

- active / inconclusive：允许现有 Controller 调度 Agent；
- dormant：跳过 Agent；
- 使用现有确定性 Finalize job identity 发出 Finalize 信号；
- Agent 完成后继续由现有流程再次触发 Finalize；
- Publication 行为不在本轮修改。

Handoff 不建立第二套业务状态机。

`ready_for_agent`、Run 的 Fetch completion 和现有确定性 Finalize job 已经足够。若数据库提交后、队列 add 前崩溃，重试看到 Fetch completion 后再次执行幂等派发。

新 executor 不等待 Agent 或 Publication 完成后才返回。

## 14. PostgreSQL 检查点模型

### 14.1 不新增 Incremental Batch/Item 表

Full Crawl 已经有天然的两级结构：

```text
channel_runs         = 一次 Full Crawl Batch
content_candidates   = 这次 Full Crawl 的 Video Items
```

因此本轮不复制：

- `incremental_youtubejs_video_batches`；
- `incremental_youtubejs_video_items`；
- Incremental `cycle_key`；
- Phase A / Phase B；
- Recent Sampling Planner；
- First-Seen ledger checkpoint。

### 14.2 Checkpoint 对应关系

| 阶段 | canonical checkpoint |
|---|---|
| Candidate ownership | `channel_candidates.snapshot_active_job_*` Fence |
| Admission | Candidate accepted snapshot + Channel promotion + matching Run |
| 抓取契约 | Business Run immutable intent；Run 中保存一致镜像 |
| Uploads | Run Uploads receipt + 同事务 `content_candidates` |
| 单视频 Detail | `content_candidates.detail_status/result_json/disposition` |
| Fetch completion | Run `detail_status=done` + Full Crawl fetch receipt + Activity Gate |
| Agent eligibility | `channels.ready_for_agent` |
| Finalize | 现有 Finalize source fence 和确定性 Job |

Raw object、日志、BullMQ progress、Rota attempt 和请求 telemetry 都是证据，不是恢复正确性的 canonical checkpoint。

BullMQ `job.data` 是任务传输载荷，可以携带数据库身份的缓存副本；执行器必须把它与 Business Run binding 重新核对。BullMQ `job.updateProgress()` 是观测信息。两者都不能保存 `current_phase`、未提交的目标集合或 Candidate 完成状态，也不能作为跳过 PostgreSQL 检查的依据。

### 14.3 建议的 Run receipt

```json
{
  "fetch_contract": {
    "executor_id": "youtubejs_full",
    "executor_version": 1,
    "contract_hash": "sha256:..."
  },
  "full_crawl": {
    "admission": {
      "status": "committed",
      "observed_at": "..."
    },
    "uploads": {
      "outcome": "complete",
      "uploads_hash": "sha256:...",
      "target_hash": "sha256:...",
      "selected_count": 30,
      "evidence_complete": true,
      "observed_at": "..."
    },
    "fetch": {
      "status": "complete",
      "completed_at": "...",
      "stored_count": 20,
      "excluded_count": 10
    }
  }
}
```

Receipt 记录已提交事实及其校验摘要，不是一个允许调用方手动跳阶段的命令。

### 14.4 Uploads hash 和 Target hash

`uploads_hash` 至少覆盖 canonical 化后的：

```text
playlist identity
完整、有序的 normalized entries
complete
stop reason
terminal reason
parse gap count
activity evidence completeness
```

它用于校验 Uploads receipt 自身，不要求能从 `content_candidates` 重建。明确 dormant 分支也必须保存该 hash。

`target_hash` 至少覆盖最终 Detail 目标中有序的：

```text
position
video_id
source_url
title
thumbnail_url
Uploads content type evidence
Uploads publication evidence
```

计算前使用 canonical JSON 和稳定字段顺序。

恢复时由数据库 `content_candidates` 的 Uploads 原始子文档重建同样的 canonical target；数量或 hash 不一致必须 fail closed。Detail 提交可以增加结果字段，但不得改写参与 target hash 的 Uploads 原始子文档。

明确 dormant 时 Detail target 是 canonical 空数组，`selected_count=0`，`target_hash` 是该空数组的稳定 hash；不能拿非空 `uploads_hash` 代替它。

## 15. 字段状态与数据完整性

新 Full Crawl 继承 Incremental YouTubeJS strict contract。

字段状态至少区分：

| 状态 | 含义 | 可以写成业务空值吗 |
|---|---|---|
| `exact` | 观察到明确值 | 写实际值 |
| `empty` | 成功观察到明确为空 | 可以 |
| `disabled` | 功能明确关闭，例如评论关闭 | 按业务规则写入 |
| `unavailable` | 内容状态决定字段不可取得 | 按 terminal 规则写入 |
| `unobserved` | 本次请求没有覆盖该字段 | 不可以 |
| `parser_gap` | 响应存在但 Parser 无法提取 | 不可以 |

硬约束：

> 没抓到不等于空；请求失败不等于 0；评论请求失败不等于评论关闭。

### 15.1 发布时间证据

继续使用不可拆分四元组：

```text
published_at
published_at_status
published_at_precision
published_at_source
```

YouTubeJS Detail 优先读取 player microformat；若 Incremental 分支已经支持可信 absolute date text，则复用该解析。不能把 relative 或 unresolved 时间当作 exact 时间通过 Migration Activity Gate。

### 15.2 评论规则

以下为 v2 策略（用户于 2026-09-07 明确评论正文是尽力采集，不应阻止发布）：

- 明确 disabled：`comments_disabled=true`，count 按现有规则为 0；
- 评论总数与正文独立判断：可信 header total 不因为正文缺失变成 unresolved；没有可信数值则保持 null/unresolved；
- 成功得到第一页：保存 count、第一页、实际排序和来源；优先 TOP_COMMENTS，有正数 total 但零正文时，最多按响应中的最新评论入口追加一次 NEWEST_FIRST 请求，不抓第二页；
- 评论请求或回退失败：保留错误/空正文证据，视频主体字段完整时仍可提交，不因此失败整条 Candidate；取消、Fence 和主体字段错误不能被忽略；
- 旧 provisional comment_count 保存在抓取证据中；若状态仍 unresolved，业务 payload 输出 null/unresolved，不伪造 exact，也不排除整个视频；正文不进入业务发布包；
- 评论 Parser gap：保存错误证据，不能写 0 或 disabled；视频主体解析仍严格校验；
- private/members-only/unavailable：按内容 terminal 规则处理，不要求公开评论 surface。

v1 续跑仍保留原规则：评论请求失败时当前 Candidate 可重试并交 Rota，不启用最新评论回退。部署 v2 前，须先将 `contents_comments_first_page_shape_check` 兼容 NEWEST_FIRST，并更新相关契约读取端；本次仅在隔离数据库验证，未修改线上结构。诊断、实测及部署边界见 `FULL_CRAWL_COMMENT_DIAGNOSIS_20260907.md`。

## 16. 失败语义

### 16.1 只有三类 Route Failure

只有以下三种 failure kind 可以处罚当前 Proxy 或触发 Rota 换线：

```text
proxy_transport
youtube_rate_limited
youtube_challenge
```

它们的共同规则：

- 当前网络阶段没有可信 checkpoint 时不提交业务结果；
- 当前 Video Candidate 保持可重试；
- 不写成 removed、unavailable、empty、0 或 partial success；
- 等当前 Managed Request 全部结束后再允许 Route 变化；
- 原始分类错误交回现有 Managed Worker / Rota。

### 16.2 其他错误

| 类型 | 处理 |
|---|---|
| 主动取消 | 停止发新请求，不写业务失败，不处罚 Proxy |
| Candidate / Run Fence 丢失 | 旧执行失效，零 canonical 写入 |
| Channel removed | 提交明确 Channel terminal observation |
| Video private/members-only/unavailable | 提交明确 Content terminal observation |
| Parser runtime gap | 不伪造空值；按 Full Crawl 现有 failed/deferred 语义保存证据 |
| Required contract 不满足 | fail closed，等待代码修复或受控 Repair |
| token/client 问题 | 保持可重试，不处罚 Proxy，不回退旧 extractor |
| upstream transient | 按现有 retry policy 重试，不处罚 Proxy |
| PostgreSQL / identity contract | 本地失败，绝不处罚 Proxy |
| 未分类错误 | 保持未提交状态并显式失败，不能猜测为内容 terminal |

### 16.3 Detail 是否继续处理后续条目

- 确认 Route Failure：立即停止本 Run 的新请求并向外抛出；
- 取消或 Fence 失效：立即停止；
- 明确 Content terminal：提交该条并继续下一条；
- 现有策略允许 settle 的 parser/runtime failure：提交为 failed/deferred，继续其他条目；
- database/contract invariant failure：立即停止。

Run 最后存在 failed Candidate 时，不得标记 Fetch complete。后续普通 retry 或受控 Detail Repair 继续处理失败条目。

## 17. 崩溃恢复矩阵

| 崩溃位置 | 恢复行为 |
|---|---|
| Snapshot 请求完成前 | 重新抓 Snapshot |
| Snapshot 成功、Admission 事务前 | 重新抓 Snapshot |
| Admission 事务提交后 | 跳过 Snapshot，进入 Uploads |
| Uploads 扫描中 | 重新执行整个有界 Uploads 扫描 |
| Uploads 成功、目标事务前 | 重新扫描；数据库尚无目标事实 |
| Uploads receipt 与 Candidates 提交后 | 不再扫描 Uploads |
| 第一条 Detail 前 | 从第一个 queued Candidate 开始 |
| Video 请求发出、响应未确定 | 新 attempt 可以重抓当前 Candidate |
| Video 响应成功、Candidate 提交前 | 最多重复请求当前 Candidate 一次 |
| Candidate CAS 提交后 | 该 Candidate 永久跳过 |
| 中途 Route Failure | 已提交 Candidate 保留，只恢复未完成 Candidate |
| 全部 Candidate 完成、Close Fetch 前 | 从 PostgreSQL 收口，不访问 YouTube |
| Fetch completion 提交、Finalize add 前 | 重放幂等 handoff |
| BullMQ ACK 前 | 返回相同数据库结果，不重复抓取 |
| Route 切换 | Run 和 target identity 不变，只继续未完成阶段 |

检查点保证“已经提交的阶段不重抓”。网络响应成功但数据库提交前进程死亡时，没有 canonical 事实，因此当前网络操作允许最多重做一次。

## 18. Runtime 和依赖

### 18.1 Channel Runtime

当前 Channel Runtime 无条件 acquire yt-dlp 和 YouTubeJS。新设计要求 Runtime 根据 executor contract 获取能力：

```text
legacy_full_v2
  -> acquire yt-dlp
  -> acquire YouTubeJS

youtubejs_full_v1
  -> acquire YouTubeJS
  -> 不 acquire yt-dlp
```

默认必须保持 legacy 行为，避免影响旧 Full Crawl 和仍依赖 yt-dlp 的 Content Enrich。

新 executor 启动时如果 YouTubeJS full-detail 能力不可用，应启动失败或 Job 显式失败，不能静默切回旧路径。

### 18.2 依赖分类

- YouTube 是 true external dependency。生产使用 YouTubeJS Adapter，测试使用 fixture/scripted Adapter；
- PostgreSQL 是 local-substitutable dependency。使用隔离 PostgreSQL 做 Interface 集成测试；
- Rota 是 remote but owned。继续使用现有 Runtime Adapter 和 Route Fence；
- BullMQ 负责投递，不负责业务 checkpoint；
- Object storage 保存 raw/diagnostic evidence，不负责决定恢复位置。

## 19. 从 Incremental 分支复用什么

必须移植或复用：

- strict required surfaces；
- WEB / IOS / ANDROID 有限 client ladder；
- terminal basic probe；
- absolute publication date fallback；
- view count 解析和字段状态；
- 评论错误原始 cause/evidence；
- `exact/empty/disabled/unavailable/unobserved/parser_gap`；
- client-aware request telemetry；
- Route Failure 分类；
- cancellation 和 Route Fence 检查；
- 有值不被错误分支覆盖为空的规则；
- 固定版本真实响应 fixture。

不得移植：

- Incremental `cycle_key`；
- Incremental Batch/Item 表；
- Phase A / Phase B；
- Anchor / Gap Abandonment；
- First-Seen ledger；
- Recent Sampling Planner；
- Feature Clock 和 task mask；
- Incremental Observation / Cursor finalization。

Full Crawl 要复用的是 YouTubeJS 获取和证据语义，不是 Incremental 的业务状态机。

## 20. 与现有模块的关系

### 20.1 继续复用的稳定业务模块

新 executor 可以继续调用现有稳定模块，例如：

- Channel qualification；
- Channel Registry promotion；
- Candidate Attempt mutations；
- Business Run binding；
- publication time evidence；
- content type resolution；
- content window；
- Full Video content store；
- Video disposition；
- Migration Activity Gate；
- Run detail status reconciliation；
- About / Crawl Observation store；
- Finalize policy 和 source fence；
- YouTube failure policy。

### 20.2 不允许依赖旧 pipeline 的私有函数

新脚本不得导入 `pipelineV2.js`，也不得为了复用旧私有函数而临时扩大它的导出面。

旧文件中的私有编排逻辑需要：

- 使用已有稳定模块重新组合；或
- 在新脚本中按现有测试和数据库行为实现等价逻辑；或
- 新增真正可被两个实现长期复用的深模块，但不能为了搬代码创建浅 pass-through。

### 20.3 Observation 和 Publication

本轮不逐 Video 写 Crawl Observation，也不逐 Video 调用 Publication。

Candidate 是恢复 checkpoint；现有 Full Crawl Initial Observation 和 Publication 仍在现有 Finalize 语义允许时统一生成。

### 20.4 后续开发落点

| 文件或模块 | 后续改动 | 约束 |
|---|---|---|
| `services/qybullmq/src/fullCrawlYoutubeJs.js` | 生产依赖装配和唯一抓取入口 | 执行入口为 `executeFullCrawlYoutubeJs()`，另提供队列生命周期关闭方法 |
| `services/qybullmq/src/fullCrawlYoutubeJsFactory.js` | 阶段编排和可注入测试 factory | Admission、Uploads、Detail、Close Fetch、Handoff 顺序只在这里定义 |
| `services/qybullmq/src/fullCrawlYoutubeJsStore.js` | PostgreSQL checkpoint Adapter | 校验 hash、Candidate 集合、Fence、CAS 和 Agent eligibility |
| `services/qybullmq/src/fullCrawlYoutubeJsModel.js` | canonical Uploads / target / strict Detail 模型 | 纯模型，不访问网络和数据库 |
| `services/qybullmq/src/finalizeDispatch.js` | legacy 与新 executor 共用的确定性 Finalize 投递 | 保持相同 source revision 和 Job identity |
| YouTubeJS strict 内部 Adapter | 从 Incremental 分支移植 Channel、Uploads、Detail、Comments 能力 | 可以是新脚本私有实现或私有模块，但不能形成第二个生产入口 |
| `businessRunBindingStore.js` 及其 prepare 调用点 | 新 Run 冻结 fetch contract；历史缺失字段只解释为 legacy | 不改写已有 immutable intent |
| `channelRunBinding.js` 或 Admission store | materialize 时写入并校验 Run contract 镜像 | 只能首次写入或相同值重放 |
| `worker.js` | 在一个装配点选择 legacy 或 YouTubeJS executor | Repair 分流保持现状 |
| `channelExecutionRuntime.js` | 按已解析契约获取所需抓取能力 | YouTubeJS 新路径不 acquire yt-dlp；legacy 行为不变 |
| `runDetailStatus.js`、`migrationActivityGate.js` | 复用现有业务语义，必要时只增加新 executor 所需的窄入口 | 不复制 Activity Gate 或 Finalize 状态机 |
| Schema / migration | 首版复用现有表和 JSONB receipt；只补必要约束或索引 | 不新增 Full Crawl Batch/Item 表 |
| tests | 新增静态、fixture、PostgreSQL、Redis 和崩溃注入测试 | 测试通过同一个 executor Interface 驱动 |

`pipelineV2.js`、Agent、Finalize 和 Publication 的首版目标都是行为不变。若移植 strict helper 必须调整共享文件，应先证明 legacy caller 的返回和失败语义没有变化；否则把新能力留在新 executor 的内部模块。

## 21. Repair 兼容范围

### 21.1 第一版必须支持

- 新 Query Full Crawl 的普通执行；
- 新 Migration Full Crawl 的普通执行；
- 同一 YouTubeJS Run 的 BullMQ retry；
- stalled recovery；
- Route Failure 后换 IP 恢复；
- 进程重启恢复；
- Fetch completion 后幂等 handoff。

### 21.2 第一版不直接切换

以下特殊 Job 首版继续由 legacy executor 处理：

- about-only publication gap repair；
- publication repair child Run；
- channel full repair；
- checkpoint repair；
- 独立 content detail repair；
- 已经存在的 legacy migration retry Run；
- 已经存在 `api_pending` 的 Run。

这不是永久放弃 Repair，而是避免在正常 Full Crawl 尚未稳定时同时重写多个恢复协议。

### 21.3 后续 Repair 迁移原则

- Repair 必须读取父 Run 的 executor contract；
- 修复同一个 Run 时必须继续原 executor；
- 需要切换 executor 时必须创建新的受控 Business Run；
- 不允许 Repair 把 YouTubeJS Run 重新送进 yt-dlp/Data API；
- 每种 Repair 都要明确它修复的是 Admission、Uploads、单条 Detail 还是 Finalization；
- Repair 全部迁移完之前不能删除 legacy worker 能力。

## 22. 新旧实现装配和灰度

### 22.1 装配原则

Worker 同时包含两个 executor，但每个 Job 只能由自己的 immutable contract 选择其中一个：

```js
const executor = fetchContract === "youtubejs_full_v1"
  ? executeFullCrawlYoutubeJs
  : processChannelCrawlV2;
```

这段选择只能存在于一个装配点。

禁止把 `if youtubejs_full_v1` 分散到旧 pipeline、Detail、Finalize 或业务 policy 中。

### 22.2 上线顺序

1. 给当前 legacy Full Crawl 补 characterization tests 和最终数据库快照；
2. 移植 Incremental 分支的 YouTubeJS strict 能力，默认不影响旧 caller；
3. 新建 `fullCrawlYoutubeJs.js` 和 fixture tests；
4. 实现 Admission 和 checkpoint 恢复；
5. 实现 bounded Uploads 和原子 target 固化；
6. 实现串行 Detail 和逐 Candidate CAS；
7. 实现最终 Activity Gate、Fetch completion 和 handoff；
8. Runtime 支持 YouTubeJS-only capability；
9. Business Run intent 开始支持不可变 fetch contract，默认仍是 legacy；
10. 完成隔离 PostgreSQL、Redis 和崩溃注入测试；
11. 先选择少量新 Migration Candidate 使用 `youtubejs_full_v1`；
12. 观察完整 Migration 生命周期和 Publication 结果；
13. 扩大 Migration 流量；
14. 再选择少量新 Query Candidate；
15. 全量新正常 Run 切换后，逐项迁移 Repair；
16. 所有 legacy Run、Detail task、Data API task 和 Repair 排空后，才允许删除旧 Full Crawl 抓取分支。

### 22.3 回滚

- 停止给新 Business Run 分配 `youtubejs_full_v1`；
- 已经创建的 YouTubeJS Run 继续由新 executor 排空，或明确暂停等待修复；
- 不允许把未完成 YouTubeJS Run 交给 legacy executor；
- 不允许原地改写 contract hash；
- 需要重新走 legacy 时，必须由受控流程创建新的 Business Run identity。

## 23. 可观测性

每个 Run 至少记录：

- executor id/version/hash；
- started/resumed/completed；
- 从哪个阶段恢复；
- Admission outcome；
- Uploads complete/partial/empty；
- Uploads pages、inspected、selected、parse gaps；
- `uploads_hash` 和 `target_hash`；
- Candidate queued/running/done/unavailable/failed 计数；
- 每条 Candidate 是否 checkpoint hit；
- 每条实际请求 client；
- YouTubeJS request count；
- 评论 surface outcome；
- failure kind 和 retry mode；
- 第一次和最终 Migration Activity Gate；
- Fetch completion；
- handoff 是否重放。

指标必须能直接回答：

- 本 Run 是否调用过 HTML、yt-dlp 或 Data API；
- 为什么某个字段为空；
- 为什么某个 Candidate 被重抓；
- 重抓发生在 checkpoint 前还是后；
- 换 IP 后从哪个 Candidate 恢复；
- 为什么 Migration 被判为 dormant；
- 为什么没有进入 Agent；
- 当前 Run 使用哪个 contract；
- Finalize 派发是否为幂等重放。

新路径对禁止抓取器的调用计数必须恒为 0。

## 24. 测试计划

### 24.1 静态范围测试

- 新脚本不 import `pipelineV2.js`；
- 新脚本不 import旧 `youtube.js` 网络抓取函数；
- 新脚本不引用 `fetchChannelInitial`；
- 新脚本不引用 `fetchChannelUploads`；
- 新脚本不引用 `fetchChannelYtDlpMetadata`；
- 新脚本不引用 `fetchVideoYtDlpDetail`；
- 新脚本不 enqueue YouTube Data API task；
- 新脚本没有 extractor fallback merge；
- 旧 `processChannelCrawlV2()` 在首版实现中无行为修改；
- Worker 只有一个 executor 选择点。

### 24.2 Interface 和 fixture 测试

- Channel Snapshot 完整字段；
- Channel removed；
- hidden subscriber count；
- subscriber count parser gap；
- Uploads complete、partial、empty；
- Uploads stable ordering 和去重；
- Video、Short、Live、Upcoming；
- private、members-only、unavailable；
- comments collected、disabled、empty、parser gap；
- WEB / IOS / ANDROID client ladder；
- terminal probe；
- publication date exact/date-only/unresolved；
- view count exact/unobserved；
- 每个字段的 authoritative empty 和 parser gap fixture。

### 24.3 PostgreSQL checkpoint 测试

- Admission transaction 原子；
- Promotion 冲突只允许一个赢家；
- Business Run contract mismatch fail closed；
- Uploads receipt 和 Candidate 集合同事务；
- Uploads receipt 的 `uploads_hash` 可稳定重算；
- target hash 相同重放 no-op；
- target hash 不同重放 fail closed；
- 已完成 Candidate 不被 Uploads replay 重置；
- 每条 Detail CAS 只允许当前 fence 提交；
- stale attempt 零 canonical 写入；
- 全部 Candidate 完成后从数据库 Close；
- Fetch completion 与 ready-for-agent 顺序正确；
- dormant 分支不创建 Detail work；
- YouTubeJS Run 中的 `api_pending` 触发不变量失败。

### 24.4 崩溃注入测试

对第 17 节每个位置至少注入一次崩溃，并断言：

- 已提交阶段不重抓；
- 未提交的当前网络操作最多重做一次；
- 已提交 Candidate 永久跳过；
- target 不改变；
- Route 切换不改变 Run contract；
- Close Fetch 重放不请求 YouTube；
- handoff 重放不产生重复业务结果。

2026-09-07 已执行上述范围中的两个跨进程崩溃点，Query / Migration 各一次，详见第 1 节；不代表第 17 节所有崩溃点已覆盖。可重复运行入口：

```bash
cd services/qybullmq
node --test test/fullCrawlYoutubeJsRecovery.postgres.redis.integration.test.js
```

运行前必须设置 `MANAGED_JOB_TEST_DATABASE_URL` 和 `MANAGED_JOB_TEST_REDIS_URL`，两者均须指向专用隔离实例。数据库名必须以 `_test` 结尾，测试会初始化仓库 schema 并写入随机身份的测试数据；Redis 使用独立随机前缀，退出时删除该前缀下的测试队列。缺少连接变量时明确跳过，不可据此声称集成验收通过。测试自身强制结束它创建的子进程，不调用真实 YouTube，也不操作线上 Worker。数据库种子保留于隔离实例以便排查。

实现文件为 `fullCrawlYoutubeJsRecovery.postgres.redis.integration.test.js`，测试进程入口与 YouTube fixture 位于 `test/support/fullCrawlRecovery*.mjs`。本轮仅新增测试和更新验证记录，没有因测试失败而修改生产逻辑；首次测试配置补齐了数据库身份白名单，Migration fixture 补齐了发布时间证据并使用已有的 `passed` 状态定义。

### 24.5 错误和完整性测试

- 429、HTTP 200 challenge 和 Proxy transport 上抛 Route Failure；
- Route Failure 不写 partial/empty/unavailable；
- cancellation 不写失败事实；
- Fence 丢失不处罚 Proxy；
- Parser gap 不写空值；
- v1 comments Route Failure 使当前 Candidate 保持可重试；v2 评论失败不阻止完整视频提交，取消仍中断；
- 热门为空时只追加一次最新评论请求，排序如实入库，回退失败不补零、不失败整个视频；
- 未确认评论字段不会排除视频，实际发布消息数量与预期一致，重复发布去重；
- terminal content 不换 IP；
- PostgreSQL 错误不处罚 Proxy；
- strict surface 不足不回退 yt-dlp；
- client ladder 不跨响应拼字段；
- evidence 中记录实际 client，而不是统一写 WEB。

### 24.6 行为对比测试

使用相同数据库 seed 和可信 YouTubeJS fixture，对比 legacy Full Crawl 的业务结果：

- Candidate accepted/rejected/existing；
- Channel promotion；
- Subscriber qualification；
- selected content identities 和 position；
- content type；
- content window；
- disposition；
- Migration active/dormant/inconclusive；
- Agent eligibility；
- Finalize status；
- Publication source revision。

允许的差异只有：

- source lineage 只剩 YouTubeJS；
- 不再有 fallback/merge；
- 不再创建新的 Data API task；
- strict contract 会把过去的静默缺字段变成显式失败；
- 已提交阶段在 retry 时不会重新请求。

## 25. 验收条件

以下条件全部满足后，才允许扩大新 executor 流量：

1. `fullCrawlYoutubeJs.js` 独立存在，生产只导出一个执行 Interface；
2. 新脚本不导入或调用旧 `processChannelCrawlV2()`；
3. 新路径所有 YouTube 数据请求只来自 YouTubeJS；
4. 新路径 HTML、yt-dlp 和 Data API 请求数均为 0；
5. Query 和 Migration 从 Channel Candidate 起使用同一个新 executor；
6. Migration 仅通过 Activity Gate policy 与 Query 区分；
7. Run 的 fetch contract 在第一次网络请求前已经冻结；
8. 同一 Run 永不混用 legacy 和 YouTubeJS；
9. Admission checkpoint 后不重抓 Snapshot；
10. Uploads checkpoint 同时冻结 `uploads_hash` 和 `target_hash`，之后不重扫或重选目标；
11. Video Detail 固定串行；
12. 每条可信 Detail 立即写 `content_candidates`；
13. 已提交 Candidate 不重复请求；
14. 当前 Candidate 响应后、提交前崩溃最多重请求当前一条；
15. Route Failure 只包含三种批准类型；
16. Route Failure、取消和 Fence 丢失不污染业务数据；
17. Parser gap 不写成空值或 0；
18. private/members-only/unavailable 形成明确 terminal observation；
19. 新 Run 不产生 `api_pending`；
20. 网络请求期间不持有长 PostgreSQL 事务；
21. Fetch completion 只能在全部 Candidate 合法终结后提交；
22. `ready_for_agent` 只能在 Fetch completion 时打开；
23. dormant Channel 不进入 Agent；
24. Close Fetch 重放不访问 YouTube；
25. Finalize handoff 可以幂等重放；
26. 旧 Run 和特殊 Repair 继续由 legacy executor 排空；
27. 回滚不改变已有 Run 的 executor；
28. Migration canary 完整经过 Agent、Finalize 和 Publication，无静默字段丢失；
29. Query canary 产生与现有业务契约一致的最终结果；
30. 可观测性能够证明新路径没有调用禁用抓取器；
31. BullMQ `job.data` 和 `progress` 都不是业务检查点；它们与 PostgreSQL 冲突时必须停止执行并明确报错；
32. BullMQ 失败重试、卡死任务恢复、Worker 重启和 Rota 换线都恢复同一个 Run，并且只处理 PostgreSQL 中未完成的阶段和 Candidate。

## 26. 明确拒绝的实现

- 在旧 `pipelineV2.js` 中逐段删除 fallback 后继续扩展；
- 新脚本包装旧 `processChannelCrawlV2()`；
- 新脚本调用旧 pipeline 的私有抓取函数；
- 通过可变环境变量决定一个已有 Run 的 executor；
- 把 BullMQ attempt 或 Proxy Route 当成 Full Crawl identity；
- 把 checkpoint 写进 BullMQ progress；
- 把可变 `job.data` 当成抓取契约、目标集合或当前阶段的唯一事实；
- 为 Full Crawl 复制 Incremental Batch/Item 表；
- 第一版增加 Uploads page checkpoint；
- 每个视频创建一个 BullMQ Job；
- 第一版并发抓取 Video Detail；
- 在不同 YouTube client 结果间逐字段 merge；
- Required Surface 失败后调用 yt-dlp 或 Data API；
- 把 Route Failure 写成 partial、empty 或 unavailable；
- Admission 后立即设置 `ready_for_agent=true`；
- Item 成功后立即逐条 Publication；
- Full Crawl 重构顺带修改 Agent、Finalize 或业务库投递协议；
- 未排空 legacy Run 就删除旧抓取器或 Data API worker。

## 27. 建议实现顺序

1. 冻结本设计及 `youtubejs_full_v1` contract manifest；
2. 为现有 Full Crawl 建立 characterization fixture 和数据库结果快照；
3. 从 Incremental 分支移植 strict YouTubeJS 能力；
4. 增加新脚本的 test factory 和 scripted YouTube Adapter；
5. 实现 Business Run fetch contract 冻结和单点 executor 选择；
6. 实现 Admission，完成 Candidate / Promotion / Run 原子 checkpoint；
7. 实现 bounded Uploads、`uploads_hash` 和 `target_hash`；
8. 实现 Migration 第一次 Activity Gate；
9. 实现 `content_candidates` 原子固化；
10. 实现串行 Video Detail、Comments 和逐 Candidate CAS；
11. 实现最终 Run reconciliation 和 Activity Gate；
12. 把 `ready_for_agent` 移到 Fetch completion；
13. 实现幂等 Finalize handoff；
14. 让 Channel Runtime 按 contract 只 acquire YouTubeJS；
15. 完成静态、fixture、PostgreSQL、Redis 和崩溃注入测试；
16. 配置默认保持 legacy，部署但不切流量；
17. 对少量新 Migration Run 开启 canary；
18. 扩大 Migration 后再开启 Query canary；
19. 切换全部新正常 Full Crawl；
20. 单独设计并迁移 Repair compatibility；
21. 所有旧工作排空后再讨论删除 legacy 实现。

## 28. 最终边界

本轮最终边界是：

```text
一个新的 Full Crawl YouTubeJS 深模块
  |
  +--一个生产 Interface
  +--一条 YouTubeJS-only 抓取路径
  +--固定业务阶段顺序
  +--复用现有 Run/Candidate 检查点
  +--逐 Video 立即提交
  +--换 IP 后只继续未完成工作
  +--完成后交回现有 Agent / Finalize / Publication

旧 Full Crawl
  |
  +--不包装
  +--不混用
  +--负责排空旧 Run 和尚未迁移的 Repair
```

Full Crawl 的业务阶段仍然分层；被移除的是每个阶段内部的多抓取器回退层。
