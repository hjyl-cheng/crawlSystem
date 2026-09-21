# 全量抓取远程执行阶段交接

最新生产状态（2026-09-20 08:36 UTC）：P6 全量 Worker 执行准备已完成。中心 execution 已开启，兼容槽启动成功，Rota 就绪容量 125；全量节点 executionAvailable=true，接单保持 0。原 62 个增量槽位全部恢复并通过连续采样，接单配置完整保留。尚未开始真实频道灰度，P6 整体验收未完成。详见 [P6 执行准备](FULL_CRAWL_REMOTE_P6_EXECUTION_READY_20260920.md)及 `reports/fullcrawl-p6-execution-ready-20260920.json`。下文较早状态保留为阶段历史。

更新时间：2026-09-20（UTC）。当前阶段：P5 已完成隔离验收，下一阶段为 P6。最新证据见 [P5 发布验收](FULL_CRAWL_REMOTE_P5_RELEASE_20260920.md)及 `reports/fullcrawl-p5-validation-20260920.json`。下文保留 P0/P1 历史证据；生产全量远程链路仍未部署。

## 本阶段授权与边界

- 本轮授权范围包含 P0～P6，最终目标包括受控生产灰度。
- 本阶段只做代码、测试和只读盘点；没有暂停队列、调整节点配置、迁移数据库、部署镜像或新增生产 Worker。
- 现有增量任务进度、接单配置、spool、检查点和未确认结果不作清理或重置。
- 用户已授权本轮继续至生产灰度 P6；授权不改变各阶段停止条件，未达到阶段门槛时保持全量接单关闭。

## 代码基线

- Git HEAD：`bee141e41b609a583a2b86b34967fce1af404a80`（`feat(dashboard): register node worker types and guard deployment`）。
- 工作区已有修改：`docs/FULL_CRAWL_REMOTE_EXECUTION_PLAN_20260918.md`；本阶段新增本交接文档。
- 仓库未发现适用的 `AGENTS.md`。

## P0 兼容矩阵（代码核对）

| 任务/入口 | 当前执行路径 | 首版远程决定 | 关键身份或状态 |
| --- | --- | --- | --- |
| `channel-snapshot` 普通全量 | `worker.js` → `executeFullCrawlYoutubeJs` 或旧本地执行器 | 远程全量候选；必须覆盖 `youtubejs_full_v1/v2/v3` | candidate、business run、dispatch generation、attempt、fetch contract |
| `channel-snapshot` Migration 控制分支 | 中心 `prepareControlledMigrationSnapshot`，之后仍进入全量生命周期 | 控制任务留中心，采集阶段才可远程 | migration batch/item、candidate attempt、run |
| `migration-channel-start` | 中心 `startControlledMigrationChannel` | 中心执行 | migration batch/channel |
| `channel-full-repair` | 本地全量 repair 生命周期 | 首版保留本地兼容路径 | repair batch、parent run、repair round |
| `channel-crawl-repair` | 本地全量 repair/恢复路径 | 首版保留本地兼容路径 | parent run、promotion candidate、retry intent |
| `channel-detail-repair` | `processContentDetailBatchV2` 本地执行 | 首版保留本地兼容路径 | content detail fence、repair identity |
| `channel-checkpoint-repair` | `processCheckpointRepairV2` 本地执行 | 首版保留本地兼容路径 | target run、repair round、checkpoint identity |
| `youtube-data-api-batch` 与 `video_api_continuation` | 中心 Data API / delayed / replay | 中心执行；远程槽位不得等待配额 | 稳定 request ID、run identity、API status |
| `youtube-channel-incremental` | 远程增量 supervisor/processor | 继续保护，不能被全量路由混领 | Plan、Feature Clock、incremental run、incremental fence |

结论：增量已有远程传输、整频道 journal 和恢复基础设施，但这些实现仍固定于 `incremental_collect`、增量队列和增量业务 fence，不能计入全量远程工作包完成度。

## 已取得的验证证据

以下测试在当前工作区通过：

- `fullCrawlFetchContract.test.js`
- `fullCrawlYoutubeJsFactory.test.js`
- `fullCrawlYoutubeJsModel.test.js`
- `proxyBusinessRun.test.js`
- `videoApiContinuation.test.js`
- `remoteNodeProtocol.test.js`
- `remoteNodeIncrementalRuntime.test.js`

共 7 个测试文件通过；此前节点类型、增量部署配方、远程监督器和 Worker 激活相关测试也已通过。`git diff --check` 通过。

## 生产只读基线（2026-09-18 UTC）

以下查询在生产容器内以只读方式执行，时间均为查询返回时间；没有暂停队列、改变接单设置、写数据库或部署。当前源码基线为 `bee141e41b609a583a2b86b34967fce1af404a80`。

### 实时节点和传输

- `verify-workers.mjs`：更新节点 47/47 deployed、connected、allowed、ready、active、running；增量节点 02 为 15/15；远程增量合计 62 个 Worker。
- `incremental-22`、`incremental-28`、`incremental-41` 均在线且可接单；自 03:18 UTC 起分别完成 487、460、451 个增量任务（脚本统计口径为已完成业务任务）。
- Remote Center HTTP 状态和 NATS transport health 均为 200；快照 `pending=0`、`unacknowledged=0`，RPC `rejected=0`、`timedOut=0`。
- Remote Center 数据库共有 66 条连接记录，其中 62 条 connected/enabled/accepting。

### 队列和任务分类

队列快照及任务样本（约 07:50 UTC）：

- `youtube-channel-incremental`：waiting 51、active 62、delayed 0、failed 996；waiting/active 样本名称均为 `channel.incremental.plan`。
- `youtube-channel-crawl`：waiting 0、active 0、delayed 6、failed 13887；延迟和失败样本名称均为 `channel-snapshot`，当前没有远程全量样本。
- `youtube-finalize`：active 1，样本名称 `finalize-channel`；`youtube-content-detail` 和 `youtube-agent-incremental` 当前为空。
- `youtube-data-api-batch`：failed 1、completed 66（计数随清理策略变化）；失败样本名称 `youtube-data-api-batch`。
- 07:36～08:05 UTC 的 30 分钟采样中，增量每分钟持续出现 started/completed 事件；`youtube-channel-crawl` 始终未暂停且 waiting/active 保持为 0，未发现全量任务被错误路由到增量远程节点。

### 数据库、Rota 和资源快照

- PostgreSQL 连接数约 38～46，`deadlocks` 在采样窗口保持 6408；事务提交/回滚计数持续增长，未观察到停滞。
- 增量队列 waiting 约 51～65、active 61～62，短时 delayed 0～3；增量失败总数在窗口内保持 996。事件流持续推进，但该快照不构成长期错误率 SLO。
- Rota capacity 快照：`workload_scope=qy-production`；channel desired/provisioned/assigned/ready/claimed 均为 123；reserve 1277，minimum reserve 32；detail 0；discover ready 2；query_quality ready 1。
- 资源快照：Remote Center 110.29% CPU、412.8 MiB/768 MiB；NATS 5.77% CPU、32.09 MiB/384 MiB；Rota 156.27% CPU、约 992 MiB；crawler PostgreSQL 333.11% CPU、约 1.14 GiB；Redis 14.03% CPU、约 808 MiB。
- 采样脚本中的 Rota control URL 在控制器进程未配置，因此每分钟 `rota` 字段返回 `Rota proxy control URL is not configured`；上面的 capacity 快照来自独立只读控制入口，不能把脚本错误误判为 Rota 故障。
- 磁盘只读快照：生产主机文件系统约 80% 已用、可用约 171 GB；`qy-pg4` 数据目录约 224 MiB，`qy-rd7` 数据目录约 4 KiB，crawler PostgreSQL 数据目录约 221 GiB，crawler Redis `/data` 约 1.0 GiB；远程中心和增量 Worker 的 `/var/spool` 均为 0。该主机共享磁盘余量不能直接作为全量 spool 配额，P1 必须设置独立上限和停止门槛。

### 源码和线上版本核对

`runtime/bug0917-deploy/check-live-source.py` 已对以下生产容器的 7 个共享源码文件逐一计算 SHA-256，并与工作区 `services/qybullmq` 对照一致：

- `qy-newcrawler-fresh-worker-finalize-1`
- `qy-newcrawler-fresh-controller-fullcrawl-canary-1`
- `qy-newcrawler-fresh-worker-incremental-1`
- `qy-remote-node-center`

这只证明所列文件一致；镜像层、未列出的文件、环境变量和运行时外部依赖仍需在发布前单独固定。

## P0 生产规模与未处理状态核对

生产 PostgreSQL 聚合查询（查询时间约 08:23～08:29 UTC）得到：

- 近 30 天 `channel_runs` 按 `crawl_mode` 分布为 full 397,691、incremental 347,450。
- 近 30 天全量 run 状态为 done 395,685、waiting_detail/queued 1,915、failed 76、waiting_agent/done 15。
- 近 30 天全量 candidate 状态为 done 11,070,489、queued 32,532、running 385、failed 1,589、unavailable 76；全量 API 状态为 not_needed 11,105,014、done 51、unavailable 6，当前没有 `api_pending` 样本。
- 最近完成的 10,000 个全量 run 时长样本：最短约 11,459 秒，最长约 559,089 秒，平均约 176,763 秒。样本包含历史长任务和迁移延迟，只用于容量和排空上限估算，不能当作新远程 Worker 的性能承诺。
- `youtube-channel-crawl` 队列当时 waiting/active 为 0 只表示 BullMQ 当前没有待领取 job；不能据此声称上述 waiting_detail 或 queued candidate 已清空。它们仍属于本地全量生命周期，首版远程化必须明确其接管与恢复边界。

## attempt 与详情批次的代码事实（W01 冻结输入）

本地全量路径目前有两层计账，不能直接当作远程批次协议：

1. 频道层由 BullMQ `attemptsStarted/attemptsMade`、`channel_candidates.snapshot_attempts` 和 `channel_execution_attempts` 共同约束；过期 job 由 candidate attempt fence 拒绝，旧代次不能结算新状态。
2. 详情层 `fullCrawlYoutubeJsStore.claimNextDetail()` 在单条领取时立即执行 `detail_status='running', attempts=attempts+1`。`videoExecutionRecovery.claimVideoExecution(..., { recovery: { kind: 'full' } })` 在恢复时把仍为 `running` 的目标置回 `queued`，但不会减少已经增加的 `attempts`。

由此形成的 W01 冻结提案是：远程详情批次必须先建立独立 reservation/batch 身份，只有节点写入可验证的 `started` 证据时才把对应候选从 `queued` 变为 `running` 并增加 `attempts`；未开始的批次后缀不得增加失败或尝试次数。批次失联且某条目标存在不确定的 started 证据时，按“已消耗一次执行预算、业务结果待恢复”处理，直到中心收到可验证完成/失败或显式恢复结论。批次应用与 fence 校验、候选状态、幂等 applied 标记必须在同一事务内完成。

该提案在 P1 以本地失败、中心崩溃、节点失联和 API 接续样例转换为实现契约；P0 不修改现有 `attempts` 或生产业务记录。

本地恢复相关单元测试已通过：

- `fullCrawlSnapshotRecovery.test.js`
- `pipelineDetailRecovery.test.js`
- `wholeChannelRecovery.test.js`
- `remoteSupervisorIdleRecovery.test.js`
- `fullCrawlYoutubeJsModel.test.js`

隔离 PostgreSQL/Redis 集成测试也已通过：

- `channelCandidateAttemptMutations.postgres.integration.test.js`
- `videoExecutionRecovery.postgres.integration.test.js`

补充的隔离交接/恢复测试也已通过：

- `remoteExecutionHandoff.postgres.integration.test.js`
- `fullCrawlYoutubeJsRecovery.postgres.redis.integration.test.js`
- `dataApiBatchOrphanRecovery.controller.postgres.redis.integration.test.js`

这些测试证明当前本地恢复、API 孤儿回收和远程交接边界；全量远程批次协议尚未实现，因此 P1 仍需新增协议级测试。

仓库中已有可复用的只读入口，但必须在受控生产运行环境执行：

- `runtime/bug0916-deploy/production_check.mjs`：数据库、远程节点连接和近 3 分钟任务事件汇总。
- `runtime/bug0918-01/verify-workers.mjs`：节点部署状态和固定 Worker 的增量完成证据。
- `runtime/bug0917-deploy/observe.mjs`：数据库只读状态与指定队列计数。
- `services/qybullmq/scripts/checkRemoteNodeCenter.mjs`：远程中心部署状态与传输健康检查。

这些脚本的连接字符串、token 和环境变量不能从本工作区推断或补造；执行时必须使用生产既有运行环境，并保留查询时间、代码版本和输出摘要。

## P0 完成记录（2026-09-18 UTC）

P0 门槛已满足，结论如下：

1. 队列、任务名称和执行去向已核对；没有发现全量任务被增量远程 Worker 错误接收。全量仍保持本地路径，远程全量接单关闭。
2. 线上 Worker、连接、NATS、Rota、数据库、Redis、磁盘和 spool 只读证据已记录；长任务和待处理候选明确列为 P1 容量与恢复约束。
3. 全量自身的 attempt/批次计账提案已冻结为设计输入：reservation 不计尝试，started 证据才计尝试，不确定 started 按预算已消耗处理，业务 fence 和 applied 标记同事务幂等落库。
4. 生产增量在 30 分钟窗口持续推进，但该证据不扩展为全量远程可用性或长期 SLO。

## P0 冻结结论

1. 首版继续采用专用全量节点，一个 slot 固定一种 role。
2. 普通 snapshot 远程化与 legacy/repair 本地兼容路径分开验收。
3. 不复用增量 Plan ID、incremental run 或 incremental business fence 伪造全量身份。
4. P1 可以开始共享逻辑整理；必须继续保持全量接单关闭，任何生产 schema、部署或节点配置变更都要等后续阶段门槛。

## P1 共享逻辑整理完成（2026-09-18 UTC）

已完成：

- 在 `services/qybullmq/src/fullCrawlCollector.js` 冻结三阶段 Collector 接口：`collectAdmission`、`collectUploads`、`collectDetail`。
- 在 `services/qybullmq/src/localFullCrawlCollector.js` 增加 Local Adapter，将现有 YouTubeJS 方法映射到共享 Collector；不持有数据库凭据、不写业务状态、不改变重试预算。
- `fullCrawlYoutubeJsFactory` 改为依赖 Collector seam，同时保留旧 `youtube` 注入方式，降低现有本地调用的切换风险。
- `fullCrawlYoutubeJs.js` 已显式装配 Local Adapter；远程 Adapter 尚未实现，不能据此宣称全量远程可用。

验证证据：

- 单元测试通过：`fullCrawlYoutubeJsFactory.test.js`、`localFullCrawlCollector.test.js`、`fullCrawlYoutubeJsModel.test.js`、`fullCrawlFetchContract.test.js`、`fullCrawlSnapshotRecovery.test.js`。
- 隔离集成测试通过：`fullCrawlYoutubeJsStore.postgres.integration.test.js`、`fullCrawlYoutubeJsRecovery.postgres.redis.integration.test.js`、`channelCandidateAttemptMutations.postgres.integration.test.js`、`videoExecutionRecovery.postgres.integration.test.js`。
- `git diff --check` 通过；当前代码版本仍为 HEAD `bee141e41b609a583a2b86b34967fce1af404a80`，工作区包含未提交的 P1 代码与文档改动。

未解决问题（转入 P2）：

- Collector 目前只有 Local Adapter；Remote Adapter、全量 role/capability、结果批次和中心应用仍属于 P2～P3。
- reservation、started、applied 的远程协议尚未实现，当前 Collector 不能接入生产远程节点。
- 需要继续验证 Collector 接口对 migration、repair/legacy 本地兼容路径和 API continuation 的调用约束。

P1 阶段结论：共享生命周期已经具备可替换 Collector seam，Local Adapter 对照和本地全量/增量回归通过；P1 条件满足。下一步进入 P2 的全量身份、注册/心跳能力和传输协议设计。全量生产接单、部署、schema 迁移和节点配置继续关闭。

后续 P2～P6 的完整执行清单、阶段门槛、停止条件和恢复入口已写入[后续执行清单](FULL_CRAWL_REMOTE_EXECUTION_FOLLOWUP_20260918.md)。2026-09-20 已完成 P2～P5 隔离验收，最新状态见 [P5 发布验收](FULL_CRAWL_REMOTE_P5_RELEASE_20260920.md)；下一阶段为 P6。P4 连接入口与 P5 执行入口使用独立开关，生产全量仍未部署或开放接单。


## 2026-09-20 P5 发布前验收交接

P5 已完成隔离分层验收，详见 [发布/回退清单](FULL_CRAWL_REMOTE_P5_RELEASE_20260920.md) 和 `reports/fullcrawl-p5-validation-20260920.json`。中心已具备默认关闭的真实全量执行入口，复用完整原 Worker 兼容流程、candidate/Discovery/Finalize handoff 和合同约束下的 API fallback。容量包括一个单独的本地兼容槽，发布进程通过数据库 session 锁防止重复申请总预算。

最终镜像由隔离 registry 的真实 manifest digest 固定并按 digest 回拉，最终中心与节点包均实际运行验收。旧中心镜像在新 schema 上启动、迁移重复执行/短锁超时、API 接续阻止回退、Query/Migration × v1/v2/v3、原 Worker、增量 HTTP/NATS 整频道与 API/崩溃恢复、Publication 事务和持久投递均有通过记录。原本地与远程 collector 的选定业务字段对照已存档。

所有生产 schema、部署、节点凭据和接单设置未改变。临时 P5 容器清理，本地镜像、registry 分发数据和测试证据保留；原 P2/P3 基础隔离环境保留。Git 差异仍未提交。本轮已执行至 P5，下一阶段为 P6；实际进入灰度前须做最新线上预检，不能直接使用旧快照开放接单。

边界：本轮是受控观察数据的分层验收，未对一个真实公共频道连续完成外部 Agent 与生产发布，也未采集生产灰度性能。真实产出与增量指标仍需按 P6 的 1 槽/10 频道/30 分钟门槛验证。

## P6 首轮只读预检（2026-09-20 07:07–07:09 UTC）

已执行首轮生产预检，尚未开放灰度、迁移或部署。两台登记节点均承载增量（47 + 15 个，全部在线接单），没有空闲全量节点，需要准备独立服务器，可复用已有闲置机器。中心 16 CPU、15 分钟负载约 20.4、磁盘已用 88%，资源放行仍待复核；不能只新增节点就直接开启灰度。已保存原接单配置、镜像/环境哈希、两个完整窗口的有效业务完成及队列/Rota/NATS/DB 证据。详见 [P6 首轮预检与服务器建议](FULL_CRAWL_REMOTE_P6_PREFLIGHT_20260920.md) 和 `reports/fullcrawl-p6-preflight-20260920.json`。此前 P5 状态为历史验收记录，P6 当前状态为首轮预检完成、灰度未开始。

## P6 专用节点已就绪（2026-09-20 07:28 UTC）

用户已添加“全量采集节点”（8a07de4f-a3ee-428f-a959-2aee9c7b6be7）。只读实测 SSH 固定指纹、sudo、节点身份、Docker/Compose、监控/NTP、HTTPS/WSS 均正常；2 核、标称 8 GB 内存/80 GB 磁盘，目前无容器。服务器缺失阻塞已解除，适合先做单 Worker 灰度准备。尚未部署 Worker，中心资源预算、全量 schema、镜像分发及兼容升级仍待完成。最新证据见 [节点环境核验](FULL_CRAWL_REMOTE_P6_NODE_ENVIRONMENT_20260920.md) 和 `reports/fullcrawl-p6-node-environment-20260920.json`；本次未修改生产状态。
