> 状态：本文件保留第一版技术选项，已被 [增量效率修订方案](incremental-db-read-pressure-fix-plan-20260923.md) 替代，不代表当前实施顺序。新增表、索引及阈值均须按修订方案重新评估。

# 增量数据库读取压力修复方案

日期：2026-09-23。状态：方案已细化，现网只读核查已完成；下述代码、索引、配置调整尚未实施。

目标：减少历史迁移与发布补偿对共享采集库的无效读取，降低增量准备、结果应用和租约续期延迟，同时保证历史任务最终收敛、发布完整性及任务所有权约束。

## 1. 证据与优先级

基础测量见 [读取压力快照](incremental-db-read-pressure-20260923.md) 和 [9 月 22 日中心压力报告](center-database-pressure-20260922.md)。9 月 23 日 06:13 UTC 补充了生产 EXPLAIN（未使用 ANALYZE）、索引与版本核查。

| 路径 | 已确认事实 | 修复决策 |
| --- | --- | --- |
| 自动发布补偿 | `reconcileAutomaticPublicationBacklog` 返回上限 25，但计划先扫描 channels、finalized_profiles、channel_delivery_state、channel_stream_state，再关联排序；此前同一查询运行至约 52 秒 | 首批修复：限制扫描输入，保留持久补漏 |
| Finalize 终态收敛 | `reconcileTerminalFinalizedRunStates` 使用跨表 UPDATE，没有分页；本次再次捕获已运行约 62 秒且等待 DataFileRead | 首批修复：限定待收敛 Run，分页提交 |
| 迁移控制/重试 | 两张表在 30 秒内分别读取约 1,689 / 1,017 MiB；相关索引和部分有界扫描已存在 | 先归因到调用点，再修剩余历史扫描；不能直接认定缺索引 |
| 两类 outbox | crawler 领取计划为 BitmapOr；publication 领取使用部分索引，但 destination 与租约条件仍为 Filter；历史采样存在秒级读取等待 | 独立验证分支筛选、排序及待处理集合大小，按实测选择索引与改写 |
| 增量任务行锁 | tasks 的 FOR UPDATE、心跳 UPDATE 出现约 2–3 秒等待 | 第二阶段缩短事务持锁范围，保持 generation/lease/fence 校验 |

边界：表读取增量不是某条 SQL 的完整归因；EXPLAIN 是估算计划，不证明实际返回行数、耗时或缓存命中。历史 30 秒快照只能定位方向，不能直接作为完整性能验收基线。

已经存在且已核对的优化：

- 已部署控制器的 `migrationThroughput.js`、`migrationBatchControl.js`、`migrationSystemRetryRecovery.js`、`controller.js` 与当前工作区对应文件 SHA-256 一致。
- Controller throughput 与 Finalize source-change recovery 均已启用。迁移完成批次保留最终采样、恢复扫描分 active/legacy 游标、Finalize 队列水位控制均已存在。
- 迁移控制表的批次/状态、candidate、初始 pending 索引，以及 retry active/legacy 部分索引均在生产存在，不重复创建。
- `autovacuum=on`，四张热点表均有优化器列统计。`pg_stat_user_tables.n_live_tup` 与 `pg_class.reltuples` 差异很大，维护时间字段为空；这不能证明表只有少量有效行或严重膨胀。重建、VACUUM FULL、全局 ANALYZE 均不是默认动作。

## 2. 阶段 A：建立可比基线与可回退降载

### 2.1 基线和 SQL 归因

采样 15 分钟，之后每一阶段观察至少 30 分钟；固定 Worker 接单数、全量/增量构成，记录输入变化。按应用边界记录以下指标，查询日志只含固定标签及参数规模：

- DB 连接取得等待、SQL 耗时、事务持续时间、任务行取锁等待；增量创建到指令、结果收到到 applied 的 P50/P95。
- 增量唯一成功任务/分钟、失败率、续租失败、重复应用、发布等待数量与最老到期任务年龄。
- 数据库 cgroup 实际读字节、CPU、I/O pressure；PG 读取块、宿主机 iowait；不能用 `blks_read` 替代实际磁盘读取。
- Controller 每个后台循环的调用数、扫描候选数、有效状态变化数、完成轮次和游标进度。
- PgBouncer 的等待客户端数、最长等待和服务连接占用；44–45 个后端连接并不能排除池内排队。

复用已有 controller_work_cycle 日志，给缺失路径添加固定 SQL 标签。通过 PgBouncer transaction pooling 获取的 application_name 只作辅助，服务归因同时看 SQL 标签与客户端日志。不要为获取 pg_stat_statements 立即重启生产库；需要扩展时另排维护窗口。

### 2.2 临时降载

可先把已有 `PUBLICATION_ONBOARDING_RECONCILE_INTERVAL_MS` 从默认 60 秒调至 300 秒，保持批量 25。影响是历史自动 onboarding 补偿频率降低；正常 Finalize 提交后的发布衔接继续执行。观察 backlog 年龄与增量延迟，恶化则恢复原值。当前该变量未显式设置，应在持久化部署配置中记录原默认值与新值。

不要通过增加 batch size、DB 连接数、增量 Worker 数来掩盖慢扫描。不要关闭发布器或暂停整个 youtube-finalize 队列。两个 publisher 的 poll 参数只在空批次后生效，单改 poll 间隔不能限制有积压时的持续读取。

## 3. 阶段 B：先修两条已确认的昂贵扫描

### 3.1 自动发布 onboarding：事件驱动 + 有界历史补漏

修改入口：`publicationChannelOnboarding.js:reconcileAutomaticPublicationBacklog`、Controller 调用处、数据库 schema 与对应迁移脚本。

1. 正常路径继续使用 `commitFinalizedProfile → reconcilePublicationAfterFullCrawl`。需要异步补偿时，在同一业务事务中持久登记 channel/run 的待检查 generation；请求去重但不吞掉新一代变更。
2. 补偿循环从已到期的请求集合领取，保留租约、过期回收、`FOR UPDATE SKIP LOCKED`、提交前 ownership/Run 校验。初始每批 25；只对这批 ID 执行完整资格判断。
3. 原全历史查询改成按 channel_id 的 keyset 分页：先取至多 200 个窄 ID，再进行关联。不能把 LIMIT 留在跨表排序之后，也不能用 OFFSET 翻页。
4. 扫描游标、轮次上界和租约持久化；该页请求全部登记成功后才推进。崩溃重复扫描允许，不能丢任务。变化发生在已扫过页的频道，依靠事务登记唤醒，下一轮审计兜底。
5. stream 激活、capture_enabled_at、delivery mode、ownership、promotion、Finalize、Agent 及 gap repair 状态改变都可能影响资格。必须覆盖这些唤醒来源；stream 范围改变时持久安排有界重扫，不能只监听 Finalize。
6. 同一页公共 stream eligibility 可在同一数据库快照内计算一次；最终写入事务重新核验，不能用跨请求 TTL 缓存代替所有权判断。尚未就绪的频道退避，不阻塞后续页。

待处理请求表的候选索引为 `(next_check_at, channel_id)` 的 pending 部分索引，并按实际领取谓词验证。这里是新设计，当前仓库没有已实现的 onboarding 请求表，不能直接引用为现成能力。

语义约束：保持 bootstrap pending 的可恢复性、capture 时间边界、已有 owner 排他性、publication gap repair 阻断及完整初始包校验。频道跨批次重试时仍以当前 Run/fence 为准。

### 3.2 Finalize 终态收敛：小集合更新

修改入口：`controller.js:reconcileTerminalFinalizedRunStates`，优先复用已有 `finalizedProfileStore.js` 的正常提交一致性路径。

- 正常 Finalize 保持画像写入、Run 终态和发布衔接的事务一致性；后台只处理历史不一致。
- 按 pipeline cycle 与 Run 游标选择至多 100 个尚未收敛的 Run ID，再检查 channel.latest_run_id 和合法 finalized status，并原子更新。
- 对于“循环必须看大量终态行才能找到少量异常”的情况，在测试库评估未收敛部分索引：谓词必须覆盖 `status<>'done' OR detail_status<>'done' OR finished_at IS NULL`。候选键为 cycle 表达式与 run_id；是否值得建索引取决于未收敛占比和参数化计划，不能直接上线。
- 无当前批次时走持久化全局分页审计，不退化成每个主循环执行一次全表 UPDATE。
- 每页独立事务；超时不推进游标；多个实例需要数据库租约，进程内防重入不足以解决跨实例重复扫描。
- 批次完成判断须知道当前还有未检查页或未收敛项，不能因一页为空或只处理 100 个就提前标记 completed。

## 4. 阶段 C：迁移统计、重试与 outbox

### 4.1 迁移统计和重试

入口：`migrationThroughput.js`、`migrationBatchControl.js`、`migrationSystemRetryRecovery.js`、`server.js:/api/migration/batches`。

- 逐个标签核实 3 百万 migration_control_items 与 46 万 retry 行的顺序扫描由哪个调用点触发；特别检查 API throughput 开关、缺采样的冷启动回退、批次完成检查与其他调用者。
- 当前 `createMigrationProgressReader` 缺任一批次采样时会调用未限定批次的 `loadMigrationControlProgress`。改为仅为缺失批次排入后台采样；接口返回最后可用值、采样年龄或“统计准备中”，不能把缺失计数显示成 0，也不能让每次页面请求触发全历史统计。
- 终态批次保留最终采样，明确“结算时快照”和“之后的历史恢复现状”两个口径，避免静态样本被误读为实时数据。
- 活动批次先限定 batch，再做 counts 与 publishing_count；publication outbox 与 revision 的关联必须受该批次候选集合约束。在尚未证明汇总是瓶颈前，不直接引入全套触发器计数器。
- 保留已有 retry active/legacy 两路游标。新增可配置的 legacy 轮次间隔和持久进度，避免重启后反复从头读；active 收尾不等待历史大轮询。轮次有进度和最老待恢复时间告警，不能无限退避饿死历史任务。
- Finalize 当前每 2 秒执行分页审计，完整轮次间隔为 1 小时；source-change 队列水位默认 200。补充明确的每秒 DB 工作预算与审计进度，不能把减少任务队列长度误认为已限制审计读取量。

### 4.2 Outbox 领取与重试

入口：`crawlerOutboxPublisher.js:claimBatch`、`publicationPublisher.js:claimBatch` 及各自运行入口。

先在接近生产分布的独立 PostgreSQL 上比较现有查询与候选方案：绝大部分历史已完成，少量 due pending、未来 retry、未过期和过期 lease，含并发持锁。

| 候选索引 | 解决的问题 | 需要验证的代价 |
| --- | --- | --- |
| crawler pending：`(next_attempt_at, created_at, event_id) WHERE status='pending'` | 到期集合过滤，排除已完成历史 | created_at 全局排序可能仍需 Sort |
| crawler publishing：`(lease_expires_at, created_at, event_id) WHERE status='publishing'` | 过期租约直接定位 | 状态迁移带来的索引维护 |
| publication pending/retry：`(destination, next_attempt_at, revision_id)` 对应状态的部分索引 | destination 与到期条件共同约束 | occurred_at 在 revision 表，不能假称消除了排序 |
| publication leased：`(destination, lease_expires_at, revision_id) WHERE status='leased'` | 避免遍历全部 leased 历史 | 多 destination 数据分布与更新成本 |

这些索引是待比较的选项，不是一组必须全部创建的 DDL。同时比较“按创建顺序扫描待处理部分索引”的方案，避免大量未来任务使时间筛选退化。

若拆分 pending/expired 两个领取分支，必须在同一事务里有界锁定、重检状态并更新租约；重复领取、并发 SKIP LOCKED、过期回收和旧 owner 回执都要覆盖。若分支预取上限改变全局领取顺序，要明确其影响并验证频道/domain 的 sequence gap、长期失败任务公平性，不能把简单 UNION ALL 当成无语义变化的替换。

publication 的 `release-retries` 当前一次更新全部到期 retry_wait，改成按 destination 的有界领取更新，避免大事务。两类发布均保持持久回执确认后才标完成，失败退避、dead_letter 和 baseline covered 状态不变。

## 5. 阶段 D：缩短增量任务行锁时间

入口：`remoteNodes/channelPlanStore.js` 及其结果应用调用者、`youtubeSessionStore.js`、任务心跳/租约更新路径。

1. 先分开测连接池等待、取锁等待、持锁时间、中心事件循环延迟，不能把 idle in transaction 快照直接解释为 SQL 在执行。
2. JSON 解析、压缩、哈希等与锁内最新状态无关的计算放到取锁前；网络和对象存储调用不得无必要地占用任务行锁。缩减可合并的数据库往返。
3. 提交前重新核验 task generation、node/lease、coordinator、Run 和 publication fence；保持统一锁顺序。不要简单删除 FOR UPDATE，也不要让已经过期的结果通过。
4. 如需把结果应用拆成多事务，先持久化幂等 checkpoint；证明 crash/replay 与所有权转移正确后再拆。心跳改写必须保持过期 lease 不可复活。

## 6. 验证与放行标准

先在独立 PostgreSQL/Redis 运行正确性和性能验证。生产只做受控观察；不在繁忙生产库执行全历史 EXPLAIN ANALYZE。以下数值是拟定放行门槛，不是已经达到的结果。

| 维度 | 放行条件 |
| --- | --- |
| 查询性能 | 独立库 EXPLAIN (ANALYZE, BUFFERS) 证明首批两条扫描的每轮读取块减少至少 80%；样本含“无任务”、历史量放大和失败频道，不能仅测试热缓存 |
| 有界性 | 历史量增加时每页候选数/事务更新数不突破上限；被锁任务跳过后后续可回收；后台每轮进度可见 |
| 在线延迟 | 同负载 30 分钟下，增量结果收到到 applied P95 目标降低至少 20%；若基线过低，则以不回退超过 10% 和读压力下降作为门槛 |
| 有效吞吐 | 增量唯一成功任务/分钟不下降超过 5%；失败/续租超时无新增趋势，不能以减少接单获得“性能改善” |
| I/O | 数据库 cgroup 实际读字节/每千个增量成功任务目标下降至少 30%；同时报告总读取和历史补偿完成量，避免通过停补偿达标 |
| 补偿完整性 | 新事件持久化、旧事件最终被扫描到、崩溃后不丢请求；持续有负载时历史恢复仍有最低进展，最老 due 年龄不连续三个 5 分钟窗口增长 |
| 发布正确性 | 无丢失/重复业务应用，无新增 owner/fence 越权或 sequence gap 回归；已完成批次、暂停恢复与旧 Worker 回执均保持原语义 |

回归重点：onboarding capture 边界/owner 切换/bootstrap pending；终态收敛跨页与批次结算；publisher 并发领取、过期 lease、回执重放；迁移 40 万以上库存和终态冷启动；增量过期 generation 与心跳并发。

扩展现有 `publicationChannelOnboarding.postgres.integration.test.js`、`publicationPublisher.postgres.integration.test.js`、`controllerThroughput.postgres.redis.integration.test.js`、`migrationBatchControl.postgres.integration.test.js` 与远程 fence 套件。crawler outbox 补真实并发事务测试，不能只靠 SQL 字符串 mock 证明 SKIP LOCKED 正确。

## 7. 发布顺序和回退

1. **PR 1：扫描边界与诊断标签。** 自动 onboarding、终态收敛及独立验证；新增持久请求/游标 schema 先兼容部署，生产者登记先启用，消费者读路径切换后再停旧扫描。新旧消费者通过租约防止同时处理。
2. **PR 2：迁移/API 与恢复调度。** 仅修改已归因的扫描入口，加入历史轮次预算。避免与迁移队列版本控制修复混在一起。
3. **PR 3：outbox 查询与经验证的索引。** 先逐个 `CREATE INDEX CONCURRENTLY`，检查有效性和空间，再切查询；每次只建一个重索引。现有索引保留至完整观察结束，避免回退缺失依赖。
4. **PR 4：增量事务持锁优化。** 独立验证所有权及幂等，最后发布。只有数据库读取改善后仍有明显中心 CPU 瓶颈，才进一步做中心 profile/并发拆分。

每一步保留旧镜像、持久化配置、游标和待处理 generation，单组件切换并观察至少 30 分钟；不得为“验证”清空 failed、outbox 或恢复请求。

立即回退条件：出现 fence/所有权错误、丢任务或重复业务应用、死锁持续新增；或连续两个 5 分钟窗口增量有效吞吐下降超过 10%、应用 P95 升高超过 20%、发布最老 due 年龄持续恶化。新扫描超时仍不推进游标，恢复旧路径后通过兼容扫描和已有主数据补偿；新请求表及索引先保留，确认存量请求可恢复后再决定清理。

shared_buffers、work_mem、独立数据库/只读副本扩容放在上述修复之后，依据整机可用内存与真实 I/O 复测决定；任务租约、所有权和结果提交仍走主库。

## 8. 本次完成与尚待验证

已完成：现有代码和生产部署一致性核对、19 个现存索引检查、四条查询的估算计划、表统计与 autovacuum 配置检查，以及本方案。

尚待实施：性能复现数据集、上述 PR、候选索引对照、集成回归及灰度放行。原始只读计划暂存 `/tmp/db-pressure-plan-20260923/plans.json`。方案没有把尚未执行的维护、测试或性能目标写成已完成结果。
