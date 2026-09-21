# 增量抓取数据库优化方案

2026-09-20。状态：方案与只读预检完成；其中采集库 shared_buffers 已单独调整为 1GB 并完成恢复验收，其他 SQL、索引、事务和维护改造尚未执行。目标是降低中心准备、结果入库和收尾的等待，改善有效任务吞吐，同时保持任务所有权、重试、结果入库和发布语义。

最新发布记录：[1GB 参数生效与恢复验收](CRAWLER_POSTGRES_SHARED_BUFFERS_RELEASE_20260920.md)。09:27 UTC 数据库正常重启，09:45 UTC 完成 62 个增量 Worker 连续恢复采样。下文参数盘点为调整前基线；本次仅改变 shared_buffers。

依据：[性能诊断](INCREMENTAL_PERFORMANCE_DIAGNOSIS_20260920.md)、[诊断机器报告](../reports/incremental-performance-diagnosis-20260920.json)、[索引/参数/估算执行计划预检](../reports/incremental-database-plan-preflight-20260920.json)。方案预检阶段只执行目录查询和 `EXPLAIN (FORMAT JSON)`，没有执行 `EXPLAIN ANALYZE`、DDL、UPDATE 或配置变更；后续参数变更见上方独立发布记录。

## 已核实的现状

- 515 个成功、第一执行代次的增量任务平均 54.16 秒：派发前及准备 12.86 秒，采集/回传 23.08 秒，结果应用 8.50 秒，后续收尾 9.72 秒。非采集阶段约 31.08 秒，不等于全是数据库时间。
- 主机 IO wait 平均约 13.5%；活动数据库会话采样以 IO 等待为主，同时存在任务行事务锁等待。中心平均 CPU 约 1.12 核，上限 4 核。
- 发布补偿 `publication-auto-onboarding:backlog` 曾被观察到已执行约 353 秒。当前估算计划包含 `channels`、`finalized_profiles`、`channel_delivery_state` 顺序扫描；`LIMIT 25` 没有将前面的扫描和连接工作限制到 25 行。
- outbox 约 167 万行，现有 `idx_crawler_outbox_publish` 已被当前计划通过 BitmapOr 使用。不能据此断言缺索引；需要区分历史负载下计划、缓存、死元组与锁等待。
- `shared_buffers=128 MiB`，`effective_cache_size=4 GiB`，`work_mem=4 MiB`，`effective_io_concurrency=1`，`max_wal_size=1 GiB`，`max_connections=100`。数据库和其他服务共享主机，不能按独占数据库服务器直接套用内存比例。
- autovacuum 已开启，默认 vacuum/analyze 比例分别为 20%/10%。估计死元组：tasks 约 7.8 万/存活 45.7 万，outbox 约 13.1 万/存活 167 万；这些计数不是精确膨胀量。
- `whole_channel_inputs` 总占用约 27.7 GiB，包含 TOAST 和索引；代码已有保留期后裁剪逻辑。总大小不能直接认定全部可删除或全部为膨胀。

## 建议顺序

| 阶段 | 主要动作 | 为什么先做 | 发布方式 |
| --- | --- | --- | --- |
| DB0 | 建立可比基线及 SQL/事务计时 | 用有效吞吐和阶段耗时判断收益 | 先只读；必要埋点随应用发布 |
| DB1 | 优化发布补偿、终态收敛及恢复扫描 | 已有明确大范围扫描和慢查询证据 | 查询改造与少量候选索引，逐项上线 |
| DB2 | 缩短增量任务行持锁区间、减少重复读取 | 对准备、入库和收尾路径直接生效 | 并发与恢复测试后发布中心 |
| DB3 | 表级维护、outbox 针对性优化 | 减少历史数据与高频更新带来的额外读写 | 低峰、限速、分表验证 |
| DB4 | 评估内存/IO 参数和性能统计扩展 | 修正偏保守参数，建立长期观测 | 独立维护窗口，受控排空和重启 |

## DB0：基线与成功标准

1. 在没有全量灰度新增负载、没有中心重启的窗口，连续观察至少 30 分钟。记录任务类型、采集视频数、代理失败和队列供给，避免把工作量变化误认为性能提升。
2. 核心指标为有效且非重复的业务完成量/分钟、started→completed P50/P95、准备/采集/应用/收尾耗时、重试及失败率；同时观察 outbox 最老待发年龄、待恢复任务年龄和发布完成量，防止以饿死后台任务换取增量吞吐。
3. 对目标 SQL 分别记录次数、总耗时、P95、连接池排队和事务持锁时间。参数和任务数据不进入日志。优先使用短期应用计时；`pg_stat_statements` 的预加载另列 DB4。
4. 建议验收目标：同等负载下非采集阶段平均耗时较可比基线下降至少 20%，总任务 P95 有下降趋势，有效完成量不下降，重试/失联/发布积压不恶化。这是待验证目标，不是承诺提速。

## DB1：先优化已定位的大范围后台扫描

### 发布自动接入补偿

入口：`publicationChannelOnboarding.js:reconcileAutomaticPublicationBacklog`。现有查询同时判断发布流资格、频道资格、历史晋升、当前 finalized 与是否已接入；流资格中的 delivery EXISTS/NOT EXISTS 产生额外大范围扫描。

建议拆分策略：

- 在同一一致性语义下，先一次性计算可用发布流资格，避免对每个频道重复计算；可评估 MATERIALIZED CTE，但必须用计划和实测确认，不能假定它总是更快。
- 长期将候选维护为持久待处理集合，由资格变化触发入队；保留低频、有游标的兜底扫描。第一版也可先做有界候选扫描，但必须记录扫描游标、失败重试并周期性回扫，保证晚到 finalized、流状态变化及早期不合格频道不会永久漏掉。
- 不得把“在所有过滤之前 LIMIT 25”当作等价优化；会导致漏处理、空批次或饥饿。按稳定顺序选择候选，在事务中重新确认资格、所有权和幂等键后落库。
- 控制扫描每轮行数和时间预算；避免长查询一结束便立即重跑。现有轮询已经有间隔门槛，需要确认实际配置和任务耗时，再调整完成后的间隔；不能直接关闭补偿功能。

候选索引只作为验证清单，先选择能改善真实计划的一两个：

| 表 | 候选索引方向 | 验证点 |
| --- | --- | --- |
| `publication.channel_delivery_state` | `(publication_stream_id, channel_id) WHERE mode <> 'online'` | 当前索引以 destination 开头，而该反查没有 destination 条件；验证稀疏非 online 集合是否避免扫描约 20 万 delivery |
| `crawler.finalized_profiles` | `(channel_id, run_id) INCLUDE (finalized_at) WHERE status='ready_auto' AND finalized_at IS NOT NULL` | 现有仅 channel_id 主键；验证是否减少宽表读取，注意频繁更新会影响 index-only scan 效果 |
| `crawler.channels` / `channel_candidates` | 仅在最终候选查询确定后选择匹配过滤与顺序的部分索引 | 已有晋升 candidate/run 唯一索引，不重复创建；不得为了消除 Seq Scan 而盲目加宽索引 |

以上均未执行。线上采用 `CREATE INDEX CONCURRENTLY`，一次一个、不在事务块内；预留磁盘/WAL 余量，监测长事务等待、复制延迟（若存在）和 invalid index。索引有写放大成本，不一次性全建。

### 终态收敛与 finalize recovery

入口包括 `controller.js:reconcileTerminalFinalizedRunStates` 与 `finalizeRecoveryPolicy.js`。优先将反复跨历史 runs/channels/finalized 的更新改为只处理状态变化或未收敛候选；采用有界批次、稳定游标与定期补漏。恢复扫描已有部分限界机制，进一步优化其输入集合和连接计划，不重复造一套恢复逻辑。

验证必须覆盖：晚到 finalized、同频道新 run 替换旧 run、跨 pipeline cycle、已完成行不重复更新，以及扫描中途重启后不会漏处理。

## DB2：减少增量任务行竞争

`channelPlanStore.lock()` 的 `FOR UPDATE` 被协调器、命令轮询和结果接收共用。采样已发现对应行锁等待。`WholeChannelStore.input/result/receive` 涉及大 JSON、分块和解码；其中部分工作在事务持锁期间执行。

建议先记录每个调用的池等待、取得任务锁之前的等待、取得锁后的持锁时长，再逐点修改：

1. 批量读取确实需要的列，减少重复 `SELECT *` 和历史大 payload 读取；轮询空结果优先利用已有 NATS 通知，保留超时重查保障。
2. 将可纯计算的序列化、哈希和解码移出临界区。若先读后算，写入前必须重新检查 generation、租约、身份、manifest/hash 和业务执行版本；不能使用失效快照提交结果。
3. 合并能安全合并的往返，避免一项确认使用多个独立查询；事务内不得等待远端网络。业务写入与完成标记原有原子性保持不变。
4. 评估纯读取的轮询是否能使用更轻的校验路径，但不直接删除 `FOR UPDATE`。任何放宽都要证明无法把旧 generation 指令发给新 owner，无法在停止后继续写入。

回归必须覆盖并发轮询/续约/结果接收、停止与迟到结果、重复分块回执、中心崩溃恢复、租约代次切换、API continuation 和结果幂等；再通过单次中心受控发布验证生产阶段耗时。

## DB3：维护与 outbox

- 对 tasks、commands、outbox 等高更新表评估更细的表级 autovacuum 阈值，例如 vacuum 比例 2%–5%、analyze 比例 1%–2% 作为测试起点，结合表规模、变更速率和成本限额调整。不要全库同时降低阈值，以免增加当前 IO 压力。
- 需要时低峰逐表执行普通 `VACUUM (ANALYZE)`；普通 VACUUM 主要回收内部可复用空间，不承诺立即减少文件大小。先检查长事务、复制槽和 xmin 等阻止回收因素。没有证据时不执行 VACUUM FULL、全库 REINDEX 或大批量 DELETE。
- 核对 whole-channel 已有 7 天默认保留与分批裁剪是否实际运行、处理速率是否跟得上。仅处理符合保留规则的 applied 数据；保留待确认、失败/恢复、API 等待数据以及旧回执校验所需的摘要。必要的归档/物理压缩属于单独工作，不计入首轮提速。
- outbox 当前计划已命中索引，先对照真实慢执行和空队列/积压/过期租约场景。若证明 OR 的两个分支或排序造成扫描扩大，再评估两个部分索引和分支候选合并；保持统一排序、`SKIP LOCKED`、租约归属及幂等，不让失败重试永久饥饿。
- 发布网络请求当前已在 claim 事务之外，不将其当作现有问题重新改造。

## DB4：参数与长期观测

09:15 UTC 补充盘点：[共享主机内存预算](../reports/crawler-postgres-memory-budget-20260920.json)。主机 31.33 GiB 可见内存、可用约 5.11 GiB、无 swap，运行 143 个容器；同机 Rota TimescaleDB 的 shared_buffers 已约 7.83 GiB，容器工作集约 8.66 GiB，66 个本地 crawler Worker 工作集合计约 5.04 GiB。结合已有内存回收压力，**当前建议采集库第一档为 `shared_buffers='1GB'`**；2 GiB 仅作为后续候选，不直接上线 4/8 GiB。1 GiB 比当前多 896 MiB，简单静态扣减后余量约 4.24 GiB，不是峰值下的保证。先保持 work_mem、连接数和其他数据库预算不变，观察至少 1–2 小时并覆盖一次代表性业务高峰；确认余量、内存压力、SQL 时延及有效吞吐后再评估是否升到 2 GiB。该预算快照采集时尚未修改配置；随后已按 1GB 完成独立发布，见本文顶部发布记录。

`shared_buffers=128 MiB` 对当前工作量偏保守，但 OS 页缓存也在工作，不能仅据此归因全部读盘。当前主机还有 Rota、中心和其他服务，调整前需建立完整内存预算。

- `shared_buffers`：先评估 1–2 GiB 起点，确认所有容器峰值、PG 连接/排序并发和系统余量后定值；需要 PostgreSQL 重启。不能直接跳到独占数据库主机常用的 25% 内存配置。
- `effective_cache_size`：根据真实可用缓存设估计值；它影响计划选择，不是分配内存。
- `work_mem`：暂不全局放大。对确有临时文件溢出的后台查询，可先用会话或事务级 8–16 MiB 做对比，并按“并发 × sort/hash 节点 × 并行 worker”计算最坏预算。
- `effective_io_concurrency`、`random_page_cost`：当前为 1/4，先在目标查询会话做不同设置的计划与实测对比，确认存储类型和 PostgreSQL 版本后再选值，不把强制索引扫描当优化。
- `max_wal_size`/checkpoint：先读取 WAL 与 checkpoint 统计。只有确认频繁请求 checkpoint/写峰值影响延迟后才调整，不为了本轮读 IO 问题盲改写参数。
- `pg_stat_statements`：当前未加载，可与 shared_buffers 调整合并到一次维护窗口；保存原配置，排空应用在途工作，重启后确认配置生效并恢复 62 槽。`track_io_timing` 可在评估计时开销后启用；不用全量参数日志替代聚合统计。
- 不增加 max_connections，也不在此阶段新增采集 Worker。数据库仍有存储等待时扩连接往往扩大争用。

## 发布、验证与回退

每次只发布一个可归因的改动，保留原查询、镜像和参数。查询改造先用生产规模的隔离数据验证结果集合及并发语义；生产 `EXPLAIN ANALYZE` 仅限已审查、限时的只读查询。对 UPDATE/DELETE 不以“最后 ROLLBACK”为由在生产做 ANALYZE，它仍会产生执行负载和锁。

新索引异常时停止后续创建并检查有效性；确认不承载约束且无其他依赖后，可并发删除本轮新增索引。应用问题回退镜像/查询开关；参数问题恢复原值，涉及 postmaster 参数时需再次受控重启。保留全部任务、spool 和已确认结果。

观察至少 30 分钟且任务构成可比，出现持续失败率上升、outbox/恢复年龄增长、IO 压力明显增加或吞吐下降时停止扩大发布并回退该项改动。不能只看中心状态数量下降就宣布成功。

## 推荐先执行的工作包

**先做 DB0 + DB1 的发布补偿查询优化**：补齐可比基线，在隔离环境验证流资格预计算与候选限界，挑选有效的少量索引，产出实际执行前后对照，再上线验收。随后做 DB2 持锁区间优化。DB4 的数据库重启单独安排，不与 SQL/事务改造同时发布。

15 Worker 节点内存换页是独立限制，数据库优化不能解决它；增加节点内存或受控降并发另行处理，以免混淆本轮数据库优化收益。
