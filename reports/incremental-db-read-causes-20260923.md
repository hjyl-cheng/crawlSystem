# 数据库读取压力来源说明

基于 2026-09-23 11:42–11:43 UTC 的 61 秒读取采样，并追加只读表统计、配置检查及生产源码 SQL 的 EXPLAIN（没有 ANALYZE，没有实际执行补偿查询）。原始追加证据位于 runtime/incremental-read-causes-20260923/snapshot.jsonl。

结论：读取负担来自共享后台的大范围扫描，加上增量中心频繁访问多张状态表；有限缓存和频繁更新留下的旧版本可能进一步放大访问成本。不是“新抓取的数据量大，所以必须读这么多”，也没有证据能把所有压力归因于一个环节。

## 1. 批量返回少，不代表底层扫描少：已确认

生产自动发布补偿 SQL `publication-auto-onboarding:backlog` 使用 LIMIT 25，但需要关联 channels、channel_candidates、channel_runs、finalized_profiles，并检查 publication 的 owner、stream、delivery 状态，最后按 accepted_at/channel_id 排序。

只读 EXPLAIN 显示：

- channels 和 finalized_profiles 使用 Seq Scan，再进行 Hash Join、Sort、Gather Merge。
- channel_delivery_state 的 online 与非 online 条件均出现 Seq Scan；owned 状态的 channel_stream_state 使用 Bitmap Heap Scan。
- channel_candidates、channel_runs 等后续关联使用索引，不是完全没有索引。
- 估算最终返回 1 行，但前面仍需访问较大集合。估算行数不是真实扫描行数；没有执行 ANALYZE，不能用 cost 当作毫秒或把估算当成实际读取量。

前述采样窗口 channels/finalized_profiles 的 seq_tup_read 分别增加约 39.83 万/39.63 万行；活动快照捕获该补偿查询已运行约 233 秒。表级扫描计数还可能包含其他查询，不能将这些行全部精确归给这一条 SQL。

这是一条后台发布补漏路径，并非每个增量频道所必需的 YouTube 抓取操作，但它与增量共用数据库和缓存。

源码：services/qybullmq/src/publicationChannelOnboarding.js:675。默认补偿间隔为 60 秒；这是代码默认值，不是本次测得的实际启动频率，长查询和循环调度会改变实际频率。

## 2. 状态收敛与 Finalize 恢复也会读取历史数据：已确认路径，贡献待量化

`controller.js:733` 的 reconcileTerminalFinalizedRunStates 通过 UPDATE … FROM 关联 channel_runs、channels、finalized_profiles 查找状态不一致记录，没有本条 SQL 的批次 LIMIT；pipelineCycleId 为空时该条件不限制周期。即使最终只更新少数行，也需先读取候选集合。前述快照捕获相同形状语句已运行约 43 秒，但不能据此认定生产调用参数为空。

Finalize recovery 对候选频道还会通过 LATERAL 子查询读取 content_candidates 和 contents 的最新更新时间。现有 channelIds 分支已经限定输入集合，不能笼统称所有 Finalize 查询都是全库扫描；本次有 DataFileRead 等待证据，具体累计读取量尚未归因。

## 3. 增量自身的一次逻辑操作，可能带出多次状态读取：已确认

incrementalCoordinator.js 将 query 包装成受业务执行权保护的事务；已有外层事务时会复用，没有外层事务时才重新进入。channelPlanStore.transaction 会：

1. SELECT … FOR UPDATE 读取并锁定 remote_ingestion.tasks。
2. 调用 incrementalBusinessFence，依次读取并锁定 Plan、dispatch_outbox、business_run_bindings、channel_runs、channel_execution_attempts，还检查频道变更锁。
3. 执行本次业务操作。
4. 更新 coordinator_until。

轮询、输入分片、结果接收和心跳也会访问任务状态。多 Worker 并发使 SQL 数量远大于频道完成数。不能把逻辑操作数等同 SQL 次数，也不能根据全表更新/新建速率直接推算每个增量任务的成本。

前述采样中 tasks 读取块折合 27.05 MiB/s，channel_runs 65.18 MiB/s，channel_execution_attempts 17.32 MiB/s；这些是共享表统计，包含其他调用者。tasks 的 FOR UPDATE 还在多个活动快照中等待事务锁。锁竞争本身是等待，不直接等于更多读取，不能把全部 I/O 归因于锁。

这些校验用于防止过期执行和重复应用，不能直接删除。优化目标是减少同一短事务内的重复读取、无必要续约、重复整包处理，控制持锁区间，而不是取消业务正确性约束。

## 4. 热点表及数据体积较大，缓存不足以覆盖全部内容：事实已确认，缓存竞争贡献待量化

追加查询确认 shared_buffers=1 GiB、work_mem=4 MiB、effective_cache_size=4 GiB。effective_cache_size 是规划器估计，不是实际分配的缓存；操作系统也有页缓存，因此不能简单用“表大小大于 1 GiB”证明所有热点都装不下。

| 表 | 堆文件 GiB | 含索引/TOAST 总大小 GiB |
| --- | ---: | ---: |
| crawler.channel_runs | 1.68 | 11.29 |
| crawler.channel_execution_attempts | 2.31 | 3.10 |
| remote_ingestion.tasks | 1.09 | 1.40 |

后台扫描与在线任务共同访问这些内容时，有缓存竞争风险。前述实测 shared buffer 命中率 82.71%，缓存外读取块折合 197.35 MiB/s，而数据库容器实际块设备读取为 75.93 MiB/s；两者不是同一个计数，不能重复相加，也不能用差值精确计算 OS 命中率。

进一步拆分前述同一窗口：channel_runs 的 65.18 MiB/s 中，堆约 22.35、索引约 1.08、TOAST 约 41.74 MiB/s。TOAST 用于存放较大的字段，约占该表本次读取的 64%，说明大字段访问已是实际读取来源，值得核查 result_json 的访问和查询投影。tasks 则主要为堆读取 26.44 MiB/s、索引 0.62 MiB/s；不能把所有热点一概解释为大 JSON。

不能将 channel_runs 的全部 11.29 GiB 都称作 JSON 大小或死数据，也尚未将 TOAST 读取精确归给某条 SQL 或某个字段。临时文件新增约 295 MiB 说明有落盘，尚未确定是哪条 SQL 的排序/哈希，因此不直接建议全局提高 work_mem。

## 5. 高频更新及旧版本是需要检查的放大因素，尚未证明是当前主因

追加查询的估计死元组：tasks 约 11.9 万，channel_runs 约 14.2 万，channel_execution_attempts 约 6.1 万。统计为估计值，不能当成准确死数据体积或膨胀率。tasks 最近自动清理时间 10:46 UTC，Run 为 07:57 UTC；attempts 的 last_autovacuum 为空不意味着从未普通 VACUUM，也不能仅据此宣称清理失效。

租约、任务状态、Run 结果反复更新会产生 MVCC 旧版本，并涉及索引维护；未回收旧版本可能增加扫描成本和缓存占用。是否值得单表维护，应先查统计有效性、索引访问、清理进度与 xmin，而不是看到 n_dead_tup 就执行维护。

早间 crawler_outbox 读放大已有维护与复测证据；前述窗口该表读取仅约 0.058 MiB/s，追加查询也确认自动清理继续运行。它已不是当前主要读取热点，不能照搬早间结论。

## 处理顺序

优先限制后台补偿的候选范围并评估降频；其次测量增量每类 Plan 的 SQL 次数、重复读取和持锁时间，再选择最小改动；之后评估热点表维护及内存配置。新增索引、增加 shared_buffers 或扩机器都应以执行计划和同负载对照决定。

当前已确认“存在扫描和重复状态读取机制”，尚未证明它们分别占实际读盘的百分比。需要 SQL 累计统计或受控单变量对照才能精确分摊；本次没有做生产降频、EXPLAIN ANALYZE、参数修改或故障注入。
