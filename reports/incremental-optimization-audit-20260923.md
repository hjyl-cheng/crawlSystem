# 增量抓取优化巡检

检查时间：2026-09-23 09:15–09:18 UTC（北京时间 17:15–17:18）。生产访问全部只读；本次没有修改业务源码、任务状态、配置或部署。

结论：增量持续产出，但应优先处理任务长期 pending、详情缺失/候选位置冲突和 Plan/Run 状态不一致，再优化中心周转与数据库竞争。不能只增加 Worker，也不能把队列 completed 当作所有业务领域成功。

## 本次实测

| 指标 | 结果与口径 |
| --- | --- |
| 最近 15 分钟队列完成 | 1,198 个唯一 Job，约 79.9 个/分钟；完成执行 duration P50 29.26 秒、P95 69.57 秒，不含此前排队/重试 |
| 同窗口失败事件 | 52 条、47 个唯一 Job，最终错误均为预算耗尽；不是最初错误，也不能与 completed 简单相加计算失败率 |
| 业务 Plan 终态窗口 | 另一稍晚的 15 分钟查询：succeeded 1,199、partial 1、failed 2；与队列查询不是同一 cohort |
| 成功 Plan 构成 | About-only 455、Video-only 300、About+Video 444；必须按构成比较性能 |
| 启用远端 Worker | 主节点 47 + node02 8，共 55 个 enabled/accepting/online；备用节点 20 个 enabled=false |
| 传输任务快照 | leased 32、pending 5；后续两次确认其中 3 个 pending 已持续约 80–85 分钟 |
| 事件收件箱 | 非 applied 为 0；历史 sequence_gap 目前没有继续积压 |

96 个最近成功、首次 generation、已 applied 的整频道任务小样本：准备平均 6.23 秒；command 创建到收到平均 17.25 秒；收到到 applied 平均 5.69 秒；task 创建到 applied 平均 29.17 秒、P95 65.65 秒。这是完成样本，不含 task 创建前排队、最终 Feature 消费和全部重试。17.25 秒包括远端领取、抓取、传输与中心持久接收，不能全部归为网络耗时。

## P1：恢复长期未领取的定向任务

3 个任务创建于 07:52–07:54 UTC，09:18 仍为 pending；Plan=dispatched、Run=running。目标分别为 node02 的 incremental-2、incremental-6，以及主节点 incremental-38。三者目标 Worker 都是 enabled=true、accepting=true、online=true、activation_requested=true。

这证明“Worker 在线且接单”不足以证明指定任务能够推进。当前还没有完整复现其准入、旧执行结算及绑定阻塞原因，不能断言关闭接单或代理不足就是根因。

建议给 pending 增加年龄告警和具体阻塞原因，逐项核对目标绑定、旧 execution attempt、业务预算和租约结算；需要转交时通过既有恢复路径重新获取合法执行权，保留相同 Plan 和 checkpoint，不直接改目标或复活旧 lease。验收以三个任务实际推进或按事实终止为准。

## P1：修复仍在出现的详情缺失和候选位置冲突

当天 failed Plan 的 Run 错误互斥归类：详情缺失 34、候选位置冲突 19、网络/超时 28、其他或无对应 Run 错误 184，共 265。分类只检查 Run error_message 和 video domain error，不等于完整根因统计，也不是最近 15 分钟新增数。

- **详情续跑**：`incrementalCoordinator.js:193` 在整频道结果没有 captured detail 或 item.error 时抛出 `WHOLE_CHANNEL_DETAIL_MISSING`。`wholeChannelCollector.js:84` 遇到 api_pending 会停止后续详情采集。需要重放实际失败回执，覆盖已经 captured、API 回补完成、尚未采集 pending、已有 checkpoint 的混合情况；需要网络的剩余项应转入新合法执行代次。
- **候选位置**：`incrementalYoutubeJsVideo.js:872` 按 `(run_id,source_content_id)` upsert，同时覆盖 position；这不能处理另一个唯一约束 `(run_id,position)`。应复现同一 Run 重试、不同 phase 和补抓时的位置分配，保证稳定且不冲突，明确扫描排序与候选身份的区别。

边界：仓库已有 `remoteExecutionHandoff.postgres.integration.test.js:460` 的“API 后仍有未采视频”测试，因此不能宣称整个续跑功能不存在。测试依赖隔离 PostgreSQL/Rota，whole-channel 分支还依赖 NATS。本次未运行该集成环境，也未完整复现这 34 个 Plan 的触发条件；建议先用真实失败样本扩充场景，再修复。不要删除约束或将剩余 pending 标成 complete。

## P1：让队列、Plan、Run 和远端任务一致收尾

当天 Plan 已终态，但 Run 保持 running 且 Run 更新时间距采样超过 15 分钟的记录：

| Plan 状态 | Run 状态 | 数量 |
| --- | --- | ---: |
| failed | running | 237 |
| succeeded | running | 14 |

这些已超过通常几秒的完成传播窗口，值得单独治理。不能据此认定它们仍在采集，也不能把 succeeded Plan 改成 failed。

代码切入点：`incrementalChannelRunner.js:223`、`:227` 对 markDomain/fail 的异常静默忽略；`incrementalCoordinator.js:242` 也忽略远端状态收尾失败；`incrementalRunStore.js:122` 重领会将旧 Run 改为 running。`incrementalTerminalFailure.js:136` 会发布终态 Observation，但普通非永久频道错误路径未在该函数中统一更新 Run.status。这解释了状态可能分开演进的路径，尚未逐条证明所有 251 条记录的原因。

建议记录收尾失败的原始错误、执行代次和是否失去所有权；补充幂等终态对账。只有证明没有活动执行或 API/Agent 等待交接，才依据持久化领域结果和 Observation 收尾，保留 fence 与原子提交要求。告警应区分“正常交接等待”和“终态残留”。

## P1/P2：按日内积压和真实吞吐评估容量

09:16:55 UTC，当天尚未安排 scheduled_at、已经 eligible 的 planned 有 65,543 个：About-only 24,882、Video-only 13,517、About+Video 27,144。

若目标是代码规定的 UTC 21:30 调度窗口结束前全部处理，剩约 733 分钟，忽略新增工作和失败重试也需约 89.4 个/分钟；当前约 80 个/分钟的短窗口速度不足。它是容量风险提示，不是精确完工预测：待处理任务构成更偏 About+Video，外部代理质量和时段吞吐会变化，是否要求日内清空也需依据业务 SLO。

建议显示到期未调度数、最老 due 年龄、按任务构成估算的清空时间，并用至少 15–30 分钟的多个窗口测量。先恢复被卡住的槽位和失败重试，再决定增加节点或调度额度。不能把 65,543 个 planned 全部解释为 Worker 队列里等待。

## P2：减少中心重复计算、数据库往返和持锁

本次小样本准备+应用约 11.92 秒，占 task 创建到 applied 平均时长约 41%；这包含数据库、调度和计算，并不代表全是可消除 CPU。此前 06:33–06:34 的报告测得主线程约占一核 95.8%，本次没有重复 CPU profile。

已离线复现 `WholeChannelStore.input()` 的重复整包计算：每次读取一个分片，都在任务事务内读取整个 input_json，再执行 wholeChannelParts。数据库和锁用 stub，实际输入处理函数与协议函数未改。

| 合成输入 | 分片数 | 完整序列化次数 | 累计序列化量 | 3 次测量中位耗时 |
| --- | ---: | ---: | ---: | ---: |
| 0.5 MiB | 1 | 1 | 0.5 MiB | 21.92 ms |
| 1 MiB | 2 | 2 | 2 MiB | 42.40 ms |
| 4 MiB | 8 | 8 | 32 MiB | 384.40 ms |
| 8 MiB | 16 | 16 | 128 MiB | 1,285.85 ms |

次数是确定性证据；计时是本机合成样本，不含数据库、真实对象结构和生产并发，不能当成线上提速承诺。本次生产 96 个样本的 input_json PostgreSQL 文本大小 P95 约 592 KiB、最大约 1.51 MiB；它含 JSON 文本格式开销，不是精确协议字节数。没有证据证明线上常见 8 MiB 输入，因此该项优先级低于已观察到的任务失败/卡住。

优化建议：不可变输入按 command_id、generation、input hash 复用编码和 manifest，限制缓存字节数并支持失效重建；每次请求仍验证当前 lease/fence。`wholeChannelStore.js:80` 每收到一片还会读出之前全部结果分片，可先查收齐情况，收齐才完整读取/校验。**此次样本结果全部只有 1 片，因此结果分片累计读取目前只是潜在问题。**

`incrementalCoordinator.js:36` 的独立 query 会进入带业务 fence 的任务事务，`channelPlanStore.js:74` 又锁任务并续 coordinator。应先测每类 Plan 的 SQL 次数、连接池等待、持锁时间及事件循环延迟，合并安全的短操作；不能删 fence 或将网络请求包进长事务。

## P2：后台数据库降载和可观测性

09:15:46 数据库快照存在 DataFileRead，另有一组运行约 51.6 秒的并行查询；仅凭该快照不能确定 SQL 身份或宣称磁盘饱和。历史报告已定位昂贵的 onboarding、迁移统计和 outbox 扫描，见 [读取压力方案](incremental-db-read-pressure-fix-plan-20260923.md)。

建议按已有方案对一项后台扫描做受控降频对照，观察业务成功 Plan 吞吐、分阶段 P95、CPU/I/O 和发布延迟的净变化。本次没有执行降频、EXPLAIN ANALYZE 或加索引。初始两个较大窗口查询及一个 1000 条 task 查询触发 8 秒上限，随后改为按最近 100 个成功 Plan 的业务键点查成功；这也说明巡检查询本身必须有界，超时不能被当作业务耗时数据。

监控优先补充：唯一 Plan 的 succeeded/partial/failed 与领域完成数、当前活跃任务、pending 年龄、业务预算耗尽前的原始错误、API/Agent 交接、event inbox 最老等待和已发布未被消费的事件年龄。当前 inbox 非 applied 为 0，不应把 9 月 22 日的缺序积压再次列为当前故障；仍需保留投递对账，published 仅代表入队。

## 验证与交付

- 核查线上中心镜像 `pachongsys-94d1b12-intake-capacity`；六个关键文件（incrementalCoordinator、wholeChannelStore、wholeChannelProtocol、incrementalChannelRunner、incrementalTerminalFailure、incrementalYoutubeJsVideo）的 SHA-256 与当前工作区一致。
- 执行 `node --test test/incrementalChannelRunner.test.js test/incrementalRunStore.test.js test/incrementalVideoSnapshot.test.js test/wholeChannelRecovery.test.js test/baselineBundle.test.js`，5 个测试文件全部通过，不能代替 PostgreSQL/Rota/NATS 集成验证。
- `parts-benchmark.mjs` 调用真实 input handler，getter 计数复现重复整包序列化；只生成诊断脚本，没有修改生产代码。
- 原始聚合采样、三份只读查询脚本、微基准及测试输出保存在 `runtime/incremental-optimization-audit-20260923/`。各查询独立只读事务、statement_timeout=8 秒、lock_timeout=1 秒，时间窗口略有差异。
- 首次容器列表读取因自动审批服务并发超限而被拒绝；稍后同一只读动作重试成功，后续采样正常完成，无遗留审批阻塞。

建议首批顺序：定向 pending 排障与恢复 → 实际详情/位置冲突复现修复 → 终态一致性 → 按任务构成建立性能基线，再选择中心或数据库的一项优化做对照。上述事项可以分别验收，不承诺未经对照验证的统一提速比例。
