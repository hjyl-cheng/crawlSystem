# 数据库修复执行记录（首轮维护与观察完成，事故补偿未完成）

按 `db_repaire.txt` 分步执行。已完成 Outbox 单表普通 VACUUM、自动清理阈值配置及 30 分钟观察，确认该表读放大下降；没有证明增量抓取整体提速。没有新增表、列或索引，没有部署业务代码。此次诊断发生两次操作事故，服务已恢复，但 3 个增量 Plan 尚未补采成功，因此整个修复工作不能标记全部完成。

## 基线与本轮优先级

- 在隔离工作树 `/tmp/pachongsys-db-repair-20260923` 准备候选配置，保留原工作区已有改动。
- 中心镜像为 `pachongsys-94d1b12-intake-capacity`，控制器为 `pachongsys-b862901-migration-idle-20260922`。
- 重新采集 15 分钟数据库/容器资源基线：`runtime/db-repair-20260923/baseline-stable-{db,host}.jsonl`。
- 初始候选是把自动 onboarding 补偿间隔 60000 改为 300000 毫秒。已保存原控制器配置及单变量候选，尚未应用；批量仍为 25。
- 新采样发现 `crawler.crawler_outbox` 是当前最大的表级读取来源。07:42:09–07:43:10 UTC，该表发布索引 `idx_crawler_outbox_publish` 增加 6,954,157 次条目读取；活动语句采样主要命中实际领取 UPDATE。
- 07:43:28 的真实计数只有 2 条 pending，没有 publishing/dead_letter。完整领取 UPDATE 的 EXPLAIN 使用现有 BitmapOr 和主键更新路径。不能用“缺索引”解释此现象。
- 新假设：频繁状态更新留下的可回收旧版本，造成领取索引及堆访问放大。验证方式是单表普通 VACUUM 前后比较索引条目读取/领取次数、表块读取、实际待发送量和业务结果；VACUUM VERBOSE 同时提供实际回收量。若无改善，撤销此归因，继续检查查询路径。
- 据此优先执行有成本预算的单表普通 VACUUM，暂不应用 onboarding 降频，以免混合变量。并非根据 `last_autovacuum` 为空或不准确的 `n_dead_tup` 单独决定维护。

## 操作边界

已执行（07:49:22–07:52:00 UTC）：

```sql
SET lock_timeout = '3s';
SET statement_timeout = '10min';
SET vacuum_cost_delay = '5ms';
SET vacuum_cost_limit = 200;
VACUUM (VERBOSE, INDEX_CLEANUP ON, TRUNCATE FALSE, PARALLEL 0)
  crawler.crawler_outbox;
```

这是清理不可见历史版本、让空间可重用的常规维护，不删除待发布业务事件，不重写整表，不构建新索引。关闭尾部截断，避免为归还文件尾部空间申请排他锁。普通 VACUUM 会产生维护 I/O/WAL，不能承诺磁盘使用绝对不波动，也通常不会把表文件直接缩小。

执行前确认当前库是 `newcrawler_crawler` 主库、执行角色具备表维护权限、没有复制槽/预备事务长期保留 xmin，最老活动事务约 54 秒。维护过程与结束后指标分开统计。上述连接当时实际经过 PgBouncer，因此多条 session SET 的后端一致性不应假定；以后维护必须直连。VERBOSE 记录的实际执行平均读取为主表 19.135 MB/s、TOAST 21.814 MB/s。

## 检查与候选配置

- `node --test test/publicationChannelOnboarding.test.js test/controllerWorkLoops.test.js` 通过。
- 叠加候选 Compose 配置的 `docker compose ... config --quiet` 通过。
- 隔离工作树 `bash scripts/verify.sh` 通过。
- 本轮无业务代码修改，上述测试不是生产性能收益证明。
- 控制器原配置与候选创建规格仅保存在 `runtime/db-repair-20260923/private/`，目录 0700、文件 0600，未提交凭据。
- `controller-config.py prepare` 只保存配置。没有执行 `apply`，不要把候选 300 秒配置误认为已上线。

## 诊断事故及恢复

07:30:23 UTC，临时 CPU 分析脚本通过 Inspector 的 Runtime.evaluate 执行动态 import 来关闭调试器，触发 Node 20 的 `ERR_VM_DYNAMIC_IMPORT_CALLBACK_MISSING` 未处理异常。这是此次诊断操作造成的生产中心中断，不能计入优化效果。

中心自动重启时，全量兼容执行槽暂未释放，连续启动失败，07:31:24 恢复就绪，容器重启计数为 8。Inspector 已关闭，危险脚本已替换为明确报错退出的停用版本，后续观测不再接入运行中主进程调试器。原 07:28 开始的基线作废，改从 API 容器执行只读采样。

07:44:47 检查 07:27:00–07:31:25 创建的 227 个增量传输任务：224 applied，2 failed/REMOTE_CENTER_INTERRUPTED，1 failed/Error，没有 pending/leased/received 残留。applied 不等同完整业务成功，3 个 failed 仍需核对关联 Plan 与队列，不能宣称无影响。

## 验收

已完成 08:01:19–08:31:26 UTC 的 61 次数据库采样及同步资源观察。业务结果每 5 分钟读取一次。资源指标和有效增量完成量分开报告；不把队列 completed 当作全部领域 complete。没有充分同负载证据，不宣称已提升增量吞吐，未继续新增结构。

## 已完成维护及持久配置

普通 VACUUM 耗时 158.325 秒。主表移除 20610 个元组及 367015 个失效行指针，90 个死元组受当时 xmin 限制暂不可回收。发布索引新增 3129 个 deleted page、已有 3072 个 reusable page；这些是复用空间，不代表新建索引或文件已缩小。主表 WAL 为 899283375 字节，TOAST 为 4341492 字节，合计约 0.90 GB。

07:54:57 设置该表 `autovacuum_vacuum_scale_factor=0.005`、`autovacuum_vacuum_threshold=1000`。约 187 万行时触发参考值约 10355，原默认约 374245。保持全局 worker 数量、成本和扫描间隔。此前该表 reloptions 为 NULL；应用前用同一事务脚本 ROLLBACK 验证，应用后读取配置确认。

可审核 SQL 在 `ops/db-maintenance/crawler-outbox-autovacuum.sql`，回退在同目录 `crawler-outbox-autovacuum-rollback.sql`。两者设短锁超时、核对库名和现存配置，回退只恢复这两个参数，不覆盖其他表设置。普通 VACUUM 不可“撤销回收”，也没有需要恢复的已删除业务数据。

## PgBouncer 诊断事故

07:42 和 07:53 的索引归因脚本错误使用 session 级 `SET default_transaction_read_only=on`。API 的 POSTGRES_HOST/PORT 实际为 `crawler-pgbouncer:6432`，并不是直连；此前按环境变量名称推断直连是错误的。PgBouncer 是 transaction pooling，`server_reset_query_always=0`，会话状态影响了池内后端，发布器出现 `cannot execute UPDATE in a read-only transaction` 和 `Crawler Writer database must be writable`，短时停止并自动重启。07:56:06 有 165 条 pending，最老 07:53:06。

自动审批拒绝生产库 `RECONNECT`，理由是可能影响连接且缺少明确授权；没有执行该动作。改为获准的事务内复位诊断参数，既不关闭连接也不取消事务。第一次租到的后端参数已为正常默认值，因此不能把自动恢复全归因于此复位。后续检查与清理仅针对探针留下的已知设置。

07:58:13 发布器重新运行；08:00:06 pending/publishing/dead_letter 计数均为 0，近期日志显示持续发布，最近 4 分钟已发送 633 条事件，未见新错误。后续探针已改为明确的 `crawler-postgres:5432`，实际验证端口 5432、默认 read_only=off；只读和 statement_timeout 仅在事务内设置。

原 07:53–07:54 的“零索引活动”发生在发布异常期间，完全剔除，不能作为 VACUUM 收益证据。07:42 后的基线也可能受此影响，因此原 15 分钟窗口虽完整采集，不能作为严格无干扰的业务吞吐对照。最终将只采用发布器正常工作的新观察窗口，并明确资源机制验证与业务因果证明的区别。

中心事故的 3 个失败任务进一步核对结果：2 个 `REMOTE_CENTER_INTERRUPTED` 的 Plan 均 succeeded、所有所需领域 complete，但 Run 仍 running，Redis Job 因 `INCREMENTAL_BUSINESS_FENCE_STALE` 为 failed；保留该事实，不改写状态或重复抓取已完成领域。另一条 Plan failed、video failed、队列因 `Rota Business Run budget exhausted` 失败。

### 正常运行时的机制复测

08:04:13–08:05:14 UTC，发布器持续运行时，发布索引增加 315 次扫描、71343 次条目读取，约 226 条/扫描；此前约 62650 条/扫描（111 次扫描、6954157 条）。这支持死索引项清理显著降低读放大的结论，不等同于 SQL 每调用的精确扫描次数或同负载全链路提速。此窗口主键索引也增加 103 次扫描，发布持续正常，排除了停工造成零读量的伪改善。

08:04 的 30 次池连接检查覆盖 17 个后端，未再发现只读默认值；其中一个后端残留探针 3 秒 statement_timeout，已在租用它的事务内恢复默认 0。全程没有 RECONNECT、连接中断或业务事务取消。被拒绝的重连脚本已停用。

08:08:36 检查新增死元组 3102，自动清理尚未再次触发。不能把配置已生效写成自动清理已实测发生。

## 诊断事故任务恢复

按 Redis failed 集合实际时间范围 07:28–08:05 只读核查，发现 3 个增量 Job、1 个 Finalize Job 明确以只读事务错误失败。08:20:44 逐项核对 Job 身份、原失败原因、关联 Plan、新计划和频道活动执行，通过现有 `Job.retry('failed')` 重试，未改写 Plan/领域证据或放宽执行权。

- Finalize `run:426a99c0-45fc-4017-acd2-144b8a612742` 已 completed。
- Plan `7640e221-ac0a-501c-9883-88cf56b6883b` 原 dispatched，重试最终因 `Rota Execution Route budget exhausted` 失败；08:31 核查 Plan 也已 failed、Run failed，尚未补采成功。
- Plan `f7413213-0759-5a57-86e4-b5d26d2529db`、`5da1d799-90a7-5c79-a4c0-8aeb585db35c` 已通过失败 Observation 收敛为 failed。原有队列重试被远端 `INCREMENTAL_BUSINESS_FENCE_STALE` 拒绝，仍未恢复。没有手工把 Plan 改回 dispatched/succeeded，没有重置 Rota 预算或跳过 fence。这是本次诊断造成的未解决影响，不能以“发布器已恢复”掩盖。

首次重试前检查因按 run_id 查询尝试表超时，尚未产生任何重试；改为使用既有 channel_id 索引、更保守地拒绝频道上任何活动尝试后执行。

## 最终观察结果与边界

前窗口：07:33:38–07:48:40 UTC，31 样本/902 秒；后窗口：08:01:19–08:31:26 UTC，61 样本/1807 秒。前窗口 07:42 后受会话事故污染，不能作为严格无干扰 A/B。下表仅列同时测得的观测值，不将差值解释为全部由修复造成。

| 指标 | 前窗口 | 后窗口 | 解释 |
| --- | ---: | ---: | --- |
| Outbox 表及索引读取块折合 | 122.90 MiB/s | 0.073 MiB/s | 与失效行指针回收、每索引扫描条目下降一致 |
| 数据库总读取块折合 | 244.85 MiB/s | 87.75 MiB/s | PostgreSQL shared buffer miss，可由 OS 缓存满足 |
| shared buffer 命中率 | 81.30% | 89.57% | 不代表整个存储缓存命中率 |
| PostgreSQL cgroup 实际读取 | 46.45 MiB/s | 55.64 MiB/s | 没有证明物理读盘降低 |
| 主机 iowait | 7.10% | 8.20% | 还有其他后台负载，未改善 |
| PostgreSQL CPU | 3.04 核 | 2.80 核 | 不独立证明增量提速 |
| 中心 CPU | 1.18 核 | 1.19 核 | 进程/cgroup 总量，不是主线程独占 CPU |
| 新增死锁 | 0 | 0 | 后窗口未发现死锁回归 |

剔除第一个恢复积压的业务样本后，后续 6 个五分钟窗口 succeeded 分别为 314、322、376、331、313、295，即 59.0–75.2 个完整 Plan/分钟。同期 partial 均 0，failed 分别 3、1、0、2、1、1，cancelled 分别 0、1、0、5、2、1。这些是按 completed_at 分窗的 Plan 状态，不是队列事件数。

首次 generation 且已 applied、有 collect_channel received 记录的阶段样本，后窗口每五分钟 P95 为 46.08–63.18 秒，平均 22.90–27.11 秒；之前有效性受限的末次样本 P95 50.46 秒、平均 23.25 秒。任务构成、样本选择及恢复过程均有影响，**没有可重复的增量端到端提速证据**。

08:32:29 的 Outbox pending/publishing/dead_letter 均为 0。08:31:39 Finalize 为 waiting 0、prioritized 0、active 1、delayed 1、failed 0。恢复后所查中心、控制器、Outbox 发布器和 Finalize 日志未见新的只读事务/语句超时错误；最终没有停用发布器或补偿循环。

08:36:27 表级参数仍为 0.005/1000，n_dead_tup=7990，autovacuum_count=0：尚未再次达到阈值，不能宣称后续自动清理已完成实测。表文件 2362531840 字节，原有索引合计 485367808 字节；没有新增索引文件。根盘可用约 185 GiB，使用率 82%；总磁盘变化包含全系统写入，不全部归因本次维护。

## 未完成项与下一步优先级

1. 优先解决本次诊断造成的 3 个 failed Plan 的受控补采。原有 Job.retry 已尝试，2 个被终态 fence 拒绝，1 个重试遇到真实路由预算耗尽。需要能保留失败审计、重新核验执行权及预算的正式恢复流程；没有把 Plan 强行改回 dispatched/succeeded 或提高 Rota 预算。
2. 两个中心中断但 Plan succeeded 的任务仍有 Run running/队列 failed 残留；另外两个 readonly 后 Plan failed 的 Run 也仍 running。业务结果和运行状态清理应分别核验。
3. 当前只确认 Outbox 的无效读取问题已改善。onboarding 查询、增量事务往返/CPU 仍是后续候选；300 秒降频未上线，新增表/索引无实施证据，均未扩大改动。
4. 自动清理阈值已持久生效，但下一次自动清理尚未发生，需继续留意 xmin、worker 可用性和死元组增长。

原始材料位于 `runtime/db-repair-20260923/`；配置快照在其 `private/` 下，未写入版本控制。最终 SQL、回退 SQL、计划状态和此报告均已写回原工作区。自动审批曾两次对末次只读复核超时，拆分后重试已完成；没有因此扩大生产操作范围。
