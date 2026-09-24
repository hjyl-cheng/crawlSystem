# 增量数据库读取压力快照

时间：2026-09-23 06:08:47–06:09:18 UTC（北京时间 14:08–14:09）。仅只读检查，未修改配置、终止查询或重启服务。

增量中心的实际连接为 `crawler-pgbouncer:6432/newcrawler_crawler`，后端为采集 PostgreSQL。数据库采样使用中心环境中的直连后端配置，避免诊断本身占用连接池业务额度。

## 判断

有明显读取与 I/O 等待压力。迁移恢复、发布扫描和增量业务共享数据库，不能把全库读负载全部归因于增量。现有证据不足以认定磁盘或数据库已达到容量极限。

30.326 秒三次采样：

- PostgreSQL blks_read 增加 738,614 个 8 KiB 块，约 190.3 MiB/s；同期 shared buffer 命中率约 75.0%。这些读取可能由操作系统缓存满足，不等于物理磁盘读取。
- 宿主机 vda 六个 5 秒采样窗口读取 81–262 MiB/s、约 9,053–13,394 次读/s；设备 util 62–66%，平均读等待 0.31–0.35 ms，CPU iowait 11.5–15.1%。这些是全宿主机指标，包含其他数据库及服务。
- 采集 PostgreSQL 的一次 CPU 快照为 293.87%，约 2.94 核；宿主机 16 核。
- 目标库连接数 44–45，实例 max_connections=100。三次连接状态采样均有 3 个 DataFileRead 等待，另有 2–3 个锁等待。
- 同期临时文件字节和 deadlocks 均未增加。

## 读取集中位置

以下为表及索引统计增量，含已计入的 TOAST 表读取，不是物理磁盘读取精确归因；统计更新存在滞后。

| 表 | 约 30 秒读取 MiB | 顺序扫描行数增量 |
| --- | ---: | ---: |
| crawler.migration_control_items | 1,688.5 | 3,141,319 |
| crawler.migration_system_retry_items | 1,017.3 | 934,792 |
| publication.outbox | 707.5 | 1,550,291 |
| crawler.crawler_outbox | 699.8 | 0 |
| crawler.channel_execution_attempts | 493.5 | 0 |
| crawler.content_candidates | 417.2 | 0 |
| remote_ingestion.tasks | 291.1 | 0 |
| crawler.contents | 175.7 | 0 |

前三次活动查询快照持续捕获同一条 `publication-auto-onboarding:backlog` 查询，已运行时间从 21.43 秒增长至 51.69 秒；主进程后两次等待 DataFileRead，两个并行 Worker 等待 MessageQueueSend。crawler_outbox 领取查询也出现 DataFileRead，单次已运行 16.82 秒。Finalize recovery scan 同样出现读取等待。

增量中心对 remote_ingestion.tasks 的 SELECT FOR UPDATE 与心跳 UPDATE 存在行锁竞争，快照最长等待约 2.58 秒；这是另一类延迟，不能直接当作纯读取瓶颈。

## 后续优先级与边界

具体实施顺序、查询改写、验证门槛与回退见 [读取压力修复方案](incremental-db-read-pressure-fix-plan-20260923.md)。

优先核查迁移控制/重试扫描、两类 outbox 查询及 publication-auto-onboarding 查询的执行计划、索引和执行频率，再评估 Finalize 恢复速率与增量任务行锁持有时间。当前不宜仅凭这些快照认定增加数据库或 Worker 数量能解决问题。

未启用 pg_stat_statements，track_io_timing=off，因此本次不能精确分摊各 SQL 的累计读取耗时，也未执行 EXPLAIN ANALYZE 给生产增加额外扫描。脚本、三次数据库快照和六次 iostat 快照暂存于 `/tmp/incremental-db-pressure-20260923/`。
