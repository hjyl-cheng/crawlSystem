# 采集 PostgreSQL shared_buffers 调整记录

2026-09-20。状态：**已将 newcrawler_crawler 所在 PostgreSQL 的 shared_buffers 从 128MB 调整为 1GB，重启生效并完成增量恢复检查。** 最终恢复采样：2026-09-20T09:45:21.811Z。

机器证据：[发布报告](../reports/crawler-postgres-shared-buffers-release-20260920.json)。调整依据见 [数据库优化方案](INCREMENTAL_DATABASE_OPTIMIZATION_PLAN_20260920.md)与 [共享主机内存预算](../reports/crawler-postgres-memory-budget-20260920.json)。

## 实施与持久化

- 使用 PostgreSQL `ALTER SYSTEM SET shared_buffers='1GB'`，设置写入原数据卷的 `postgresql.auto.conf`；当前启动命令没有 shared_buffers 覆盖值。原容器与数据卷均保留，容器重建继续使用该数据卷时配置仍有效。
- 中心与调度器自然排空、正常退出（退出码 0）；PgBouncer 暂停并等待在途事务结束。确认 PostgreSQL 其他客户端连接为 0 后正常停止、启动数据库，无强杀。
- 连接池暂停约 3.04 秒，恢复后启动原中心和调度器。该时长不是整个维护窗口：中心排空与逐槽重新激活另占时间，期间增量吞吐下降。
- `shared_buffers=131072 × 8 KiB=1 GiB`，`pending_restart=false`，来源为持久卷内 `postgresql.auto.conf`。auto.conf 中其他配置未变；`work_mem=4MB`、`max_connections=100` 保持原值。Rota TimescaleDB 未变更。

## 恢复验证

- PostgreSQL、PgBouncer、中心 healthy；调度器运行正常。各容器身份未变，无新增自动重启。
- 47+15=62 个增量 Worker 全部 connected/enabled/accepting，连续三次 20 秒采样均有任务成功完成。
- 初次激活前的历史执行核对存在串行读盘等待，约 09:35:38 才首次观察到槽位恢复。曾准备按严格条件取消只读旧状态扫描，但条件检查时扫描已自然结束、已有槽位激活，实际取消数量为 0；没有取消业务查询。该启动扫描列为后续 SQL 优化项，本轮未改业务源码。
- 原 selected_slots、revision、updated_at 逐项完全一致；NATS 健康检查返回 200。
- 全量节点仍在线、executionAvailable=true、allowedCount=0；本轮没有导入频道或开始全量灰度。

本次验证的是配置生效与业务恢复，尚未证明性能提速。数据库缓存重建、历史执行核对和激活期间不能作为稳定性能窗口；建议恢复后观察 1–2 小时并覆盖代表性高峰，再比较有效完成量、阶段耗时和内存压力。

## 回退

原始配置与容器快照保存在 `runtime/postgres-shared-buffers-20260920/private/`（0700 目录、0600 文件，不入 Git）。本轮之前 shared_buffers 来源于 postgresql.conf 的 128MB。需要回退时，按同样的自然排空和连接池暂停流程执行 `ALTER SYSTEM RESET shared_buffers` 并重启，再验证 128MB 生效、连接池恢复和 62 槽恢复；也可依据受限备份恢复原 auto.conf。不要删除数据卷、任务记录或节点 spool。
