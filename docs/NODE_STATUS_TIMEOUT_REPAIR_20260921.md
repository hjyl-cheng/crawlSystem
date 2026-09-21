# 节点接任务状态超时修复：2026-09-21

服务器节点页面显示“接任务状态暂不可用”，对应 Dashboard 到中心的状态请求失败。
现场中心容器为 `unhealthy`，本机健康检查请求 4 秒超时，Dashboard 状态客户端也复现失败。

## 原因和证据

中心业务连接池最多 12 个连接；现场 12 个连接均被任务失败收尾的 UPDATE 占用，多个查询持续一分钟以上。
该语句同时以 `task_id`、执行批次和 pending/leased 状态筛选。
PostgreSQL 选择了 `remote_tasks_live_slot` 部分索引，但没有 `Index Cond`，只能扫描并过滤历史索引条目。
当时 tasks 约 42 万行、约 30 万死元组，活跃索引统计条目数为 0；小的估计成本掩盖了保留的旧索引条目。

限时只读对比中，同一不存在的任务 ID 按主键查询约 1 毫秒，原条件查询超过 2 秒被取消。
NATS transport-health 返回 200，中心内存约 142 MiB / 1.5 GiB。
因此状态请求超时的直接原因是共享业务连接池被慢收尾请求耗尽。

## 修复

`RemoteChannelExecutionStore.stop()` 改为在有超时限制的事务内按主键锁定任务，
核对执行批次及 pending/leased 状态，再按主键更新。
行锁维持从校验到更新的批次所有权；旧批次不能关闭已被新批次接管的任务。
生产库 EXPLAIN 验证锁定和更新均使用 `tasks_pkey` 的任务 ID 条件。

从原生产镜像仅覆盖 `channelExecutionStore.js`，发布为
`qy-allpachong/remote-node-center:task-stop-pkey-20260921`。
原中心完成排空后正常退出，退出码 0；针对超时旧收尾 SQL 的条件取消命令命中 0 条，实际未取消任何语句。
全量总槽位继续为 6，配置已持久化到当前正式 Compose 文件。

## 验证与边界

隔离 PostgreSQL 回归覆盖任务状态、并发新旧批次隔离，以及长快照保留旧索引条目时的查询计划。
旧代码只有稀疏索引用例失败，修复后三个用例全部通过。临时数据库和测试网络已清理。
线上以 Dashboard 原客户端复测，并按页面并行请求方式检查本地中心、全量节点和两个增量节点。
最终结果及连续样本保存在 `reports/node-status-timeout-repair-20260921.json` 和
`runtime/node-status-repair-20260921/recovery-progress.jsonl`。

全部槽位恢复后连续三轮查询成功：全量 3/3、增量 62/62 就绪；最终两轮增量 62 个均正在处理任务。
最后三轮全量状态查询约需 4–8 秒；这次修复处理了连接池被耗尽的问题，没有对整个状态聚合接口作性能改造。
观察到的长事务和数据维护需求也没有通过结束未知事务、删除历史任务或全表重建来处理。

原容器保留为 `qy-remote-node-center-before-task-stop-pkey-20260921`，自动重启已禁用。
私有回滚配置位于 `runtime/node-status-repair-20260921/private/`。
如需回滚，先排空新中心，再恢复原容器和原 Compose 镜像，避免两个中心同时运行。
