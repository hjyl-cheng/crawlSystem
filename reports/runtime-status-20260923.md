# 程序运行状态巡检

检查时间：2026-09-23 03:25–03:28 UTC（北京时间 11:25–11:28）。

结论：基础服务可访问，但远程增量采集和事件发布链路停滞，不能判定为正常运行。本次仅执行只读巡检，未修改生产状态、重启服务或部署代码。

## 当前异常

| 项目 | 实测结果 |
| --- | --- |
| 远程增量完成量 | 最近 30 分钟 applied 为 0；最后一次 applied 为 09-22 19:09:12 UTC，距检查约 8 小时 18 分钟 |
| 增量积压 | 37 个 pending，最早创建于 09-22 19:07:41 UTC；两次采样数量不变 |
| 增量主节点 | 47 个在线、允许接单 47 个；ready 36、active 37，Worker 明细中 11 个 readyForTasks=false。汇总 unready 为 10，接口存在瞬时口径差异；不能把 active 当作实际采集进度 |
| 中心日志 | 最近 15 分钟的末尾 250 条日志中，248 条为 remote_center_queue_error；日志未提供底层错误详情 |
| 采集事件发布进程 | crawler-outbox-publisher 详细状态 exited，退出码 1；docker exec 返回 container is not running。docker ps 仍显示 Up 2 days，列表状态与详细状态不一致 |
| 发布进程退出记录 | FinishedAt 为 09-22 18:52:54 UTC；Docker State.Error 为挂载目录创建失败：no space left on device。这是保留错误，不能据此认定当前磁盘仍满 |
| 事件发布积压 | pending 498、publishing 1，总计 499；两次采样不变，pending 最后更新于 09-22 19:09:11 UTC |
| Feature 事件 | 432 条 waiting_gap / sequence_gap，含历史积压；最早记录为 08-26，最新记录为 09-22 17:48 UTC |
| 全量遗留任务 | 1 个 received，创建于 09-22 06:10:54 UTC；140 个结果批次仍为 received |
| Finalize 队列 | 采样时 active 1、waiting 16、prioritized 185；failed 10,275 为保留累计数，不能视为本次新增失败 |

## 仍可用的部分

- API 和 Dashboard `/health` 均返回 HTTP 200；数据库、Redis、MinIO 的健康检查通过。
- 全量节点 10/10 在线就绪、空闲；迁移 API 当前 active=null，最新批次 completed，当前无新迁移批次执行。
- 增量节点 02 的 8 个 Worker、query 备用节点的 20 个 Worker 在线，但接单开关关闭，属于待命状态。
- 最近 15 分钟有 1,563 条 Finalize completed 事件及 6 条 Content Enrich completed 事件，后处理仍有活动；这些事件不等同于最终业务发布成功数。
- Content Enrich 监控快照标为 stale，包含超过一天的排队年龄告警；不能将该旧快照计数直接作为实时积压。

## 资源与历史重启

- 根文件系统约 999GB，使用 81%，剩余约 196–197GB；inode 使用率 2%。
- 内存约 31.3GiB，可用约 8.4GiB；16 个逻辑 CPU，采样 load average 为 7.73 / 9.07 / 10.35。
- 采集 PostgreSQL 瞬时 CPU 约 510%（约 5.1 核），存在 DataFileRead 等待；采样未发现被锁阻塞的数据库连接。单次资源快照不足以归因停滞。
- 中心累计重启 8 次、控制器/API 各 6 次、业务发布进程 35 次；这些容器最后启动约为 09-22 18:55 UTC，不代表现在正在重启循环。

## 建议处理顺序

1. 核实采集事件发布容器的 Docker 状态矛盾及退出条件，制定恢复方案，并验证 outbox 积压下降。
2. 获取中心 remote_center_queue_error 的底层错误，恢复 37 个 pending 增量任务，验证 applied 时间继续推进。
3. 核对全量 received 遗留任务、Feature sequence_gap 和 Finalize 失败积压，分别处理，避免将累计历史问题误判为同一故障。

巡检原始聚合证据保存在 `runtime/status-check-20260923/system.json`、`database.json` 和 `database-followup.json`。本报告确认了状态与影响，尚未证明这些异常具有共同根因。
