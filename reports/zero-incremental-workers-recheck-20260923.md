# 三个异常增量 Worker 复查（2026-09-23）

复查结论：三个 Worker 均未恢复，仍停留在此前诊断的同一任务、同一 attempt 上。远端保持在线，旧网络绑定已退役，但中心执行和队列占用尚未释放。候选修复尚未部署，不能把当前状态视为修复后的运行结果。

采样时间为 11:41–11:45 UTC（北京时间 19:41–19:45）。本次仅进行生产只读检查，并保存本地证据；未重启、部署、重排任务、删除锁或清理远端文件。

| Worker | 最近半小时完成任务数 | 当前 attempt 开始时间（北京时间） | 截至北京时间 19:44 的持续时间 | 最近一次完成时间（北京时间） |
|---|---:|---|---|---|
| node01 / incremental-38 | 0 | 15:53:13 | 约 3 小时 51 分 | 15:47:29 |
| node02 / incremental-2 | 0 | 15:55:01 | 约 3 小时 49 分 | 15:51:09 |
| node02 / incremental-6 | 0 | 15:54:23 | 约 3 小时 50 分 | 15:51:40 |

完成量窗口为北京时间 **19:11:06.789 ≤ 时间 < 19:41:06.789**，按 `crawler.task_events.status='completed'`、`job_id` 去重统计任务数，不是视频数。同一窗口整个增量队列完成 1,958 个任务，说明并非整个增量系统停止。最近完成时间来自向前六小时的事件查询。

两轮中心采样分别在 11:41 和 11:44 UTC，三个任务均为 `pending`，关联 attempt 均为 `running`，`updated_at` 仍等于开始时间，`finished_at` 为空；计划仍为 `dispatched`。任务自身的 `node_id`、`worker_slot`、`lease_until` 和 `coordinator_until` 均为空，但其队列和监督器占用仍然存在。

| Worker | task_id | 当前 attempt_id |
|---|---|---|
| node01 / incremental-38 | b3b67223-b5a8-4996-bffb-79d3c60426a7 | channel-attempt:3796cdeb-94f1-4a0e-b7f5-89dd92621927 |
| node02 / incremental-2 | 99541aad-88bf-40ba-b043-6150cf7edab9 | channel-attempt:99f96dc5-7b60-4fd7-bd44-4f80884ba1f5 |
| node02 / incremental-6 | 0890cbd0-d89d-4b3f-ad1f-fc01ebee8189 | channel-attempt:4917b0aa-9fe6-4b91-957b-e07177bd06f5 |

三个 BullMQ 作业在两次采样中均处于 `active`，没有完成、失败或延迟记录；`processedOn` 和启动次数未变化。隔近三分钟后，锁依然有效且剩余期限回升，证明存在持续续期：

| Worker | 首次锁剩余期限 | 再次锁剩余期限 |
|---|---:|---:|
| node01 / incremental-38 | 25,883 ms | 28,485 ms |
| node02 / incremental-2 | 23,602 ms | 26,380 ms |
| node02 / incremental-6 | 24,086 ms | 26,742 ms |

监督器 advisory lock 仍由原 PostgreSQL 会话持有：node01 对应 PID 662947，node02 两个槽位对应 PID 662919。这些会话承担多个槽位的锁，不能按单个异常 Worker 的范围直接终止会话。

三个任务共四条历史网络绑定全部 `retired`，释放回执均为 `in_flight=0`；三个槽位未查到活跃绑定。最新交接原因仍为 `FINGERPRINT_PROXY_TRANSPORT`。远端 `network.json` 均为 `phase=closed`，旧租约和时间戳没有推进。这支持“旧网络已静默”，但不能据此认定中心旧执行已经退出。

Dashboard 仍将三个 Worker 显示为 connected、enabled、processing 和 readyForTasks，`executionPhase=preparing`、`awaitingRecovery=false`。三个远端容器均为 `running`，重启次数为 0，未配置容器健康检查；容器存活和心跳在线不代表任务有进展。容器启动时间、实例标识和镜像均未变化。

远端均不存在 `pending.json` 和 `whole-pending.json`；node02 / incremental-2 仍保留旧 `claim.json`。对结果日志进行了只读检查，没有调用会初始化或修复日志的接口：

| Worker | whole 日志目录数 | 空 journal.ndjson 数 | 总日志字节 / 记录 | 根目录 .stale 文件数 | .blocked 文件数 |
|---|---:|---:|---|---:|---:|
| node01 / incremental-38 | 35 | 35 | 0 / 0 | 66 | 0 |
| node02 / incremental-2 | 36 | 36 | 0 / 0 | 62 | 0 |
| node02 / incremental-6 | 24 | 24 | 0 / 0 | 52 | 0 |

上述日志读取无报错，未发现其中存在待提交的结果记录。历史 `.stale` 文件内容不在本次检查范围内，所有文件均保留。不能仅因日志缺少回执就把这些空日志判为待恢复结果。

当前线上中心镜像为 `qy-allpachong/qybullmq:pachongsys-94d1b12-intake-capacity`，Dashboard 为 `qy-allpachong/dashboard:pachongsys-ec21efd-intake-capacity`，均不是本次修复的候选镜像。远端镜像摘要为 `sha256:a1736b34c23108f2e26582d068ae992cb2b0bd98e563864806fb8c2f327019ea`。

证据支持此前的流程问题判断：网络交接后，业务执行长时间没有推进，但中心仍持有并续期队列锁，现有机制没有让执行退出或使槽位进入恢复状态。此次复查不能精确定位卡住的具体 await 或 SQL；采样未观察到持续数小时的数据库锁等待。

两项宽范围历史查询触发本次探针设置的 8 秒 statement timeout（57014）。改为每槽位最近 100 个任务的有界查询后成功，在这 300 条记录范围内仅发现上述三个未完成任务。这不是完整历史审计，探针超时也不能直接当作生产挂起的原因。目标任务主键查询、完成数统计、网络绑定和锁查询均成功。

下一步应按原方案完成旧执行的受控退出和占用释放，再部署观测模式，核对诊断状态，随后对单个槽位启用恢复执行并验证完成量。旧网络静默只是其中一个前提；直接删锁、改任务为完成或强行重排可能与仍存活的中心执行冲突。观测模式本身只记录判断，不会主动解除本次占用。

原始证据位于 `runtime/zero-worker-recheck-20260923/`：`center-snapshot.jsonl`、`confirmation.jsonl`、`remote-snapshot.jsonl`、`remote-inventory.jsonl`、`journal-snapshot.jsonl` 和 `center-containers.txt`，同目录保留只读探针脚本。

关联材料：[原诊断](zero-incremental-workers-diagnosis-20260923.md)、[修复交付记录](incremental-worker-recovery-fix-20260923.md)、[实施方案](../0923_1905.txt)。
