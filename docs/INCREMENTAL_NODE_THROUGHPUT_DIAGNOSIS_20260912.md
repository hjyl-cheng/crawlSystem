# 2026-09-12 增量节点吞吐下降：只读排查

本轮只读检查中心 PostgreSQL、BullMQ、容器日志和远程节点状态，未修改接单配置、代码、采集数据或重启服务。中心服务在检查期间自行崩溃并被 Docker 重启，该动作不是本轮操作。

## 运行范围与吞吐

- 每日 Plan 自 UTC 00:30（北京时间 08:30）开始运行。
- UTC 01:33 附近：成功 384、部分完成 1、失败 88、运行 2、已派发 37、未派发 40,225。所有未派发 Plan 已到 eligible_at，排除无可用任务。
- UTC 01:35:36 附近：成功 443、部分完成 1、失败 89、运行 5、已派发 22、未派发 40,179。SQL 分条读取，数值是动态快照。
- 中心本地增量 Worker 20 个均暂停接单。远程节点 `43.172.83.170`，登记名称“更新节点 01”，允许接单 20 个。
- 早期心跳快照显示 20 个在线，但后续实际容器检查发现两台反复重启；不能将心跳在线数视作持续可用采集容量。

| 北京时间区间 | 成功频道 | 当期失败 Plan | 按五分钟折算成功量/小时 |
| --- | ---: | ---: | ---: |
| 08:30–08:35 | 69 | 0 | 828 |
| 08:35–08:40 | 66 | 0 | 792 |
| 09:15–09:20 | 6 | 28 | 72 |
| 09:20–09:25 | 18 | 23 | 216 |
| 09:25–09:30 | 14 | 20 | 168 |

这不是全天稳定吞吐估计。下降与内部故障、重试密集发生的时间吻合。

## 确认的问题

### 1. 同一节点记录的排他行锁形成串行等待

现场 `pg_stat_activity` 捕获多个远程 gateway 会话等待：

```sql
SELECT * FROM remote_ingestion.nodes WHERE node_id=$1 FOR NO KEY UPDATE
```

一条等待链的最上游是 `remote-node-nats-heartbeats` 事务，正在更新 `worker_connections`。领取任务、网络授权/清理、节点心跳和停用路径多处持有同一节点记录的锁；`RemoteNodeStore.transaction` 的锁等待上限是 3 秒。

代码位置：`remoteNodes/store.js`、`workerActivationStore.js`、`channelRouteStore.js`、`channelExecutionStore.js`。观察证实节点级锁竞争，但尚未逐一追溯每个 STALE_LEASE 的最初触发事件，不能把所有租约失败都归因于某一次心跳等待。

派发器持续发布任务，近期单轮有效容量常被读为 12–13，代理就绪数约 81；未到期延迟重试已被排除出压力计数。队列后续有 20 active、8 waiting，实际远程 leased 17；BullMQ active 包括执行准备和恢复，不等同于正在请求 YouTube。

### 2. 旧 Task 没收尾时继续接下一任务，形成重复失败

UTC 01:35:36 查询今日 BullMQ failed 共 91 个：

| 最终错误 | 数量 |
| --- | ---: |
| BeginTask Lease conflict occurred after a Task became active | 74 |
| canceling statement due to lock timeout | 8 |
| STALE_LEASE | 6 |
| proxy control request failed | 1 |
| Rota Execution Route budget exhausted | 1 |
| Incremental YouTubeJS recent Phase has 0 active Item claim(s) | 1 |

该口径是 BullMQ 最终错误，与 Feature Plan 异步状态更新有差异，不能与 Plan 失败数量相加。

`rotaSlotAdapter.js` 在 quiesce/CompleteTask 之后才清空 activeTask；异常路径的 finally 只清理 activeJob。后续 BeginTask 冲突时，`#recoverBeginTaskLeaseConflict` 发现仍存在 activeTask/pendingCompletion，抛出上述保护错误。现场近 15 分钟日志中 `remote_incremental_failed/RotaSlotContractError` 出现 393 次（另有重复的 queue failed 日志，未重复计数）。应修复异常收尾和接单隔离，不可移除任务栅栏。

### 3. 一个清理异常导致中心进程退出

UTC **01:32:40.937**（北京时间 **09:32:40**）：

```text
error: canceling statement due to lock timeout
at workerActivationStore.js:95
at RemoteNodeStore.transaction (store.js:22)
at centerExecutionSupervisor.js:146
```

栈对应 `closeEntry` 调用 `activation.drain` 的节点行锁。未处理的拒绝导致进程退出；Docker 于 01:32:46 自动重启，01:32:51 服务重新监听。`RestartCount=1`，`OOMKilled=false`。自动重启后成功量增长、远程实际持有任务恢复到 17 个，不能视作根因已经修复。

### 4. 两个远程 Worker 遗留结果格式不合法，重启无法恢复

UTC 01:36 检查：

- incremental-12：累计重启 112 次，未 OOM。pending.json 自 00:59:18 未消化。
- incremental-17：累计重启 154 次，未 OOM。pending.json 自 00:35:20 未消化。
- 两个 pending.json 均为请求超时结果，`error.code` 为数字 **23**，message 为 `The operation was aborted due to timeout`。

发送端 `channelPlanExecutor.js` 使用 `error.code || error.name`，将 DOMException 的数字 code 原样保存。`protocol.js` 的 `encodeResult` 允许写入，而 `decodeResult` 强制失败结果的 code 为字符串。

真实执行器的 flush 在上传之前先 decode；因此重启后遇到同一份 pending.json 就抛 `INVALID_RESULT/400`。执行循环将 400 当作终止条件，进程退出，Docker 再次重启。日志入口只打印通用 `remote_node_incremental_failed`，掩盖了错误原因。

已在本地使用真实 `encodeResult/decodeResult` 和 `RemoteChannelPlanExecutor.run`，以内存 spool 复现：数字 23 被成功编码、读取时拒绝；执行器在上传次数为 0 的情况下退出，原 pending 仍保留。字符串 `TimeoutError` 对照可以通过 decode。未重放或更改线上结果。

## 网络与资源证据边界

- 今日已返回指令：open_channel 平均 9.8 秒，scan_uploads 3.1 秒，video_detail 3.3 秒；这是含通信的指令耗时，不能当作纯 YouTube 服务耗时。
- 节点内存约 2.98 GiB 已用、4.64 GiB 可用，swap 未使用；两次短 CPU 采样空闲约 49% 和 28%。不能据此推断任何时段都无资源瓶颈，但不存在本次两台 Worker 因 OOM 退出的证据。
- 中心 PostgreSQL 采样约 605% CPU，接入进程约 95%；有并发发布慢查询。资源压力可能放大锁等待，未量化各自贡献。
- 重启后 NATS 状态样本 pending/unacknowledged 为 0，RPC admission 未拒绝/超时；不能将本次故障简单归因于消息队列带宽或 API 等待。

## 修复优先级建议（本轮未执行）

1. 统一错误序列化，处理两份已有数字错误码的 pending，保留原始结果；日志记录安全的错误码和阶段。
2. 隔离 Worker 清理失败，防止未处理异常拖垮中心进程；未收尾的槽位停止接新任务并走恢复。
3. 缩小节点级排他锁范围，保留必要的接单容量与删除/停用一致性保护，避免不同 Worker 的常规操作互相串行阻塞。
4. 完成故障回归后恢复今日失败频道，验证持久化结果、任务归属和持续吞吐。
