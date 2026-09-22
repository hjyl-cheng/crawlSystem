# 全量采集 Worker 故障定位（2026-09-22 UTC）

本轮为只读线上排查及本地隔离复现，没有更改生产代码、暂停策略、任务或暂存数据。`full-crawl-1` 保持暂停。

远程最后一次取证时间：2026-09-22 02:42:17 UTC。

## 状态与影响

中心重复采样：已部署 10 个、允许接单 9 个、就绪 5 个。`2、4、6、7、8` 就绪；`3、5、9、10` 未就绪。就绪代表中心准入条件满足，不保证每个 Worker 当时都有网络采集任务。

远程复查：`3` 重启数由 834 增至 837，`10` 由 18 增至 21；两者退出码 1、无 OOM。`5、9` 持续运行、重启数 0、暂存目录为空。`1` 保持 exited、restart=no，其 820 次重启是暂停前历史值。

82 个增量 Worker 在中心采样时全部就绪。全量数据库记录显示 `6` 的一个任务已 applied，最近阶段应用时间 02:34:11 UTC；`10` 本轮任务 failed，last_error=WORKER_CONNECTION_STALE。不能将所有中心 processing 状态解释为远端正在采集。

## 原因一：关闭会话恢复丢失连接身份，形成永久重启循环

现场 `3、10` 的 network.json 均为 `phase=closed`，lease 只有 task_id 与 generation；claim.json 则保留原始 connection。对应任务在中心都已 failed，但节点没有走到清理 claim 的步骤。

真实调用链：

1. `channelNetworkSession.js` 创建网络记录时只保存 task_id/generation，finishCleanup 将该 lease 写入 closed 记录。
2. 恢复首先调用 `pollCommands(state.lease)`；`fullCrawlWorker.js` 将 lease.connection 转给 fullCrawlPoll，但该值缺失。
3. 中心 `workerActivationStore.identity()` 返回 `INVALID_WORKER_CONNECTION`，HTTP 语义状态为 400。
4. `fullCrawlExecutor.run()` 遇 400 直接返回，外层 `nodeIncrementalRuntime` 将其概括为 `NODE_EXECUTOR_STOPPED`。原始错误未输出到进程日志。
5. network.json 仍在，下次启动重复同一失败，尚未运行后续 claim 清理。

隔离复现使用现场元数据、真实 Worker 工厂、真实恢复流程和真实中心身份校验；数据库边界用失效连接响应替代，不访问业务数据库。`3、10` 均复现 INVALID_WORKER_CONNECTION → executorReturned=true → networkRetained=true。只在隔离副本的网络 lease 中加入原 connection 后，两者均返回 closed 并移除恢复记录。

这确认的是持续重启机制。最初导致会话中断的全部网络/代理原因尚不能从现有泛化日志完整还原；没有把 WORKER_CONNECTION_STALE 当作最初触发原因。

## 原因二：中心监督锁连接池少了一个连接

`runRemoteNodeCenter.mjs` 为全量监督锁创建 max=5 的 pg.Pool。`SupervisionGuards` 构造函数随后执行 `pool.options.max=groups`，将其改为默认 4。

启动路径还会从同一池中长期占用一个全局兼容执行器锁连接，因此 4 个监督锁分组需要 4+1=5 个连接，实际最多只有 4 个。

真实哈希分组：

| 分组 | Worker |
| --- | --- |
| 0 | 2、6 |
| 1 | 1、5、9 |
| 2 | 4、8 |
| 3 | 3、7、10 |

生产 PostgreSQL 只读查询确认：一个全局锁会话（classid 781138015），另三个全量监督锁会话（classid 781138012），对应分组 0、2、3。`5、9` 所在分组 1 无监督锁；`1` 按要求暂停，不参与启动。

本地使用真实 pg.Pool 和真实 SupervisionGuards，仅替换底层数据库客户端的隔离复现：原构造路径 max=4，获取第 5 个长期连接时报 `Error: timeout exceeded when trying to connect`；仅将隔离池上限保留为 5 后，5 个连接全部可获取。这与现场固定槽位启动失败及泛化的 Error 日志一致。

## 可重复验证

诊断文件位于忽略的 `runtime/fullcrawl-worker-diagnosis-20260922/`，保留用于复查，不进入应用镜像。

- 线上状态断言：`docker exec -i qy-newcrawler-fresh-dashboard-1 node --input-type=module < runtime/fullcrawl-worker-diagnosis-20260922/probe.mjs`。已运行，退出 1，失败槽位 3、5、9、10。
- 恢复链复现：`node runtime/fullcrawl-worker-diagnosis-20260922/repro-recovery.mjs`。原记录两例 FAIL；仅改隔离输入的对照两例正常清理。
- 连接池复现：`node runtime/fullcrawl-worker-diagnosis-20260922/repro-pool-isolated.mjs`。原构造路径 FAIL；只改隔离池上限的对照 PASS。脚本总退出 1，表示原故障仍存在。
- 现场证据：status-2.json、remote-2.json、db-3.json；复现输出：repro-recovery.log、repro-pool-isolated.log。

补充验证限制：尝试在独立进程建立真实数据库连接并执行 SELECT 1 的审批两次超时，命令均未执行；因此连接池超时使用真实 pg.Pool 加内存客户端隔离复现，生产会话数量来自此前成功执行的只读 SQL。该替代验证已完成，不需要额外权限才能给出上述诊断。

## 修复方向与边界

1. 监督锁池应保留调用方为全局锁预留的连接容量，或将全局锁放在独立池中。回归测试必须覆盖实际发布装配的“全局锁 + 四组监督锁”场景。
2. 全量网络会话应持久化并传递原始连接身份；同时兼容已有缺少 connection 的历史记录，从匹配的持久化 claim 恢复身份，并维持 task_id/generation 校验。不要通过删除暂存目录或绕过中心身份校验解决。
3. 保留原始 executor 错误码和启动失败阶段，使后续日志能区分连接池超时、租约错误与协议校验错误。
4. 修复后通过不可变镜像发布并复查 9 个应启用 Worker；`full-crawl-1` 必须继续暂停。当前线上故障尚未修复。
