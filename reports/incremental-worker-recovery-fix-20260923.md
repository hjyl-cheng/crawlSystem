# 增量 Worker 无进展修复实施记录

对应方案：`0923_1905.txt`。本次交付源码、回归测试、候选镜像与发布覆盖文件；未重启、重试或修改三个生产任务，未执行生产发布。

## 实现

- `boundedPostgresRead.js`：统一覆盖连接获取、BEGIN、事务级服务器限时、SELECT、COMMIT 的客户端截止时间；取消/协议状态不确定时销毁连接；迟到借出归还；迟到应答不再驱动旧尝试；不会污染 PgBouncer 后续借用者的只读设置。
- `channelExecutionStore.js`：领取、国家交接检查、准入回查和网络绑定查询采用有界读取；stop/finish 核验监督所有权；finish 在任务行锁内复查尝试身份、传输已结束、所有绑定已退休且 in_flight=0。
- `channelRouteStore.js`：网络退休等待中的读库也响应取消，避免第二处同类挂起。
- `managedIncrementalRuntime.js`：保留直到最终结算的执行快照；以精确 attempt_id 取消；在 prepare/claim 后重新检查取消；取消期间准入若仍未返回，不另起协程猜测提交结果。清理前 stop 通过原任务行锁阻断迟到 claim/bind/grant；无本地 inner 也查询真实绑定。清理超时返回明确错误，Rota 保留原 finalization，同一未决写入不被重复发起。
- `executionProgress.js`、`incrementalCoordinator.js`：记录准入、领取、绑定、采集、结果接收/采用、停止、恢复、隔离与结束阶段；用单调时间计算年龄，UTC 展示；心跳/续锁/轮询不刷新进展。采集/回传/写入超过 15 分钟只有诊断告警，不能据此断言远端已卡死。
- `centerExecutionSupervisor.js`：独立 1 秒检查定时器，不等待共享 tick 的 SQL；active/processing 不再阻止无进展识别。仅在 enforce 且槽位白名单匹配时取消 admitting/awaiting_claim。只暂停新 Job 接入，不关闭旧任务的 heartbeat/回执/清理资格；原处理函数、Rota Completion 和网络清理均完成后才恢复接单。不会并行调用面向替代监督者的 recoverRemoteSlot。
- `deploymentAdmin.js`、`intakeSelection.js`、Dashboard：透传阶段、进展年龄、操作、取消原因与连接池数量；单列“进度超时 / 恢复中 / 需处理”，不把无进展的 active Worker 计入正常采集。

未修改远端 spool 或 heartbeat 契约：本批中心修复无需协议升级。已有远端恢复分支继续由整频道交接、应答丢失和进程中断回归覆盖；没有依据把生产的 closed 文件直接认定为错误或清空它。

## 开关与限制

| 配置 | 默认值 | 用途 |
|---|---|---|
| REMOTE_INCREMENTAL_PROGRESS_MODE | observe | 监督器只记录/告警；enforce 才触发额外阶段取消 |
| REMOTE_INCREMENTAL_PROGRESS_SLOTS | [] | JSON 字符串数组，每项为 node_id/incremental-N；enforce 必须非空，启动核验槽位存在 |
| REMOTE_INCREMENTAL_CLAIM_TIMEOUT_MS | 30000 | 原领取总期限 |
| REMOTE_INCREMENTAL_READ_TIMEOUT_MS | 5000 | 单次短读取全生命周期期限，借连接最多 2 秒且计入总期限 |
| REMOTE_INCREMENTAL_ADMISSION_TIMEOUT_MS | 30000 | 准入阶段告警/白名单取消阈值 |
| REMOTE_INCREMENTAL_STOP_TIMEOUT_MS | 45000 | 单次清理等待上限；到期隔离而非伪造完成 |

observe 不关闭有界读库、原领取期限或清理期限，它只关闭监督器新增的自动取消。切回 observe 不会中断已经开始的清理。数据库写入或网络停止事实仍不确定时保持 blocked，不能强制释放路线或删除 BullMQ 锁。

快照只保存在当前中心进程，重启后旧槽位先走原有持久事实恢复。新增阶段日志记录状态变化，可供现有日志告警使用；本批没有另建 Prometheus 服务。进程事件循环完全冻结时，同进程定时器不能自救，需要外部存活告警。

## 验证

- 原 `runtime/zero-worker-diagnosis-20260923/waitclaim-repro.mjs`：修改前 `signal_aborted=true, waitClaim_settled=false, REPRODUCED`；修改后 `waitClaim_settled=true, CLEAR`。脚本的假连接补齐了新的独占 client 接口，仍保留永不返回的实际 SELECT。
- 单元及 Dashboard 路由回归：第一轮 82 项通过；后续增加旧 attempt 迟到取消、非零网络活动、Rota 未结算不得接单、长采集仅告警等 4 项测试，均通过。
- 第一轮隔离集成：90 项通过，0 失败、0 跳过。数据库、Redis、TLS NATS 和 Go relay 均使用独立测试实例，没有连接生产数据。
- 最终隔离验收：92 项通过，0 失败、0 跳过。新增真实 BullMQ 看门狗取消/重试用例在 HTTP 和 NATS 整频道两条路径均通过；旧尝试 aborted、后续尝试 success，同一 Job 的 attemptsStarted=2，实际采集只发生一次。
- 构建后的中心镜像使用其自带 Node 运行核心测试：70 项通过，0 失败、0 跳过。
- `git diff --check` 通过；Dashboard 候选镜像的页面脚本语法通过。

回归入口（qybullmq 目录）：

```sh
npm run test:remote-progress
REMOTE_NODE_ROTA_TEST_BINARY=/绝对路径/node-forward npm run test:remote-progress:docker
```

第二条会创建并销毁临时 PostgreSQL/Redis/TLS NATS。Go relay 由 `services/rota/core/cmd/node-forward` 构建。验收入口设置 `REMOTE_INCREMENTAL_REQUIRE_INTEGRATION=true`，缺少 NATS、Redis、隔离数据库或 relay 时失败，不允许静默跳过。当前宿主 Node 26 测试运行器使用 `NODE_OPTIONS=--test-isolation=none`；镜像内生产 Node 的默认隔离测试正常通过。

## 构建与发布产物

中心：
- 基础镜像：`qy-allpachong/qybullmq:pachongsys-94d1b12-intake-capacity`，ID `sha256:f63891b1ff89bbc59e60088a12bd08cceaf5d9e158e7ab9cdc33909e9b24980d`。
- 候选：`qy-allpachong/remote-node-center:incremental-progress-20260923-8683058a5a59`。
- 摘要：`sha256:5afbcdbd16703eaae1611e96154216631c432c5e2819e287eebcb7923a990ff2`。
- 使用现有 `services/qybullmq/Dockerfile.remote-center` 构建。

Dashboard：
- 基础镜像：`qy-allpachong/dashboard:pachongsys-ec21efd-intake-capacity`，ID `sha256:fea9b643f5bccb65b7dad9b4d1155f6082b54a3582d0da05ed7af0df3b915396`。
- 候选：`qy-allpachong/dashboard:incremental-progress-20260923-8d5736e8480c`。
- 摘要：`sha256:f5be0357f10501c11c2afee16bcd9961afe619cdb143fb0f6fe6594b5760fdf1`。
- 使用 `services/dashboard/Dockerfile.incremental-progress`，只更新状态页面，保留已安装的节点管理功能。

发布覆盖文件：`deploy/compose.incremental-progress-center.yml`、`deploy/compose.incremental-progress-dashboard.yml`，均固定 digest；中心为 observe。源码清单及摘要保存在 `runtime/incremental-progress-fix-20260923/sources.json`。本地候选基于当前工作区构建，保留此前已有改动；没有将它们回退，也没有创建或提交 Git commit。

## 生产闭环尚需执行

1. 重新读取三者 task/attempt/queue/binding/spool，旧诊断快照不能当作当前状态。
2. 对照运行版本和候选源码清单，保存镜像及实际 Compose 覆盖顺序；先制定现有活跃任务的排空/可靠隔离清单，再更新中心。不能用无条件 restart 代替所有权交接。
3. 先 observe，核实实际等待阶段和依赖健康；本次修复证明了超时覆盖缺口，尚未证明三个生产任务全部卡在同一 SQL。
4. 仅挑选一个身份和网络条件均已核验的槽位启用 enforce，确认原尝试结算与合法后续产出，再扩大。
5. 回退时先关闭新的 enforce 触发，完成或隔离既有清理，再回退镜像；保存队列、DB 和 spool，不能直接清空旧状态。

以上属于生产发布与三个现存任务的处置阶段，本次尚未执行。不能将本地测试通过写成“三个生产 Worker 已恢复”。

最终日志、镜像内测试、构建日志和 `release-manifest.json` 已保存至 `runtime/incremental-progress-fix-20260923/`。构建源码清单与最终工作区逐文件摘要核对一致。

生产后续：2026-09-23 已完成发布与三个异常槽位恢复，详见 [生产发布验收](incremental-worker-progress-rollout-20260923.md)。上文“未部署”描述本文件原始交付时点，最终运行状态以新报告和 rollout manifest 为准。
