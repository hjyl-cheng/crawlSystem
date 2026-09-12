# 全量与增量共用视频执行恢复机制

## 问题与结果

增量视频检查点原来只记录单条视频的领取令牌和五分钟租约。远程执行取消后，带取消信号的事务包装器可能拒绝清理事务；下一次执行看到尚未过期的旧领取记录，会在 Worker 内循环等待。已确认的样本在继续请求前等待约 289–290 秒。

现在将 Full Crawl / 迁移已有的执行归属判断、接管及未完成视频恢复提取为 `services/qybullmq/src/videoExecutionRecovery.js`。中心本地增量和远程增量都通过同一个 `incrementalYoutubeJsVideo.js` 使用它。合法的新执行在领取事务中恢复未完成项，无须等待旧视频租约到期。

## 共用边界

- `contentDetailExecutionFence.js` 继续校验全量的候选频道、迁移恢复代次、任务及频道归属，然后调用公共模块。`fullCrawlYoutubeJsStore.js` 原来恢复 `running` 详情的 SQL 移入公共模块，仍在接管事务内执行。
- `incrementalVideoExecution.js` 负责把既有 Plan、频道、run、执行记录和检查点 cycle 映射到同一套执行归属。执行归属继续使用 `channel_runs.detail_active_*`，没有增加表或字段。
- 公共模块在同一事务中变更执行归属并恢复未完成项。增量仅恢复当前 cycle、仍处于 fetching 批次中的 claimed 项，保留 captured、settled_error、冻结的目标清单以及已完成观察结果。
- 增量视频后续事务使用公共归属判断拒绝旧执行写入，单条视频原有 claim_token 仍防止迟到结果落库。
- Runner 和最外层的终止失败记录器都跳过旧视频执行失去归属、等待恢复这两类控制信号，避免旧任务退出后为新执行写入失败 Observation。
- Clock 决定采集域的规则、增量锚点和采样、全量范围、YouTubeJS 客户端及共享 API 兜底规则沿用原实现。

## 崩溃与接管依据

不能仅凭旧执行的 PostgreSQL `status` 决定是否接管：本地进程被直接杀死时，它可能永远来不及写入 `finished_at`。

现有 Rota 表已经有 `proxy_control_tasks_one_active_business_run` 唯一索引，同一 workload / business run 只能有一个 active task。正常完成需完成请求收尾；租约失效则由 Rota 的失效任务处理结束旧 Task。`browserProfileStore.beginAttempt` 在 Rota 成功准入后记录 task_id 和递增的 attempt_number。

增量只允许同一 workload / business run 中经 Rota 准入的更高编号执行接管这种未收尾记录，同时继续检查 Plan、任务 ID、派发代次和网络身份失效标记。仅有一个更大的任意编号、其他 workload、旧 API 续跑或旧执行记录均不能授权接管。不改写旧尝试的历史状态来伪造收尾。

如果没有接管依据，或者同一执行被重复进入且仍持有视频领取，抛出 `VIDEO_EXECUTION_RECOVERY_PENDING`。公共队列包装器将任务放入 delayed，释放 Worker，且不增加 BullMQ 的失败重试计数。该信号不会根据早先的网络诊断触发切换代理，也不会被当成业务永久失败。Rota 的原有网络预算规则保留。

API 续跑仍由原有持久化请求校验控制。本地无网络上下文的续跑只可使用最新且已结束的执行；远程继续经过既有业务与网络退役校验。后续新执行接管后，旧 API 续跑也失去写入资格。

## 验证

验证使用本任务独立的 PostgreSQL、Redis 和本机测试转发程序，没有使用线上数据库或生产节点。

| 测试 | 覆盖 |
| --- | --- |
| 七个相关单元测试文件，72 项 | 全量 Store/工厂/范围、增量视频与 Runner、远程事务上下文、托管执行失败分类 |
| 增量终止失败回调回归 | 旧执行及等待恢复即使触发最后一次队列失败回调，也不写入失败 Observation；普通频道失败继续按原规则记录 |
| `videoExecutionRecovery.postgres.integration.test.js`，8 项 | 五分钟旧领取立即恢复；取消导致清理失败；进程未收尾；拒绝其他 workload；旧写入被拒绝；同一执行重复进入；接管事务回滚；API 续跑归属 |
| `videoExecutionDeferral.redis.integration.test.js`，1 项 | 真实 BullMQ、单 Worker：先延后等待频道，完成后续频道，再恢复；失败重试数不增加 |
| `remoteExecutionHandoff.postgres.integration.test.js`，18 项（含外层测试） | 真实 PostgreSQL/Redis、远程进程及本机转发；SIGKILL 恢复、API 释放 Worker、持久化结果续跑、剩余视频重新进入托管执行、国家出口交接 |
| 全量 Store、增量视频、远程业务栅栏 PostgreSQL 回归，27 项 | 全量与增量检查点兼容、API 续跑、业务写入归属与完成竞态 |

远程集成测试使用受控 YouTube 返回值及 Rota 控制接口夹具；不是线上 YouTube 吞吐测试。恢复测试确实执行 PostgreSQL 检查点和视频执行器，在首次恢复请求处用测试异常停止，以验证只请求未完成视频。

整体吞吐仍受中心数据库、CPU 和其他采集环节影响，本修复不代表已消除所有性能瓶颈。


## 部署与验证

`deploy/compose.shared-video-recovery.yml` 固定全量和本地增量的更新镜像，合并 Compose 配置时应放在现有 Worker 覆盖文件之后。中心接入服务使用 `QY_REMOTE_CENTER_IMAGE` 指定对应的更新镜像。

更新前暂停新增任务领取，等待正在采集的任务完成，再更换相关服务。新进程取得网络名额并连接队列后恢复领取；保留原容器及配置用于回滚。回滚也必须先收尾正在执行的任务。本次变更不需要数据库结构迁移。

远程节点负责现有 YouTubeJS 请求，共享恢复、检查点与 Plan 执行位于中心接入服务。更新共享恢复逻辑应更新中心接入服务；后续新增的远程采集节点会经过同一个恢复模块。

精确提交快照通过 77 项相关单元测试，中心接入、本地增量和全量的实际 Node 20 更新镜像也分别通过这 77 项测试，公共模块文件哈希一致。线上抽样检查了视频 Plan 完成状态、全量视频所选数量与入库及排除记录的一致性。生产未通过强杀正在采集的进程来注入故障，崩溃恢复由上述隔离集成测试覆盖。
