# P2 中心执行器与阶段协议验收

日期：2026-09-20（UTC）。本记录接续 P2 身份/schema、准入和业务 fence 三批实现，补齐 W04、W05。P2 隔离验收已通过。生产全量远程接单仍未开启。

## 中心执行接线

`services/qybullmq/src/remoteNodes/fullCrawlCenter.js` 提供显式装配入口 `createFullCrawlCenter`，组合真实中心 supervisor、processor、执行所有权 store 和 runtime。未接入生产启动脚本。

- supervisor 按 workload 选择原始 `youtube-channel-crawl` 队列；每个已准入全量 slot 的并发为 1，使用独立全量 advisory lock。增量仍消费 `youtube-channel-incremental`，使用原来的处理器和恢复路径。
- 全量激活必须同时满足节点/实例身份、操作员启用意图、Redis、原 Rota assignment、独占监督锁和 transport 就绪。没有 transport 实现时不能构造可用 runtime。
- processor 复用 `ProxyBusinessRunPreparer`、`executeManagedWorkerAttempt`、候选 attempt 标记/结算、Migration Retry Intent、API gate/resumable 和容量延迟逻辑。没有缓存 fetch contract 的快照先读取已冻结 binding；新 binding 使用原有默认合约规则。
- `migration-channel-start` 和受控 Migration 准备留在中心；legacy、repair 和 about-only repair 分流到必须显式提供的本地兼容 processor。兼容 processor 拥有完整本地执行流程，API replay 也必须显式提供；装配时没有默默跳过任务的默认实现。
- runtime 使用原 `BrowserProfileStore` 建立真实 Rota attempt，再创建 full task 并等待节点实际 claim。业务 run、BullMQ attempt、dispatch generation、Rota worker/route 和节点 instance 持续校验。
- `runRemoteFullCrawl` 复用 P1 的共享全量生命周期；资格、内容类型、时间窗口、API eligibility 和内容写入由中心决定。业务写入使用与结果 applied 回执相同的事务。
- 排空保持执行中 BullMQ 任务的锁；没有容量走 delayed，不增加失败次数。未结算的旧执行或未退役网络绑定阻止重新启用，不调用增量 Plan 恢复代码。

## 阶段消息合约

公共 command 绑定 `task_id/generation/stage_id/sequence/execution_hash/input_hash/target_hash`。输入包含冻结频道、完整 fetch contract、locale、时间参考、内容数量/年龄限制和可选评论开关。

| 阶段 | 中心下发 | 节点回传 | 中心应用 |
| --- | --- | --- | --- |
| admission | 频道、必须请求 About | 原始频道/About snapshot 或结构化错误 | 原资格判断、admission/rejection/终态结算 |
| uploads | 原时间参考、limit、是否已有内容、频道国家、Rota 出口国家及国家复查状态 | uploads entries、scan、activity evidence，或国家复查交接 | 冻结 uploads/target hash 和候选列表 |
| details | 最多 20 个已预留目标、原 detail fence、中心查得的登录排除证据 | 连续目标前缀，reservation/video/start ID、观察时间、原始详情/预检/错误/API-needed 证据 | 按顺序确认 started、只扣一次业务次数、原详情校验/写入及 applied |
| close_fetch | 冻结 target hash | `network_stopped=true`、`active_requests=0` | 原 Close Fetch 和 publication handoff |

- 输入最多 64 KiB；未压缩结果批次最多 8 MiB；每块最多 512 KiB，最多 16 块；JSON 深度最多 32。
- 每块和整批分别校验 SHA-256、字节数、阶段身份与顺序。收到完整结果才发 `durable_received`；业务事务成功才有 `applied`。
- 同批重投幂等，冲突、跨目标、跨代次、错误实例及缺失必要证据会拒绝。已应用块的精确 ACK 可在业务终态后只读重放，不授予新的业务写入权限。
- 错误保留 name/code/message/status/required surface 和有界 cause 链；`api_required` 必须携带原始错误及部分详情，不能凭节点交接标签获得 API 权限。仅 v3 可使用此结果种类，中心仍使用原 fallback 策略及稳定 `['full', runId, videoId]` 请求 ID。
- 同一存活所有者恢复时跳过已应用的详情前缀，不重新消耗次数。API 同步结果应用后，未开始的剩余预留被取消并重新预留；已经 started 的不被当作未执行。
- 阶段等待、唤醒通知受同一个截止时间和取消信号约束；通知返回本身不是采集成功证据。

## 隔离验证

验收环境：既有专用 Docker 内部网络 `qy-fullcrawl-p2-preflight-20260920_default`，独立测试数据库和 Redis，Node 20.20.2；源码只读挂载，无宿主发布端口。没有变更生产数据库、队列、节点配置或部署。

本轮完整回归 **283 项通过、0 失败、0 跳过**（相同用例的后续重点复验不重复计数）：

| 测试组 | 通过数 | 日志 |
| --- | ---: | --- |
| 全量、协议、容量/清理、Rota 和 API fallback 单元回归 | 160 | `logs/unit.log` |
| 原全量/增量存储、恢复、准入、业务 fence 及新阶段协调器数据库集成 | 119 | `logs/integration.log` |
| 全量中心真实队列、两种增量 revision supervisor、接单控制 | 4 | `logs/supervisor.log` |

`git diff --check` 通过。额外重点复验覆盖国家字段必须为字符串，以及没有缓存 fetch contract 的快照根据原 binding 恢复合约并完成真实队列执行。

新增重点证据：

- `remoteFullCrawlMessages.test.js`：分块/体积/JSON、不可改写的身份和输入、About 必填、v1/v2/v3 API 边界、兼容路由和监督锁隔离。
- `remoteFullCrawlCoordinator.postgres.integration.test.js`：真实 candidate/binding/run/attempt 全阶段执行，普通视频内容写入、直播预告排除、业务与回执回滚、相同所有者详情断点恢复、拒绝 admission 后 ACK，以及原 v3 fallback 的稳定请求和剩余目标重发。
- `remoteFullCrawlCenter.postgres.integration.test.js`：真实 PostgreSQL、Redis、BullMQ Worker 和原 `RotaSlotAdapter`，实际 processor/runtime 接线、独占中心、队列隔离、长任务续锁、排空和重新启用、本地兼容分流、无容量延迟不耗尽失败预算。

运行入口为 `services/qybullmq` 的 `npm run test:remote-full-crawl`，必须配置专用 `REMOTE_NODE_TEST_DATABASE_URL` 和 `REMOTE_NODE_TEST_REDIS_PORT` 才能执行全部集成测试；不提供这些变量时的 skip 不能作为验收。当前环境完整回归脚本与日志位于 `runtime/fullcrawl-p2-preflight-20260920/`。

## 与 P3 的边界

本阶段使用隔离的节点传输 fixture 和 Rota 服务响应 fixture。数据库、业务存储、中心 processor、原 Rota adapter、BullMQ 队列和锁是真实实现。兼容分流测试验证交由本地 processor，并未再次运行每一种旧版抓取网络链路。

P3 负责提供真实节点 Collector、NATS/WSS/JetStream、journal/fsync、Rota 远程 route/session/profile 与停网证明；同时完成 API 异步等待/继续、国家复查、跨进程或跨代次恢复及故障注入。P2 的安全行为是保留未结算证据、排空受影响槽位，不宣称已完成这些恢复流程。

完成 P2 不代表远程 YouTube 实采或生产发布已通过；全量开关继续关闭，按原 P3～P6 门槛推进。
