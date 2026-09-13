# 增量执行控制修复与上线记录（2026-09-12）

背景与只读证据见 [吞吐诊断](INCREMENTAL_NODE_THROUGHPUT_DIAGNOSIS_20260912.md)。本轮获得修复授权后实施；不代表前一份只读报告期间做过线上变更。

## 代码边界

中心本地与远程增量仍使用同一套 Clock/Plan、IncrementalChannelRunner、视频采集、API 续跑和 Rota 策略。远程节点只承担该 Plan 内的 YouTubeJS 网络请求。没有新增第二套采集策略，也没有改每日 Plan 生成规则、API 上限或重置执行预算。

本轮修复：

- `remoteNodes/protocol.js`、`channelWire.js`：兼容 DOMException 数字错误码，保留 native_code。旧结果使用原始压缩内容和原始哈希重放，过期结果归档，不删除现场或冒充采集成功。
- `workerActivationStore` 等节点状态读取改用共享行锁，独立 Worker 可以并行读取；任务领取容量、节点管理修改仍保留独占保护。
- 共用 `rotaSlotAdapter.js`：任务收尾失败时阻止该槽位接下一个任务，续租时继续完成原任务收尾；复用同一观察 ID、时间及完成回执。业务栅栏保持有效。
- `centerExecutionSupervisor.js`：单槽位清理失败记录阶段和错误，继续关闭其余资源，不以未捕获拒绝拖垮整个中心进程。
- 中心连接池：新增可选 `REMOTE_NODE_TRANSACTION_DATABASE_URL`。普通事务通过现有 PgBouncer 复用连接，执行锁与 LISTEN 继续使用直连专用会话。`RemoteNodeStore.transaction` 每次事务设置发布 writer_version，不依赖连接池保留会话参数。

连接池补充依据：上线运行验证时直接连接返回 53300。生产 PostgreSQL 的 max_connections=100；20 条远程监督执行锁、21 条通知监听，以及 gateway/心跳/结果池和现有业务连接池共同占满连接。没有提高数据库上限或重启 PostgreSQL。

## 回归验证

全部在隔离测试 PostgreSQL、Redis、PgBouncer 与 node-forward 进程运行，没有拿生产频道作为故障注入对象。

| 验证 | 结果 |
| --- | --- |
| 共用 Rota 生命周期 | 36 项通过 |
| 旧数字超时结果重放、原哈希与过期证据保留 | 4 项通过 |
| 槽位清理异常隔离 | 2 项通过 |
| 20 个并行心跳、节点停用与领取容量保护 | 通过；旧实现只能 1 个进入验证，新实现 20 个 |
| 真实网络授权与回收集成 | 23 项通过，0 跳过 |
| 真实 BullMQ/Redis 恢复与 handoff | 18 项通过，0 跳过 |
| 上述 handoff 经真实 PgBouncer 再验证 | 18 项通过，0 跳过，约 93 秒 |
| 40 个并发事务、最多 4 个数据库后端、发布写入保护和失败回滚 | 通过 |
| 真实中心入口通过 PgBouncer 启停、配置校验 | 通过 |

Handoff 验证包括 pending/active/已提交阶段 SIGKILL、API 等待释放槽位并续跑、国家出口切换及正常停机收尾。测试等待时间为实际 BullMQ stalled 检查两轮预留了时间；未通过缩短或跳过故障恢复验证来通过测试。

## 上线范围

- 中心接入服务：`qy-allpachong/remote-node-center:incremental-control-pool-20260912`。
- 中心本地 20 个增量 Worker：`qy-allpachong/qybullmq:incremental-control-20260912`，仍保持允许接单 0。
- 更新节点 01 的 20 个增量 Worker：私有仓库固定 digest `sha256:416f6327ca742a4ce542419d71292a8f3d53e37ebf38db32cb6dd5047839429b`，允许接单仍为 20。
- Dashboard 沿用 `migration-timeout-60s-20260911` 镜像，只更新后续节点部署镜像配置及对应部署登记。
- 本轮未重启迁移/Full Crawl Worker、调度器、Rota、NATS 或数据库。

两次中心切换前均暂停增量队列接新任务，等 active=0 再切换，切换后恢复。最终队列已恢复。

初始化诊断时发现 incremental-12、17 的数字错误码死循环；部署前又确认 incremental-5 同类故障。三个旧结果对应的任务均已由后续代次完成，原结果已归档为 stale。节点 spool 保留了证据，三台进程均恢复，不再循环退出。

运行文件及部署前快照在私有 runtime/remote-center-production/incremental-control-20260912 中，不应提交凭据或完整运行快照。保留旧容器与镜像作回滚参考，不做 Docker 全局清理。

## 生产观察

- UTC 02:19:58：远程连接/允许/启用/可接单均 20；19 个有效 leased 任务。中心本地连接 20、允许 0。
- UTC 02:20–02:25（北京时间 10:20–10:25）：成功 118、部分完成 0、失败 0，折算 1,416 个/小时。这是短时窗口，不代表全天稳定能力。
- 诊断低谷 UTC 01:15–01:30 每 5 分钟成功 6、18、14；恢复前后频道及任务掩码不同，不能将倍率解释为同一工作负载的基准测试。
- 恢复核查的只读查询曾遇到数据库容器 /dev/shm 64 MiB 接近占满。核查改为按 run_id 限定已有索引，并仅对核查事务关闭并行扫描；没有改生产数据库配置。该资源余量仍需在后续容量规划关注。

## 历史失败恢复

今日队列原有 91 条失败：88 条内部控制错误（74 个 BeginTask 冲突、8 个锁超时、6 个 STALE_LEASE），另 3 条为路由预算、代理控制和视频 Item claim 错误。88 条中 2 个 Plan 已成功，86 个待续跑。

恢复前逐项校验原队列状态/载荷/attempt 计数、冻结 Plan 哈希、dispatch outbox、业务运行、任务代次、已结束执行及已退役网络绑定，并查询 Rota 原业务预算。使用原 Job 重试，不重置 attemptsMade/attemptsStarted，不修改采集检查点、不提高 Rota 预算、不重新生成 Plan。已成功频道由现有 terminal 判断直接完成队列确认。

UTC 02:29:42 核验：88 个 Job 均已重试提交，Plan 成功 21、运行 2、已派发 65（成功数包括原来已成功的 2 个）；队列 active=20、waiting=52、delayed=0，未暂停。原有剩余 3 个非本次内部控制错误没有重置。历史失败记录保留在审计事件和上线快照，不能用队列失败数下降代替采集成功数。

新版本观察到 1 次 55P03 锁超时，原任务在第二次执行后 completed，未导致中心重启或槽位停止工作。远程 20 个容器均 healthy、重启 0、OOM 0。

抽查已部署中心接入与本地增量容器中的 rotaSlotAdapter.js、incrementalChannelRunner.js、incrementalYoutubeJsVideo.js、youtubeJs.js，四个文件 SHA-256 分别与本地工作区完全一致，验证两端没有分叉出采集实现。隔离测试容器已停止。
