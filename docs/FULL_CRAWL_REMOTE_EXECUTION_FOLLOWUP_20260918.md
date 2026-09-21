# 全量抓取远程执行后续执行清单

最新生产状态（2026-09-20 08:36 UTC）：P6 全量 Worker 执行准备已完成。中心 execution 已开启，兼容槽启动成功，Rota 就绪容量 125；全量节点 executionAvailable=true，接单保持 0。原 62 个增量槽位全部恢复并通过连续采样，接单配置完整保留。尚未开始真实频道灰度，P6 整体验收未完成。详见 [P6 执行准备](FULL_CRAWL_REMOTE_P6_EXECUTION_READY_20260920.md)及 `reports/fullcrawl-p6-execution-ready-20260920.json`。下文较早状态保留为阶段历史。

更新时间：2026-09-20。状态：P0～P5 已完成隔离验收，下一阶段为 P6。最新发布入口、固定镜像、数据对照和回退证据见 [P5 发布验收](FULL_CRAWL_REMOTE_P5_RELEASE_20260920.md)。P6 尚未执行，生产全量远程接单仍关闭。

本文件是恢复工作时的执行入口。完整背景、兼容矩阵和生产只读证据见[实施计划](FULL_CRAWL_REMOTE_EXECUTION_PLAN_20260918.md)和[阶段交接](FULL_CRAWL_REMOTE_EXECUTION_HANDOFF_20260918.md)。

## 当前基线和边界

- Git 基线：`bee141e41b609a583a2b86b34967fce1af404a80`。
- P1 已增加 `FullCrawlCollector` seam 和 `LocalFullCrawlCollector` Adapter；生产全量目前仍采用原本地执行路径。
- 增量远程链路、任务进度、接单配置、spool、检查点和未确认结果必须保持不变。
- 全量远程接单保持关闭，直到 P5 完成并明确进入 P6。
- 未经阶段门槛核验，不暂停生产接单、不调整节点配置、不迁移生产数据库、不部署全量镜像、不新增生产 Worker。

## P2：全量身份、中心协议和业务校验（W03～W06，已完成）

目标：让中心可以识别、授权和校验全量执行，但仍只在隔离环境运行。

实施顺序：

1. 冻结全量标识：`role=fullcrawl`、`mode=full_crawl_collect`、`capability=youtube.full-crawl.v1`、独立 slot/runtime revision；明确与 Rota `channel` role 的区别。
2. 扩展注册、心跳、就绪、启用和撤销校验；未知 capability、错误 workload、错误 node/slot/instance 必须拒绝。
3. 定义全量 task/execution/stage/detail reservation/result batch/result part 的 schema 草案；先使用隔离 PostgreSQL 做兼容性和幂等验证。
4. 定义 admission、uploads、details、close_fetch 的指令/结果版本、大小上限、hash、generation 和错误/交接状态。
5. 扩展中心 supervisor/processor，按 workload 选择全量 Adapter；增量仍使用现有 processor、队列和 fence。
6. 把 candidate/run/dispatch/attempt fence、详情批次 reservation、业务写入和 `applied` 标记纳入同一事务语义。

P2 交付物：schema/协议版本、中心 processor、全量身份校验、拒绝越权/旧 generation/重复实例的测试报告。

P2 通过条件：隔离环境可正确领取和拒绝全量任务；旧增量节点和增量协议回归通过；没有生产 schema 或部署变更。

## P3：远程采集、回传、API 接续和恢复（W07～W11，已完成隔离验收）

目标：在隔离真实依赖中闭环一个完整频道。

实施顺序：

1. 实现 Remote Collector Adapter，复用 NATS/WSS、JetStream、relay、journal 和专用 full result subject/stream。
2. 完成 admission：About 证据回传、中心资格判断、candidateSettled 和迁移进度。
3. 完成 uploads：冻结目标集、uploads hash/target hash、国家复查和停止网络回执。
4. 完成 details：批次 reservation、started 证据、逐视频本地检查点、批次封包、中心按顺序应用。
5. 实现 `broker_ack`、`durable_received`、`applied`、`fetch_complete` 四级确认；中心承担 durable evidence 恢复责任。
6. 实现 v3 Data API 接续：稳定 request ID、延迟等待、释放采集 slot、结果回放和剩余目标续跑；v1/v2 不获得额外 fallback 权限。
7. 注入节点重启、中心崩溃、ACK 丢失、重复批次、旧 generation、spool 满/损坏、API 提交前后崩溃等故障。

P3 交付物：一个频道从 admission 到 publication handoff 的隔离闭环、恢复报告、结果体积和 spool 上限证据。

P3 通过条件：本地/远程结果逐字段一致；重复/乱序/缺失/损坏明确处理；旧执行不能覆盖新执行；API 等待不占用远程采集 slot。

## P4：部署和页面接线（W12，已完成隔离验收）

目标：实现专用全量节点从登记到排空/删除的完整流程，仍不开生产接单。

实施顺序：

1. 固定全量镜像/安装包、runtime revision、capability 和资源计数。
2. 接通 Dashboard 的全量节点预览、初始化、部署、就绪、接单额度和排空状态。
3. 验证重试、扩容、暂停、排空、删除和中心重启后的状态恢复。
4. 确认现有增量节点的部署身份、配置额度和接单状态保持不变。

P4 交付物：专用节点隔离部署记录、页面状态对照、失败重试和回退演练记录。

P4 通过条件：新建专用节点流程闭环；全量开关默认关闭；旧增量部署回归通过。

## P5：发布前验收（W13，已完成隔离验收）

目标：在隔离环境完成发布级质量和回退验证。

必测项：

- 将真实 `createFullCrawlCenter` 与原业务 handoff、完整 legacy/repair 本地 processor、API fallback 固定为发布执行入口；P4 的部署入口只允许连接验证，不创建全量 consumer。

- Query 与 Migration 来源；v1/v2/v3 合约；legacy/repair 本地兼容路径。
- About、uploads、详情、评论、内容类型、时间窗口、登录限制、零内容和直播场景。
- 运输分块、大小上限、ACK 丢失、重复/乱序/冲突、磁盘故障和清理保留。
- Rota 授权续期、国家复查、代理切换、停止回执和浏览器状态。
- Data API 延迟、重复交接、配额恢复和后续网络执行。
- 中心/节点终止边界、BullMQ 重投、过期 fence、旧镜像兼容和 schema 回退。
- 增量完整回归：注册、整频道执行、结果、API 接续、恢复、部署和接单控制。

P5 交付物：固定镜像 digest、schema 迁移脚本、测试报告、数据对照、故障注入、发布清单和回退清单。

P5 通过条件：第 12 节必测项全部通过；全量接单仍关闭；形成可执行的发布/回退步骤。本轮采用隔离分层验收，证据、测试边界和最终镜像见 [P5 发布验收](FULL_CRAWL_REMOTE_P5_RELEASE_20260920.md)；真实公共频道连续投递及生产性能由 P6 验证。

## P6：生产单 Worker 灰度和观察（W14）

只有 P5 通过后才进入本阶段。每一步都要更新交接文档并保存前后状态。

1. 发布前重新读取线上节点、队列、Rota、数据库、Redis、NATS、磁盘、spool、镜像 digest 和配置 hash；不得用历史记录替代。
2. 兼容 schema 迁移采用短锁等待和可回退步骤；全量接单默认关闭。
3. 如需中心升级，先保存增量接单配置并执行可验证排空；恢复增量后观察其持续推进。
4. 部署一台专用全量节点和一个 full-crawl slot；校验身份、网络、资源和停止回执。
5. 只开放一个全量 slot，观察至少 30 分钟并完成至少 10 个有效全量频道，覆盖真实数据写入和发布。
6. 通过后扩展到 5 个 slot，观察至少 60 分钟并累计至少 30 个有效频道，Query/Migration 各至少 5 个；每次扩容更新证据。
7. 若增量完成速率连续两个 15 分钟窗口下降超过 10%、P95 等待/完成时长增加超过 20%、错误率超过基线 1 个百分点、出现错写/丢失/越权/续锁异常/重复应用，立即关闭全量新领取并进入回退。
8. 全量结果积压增长、spool 达到 80% 或共享资源触及 P0 保留底线时，停止扩大并保留未确认证据。

P6 交付物：发布前后状态、真实频道结果、增量指标、灰度观察、扩容记录、停止/回退记录和最终结论。

## 恢复工作时的第一步

读取本文件、[P5 发布验收](FULL_CRAWL_REMOTE_P5_RELEASE_20260920.md)、机器报告 `reports/fullcrawl-p5-validation-20260920.json` 和当前 `git status`，保留 P1～P5 未提交差异。P5 已装配真实发布中心、原 legacy/repair processor 和业务 handoff，完成原 Worker/增量回归、显式 schema/回退检查、固定镜像按 digest 回拉及实际包验收。本轮已执行至 P5，下一阶段是 P6；实际进入灰度前须重新执行线上只读预检和资源/增量基线核验。不能把 P5 隔离测试或本机 registry 地址当作生产发布记录。

## P6 首轮只读预检（2026-09-20 07:07–07:09 UTC）

已执行首轮生产预检，尚未开放灰度、迁移或部署。两台登记节点均承载增量（47 + 15 个，全部在线接单），没有空闲全量节点，需要准备独立服务器，可复用已有闲置机器。中心 16 CPU、15 分钟负载约 20.4、磁盘已用 88%，资源放行仍待复核；不能只新增节点就直接开启灰度。已保存原接单配置、镜像/环境哈希、两个完整窗口的有效业务完成及队列/Rota/NATS/DB 证据。详见 [P6 首轮预检与服务器建议](FULL_CRAWL_REMOTE_P6_PREFLIGHT_20260920.md) 和 `reports/fullcrawl-p6-preflight-20260920.json`。此前 P5 状态为历史验收记录，P6 当前状态为首轮预检完成、灰度未开始。

## P6 专用节点已就绪（2026-09-20 07:28 UTC）

用户已添加“全量采集节点”（8a07de4f-a3ee-428f-a959-2aee9c7b6be7）。只读实测 SSH 固定指纹、sudo、节点身份、Docker/Compose、监控/NTP、HTTPS/WSS 均正常；2 核、标称 8 GB 内存/80 GB 磁盘，目前无容器。服务器缺失阻塞已解除，适合先做单 Worker 灰度准备。尚未部署 Worker，中心资源预算、全量 schema、镜像分发及兼容升级仍待完成。最新证据见 [节点环境核验](FULL_CRAWL_REMOTE_P6_NODE_ENVIRONMENT_20260920.md) 和 `reports/fullcrawl-p6-node-environment-20260920.json`；本次未修改生产状态。
