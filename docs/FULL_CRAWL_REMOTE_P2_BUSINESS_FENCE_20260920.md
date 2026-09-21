# P2 第三批：真实业务所有权与详情事务

本文件保留第三批完成时的历史状态；后续 W04/W05 已补齐，P2 最终状态见 [中心与协议验收](FULL_CRAWL_REMOTE_P2_CENTER_PROTOCOL_20260920.md)。

日期：2026-09-20。根据“先完善 P2、再进入 P3”的决定，本批完成 W06 的核心业务校验，以及详情 reservation/started/applied 的事务实现。P2 仍未整体完成，W05 的完整阶段消息合约及 W04 中心 processor/supervisor 接线尚未完成。生产全量远程接单未开启。

## 已实现

`fullCrawlBusinessFence.js` 读取真实 PostgreSQL 业务记录，核验以下关联：

- candidate 的频道、状态、dispatch generation、当前 BullMQ job 和 attempt。
- business binding 的 run、candidate、完整 intent hash、冻结 fetch contract 和身份策略。
- materialized run 的频道、candidate、crawl mode、状态、fetch contract，以及频道的当前 run 和首次提升归属。
- 原始 execution attempt 的业务 run、job、零基 job_attempt、dispatch generation、运行状态、Rota worker/instance/slot、route generation 和 network identity；已有更新 attempt 时拒绝旧执行。
- 运输 task 与持久执行证据的 input hash、运输 generation，以及 node、slot、deployment、config、instance、relay boot、runtime revision。
- 网络 slot 的 Rota worker；若已建立 network binding，其节点/slot 和原始 Rota route identity 也必须一致。

准入前 binding 可以为 reserved 且 run 不存在。原 `BrowserProfileStore.beginAttempt()` 在准入前创建的 attempt，其 `run_id` 可以一直为 NULL，但 `business_run_id` 必须完整匹配；不能因此放宽到接受另一个非空 run_id。业务 attempt 的 `job_attempt` 必须等于冻结输入中的 `job_attempt - 1`。

`RemoteFullCrawlExecutionStore` 将真实 fence 装配到专用 activation Adapter。`prepare()` 只接纳已存在的原始业务/Rota attempt，冻结目标节点实例并建立待领取证据；本身不创建替代 attempt，不操作 BullMQ 队列。相同准备请求可重放；校验失败不能推进运输 generation。

`withLease()` 将所有权校验和中心业务写入放在同一事务内；`renew()` 通过同一校验续租。drain 禁止新准备/领取，但允许当前合法 owner 结算；错误实例、过期租约、更新代次均不能续租或写入。通用 HTTP 工作 heartbeat 仍拒绝全量任务；新增续租尚未接成节点网络入口。

## 准入前的并发新 attempt

run 尚不存在时，单靠 run 行锁或查询最大 attempt_number 无法阻止并发插入新 attempt。新增显式 `fullCrawlBusinessSchema.sql` 在全量 attempt INSERT 前锁定真实 candidate 和 business binding，与远程业务事务共用锁。该保护也覆盖通过既有本地 `BrowserProfileStore` 插入的全量 attempt。

业务 fence 检查该触发器已安装且启用；缺失时报 `FULL_CRAWL_BUSINESS_SCHEMA_REQUIRED`，不能悄悄降级。集成测试验证：旧执行持有业务事务时，新 attempt 确实等待数据库锁；新 attempt 提交后，旧执行立即被拒绝。

该 schema 同时扩展执行证据中的完整连接/Rota 身份及详情应用 hash/回执。旧草案行可以保留，但缺少完整身份的行不能授权执行。它仍不属于生产启动迁移。

## 详情计账与应用

`RemoteFullCrawlDetailStore` 提供三个中心事务入口：

1. `reserve()`：核对原有 inline detail fence、uploads 文档及完整目标 hash，从数据库中的冻结目标按顺序预留最多 20 条；不改变候选状态或 attempts。未完成的旧预留、running 或 API pending 内容会阻止新预留。
2. `started()`：将确切 start ID 与预留绑定，在同一事务中置候选为 running 并增加一次 attempts。相同 start ID 重放不计次；跨目标重复 ID、不同 ID 替换和跳过未开始前缀均被拒绝。
3. `apply()`：按目标顺序应用结果；校验原执行、目标和 started 证据。中心 decoder/writer 必须使用传入的同一 PostgreSQL client。业务写入、候选结算和 applied hash/回执一并提交；中途抛错全部回滚。相同结果字节重放返回原回执，不再次调用业务 writer；不同结果字节报冲突。

本批测试实际调用原 `FullCrawlYoutubeJsStore.commitAdmission()`、`commitUploads()`、`commitDetail()`，不是用固定成功回调替代业务存储。测试中的 supervisor readiness 仍是 fixture；生产 supervisor 还未装配。

## 安装与验证

仅在专用 `remote_node_ingestion_test` 中先安装 crawler 业务 schema，再执行：

```bash
node scripts/prepareFullCrawlP2Schema.mjs --apply --with-business-fence
npm run test:remote-full-crawl
```

必须显式提供 `REMOTE_NODE_TEST_DATABASE_URL`，安装脚本核验真实数据库名。未提供 `--with-business-fence` 时仍只安装第一批远程 schema。脚本的新增入口已在隔离数据库重复应用验证。

新增 `remoteFullCrawlFence.postgres.integration.test.js`，覆盖真实准入前/后业务记录、错误归属、线路/实例/代次变化、新 attempt 并发、fence 缺失、租约失效、冻结目标变化、详情计账、应用顺序、回滚及重复结果。

扩大回归：68 项单元、115 项集成测试通过（包含新增 30 项业务 fence/详情事务测试），0 失败、0 跳过。最终代码的全量专项 41 项再次全部通过，包含最后补充的跨目标重复 start ID 拒绝检查；与扩大回归有重叠，不重复计入 183 项。专项日志为 `runtime/fullcrawl-p2-preflight-20260920/logs/full-final.log`；扩大回归日志为同目录 `unit.log` 和 `integration.log`。`git diff --check` 通过。所有执行均在隔离容器/数据库内，未修改生产数据库、队列、部署或节点。

## 尚未完成与下一步

- 完整的 admission/uploads/details/close_fetch 结果消息、错误/API 交接、payload 解码与校验仍须完成 W05。`apply()` 的 callback 是受信任的中心业务入口，不能直接把节点提供的分类/处置结果当作中心决策。
- 未实现全量分块的 durable_received、NATS/WSS 回传、旧代次已接收证据恢复、uncertain started 的恢复或 API 接续。当前保守阻止旧证据未结算时重领，不能据此宣称已可恢复完整远程任务。
- 全量队列 processor、Rota/runtime、supervisor readiness、容量控制及 legacy/repair 分流仍是 W04；当前生产入口仍只装配增量。
- 下一步继续 W05 的完整阶段合约与中心应用，再装配 W04 并完成 P2 隔离验收。达到 P2 门槛后才进入 P3 的远程采集闭环。
