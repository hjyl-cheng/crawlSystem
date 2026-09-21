# P2 第一批：全量身份、阶段合约与隔离 schema

日期：2026-09-20。范围为准备记录中确定的下一步：将全量标识固化为代码，建立阶段合约和可运行的隔离 schema 草案。P2 已开始，尚未整体完成。

## 本批实现

`services/qybullmq/src/remoteNodes/collectingWorkload.js` 定义全量身份：

| 字段 | 值 |
| --- | --- |
| role | `fullcrawl` |
| mode | `full_crawl_collect` |
| slot | `full-crawl-N`，N 为不带前导零的正整数 |
| capability | `youtube.full-crawl.v1` |
| queue | `youtube-channel-crawl` |
| runtime revision | `youtubejs-full-crawl-v1` |

`workerConfig.js` 仅在调用方明确指定 `full_crawl_collect` 模式时接受全量配置；默认连接入口、增量入口继续拒绝全量配置，拒绝 role/mode/slot 混用。现有增量 slot 保持兼容，但不能占用 `full-crawl-` 命名空间。

`fullCrawlProtocol.js` 提供执行、阶段和结果批次合约校验：

- 执行输入必须是普通 `channel-snapshot`，带 candidate、run、business run、job attempt、dispatch generation、执行 attempt 和完整冻结抓取合约。
- 只接受带正确 manifest hash 的 `youtubejs_full_v1/v2/v3`；拒绝 legacy、repair、增量 Plan 字段和隐式默认合约。
- 阶段为 `admission / uploads / details / close_fetch`，使用稳定 JSON hash 校验输入，绑定 execution hash、task、generation、stage 和 target hash。
- 详情单批最多 20 个目标，视频和 reservation ID 不重复，ordinal 严格递增。
- 输入上限 64 KiB，结果单批上限 8 MiB，按 512 KiB 定长分块（末块可不足），最多 16 块。批次 manifest 必须与阶段身份和大小匹配。

这些大小是第一版协议硬上限，不是已完成压力测试的吞吐参数。结果字节校验、分块组装、持久回执与实际传输将在后续阶段接入。

## 隔离 schema 草案

`fullCrawlSchema.sql` 通过显式迁移扩展 worker role/mode 约束，并增加五张表：

| 表 | 用途 |
| --- | --- |
| `full_crawl_executions` | task/generation 对应的全量输入和节点实例证据 |
| `full_crawl_stages` | 阶段顺序、输入与目标 hash |
| `full_crawl_detail_reservations` | 目标预留、started 证据和应用状态 |
| `full_crawl_result_batches` | 批次幂等身份、完整大小、接收与应用状态 |
| `full_crawl_result_parts` | 有界分块及 hash，归属于确切批次 |

运输租约仍仅保存在现有 `remote_ingestion.tasks`；没有在新表中复制第二套 lease。预留行的默认状态不包含 started 证据，不能直接变成 applied；后续 W06 负责把真实 started 证据、业务尝试计账和应用标记放进同一事务。

数据库约束覆盖角色混用、缺失执行代次关联、阶段重复序号、目标重复、未开始却已应用、分块越界和分块长度不一致。业务身份与现有 candidate/run/attempt 的实时匹配、阶段合法转移、payload hash 实算及完整接收条件仍由待实现的 W04～W06 负责，不能把表约束当作已通过这些校验。

## 应用与测试入口

新增 `scripts/prepareFullCrawlP2Schema.mjs`：不读取生产 `DATABASE_URL` 默认值；必须提供 `REMOTE_NODE_TEST_DATABASE_URL`，且实际数据库名必须为 `remote_node_ingestion_test`。不带 `--apply` 只预览；带参数时在短锁超时和事务内应用已有远程 schema 与全量草案。当前生产 schema 入口未引用本文件。

在 P2 专用内部网络的测试执行器中：

```bash
node scripts/prepareFullCrawlP2Schema.mjs --apply
npm run test:remote-full-crawl
```

必须先配置上述测试 URL，否则集成测试会按项目现有约定跳过；本次真实集成验证已提供 URL。仓库根目录的 `runtime/fullcrawl-p2-preflight-20260920/run-preflight.sh` 已包含本批新增测试以及 P1、远程增量回归。

schema 工具已在专用库验证预览、连续两次幂等应用、错误库拒绝。五张表已保留在测试库中，生产未应用。日志为 `runtime/fullcrawl-p2-preflight-20260920/logs/schema-prepare.log`。

发现并修复既有回归夹具的重复运行问题：`remoteIncrementalFence.postgres.integration.test.js` 使用固定的 `business_run_id='mismatch'`，复用测试库时触发业务 attempt 唯一约束；改为带当前执行 attempt ID 的独有值，保留“错误业务归属必须阻塞恢复”的原断言。

最终回归结果：**68 项单元、46 项真实 PostgreSQL/Redis 集成测试全部通过，0 失败、0 跳过**。日志位于同一 runtime 目录的 `logs/unit.log`、`logs/integration.log`。`git diff --check` 通过。NATS 本轮仍为依赖连通性验证，不包含全量消息回传或 NATS/WSS 端到端采集测试。

## 剩余 P2 工作

1. W03 后续更新：全量节点注册、心跳、就绪、启用/撤销和 capability 校验已在 [P2 第二批](FULL_CRAWL_REMOTE_P2_ADMISSION_20260920.md)实现；`RemoteWorkerActivationStore` 已支持独立全量准入接口，但真实业务校验和中心 processor 仍未装配，不能据此开启生产接单。
2. W05 余项：各阶段业务输入/结果、错误/API 交接合约，以及消息通道与结果分块校验接线。
3. W06 后续更新：真实 candidate/run/attempt/route fence，以及 reservation、started、业务写入和 applied 的事务保护已在 [P2 第三批](FULL_CRAWL_REMOTE_P2_BUSINESS_FENCE_20260920.md)实现；仍需与完整阶段消息、中心 processor 和恢复路径接线。
4. W04：在上述约束之上实现中心全量 processor，按 workload 分派队列、续锁、并发和容量控制；保留 legacy/repair 本地兼容路径。
5. 补齐领取与拒绝全量任务的端到端测试和旧增量协议专项回归，随后才能判定 P2 通过。

本批没有启用 Dashboard 全量部署、没有新增生产 Worker、没有运行全量网络采集；生产接单开关保持原状。
