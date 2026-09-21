# P2 环境检查与执行准备

检查日期：2026-09-20（北京时间约 09:55～10:03）。结论：P1 基线回归通过，P2 隔离开发环境已就绪。P2 的全量身份、schema、中心 processor 和协议实现尚未开始；本记录不代表 P2 验收通过。

## 代码基线

- 仓库：`/root/workspace/agent/pachongsys-release`。
- HEAD：`bee141e41b609a583a2b86b34967fce1af404a80`。
- P1 的 Collector、Local Adapter、调用方与测试改动仍在工作区，未提交；本次未修改业务源码。
- `git diff --check` 通过。
- 修改交接状态前，已备份 P1 的 9 个源码、测试和文档文件，保存 SHA256 清单及 tracked diff：`runtime/fullcrawl-p2-preflight-20260920/p1-baseline.tar.gz`、`p1-manifest.json`、`p1-working-tree.patch`。这些不是 Git 提交，不包含生产凭据或数据。

## 隔离依赖与资源

Compose：`runtime/fullcrawl-p2-preflight-20260920/compose.json`，项目名 `qy-fullcrawl-p2-preflight-20260920`。

| 服务 | 版本/配置 | 用途 |
| --- | --- | --- |
| PostgreSQL | 16.14，内存上限 512 MiB，数据目录为 512 MiB tmpfs | 独立测试 schema 和恢复验证 |
| Redis | `redis:7-alpine`，内存上限 128 MiB，数据上限 64 MiB，关闭持久化 | 隔离 BullMQ 测试队列 |
| NATS | `nats:2.12-alpine`，启用 JetStream，内存上限 128 MiB，存储为 64 MiB tmpfs | P2 消息依赖准备 |
| 测试执行器 | 当前 qybullmq 镜像，Node v20.20.2，内存上限 768 MiB、1 CPU | 只读挂载工作区、串行执行现有测试 |

三个服务位于独立 `internal=true` 网络，未发布宿主机端口，未加入生产网络。测试 PostgreSQL 使用专用 `p2_test` 用户；容器只通过该内部网络互访，无生产凭据、数据目录或消息存储挂载。三个依赖容器保留运行，重启策略为 `no`；测试执行器退出后自动删除。

测试执行器镜像：`qy-allpachong/qybullmq:pachongsys-ec3aeb0`，本次解析 ID 为 `sha256:0eb79cc351f412887b307d98a0fd18040753f78737eeecb5d131f77211b78f75`。实际被测源码来自只读挂载的当前工作区，含 P1 改动。

宿主机检查时磁盘可用约 112 GiB、使用率 87%，可用内存约 5.9 GiB。测试结束后三个依赖实际内存合计约 149 MiB。当前资源适合有上限、串行的 P2 协议/schema 测试；不能据此认定具备全量生产容量或大规模压力测试余量。

宿主机 Node 为 v26.7.0；正式回归使用上述运行镜像中的 v20.20.2，以匹配运行时。

## 验证结果

| 验证 | 结果 | 证据 |
| --- | --- | --- |
| PostgreSQL 身份及基础 schema、Redis PING、NATS JetStream API | 通过 | `runtime/fullcrawl-p2-preflight-20260920/logs/dependencies.log` |
| 8 个文件的单元回归 | 58 通过、0 失败、0 跳过 | `logs/unit.log` |
| 8 个文件的真实 PostgreSQL/Redis 集成回归 | 45 通过、0 失败、0 跳过 | `logs/integration.log` |

单元覆盖：Full Crawl Factory、Local Collector、Model、冻结合约、Snapshot 恢复、远程节点协议、Rota 及增量运行时。

集成覆盖：Full Crawl Store、Query/Migration 全量 SIGKILL 恢复与重复交付、Candidate attempt 写入保护、共享视频执行接管、远程节点注册/部署/激活、增量业务 fence 与 HTTP 结果应用。

范围限制：NATS 本次验证的是 JetStream 连通性；增量集成使用 HTTP transport 分支。未配置 `REMOTE_NATS_TEST_URL`，因此条件启用的 NATS/WSS/TLS 集成场景未执行，不能将 TAP 的“0 skipped”理解为全部传输分支都已覆盖。中心 supervisor 的专用 PostgreSQL/Redis 测试也不在本次清单内。P2 修改对应入口后应运行相应专项回归。

## 可重复执行入口

在仓库根目录执行：

```bash
runtime/fullcrawl-p2-preflight-20260920/run-preflight.sh
```

脚本仅使用当前已存在的镜像（`--pull never`），启动专用依赖、初始化测试库、检查依赖，并依次执行单元和集成回归，日志写入同目录 `logs/`。库名分别为：

- `fullcrawl_p2_test`：P1 基础 schema 和全量恢复测试。
- `video_execution_recovery_test`：按现有 fixture 要求提供空库，由事务建立测试表并回滚。
- `remote_node_ingestion_test`：按现有远程隔离校验器要求命名，供远程协议测试使用。

不需要生产数据库导出。数据库为 tmpfs；停止或重建 PostgreSQL 容器后测试数据会丢失，重新运行脚本可建立基线。后续新增的 P2 DDL 应保存为源码，不能只留在测试数据库中。

暂不使用环境时可执行以下命令释放资源；命令仅针对本次专用项目：

```bash
docker compose -f runtime/fullcrawl-p2-preflight-20260920/compose.json down -v
```

## P2 实施入口与顺序

按[后续执行清单](FULL_CRAWL_REMOTE_EXECUTION_FOLLOWUP_20260918.md)推进 W03～W06。首先将既有计划中的标识写入统一合约模块：`role=fullcrawl`、`mode=full_crawl_collect`、`slot=full-crawl-N`、`capability=youtube.full-crawl.v1`、`runtime_revision=youtubejs-full-crawl-v1`。Rota 仍使用 `channel` role。

| 工作包 | 已定位的入口 | 首批验证点 |
| --- | --- | --- |
| W03 身份与 schema | `remoteNodes/workerConfig.js`、`workerActivationStore.js`、`workerConnectionSchema.sql`、`workerActivationSchema.sql`、`deploymentAdmin.js` | 现有 role/mode/runtime 校验和数据库约束固定为增量；扩展时必须继续拒绝身份混用、未知版本、重复实例 |
| W05 阶段协议 | `fullCrawlCollector.js`、`remoteNodes/protocol.js`、`channelPlanContract.js`、`schema.sql` | 定义 admission/uploads/details/close_fetch；保存业务代次与运输代次、input/target/payload hash、大小限制和幂等身份 |
| W06 业务校验 | `channelCandidateAttemptMutations.js`、`contentDetailExecutionFence.js`、`fullCrawlYoutubeJsStore.js`、`remoteNodes/incrementalBusinessFence.js` | 全量使用自身 candidate/run/attempt 约束；reservation 不增加尝试，started 才计账；校验、业务应用、applied 标记同事务 |
| W04 中心调度 | `remoteNodes/centerExecutionSupervisor.js`、`centerIncrementalProcessor.js`、`worker.js` | 当前 supervisor 固定 `INCREMENTAL_QUEUE` 与增量 processor；引入明确 workload 分派及全量 processor，验证队列隔离、续锁与容量不足行为 |

需先形成 schema 草案，明确 executions、stages、detail reservations、result batches/parts 与现有通用 tasks/receipts 的边界，再在专用 PostgreSQL 中验证。中心 dispatch 必须依赖上述身份和业务校验就绪，不能只替换队列名称。

P2 通过门槛仍为：隔离环境正确领取/拒绝全量任务，拒绝越权、旧 generation 和重复实例，旧增量协议回归通过；本次环境准备没有提前通过这些新增功能门槛。

## 生产只读快照

2026-09-20 10:00:22 北京时间查询：远程注册均为 `incremental / incremental_collect`；66 条历史注册中 62 条未退役，62 个在线、62 个 enabled。最近 5 分钟增量事件为 completed 415、started 425、failed 12。该短窗口只证明当时持续推进，不是性能对照或无影响证明。

本次生产操作限于只读查询。没有变更生产 schema、接单配置、节点镜像或队列，全量远程部署开关继续关闭。
