# P5 全量远程执行发布前验收

日期：2026-09-20。状态：P5 隔离验收通过。范围：隔离发布验收，生产 schema、容器、节点凭据和接单设置未改变。P6 生产灰度未执行。

机器证据：[fullcrawl-p5-validation-20260920.json](../reports/fullcrawl-p5-validation-20260920.json)。本阶段继承 P4 的 Dashboard/SSH/退役验收，本轮执行最终中心入口、真实依赖、原 Worker、发布事务及最终镜像验证。

## 发布执行入口

`services/qybullmq/scripts/runRemoteNodeCenter.mjs` 现在提供三个明确状态：

| 配置 | 行为 |
| --- | --- |
| 全量 deployment/execution 均未启用 | 保持原增量中心行为 |
| 只启用 `REMOTE_NODE_FULL_CRAWL_DEPLOYMENT_ENABLED=true` | 注册、连接和退役全量节点；不创建全量 consumer |
| 同时启用 `REMOTE_NODE_FULL_CRAWL_EXECUTION_ENABLED=true` | 校验完整 schema、原业务兼容配置、节点白名单和容量后，装配 `createFullCrawlCenter` |

执行入口接入原候选结算、Discovery 唤醒、Finalize 入队和 Data API fallback。`worker.js` 导出原 Worker runtime；嵌入模式复用完整原 processor、候选/恢复归属、managed Rota、API 延后/回放及失败落库，不创建第二套队列 consumer，不安装额外信号处理器，也不能调用独立 Worker 的启动方法。

原 `pipelineV2` 的出站队列改为首次使用时建立，关闭时回收。原独立 Worker 仍从 `node src/worker.js` 启动。legacy/repair 继续在中心本地兼容槽执行；支持的全量 snapshot 才进入节点。暂停检查也覆盖兼容和控制任务。

中心只有在 NATS 连接和 full result consumer 可用时才能报告全量执行就绪。中心先关闭消费者、保持活动 BullMQ 锁完成收尾，再关闭兼容 Rota、队列、传输及数据库。

## 容量和配置约束

- `REMOTE_NODE_FULL_CRAWL_NODE_IDS` 必须是明确的 UUID 白名单；本阶段不使用无界的 dashboard 自动准入。
- `REMOTE_NODE_FULL_CRAWL_TOTAL_SLOTS` 至少为 2，包含 **一个本地兼容槽**。配置为 2 时最多一个远程全量槽；配置为 6 时最多五个远程槽。
- 发布 runtime 持有独立 PostgreSQL session advisory lock `(781138015,1)`。另一个发布进程不能另领一份全量容量；已有逐节点/slot supervision lock 仍保留。锁连接失效会关闭全量 runtime。
- 全量需明确设置 `WORKER_QUEUES=youtube-channel-crawl`、`PROXY_SLOT_ROLE=channel`、独立 `PROXY_WORKER_ID`、`YOUTUBEJS_EXTRACTOR_MODE=full`、`SKIP_SCHEMA_MIGRATION=true`。不得复用 canary 专属队列前缀。
- 原 `DATABASE_URL`（或其文件配置）必须与中心事务数据库 URL 一致；仍验证 `EXPECTED_CRAWLER_DATABASE` 和 `FORBIDDEN_CRAWLER_DATABASE`。
- 原 `REDIS_HOST/PORT/PASSWORD`、`BULLMQ_PREFIX` 必须与 `REMOTE_NODE_REDIS_URL`、`REMOTE_NODE_QUEUE_PREFIX` 完全对应。原出站队列不支持 Redis username/TLS，因此本次全量入口明确拒绝这些配置，不静默降级。
- 原本地 Rota control token 必须与中心专用 control token 文件一致；route-reader token 仍独立。原浏览器 profile 密钥必须与中心 profile 文件一致，不能重新生成密钥覆盖已有 profile。
- `ROTA_PROXY_BASE_URL` 和 `ROTA_BULLMQ_PROXY_PASSWORD` 用于原本地兼容流程。远程节点继续只取得自己的限时路由授权。
- `YOUTUBEJS_VIDEO_API_BATCH_FALLBACK=true` 才装配 API fallback；每个冻结合同仍决定能否使用 fallback，v1/v2 不因开关而升级为 v3。

可选 Compose 叠加文件：`deploy/compose.remote-node-full-crawl.yml`。两个全量开关默认均为 false；明确把中心上限设为 1536 MiB、192 PID、128 MiB `/tmp`，为原本地兼容执行预留资源。配置解析已验证，尚未应用于生产。实际发布前必须重新核对宿主和 Rota 余量；此上限不是容量测量结果。

## 镜像与分发证据

本次将已安装的中心/采集基础镜像推入 **本机随机回环端口的独立 registry**，读取真实 manifest 内容并计算 SHA-256，再以基础 manifest digest 构建发布镜像。两个最终镜像也经 push、manifest 读取、按 digest pull 验证。

最终引用、基础引用、image ID、manifest 哈希和源码哈希均见机器报告。`runtime/fullcrawl-p5-20260920/manifests/` 保存原始 manifest；`image-source-files.json` 保存进入镜像的逐文件哈希。测试时只挂载测试代码和测试证书，未用源码目录覆盖新/旧中心镜像内的应用代码。

registry 容器验收后删除；本地 Docker 镜像和 `runtime/fullcrawl-p5-20260920/registry/` 的分发数据保留。报告中的 `127.0.0.1:端口` 是隔离验收地址，不是生产发布仓库。P6 准备发布时需将同一产物复制到批准的发布仓库并重新核验 manifest digest。未向外部或生产 registry 推送。

## 验收矩阵与证据边界

所有本轮纳入报告的最终测试组均要求零失败、零跳过；不把重叠测试数量相加作为独立业务场景数。

本轮最终结果：

| 测试组 | 通过 / 失败 / 跳过 |
| --- | --- |
| 原 Worker 接线与初始配置回归 | 36 / 0 / 0 |
| 全量/增量/远程单元矩阵 | 299 / 0 / 0 |
| 全量及增量 HTTP 集成矩阵 | 125 / 0 / 0 |
| 增量 TLS NATS 与整频道恢复 | 62 / 0 / 0 |
| 原 Worker 实际进程回归 | 18 / 0 / 0 |
| Query/Migration × 合同与内容策略 | 47 / 0 / 0 |
| 原发布持久投递与恢复 | 2 / 0 / 0 |
| Publication 事务、钩子与接收 | 13 / 0 / 0 |
| 共享 Data API 配额恢复 | 1 / 0 / 0 |
| 旧中心镜像 / 新 schema | 1 / 0 / 0 |
| 最终中心镜像实际入口 | 5 / 0 / 0 |
| 最终节点镜像生命周期 | 1 / 0 / 0 |
| 最终中心镜像业务数据对照 | 5 / 0 / 0 |


| 计划要求 | 本轮证据 |
| --- | --- |
| 真正发布入口、默认关闭、错误配置、退出 | `remoteFullCrawlRelease`：真实中心子进程、BullMQ、TLS NATS、签名 relay、节点结果应用及原 Finalize 入队；多中心容量排他、legacy/repair 原错误验证和失败事件 |
| Query/Migration，冻结 v1/v2/v3 | `remoteFullCrawlCoordinator` 的 2×3 数据库执行矩阵，验证保存的合同不变；Migration 活跃度采用明确发布时间证据；原 Query/Migration 进程恢复测试 |
| About、准入、普通视频、Shorts/直播、登录限制、零内容、窗口、评论 | 原 factory/model/store 单元矩阵、content window/detail/comment 策略测试及远程 coordinator/节点实际写入；三种合同均使用同一原业务执行器 |
| 数据对照 | TLS/WSS 两种远程执行与原本地 collector 在相同观察输入下比较 candidate、channel、detail attempts/disposition、contents 的 ID/type/title/views，以及 Run 的 selected/stored/excluded 和状态；原始比较输出写入机器报告关联文件 |
| Finalize、Agent、Publication | 真实入口验证原 Finalize 队列中有持久任务；原 Worker 验证 Agent/Detail/API 归属与恢复；独立 Crawler/Business 测试库验证 Publication hooks、Revision/Current/Outbox 原子性、Publisher 持久回执、Ingress 幂等和恢复投递 |
| 分块、大小、ACK 丢失、重复、乱序、冲突 | full message/fence/node/NATS 测试；大于单分块的 About 证据、SQL ACK 丢失、重放、hash 冲突和事务回滚 |
| 节点/角色/slot/generation 与撤销 | full activation/fence、原增量 fence、竞争 supervisor 与旧连接证据拒绝测试 |
| Rota、国家、浏览器 profile、停止回执 | 真实 relay 与网络会话测试；身份不匹配、续期失败、国家复查、丢失 grant/stop ACK、profile checkpoint 恢复 |
| API 等待、配额和后缀执行 | full v3 接续、丢失 BullMQ continuation 恢复、原 Worker API 等待/晚到结果；增量 HTTP/NATS 和整频道模式释放槽及未完成后缀继续 |
| 进程故障和重投 | 原 Worker/增量中心 SIGKILL 提交边界测试、全量 SQL/journal 重启恢复、实际中心与节点容器重启、队列续锁及旧 fence 拒绝 |
| 磁盘、清理和保留 | full node spool 压力、journal 恢复；实际节点容器删除后保留 spool；回退脚本只读检查，不删除表和结果证据 |
| 旧版本、迁移与回退 | 旧中心镜像在预先准备的新 schema 上实际启动；新镜像执行入口；重复迁移、3 秒锁等待超时回滚、错误数据库拒绝、API 接续阻止回退 |
| 增量完整回归 | 原注册/部署/接单、整频道、API、结果、恢复，分别运行 HTTP 和 TLS NATS 路径 |
| 页面部署 | 继承 P4 已通过的 Dashboard PostgreSQL/路由、SSH 安装、扩容/退役证据；本轮未修改这些实现 |

这些是受控观察数据和真实隔离依赖的分层验收。并未以一个真实公共频道连续跑完节点抓取、外部 Agent 和生产 Business 发布，也没有生产流量、浏览器截图或线上性能数据。数据对照文件明确列出比较字段，不声称所有非确定性时间戳或原始响应字节一致。真实频道产出和增量指标须在 P6 观察确认。

## 显式迁移与回退清单

发布工具：`services/qybullmq/scripts/fullCrawlReleaseSchema.mjs`。必须显式提供 `FULL_CRAWL_RELEASE_DATABASE_URL` 与 `FULL_CRAWL_RELEASE_EXPECTED_DATABASE`，不读取默认业务数据库地址。

```sh
node services/qybullmq/scripts/fullCrawlReleaseSchema.mjs --check
node services/qybullmq/scripts/fullCrawlReleaseSchema.mjs --apply
node services/qybullmq/scripts/fullCrawlReleaseSchema.mjs --rollback-check
```

这里列出的是 P6 待执行命令，本轮只在隔离库运行。工具先核对数据库名称。`--apply` 在单事务内设置 `lock_timeout=3s`、`statement_timeout=15s`，取得迁移锁，应用四份增量 SQL，并验证必需列及业务/通知 trigger 后提交。原有 remote 基础 schema 和 crawler schema 是前提；不会借此工具重装整个业务库。启动入口只验证 schema，不执行迁移。

发布顺序：

1. 进入 P6 时重新读取线上增量基线、节点/任务/结果积压、资源、配置哈希和旧镜像摘要，保存原接单设置；不可使用本报告替代线上预检。
2. 固定批准仓库中的新旧镜像及源码清单，显式执行 schema 迁移；遇到锁超时停止该次操作。
3. 按原计划排空中心升级影响范围；部署兼容中心时两个全量开关保持关闭。验证旧增量节点、原接单设置及完成进度。
4. 配齐上述原兼容配置、独立全量白名单和 `TOTAL_SLOTS=2`，部署一个专用节点/slot。先验证连接、profile、路由、资源和 ready，再按 P6 操作开放一个远程全量槽。
5. 按计划完成至少 30 分钟、10 个有效频道的观察，核验业务发布和增量指标后才考虑扩容。

回退顺序：

1. 通过全量 intake 控制停止新领取，保留活动 BullMQ 锁及节点证据，等待原任务收尾；不直接强杀正在执行业务的中心。
2. 执行 `--rollback-check`。仍有全量接单请求、活动 task/attempt、未明确停止的网络 binding、未应用/冲突结果，或尚未完成 fetch 的 `received` API/国家接续时，返回 `ready=false` 和非零退出码。
3. 处理并复核阻塞项后，再关闭全量执行开关或切回旧中心镜像。旧中心采用已存在的新 schema，不重新执行旧 schema 安装脚本。
4. **保留兼容 schema、tasks、batches、parts、profile、API handoff 和 spool**。本方案的回退是应用回退，没有破坏性的 down migration。
5. 恢复并核对原增量接单设置及持续推进；全量证据继续保留，直到业务与保留策略确认可清理。

`--rollback-check` 是操作前快照；先停止接单并保持全量控制状态不变，再做回退，不能把一次历史检查当作后续操作的永久授权。
