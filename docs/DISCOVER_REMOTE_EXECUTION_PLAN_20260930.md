# Query Discover 远程执行实施方案

日期：2026-09-30。状态：方案已确认；P1–P3 已在工作区完成并通过隔离测试，未提交，未改动任何线上服务或配置。进度见文末“实施记录”。

## 目标

让用户在“服务器节点”页面把服务器登记为 Discover 类型，部署 Discover Worker 并开启接单后，Query 找频道流程的 YouTube 搜索请求由远程服务器执行；中心保留调度、页面意图、候选频道写入和后续派发。做法与远程增量、全量一致：节点只负责网络采集，不持有数据库、Redis 或 Rota 管理凭据。

首个使用者是已登记的 `query 备用`（43.153.54.89）。

## 当前事实

以下来自本次源码阅读和 2026-09-30 02:30–03:10 UTC 的只读生产检查。

| 事实 | 依据 |
| --- | --- |
| 一个 Discover 任务只做一次 YouTube 请求：首页搜索（HTML）或翻页（innertube `youtubei/v1/search` POST）。其余都是数据库和队列操作 | `worker.js` `processDiscoverPage`；`youtube.js` `fetchPopularThisYearVideoSearchInitial`、`fetchSearchContinuation` |
| 请求通过 Rota `discover` 槽位分配的代理，用 undici `ProxyAgent` 直连，不使用 YouTubeJS 或指纹请求进程 | `discoverExecutionRuntimeAdapter.js` |
| 结果处理：解析 `ytInitialData`/`ytConfig`，提取视频作者为候选频道，写 `channel_candidates`、`channel_candidate_sources`，为新频道向 `youtube-channel-crawl` 派发 `channel-snapshot`，更新 `query_pages`，按续页令牌决定是否翻页 | `worker.js` 560 行起 |
| Rota 已有 `discover_page` 任务类型和 `discover` 角色；远程线路查询不限制角色 | `rota/core/internal/proxycontrol/types.go`、`remote_route.go` |
| 远程节点只支持 `incremental`、`fullcrawl` 两种任务 | `remoteNodes/collectingWorkload.js`、`workerConfig.js`、`dashboard/src/nodeWorkerTypes.js` |
| `query 备用` 已登记为 `incremental`，20 个增量 Worker 在线但未启用（enabled=0） | `dashboard_server_nodes_v1`、`remote_ingestion.worker_connections` |
| `全量采集节点`（43.172.77.209）10 个全量 Worker 在线但未启用；`youtube-channel-crawl` 当前没有任何消费者 | 同上；Redis BullMQ 连接 |
| `youtube-discover-page` 队列被控制器以 `proxy_cooldown_ratio_high` 自动暂停；当前比例约 0.82，阈值暂停 0.35/恢复 0.15 | Redis `bull:youtube-discover-page:meta`；控制器日志 |
| Query 调度每次切片 3 个 Query，Discover 队列上限 3；Rota Discover 槽位 2 个 | `crawler.settings.query_scheduler`；Rota 配置 |

## 前置问题（与远程化无关，但不解决则流程跑不通）

1. **Discover 队列被暂停**：冷却比例阈值是按旧的 5 分钟冷却设定的，与 2026-09-29 上线的 Rota 阶梯冷却冲突。需先选定处理方式（改判断指标 / 调阈值 / 缩短 Rota 最长冷却）。
2. **全量没有消费者**：Discover 找到的新频道要经 `youtube-channel-crawl` 验证入库。全量节点未开启接单时，候选会停在 `queued`，批次无法完成。
3. **调度吞吐上限**：切片 3、队列上限 3、Rota Discover 槽位 2。远程 Worker 数量超过这些上限不会带来提速，需一起调整。

## 方案

### 职责划分

| 中心保留 | 远程 Discover 节点 |
| --- | --- |
| Query 调度、页面意图（`query_pages`）认领、BullMQ 消费与重试 | 执行一次搜索请求（首页或翻页） |
| Rota Discover 槽位、任务、观察上报与换线决策 | 通过签名线路授权和本机 relay 走代理 |
| HTML/JSON 解析、候选提取、资格判定、全部数据库写入 | 回传原始响应（状态码、必要响应头、正文压缩） |
| 原始证据存储（`saveFetchedRaw`）、网络事件、翻页与全量派发 | 心跳、停止回执、未确认结果本地 spool |

解析放在中心：解析规则变化时无需重新部署节点，中心也能完整校验节点回传的内容。

### 新增标识

```text
role:             discover
mode:             discover_collect
slot:             discover-N
capability:       youtube.discover-page.v1
runtime_revision: youtube-search-discover-v1
queue:            youtube-discover-page
Rota role/kind:   discover / discover_page（已有）
```

一台服务器仍只登记一种类型。

### 中心侧

1. **拆分 `processDiscoverPage`**（行为不变的重构）：
   - `prepareDiscoverPage`：校验并认领页面意图（现有 `query_pages` 更新）。
   - `fetchDiscoverPage(intent) → { url, status, rawText, contentType }`：采集 Seam。本地 Adapter 调用现有 `youtube.js` 请求函数；远程 Adapter 下发节点命令。
   - `applyDiscoverPage(intent, fetched)`：现有解析、候选写入、派发、翻页逻辑原样搬入。
   本地 `worker-discover` 继续可用，作为回退路径。
2. **远程执行**：`centerExecutionSupervisor` 按“已就绪的远程 Discover 槽位”创建 BullMQ consumer（每槽位并发 1），与增量/全量同样按 workload 选择 processor、队列和恢复逻辑。
3. **Rota**：中心为远程槽位租用 `discover` 槽位、`BeginTask(discover_page)`，根据节点回传的失败分类上报观察（代理传输失败、YouTube 风控），沿用现有换线和预算规则。
4. **失败语义**：与本地一致，请求失败时标记页面 `failed`、记录网络事件并按原重试策略处理；节点不能直接改页面状态。

### 传输协议

- 一个 BullMQ 任务对应一个远程任务和一条 `collect_search_page` 命令，使用 `remote_ingestion.tasks` 的租约、代次和心跳。
- 命令输入：`kind`（initial/continuation）、`query_text`、`language`、`country`、`filter_param`，或续页所需的 `ytConfig`（apiKey、clientName、clientVersion、context）和 `continuation`。输入由中心生成并冻结，节点不做改动。
- 结果：沿用全量的分块回传（单块不超过 512 KiB），单页正文设总大小上限，超限明确报错，不截断。搜索页实际大小需在实现前抽样测量。
- 结果主题与增量、全量分开：`qy.remote.discover.results.<nodeId>`，独立积压上限。

### 节点侧

- 新运行时 `discoverNode.js`：复用 NATS 客户端、连接监督、签名线路授权、本机 relay、spool 和停止回执。
- HTTP 请求经本机 relay 走 Rota 授权线路；请求头、语言和国家参数与本地 `youtube.js` 保持一致（抽出共享的请求构造函数，避免两份实现分叉）。
- 节点镜像：体积小于增量和全量，不需要 YouTubeJS 和 Python 指纹进程。

### 仪表盘

- `nodeWorkerTypes.js` 新增 `discover`（显示为“Query 发现”，队列 `youtube-discover-page`）。
- 部署、接单数量、启停、退役流程接入 Discover 类型。
- **新增“候选频道”页面**（导航加 `/candidates`）：目前仪表盘没有展示 `crawler.channel_candidates` 的页面，只有迁移频道列表读取迁移来源映射。新页面只读，默认只显示 Query 发现来源（`source_json.source='youtube_search_discovery'`）：
  - 列：频道（头像、标题、handle、链接）、状态（discovered/queued/validating/accepted/rejected/existing）、搜索订阅数、来源 Query 与页码、排名、所属批次、发现时间、验证完成时间、拒绝原因。
  - 筛选：批次、状态、Query 关键字、时间范围；统计卡片：各状态数量、待全量验证数量。
  - 分页和统计沿用现有列表页的做法（独立统计接口、缓存、限时查询），避免大表全扫。
- **仪表盘代码基线**：线上仪表盘镜像 `query-start-controls-20260924` 来自工作树 `/tmp/pachongsys-query-start-20260924`（基线 `fa5656a`）。逐文件比对后，它与当前分支仪表盘代码只差 `controlledWritePolicy.js` 及其测试：线上版本在受控迁移期间放行 Query 调度的 `draft/start/resume/pause/stop` 请求。Query 调度页面和接口本身当前分支已有。P4 已把这处差异并入本分支，上线不会丢失 Query 调度控制。
- **`query 备用` 的转换**：当前规则不允许已部署节点改类型。它的 20 个增量 Worker 未启用、无在途任务，建议退役这 20 个 Worker，删除登记后按 Discover 类型重新登记和部署。首版不实现同机混合部署或原地转换。

## 分阶段实施

| 阶段 | 内容 | 验收 |
| --- | --- | --- |
| P1 | 中心拆分 `prepare / fetch / apply`，本地 Adapter | 现有 Discover 测试全部通过；本地 Worker 行为不变 |
| P2 | Workload 标识、协议、中心远程 processor、结果接收与恢复、Rota 接线 | 隔离 Postgres/Redis/NATS 集成测试：正常、失败、翻页、重复回传、租约过期、节点重启 |
| P3 | 节点运行时与镜像 | 隔离端到端：假 YouTube 服务 + 真实 relay，结果与本地路径逐字段一致 |
| P4 | 仪表盘类型、部署与接单控制；候选频道页面 | 隔离环境完成登记→部署→启用→退役；候选页面在大数据量下分页和统计正常；线上已有页面（含 Query 调度控制）无回归 |
| P5 | 生产灰度：`query 备用` 先部署 1 个 Discover Worker，小批 Query | 页面、候选、全量派发与本地路径一致；无增量、全量回归 |
| P6 | 按实际上限扩容（同时调整 Rota Discover 槽位数和调度上限） | 发现吞吐按 Worker 数提升；失败率不高于本地基线 |

粗略估计 P1–P4 开发与隔离测试需要 2–4 个工作日，实际视测试环境和结果大小测量而定。

## 已确认的决定（2026-09-30）

1. 按本方案实施：中心解析，节点只做搜索请求；首版一台服务器一种类型。
2. `query 备用` 的 20 个未启用增量 Worker 退役删除，改登记为 Discover。
3. 发现结果写入**候选频道列表**（`crawler.channel_candidates`），沿用现有收尾逻辑：新频道派发 `channel-snapshot` 到全量队列排队，全量节点开启接单后再验证入库。不写入待迁移列表。
4. 全量节点暂不开启接单。
5. Discover 暂停问题通过**调高控制器阈值**解决（环境变量，重启控制器）；不改代码。
6. 不先启动本地 `worker-discover`，等远程版本。

### 与决定相关的事实

- 视频搜索结果不带作者订阅数（历史 16 个搜索发现的候选全部缺失），Discover 阶段只能排除已在库频道和订阅数明确不达标的频道；资格由全量 `channel-snapshot` 验证。
- 待迁移列表原始同步的 401,325 个频道已全部开始迁移，其中 397,671 个已进入频道主表；巴西导入的 18,091 个中尚有 17,391 个未开始。
- 控制器在全量队列积压达到 100 时暂停 Discover（`DISCOVER_PAUSE_CHANNEL_BACKLOG`，恢复阈值 40）。全量不接单时，Discover 约发现 100 个新频道后会自动暂停。

## 仍需确认

1. 控制器阈值的具体数值：冷却比例暂停/恢复（当前默认 0.35/0.15），以及是否同时调高全量积压暂停/恢复（当前 100/40）。
2. 初始 Discover Worker 数量，以及是否同步提高 Rota Discover 槽位（当前 2）和调度切片/队列上限（当前 3/3）。

## 实施记录

### P1 中心拆分（完成）

- `youtube.js`：搜索请求拆为 `discoverSearchPageRequest`（构造）、`requestDiscoverSearchPage`（发送，返回原始响应）、`parseDiscoverSearchPage`（解析）。原 `fetchPopularThisYearVideoSearchInitial`、`fetchSearchContinuation` 改为两者组合。
- 新增 `discoverPageProcessor.js`：`processDiscoverPage` 自 `worker.js` 迁出，拆为认领、请求、应用三步；只有请求一步访问网络且可替换。Discover 的换线检查点、解析契约失败、最终失败终态三段处理也迁入并导出，本地与远程共用。
- 迁移前后 `query_pages` 等全部 SQL 逐条比对一致（13 条 + 7 条）。

### P2 协议与中心执行（完成）

- 任务类型：`DISCOVER_WORKLOAD`（`discover` / `discover_collect` / `discover-N` / `youtube.discover-page.v1`，Rota 角色 `discover`）。全量与 Discover 统一为“专用能力”：节点只能声明一种专用能力，专用任务只能由对应连接凭任务校验领取。
- 数据库：`remoteNodes/discoverSchema.sql`（放宽 worker 连接与命令操作约束，原有规则不变）。中心开启 Discover 时校验已执行。
- 中心：`discoverPageStore.js`（准入、领取等待、单命令、结果接收、完成、停止）、`managedDiscoverRuntime.js`（Rota 身份运行时）、`centerDiscoverProcessor.js`（与本地相同的托管尝试与失败处理）、`discoverRecovery.js`（中心中断后的槽位恢复）、`discoverCenterRuntime.js`（组装）。线路授权复用 `RemoteChannelRouteStore`，业务运行 ID 为 `discover-page:<page_id>`。
- 节点：`discoverNode.js`（执行器、线路运行时、进程入口）、`scripts/runRemoteNodeDiscover.mjs`、`services/remote-node/Dockerfile.discover`。请求只在网络会话提供的代理上下文中发出，无直连退路。
- 传输：NATS 新增 `discover_commands` 与结果类型 `discover_result`；线路 RPC 按任务能力分发到 Discover 线路存储，YouTube 会话请求对 Discover 拒绝。
- 中心启动：`REMOTE_NODE_DISCOVER_EXECUTION_ENABLED=true` 时启用，要求已开启中心执行与 NATS、已执行迁移、配置对象存储（`S3_*`）与 `ROTA_DISCOVER_IDENTITY_POLICY_ID`；默认关闭，现有行为不变。
- 读取爬虫设置抽为 `crawlSettings.js` 工厂函数，`pipelineV2.getCrawlSettingsV2` 行为不变。

开发中发现并修复：NATS 结果封装 `resultEnvelope` 只允许既有结果类型，`discover_result` 会被两端拒绝（端到端测试发现）。

### 测试

- 单元：`remoteDiscover.test.js`（13）、`remoteDiscoverCenterProcessor.test.js`（5）、`discoverPageProcessor.test.js`（11）。
- Postgres 集成：`remoteDiscover.postgres.integration.test.js`（约束、注册、准入、领取权限、业务校验、线路绑定、结果幂等、恢复）。
- NATS 端到端：`remoteDiscoverNats.postgres.integration.test.js`（真实 TLS NATS、节点执行器、真实请求函数 + 模拟 YouTube；远程失败的消息、失败分类、Rota 换线分类与本地基准逐项相同）。
- 全部单元测试回归：1880 项中 3 项失败，均已确认与本改动无关（2 项在未改动代码上同样失败；1 项扫描到 git 忽略的 `runtime/` 本地脚本）。

### P3 节点镜像与真实 relay 验证（完成）

- 本机构建 `qy-allpachong/remote-node-discover:p3-20260930`（未推送）。基础镜像 `remote-node-incremental:stale-youtube-session-20260922`：依赖锁文件与当前代码一致；镜像内 relay（`node-forward`）与当前源码编译结果哈希相同。以 1000 用户运行，无指纹进程。
- 镜像冒烟：Node 20 加载全部 Discover 模块；节点侧单元测试 23 项在镜像内通过；缺少健康文件时健康检查返回 1；缺少节点配置时以明确错误码退出。
- `remoteDiscoverRelay.postgres.integration.test.js`：真实 relay + 真实网络会话 + 真实 NATS，经假上游代理访问测试证书的 `www.youtube.com`。验证请求经 relay 使用上游凭据（节点代码不接触）、请求期间线路续期、结束后线路退役且在途为 0；上游拒绝时线路同样安全退役，失败分类与本地基准一致。

开发中发现并修复：`wholeChannelSchema.sql`、`fullCrawlSchema.sql` 与 `discoverSchema.sql` 各自定义同名约束。重新执行较早的脚本会把约束收窄，已有 Discover 数据时迁移失败，没有时则悄悄删掉 Discover 许可。已把三处定义统一为同一放宽版本，并新增测试要求所有 SQL 文件中重复定义的同名约束完全一致。

另一发现（未改）：上游代理拒绝 CONNECT（relay 或 Rota 返回 502）时，错误为 `fetch failed`，本地与远程都不会被判为可换线的 `proxy_transport`，Rota 不隔离该代理，只由 BullMQ 重试。这是本地 Discover 既有行为，建议另行修复失败分类。

### P4 部署、接单控制与候选频道页面（完成，未上线）

中心：
- `deploymentAdmin.js` 支持第三种固定镜像 `discover`：按镜像选择 Discover 工作负载，Worker 编号为 `discover-N`，节点能力为 `youtube.discover-page.v1`；状态、接单开关、网络容量查询和退役都交给 Discover 执行器。Discover 镜像必须固定摘要，且不能与增量或全量镜像相同。
- `workerRetirement.js` 接受 `discover-N`，退役前检查该槽位没有未结束的 Discover 投递和未退役线路。
- `runRemoteNodeCenter.mjs`：`REMOTE_NODE_DISCOVER_DEPLOYMENT_ENABLED=true` 时要求 `REMOTE_NODE_DISCOVER_IMAGE`；`REMOTE_NODE_DISCOVER_EXECUTION_ENABLED` 必须同时开启部署（与全量一致）。只开部署不开执行时，节点可以部署并连上中心，但不会接到页面。

仪表盘：
- `nodeWorkerTypes.js`：`discover`（Query / 发现）可选择、可部署；每种类型带 `mode` 和 Worker 编号前缀。
- `collectDeployment.js` 用按类型的配置取代全量特例。Discover：镜像 `SERVER_NODE_DISCOVER_IMAGE`，开关 `SERVER_NODE_DISCOVER_DEPLOYMENT_ENABLED`，必须配置 NATS，不启用整频道传输，暂存挂载 `/var/lib/qy-node/discover-spool`，内存 768m、CPU 0.5、进程数 128。
- 节点端 `deployWorkers.py`、`removeWorker.py` 按同一组固定配置校验 Discover。
- 新增只读“候选频道”页面 `/candidates`（导航在“频道列表”之后）与统计接口 `/api/candidates/statistics`。候选表里还有约 40 万条旧库迁移候选，所以每次读取先取出 Query 批次（有 query_pages 的批次），再以数组参数限定批次、以来源字段限定 `youtube_search_discovery`。用子查询限定批次时，数据库估不出旧库批次占了几乎全部行，会全表扫描；改为数组参数后走批次索引（集成测试检查执行计划）。列表只取下一页判断是否有更多，不计数；统计经现有统计缓存和独立连接执行。
- `controlledWritePolicy.js` 并入线上版本的 Query 调度放行。

测试：
- 中心：`remoteDiscoverDeployment.postgres.integration.test.js`（镜像/编号/能力、节点类型冲突、重复登记、接单开关、仅部署时不可执行、忙碌与未结束投递阻止退役、退役完成）。
- 仪表盘：`candidateChannels.test.js`、`candidateChannels.postgres.integration.test.js`（旧库行排除、筛选、北京时间日期、分页、统计、执行计划），`nodeWorkerHealth.test.js` 新增 Discover 部署配方与安装器校验，部署与退役集成测试扩展到 Discover，`test_remove_worker.py` 新增 Discover。
- 本机启动仪表盘连测试库，`/candidates` 页面、筛选和统计接口返回正确。
- 既有失败（与本改动无关，未改动代码上同样失败）：`remoteWorkerRetirement.postgres.integration.test.js`、`serverNodeWorkerDeployment.postgres.integration.test.js` 仍在调用已移除的按数量接单接口。

### 下一步

P5 灰度（需确认后执行）：推送 Discover 节点镜像；中心迁移 `discoverSchema.sql` 并开启 Discover 部署与执行；仪表盘上线；退役 `query 备用` 的 20 个增量 Worker 并删除登记，按 Discover 类型重新登记、先部署 1 个 Worker；调高 Discover 暂停阈值；小批 Query 验证。
