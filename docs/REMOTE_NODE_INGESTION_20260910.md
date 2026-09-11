# 远程频道 Plan 接入（隔离实现）

## 范围与上线边界

中心继续拥有调度、业务执行代次、API fallback、Agent、Finalize 和发布。远程节点只通过 HTTPS 领取采集步骤、续租、提交结果；不提供数据库或 Redis 凭据，不创建第二个业务调度器。

本阶段增加独立模块、独立建表文件和隔离测试入口。**不修改现有 worker/controller、默认 schema.sql、Compose、Dashboard 部署或生产队列**。不会自动初始化用户节点、安装 Docker、分配 Rota slot 或派发正式任务。

用户再次明确：当前先搭建多服务器模式，不能改动现有增量和迁移流程。后续实现以节点接入、任务运输、归属校验和部署编排为边界；原有采集顺序、Clock 调度、迁移派发、重试/休眠/API 策略继续复用，不能为了远程执行顺便重写。

现已在隔离环境中复用原有增量 Plan runner，跑通 About、Video、组合计划、中心 Agent 登记及 API batch 续跑；新增 Rota 本机转发、中心配置读取，以及频道网络执行的持久绑定、自动授权/续租/收尾。正式 BullMQ/clock 派发映射、真实业务执行栅栏接线、节点真实身份会话和节点部署仍待接入；不能把隔离验证称为已经上线的远程增量 Worker。

用户确认的任务粒度是：**中心按现有 clock 生成一个频道的 Plan，不同服务器领取不同频道；分页、视频详情只是该频道任务内部的采集步骤**。不建立分页或视频详情的独立业务调度器，不让用户分别配置这些步骤。

## 协议与任务粒度

- 中心提交原有 `schema_version=5` 增量 Plan，能力为 `youtube.incremental.plan.v1`，唯一 work_key 为 plan_id + dispatch_generation。冻结的 task_mask、capacity、clock/policy/planner 版本原样保留。节点收到采集 Plan，中心执行上下文不接受节点覆盖。
- `youtube.about.v1` 是第一轮收发与落库验证契约，保留作协议回归测试；实际频道适配使用上面的整个 Plan。Agent-only Plan 留在中心，不产生远程采集任务。
- 管理侧登记节点 token 的 SHA-256 摘要、允许能力和并发上限；网络接口不提供开放注册或新增任务。每个执行进程使用独立本地 spool 目录。
- `POST /v1/work/claim` 带持久化 claim_id。领取响应丢失后，同一个 claim_id 可以取回同一租约；已失效的 claim_id 明确拒绝，不能静默领取另一个任务。
- `POST /v1/work/:id/heartbeat` 带 generation。租约采用中心数据库时间，过期后不能复活旧代次。
- `POST /v1/work/:id/results` 带 batch_id、generation、version、outcome、data/error。一个采集步骤一个终态结果。gzip 上传，限制压缩体和解压体大小。
- `GET /v1/receipts/:batch_id` 区分 received/applied/failed/cancelled，仅所属节点可读。
- draining 节点不能领新工作，但可续租、提交正在执行的工作；disabled 节点不能访问。

## 持久化与并发

独立 `remote_ingestion` schema 保存节点、频道任务、内部请求与结果。每个结果只存一份压缩载荷，使用 PostgreSQL 同步提交后才回复 received，避免先写对象存储再写索引的双写窗口。这是有大小和积压上限的收件箱，不是额外长期搜索快照；生产保留/清理策略尚未启用。

领取使用行锁和 SKIP LOCKED；节点行锁保证多进程不突破节点并发上限，频道 scope 唯一索引阻止同频道同时分给两个节点。任务行持有 generation、node_id、lease_until。领取请求记录独立保存，防止旧领取请求重放产生新任务。执行租约可以重投，外部 YouTube 请求不承诺只执行一次。generation 是所有权代次，失败租约计数单独保存；API 返回后的正常续跑不消耗失败重试预算。

接收时锁定任务并验证归属、generation、未过期租约。相同 batch_id/相同原始 JSON 字节可重复确认；不同内容或不同身份冲突。初版收发契约的接收与任务 received 状态同一事务提交；频道 Plan 的内部请求保存到 channel_commands，收到结果只确认该请求，节点不能自行把频道标为完成。

初版结果处理器锁定 received 任务，通过同一个 PostgreSQL client 执行业务写入和任务 applied 更新，一起提交。处理失败使用保存点回滚业务副作用，并记录有上限的重试/backoff；耗尽保留 failed 和原载荷。处理函数禁止网络、跨库写入或自行 COMMIT。这只解决中心接收阶段的原子性；发布跨库沿用现有机制。

频道 Plan 由现有 `IncrementalChannelRunner`、`IncrementalRunStore`、`executeIncrementalYoutubeJsVideo` 在中心推进；只将 openChannel/scanUploads/fetchYoutubeJs 请求交给同一远程所有者。内部请求不会另建 BullMQ 任务，远端永远不执行 SQL。原有 Video finalized checkpoint 继续负责幂等；新的适配器把原有 About writer 与 About 域完成标记放入同一事务，避免提交确认丢失时重复写观测。已有域完成状态不能被连接错误降级。最终 run 完成与远程 task 完成也在同一事务中提交。

每次中心事务同时验证远程代次、协调者租约和显式注入的业务执行栅栏。网络等待期间不占用数据库事务。中心协调者崩溃后可以接管，旧协调者无法继续提交。已有接收证据可以重放，不要求节点重新抓取已回传内容。

API 兜底调用既有共享模块，request/partial detail 仍进原有中心表。遇到 VIDEO_API_PENDING 时远程任务进入 received + waiting_central，释放节点容量；既有 Video 域保持未完成。中心 `resumeAfterApi` 验证请求归属和非 pending 状态后重新开放同一个 Plan；恢复沿原有视频 checkpoint 使用 API 结果。该调用尚未接进正式 controller 的自动扫描。国家重检的交接证据同样保留；第五阶段已将本次出口国家交给中心和节点会话，跨执行的国家重检交接仍待接通。

取消会锁定任务；已取消或旧代次结果不得写业务表。正式桥接还必须在同一事务里验证现有 business run/candidate/clock 执行栅栏，不能只相信新的网络租约。

## 节点断线与资源上限

执行器一次处理一个步骤。领取请求及上传结果先写本地临时文件、fsync、原子 rename、目录 fsync。只在中心明确持久接收后删除结果。重启先回传已有结果，再恢复领取请求。响应丢失不重新抓取已经保存在本地的结果。

永久拒绝的载荷保留在本地 blocked 文件中并停止新领取，便于排查，不把它当采集成功。临时故障退避重传。spool 限额包含 pending/blocked/临时文件；满额停止领取。SIGTERM 停止新领取，等待当前步骤及回传；断网达到停机等待上限后保留 spool，供重启恢复。

中心限制解压后 4 MiB、压缩后 1 MiB、并行 HTTP 请求、节点租约数及待处理积压。先对领取施加背压，已经领取的合法结果仍允许交回。收件箱的保留/清理策略需在正式上线前接入，不默默删除失败证据。

## 网络与后续接入

正式入口需要独立 HTTPS 路由，节点 token 通过密钥文件分发；不放入任务体、日志或 Dashboard settings 明文。第一阶段测试入口只绑定 loopback，要求明确的专用测试数据库名。

Rota 原有控制与转发通过进程内状态关联，不能直接多开共享 Rota 数据库。第三阶段已增加独立、无数据库的本机转发入口；第四、五阶段补充配置读取和持久授权接线。中心/远端真实网络身份、休眠国家交接和真实国家切换验证完成前，不开放远程采集部署。频道测试使用可重放 YouTubeJS snapshot/detail；转发测试使用本机模拟代理，不访问真实 YouTube，也不代表跨主机网络性能验证。

## 实现入口

`services/qybullmq/src/remoteNodes/`：

- `store.js` / `schema.sql`：中心管理、租约、持久收件箱和原子处理事务。
- `gateway.js` / `client.js` / `protocol.js`：鉴权、领取/续租/回传/收据协议及大小限制。
- `executor.js` / `spool.js`：单进程执行、退避、磁盘持久化和停止；单目录只能归一个执行进程使用。
- `processor.js`：与 HTTP 接收独立的处理循环，最多 4 路事务，停止时完成正在处理的事务。
- `aboutExtractor.js` / `aboutIngestion.js`：可序列化 About snapshot 和既有业务 writer。真实 session 和事务内 business fence 均必须显式注入，没有放行默认值。
- `channelPlanContract.js` / `channelPlanStore.js`：冻结现有 Plan、频道互斥、中心协调者栅栏、同一节点内部请求以及 API 续跑。
- `incrementalCoordinator.js`：组合原有 runner/About/Video/Agent 和远程网络请求，原有采集模块文件未修改。
- `channelPlanExecutor.js` / `channelWire.js`：远程单频道会话、整个 Plan 的停止/恢复和错误证据传输。
- `channelRouteStore.js` / `routeSchema.sql`：中心持久保存节点 slot、频道执行和已有 Rota Task 的绑定，签发授权并记录停止确认。
- `channelNetworkSession.js`：节点申请/续租代理、本机断网收尾和本地恢复记录。
- `rotaChannelRuntimeAdapter.js`：通过原有 RotaSlotAdapter 的 identityRuntime 接口等待节点停网；原有 Task/预算管理仍在原适配器。

频道接收端启动脚本是 `scripts/runRemoteNodeGatewayIsolated.mjs`，只接受 `REMOTE_NODE_TEST_DATABASE_URL`，连接后验证数据库名必须为 `remote_node_ingestion_test`，只监听 127.0.0.1；不读取生产 `POSTGRES_*`，不自动登记节点或产生任务。该入口只启动接收端；处理器和执行器在集成测试中独立运行，正式编排尚未接入。本机转发的独立入口见第三阶段。

## 第一阶段验证记录（2026-09-10）

独立 PostgreSQL 测试库 `remote_node_ingestion_test`，loopback HTTP，临时磁盘 spool。最终使用现有生产镜像 `qy-allpachong/qybullmq:migration-resume-20260909` 的 Node 20 运行，限制 0.5 CPU、384 MiB，代码只读挂载，一次性容器退出后删除。

**32 项测试通过，0 失败，0 跳过**（包含 17 个数据库/HTTP 子测试、套件计数、5 个协议/磁盘测试、9 个既有 About 回归测试）。验证内容：

- 同一个领取请求重发不多领任务；并发领取不突破节点容量；禁止领取未授权能力；中心上下文不发送给节点。
- 过期代次、其他节点写入、冲突批次、跨节点查收据均拒绝；相同载荷重复提交可再次确认。
- 接收与应用状态分离；上传响应丢失后重建执行器并读取原 spool，只重传、不再采集。
- 真实 PostgreSQL 业务写入后注入异常，写入回滚；实际 COMMIT 完成后模拟确认丢失，重试不会再次执行业务写入。这里的连接/响应丢失采用故障注入，不冒称真实机器宕机演练。
- 并发处理、重试上限、执行租约耗尽、drain、中心取消、积压背压、处理器停止，以及远程解析失败均按各自状态处理。
- 压缩炸弹、超大结果、无效确认被拒绝；磁盘额度不足停止领取；过期结果保留为 blocked 证据。
- About 使用可重放 snapshot，经执行器→spool→HTTP→PostgreSQL 收件箱→既有 `buildAboutObservation` / `recordAboutObservation`，真实写入 `crawler.channels`、观测和指标快照。核验订阅 1,234、总播放 98,765、视频 42；重复处理未新增快照。
- 中心 run 改为 failed 后，即使网络租约有效也拒绝应用；伪造其他频道的 snapshot 不会写入频道数据。

上述第一阶段未把视频/分页纳入频道适配；第二阶段扩展验证见下节。两轮均未访问真实 YouTube、未派发正式频道、未验证跨主机吞吐或 Rota 国家切换，不能据此启用页面里的正式远程 Worker 部署。

本地复跑（需先准备同名独立测试库）：

```bash
cd services/qybullmq
REMOTE_NODE_TEST_DATABASE_URL=postgresql://postgres@127.0.0.1:55447/remote_node_ingestion_test npm run test:remote-nodes
```

该命令会应用 schema 并清空 `remote_ingestion` 测试表，且先检查数据库名。默认 `npm test` 未配置测试 URL 时会跳过 PostgreSQL 集成部分。

## 第二阶段：频道 Plan 适配验证

仍使用同一个独立测试库和生产镜像的 Node 20，0.5 CPU / 384 MiB 临时容器，不接触生产任务。

先完成 **83 项针对性回归测试，0 失败、0 跳过**，涵盖既有 Plan、ChannelRunner、About、视频 planner/checkpoint/detail 策略和远程协议。随后补上 About 提交确认丢失保护，重跑频道适配的 **11 项测试（10 个场景和套件计数），全部通过**。原有采集源码未修改；既有文件差异仍只有 package.json 的两个隔离命令。

- 两个模拟节点通过真实 HTTP 领取不同频道；About-only 不请求视频列表/详情，Video-only 不写 About 观测。
- Video-only 使用原有检查点与业务 writer，验证新视频播放 321、点赞 12、关闭评论 0，最终 checkpoint finalized。
- About + Video + Agent 共用一个远程会话；Agent 进入真实中心 agent_refresh_requests，频道等待 Agent。
- Agent-only 留在中心；不能在同 work_key 下改变 clock_version 或 task_mask；同频道不能被两个节点/协调者同时接管。
- 节点不能自行完成频道、查询其他节点请求或提交不存在的请求；网络错误/必需字段错误的原有分类跨传输保留。
- 内部请求已提交、HTTP 确认丢失时，从 spool 恢复，未重新请求 YouTube。
- YouTubeJS 必需字段错误仍按既有策略尝试 3 次后登记 API batch；API pending 释放节点，返回后在新节点继续原 Plan，详情请求次数仍是 3 次，失败租约计数仍是 0。
- About 写入提交确认丢失的恢复案例，验证域完成标记和观测写入不会分离，恢复不产生第二条观测。

正式接入前还需要：把业务栅栏接到真实 clock/managed-job 所有权；把频道适配和 API 续跑调用接入现有 controller；远程 Rota/身份/国家重检；真实节点端到端采集和吞吐验证；收件箱保留策略；页面 Docker/Worker 部署流程。当前内部请求采用短轮询，生产规模的通知/批量效率尚未验收，不据此预估扩容吞吐。

## 第三阶段：中心配置下发与节点本机转发

用户确认：保留中心 Rota 的选代理、任务观察、重试预算、国家匹配；只补必要的配置下发和节点本机连接能力。当前 Worker 的 Assignment 返回 `proxy_user`、租约、路由代次、出口国家；`rotaSlotAdapter` 把它们拼到 `ROTA_PROXY_BASE_URL`，默认经过中心 Rota 的 8000 端口。它没有从 Assignment 直接获得上游地址/协议/凭据。

新增实现没有编辑中心 Rota 现有文件，也没有接入 `cmd/server`、启动迁移或运行中的 Worker：

- `services/rota/core/internal/nodeforward/`：无数据库的受控 CONNECT 转发；没有选代理、池扫描、任务调度、API fallback 或重试计数。
- `services/rota/core/cmd/node-forward/`：独立启动入口，只允许 loopback 监听；公钥和本地控制 token 来自文件，token 文件要求仅文件所有者可读写。SIGTERM 显式关闭已劫持隧道。
- `src/remoteNodes/routeGrant.js`：中心签发器。每次签发/续租都调用必需的 `authorize`，核对返回的节点、slot、频道任务代次，并把有效期限制在频道租约和 Rota 租约结束前。`authorize` 必须负责真实所有权校验和持久化路由 epoch/token；目前没有接入生产授权查询，也没有开放签发 HTTP 入口。
- `src/remoteNodes/localRotaClient.js`：节点本机控制客户端。应用前核对频道租约和本机启动标识，应用后核对完整确认；只有转发端验签成功后才返回本机代理 URL 和出口国家。

数据流为中心签发所分配代理的配置，经受保护的控制链路交给节点；采集连接则走 `Worker -> 本机转发 -> 上游代理 -> YouTube`。下发一条授权路由，不下发完整代理池，也不下发数据库/Redis 凭据。Ed25519 签名用于验证授权，**不加密配置**，因此配置传输仍须使用 HTTPS；本机控制只在同机 loopback 使用，不能公开映射端口。配置不能进入普通任务日志或长久收件箱。

授权绑定 node、启动随机数、slot、epoch、频道 task/generation、route、network identity、国家和过期时间。启动随机数每次重启变化，旧授权不能复活。新 epoch 必须使用新的本机代理 token；旧 epoch、同 epoch 改上游/任务、跨节点配置均拒绝。续租只延长同一路由的有效期，保持当前连接；撤销和过期关闭现存连接并取消未完成握手。授权计时使用单调时钟期限，避免节点时钟回拨让旧隧道无限存活；正式初始化仍需校验节点时间偏差。

切路由时的强制关闭是最后的隔离保障，不能代替现有 `quiesce -> Task complete -> Route change` 顺序。正式接线仍须遵守执行中的 Slot 路由固定规则，并把相同的国家/身份上下文同时交给中心 runner 和远端 YouTubeJS session。节点转发不自行决定切哪个国家；`egress_country` 是中心所选代理的元数据，不是页面语言设置，也不是本模块自行做出的地理检测结果。

支持 HTTP/HTTPS、SOCKS4/4a/5，并通过 Rota 既有 `sharenode` 模块复用 VLESS、VMess、Trojan、Shadowsocks、Hysteria2 的解析和拨号。标准代理握手使用可取消拨号，避免撤销时卡在旧同步 SOCKS/CONNECT 握手中。仅转发允许的 YouTube/Google 域名到 443，不提供任意 HTTP、直连或回退中心转发。HTTPS 上游要求有效证书，与中心旧实现的宽松证书策略有差异；不能据此宣称现有全部上游已兼容，正式接入前必须核对实际协议和证书。

### 验证范围

使用本机模拟上游，验证实际 TCP/CONNECT 路径，不请求真实 YouTube，不使用正式 Rota pool/slot：

- US/BR 两个模拟上游各返回不同标识，验证切换前后实际连到不同上游、旧隧道关闭，旧完成/撤销请求不能影响新路由。US/BR 只是测试标识，不是实际国家 IP 测试。
- 相同激活请求可重复确认；续租保留隧道；撤销、过期、重启后旧配置均不能恢复旧路由。
- 错签名、错本机 token、其他节点/频道/代次、无效 slot、过长有效期、修改中的同代路由都被拒绝。
- 连接数限额包括正在拨号的请求；撤销会取消无响应上游的 CONNECT 握手。
- SOCKS4/4a/5 使用真实本机模拟协议握手验证目标/认证信息；HTTP 测试覆盖首包缓冲不丢失和上游认证隔离；无效 HTTPS 证书拒绝；CONNECT 响应头有大小限制。共享节点协议尚未进行真实上游连通测试。
- 临时容器使用生产镜像的 Node 20，0.5 CPU / 384 MiB，代码/测试 Go 二进制只读挂载，完成 `Node 签发 -> Go 验签/转发 -> 模拟上游 -> 回传`，并验证续租和撤销。

Go 普通测试覆盖 8 个顶层用例（另含 3 个 SOCKS 子用例）。Node 20 本轮 9 项测试通过，0 失败/跳过，含 4 项新增签发/接线测试和 5 项既有远程协议回归。Go race detector 尝试因环境缺少 C 编译器而未能编译，不能将普通并发行为测试称为通过了 race detector。

复跑入口：

```bash
cd services/rota/core
CGO_ENABLED=0 go test ./internal/nodeforward ./cmd/node-forward
CGO_ENABLED=0 go build -o /tmp/pachongsys-node-forward-test ./cmd/node-forward
cd ../../qybullmq
REMOTE_NODE_ROTA_TEST_BINARY=/tmp/pachongsys-node-forward-test npm run test:remote-rota
```

没有设置 `REMOTE_NODE_ROTA_TEST_BINARY` 时只运行签发/客户端单元测试，跨语言测试明确跳过。测试二进制位置不用于正式部署。

### 接下来仍需接通

真实中心 Rota 配置授权与节点 slot 映射；频道协调者/执行器自动申请、续租和释放路由；身份与指纹配置的节点适配；中心和远端一致的休眠国家重检；真实节点和真实 YouTube 验证。上述事项完成前，不开放 Dashboard 正式部署远程 Worker，也不把本阶段当作已能运行真实增量任务。

## 第四阶段：真实 Rota 配置读取（默认关闭）

已增加 `proxycontrol.Manager.ReadRemoteRoute`，读取现有 Rota 表中的真实上游配置，不再要求调用者自行查询代理池。这个方法是单条有 2 秒截止时间的查询，不写数据，也不取得全局代理调度锁。

读取条件同时覆盖 Slot、Lease history、活跃 Task、已分配 Proxy：Worker/instance、Slot 当前租约与历史租约、route_generation、Rota task_id、business_run_id、job_execution_id、workload、身份策略和 network_identity_key 必须一致；路由必须已就绪，Task 必须活跃，两个租约和代理身份有效期都必须有至少 3 秒余量。返回有效期取三者最短值。接口不能指定任意 proxy_id、选代理或开始任务。

遵守原有运行中任务的路由归属规则：后台健康标记本身不会夺走活跃 Task 的路由权限。任务完成/解绑、租约释放/过期、路由转移或身份变化则拒绝读取。返回的是读取时的一致快照；它不承诺在 HTTP 返回之后 Rota 状态永远不变，正式签发时仍需复核频道租约，换路由仍须先停止请求、撤销旧授权再完成原 Task。

新增 `api.NewRemoteRouteHandler` 和显式的 `Server.EnableRemoteRouteRead`。默认 `cmd/server` 不调用它，默认路由仍为 404。显式启用后路径为 `/internal/v1/remote-route`，要求与普通 Worker 控制 token 不同的中心专用 token，且部署时应仅允许中心内部访问。没有编辑现有 `server.go`、配置默认值、Compose 或普通 `proxy-control.Interface`，没有启用线上接口。

普通 `RemoteRoute` JSON 和格式化日志不含上游凭据；只有这个专用 handler 显式输出上游协议、地址、用户名和密码。响应使用 `Cache-Control: no-store`，异常响应不返回数据库错误中的敏感内容。读取权限只给中心协调器，不给远程 Worker。

`src/remoteNodes/rotaRouteSource.js` 是中心专用调用客户端，使用已保存的 Rota Task Fence 发起请求，核对响应归属、身份字段和协议；拒绝重定向、异常超大响应及过期结果。按照 Rota 的剩余租约时间从本次请求开始计时，保守扣除往返延迟，避免中心与 Rota 时钟偏差延长授权。该客户端不调用 claim、beginTask、切 IP 或重试预算接口；原有这些流程仍归 Rota 管理。

### 验证

- 独立 PostgreSQL 数据库 `remote_node_ingestion_test`，新增测试在任何建表操作前核对实际数据库名。复用既有测试工厂，在随机 `rota_proxy_control_test_*` schema 中运行原有同步资源、分配代理和 BeginTask，再调用真实配置读取方法；结束删除这些临时 schema。
- 4 个数据库测试组通过，其中一组包含 11 个过期/解绑/身份不一致子场景；另外 2 个接口测试通过。完整对比读取前后的任务、Slot、租约、预算与命令收据，确认读取不产生写入。
- 测试确认只能读取 assigned 代理，不能读 reserve 代理；普通 JSON/日志不泄露上游认证信息，只有专用接口可返回；普通 Worker token 被拒绝；默认不开启接口。
- Node 20 新增配置客户端测试与既有签发/本机转发集成一起运行；读取测试使用接口响应夹具，真实 SQL 单独由上面的 PostgreSQL 测试覆盖。不能把两部分验证称为已经完成真实 Rota 到远程服务器的端到端采集。

复跑 Rota 配置读取：

```bash
cd services/rota/core
ROTA_TEST_DATABASE_URL=postgresql://postgres@127.0.0.1:55447/remote_node_ingestion_test \
  CGO_ENABLED=0 go test -run '^TestRemoteRoute' ./internal/proxycontrol ./internal/api
```

后续尚需把中心保存的频道任务与 Rota Task/节点 slot 建立持久映射，将此客户端接到签发器的 `authorize`，以及让节点频道会话自动申请/续租/释放路由。配置读取能力完成不等于这套远程 Worker 已能部署运行。

第四阶段最终检查：Node 20 的 7 项配置读取/签发/转发测试全部通过，0 失败、0 跳过；原有 Rota BeginTask 幂等、live-lease、国家重检与 proxy-control 接口的针对性回归也通过。Go 格式、JavaScript 语法和已跟踪文件差异检查通过。临时 Node 容器自动删除，没有提交、推送或部署本阶段代码。

## 第五阶段：频道执行与 Rota 授权的持久绑定

中心显式登记节点本地 slot 与原有 Rota worker_id 的对应关系。登记/绑定仅提供给中心代码，不提供给节点 HTTP 接口。`routeSchema.sql` 是独立、默认不应用的建表文件；保存于同一 `remote_ingestion` schema，不修改生产默认 schema。

绑定记录冻结频道 task/generation、Rota Slot/Lease/Task/Execution 归属、身份策略及出口国家。读取 Rota 配置前后都验证频道租约与必需的业务栅栏，HTTP 等待不占用数据库事务。绑定以后不允许同一执行悄悄更换代理；slot 只有在旧绑定完成停网确认后才能复用。

每个 slot 只缓存一份 AES-256-GCM 加密的授权，用于相同请求的确认重传；上游配置比对保存带密钥的摘要，不把明文凭据写进绑定历史。续租覆盖缓存，释放清除缓存。密钥必须由中心显式注入，签名授权仍须通过 HTTPS 传输。节点 `network.json` 可能短暂包含待应用的授权，沿用 spool 的受限权限和空间上限，不能当普通日志输出。

节点会话自动申请授权、应用到本机 Go relay、定时续租；配置到期前中止网络执行。原有 RotaSlotAdapter 继续负责 Claim、Rota Lease 续租、BeginTask、观察、重试预算和 CompleteTask。新增 runtime 在 execute 内绑定，而不是在 acquire 中提前开放代理：即使绑定已 COMMIT 但返回丢失，原适配器仍会进入 quiesce，按频道执行查找已提交的绑定并收尾。

停止遵循以下顺序：

1. 中心设置 stop_requested，停止新的代理授权。若从未签发授权，可直接标记未启动收尾。
2. 已签发授权必须由节点调用本机 `/v1/retire`。本机关闭隧道、取消未完成握手，并等待处理中连接数归零；该操作不需要中心在线。
3. 节点持久保存并回传停止确认，中心标记绑定 retired。确认丢失时只重传确认。
4. 中心 runtime 观察到真实的零连接确认，原有适配器才调用 CompleteTask。等待超时会抛错，不伪造成功或零连接。

节点重启优先恢复旧授权/待回传结果；中断后的清理意图也持久保存。清理成功后，closed 标记阻止旧执行继续领取/续租，待中心改变状态或租约失效后才允许新代次。旧释放确认重复到达时只确认旧记录，不能停止新代次。同一本地进程启动标识、相同 epoch 的续租保留连接；新 epoch/启动标识不能恢复旧授权。

### 验证记录

同一独立 PostgreSQL 测试库，真实 HTTP 网关和本机 Go relay，使用可重放 About 内容及本机模拟上游。Rota 配置 SQL 的真实读取沿用第四阶段验证；本轮 Rota HTTP 控制响应使用夹具，调用的是未修改的原有 RotaSlotAdapter，不能把它称为真实 Rota 服务到远程主机的完整测试。

- 初轮远程协议、频道、签发和本机转发共 50 项通过。随后新增停止、绑定失败和恢复用例；最终在生产镜像的 Node 20 中运行网络集成与原有 RotaSlotAdapter 回归，**51 项通过，0 失败、0 跳过**（15 个网络场景、1 个父测试、35 个既有适配器测试）。临时容器限制 0.5 CPU / 384 MiB，代码只读挂载，退出自动删除。
- 完整 About Plan 自动申请/续租代理，通过本机 relay 建立 CONNECT，使用原有 runner/writer 保存订阅 1,234、总播放 98,765、视频 42，完成后收到零连接确认。
- 原有适配器执行完整远程 Plan，刻意延迟节点释放确认，验证尚未确认时没有调用 CompleteTask；收到确认后才结束 Task、最终释放 Lease。
- 验证签发响应重放、重建中心 store、并发取消、身份不一致、停止前未签发授权、停止后拒绝续租、活动连接实际关闭、绑定 COMMIT 确认丢失、节点重启、授权未应用、激活/释放响应丢失。
- 验证续租失败不会把频道标成功；中断后旧执行不能反复续租，下一代执行可正常使用新 epoch，重复的旧停止/释放不会影响新连接。
- Go 本机转发普通测试通过，包含新增本地 retire 用例（9 个顶层用例，另有 SOCKS 子场景）。本轮没有可用的 C 编译器，未完成 race detector 验证。

复跑本阶段：

```bash
cd services/qybullmq
REMOTE_NODE_TEST_DATABASE_URL=postgresql://postgres@127.0.0.1:55447/remote_node_ingestion_test \
REMOTE_NODE_ROTA_TEST_BINARY=/tmp/pachongsys-node-forward-test \
node --test --test-concurrency=1 test/remoteChannelNetwork.postgres.integration.test.js test/rotaSlotAdapter.test.js
```

未设置两个测试环境变量时网络集成明确跳过，不能把跳过当通过。测试先核对真实数据库名，再应用独立 schema；共享测试采用 advisory lock 串行，测试重置的 CASCADE 仅用于该测试库的 remote_ingestion 表。

### 后续接线边界

这一轮完成的是一个频道网络执行的绑定、自动授权和可靠收尾。runtime 目前固定绑定一个 lease；原有 Rota 在同一 BullMQ 执行内换代理/国家时会开始第二个 Task，正式桥接还须把这个交接映射到新的远程执行代次，保持原有预算与失败计数，不能直接复用旧绑定。现有适配器换代理回归通过，不代表这个跨节点交接已经实现。

仍需实现真实 clock/managed-job 事务栅栏、生产派发与 API 续跑接线；节点真实 YouTubeJS 指纹/profile/session 适配（withRuntime 必须注入，没有直连默认值）；向远端传递完整的国家重检状态并验证无备用国家节点时休眠；Dashboard Worker 配置/Docker 部署、收件箱保留策略与实际节点测试。旧 HTTPS 代理证书兼容性也须用实际节点验证。

上述新代码未被正式 worker/controller/Compose 导入，Rota 配置接口仍默认关闭，未自动登记节点或派发频道。本阶段没有提交、推送、部署、重启线上服务，也没有改动现有迁移队列和原有采集模块。

## 第六阶段：用已有 Clock 和业务记录校验远程任务

新增中心模块 `remoteNodes/incrementalBusinessFence.js`，读取既有 `feature_clock.daily_channel_plans`、`dispatch_outbox`、`crawler.business_run_bindings`、`channel_runs` 和 `channel_execution_attempts`。这里没有生成新 Clock、重算 Plan、登记采集尝试或修改上述业务记录。

`enqueueRemoteIncrementalJob` 在一个事务里登记远程运输记录并校验真实业务归属。校验失败会回滚未提交记录，节点看不到任务；相同任务重传返回原记录。锁顺序先远程任务、后业务记录，与接收/协调事务一致。原 Plan 的频道、任务 mask、版本、容量、派发 generation 和 hash 必须匹配；原有业务绑定必须有效，执行记录必须属于这个 run/频道/job，且仍在运行，没有更新的 Rota attempt。

`assertRemoteIncrementalBusinessFence` 可注入已有远程协调器及路由授权 store，落库前使用同一事务再核验。原有 Rota attempt 的 Worker/instance/slot/Task/network identity 也与待绑定的 Rota 配置比对。协调器上下文直接使用真实 attempt_id，远程租约 generation 单独承担运输所有权，不再拼进业务 attempt_id。

允许原流程的正常收尾次序：Feature 可能先消费已提交的 About/Video 观测，再更新 Plan 状态。只有已有对应域完成证据，才允许当前执行完成最后事务；`channel_dormant` 取消还须有这个 run 的 Video 休眠证据。手动取消、已停止/被更新执行取代或业务记录不匹配会拒绝写入。终态 Plan 不能用该入口创建新远程任务。

这不是正式 BullMQ 锁的替代品。正式装配仍须由持锁的中心执行者调用；API 等待结束、换代理、换国家时的 execution 交接尚未接入。当前严格校验只接受运行中的原始 execution，不能让 API 续跑直接复用已关闭的 attempt，也不能因此改写原有重试计数。真实节点身份/profile 适配与节点启动入口、Dashboard 部署流程仍待完成。

### 本轮测试发现与修复

全部验证在 `remote_node_ingestion_test` 进行。新测试应用原有 crawler/feature schema，并调用原来的 `ProxyBusinessRunPreparer`、`IncrementalRunStore` 和 `BrowserProfileStore.beginAttempt` 创建测试业务记录，没有伪造业务栅栏回调替代校验。About 内容仍是夹具，未请求真实 YouTube。

- 校验变更 mask/派发代次、取消 Clock、结束或替换 execution、不同频道/策略、终止业务绑定；验证失败不能登记远程任务或写业务数据。
- 原有完整 About Plan 经 HTTP/节点执行器和真实业务归属校验落库；提前消费观测后仍能完成最后收尾。并发重复接入与受保护写入返回同一个运输记录，事务异常一起回滚。
- 联合测试暴露新 spool 的并发问题：续租保存与领取文件删除同时发生，容量扫描会报 ENOENT。已将单个 spool 实例的空间检查/保存/删除串行化，并验证并发写入合计不能突破磁盘上限，失败后仍可继续收尾。每个 Worker 仍必须独占目录。
- 联合测试暴露新接入层的外键死锁。任务收尾持有 task 锁、需要 nodes 外键的 KEY SHARE；授权读取持有 nodes 的 FOR UPDATE、等待 task，形成循环。固定并发顺序的测试先实际报 `40P01`。只将新接入模块的 nodes 锁改为 `FOR NO KEY UPDATE`，保留节点容量操作互斥、允许外键检查后，复现及 17 项网络测试全部通过。没有改动原有采集模块，也没有增加靠重试掩盖死锁的逻辑。

最终联合回归使用生产镜像的 Node 20、0.5 CPU / 512 MiB 一次性容器，源码只读挂载：**68 项通过，0 失败、0 跳过**，覆盖远程协议/spool、业务归属、网络生命周期、原有频道 Plan 和 API 续跑、接收落库。测试日志保存在本机 `/tmp/remote-multiserver-final-tests-20260910.log`；死锁修复前后的最小场景日志为 `/tmp/remote-lock-repro-20260910.log` 与 `/tmp/remote-lock-fixed-20260910.log`。未留下调试日志代码。

现有已跟踪文件只增加 package.json 的 5 个隔离入口/测试命令；本轮实现均在新增 remoteNodes 模块、测试和说明文档中。未提交、推送、部署或重启正式服务。

复跑新增业务归属测试：

```bash
cd services/qybullmq
REMOTE_NODE_TEST_DATABASE_URL=postgresql://postgres@127.0.0.1:55447/remote_node_ingestion_test \
npm run test:remote-fence
```

### 今日 Clock 只读核实

查询时间为 2026-09-10 08:24 UTC（北京时间 16:24），数据库 `newcrawler_crawler`。使用 READ ONLY 事务、显式 UTC 和 10 秒 statement_timeout；未调用启动/派发/恢复函数。

- 今日 Plan 共 13,697：13,695 succeeded、1 failed、1 cancelled；派发 outbox 全部 published。
- 没有今天或以前仍 planned/dispatching/dispatched/running 的 Plan；没有到期未生成计划的活跃频道，也没有到期未处理的休眠复查。
- Carlos Prates（`UCuEU09Wp7bcAm3IWVWqcsTA`）的采集 run 为 done、About/Video 都完成；频道转 dormant，Plan cancelled，error_code 为 channel_dormant，复查日 2026-11-18。
- Márcia Romano Vendas（`UCmMOWVJczkIG4vNkHvA8gzg`）被 YouTube 因 Community Guidelines 移除，频道 removed、Plan/run failed。该结果不是仍在执行的任务。

这些是查询时的状态，不能据此保证后续人工操作或新入库频道不会改变当天状态。本轮没有重新派发今天的正式 Plan。

## 第七阶段：Dashboard 节点环境准备

新增固定环境脚本及页面手动入口，复用已有 SSH/Beszel 登记，准备 Docker、Compose 与节点目录；进度持久保存，允许失败重试和完成后重新检查。独立脚本不启动 Worker，不参与频道采集或调度。实现、隔离测试与实际部署的剩余边界见 [节点运行环境准备](SERVER_NODE_RUNTIME_20260910.md)。本轮仍未部署真实节点或正式服务。

## 第八阶段：独立节点进程、接入镜像与部署方案

增加 `services/remote-node/Dockerfile`、`runRemoteNodeConnection.mjs` 和中心 `RemoteWorkerConnectionStore`，将原来的无数据库 Go relay 包装成可独立启动、通过 HTTPS 核实部署身份、上报心跳并接受健康检查的节点接入镜像。当前唯一模式为 `connect_only`，无采集执行器且不调用 claim，不能当作已接通真实增量采集。

中心只接受事先登记的 node/slot、deployment_id 和配置摘要；连接期内的其他进程不能覆盖原进程。新的 `workerConnectionSchema.sql` 只应用到专用测试库，不加入生产默认 schema。Dashboard 增加已保存计划的只读部署方案入口，固定镜像摘要、独立容器资源和密钥文件引用，不在查询时产生部署或派发。

多阶段镜像实际构建完成；使用真实 Docker Compose、两个容器、真实 Go relay、临时 HTTPS 中心和专用 PostgreSQL 数据库，完成连接、健康检查、断线重连、重复实例拒绝及正常停止验证。确认测试期间没有领取请求、原频道任务记录不变、日志不包含 token。临时容器/网络/登记完成后清理。连接单元/协议/数据库 13 项通过，Dashboard 8 项通过，浏览器新增只读预览验证通过。详细复跑方式和上线边界见 [节点接入镜像](../services/remote-node/README.md)。

仍待接线：完整采集镜像和节点 YouTubeJS 身份会话、中心知道每个频道所属的具体远程 slot、原 Rota 切代理/国家和 API 续跑的执行交接、中心生产网关登记与页面实际部署动作。本轮没有修改既有 worker/controller/Clock/迁移流程，也没有将连接状态冒充采集 Worker 就绪。

## 第九阶段：真实 YouTubeJS 会话与具体 Worker 归属

新增 `remoteNodes/incrementalWorker.js`，将原 `RemoteChannelPlanExecutor`、本机 Rota 会话和真实 YouTubeJS 运行环境组装在一起。节点不再需要自行实现 `withRuntime`。一个进程对应一个 Worker/slot/独占 spool，一次持有一个频道 Plan；使用原 `openYoutubeJsChannel` 和 `fetchYoutubeJsVideoDetail`，没有复制分页、详情解析或重试规则。

中心登记 network slot 后，领取必须带具体 slot。任务和领取回执持久记录 `worker_slot`；相同领取 ID 可重放，换 slot 重放会被拒绝。同一 slot 的并发领取只会得到一个频道。上一代仍有活跃授权，或已过期但没有确认网络收尾时，不接新任务；已过期且旧绑定确认退休后，可以恢复原任务的新代次。该改动只在新增远程运输表内，不修改原 BullMQ 派发。

`RemoteYoutubeSessionStore` 冻结中心已有的 YouTubeJS profile、visitor data 和 Cookie，绑定具体频道执行、slot、relay boot、route 与 identity。它不创建执行尝试或替换浏览器身份。真实业务栅栏存在时，profile 必须匹配原 `channel_execution_attempts` 的 profile group/revision/client ID。配置仅保留 YouTubeJS 客户端，不传 yt-dlp 配置、数据库密码或 Rota 管理凭据。会话和最终回执在测试库内加密保存。

中心使用 `youtubeSessions.bind(...)` 或 `createRemoteRotaChannelRuntime` 的 `youtubeSessions/youtubeSession` 参数，先冻结会话再允许网络授权。冻结未完成时授权返回可等待的 503；节点不会先开始采集。节点通过 HTTPS 会话接口读取配置，使用原 FingerprintGateway 和 YouTubeJS，在本机 Rota 的授权代理下运行。运行结束先等待已进入的请求，再释放 YouTubeJS、关闭指纹网关，最后回传 Cookie/请求统计并退休网络。租约取消、采集异常和进程中断不提交成功 Cookie。

`youtube-session.json` 保存最终回传前的状态，权限沿用 spool 的 0700 目录/0600 文件。丢失确认时重传完全相同的回执，不再次采集。进程重启发现没有完成回执的旧会话时，上报 interrupted；该浏览器会话不允许重新打开。一个进程内同时启动第二个频道会被拒绝。

中心的 `createRemoteYoutubeCheckpointConsumer` 使用原 `BrowserProfileStore.checkpointCookies` 和原 Cookie 加密方式写回浏览器状态。网络必须确认退休，中心任务必须成功应用，原 Clock、run、attempt、profile 仍须匹配；旧执行不能覆盖新执行。浏览器写入、采集统计和已应用标记在同一个事务中提交，确认重放不重复写入。它不代替持锁中心执行者完成 `finishAttempt`；最终执行汇总仍由原执行生命周期负责。

原远程业务栅栏新增一个仅供最终浏览器 checkpoint 使用的可选参数 `allowCompletedRun`：仅接受自身 task 已应用且指向同一 run、同一完成状态、所有应执行域已有完成/部分完成/Agent 排队证据的 `done` 或 `waiting_agent`。默认参数没有变化，普通采集写入和新任务接入仍要求业务 run 正在运行。失败及 API 等待的任务不应用成功 Cookie。

### 隔离验证

在 Node 20 一次性容器、`remote_node_ingestion_test`、真实 Go relay 和原 crawler/feature schema 中验证：

- 具体 slot 的并发领取、重复领取、跨 slot 拒绝、旧网络未收尾时不领取新频道。
- 实际 YouTubeJS 会话和频道响应解析，授权 profile 传递、Cookie 回执加密和归属校验。
- 组装后的远程 Worker 执行完整视频 Plan；通过原增量空上传列表策略，将测试频道转为 dormant 并完成 run。
- 原 RotaSlotAdapter 等待远程 Plan、会话回执和真实 Go relay 的零连接退休后才完成 Task。
- 浏览器 Cookie 使用原存储加密方式写回，等待 Agent 不阻塞；旧执行被替换时写回失败且 Cookie 不变；已确认回执可重放。
- 原有远程 Plan、API 待处理续跑、结果入库、业务栅栏、连接模式和 spool 回归。

YouTube 返回内容使用本地夹具；真实解析器、Go relay、数据库和原 Plan runner 已执行，但未对实际 YouTube 或那两台服务器进行采集验收。这不等同于已部署可接收正式任务的节点。

最终联合回归 **89 项通过，0 失败、0 跳过**。测试日志：`/tmp/remote-youtube-final-tests-20260910.log`。单独复跑新增范围：

```bash
cd services/qybullmq
REMOTE_NODE_TEST_DATABASE_URL=postgres://postgres@127.0.0.1:55447/remote_node_ingestion_test \
REMOTE_NODE_ROTA_TEST_BINARY=/tmp/pachongsys-node-forward-test \
npm run test:remote-youtube
```

### 正式启用前仍需完成

1. 将原中心执行尝试的创建/完成与远程 Worker 领取时序接入实际执行入口，使用已经创建的 profile 和 attempt；接通最终遥测到原执行汇总。
2. 原 Rota 在同一作业内切代理/国家时的跨节点执行代次交接、完整国家重检上下文，以及 API 续跑后的新执行绑定。当前 adapter 仍绑定单个 lease，不能据此宣称上述交接已完成。
3. 完整采集镜像、中心激活校验、页面部署/缩容执行和实际节点验收。目前的独立镜像及页面预览仍为 `connect_only`，不会因为新增工厂模块而自动开始采集。

本轮仅修改新增远程接入模块、隔离测试、说明及测试命令。未修改既有 YouTubeJS、增量/迁移采集实现、生产 worker/controller/Clock 或 Compose；新 schema 仅应用到专用测试库。未提交、推送、部署、重启正式服务或派发正式频道。


## 第十阶段：原执行生命周期与远程交接

新增 `RemoteManagedIncrementalRuntime`，接入原 `RotaSlotAdapter` 的运行环境接口；调用方继续使用原 `executeManagedWorkerAttempt`、`ProxyBusinessRunPreparer`、`runVideoApiResumable` 和共享 API fallback。新模块负责把中心的每次原始 Rota Attempt 对应到节点上的一次执行，不另写重试、换国家或休眠策略。原 `channelExecutionRuntime.js` 仅将已有 `failureDecisions` 导出，函数内容不变。

`RemoteChannelExecutionStore` 在同一事务中调用原 run/profile/attempt 存储方法并登记远程任务。先验证冻结的 Plan、Clock、原执行归属及目标 node/slot；校验失败时，新 Attempt、profile 和远程登记一起回滚。目标 Worker 才能领取该任务。已绑定原执行的任务租约过期后，不由节点自行重领旧 Attempt，必须由中心按原恢复规则创建下一次执行。

换网络、换国家或 API 后重新进入网络采集时，沿用同一个 Plan、business run 和远程 task，记录交接历史并更新原 Attempt；节点下一次领取增加运输代次。前一 Attempt 必须已经结束，前一网络绑定必须确认退休。下一次 Rota Attempt 编号必须递增，原预算不清零。上传列表的国家复查结果单独冻结为执行上下文；仅从新模块传给原严格 Plan 校验器的 Job 副本中剥离这一个已知字段，不放宽原校验器。副本显式保留 BullMQ 通过原型 getter 提供的 queueName，避免对象展开丢失真实队列归属；交接测试使用实际 BullMQ Job 对象，仅替代其 Redis 写入方法。

异常收尾只结束所属的远程运输记录，不借此写采集业务数据。原 runner 已把 run 标记失败后，中心仍能关闭节点执行；不会要求失败的 run 再次变回 running 才允许释放。接入事务已提交但响应丢失时，可按原 Rota task ID 找回同一 Attempt 并结束它，避免残留占用。

API 等待沿用原持久请求与延迟机制：释放远程 Worker 后，共享 API 模块继续处理。`runRemoteIncrementalApiReplay` 使用原 runner、原检查点和禁止网络的 replay 上下文，仅在核实 API 请求属于同一 run、原网络已退休、Clock/Plan/最新 Attempt 仍匹配时应用结果。纯 API 结果补齐不新建 Rota task；发现还有未采的视频时，由原 `VIDEO_API_NETWORK_REQUIRED` 分支重新进入正常 Rota 执行。禁止用 API replay 绕过过期执行校验。

已经持久接收的频道结果和 Cookie 回执，即使原执行代次已经推进，原节点仍可重传完全相同的回执并收到确认。历史确认不授予采集写入权限；变更 batch、内容、原节点或归属的请求继续被拒绝。

实际节点 `run` 循环的故障注入还发现并修复了一个新接入模块的时序问题：节点已经领取、中心接入确认丢失，此时旧租约失效会让整个 Worker 循环退出；若已经保存网络申请但中心尚未创建绑定，清理又会被拒绝。现已让单次执行过期进入原持久恢复步骤，清理旧领取后再接任务；不会清除未确认的结果。无绑定的清理仅在该执行已经关闭或被替换、历史 claim 证明该节点/slot 确实拥有该代次时返回确认，不创建网络授权。活跃执行、错误代次或错误 slot 继续拒绝。测试分别覆盖“尚未申请网络”和“已经申请、尚未绑定”两个时序，同一个常驻 Worker 均继续完成后续执行。

### 本阶段验证

`remoteExecutionHandoff.postgres.integration.test.js` 使用独立 PostgreSQL 数据库、原业务 schema、真实 Go relay、实际远程 Worker/YouTubeJS 会话，以及原 Rota adapter 和增量 runner。YouTube 响应与 Rota 控制返回使用夹具，没有访问真实 YouTube：

- 美国出口空上传列表 → 巴西备用出口 → 按原判定进入休眠；没有巴西备用出口时，不增加 YouTube 请求并进入休眠。
- 网络失败按原规则记录 Observation、退休旧网络、换出口并续跑；业务 run、Plan 和预算不被重建。
- 视频必需字段三次尝试仍失败后进入共享 API；等待时同一 Worker 实际完成另一个 About Plan，随后原频道用 API 结果完成。
- API 补齐第一个视频后仍有第二个视频：replay 不发网络请求，正常执行只采剩下的视频；检查库中恰好两条内容、一个 API 请求。
- 已取消的 Clock 拒绝接入，新增运输任务和 Attempt 回滚；接入确认丢失后，原 BullMQ 重投模式可以继续。
- 上一代已接收结果及 Cookie 的确认可重放，修改回执或用旧代次发新请求仍被拒绝。

复跑：

```bash
cd services/qybullmq
REMOTE_NODE_TEST_DATABASE_URL=postgres://postgres@127.0.0.1:55447/remote_node_ingestion_test \
REMOTE_NODE_ROTA_TEST_BINARY=/tmp/pachongsys-node-forward-test \
npm run test:remote-handoffs
```

最终联合回归 **182 项通过，0 失败、0 跳过**（Node 20、1 CPU / 768 MiB 一次性容器，源码只读挂载），日志 `/tmp/remote-handoff-final-tests-20260910.log`。随后将交接夹具改为真实 BullMQ Job 并修复 getter 保留问题，该模块相关的 **9 项重新通过，0 失败、0 跳过**，日志 `/tmp/remote-handoff-bullmq-job-tests-20260910.log`。两份记录有重叠，不合计为 191 项独立测试。语法检查和 `git diff --check` 通过。

### 当前接线范围

本阶段补上第九阶段列出的执行交接模块，并在隔离环境中组装验证。生产 worker/controller/Clock/迁移入口没有接入该模块；完整采集镜像、中心激活登记、页面部署和缩容动作、真实节点验收仍待完成。已有独立镜像和页面预览继续标记 `connect_only`。新 schema 仅应用到专用测试库，未部署、重启、派发正式任务或自动操作两台节点服务器。

## 第十一阶段：完整镜像、中心启用门禁与页面手动部署

本阶段把先前隔离组装的采集执行器封装到 `services/remote-node/Dockerfile.collect`。镜像运行原 YouTubeJS、Python 指纹服务和本机 Go relay，增加持久 spool；不安装 yt-dlp。每个容器一个增量 Worker，内存上限 768 MiB、CPU 0.5、退出宽限 16 分钟。轻量 `connect_only` 镜像仍单独保留，并回归验证。

新增 `RemoteIncrementalProcess`：加载完整采集模式配置后先连接中心、等待启用，不能因为容器 healthy 就接收频道。中心 `RemoteWorkerActivationStore` 以 node/slot、deployment ID、config hash、instance ID、relay boot ID、运行时版本和活跃心跳校验新领取。启用方法要求中心执行监督器提供事务内校验，不能通过节点心跳或页面参数打开。停止派发后禁止新领取，但允许原领取回执恢复；替换实例必须等待旧执行与网络绑定收尾，并重新启用。节点失联、心跳回执不匹配或健康文件写入失败时停止新领取；SIGTERM 等待在途执行收尾后关闭 relay。

中心 `deploymentAdmin.js` 使用与 node token 分离的管理员凭据。准备部署在同一事务登记节点、固定 slot 和配置摘要，凭据使用现有认证加密方法持久存储。重复请求返回同一份节点/slot 凭据；增加数量保留旧 slot，拒绝缩容、更换镜像或 deployment ID。准备步骤不向 Rota 分配线路，也不启用任务。

Dashboard 新增明确的“部署 Worker”动作。预览仍为只读；部署前冻结已保存数量和镜像，持久写入 operation ID 及进度，再依次执行中心登记、SSH、文件下发、容器启动、容器检查、中心连接。后台操作失败时保留已完成步骤、原部署和暂存文件，密码及中心返回凭据不写进登记 JSON/错误日志。页面刷新后可继续查看进度；成功自动关闭弹窗，重新打开可查看详情。缺少中心部署配置时禁用执行按钮。

`nodeRuntime/deployWorkers.py` 是固定安装脚本，通过现有 SSH/SFTP/sudo 执行。它核对节点 identity、固定镜像、容器权限与挂载路径；配置/凭据不可覆盖为不同内容，单个 Worker 的 spool 使用固定独立目录。`compose up --no-recreate --pull never` 保留已有容器；增加数量只新增实例。运行环境准备补齐 Python 3。真实节点的缩容、替换镜像及删除前运行核验尚未接通，因此保持拒绝。

### 当前部署边界

新增 `scripts/runRemoteNodeCenter.mjs` 仅负责 HTTPS 反向代理后的中心网关、登记、心跳及已有远程协议。它要求显式配置数据库、密钥和控制地址；检查可选表但不应用 schema。**尚未接通生产 BullMQ 消费者与实际执行监督器，不会自动启用新部署 Worker。** 因而本阶段完成的是可手动部署并核实连接的完整采集进程，不能将其描述为已经投入正式频道采集。

下一步要把此前测试通过的 managed runtime 接到独立中心执行入口，以原 Clock/Plan 和原 Rota 执行归属驱动远程 slot，并将实际就绪状态接入启用事务。随后由用户在页面选择节点、数量并手动部署，验收一个真实到期 Plan 的领取、采集、中心写入和最终完成。保持既有迁移及本机增量入口的采集策略不变。

新 `workerActivationSchema.sql` 只应用到隔离测试库。本轮没有连接两台真实服务器，没有应用生产 schema、上线 Dashboard、推送镜像、启动生产中心入口、重启正式 Worker、提交或推送 Git。

### 本阶段验证记录

- `remoteDeploymentAdmin.postgres.integration.test.js`：真实 PostgreSQL/HTTP，验证管理员与节点凭据隔离、登记幂等、凭据加密、原 slot 保留及非法方案拒绝。
- `serverNodeWorkerDeployment.postgres.integration.test.js`：真实页面 HTTP 和登记数据库，SSH/中心服务使用夹具，验证旧版本拒绝、失败重试、增加数量、缩容及删除拒绝。
- `serverNodeRuntime.ssh.integration.test.js`：真实 SSH/key/SFTP/sudo 与固定脚本，共 **10 项通过，0 失败、0 跳过**。包管理和 Docker daemon 在临时容器内模拟，未挂宿主 Docker socket。覆盖初次部署、凭据权限、重试、扩容、拉取失败、节点/配置/挂载冲突和暂存数据保留。日志 `/tmp/remote-worker-deployment-ssh-tests-20260910.log`。
- 完整采集镜像真实 Docker 验证：两个容器及实际 Go relay，HTTPS 信任、等待启用时零领取、测试回调启用后的空任务领取、断线重连、冲突实例拒绝、停止派发、SIGTERM 正常退出、暂存文件保留、Python 依赖加载与未安装 yt-dlp。仅使用无任务测试 capability，未请求真实 YouTube。日志 `/tmp/remote-node-collect-docker-20260910.log`。
- 轻量镜像同样通过真实容器连接回归，保持零任务领取。日志 `/tmp/remote-node-connection-docker-final-20260910.log`。
- 浏览器验证手动部署、只使用已保存数量、密码清空、重复提交禁用、刷新进度、失败重试、成功自动关闭、重新查看、移动端无横向溢出、缺少配置时禁用；无页面脚本错误。日志 `/tmp/qy-node-runtime-ui-test/deployment-browser.log`。
- Dashboard 最终相关回归 **17 项通过，0 失败、0 跳过**，日志 `/tmp/remote-deployment-dashboard-final-tests-20260910.log`。
- 远程模块最终联合回归 **111 项通过，0 失败、0 跳过**，包含原频道计划/网络/执行栅栏/国家和 API 交接、新增启用与部署登记测试，日志 `/tmp/remote-deployment-final-tests-20260910.log`。此集合与第十阶段 182 项联合测试范围不同，不作数量相减或累加。与本阶段 Dashboard、SSH 三组共 138 项；真实 Docker 和浏览器另行记录。语法检查及 `git diff --check` 通过。

## 第十二阶段：独立中心执行入口与真实 BullMQ 接线

新增 `RemoteCenterExecutionSupervisor` 和 `createCenterIncrementalProcessor`，把第十阶段的 managed runtime 接入实际 BullMQ Worker。每个已允许的远程 node/slot 对应一个中心消费者和一个原 `RotaSlotAdapter`，并发固定为 1；消费者共用原 `youtube-channel-incremental` 队列，中心总槽数最多 32。原迁移队列、本机 worker.js、controller 与 Clock 生成入口没有改动。

处理器使用原 `ProxyBusinessRunPreparer`、`executeManagedWorkerAttempt`、`processManagedWorkerJob`、`runVideoApiResumable` 和增量 runner。只在 Plan 要求 About 或 Video 时请求远程节点；Agent-only Plan 留在中心排后处理任务。API 等待使用原 BullMQ delayed job，释放节点槽；API 结果回放仍经过原执行/检查点校验，有剩余网络步骤才回到 Rota。失败使用原错误分类、重试判定和幂等终态 Observation，任务事件写入原表。不会另生成 Clock 或迁移任务。

### 就绪、独占与退出

中心为每个 node/slot 使用独立 PostgreSQL session advisory lock（namespace 781138012）。重复中心进程拿不到锁时不会创建 Rota 执行或消费者。启用、节点心跳和新领取除了精确节点实例，还验证实际中心消费者的主 Redis 连接、Rota 就绪和该 backend 仍持有独占锁。原始执行接入和每次远程采集业务事务也核实中心归属；中心丢失独占权后不能靠数据库中旧 enabled 标志继续写。

独占锁连接异常时，中心暂停该消费者，并经原 Rota adapter 中止、收尾自己的在途执行。健康心跳过期时停止新领取和队列派发，恢复连接后可继续。手动 `drain` 另外持久保存 `activation_requested=false`，中心重启不会取消手动暂停；正常进程退出仅清除实际启用状态，保留运行意图。

SIGTERM 先停止新取任务，保持当前 BullMQ 锁续期、Rota 心跳和节点命令处理，等待当前频道完成，再关闭 Rota 和网关。测试发现 BullMQ `waitUntilReady()` 返回的是阻塞取任务连接，关闭中心时该连接会先断开；现改为检查实际续锁的主连接，避免把正常暂停误判为中心失联。API 纯回放也作为当前作业收尾，不依赖 Rota active_job 标志。

**硬崩溃的恢复边界：** 新中心若发现某槽仍有旧的 pending/leased 运输任务或未退休网络绑定，会暂停这个槽并报告 `remote_center_previous_execution_unsettled`，不覆盖旧 Attempt，也不把它记为成功。原执行收尾后会重新尝试接入。强杀后全部异常记录的自动清理并未在本阶段新增，真实节点验收前仍需单独验证；该槽的等待不阻塞其他槽。

### 显式启动配置

`runRemoteNodeCenter.mjs` 默认仍只运行接入网关。启动消费者必须同时配置：

- `REMOTE_NODE_EXECUTION_ENABLED=true`。
- `REMOTE_NODE_EXECUTION_NODE_IDS`：允许执行的节点 UUID，以逗号分隔；未列入的节点可以连接，但不能因本入口开始接任务。
- `REMOTE_NODE_REDIS_URL` 与 `REMOTE_NODE_QUEUE_PREFIX`：明确指定中心实际 Redis 和队列前缀，不默认接入其他队列；测试使用单独实例和随机前缀。
- `REMOTE_NODE_PROFILE_SECRET_FILE`：与现有中心浏览器 profile 加密配置一致的密钥文件。
- `REMOTE_NODE_ROTA_CONTROL_TOKEN_FILE` 与 `ROTA_PROXY_CONTROL_URL`：原 Rota claim/renew/task 控制凭据及地址。
- 原路由读取的 `REMOTE_NODE_ROTA_TOKEN_FILE` 继续独立使用，不能与上述控制 token 相同；`REMOTE_NODE_ROTA_ROUTE_URL` 必须指向 `/internal/v1/remote-route`。
- `ROTA_IDENTITY_POLICY_ID`、`ROTA_WORKLOAD_SCOPE_EXPECTED`、`YOUTUBEJS_EXTRACTOR_MODE=full`，以及与原系统一致的 `YOUTUBEJS_VIDEO_API_BATCH_FALLBACK` 开关。

继续要求第十一阶段的数据库、签名/加密密钥、镜像及网关配置。主业务池设置原 `publication.writer_version`。启动仅检查 schema，不自动建表；`workerActivationSchema.sql` 新增 `activation_requested`，仅在隔离库应用。中心入口需要当前源码可访问的原身份策略 catalog。

API 设置读取提取为 `youtubeApiSettings.js`，本机和远程中心共同使用。数据库设置优先级、环境回退、key 去重、配额边界及 30 秒缓存规则保持原样。这样新入口读取 API 设置时，不会导入 pipelineV2 顶层并自动创建整套队列连接。这是本阶段唯一涉及原 pipelineV2 的重构，不调整采集/重试/休眠策略。

### 验证方式

`remoteExecutionHandoff.postgres.integration.test.js` 扩充真实 BullMQ、Redis、节点常驻进程与就绪校验场景，复用之前的 YouTube/Rota 返回夹具：

- 原 Clock/dispatch 记录 → 真实 BullMQ Job → 中心 Rota Attempt → 远程节点 → 原业务写入 → BullMQ completed。
- API 持久等待时，同一槽完成另一个频道；结果就绪后直接回放，未额外申请 Rota Task。
- 美国空列表 → 原规则切巴西；中心停止派发时仍让当前频道完成。
- API 回放期间停止中心，同样等待其提交完成。

`remoteCenterSupervisor.postgres.integration.test.js` 使用真实 Redis 消费者与 PostgreSQL 锁，测试重复中心、心跳过期、隔离库 backend 被终止、重新接入、重启保持手动暂停，以及拒绝覆盖旧未收尾任务。`remoteCenterEntry.integration.test.js` 启动实际中心入口子进程，验证显式配置、退出、无自动任务生成及缺少队列配置时拒绝启动。上述均没有访问真实 YouTube 或两台节点服务器。

复跑前需要启动专用测试 Redis 并设置 `REMOTE_NODE_TEST_REDIS_PORT`，本夹具专用密码为 `remote-center-fixture-only`，不能用于生产。数据库仍要求真实库名 `remote_node_ingestion_test`，所有远程集成测试使用同一 advisory lock 串行执行：

```bash
cd services/qybullmq
REMOTE_NODE_TEST_DATABASE_URL=postgres://postgres@127.0.0.1:55447/remote_node_ingestion_test \
REMOTE_NODE_ROTA_TEST_BINARY=/tmp/pachongsys-node-forward-test \
REMOTE_NODE_TEST_REDIS_PORT=55449 npm run test:remote-center
```

未配置专用 Redis 时，真实队列场景会跳过，不能视为已经验收。本阶段没有上线、提交、推送、操作真实节点或调整正式 Worker 数量。

联合故障测试另发现一个节点退出竞态：心跳请求发出后收到 SIGTERM，校验器使用变化后的 stopping 状态判断旧请求回执，偶尔误报 `NODE_CONNECTION_REJECTED`。现按该次实际发出的 `accepting` 字段校验回执；新增挂起心跳后停止节点的测试，正常退出不再误判。此修改仅涉及新增远程节点进程。

本阶段最终联合回归 **215 项通过，0 失败、0 跳过**，日志 `/tmp/remote-supervisor-final-tests-20260910.log`。随后补充“执行前重新读取当前连接配置”的校验，以及 API 回放期间停止进程的用例，相关真实队列/独占权测试 **13 项再次通过，0 失败、0 跳过**，日志 `/tmp/remote-supervisor-admission-final-20260910.log`；两组有重叠，不累加。页面部署徽标改为客观的“部署和连接检查通过”，不再把中心已经启用的实例固定标为等待。

部署页面浏览器复验通过，包含手动触发、已保存数量、密码清空、防重复提交、刷新进度、失败重试、成功自动关闭、移动端布局与无自动写操作；无页面脚本错误。日志 `/tmp/qy-node-runtime-ui-test/deployment-browser-center-20260910.log`。

最终完整节点镜像构建及真实 Docker 验证通过，镜像摘要 `sha256:42b56ee06d515f493e50874c8d6446f37949bf8645ac79ebb6cb451f78a7bf6e`，日志 `/tmp/remote-supervisor-node-docker-20260910.log`。覆盖两台独立容器、实际 Go relay、HTTPS、启用/停止领取、断线重连、冲突实例、SIGTERM 和无凭据日志。本轮临时 Redis、Docker 容器及测试网络已清理；本地镜像保留用于后续验收。语法检查与 `git diff --check` 通过。

## 第十三阶段：强杀后的恢复与持久结果确认

本阶段接续第十二阶段的未收尾槽隔离机制。测试发现两处恢复缺口：节点重启看到旧租约仍有效，就跳过网络清理；新中心检测到旧任务后只等待，没有自动关闭旧执行。前者在真实节点子进程 SIGKILL 后复现：数据库网络状态仍为 `active`，断言应为 `retired`；后者在持有独占锁的中心子进程被杀后复现：原 Attempt 仍为 `running`。失败记录分别为 `/tmp/remote-network-crash-red.log`、`/tmp/remote-center-crash-red.log`。

### 收尾规则

- `centerExecutionRecovery.js` 在新中心独占锁的同一个 PostgreSQL session 中执行事务。没有该槽的锁不能恢复；节点、任务及旧 Attempt 身份不匹配或存在更新执行时，保留隔离状态。
- 先关闭旧 pending/leased 运输任务及 coordinator，阻止旧写入继续。原 Plan、channel run、视频检查点、已入库的命令结果、Rota 预算与重试限制均不重建、不清零。
- 从未签发路由授权的绑定可直接收尾；签发过的绑定必须收到节点 relay 的实际停止回执。租约到期或 `stop_requested=true` 都不算停止证明。
- 网络安静后，中断的 Attempt 记为 `aborted`。任务与业务 run 已在同一事务成功提交的，保留业务成功，并通过原校验应用已保存的成功 profile checkpoint；随后重投只确认完成，不重复采集或发布观测。
- 每次最多处理该槽 32 个候选记录，并限制 SQL 等锁/执行时间。恢复异步按槽执行，期间该槽不创建新消费者，其他槽继续协调。手动暂停的槽仍可收尾，但不会因此取消暂停。
- Rota 的旧执行租约与新租约申请仍由原 Rota 机制处理。本模块不会冒充旧 Worker 调用成功完成，也不会给旧网络伪造零在途回执。

节点启动时，持久 `active` 网络状态视为旧进程留下的会话，先停止并回传回执，再等中心收尾；不会用新浏览器重新跑旧身份的在途请求。结果确认可以在这个等待期间继续：中心已经接收的同一 batch 重发只确认原结果；中心明确返回 `STALE_LEASE` 的未接收结果原样移到 `.stale` 文件，拒绝写入业务库但不永久停掉 Worker。该文件仍占用暂存配额；冲突结果继续 `.blocked`，暂存满继续停止领取。

### API、国家切换与启动边界

SQL 已提交 `VIDEO_API_PENDING` 而进程未及更新 BullMQ job 时，中心处理器从同一冻结 Plan 的持久请求重建 continuation，交给原 API 等待/回放路径。API 回放没有增加 Rota Task 或额外 YouTube 请求。

国家重查结果尚未写回 job 时，不推断“巴西可用”或“没有备用池”。由原 Rota adapter 使用下一次原预算内的 Attempt 重新执行国家决策，得到明确回执后再继续。正常已确认的国家交接不重复处理。

联合测试还捕获了启动退出竞态：中心先打印 ready，再安装 SIGTERM 处理器，测试在 ready 后立即停止时会被信号直接结束。现在先安装处理器再公布 ready；验收连续启动并立即停止三次，检查正常退出。

### 验收范围与限制

`remoteExecutionHandoff.postgres.integration.test.js` 启动真正的中心测试子进程、BullMQ Worker、隔离 Redis/PostgreSQL、HTTP 网关与 Go relay，在以下位置 SIGKILL 中心：入队执行已接入但还没请求、已授权网络、原业务事务已成功提交但还没确认队列。等待真实 BullMQ 执行锁过期后，由替代 Worker 重投原 Job。验证 Job 的执行次数为 2、Plan/run 仍各一份、Rota 次数不重置、About 观测只有一条。另一个用例由实际 `RemoteCenterExecutionSupervisor` 自动检测并收尾旧接入，未手动修改任务状态或将 Job 移回等待。

`remoteChannelNetwork.postgres.integration.test.js` 强杀已完成路由激活的节点子进程，验证实际 relay 停止回执。`remoteChannelPlan.postgres.integration.test.js` 分别在结果暂存后尚未上传、中心已入库但节点未处理确认时强杀节点，验证过期原始结果留档、已接收结果幂等确认，恢复不触发重复 YouTube 操作。

这些测试的 PostgreSQL、Redis、HTTP、Go relay、子进程信号是真实的；YouTube 内容、Rota 控制返回和 API 数据使用受控夹具。不是两台实际服务器或真实频道采集验收。节点长期离线、磁盘遗失、停止回执缺失仍保留等待；原 BullMQ/Rota 重试预算已耗尽时仍遵循原终态规则，不无限重试。本阶段没有修改迁移、本机 worker、Clock 或采集策略，也没有新建业务表、上线、提交或推送。

最终联合回归 **224 项通过，0 失败、0 跳过**，日志 `/tmp/remote-crash-final-20260910.log`，覆盖远程模块与原增量 runner、Plan、managed job/attempt、API fallback/continuation 及设置读取。自动监督器恢复用例约 37 秒，包含默认 BullMQ stalled 检查周期，未缩短正式队列参数；另外三项强杀边界用例使用测试专用短租约。源码和测试夹具的语法检查、`git diff --check` 通过。

完整采集镜像本地重建及 Docker 验证通过：`qy-remote-node-incremental:20260910`，镜像摘要 `sha256:79587852dad13f4e4f7adaf916866fce42acef9f6ddb1e9be994cf5ef690bcd7`，构建日志 `/tmp/remote-crash-node-build-20260910.log`，容器验证日志 `/tmp/remote-crash-node-docker-20260910.log`。验证包括两台独立容器、实际 Go relay、HTTPS、健康检查、启用/停止领取、断线重连、拒绝重复实例、SIGTERM 及无凭据日志。临时节点容器、测试网络及本轮 Redis 已清理，保留本地镜像和验收日志。

## 第十四阶段：中心接入服务生产部署

2026-09-10 用户授权部署中心接入服务。本阶段上线独立容器 `qy-remote-node-center`，镜像 `qy-allpachong/remote-node-center:gateway-20260910`，使用单独 Compose 项目 `qy-remote-center`。资源上限为 1 CPU、768 MiB，接入现有 `qy-newcrawler-crawler-runtime` 网络，无宿主机端口。原 Worker、Dashboard、Rota 均未重新部署。

正式库 `newcrawler_crawler` 新增 `remote_ingestion` 下 11 张接入状态表，通过显式数据库身份检查、事务和锁等待限制应用。启动脚本不自动建表。中心使用直连 PostgreSQL，避免未来 session advisory lock 经过事务连接池。私有环境和凭据位于被 Git 忽略的 `runtime/remote-center-production/`，未放入镜像或文档。

现有 nginx 新增 `/node-execution/v1/` 转发至中心 3187，继续使用原域名 HTTPS；没有新增页面，也没有修改现有页面、队列或监控路由。首次临时配置预检发现公共代理文件与新 location 的超时设置重复，在加载前改为该 location 自己设置必要代理头。随后临时配置和实际配置均通过 `nginx -t`，以热加载生效。

上线验证：

- 容器 healthy；带内部管理凭据的本地健康检查访问数据库成功。
- 公网节点心跳无凭据、错误凭据均返回 401 JSON；内部管理路径返回 404；原页面仍返回正常登录 302。
- 对比部署前后全部 66 个原 Worker，容器 ID、启动时间均一致，全部仍在运行。
- 新接入 schema 全部 11 张表记录数为零；中心启动日志为 `activation: disabled`，没有创建远程节点或采集任务。
- 新镜像入口在隔离数据库验证启动、连续立即 SIGTERM 和错误配置拒绝启动，1 项测试通过；构建、脚本语法检查、Compose 健康检查和 `git diff --check` 通过。

当前 `REMOTE_NODE_EXECUTION_ENABLED=false`。本阶段仅完成中心服务和 HTTPS 入口，不代表远程采集端到端上线。生产 Rota 尚未启用远程路由读取入口；本地采集镜像尚未发布到远程节点可拉取的仓库；Dashboard 的最新部署功能及控制配置也需要下一阶段上线。没有连接两台真实远程服务器，没有替用户部署远程 Worker，也没有提交或推送源码。

维护命令、回退边界见 `services/remote-node/README.md`。回退保留新表、凭据和采集数据；nginx 原配置备份为 `runtime/remote-center-production/nginx.before.conf`。Worker 对比记录为同目录 `worker-verification.json`。本阶段测试日志为 `/tmp/remote-center-production-build-20260910.log` 与 `/tmp/remote-center-deployment-entry-test-20260910.log`。

## 第十五阶段：生产路由读取、私有仓库与页面部署接线

2026-09-10 用户继续授权，并明确选择中心私有镜像仓库。本阶段没有 SSH 到真实节点，没有启动远程 Worker，远程执行开关仍为 false。页面登记的 `43.172.83.170` 已完成 SSH/监控初始化，但运行环境尚未准备；监控总内存 7.44 GiB，保存数量为 20，按现有部署门槛需要至少 15.5 GiB。未代替用户更改数量，建议先改为 3。

### 生产服务

- `qy-remote-rota-reader`：复用 Go `Manager.ReadRemoteRoute` 和专用认证 handler 的独立入口 `cmd/remote-route`。不运行原 Rota 的迁移、分配或协调循环；不重启原 Rota。专用数据库角色 `qy_remote_route_reader` 仅授予 public schema 的 usage 以及 proxies、proxy_running_slots、proxy_control_leases、proxy_control_tasks 四表 SELECT，角色和连接都设置默认只读。连接上限 4，语句超时 2.5 秒，并校验实际数据库名。容器仅在内部网络提供 3188。
- `qy-remote-center-private`：内部 HTTPS 3443 仅转发中心部署 prepare/status 与路由读取，使用私有 CA；Dashboard 和中心显式信任该 CA，未关闭 TLS 校验。另一个内部端口 3189 提供经过 Basic 认证的镜像 GET/HEAD；拒绝上传、删除。
- `qy-node-registry`：固定摘要的 registry:3，镜像数据存于私有 runtime 的 registry 目录。宿主机只监听 127.0.0.1:35000，供管理员上传。公网通过已有 Dashboard 域名 `/v2/` 拉取，nginx 热加载新增转发，原登录和节点 `/node-execution/v1/` 路由保留。
- 中心接入容器改用内部 HTTPS 路由与实际私有仓库镜像，仍不消费任务。原 66 个 Worker 的 ID、启动时间均不变；原 Rota 仍为 2026-09-09 启动的进程。

采集镜像为 `newcrawdashboard.137-175-93-199.nip.io/qy-node-incremental@sha256:79587852dad13f4e4f7adaf916866fce42acef9f6ddb1e9be994cf5ef690bcd7`，内容与第十三阶段验收镜像一致。私有仓库读取凭据存于 root-only 的 `secrets/registry-pull.json`，只在用户点击部署时读取，核对镜像主机后随 SSH 私有 bundle 下发。节点只在 pull 期间创建 `/run/qy-registry-*` 下的 root-only Docker config，成功或失败后均清除，不修改全局 Docker 登录，不写入页面 registry、预览或 Worker 配置。

### Dashboard 保留线上修复

首次源码比对发现原生产镜像额外包含 migrationInventoryRead.js 及迁移列表相关修复，工作区没有这些完整文件。初次完整 Dashboard 镜像曾短暂启动，随即回退原镜像并验证 healthy。最终用 `services/dashboard/Dockerfile.node-management` 继承原生产镜像 `sha256:d70cfbcafc4b75cb03c4d1fd24722a0faab56e407faec17bb3f0eaf04a567f6d`，只覆盖 serverNode 模块、节点样式和部署权限，并由受限安装脚本加入两个 import 与节点路由参数。所有 migration 文件与原镜像逐字节一致，server.js 的原迁移读取与错误显示逻辑保留。

实际上线镜像 `qy-allpachong/dashboard:remote-deploy-overlay-20260910`。本阶段未把另一工作区的迁移文件覆盖回本工作区；后续完整重建前应合并这些线上差异。Compose 处理相同项目/service 标签时清除了历史已停止的 Dashboard 备份容器，保留了原镜像和完整回退配置；没有移除持久数据或其他服务。

部署文件：`deploy/compose.remote-node-center.yml` + `deploy/compose.remote-node-services.yml`；Dashboard 通用叠加配置为 `deploy/compose.remote-node-dashboard.yml`。本次实际 Dashboard 使用按运行容器保存的独立清单 `runtime/remote-center-production/dashboard.compose.json`，原环境、存储挂载、资源参数和主要网络均保留。回退清单为 `dashboard.rollback.compose.json`，不需要操作采集队列。nginx 本阶段之前的备份为 `nginx.before-registry.conf`。

### 验证结果

- Go 独立路由入口与认证测试通过，未认证或普通控制凭据不能读取路由，普通 claim 接口不存在。
- Dashboard 部署相关单元/路由测试 11 项通过；真实 SSH/SFTP/sudo 的隔离部署测试 11 项通过（含失败后的拉取凭据清理、错误仓库拒绝、原暂存结果保留）；专用数据库中的页面注册/部署事务回归 2 项通过。
- 最终生产叠加镜像内 4 项测试通过，包括原迁移列表读取测试。生产镜像差异比对记录为 `dashboard-overlay-verification.json`。
- 公网仓库无凭据 401、认证读取 200、上传和删除 403；清单、配置 blob 与最大层（41,422,030 字节）通过 SHA256 校验。
- 中心到路由读取服务的内部 HTTPS：错误凭据 401；有效凭据查询不存在的完整任务栅栏返回 409，验证实际 SQL 可执行，不创建任务。中心管理 HTTPS 状态查询 200、零 Worker。
- 线上 Dashboard 的 onboarding、runtime、workerDeployment 三项 capability 均为 true；直接调用实际部署控制 client 成功，私有仓库凭据与镜像匹配。节点监控正常，原页面健康检查通过。

日志：`/tmp/remote-rota-reader-unit-20260910.log`、`/tmp/remote-registry-dashboard-tests-20260910.log`、`/tmp/remote-registry-ssh-tests-20260910.log`、`/tmp/remote-dashboard-deploy-pg-20260910.log`、`/tmp/remote-dashboard-overlay-tests-20260910.log`。构建/部署记录同名前缀位于 /tmp。测试不代表两台真实节点已经完成真实频道采集；下一步由用户在页面准备环境、调整数量、部署 Worker，再核实连接并启用明确节点的中心消费者。
