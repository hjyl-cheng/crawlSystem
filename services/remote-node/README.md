# 远程节点镜像与手动部署

提供两个独立镜像。`connect_only` 用于连接验收；`incremental_collect` 包含原 YouTubeJS 增量采集执行器、Python 指纹请求服务及本机 Go relay。完整镜像不安装 yt-dlp。节点通过 HTTPS 接入中心，不需要配置中心 PostgreSQL、Redis、MinIO 或 Rota 管理凭据。

**2026-09-11 当前状态：已启用生产远程增量采集。** 页面支持部署、开始接任务和暂停接任务；新 Worker 默认待命。中心使用原增量队列，部署或开始时自动补足 Rota 网络名额。配置与实测结果见 [自动网络扩容](../../docs/REMOTE_NODE_AUTO_CAPACITY_20260911.md) 和 [节点接任务控制](../../docs/SERVER_NODE_EXECUTION_CONTROL_20260911.md)。下文保留各阶段部署说明，早期未启用描述属于历史记录。

## 构建

在仓库根目录执行：

```bash
docker build -f services/remote-node/Dockerfile -t qy-remote-node-connection:20260910 .
docker build -f services/remote-node/Dockerfile.collect -t qy-remote-node-incremental:20260910 .
```

镜像从同一源码编译 Go relay，最终使用 Node 20、UID/GID 1000。页面部署必须使用已经发布到节点可访问仓库的 `repository@sha256:...`，不能直接使用上述本地标签。

## 页面操作

1. 手工添加服务器，完成 SSH、监控初始化和 Docker/Compose 运行环境准备。
2. 保存增量 Worker 数量，查看部署方案。预览只使用已保存的数量，不写数据库、不连接 SSH。
3. 点击“部署 Worker”，必要时输入 sudo 密码。Dashboard 先持久登记部署，再向中心获取该节点的专用凭据，通过 SSH 下发固定配置并启动容器。
4. 六个阶段依次为中心登记、SSH、部署文件、启动容器、检查容器、中心连接。成功后自动关闭进度窗口，显示部署和连接检查通过；中心启用后自动接收任务。

首次部署支持 1～32 个增量 Worker，每个容器内存上限 768 MiB、CPU 上限 0.5，主机总内存至少容纳全部容器上限另加 512 MiB。此检查不代表主机当前一定有足够空闲内存，实际数量仍应结合监控确定。其他 Worker 类型尚未接入此入口。

失败重试和增加数量沿用同一部署 ID、旧实例配置及凭据；`compose up --no-recreate` 保留已有容器。新增入口拒绝直接缩容、替换镜像或替换部署。管理 Worker 中可逐个删除空闲 Worker：中心先阻止接单并确认任务与执行锁已释放，节点再移除对应容器；保留采集文件和历史登记，后续新增使用新的编号。删除中断可重试同一操作，未完成期间禁止并行部署和修改接单数量。已有部署记录的节点不能直接删除。

Dashboard 重启后仍能展示数据库中的阶段记录；后台操作不会跨重启自动续跑。未结束的操作在 20 分钟期限内防止重复提交，期限结束后可重试原部署。节点安装步骤有独立锁及超时，重试不会删除暂存结果。

## 文件与运行状态

| 容器路径 | 内容 |
| --- | --- |
| `/run/secrets/node-config.json` | 固定 mode、node_id、slot、deployment_id、gateway_url |
| `/run/secrets/node-token` | 该节点的 HTTPS 身份凭据 |
| `/run/secrets/relay-token` | 该实例本机转发控制凭据 |
| `/run/secrets/route-public.pem` | 中心路由签名公钥 |
| `/var/lib/qy-node/spool` | 完整采集镜像的持久结果及恢复暂存区 |

凭据必须为容器用户可读的 600 普通文件，不接受符号链接。部署文件放在节点 `/etc/qy-node/runtime/deployments/<deploymentId>/`，暂存数据按 slot 放在 `/var/lib/qy-node/spool/incremental-N`。容器使用只读根文件系统、受限 tmpfs、CPU/内存/日志上限，不挂 Docker socket、不使用 host network、不发布端口。

Go relay 只监听容器本机。中心预先登记部署 ID 和配置 SHA256；心跳必须匹配精确的实例、relay 启动标识及运行时版本。连接成功不等于允许采集。节点镜像和部署模板禁用 Docker 定时健康检查；部署先检查容器运行及槽位身份，再由中心已有的注册、有效心跳确认连接，启用接单仍需通过就绪检查。运行期间由既有心跳、任务续租和超时机制判断在线及任务状态，不为检查状态重复加载采集程序。中心停止派发后拒绝新领取，原领取仍可恢复确认；SIGTERM 先停止领取，等待当前工作结束，再关闭 relay。完整镜像退出宽限为 16 分钟。

节点断线时停止新领取，健康检查失败并重连。重复实例不能替换仍然存活的实例；旧实例的频道或网络绑定尚未收尾时，即使连接已过期，也不能由新实例抢占。替换后的实例必须重新通过中心启用校验。

## 中心配置

新增独立入口 `services/qybullmq/scripts/runRemoteNodeCenter.mjs`，默认仅监听中心 `127.0.0.1:3187`，由现有 HTTPS 反向代理接入。它不会自动应用 schema，缺少可选表时拒绝启动。

需要明确设置：

- `REMOTE_NODE_DATABASE_URL`：中心直连数据库，供执行锁和 `LISTEN` 专用会话使用，不能指向事务连接池。
- `REMOTE_NODE_TRANSACTION_DATABASE_URL`：可选的中心事务连接池（例如现有 PgBouncer），供 gateway、心跳和结果事务复用连接；未配置时沿用直连地址。每个事务自行设置发布写入版本，不能依赖连接池保留会话设置。
- `REMOTE_NODE_ROUTE_PRIVATE_KEY_FILE`：路由签名私钥。
- `REMOTE_NODE_ENCRYPTION_KEY_FILE`：64 位十六进制的加密密钥。
- `REMOTE_NODE_ADMIN_TOKEN_FILE`：仅供 Dashboard 使用的部署控制凭据。
- `REMOTE_NODE_ROTA_TOKEN_FILE`、`REMOTE_NODE_ROTA_ROUTE_URL`：中心调用 Rota 的配置。
- `REMOTE_NODE_COLLECT_IMAGE`、`REMOTE_NODE_GATEWAY_URL`：固定摘要镜像及节点访问的 HTTPS 地址。

Dashboard 对应配置：`SERVER_NODE_STATE_DIR`、`SERVER_NODE_COLLECT_IMAGE`、`SERVER_NODE_GATEWAY_URL`、`SERVER_NODE_WORKER_CONTROL_URL`、`SERVER_NODE_WORKER_CONTROL_TOKEN_FILE`。控制地址必须 HTTPS，控制凭据文件必须为 600 普通文件；凭据不会写入节点登记 JSON 或日志。缺少配置时，部署按钮不可执行，仍可保存计划。

可选 schema 依赖顺序：`schema.sql` → `routeSchema.sql` → `youtubeSessionSchema.sql` → `workerConnectionSchema.sql` → `workerActivationSchema.sql`。这些文件位于 `services/qybullmq/src/remoteNodes/`。正式增量执行还依赖前面阶段的原业务 schema 及执行绑定记录，不能只应用这些表就启用生产采集。2026-09-10 已在正式库应用接入 schema，执行开关仍关闭，详见下方生产部署记录。

管理员准备和状态接口为 `/internal/node-deployments/prepare`、`/internal/node-deployments/status`，使用单独管理员 token。节点 token 无权调用它们，公网没有 activate 接口。生产启用必须由中心执行监督器在事务中验证精确 slot/instance、实际队列处理器及 Rota 身份；默认未开启监督器；显式配置后由监督器验证并启用名单中的节点。

## 隔离验证

测试数据库必须显式指定且真实库名为 `remote_node_ingestion_test`。Docker 测试使用临时内部网络、HTTPS 证书、两台容器和冲突实例；收尾清理自己创建的资源，不访问用户登记的两台服务器。

```bash
cd services/qybullmq
REMOTE_NODE_TEST_DATABASE_URL=postgres://postgres@127.0.0.1:55447/remote_node_ingestion_test npm run test:remote-deployment
REMOTE_NODE_COLLECT_TEST_IMAGE=qy-remote-node-incremental:20260910 \
REMOTE_NODE_TEST_DATABASE_URL=postgres://postgres@127.0.0.1:55447/remote_node_ingestion_test npm run test:remote-collect:docker
REMOTE_NODE_CONNECTION_TEST_IMAGE=qy-remote-node-connection:20260910 \
REMOTE_NODE_TEST_DATABASE_URL=postgres://postgres@127.0.0.1:55447/remote_node_ingestion_test npm run test:remote-connections:docker
```

Docker 采集模式测试仅使用无任务的隔离 capability，启用校验使用测试回调，验证真实进程及领取门禁，不代表已验证真实频道采集。频道策略、切国家、API 延迟和执行交接由前面阶段的隔离业务夹具覆盖。

## 中心执行阶段

第十二阶段的完整配置、独占锁与收尾说明见 [远程接入说明](../../docs/REMOTE_NODE_INGESTION_20260910.md#第十二阶段独立中心执行入口与真实-bullmq-接线)。核心开关为 `REMOTE_NODE_EXECUTION_ENABLED=true`，节点范围由 `REMOTE_NODE_EXECUTION_NODE_IDS` 明确指定，还需显式 Redis 地址/前缀、原身份策略、profile 密钥和独立 Rota 控制凭据。页面部署之后，列入名单且通过中心实际就绪检查的节点可以接任务。

本机采集和迁移入口保持原样。第十三阶段增加了中心/节点强杀后的收尾：新中心持有独占锁后，停止旧运输任务，等待真实网络停止回执，再让原 BullMQ 任务按原预算继续。已提交的成功频道直接确认，不重复采集；API 和国家切换中途退出也保留原流程。节点重启先回传保存的结果和停止回执，不恢复已经丢失的浏览器会话。

过期且未被中心接收的结果保存在 spool 的 `.stale` 文件中，不作为业务成功，也不单独阻塞 Worker；仍计入磁盘上限。回执冲突、损坏文件或暂存区满仍会停止领取，需要排查。节点长期离线、磁盘遗失、没有停止回执时，对应槽继续等待，不凭超时认定安全。真实远程服务器尚未部署采集容器，验收详情见 [第十三阶段](../../docs/REMOTE_NODE_INGESTION_20260910.md#第十三阶段强杀后的恢复与持久结果确认)。

## 中心接入生产部署（2026-09-10）

独立容器 `qy-remote-node-center` 已上线，使用 `deploy/compose.remote-node-center.yml`。仅提供接入服务，`REMOTE_NODE_EXECUTION_ENABLED=false`；没有消费正式增量队列。节点 HTTPS 基址为 `https://newcrawdashboard.137-175-93-199.nip.io/node-execution`，nginx 仅转发 `/node-execution/v1/`，内部部署管理接口不对公网开放。

部署配置和凭据保存在被 Git 忽略的 `runtime/remote-center-production/`。在仓库根目录执行：

```bash
docker compose --env-file runtime/remote-center-production/compose.env \
  -f deploy/compose.remote-node-center.yml ps
docker exec qy-remote-node-center node scripts/checkRemoteNodeCenter.mjs
```

中心镜像构建文件为 `services/remote-node/Dockerfile.center`。入口默认监听回环地址；Compose 显式设置 `REMOTE_NODE_CENTER_BIND=0.0.0.0`，只在现有 Docker 网络监听 3187，不发布宿主机端口。数据库使用直接 PostgreSQL 连接，为后续 session advisory lock 保留会话语义。schema 工具 `applyRemoteNodeSchema.mjs` 默认只读；应用必须设置预期数据库名并传 `--apply`。

同日已完成生产 Rota 独立只读接口、中心私有镜像仓库及 Dashboard 部署控制接线。实际远程 Worker 仍由用户从页面手动部署；远程执行开关保持关闭，连接验收后再启用指定节点并验证真实频道。

回退本阶段可停止独立服务并恢复部署前的 nginx 配置后校验、热加载。配置备份为 `runtime/remote-center-production/nginx.before.conf`；同时撤回模板中的节点路由，避免 nginx 重建时重新生成。保留接入表和凭据，不删除原始采集数据。已有远程执行后不能直接照此停止，必须先排空。

后续维护新服务时需要同时带 `deploy/compose.remote-node-services.yml`，以保留中心的内部 TLS 与 Rota 路由地址。Dashboard 的生产接线使用 `SERVER_NODE_REGISTRY_CREDENTIALS_FILE` 读取匹配镜像主机的私有拉取凭据；节点只在下载期间创建临时 Docker config。公网 `/v2/` 仅允许经认证的 GET/HEAD，管理员通过中心回环端口 35000 上传。

2026-09-10 的 Dashboard 基于原线上镜像叠加节点功能，使用 `services/dashboard/Dockerfile.node-management` 保留当时尚未合入的迁移列表修复。2026-09-11 已归并 `agent/bugfix-optimization`，统一源码现已同时包含节点管理与这些迁移修复，后续可使用普通 Dashboard Dockerfile 构建。叠加构建文件保留作历史部署记录，不需要为本次源码归并重启正在迁移的服务。详细部署、测试与回退记录见 [第十五阶段](../../docs/REMOTE_NODE_INGESTION_20260910.md#第十五阶段生产路由读取私有仓库与页面部署接线)。

## 整频道执行版本

页面新增节点及扩容通过 NATS 部署时，安装包显式设置 `REMOTE_NODE_WHOLE_CHANNEL=true`，并检查容器实际采用该模式。中心需先完成 `--nats --whole-channel --apply` 显式数据库升级并启用同名开关。新旧模式仍使用共享 YouTubeJS/Rota 采集策略；API-batch 和发布留在中心。详见 [生产验证](../../docs/WHOLE_CHANNEL_PRODUCTION_ROLLOUT_20260913.md)。

升级已有节点必须先排空其正在执行的任务，再同步中心 `REMOTE_NODE_COLLECT_IMAGE`、Dashboard `SERVER_NODE_COLLECT_IMAGE`、中心部署登记和页面节点登记的镜像摘要。不能仅更新页面默认镜像，否则现有部署的身份校验会拒绝扩容。保持原有允许接任务数量和凭据。

### 删除空闲 Worker

页面位于「服务器节点 → 管理 Worker → 已部署的 Worker」。在线且没有执行/恢复任务的 Worker 可点击删除；状态未知的 Worker 不可删除。确认后先保存停止接单状态，等待监督器释放该 Worker 的执行锁，再由固定脚本移除它的容器并更新部署清单。不会删除频道、视频、评论、采集检查点或历史网络记录。允许删除至零个 Worker，之后仍可新增。

中心内部接口为 `POST /internal/node-deployments/retire`，沿用管理员鉴权。`reserve`、`ready`、`finish` 使用同一个操作 ID；`ready` 与 `finish` 都重新核实未完成任务并取得排他执行锁。新增两个退役字段需显式运行 `scripts/applyRemoteWorkerRetirementUpgrade.mjs --apply` 并提供数据库身份校验变量，启动过程不自动迁移。旧版本没有退役拦截能力，已有退役记录后不可直接回滚到旧中心。

## 全量节点部署包（P4，默认关闭）

全量使用独立镜像 `Dockerfile.full-crawl`，`role=fullcrawl`、`mode=full_crawl_collect`、`capability=youtube.full-crawl.v1`、`runtime_revision=youtubejs-full-crawl-v1`、`full-crawl-N` 槽位。基础采集运行环境只复用 Python、fingerprint gateway、Node 依赖和 relay；镜像复制本次源码并替换启动入口。构建参数 `COLLECT_RUNTIME_IMAGE` 必须由发布清单指定；发布构建使用仓库摘要，不能自行换用 latest。

```sh
docker build -f services/remote-node/Dockerfile.full-crawl \
  --build-arg COLLECT_RUNTIME_IMAGE="$VERIFIED_COLLECT_RUNTIME_DIGEST" \
  --build-arg QY_VCS_REF="$SOURCE_REVISION" \
  -t "$FULL_CRAWL_IMAGE_TAG" .
```

Dashboard 的 `SERVER_NODE_FULL_CRAWL_DEPLOYMENT_ENABLED=true` 与 `SERVER_NODE_FULL_CRAWL_IMAGE=<repository>@sha256:...` 显式开放部署。现有 Compose overlay 对应 `QY_REMOTE_FULL_CRAWL_DEPLOYMENT_ENABLED` 和 `QY_REMOTE_FULL_CRAWL_IMAGE`，默认关闭/空值。中心的独立 `center.env` 需要 `REMOTE_NODE_FULL_CRAWL_DEPLOYMENT_ENABLED=true` 和相同摘要的 `REMOTE_NODE_FULL_CRAWL_IMAGE`；增量镜像配置保持原值。两端使用原 HTTPS 管理端点和 TLS NATS/WSS 接入地址。

这个部署开关只允许登记、安装和连接验证，**不创建全量 BullMQ consumer**。`runRemoteNodeCenter.mjs` 使用关闭接单的 `createFullCrawlDeploymentRuntime`；部署后显示已连接、待命，开始接单返回中心未开放。P5 发布装配必须把真实 `createFullCrawlCenter` 的 `supervisor`、`activation` 与 `transport.service` 分别传给部署管理的 `fullCrawl.execution`、`fullCrawl.activation` 和 NATS 中心的 `fullCrawls`，并配置原业务 handoff、完整 legacy/repair 兼容 processor 和 API fallback。隔离验收已验证这种连接的就绪与排空；生产入口开放须另过 P5/P6 门槛。

每 Worker 固定内存保护上限 768 MiB、CPU 0.5、PID 128、spool 256 MiB。宿主 `/var/lib/qy-node/spool/full-crawl-N` 挂载到容器 `/var/lib/qy-node/full-spool`；UID 1000、目录 700、凭据文件 600、容器根文件系统只读。Rota 总容量为固定本地槽位加所有活跃节点登记数量，重试不累加；fullcrawl 仍使用 Rota `channel` role。

扩容保留原 deployment ID、已有 slot、配置 hash 和凭据；已删除 slot 留墓碑，不复用。先暂停并排空，再逐 Worker 退役删除容器，保留配置、spool 和中心任务/结果证据；API 等待在原 attempt 结束且实际网络退役后不占采集槽位。已有历史的服务器登记不会当作“未使用节点”直接删除。更换镜像不能绕过现有部署身份与排空校验。

P4 隔离记录和页面状态对照见 `docs/FULL_CRAWL_REMOTE_P4_DEPLOYMENT_20260920.md`。本阶段没有迁移生产 schema，也没有修改生产节点、接单额度或部署。
