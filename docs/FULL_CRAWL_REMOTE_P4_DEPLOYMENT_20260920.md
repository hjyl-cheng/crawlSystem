# P4：全量节点部署和管理页验收

日期：2026-09-20（UTC）。范围：W12，仅隔离环境。P4 实现及分层验收已完成；生产全量接单仍关闭。下一阶段为 P5 发布前验收。

## 实现与默认边界

- `Dockerfile.full-crawl` 固定全量入口，复用已验证的采集基础运行环境。全量身份为 `fullcrawl / full_crawl_collect / youtube.full-crawl.v1 / youtubejs-full-crawl-v1`，slot 使用 `full-crawl-N`。
- Dashboard 按服务器登记类型选择独立固定摘要镜像，接通预览、初始化、部署进度、重试、增加数量、接单额度、开始/暂停及 Worker 删除。页面的部署能力也受全量专用开关控制；未配置时禁用全量部署按钮。
- 新部署默认 `activation_requested=false`，登记和扩容不修改已有接单状态。增量 config 字节及 hash 不变；不能以增量镜像登记全量配置，不能把已部署服务器改成其他功能类型。
- 全量每进程内存上限 768 MiB、CPU 0.5、PID 128、spool 256 MiB；全量暂存路径独立。Rota 的 `channel` 总容量汇总固定本地槽位与全部活跃远程部署；登记重试不会重复增加数量。
- 接单配置持久化在原 intake 表，页面就绪状态按 workload 调用真实 activation 验证器。真实 supervisor 集成测试发现并修正了全量心跳 ready 而页面错误显示 unready 的问题。
- 删除先持久化退役请求、停止领取，再取得原 supervisor 独占锁确认排空，最后删除容器并完成登记退役。旧 slot 留墓碑；配置、凭据和 spool 保留。全量原 attempt 未结束或网络没有实际零在途退役回执时拒绝删除。已结束且安静的 API handoff 不占采集槽位，删除 Worker 不删除其中心结果证据。
- 修正旧 `workerCountSchema.sql`：允许登记 Worker 数量降为 0，避免重新应用该升级后无法删除最后一个 Worker。此 SQL 只在隔离库执行。

专用部署开关为 Dashboard `SERVER_NODE_FULL_CRAWL_DEPLOYMENT_ENABLED` 和中心 `REMOTE_NODE_FULL_CRAWL_DEPLOYMENT_ENABLED`，均默认关闭。两端还需配置相同摘要的 `*_FULL_CRAWL_IMAGE`；原增量镜像不变。

P4 的可运行中心入口接入 `createFullCrawlDeploymentRuntime`，只登记、连接和保留证据，**不创建全量 consumer**；因此普通 P4 部署验证的正确状态是“已连接、待命、额度尚未启用”。隔离执行验收另使用真实 `createFullCrawlCenter`，把其 supervisor/activation 接入同一个部署管理 API，验证实际 ready、开始和排空。P5 必须把这份执行装配与原 handoff、完整 legacy/repair 本地 processor、API fallback 一起固定为发布入口；不能把部署开关当作生产接单开关。

## 页面状态对照

| 条件 | 管理 API / 页面结果 |
| --- | --- |
| 全量部署开关关闭或镜像缺失 | 预览不可用；部署按钮关闭；API 拒绝部署 |
| 登记成功，镜像下载失败 | 步骤 pull=failed；保留 deployment ID、已有数量、凭据和暂存；允许重试 |
| 容器运行但未收到每个 slot 的有效心跳 | 部署未完成；不承认新增数量，不启用接单 |
| 每个 slot 已连接，执行装配未开放 | connected；待命；ready=false；开始接单被中心拒绝 |
| 真实 supervisor、队列、Rota 与 transport 均就绪 | ready=true；按保存额度接单 |
| 正在执行时暂停 | allowedCount=0；保留 configuredCount；显示收尾；原任务锁继续续期 |
| 中心重建 | 从 SQL 恢复额度和暂停状态，不自动扩大接单 |
| Worker 删除失败 | 保留同一 operation ID 和 slot，重试继续；不提前减少数量 |
| 排空且容器删除确认 | 中心 worker_count 减少；不复用已删除 slot；spool/SQL 证据保留 |

状态对照通过管理 HTTP API、真实 PostgreSQL 与 BullMQ 验证；没有把浏览器截图或公共 YouTube 样本当作本阶段证据。

## 隔离测试

结果及源码 SHA256 清单见 `reports/fullcrawl-p4-validation-20260920.json`。日志位于 `runtime/fullcrawl-p4-20260920/logs/`。

| 验证组 | 通过 / 失败 / 跳过 | 证据 |
| --- | --- | --- |
| Dashboard 类型、安装计划、数量、接单、注册回归 | 27 / 0 / 0 | dashboard-unit.log |
| 管理路由及全量/增量 PostgreSQL 部署、失败重试、扩容、删除恢复 | 7 / 0 / 0 | dashboard-postgres.log |
| 中心全量/增量登记、容量、退役与最后一个 Worker 删除 | 4 / 0 / 0 | center-deployment.log |
| 实际全量与增量 BullMQ supervisor、页面 ready/排空、原任务续锁 | 4 / 0 / 0 | supervisor.log |
| 全量真实 SSH/SFTP/sudo 安装器 | 11 / 0 / 0（10 个子场景及父测试） | ssh-full.log |
| Python 安装/退役脚本 | 7 / 0 / 0 | installer-python.log |
| 全量节点、TLS/WSS NATS、API 交接、旧连接及恢复回归 | 16 / 0 / 0 | full-runtime-regression.log |
| 固定摘要全量镜像生命周期 | 1 / 0 / 0 | package.log、package/acceptance.json |

这些组包含共享回归，不把数字相加当作独立业务场景总数。`git diff --check` 通过。

SSH 夹具使用真实 SSH、SFTP、sudo 和安装器文件写入，Docker/包管理器由夹具模拟，未挂载宿主 Docker socket。旧夹具缺少当前 image inspect/pull 命令及过时的步骤断言已修正；干净夹具最终十个场景全部通过。

镜像生命周期测试另外使用真实 Docker 容器、TLS NATS、PostgreSQL 提交通知及镜像内 relay/full node，独立验证完整依赖、只读根文件系统、UID 1000、独立 spool、中心重启、节点重启、关闭接单、容器删除和证据保留。重启夹具在节点停止后显式模拟心跳过期，不以时间到期代替业务任务排空证明。该测试不消费公共 YouTube 任务；真实任务消费、阶段执行和排空由 supervisor/P3 回归覆盖。

## 固定产物、回退和下一阶段

专用镜像最终摘要、基础镜像 ID、实际节点/deployment ID、源文件 hash 记录在机器报告中。本次构建复用本地已有基础镜像；其导入摘要无法由 BuildKit 解析时使用已核对 ID 的本地标签构建。P5 发布构建须验证基础镜像的实际仓库 manifest digest，并把可分发产物推入发布仓库固定摘要；P4 没有对外发布镜像。

隔离回退演练：部署失败保留原登记 → 原 ID 重试；暂停新领取 → 原任务正常排空并保持 BullMQ 锁；退役过程中断 → 原 operation ID 继续；删除最后一个 Worker → 计数归零但保留 spool 和 SQL 证据。已有历史的服务器登记仍不能通过“未使用节点删除”绕过退役；本阶段提供的是 Worker 退役删除，不物理删除业务历史。

本轮一次性 SSH、镜像节点、测试中心、测试 NATS 和临时 supervisor Redis 在验收后清理，复用的 P2/P3 基础测试环境保留。没有修改生产数据库、生产容器、增量接单额度或节点凭据。

下一步 P5：固定发布级中心执行装配和原兼容 processor；完成计划要求的来源/合约/详情/评论/国家/API 故障矩阵；验证可分发镜像摘要、schema 发布顺序、回退步骤和增量完整回归。P5 通过前不能进入生产单 Worker 灰度。
