# 远程 NATS 接入与查询优化：2026-09-11

本轮已部署中心接入服务、节点 01 的 20 个远程增量 Worker、本地 20 个增量 Worker 和 Dashboard。最终接单配置保持中心 0、节点 01 为 20。增量队列已恢复；40 个迁移 Worker 保持原镜像运行。

## 通信和查询

- BullMQ 继续负责原来的频道任务；中心原来的 Plan runner、任务代次、Rota 网络所有权和结果写入校验继续生效。
- 节点通过 `wss://newcrawdashboard.137-175-93-199.nip.io/node-messages` 建立官方 NATS 长连接。Nginx 只负责 WebSocket 升级；不进入 Dashboard 请求处理，也不使用 HTTP 轮询。4222 在节点公网测试中连接超时，本机监听正常，故采用已经开放的 443 入口。
- 中心通过 Docker 内部的 TLS NATS 连接访问同一消息服务。节点独立凭据和 subject 权限由既有加密登记生成。
- 采集指令仍以 PostgreSQL 为持久依据，事务提交后的 LISTEN/NOTIFY 唤醒等待请求；JetStream 持久保存结果消息。节点收到 SQL 写入确认后才移除本地待上传文件。
- 等指令、等结果、等领取、等旧网络释放的四处 100 ms 循环，在 NATS 路径使用通知唤醒。等待超时或通知丢失后重新核对 SQL，不依赖通知本身保证数据完整性。
- 中心远程接单协调由每秒扫描改为配置/状态变化通知，5 秒补查。普通心跳更新时间不触发全体扫描；断线后的重新上线会通知。
- 本地接单控制由每 2 秒检查改为设置变化通知，5 秒心跳兼补查；业务连接仍走原 PgBouncer，LISTEN 使用明确配置的 PostgreSQL 直连。
- NATS 凭据由变更通知触发同步，60 秒补查。官方服务验证配置后热重载授权和证书。

继续保留：节点/任务/网络续租、页面 5 秒刷新、迁移与发布调度循环、短暂的网络绑定失败重试。长连接不能替代任务所有权核验和数据库提交。

## 验证

- 原生 TLS NATS：27 项 PostgreSQL、Redis 和消息服务集成测试通过。
- 正式网关相同路由的 WSS NATS：27 项集成测试通过。包含完整 About Plan、空上传列表休眠、含视频详情的 Video Plan、数据库字段核对、旧代次拒写、重复结果、事务回滚、权限热重载/撤销及中心进程正常关闭。
- 节点生命周期、并发接入、持久 spool、API 延后与回到网络执行：21 项回归测试通过。
- 页面配方及实际 Python 安装器：3 项测试通过；NATS/WSS 配置不改变已有节点 config hash。
- 正式节点 WSS 证书验证及 HTTP 101 升级通过，20 个新 Worker 全部健康、在线、可接单。
- 正式队列保持暂停且无活动任务时测试远程 20→19→20、本地 0→1→0，均恢复原值。一次状态收敛观测分别为 39 ms、1,124 ms、11 ms、11 ms；这是本次控制验证，不代表公网延迟保证。
- 观测到 NATS 心跳和等待请求均无拒绝、超时、积压。部署前后增量队列原有失败计数均为 23，本轮没有重试或改写这些历史失败任务。

当前没有待执行频道，未做正式满负载频道吞吐测试。消息服务使用单副本，不能据此宣称磁盘丢失或多机故障容错；单中心 Worker 容量限制也没有在本轮提高。传输回执保存的是小型确认记录，尚未配置独立的历史保留清理任务。

## 部署与恢复

镜像标签为 `nats-20260911`；远程节点不可变镜像：

`newcrawdashboard.137-175-93-199.nip.io/qy-node-incremental@sha256:de79191630c821f117e5c0563736a17c0821a24a1567c55228fda7537e9c441b`

节点镜像已推送中心私有仓库；中心和页面登记在确认实际部署成功后同步到该 digest。页面后续添加 Worker 会继续使用此镜像和 WSS 地址。

- 消息服务正式配置：`runtime/remote-center-production/nats/compose.json`。证书只读挂载现有证书目录，支持续期后的重载；原始 TCP 端口仅映射到本机。
- 中心配置：`runtime/remote-center-production/center.env` 及现有 center/services Compose 文件。中心基础模板已包含 NATS 凭据目录挂载。
- 页面配置：`runtime/remote-center-production/dashboard.compose.json`。
- 本地 Worker 通知覆盖：`deploy/compose.local-intake-notifications.yml`，读取受限 `runtime/remote-center-production/nats/local-intake.env` 中的直连 URL。现有容器的环境和 22 份完整回滚配置保存在受限 `nats-rollout` 目录。
- 节点保留原 deployment ID、slot、config hash 和 spool；节点原 Compose 保存为 `compose.before-nats-20260911.json`。

回滚也必须先关闭新接单并等待在途任务完成，再恢复中心/节点镜像与相匹配的登记；不要让同一 slot 同时运行两种传输。原来的 22 个中心侧容器已停止并保留回滚，不应直接一起启动。原采集数据及新加的回执表无需为回滚而删除。
