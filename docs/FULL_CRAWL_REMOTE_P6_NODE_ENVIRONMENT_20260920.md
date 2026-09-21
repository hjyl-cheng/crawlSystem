# P6 专用全量节点环境核验

2026-09-20 07:25–07:28 UTC。结论：用户新增的专用节点基础环境满足单 Worker 灰度准备要求；服务器缺失这一阻塞已解除，生产发布和接单条件尚未全部满足。本次仅执行只读核验，没有安装、部署、迁移、修改接单或创建节点凭据。

机器证据：[fullcrawl-p6-node-environment-20260920.json](../reports/fullcrawl-p6-node-environment-20260920.json)。原始证据和可重用 SSH 探针位于 `runtime/fullcrawl-p6-20260920/`。探针使用 Dashboard 已存密钥及已固定主机指纹，不生成新密钥，不输出密钥内容。

| 项目 | 实测结果 |
| --- | --- |
| 登记 | 全量采集节点，`8a07de4f-a3ee-428f-a959-2aee9c7b6be7`，`43.172.77.209:22`，用户 ubuntu，role=fullcrawl |
| 初始化 | provisioning=ready、runtime=ready，无部署记录，Worker 计划为空 |
| SSH/权限/归属 | 密钥登录成功、固定指纹一致、sudo 可用、远端 node-id 与登记匹配 |
| 系统 | Ubuntu 24.04.4 LTS、x86_64、2 vCPU |
| 内存 | 总计约 7.51 GiB（标称 8 GB），可用约 6.88 GiB；swap 约 1.94 GiB，未使用 |
| 磁盘 | 分区约 78.63 GiB（标称 80 GB），可用约 69.50 GiB，使用率 8%，inode 使用率 3% |
| 负载 | 1/5/15 分钟约 0.007/0.055/0.024；监控 CPU 约 1% |
| Docker | 29.8.1，Compose 5.5.1，cgroup v2/systemd，支持内存/PID 限制；0 容器、0 镜像 |
| 监控与时钟 | Beszel 在线且样本新鲜；监控服务 active；NTP 已同步，系统时区 Asia/Shanghai |
| 目录 | runtime secrets/spool 均 root:0700、为空；节点专属 Worker spool 尚未创建，属于后续部署步骤 |
| 中心 HTTPS | 实际 `/node-execution/v1/node/heartbeat` 路由无认证 GET 返回 401，证书校验通过 |
| 正式镜像仓库 | `/v2/` 无认证访问返回 401，TLS 正常；未携带拉取凭据、未拉取镜像 |
| 消息入口 | DNS 正常；TLS 1.3 验证通过，证书到期 2026-11-18；`/node-messages` 返回 101 Switching Protocols |

401 是此次未携带节点/仓库认证的预期结果；101 证明 WSS 路由可达，不代表节点已认证、已注册或可领取任务。节点尚无 Worker，因此 Rota grant、节点 profile、逐槽 spool、停止回执和真实采集仍需在发布部署阶段验收。

该节点可以先配置 1 个全量 Worker。虽然内存足够容纳更多容器，但只有 2 vCPU，不能以空闲快照推断可稳定运行 5 个 Worker，扩容须按实际灰度数据决定。

## 中心侧待办

- Dashboard 全量部署开关仍关闭，`SERVER_NODE_FULL_CRAWL_IMAGE` 未配置；中心 deployment/execution 开关也未启用。
- 五张全量核心表仍不存在，需执行 P5 显式兼容迁移和受控中心升级，并保留旧增量修复与接单配置。
- 中心主机仍为 16 CPU，复查负载约 22.2/21.0/20.6，磁盘已用 88%。节点闲置资源不能代替中心共享资源预算；需继续落实兼容槽和数据库/消息处理余量。
- 现有 47+15 个远程增量 Worker 全部 connected/enabled/accepting。中心 transport health=200，结果 pending/unacknowledged=0；RPC 超时累计 15，较上一轮未增长。
- 全量镜像实际分发和拉取、完整节点认证连接、原增量发布前基线及灰度观察尚待执行。

下一步是完成中心资源与发布准备，然后部署一个全量 Worker；无需继续寻找或添加另一台全量服务器。P6 的 30 分钟/10 个有效全量频道验收尚未开始。
