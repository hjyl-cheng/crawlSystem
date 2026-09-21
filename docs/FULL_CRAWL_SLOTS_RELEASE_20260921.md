# 全量中心槽位调整：2026-09-21

按用户要求，将运行中中心的 `REMOTE_NODE_FULL_CRAWL_TOTAL_SLOTS` 从 2 调整为 6。
远程全量 Worker 上限为 5，另保留 1 个本地兼容槽。

- 使用原镜像重建 `qy-remote-node-center`，仅更改上述环境变量。
- 等待现有任务收尾后正常退出，旧容器退出码为 0，未强制终止。
- 已持久化到当前 Compose 来源：`runtime/worker-slot-recovery-20260917/center.deploy.private.json`；Compose 配置解析通过。
- Rota channel 容量由 127 提高到 129，并验证 129 个已就绪，覆盖本地 62、远程增量 62、远程全量最多 5。
- 2026-09-21 03:46:47 UTC 验证：全量 3/3 就绪、未就绪 0；增量 62/62 就绪且正在执行；中心 healthy、重启次数 0。
- 当前仅登记并放行一台全量服务器，其 3 个 Worker 均可接单。未来增加全量服务器仍需更新 `REMOTE_NODE_FULL_CRAWL_NODE_IDS` 白名单；总槽位变量不会自动授权新节点。

不含凭据的验证报告：`reports/fullcrawl-slots-20260921.json`。
操作脚本和恢复过程：`runtime/fullcrawl-slots-20260921/`。
原容器保留为 `qy-remote-node-center-before-slots6-20260921`，已禁用自动重启。
私有目录保存原容器及 Compose 配置；如需回滚，应先排空并停止新中心，再恢复原容器和原 Compose，避免同时运行两个中心。
