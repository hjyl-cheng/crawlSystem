# P6 全量 Worker 执行准备

2026-09-20，最终验收时间：08:36 UTC（增量恢复验收 08:36:16）。**执行准备完成，全量接单保持 0；尚未开始真实频道灰度，不代表 P6 全部验收通过。**

机器报告：[fullcrawl-p6-execution-ready-20260920.json](../reports/fullcrawl-p6-execution-ready-20260920.json)。此前连接阶段记录见 [P6 生产连接发布](FULL_CRAWL_REMOTE_P6_RELEASE_20260920.md)。本阶段沿用 P5 固定镜像，未修改业务源码。

## 本次完成

- 启用中心 `REMOTE_NODE_FULL_CRAWL_EXECUTION_ENABLED=true`，保留原有增量环境配置，补齐原兼容 processor 所需数据库、Redis、profile、代理和业务参数。配置经无网络临时容器校验，实际密钥文件匹配通过。
- 本地 channel 预算从 61 调整到 62，Rota 总容量补到 125：本地 62（含中心兼容槽）+ 远程增量 62 + 远程全量 1。全量执行总预算 2 = 远程 1 + 兼容 1。
- 中心在 08:26:17 UTC 开始自然排空，08:28:41 完成替换，旧进程退出码 0，无强杀。新中心 healthy、0 重启，兼容槽 `fullcrawl-center-compatibility-p6` 已取得 Rota 槽位，执行运行时启动事件确认预算 2/1/1。
- 数据库身份为 `newcrawler_crawler`；全量执行预算的独占 advisory lock 恰好 1 个。未创建远程全量任务或执行记录。
- 全量节点 `8a07de4f-a3ee-428f-a959-2aee9c7b6be7`（43.172.77.209）单个 Worker 连接正常，`executionAvailable=true`，`allowedCount=0`、`intakeEnabled=false`、`readyForTasks=false`。这里未就绪是零接单的预期状态。
- 节点容器 running、0 重启，768 MiB、PID 128、非 root 用户、只读根目录、持久 spool 挂载正确；节点身份、固定 SSH 指纹、监控、TLS/WSS 检查通过。
- 原 47+15=62 个增量 Worker 全部恢复 connected/enabled/accepting，连续三次 20 秒采样均有成功完成事件。原接单 selected_slots、revision、updated_at 逐项保留。
- 中心实际 Compose 清单已持久化，解析后的镜像、全部环境变量、内存和 PID 配置与运行容器一致。

## 验收边界和下一步

本轮没有导入频道、开放接单或领取远程全量任务。生产实际任务的 route/profile、停止回执、结果入库和业务发布仍需在首次灰度执行中验证；本轮仅证明运行时可以启动、节点可连接、预算和配置已就绪。

排空和逐槽恢复暂时降低增量吞吐，恢复期存在少量任务超时；成功恢复不等于零失败，也不等于新增全量负载下的性能验收。维护窗口不计入可比基线。实际放行前仍需至少 30 分钟稳定增量基线及共享 CPU/IO、数据库等待、磁盘趋势检查。本次预检主机可用内存约 5.3 GiB、磁盘剩余约 98 GiB，只是瞬时资源快照。

下一步先核对原队列与有效频道供给。若供给不足，再少量导入真实、符合条件且未完成的频道，避免重复生产任务。只开放 1 个远程全量槽，观察至少 30 分钟并完成至少 10 个有效频道，验证入库、发布、路由/profile 与停止回执；通过后才考虑扩容。

## 持久化与回退

过程脚本与脱敏证据位于 `runtime/fullcrawl-p6-20260920/release/`；私有配置位于其 `private/`，目录 0700、文件 0600，不进入 Git。当前中心固定镜像摘要继续为 `sha256:3296e0621883fda8755304780d7ff54b910feab6034a490d6297bcd019430675`。

切换前连接阶段容器 `qy-remote-node-center-before-p6-execution` 已保留，自动重启关闭，其配置见 `private/qy-remote-node-center.execution.old.json`。此前更早阶段备份继续保留。回退前重新核对零接单及在途/未确认结果，平滑停止执行中心并释放预算锁，再恢复连接阶段容器及对应持久配置；保留 schema、凭据、任务证据、检查点和 spool。
