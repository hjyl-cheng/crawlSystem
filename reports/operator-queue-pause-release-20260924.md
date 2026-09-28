# 监控队列人工暂停修复

2026-09-24 已更新生产 API 和迁移 Controller，并暂停 `youtube-channel-crawl`。

## 原因

监控页面的暂停按钮发送 `PUT /queues/api/queues/youtube-channel-crawl/pause`。原 controlled-canary 写保护返回 HTTP 423，错误为 `non-Migration writes are disabled during the controlled canary`。

绕过页面直接调用 BullMQ pause 也不能保持暂停：控制器每 2 秒协调迁移队列，已完成批次按 `migration_idle` 恢复消费。此前直接暂停在约 291 毫秒后被恢复。

## 修复

- 精确放行上述单个队列的 PUT pause/resume 路由；清空、删除、批量暂停及其他队列写入仍受现有保护。
- 在 Bull Board 通用处理器前接入人工控制路由，将暂停意图保存到 `crawler.settings` 的 `migration_operator_queue_pauses`。
- 人工控制与协调器共用调度器行锁和 Redis 单调版本保护；协调器优先保持人工暂停，原有 legacy 控制路径也遵守该意图。
- 继续按钮清除人工暂停并通知控制器。控制器按迁移状态和代理容量决定何时恢复；API 不新增代理容量凭据。页面可能在下一次协调及刷新后显示恢复。

## 验证

原线上按钮请求已复现 HTTP 423；回归测试在修复前断言失败（423 != 200）。修复后本机及最终 Node 20 API 镜像均为 18 项针对性测试通过、0 失败、0 跳过，包含真实独立 PostgreSQL/Redis、HTTP 路由、持续协调、控制对象重建、版本栅栏、延迟/优先任务和容量限制。临时测试容器已删除。

生产服务从各自当前镜像派生，仅替换本次涉及的源码；API 的队列控制模块与当前 Controller 策略对齐。未打包工作区其他功能的修改。旧服务正常退出码均为 0；原环境、网络、资源与重启配置保留。

- Controller 镜像：`qy-allpachong/qybullmq:operator-queue-pause-20260924-controller`
- API 镜像：`qy-allpachong/qybullmq:operator-queue-pause-20260924-api`
- 09:57:40 UTC：实际按钮接口返回 HTTP 200、`paused: true`。
- 09:57:45、09:57:50、09:57:55 UTC：页面队列接口均返回 `isPaused: true`、active=0；控制版本推进至 92335、92338、92340，原因持续为 `operator_paused`。

已验证页面所用 HTTP 接口，未进行带登录会话的浏览器点击测试。

## 发布与回退

发布脚本和构建目录：`runtime/operator-queue-pause-20260924/`。受限 `private/` 中保存原容器快照、新容器重建配置和镜像 ID；新容器的 `qy.runtime.create_spec` 指向该配置。旧容器保留为 `*-before-operator-pause-20260924`，已停止且禁用自动重启。

回退入口：`python3 runtime/operator-queue-pause-20260924/release.py rollback api`，以及 `rollback controller`。回退 Controller 会失去对人工队列暂停意图的支持，旧自动策略可能恢复队列；需要保持人工暂停时不要直接回退到旧控制器。
