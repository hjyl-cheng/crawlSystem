# 发现队列恢复按钮修复

2026-09-24 已发布，并通过页面同一路由恢复 `youtube-discover-page`；`youtube-channel-crawl` 继续保持人工暂停。

## 原因与行为

原 `PUT /queues/api/queues/youtube-discover-page/resume` 被 controlled-canary 写保护返回 HTTP 423。此前人工队列控制只覆盖 `youtube-channel-crawl`，且迁移协调器始终把发现队列设为 `controlled_migration` 暂停。

现在两个队列共享精确的 pause/resume 路由名单。发现队列的人工选择使用既有 `crawler.settings.migration_operator_queue_pauses` 持久化：true 表示人工暂停，false 表示显式申请开启。协调器在迁移自然完成、调度器自然停止后允许发现消费队列待命，仍检查发现代理容量和频道、详情、API、Agent 队列积压，使用现有积压阈值及恢复低水位。未知容量或积压读数保持暂停。

迁移仍在执行或调度器被人工暂停/停止时，恢复发现队列返回可解释的 HTTP 409。开启队列不会启动 Query 调度，也不解除词库导入的迁移写保护。正常发现周期不绑定迁移批次时，既有主控制器仍负责正常发现限流。

## 验证

- 线上同一恢复请求复现 HTTP 423。
- 修复前新增权限和策略回归均失败；修复后本地与最终 Node 20 API 镜像均通过 20 项针对性测试，0 失败、0 跳过。
- 独立 PostgreSQL/Redis 测试覆盖 HTTP 恢复、连续协调、控制对象重建、人工暂停独立性、积压暂停与恢复、活动迁移拒绝恢复，以及之前的队列版本栅栏等回归。
- 10:31 UTC 实际恢复接口返回 HTTP 200、pending=true，由控制器应用。
- 10:31:15、10:31:20、10:31:25、10:31:30 UTC 页面接口返回发现队列 isPaused=false、频道抓取 isPaused=true，两者 active=0。控制版本从 93339 推进至 93347，原因分别持续为 operator_resumed / operator_paused。
- API healthy；API、Controller 均运行且重启次数为 0。

验证使用页面所用的 HTTP 路由，未进行带登录会话的浏览器点击测试。没有导入词库或开始新的发现批次。

## 发布与回退

仅替换当前 API 和 Controller 镜像中的相关模块，不包含工作区其他功能改动。环境、网络、资源和重启配置保留，旧容器正常退出码均为 0。

- API：`qy-allpachong/qybullmq:operator-discover-resume-20260924-api`
- Controller：`qy-allpachong/qybullmq:operator-discover-resume-20260924-controller`
- 构建、发布脚本及受限重建/回退快照：`runtime/operator-discover-resume-20260924/`
- 旧容器后缀：`-before-discover-resume-20260924`，已停止并禁用自动重启。

回退入口为该目录 `release.py rollback api` 和 `release.py rollback controller`。回退 Controller 后旧策略会重新暂停发现队列；频道抓取的人工暂停仍由上一版本支持。
