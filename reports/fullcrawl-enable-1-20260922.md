# 启用 full-crawl-1

按用户最新指令“启动 full-crawl-1”执行；该指令取代此前对该槽位的暂停要求。未修改程序源码，沿用已发布的 `8313058` 修复版本。

## 操作

- 保留并备份历史任务暂存，使用修复镜像启动 full-crawl-1；启动时间为 2026-09-22 03:31:33 UTC。历史恢复记录已自动处理。
- 远程 Compose 移除该槽位的 paused profile，恢复 `restart: unless-stopped`。
- 中心配置移除该槽位的维护暂停项。等待任务收尾后优雅重载中心，旧进程于 03:40:57 UTC 正常退出，退出码 0；新中心于 03:41:08 UTC 通过健康检查并完成配置持久化。
- 恢复全量接单，选中全部 10 个 worker。旧固定槽位上限和节点白名单配置仍不存在。

## 验收

- 03:43:44 UTC 远程检查：10 个全量容器均运行修复镜像，状态 running，重启次数均为 0；full-crawl-1 的启动配置已持久保存。
- 03:43:55 UTC 观察到 full-crawl-1 已连接、已启用、未暂停、就绪且正在处理任务。
- 03:44:12 UTC 状态断言 PASS：全量部署、允许、连接、就绪均为 10；三个增量节点分别 15/15、20/20、47/47 就绪，总计 82/82。
- 03:44:31 UTC 中心检查：healthy、重启次数 0；运行环境和持久化配置均已解除目标槽位暂停，旧槽位上限及白名单配置均未恢复。

以上为启动及就绪检查，不代表所有业务采集任务均成功。

## 证据和备份

运行证据位于 `runtime/fullcrawl-enable-1-20260922/`，包括 `finish.log`、`final-status.json`、`remote-final.json` 和 `center-final.json`。该目录的 `private/` 包含受限配置备份，不应纳入 Git。

远程暂存备份：`/var/lib/qy-node/backups/fullcrawl-enable-1-20260922/full-crawl-1.tar`。远程部署目录保留 `compose.before-enable-full-crawl-1-20260922.json`。

中心旧容器 `qy-remote-node-center-before-fullcrawl-enable-1-20260922` 已停止并关闭自动重启，供必要时核对和回退使用。
