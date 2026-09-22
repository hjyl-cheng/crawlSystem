# 全量 Worker 恢复与中心连接池修复

发布版本：`83130582ae91ae7aea2ffbe588b54a7633fd6a46`。

## 修复内容

- 新网络会话记录持久化原始连接身份，避免关闭会话恢复时产生 INVALID_WORKER_CONNECTION。
- 历史记录缺少身份时，仅从同任务、同执行代次、同节点及槽位的持久化 claim 补取；不借用新进程身份，不绕过中心校验。记录不匹配时保留现场并报 FULL_CRAWL_RECOVERY_IDENTITY_MISMATCH。
- 全量执行器保留不可恢复协议错误，避免将原始原因掩盖为 NODE_EXECUTOR_STOPPED。
- 监督锁模块不再缩减调用方配置的连接池，保留全局锁加四组监督锁需要的五个连接。
- 中心启动失败日志增加失败阶段，并明确标识 SUPERVISION_POOL_TIMEOUT。

## 测试

- 新增本地回归 10 项全部通过，修复前已确认失败。
- QYBullMQ：1807 项通过，277 项按环境条件跳过。
- Dashboard：99 项通过，12 项按环境条件跳过。
- 实际中心镜像隔离集成：57 项通过，0 失败，0 跳过。覆盖真实 PostgreSQL 五个锁会话、多节点扩容、暂停保持、NATS 全量执行与增量网络恢复。
- 实际节点镜像：8 项恢复测试通过，0 跳过。
- `scripts/verify.sh` 通过；`scripts/test.sh` 在既有 Auth `gatewayConfig.test.js` 断言失败处停止，相关代码未修改。未声称整个仓库测试全部通过。

## 产物

- 中心：`qy-allpachong/qybullmq:pachongsys-8313058-fullcrawl-fix-20260922`
- 节点：`newcrawdashboard.137-175-93-199.nip.io/qy-fullcrawl-node@sha256:310e98b063f63a193e7f99a4c4932627a2a5f7f132b454f026a2dbd17f2a3c85`
- Dashboard 程序镜像未变，部署配置将引用新的全量节点摘要。

## 发布证据

03:09 UTC 暂停全量接单；03:10:47 全量任务收尾完成。先更新 full-crawl-10，03:11:58 核验其运行超过一分钟、重启 0 次、旧 network.json 与 claim.json 自动清理。随后更新 full-crawl-2 至 9。

03:16:50 远程核验：9 个应启用容器均使用新版本、running、重启 0 次。full-crawl-1 仍 exited，历史重启数 820 未增加。暂停容器未启动；其 Compose 镜像引用已更新，供将来明确解除暂停后使用。

任务暂存目录完整保留。替换前在远程受限目录 `/var/lib/qy-node/backups/fullcrawl-worker-fix-20260922/` 保存各 worker 的暂存备份；配置备份位于原部署目录的 `compose.before-worker-fix-20260922.json`。

运行证据、测试日志及受限回退配置位于 `runtime/fullcrawl-worker-fix-20260922/`。不将该目录中的私密容器配置纳入 Git。

## 最终验收

发布完成。03:20:54 UTC 中心优雅替换完成，旧进程退出码 0；03:21:29 Dashboard 配置切换完成，旧进程退出码 0。03:22:14 恢复全量接单。

03:24:08 UTC 状态断言 PASS：全量部署 10、允许 9、连接 9、就绪 9、未就绪 0；full-crawl-1 为维护暂停。三个增量节点分别 15/15、20/20、47/47 就绪，总计 82/82。

03:23:31 UTC 远程采样以及最终暂停检查：9 个新容器均 running、重启数 0，没有进程失败日志；full-crawl-1 为 exited、restart=no、Compose paused profile 保留，历史重启数未增长。观察窗口内未再出现 NODE_EXECUTOR_STOPPED。

中心 PostgreSQL 证据确认恰有 5 个全量锁会话：1 个全局锁和 4 组监督锁；full-crawl-5、9 的锁均已持有。发布后日志未出现 remote_center_slot_start_failed 或 SUPERVISION_POOL_TIMEOUT。

中心及 Dashboard 均 healthy、重启数 0；运行配置和持久化配置均引用修复节点摘要。中心暂停名单保留，旧的固定槽位上限和节点白名单环境变量仍不存在。登记数据库和 Dashboard 节点记录也已同步新镜像，后续扩容使用修复版。

业务任务仍可能因现有代理/执行预算策略失败：本次观察到 full-crawl-8 一次 EXECUTION_ROUTE_BUDGET_EXHAUSTED，但其进程没有退出或重启，后续仍就绪。该业务错误不等同于此次已修复的恢复循环；本报告不将“就绪”解释为所有采集任务均成功。

## 回退与工作区

- 中心旧容器：`qy-remote-node-center-before-fullcrawl-worker-fix-20260922`，已停止且关闭自动重启。
- Dashboard 旧容器：`qy-newcrawler-fresh-dashboard-1-before-fullcrawl-worker-fix-20260922`，已停止且关闭自动重启。
- 旧节点镜像摘要保留为 `sha256:eef3acf34ec19620d0baf111388136f0d622377831075d2576c3057ac5979164`；回退必须同时一致更新运行容器、远程 Compose、登记记录及中心/Dashboard 引用，并保留 full-crawl-1 暂停。旧版包含本次缺陷，不建议常规回退。
- 精确发布源码已提交到 `codex/fullcrawl-worker-fix-20260922`，隔离工作树 `/tmp/pachongsys-fullcrawl-worker-fix-20260922` 干净。原工作区的既有用户修改完整保留；本次修复同步保留在原工作区以供查看。
