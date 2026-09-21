# P6 生产连接阶段发布

后续更新：08:36 UTC 已完成 [P6 执行准备](FULL_CRAWL_REMOTE_P6_EXECUTION_READY_20260920.md)，execution 已开启、接单仍为 0。本文保留 08:09 UTC 连接阶段的历史快照。

2026-09-20，最终验收快照约 08:09 UTC。状态：**兼容迁移、中心/Dashboard 发布、单全量 Worker 待机部署完成；全量执行与接单仍关闭，P6 灰度验收尚未开始。**

机器报告：[fullcrawl-p6-release-20260920.json](../reports/fullcrawl-p6-release-20260920.json)。命令脚本、配置摘要和过程证据在 `runtime/fullcrawl-p6-20260920/release/`，受限配置备份在其 `private/`（目录 0700、文件 0600），不进入 Git。此前只读预检文档保留历史状态，以本记录为最新生产状态。

## 已执行的生产变更

1. 将 P5 中心/节点镜像发布到现有正式 registry 的新仓库，验证 manifest 与 P5 完全一致；公开 HTTPS 入口使用既有拉取凭据核验成功。没有覆盖增量节点镜像。
2. 在明确的 `newcrawler_crawler` 数据库应用四份已验收兼容 SQL。采用单事务、锁等待 3 秒、语句超时 15 秒；迁移成功，必要列与触发器检查通过。没有破坏性 down migration。
3. 基于当前线上 Dashboard 镜像构建节点管理覆盖镜像，保留原 `server.js` 和无关业务代码；最终镜像内节点类型/登记测试 4/4 通过。启用全量部署入口，配置固定节点镜像摘要。
4. 中心先以两个全量开关均关闭的配置升级，待原 62 个增量槽位全部恢复且连续三个采样推进后，再切换到仅允许全量部署/连接的配置。两次均 SIGTERM 自然排空，无强杀，旧进程退出码 0。
5. 使用正式 Dashboard 部署协调器，在“全量采集节点”部署 1 个 `full-crawl-1`，`syncIntake=false`。中心登记、SSH、文件、镜像拉取、启动、容器验收和连接检查全部完成。
6. 中心和 Dashboard 现用 Compose 清单已持久化；解析后的环境与实际运行容器逐项匹配，避免下次重建回到旧配置。

## 最终状态

| 项目 | 验收结果 |
| --- | --- |
| 中心 | healthy、0 重启；固定 digest 发布；4 CPU 上限保持不变，内存 1536 MiB、PID 192、tmpfs 128 MiB |
| Dashboard | healthy、0 重启；`qy-allpachong/dashboard:fullcrawl-p6-20260920`，image ID `sha256:54ca032d70585c32ef0728b35c90d947b4de81c3d424dc5c5ffef8af750d8212` |
| 增量 | 原两节点 47+15=62 个全部 connected/enabled/accepting；原 selected_slots、revision、updated_at 完整保留 |
| 全量节点 | `8a07de4f-a3ee-428f-a959-2aee9c7b6be7`，43.172.77.209，部署 ID `b19c5c9a-4a09-4a61-bdb2-9b69bed02933` |
| 全量 Worker | deployed=1、connected=1、standby=1、allowed=0、ready=0、active=0、executionAvailable=false |
| 节点容器 | running、0 重启，768 MiB、PID 128、UID/GID 1000、只读根目录；spool 持久挂载正确，采样占用 0 |
| 全量开关 | deployment=true，execution=false，intake=false |
| Rota | channel desired/provisioned/ready=124，claimed=123，reserve=1078；新增 1 个部署容量尚未用于采集 |
| NATS | health HTTP 200，pending=0，瞬时 unacknowledged=1；新中心 RPC rejected/timedOut=0 |
| 原全量队列 | waiting=0、active=0、delayed=6，未暂停；不得制造重复生产任务凑灰度数量 |

正式节点镜像：

`newcrawdashboard.137-175-93-199.nip.io/qy-fullcrawl-node@sha256:eef3acf34ec19620d0baf111388136f0d622377831075d2576c3057ac5979164`

中心当前引用：

`127.0.0.1:35000/qy-fullcrawl-center@sha256:3296e0621883fda8755304780d7ff54b910feab6034a490d6297bcd019430675`

35000 是持续运行的正式 registry 本机入口，不是 P5 已删除的临时 registry。中心公开同摘要引用亦保存在机器报告中。节点已从正式公网引用实际拉取并运行，首次部署约耗时 7 分钟，未发生部署失败。

## 增量保护与证据边界

- 线上中心 344 个源码文件逐一核对，差异均为当前工作区已有且 P5 验收覆盖的变更，没有发现线上独有源码被无意覆盖。两个连接阶段配置均保留原增量环境值，未注入未来本地兼容执行的额外业务环境。
- 第一轮排空为 07:44:28–07:47:24，全部槽位恢复并连续采样通过于 07:55:23。第二轮排空为 07:56:41–07:57:45，全部槽位恢复并连续采样通过于 08:05:35。
- 升级排空及逐槽激活期间吞吐暂时下降；不能将该窗口算作稳定吞吐，也不能宣称生产增量完全未受维护影响。最后两个恢复观察窗口的完成事件持续回升，原接单选择未被改变。
- 当前仅验证真实注册、下载、容器和认证连接。尚未验证生产全量的 Rota grant/profile、停止回执、真实频道入库和发布，不算完成单 Worker 灰度。
- 中心先前 CPU 空闲约 13–22%、IO wait 9–20%，磁盘仍约 88% 已用、剩余约 99 GiB。本次未开启新增采集/兼容执行负载；这些瞬时采样不足以宣布全量执行资源预算通过。
- 无新增业务测试；继承 P5 已完成矩阵，本轮补充最终 Dashboard 覆盖镜像测试、配置身份校验、schema 检查、正式镜像摘要和真实部署验收。

## 下一步与回退

下一步是执行阶段准备及单槽灰度，仍需按门槛推进：

1. 重新读取可比的至少 30 分钟增量基线、共享 CPU/IO/数据库等待和磁盘趋势，明确保留容量；核实原队列有真实业务供给。
2. 完整兼容执行配置已准备并通过无网络校验，但尚未应用。开启 execution 前须重新核对与当前环境的差异，保留原增量语义和 profile 密钥连续性。
3. 当前本地 channel 预算为 61，部署总容量 124。开启兼容槽后须明确调整为本地 62 + 远程增量 62 + 远程全量 1 = **125**，显式确保 Rota 容量后再放行，不能把 124 当作已覆盖兼容槽。`TOTAL_SLOTS=2` 表示 1 远程 + 1 本地兼容。
4. 受控升级执行配置并恢复增量，验证全量路由/profile/停止回执，再开放一个全量槽；至少观察 30 分钟、完成 10 个有效全量频道并核验业务发布后才考虑扩容。

旧中心容器 `qy-remote-node-center-before-p6-off`、关闭全量功能的兼容中心 `qy-remote-node-center-before-p6-connection`、旧 Dashboard `qy-newcrawler-fresh-dashboard-1-before-p6-connection` 均保留且已关闭自动重启。原镜像、私有配置备份和发布脚本保留。

发布前回退只读检查返回 ready=true。该结果是当时快照，不是永久放行；实际回退须重新停止全量接单、检查在途与未确认结果，保留 schema、任务证据、凭据与 spool。不要直接删除新增全量节点或清空 spool。
