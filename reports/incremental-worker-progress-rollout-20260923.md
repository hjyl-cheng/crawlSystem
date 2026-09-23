# 增量 Worker 修复生产发布（2026-09-23）

授权：用户“后续按计划进行”。中心和 Dashboard 修复已上线，三个异常远端已升级并恢复实际任务产出。当前仅 node01 / incremental-38 开启额外阶段自动取消，其余槽位维持观测。

## 已执行的交接

- 保存中心和 Dashboard 实际 Docker inspect 到 `runtime/incremental-progress-rollout-20260923/private/`（目录 0700、文件 0600），未输出凭据。中心 479 个文件和 Dashboard 页面源码均匹配候选清单。
- 11:59 UTC 通过已有节点 API 暂停两个增量节点及共用中心的全量节点接单。备用节点原本关闭，保持原状。全局队列未暂停。
- 12:02 UTC 正常任务全部排空，全量队列无活动任务，网络活跃绑定为零，只剩三个原异常 Job。原监督者仍续锁。
- 12:02:46 向旧中心发出 SIGTERM；挂起处理函数阻止正常退出。再次核验网络、队列、身份及禁止自动重启后，于 12:03:53 隔离旧进程（退出码 137），确认旧监督锁消失。没有删除队列锁、强移 active Job 或清理业务数据。
- 12:04:11 启动固定摘要候选中心，observe 模式，增量监督范围先限制为 node01。新中心在全量兼容模块初始化阶段发生六次启动重试，随后成功启动；原日志未记录具体错误码，不将原因猜测写成定论。
- Dashboard 正常退出（退出码 0）后更换固定摘要镜像；入口代理配置校验并平滑 reload。
- 12:06 UTC 恢复 node01 和原启用的全量节点；node02 继续暂停。
- 12:07:09 原 node01 / incremental-38 attempt 被新监督者核验后结算为 aborted，记录 `supervisor_replaced`、`network_quiesced=true`。原业务计划随后由合法后续执行完成，未人为标记成功。

## 观测中新确认的远端问题

中心修复后，incremental-38 仍然不领取，新增领取尝试在 30 秒内按 REMOTE_CLAIM_TIMEOUT 完成停止、网络查证、attempt 结算和 Rota Completion，避免再次无限占锁。

三个远端均保留旧 `youtube-session.json`，旧租约查询均返回 409 / STALE_LEASE；其中 node01/38、node02/6 checkpoint 状态为 success，node02/2 为 failed。运行中的远端镜像缺少已有的旧 checkpoint 恢复修复。此前仅检查 pending、whole-pending 和 whole 日志，不足以排除 YouTube checkpoint 恢复阻塞。

12:10 UTC 对 incremental-38 做过保持原镜像的单槽位重启，旧文件依然阻塞，证明普通重启不足以解决。所有重启保留 spool。随后按方案批次 D 使用固定摘要远端修复候选：`newcrawdashboard.137-175-93-199.nip.io/qy-node-incremental@sha256:242f4609f406b6476d8192a9f4c0b327c50932e66a04a544b6009e7eb20391d3`。

候选在恢复路径中仅对明确 409 / STALE_LEASE 归档旧 checkpoint，保留证据；其他错误仍阻止接单。未修改 heartbeat 协议，也未手工删除旧 checkpoint。候选与当前工作区在全量通用接口上有差异，本次部署的是已针对增量验证的旧会话修复镜像。

12:24:11 首先升级 incremental-38，旧容器保留，原挂载和配置身份不变。12:24:45 已确认旧活动 checkpoint/network/claim 文件不存在；12:25:46.540 首个后续 Job 完成，且已领取下一份任务。

## 远端回归

- 实际旧镜像重放两项旧 checkpoint 用例：均失败，均未走到新 claim；其余 15 项由测试名称筛选未运行。
- 实际候选镜像：YouTube runtime、whole-channel 恢复和协议核心 37 项通过，0 失败、0 跳过。
- 增量进程就绪与恢复：7 项通过；专属全量进程的 1 项显式排除，该镜像不含全量模块。
- 最初测试调用缺失 whole-channel 夹具挂载且包含全量专属测试，已分别纠正测试环境和范围，未把失败结果算作通过。

原中心源码与隔离 PostgreSQL/Redis/NATS/Rota 的 92 项集成验收、候选中心内 70 项测试记录见原修复报告。本次未重复运行无变更的整套中心测试。

## 证据与回退

执行清单、脱敏采样、测试日志及部署脚本保存在 `runtime/incremental-progress-rollout-20260923/`。原中心、Dashboard 和升级的远端容器均停止保留，禁止与新容器同时启动。回退前须停止新领取并排空/隔离新执行，不允许直接同时启动旧中心。

远端备份和准确创建规格位于各宿主机 `/var/lib/qy-progress-upgrade-20260923/<slot>/`；同目录保存针对该槽位的 Compose 镜像覆盖文件。已将三个服务在各自 `/etc/qy-node/runtime/deployments/<deployment_id>/compose.json` 中的 image 原子更新为固定摘要，并保存原文件；其他服务字段保持不变。

## 第二阶段进展

首个槽位完成验收后，于 12:27:14 和 12:27:45 分别升级 node02 / incremental-2、incremental-6。两者原网络 closed、无 pending/whole-pending，旧容器均在 30 秒正常退出等待后停止（退出码 137），原挂载和旧容器保留。三个目标的原 checkpoint 都已在新版本正常恢复路径中归档为 `.stale`，只读查询确认 task_id 和 generation 匹配。

12:26:43 再次停止 node01 和全量节点的新任务接入，为单槽位 enforce 配置切换排空。12:28:48 增量和全量活动队列均为零，活跃网络绑定为零。12:29:29 向 observe 中心发送正常退出信号，12:32:17 确认正常退出（退出码 0）；12:32:46 启动最终中心，健康检查通过且重启次数为 0。此阶段只开启 node01 / incremental-38 的额外监督器自动取消，其余槽位继续观测，恢复原 Dashboard 准入范围。

观测发现启动和停止过程仍受逐槽位历史未结算查询影响：无活跃业务时 `remoteSlotUnsettled` 仍产生数据库读取等待。这是后续优化点，当前未通过修改历史数据或取消所有权核验来绕过。

## 最终验收

统一窗口：**12:32:46.975–12:37:35.427 UTC（北京时间 20:32:47–20:37:35）**。统计 completed 事件中的唯一 Job，不是视频数，包含中心逐槽位启动时间。

| Worker | 该窗口完成任务数 | 最近完成（UTC） | 原挂起 attempt |
|---|---:|---|---|
| node01 / incremental-38 | 4 | 12:37:11.836 | aborted，12:07:09.458 结算 |
| node02 / incremental-2 | 2 | 12:37:11.466 | aborted，12:34:45.161 结算 |
| node02 / incremental-6 | 3 | 12:37:26.815 | aborted，12:34:43.851 结算 |

同一窗口整个增量队列完成 152 个任务。原 node01/38 业务计划由后续合法执行完成（succeeded）；原 node02/2、node02/6 计划最终为 failed，保留其实际失败结果，没有手工改成功或增加重试预算。三个槽位恢复后的新任务均有完成事件，因此“槽位恢复”和“旧业务计划成功”没有混为一谈。

维护前后接单开关逐项一致：node01、node02、全量节点均开启，备用节点关闭；节点连接数量分别为 47/47、8/8、10/10、20/20。最后快照三个目标均有阶段推进，未处于 overdue/blocked；短暂 stopping/recovering 属于尝试收尾，后续完成事件证明已退出。

最后所有权核验：每个目标恰好一个监督 advisory lock 所有者、至多一个活跃网络绑定；每槽位最近 100 个任务内，至多一个未结算执行。这是有界采样，不是全部历史或全部领域写入的重复性审计。旧 checkpoint 均已归档保留。

最终中心和 Dashboard 健康检查通过，旧中心、observe 中心和旧 Dashboard 均停止且 restart policy 为 no。Dashboard 页面和 JS 在服务入口均 HTTP 200；宿主机未认证代理请求为 403，未进行登录后的浏览器端到端验收。

中心最终启动后的日志未出现新的启动失败、槽位启动失败、监督重建失败或恢复等待事件。业务失败计数仍有 BUSINESS_RUN_BUDGET_EXHAUSTED、EXECUTION_ROUTE_BUDGET_EXHAUSTED 等既有预算终止；没有用不同任务构成的短窗口宣称失败率改善。

额外看门狗已配置为 enforce，但当前窗口**没有观察到 incremental-38 因 REMOTE_EXECUTION_OVERDUE 被自动取消**。生产已验证实际网络失败交接、领取超时的有界收尾、旧所有者恢复及三个槽位持续产出；额外看门狗取消路径的成功证据仍来自此前隔离集成测试。未为制造触发而向生产注入故障，也未将白名单扩大到所有槽位。

## 当前发布配置与下一阶段

- 中心：`qy-allpachong/remote-node-center@sha256:5afbcdbd16703eaae1611e96154216631c432c5e2819e287eebcb7923a990ff2`。
- Dashboard：`qy-allpachong/dashboard@sha256:f5be0357f10501c11c2afee16bcd9961afe619cdb143fb0f6fe6594b5760fdf1`。
- 三个远端：上文固定摘要 `242f4609…`；其他远端未升级。
- Compose 覆盖：先 `deploy/compose.incremental-progress-center.yml`，再 `deploy/compose.incremental-progress-enforce.yml`；精确运行创建规格为本次 private 目录下 `center-enforce.create.json`。Dashboard 对应 `dashboard-observe.create.json`。
- 实际发布清单：`runtime/incremental-progress-rollout-20260923/release-manifest.json`。原构建 manifest 的 deployed=false 是历史构建记录，本清单是本次发布结果。

下一阶段保留单槽位灰度，收集额外看门狗真实触发或明确隔离证据后，再决定扩大白名单。启动/停机历史查询耗时，以及其他旧远端遇到同类 stale checkpoint 的风险，作为独立后续优化点，不在本轮顺带批量变更。

生产原中心具体挂起的 await 仍未精确定位；本次另外确认并实际修复了远端旧 checkpoint 阻止领取的链路。不要将这些证据表述为已证明所有挂起都由同一 SQL 引起。

## 版本归档核对

本次 Git 归档包含进度监督、有界读取、执行清理、Dashboard 状态展示、回归测试及发布记录。混合文件已逐段拆分，既有容量控制、其他业务修复和数据库维护改动保留在工作区；测试中的接单参数同步到 HEAD 已有的布尔开关契约。线上镜像基于发布时完整工作区构建，包含此前改动，不应将其摘要视为仅由本次 Git 提交独立构建的产物。

从 HEAD 与暂存补丁生成独立源码副本，复验核心测试 70 项、Dashboard 节点路由测试 4 项，全部通过且无跳过；Dashboard 页面脚本、中心启动入口语法及暂存差异空白检查通过。此前 92 项隔离集成验收沿用发布记录，本次归档未重复执行。运行时私有配置、凭据和原始取证目录未纳入提交。
