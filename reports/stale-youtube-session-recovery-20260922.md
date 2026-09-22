# Worker 旧 YouTube 会话恢复修复

用户报告的节点：`54a7cdd3-eb9c-4713-8d2f-21f4a5279de0`。
受影响槽位：10、12、16、19、20、26、28、29、31、34、50。
本次完成本地源码修复及回归验证；未连接线上重新采样，也未部署或重启这些 Worker。

## 根因与行为

`RemoteChannelPlanExecutor.runOnce()` 在处理旧 claim 和领取任务前调用网络会话恢复；该恢复首先提交 `youtube-session.json`。中心已推进任务代次时，旧 checkpoint 收到 `409 / STALE_LEASE`。原实现只在有效 ACK 后删除活动文件，因此每次恢复及普通重启都会重放同一旧会话，永远到不了 claim。独立心跳原来只根据进程是否停止设置 `accepting`，没有参考恢复结果。

恢复现在仅对明确的 `409 / STALE_LEASE` 使用现有 spool 原子 rename + 目录 fsync 归档为 `.stale`。保留旧 request/checkpoint 证据，后续恢复不再扫描该文件；归档仍计入磁盘限额。随后继续原有网络释放和旧 claim 清理流程，不绕过中心任务代次校验。正在执行的会话提交遇到 stale 仍让本次执行失败，下一轮恢复才归档，不能把过期执行当作成功。

执行器启动、恢复失败或 spool 不可写时 `intakeReady=false`。独立心跳发送 `accepting=false`，沿用协议的 `draining / ready_for_tasks=false` 状态；本地领取门禁也检查该值。恢复通过后重新开放就绪。心跳响应在途期间若恢复失败，旧 ready 响应不能打开本地门禁。中心展示会在下一次心跳更新，不能理解为瞬时同步。

临时网络错误、鉴权错误、checkpoint 冲突、所有权不匹配及异常状态码均继续保留文件并阻止接单。归档失败也继续报错，不静默删除证据。

## 验证

修复前运行以下命令，两个旧会话场景均在真实执行器恢复调用链报 `STALE_LEASE`，未到领取步骤：

```sh
cd services/qybullmq
node --test --test-isolation=none --test-name-pattern='stale YouTube session recovery' test/remoteYoutubeRuntime.test.js
```

修复后运行 YouTube runtime、进程心跳、whole-channel 恢复和 spool/protocol 四个测试文件，43 项通过。随后增加归档失败及真实执行器/进程联动两项测试，重跑 runtime 与进程两个文件，25 项全部通过。合计 45 个不同测试通过，无跳过；`git diff --check` 通过。

```sh
node --test --test-isolation=none --test-concurrency=1 \
  test/remoteYoutubeRuntime.test.js test/remoteNodeIncrementalRuntime.test.js \
  test/wholeChannelRecovery.test.js test/remoteNodeProtocol.test.js
```

测试使用真实临时磁盘 spool 和远程客户端桩，覆盖有/无 checkpoint、新建 Worker 后不再回放、旧 claim 清理后重新调用 claim、临时故障保留文件、归档失败、活动执行仍失败以及心跳响应竞争。既有 whole-channel 测试还覆盖真实 SIGKILL 恢复。未运行外部 PostgreSQL/NATS/Rota 集成，也未据此宣称线上 11 个 Worker 已恢复。

## 线上验收条件

部署修复版本时保留各槽位 spool，先核对受影响槽位的当前任务与活跃执行状态，逐个替换可安全停止的进程。新版本将自行处理旧文件，无需手工清空目录。验收必须看到旧活动会话进入归档、旧 claim 解除、领取新任务以及实际任务进度；仅心跳 ready 或容器 running 不足以证明恢复。
