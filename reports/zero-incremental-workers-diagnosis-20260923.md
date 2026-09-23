# 三个增量 Worker 零完成诊断

日期：2026-09-23。对象：主节点 incremental-38，node02 incremental-2、incremental-6。检查为只读数据库、Redis、Dashboard 状态、远端容器及脱敏 spool；没有修改任务、清理文件、重启或部署，也没有启用生产 Inspector。

## 结论

三者均在代理传输失败后的重试交接阶段停止推进。旧网络执行已经退休，中心为同一传输任务建立了新 attempt 并将任务置为 pending，但新代次没有被远端领取；中心仍保持 BullMQ active 并续锁，Dashboard 将其显示为 processing/ready，不进入 awaitingRecovery。

**可以确认异常恢复和进度健康判定存在覆盖缺口，不能把这次持续两小时以上的零产出解释成正常等待或单纯 Worker 数量不足。** 保护旧执行不与新执行并发的原则合理，但“有心跳/锁续期”不能替代“任务有进展”。

尚未拿到运行中中心协程的具体等待栈，因此不能宣称已证明某一条 SQL、某个 NATS 请求或远端 closed spool 本身是挂起根因。已证实触发类别、持久状态和恢复未收敛；精确挂起点仍需进一步定位。

## 逐个证据

数据库采样时间 09:56:19–09:56:22 UTC（北京时间 17:56）。

| Worker | 传输任务 | 新 attempt 开始（UTC） | 状态 |
| --- | --- | --- | --- |
| 主节点 incremental-38 | b3b67223-b5a8-4996-bffb-79d3c60426a7 | 07:53:13.522 | task pending，generation=2；attempt running |
| node02 incremental-6 | 0890cbd0-d89d-4b3f-ad1f-fc01ebee8189 | 07:54:23.894 | task pending，generation=1；attempt running |
| node02 incremental-2 | 99541aad-88bf-40ba-b043-6150cf7edab9 | 07:55:01.420 | task pending，generation=1；attempt running |

三个任务均 node_id/worker_slot/lease_until/coordinator_until=NULL，target_node_id/target_worker_slot 指向上述 Worker。generation 仍是上一次领取的数字；下一次成功 claim 才增加 generation，不能把 pending 上的 generation 解释为已经领取新执行。

其他交叉证据：

- `execution_handoffs` 的最后交接原因均为 `FINGERPRINT_PROXY_TRANSPORT`；主节点 38 连续经历两次该交接，另外两个各一次。
- 所指旧 network binding 均 retired；分别在 07:53:11.815、07:54:23.219、07:54:59.743 UTC 退休。说明旧网络已释放，不是仍有正常采集占用它。
- 09:58:44 UTC，三个 BullMQ Job 均 active，没有 completed/failed/delayed 终态，锁 TTL 分别约 25.8、24.1、22.6 秒。与两小时前的 processedOn 相结合，说明当前仍维持任务锁，不能靠“锁过期后 stalled 重派”自然恢复。
- Dashboard 现有 GET 接口返回三个 Worker 都 connected=true、processing=true、readyForTasks=true、awaitingRecovery=false、executionPhase=preparing。
- 远端三个容器都运行中、重启计数 0。三个 network.json 都停在旧 task/generation 的 phase=closed，修改时间与旧 binding 退休时间吻合；whole-pending.json 和 pending.json 均不存在，没有发现待上传结果。node02-2 仍有旧 claim.json，另两个没有。
- 本次数据库活动快照未发现这些执行持续两小时的数据库锁等待；短时锁/I/O 等待与该具体挂起根因不能混为一谈。
- 中心最近 20 分钟按三个 node/slot 过滤，没有对应错误事件。无日志不能证明没有内部错误，但说明现有告警很难直接显示卡在哪一步。

node02-2 的 Redis Job 还保留一次历史 `cannot execute INSERT in a read-only transaction` failedReason；当前 Job 已再次 active。该字段不能直接当作此次持续挂起的最终原因，也不能将其他维护报告中“3 个失败 Plan”自动对应到本次三个 Worker。

## 流程设计上的具体缺口

### 1. 重试交接缺少独立的进度截止时间

`channelExecutionStore.js:103` 在新 attempt 准入时，将同一 task 重置为 pending，清空旧领取身份和租约。随后中心等待远端重新 claim。

`managedIncrementalRuntime.js:32` 设置了 30 秒 claim timeout，但 `channelExecutionStore.js:115` 的 waitClaim 只在循环边界检查 AbortSignal；直接等待 `store.pool.query`，没有将取消信号传给该查询，也没有覆盖查询等待本身的独立应用层截止时间。

离线执行真实 waitClaim 函数，注入不返回的数据库查询：30 ms 后 signal 已 aborted，到 150 ms waitClaim 仍未 settled；放行查询后才拒绝。输出：

```json
{"case":"in_flight_query_does_not_resolve","timeout_ms":30,"observed_after_ms":150,"signal_aborted":true,"waitClaim_settled":false,"verdict":"REPRODUCED"}
{"after_query_returns":{"outcome":"rejected","name":"AbortError"}}
```

这是**超时覆盖不完整的确定性复现**，不是生产查询已经挂起的证明。若后续证实该等待点，应同时处理连接失效/取消与旧查询可能迟到的情况；只加 Promise.race 而让底层操作继续执行，不足以安全解决问题。

### 2. 自动恢复没有覆盖“所有者仍活着但没有进展”

`centerExecutionSupervisor.js` 在建立新监督者时检查 previous execution，并通过 entry.blocked 进入 recoverRemoteSlot；也有 idle route 恢复，但条件明确要求 `!entry.processing`。

这三个 Worker 却保持 processing=true、ready=true。健康连接和持续队列续锁使原所有者继续存在，也就没有触发上述替换所有者/空闲路线恢复。当前路径缺少针对 admission/claim/collect/receive/apply 各阶段的无进展检测与受控终止交接。

应保留单一执行权：看门狗发现无进展后，要先中止原执行、确认网络静止并结算旧 attempt，再允许相同 Plan 的下一次合法尝试；不能直接把任务塞给第二个 Worker。

### 3. 状态展示将控制面就绪与工作进度混在一起

`deploymentAdmin.js:197` 的 processing 来自中心处理标志，readyForTasks 主要验证连接、接单配置、监督权和路线；目前即使长时间停在 preparing，仍会显示 active/ready。

应分别展示在线、可接单、正在处理、最近进展时间和阻塞原因；针对“pending 已有新 attempt、无有效领取、超过阶段期限”告警，不要把这些状态统称为正常运行。

### 4. 旧执行清理与新执行领取需要覆盖竞态测试

远端 `channelNetworkSession.js:26` 对 closed 网络状态会查询旧 lease 的状态；`channelPlanExecutor.js:51` 先执行恢复再接新任务。中心同时允许同一 task ID 准入新 attempt，并在 claim 前保留上一次 generation。这一交接需要用本次真实时序验证：旧网络退休、closed spool、旧 claim 有/无、新 attempt 已提交、领取应答丢失、查询取消或连接异常。

源码理论上会在 STALE_LEASE 时清理旧 closed 记录，故不能仅凭文件存在断言这里一定发生了循环等待。本次记录的组合应作为回归 fixture，而不是靠删除 spool 或强行改库绕过。

## 建议处理顺序

1. 保存这三个任务、attempt、队列和 spool 的对应证据；本次已完成脱敏取证。
2. 进一步定位中心当前等待点及远端恢复调用。若需临时仪表，先离线验证工具，再做单 Worker、短时、可清理的定位；本次未启用生产调试器。
3. 通过现有执行权与网络静止检查制定逐 Worker 的恢复操作；本次未重启或清理，不能报告已恢复。
4. 修复阶段截止时间和无进展恢复，补齐上述交接竞态测试及阻塞原因指标。单次重启能否解除不是流程修复验收，必须验证同一异常再次出现时可自动收敛且不重复应用数据。

## 证据和边界

目录：`runtime/zero-worker-diagnosis-20260923/`。包括三任务只读症状探针及 REPRODUCED 输出、交接/队列快照、远端脱敏 spool 与容器快照、waitClaim 离线复现及输出。数据库均明确直连 crawler-postgres:5432，事务级 READ ONLY、8 秒 statement_timeout，未设置会话级只读参数。

核验 channelExecutionStore、managedIncrementalRuntime、transportSignals、centerExecutionSupervisor 四个文件的 SHA-256，生产中心与当前工作区一致。远端镜像摘要为 a1736b34c23108f2e26582d068ae992cb2b0bd98e563864806fb8c2f327019ea，本次没有逐文件比对远端源码，因此涉及远端代码行为的推导仍需该版本回归核实。

首次只读请求被自动审批服务 429 限流拒绝，同一请求重试成功；后续取证完成，没有遗留审批阻塞。
