# 增量抓取重复失败诊断（2026-09-24）

已确认两层原因：网关的 SSL/代理传输失败触发换路重试；业务总预算耗尽后，增量错误处理仍允许同一业务运行继续重试。后者已用真实错误处理函数稳定复现，是明确缺陷。

## 样本与证据范围

05:58 UTC 从此前一小时选出 12 个各失败 5 次的任务，再按 Plan、Job、Business Run 关联两套数据库。样本专门选取重复失败最严重的任务，以下比例不能代表全部增量任务。

105 条网络尝试的最终错误（crawler.channel_execution_attempts）：

| 错误 | 次数 |
|---|---:|
| FingerprintGatewayError / SSLError curl_code=35 | 81 |
| FingerprintGatewayError / ProxyError curl_code=56 | 7 |
| STALE_LEASE | 7 |
| YoutubeCollectionFailureError（同一视频版权移除） | 4 |
| YouTube bot challenge HTTP 200 / player | 3 |
| WHOLE_CHANNEL_DETAIL_MISSING | 3 |

Rota 另记录 98 条换路/换指纹观察：91 条 proxy_transport / fingerprint_gateway / rotate_route，7 条 youtube_challenge / rotate_profile。观察记录与尝试的最终异常是不同口径，不能直接逐项相等。

当前网关在捕获 RequestException 后只传递异常类型、curl_code、failure_kind 和 session_reset（services/qybullmq/scripts/fingerprint_gateway.py:213），没有返回底层异常全文。因此已经定位到 SSL 连接/代理传输层，但尚不能进一步断言是代理出口、TLS 指纹兼容性或具体链路故障。

## 为什么变成重复失败

样本持久化策略为单次 Execution 最多切换 2 次路由，即最多 3 次网络尝试；同一个 Business Run 总预算为 9 次。队列重试使用原 Business Run，不会恢复这 9 次预算。

60 条队列失败事件中：

| 错误 | 次数 | 实际重试分类 |
|---|---:|---|
| EXECUTION_ROUTE_BUDGET_EXHAUSTED | 31 | system_retry |
| BUSINESS_RUN_BUDGET_EXHAUSTED | 22 | system_retry |
| STALE_LEASE | 7 | unknown / default |

具体样本 Plan `42a2e4d1-8ce4-593f-a880-7e36f5003f5f`：

| 执行开始 UTC | 失败 UTC | 结果 |
|---|---|---|
| 05:36:31.241 | 05:37:15.992 | 单次换路预算耗尽 |
| 05:38:01.000 | 05:38:29.904 | 单次换路预算耗尽 |
| 05:39:05.895 | 05:40:01.157 | 单次换路预算耗尽 |
| 05:40:53.672 | 05:40:53.816 | 总预算耗尽，144 ms 失败 |
| 05:41:44.998 | 05:41:45.538 | 总预算耗尽，540 ms 失败 |

前三轮耗尽 9 次网络预算，后两轮已经无法获得新的网络尝试额度，属于无效重试。

## 已确认的代码缺陷

1. `services/qybullmq/src/rotaSlotAdapter.js:599` 将控制端预算错误包装为 RotaBusinessRunBudgetExhaustedError，并保留原始 ProxyControlRequestError cause。
2. `services/qybullmq/src/managedWorkerJob.js:65` 遍历 cause；遇到任何 ProxyControlRequestError 都归为可重试 proxy_control，即使原错误 retryable=false。
3. `services/qybullmq/src/managedWorkerJob.js:496` 的预算耗尽终止入口只适用于 channelCrawl，不包含 channelIncremental。
4. `services/qybullmq/src/channelCandidateWorkerLifecycle.js:62` 虽识别 businessRunBudgetTerminal=true，但 `services/qybullmq/src/remoteNodes/centerIncrementalProcessor.js:94` 没有用该标记停止重试或立即终结增量 Plan。

诊断命令（连续运行两次，均以断言失败退出）：

```sh
/tmp/center-optimization-node20 runtime/incremental-repeat-causes-20260924/repro-budget.mjs
```

真实函数链输出：

```json
{"terminalCalled":false,"discarded":false,"businessRunBudgetTerminal":true,"permanentFailure":false,"retry_mode":"system_retry","retry":true,"category":"proxy_control"}
```

断言要求耗尽预算的增量业务停止重复执行，因此当前失败正好捕获该缺陷。该脚本不连接生产数据库、不执行抓取。

## 尚未证实与处理优先级

- STALE_LEASE 确实仍有发生；当前证据不足以区分租约过期、generation/owner 变化或续租异常，不能直接归因于中心 CPU 饱和。
- 4 条版权移除最终异常对应的观察分类需要单独核对，不能仅凭最终错误断言分类错误。
- 优先修正增量 Business Run 总预算耗尽的终止与 Plan 状态结算，并让预算终止语义优先于通用系统重试分类。回归应覆盖真实 cause 链和中心增量处理入口，不能仅测试全量队列。
- 随后补充脱敏的底层 SSL/代理错误证据，按 worker、代理路线和目标阶段归因；再针对租约失效补齐续租/换代时间线。
- 增加 worker 不会修复这些错误，增加预算也不能解决已经确认的错误分类问题。修复终止逻辑能消除无效重试，但不会单独解决最初的 SSL 失败。

本次为只读诊断；新增诊断脚本与报告，未修改或发布生产实现。原始脱敏证据保存在 `runtime/incremental-repeat-causes-20260924/` 的 samples、event-trace、rota-stage、deep-trace 和 repro-budget 文件中。
