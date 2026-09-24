# youtube-finalize 积压与来源核查

核查时间：2026-09-23 03:51:23–03:52:34 UTC（北京时间 11:51–11:52）。仅执行只读 Redis 查询及 PostgreSQL 只读事务，未修改生产状态。

## 结论

当前任务主要是历史频道迁移的 Finalize 补偿任务。大量失败记录的直接错误是对象存储可用空间低于最低阈值；当前 Worker 正在处理恢复任务，不能把 failed 或 completed 的保留数量解释为实时等待数量。

| 队列状态 | 第一次采样 | 第二次采样 |
| --- | ---: | ---: |
| wait | 11 | 17 |
| prioritized | 185 | 185 |
| active | 1 | 1 |
| delayed / paused | 0 / 0 | 0 / 0 |
| failed | 7,604 | 7,474 |
| completed（保留记录） | 10,000 | 10,000 |

第二次采样实际等待为 202 个，另有 1 个执行中。Redis 多次读取期间队列继续变化，统计不是跨系统原子快照。

## 来源

- 第二次采样的 203 个在途任务全部 reason=`controller-finalize-source-change`，均为 full Run，Run 的 status/detail_status 均为 done。
- 201 个属于已 completed 的迁移批次，2 个属于历史 legacy-results-canary 批次；没有增量 Run。
- 其中 168 个来自 `migration-19eff406-4ec3-442e-a448-882b14caae69`，27 个来自 `migration-956e53ed-1e5c-4fe2-bcd8-2268cc838959`。
- 最近五个迁移批次均已 completed。当前队列来源应理解为历史数据收尾恢复，而非这些批次重新采集。

## 失败与恢复证据

- 第一次采样 7,604 个 failed 中，7,602 个错误为 `Storage backend has reached its minimum free drive threshold. Please delete a few objects to proceed.`；其余为 1 个数据库关闭期间登录错误及 1 个 Redis AOF 无可用空间错误。
- 约 71 秒内 failed 减少 130。第二次采样前五分钟记录 535 次 completed 事件，对应 535 个不同 Job；该窗口未发现同一 Job 重复完成。
- 最近完成记录的独立读取中，439 个结果为 ready_auto / publication not_ready、80 个 no_change、7 个 not_owned、7 个 revised。因此队列处理成功不能等同于业务发布成功。
- 这些错误证明历史失败原因；本次没有单独测量对象存储当前剩余空间，不能据此声称当前仍然空间不足。

## 代码对应

- `services/qybullmq/src/finalizeChangeRecovery.js`：后台按 source generation 恢复，默认 waiting + prioritized 达到 200 时暂缓新增恢复工作；现存 failed/completed Job 仍需处理时调用 retry。因而等待量可能维持约 200，同时 failed 逐步减少。
- `services/qybullmq/src/finalizeDispatch.js`：入队保存 channel_id、run_id、reason、source_revision 和 pipeline_cycle_id。
- `services/qybullmq/src/pipelineV2.js`：Finalize 生成最终频道画像并调用 saveJsonRaw 保存，再提交最终画像及发布衔接，存储写入失败会使任务失败。

本次为来源与运行状态诊断，未执行修复或变更测试。只读采样脚本与完整统计暂存于 `/tmp/finalize-diagnosis-20260923/`。
