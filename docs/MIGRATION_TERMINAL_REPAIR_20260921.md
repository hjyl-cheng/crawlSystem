# 迁移批次终态与自动收尾修复

2026-09-21。数据库：`newcrawler_crawler`。

批次 `migration-19eff406-4ec3-442e-a448-882b14caae69` 已于 **2026-09-21 03:13:19.809 UTC** 完成，状态为 `completed`，控制版本从 7 升至 8。

| 最终结果 | 频道数 |
| --- | ---: |
| 成功 | 181,647 |
| 休眠 | 189,734 |
| 拒绝 | 3,491 |
| 最终失败 | 2,126 |
| 合计 | 376,998 |

`pending=0`、`started=0`。最终失败是本批次的失败结果，不能理解为这些频道的数据已成功抓取。

## 已确认的原因与处理

接续前一轮修复时剩余 43 个 started 项目。本轮逐项对照 PostgreSQL 与 Redis 后处理：

- 5 个 Run 已失败，Redis 对应作业也已失败且没有活动恢复，补写持久终态证据。
- 22 个 Candidate 已失败且快照预算耗尽，Redis 作业已失败，但数据库保留旧所有权栅栏。按作业 ID、尝试次数和派发代次精确校验后清理，保留原始错误及修复证据。
- 10 个 Run 等待 Agent，但 Agent 已因缺少必需字段永久失败，重试时间为 infinity。确认没有活动 Agent 作业、恢复任务或可重试 refresh 后，将对应最新 Run 记录为终态失败。
- 6 个详情 Run 的作业因数据库连接故障退出。先复用原 Job 恢复，随后发现其 Data API 请求一直 pending：底层 Task 已耗尽 3–4 次尝试，调度器不再派发，但失败传播 SQL 又遗漏 pending 状态。修复后这 6 个请求自动失败，Worker 写入 Run 终态证据，Controller 自动结算。

全部项目终态后又复现完成事务的计数约束错误：原逻辑保留 Candidate 的 accepted 数，却把 Run 最终失败数写入同一组 failed 数，导致重复计数。完成事务现在重新计算同一层级的 Candidate 统计，并将最终失败数独立保存到 `result_json.terminal_failed_count`。控制批次的频道结果仍以 migration_control_items 为准。

## 永久代码修复

- `channelCandidateWorkerLifecycle.js`：普通终态 Run 失败记录 `channel_run_terminal_failure`。
- `worker.js`：没有 Candidate 的终态 Run 失败同样记录证据。
- `migrationBatchControl.js`：识别持久 Run 终态、预算耗尽和匹配当前作业的终态重试证据；旧作业证据不能关闭正在恢复或较新的 Run；完成时统一 Candidate 计数。
- `videoApiBatchRequests.js`：无活动 API Batch、尝试预算耗尽的 pending Task 向等待请求传播失败。预算未耗尽和仍有活动 Batch 的任务保持等待。
- `reconcileMigrationOrphanedRuns.mjs`：审计默认只读；应用时锁定调度器、批次与 Candidate，重新审核最新 Run，并在事务内校验目标集合和实际更新集合完全一致，冲突即回滚。

## 验证与发布

两个新增故障场景均在隔离 PostgreSQL 中先复现失败，再验证修复通过：API 请求永久 pending、完成计数约束冲突。实际收尾 SQL 还覆盖活动重试、活动栅栏、较新 Run、旧作业证据等保护条件。

相关验证包含 Worker 生命周期 11 项、批次控制 4 项、批次完成 8 项、真实 SQL 终态/完成事务 1 项、真实 API 请求流程 1 项，均通过。最终 Controller 镜像再次通过 13 项批次相关验证。语法检查与 `git diff --check` 通过。

生产工作区含其他未发布改动，因此从当时的生产镜像提取 Worker 基线，仅加入本次修复文件构建镜像：

- 40 个频道 Worker：`qy-allpachong/qybullmq:migration-terminal-20260921-69b717c`。
- Controller：`qy-allpachong/qybullmq:migration-terminal-20260921-ec3aeb0-v2`。

滚动替换期间短暂停频道队列并在完成后恢复；原环境变量、命令、网络、挂载和资源配置保持一致。旧容器保留用于回滚，并关闭其自动重启。Controller 停止时等待 60 秒后退出码为 137；未将其描述为优雅退出。数据库事务保护使未提交事务回滚，替换后的 Controller 已恢复运行。

运行记录位于被 Git 忽略的 `runtime/migration-terminal-repair-20260921/`：审计分类、逐阶段事务结果、测试红绿日志、镜像构建、创建清单、回滚配置与最终验收。含环境变量的私有清单权限为 600，不应提交。最终数据库验收与镜像清单的脱敏报告见 `reports/migration-terminal-repair-20260921.json`。
