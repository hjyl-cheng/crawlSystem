# 2026-09-21 迁移启动恢复阻塞修复

## 原因与修复

旧批次的 3 个恢复项（795109、805769、806775）对应 Run 已因 Rota 执行预算耗尽终止，但恢复项仍以 `resolved/job_completed` 保存。恢复扫描将其重新打开，反复重排已完成的原始 Job。启动准入在扫描窗口内看到 `retrying`，因此拒绝新批次。

`migrationSystemRetryRecovery.js` 现在在重新激活或重新排队前识别预算终态，并以 `recovery_business_run_budget_exhausted` 收尾。事务按 Candidate、Retry、Run、Channel 的顺序加锁，重新核验批次、代际、最新 Run、预算终态与执行权；不会解除仍有执行权的任务。

三个历史 Run 还留有较早 attempt 的详情执行权。通过精确 ID、终态证据、当前代际、已释放根任务执行权和原始 BullMQ Job 已完成的多重检查，事务清除了这三个过期详情执行权并留下审计标记。控制器随后自动结算恢复项。

## 验证与部署

- 独立 PostgreSQL 回归在修复前复现 completed Job 再次被 retry；修复后通过。
- 恢复、准入及数据库回归共 20 项通过，无跳过。覆盖 retrying、dispatched、legacy job_completed，以及扫描后代际、执行权、预算证据、批次、状态变化。
- 仅将恢复模块覆盖到当前生产控制器镜像，镜像为 `qy-allpachong/qybullmq:migration-budget-recovery-20260921`。未重建其他脏工作区文件，未重启采集 Worker。
- 保留旧控制器及私有创建配置。旧控制器在 60 秒停止期限后退出 137；新控制器配置与原配置核验一致，重启计数 0。
- 两个主要循环 Job 的 attemptsMade 保持 231684、216210，不再增长。
- 在真实 API 容器调用 `createMigrationControlBatch(selection=100)` 成功，再回滚事务；确认无真实批次残留。

## 手动批次的收尾

`legacy-results-manual-v2` 只有一个 Candidate，Run、Agent、最终发布均完成，发布阻塞数为 0，队列中不存在本批次任务。它因旧完成检查等待共享队列的其他历史 Finalize 任务而停在 finishing。

已通过 `settleCompletedMigrationBatch` 的正常事务逻辑收尾，调度器恢复为 `stopped/pipeline_complete`。本次没有修改旧流水线对共享队列积压的全局判断；受控迁移批次继续使用自己的完成逻辑。

操作脚本及快照：`runtime/migration-budget-recovery-20260921/`。结果：`reports/migration-budget-recovery-20260921.json`。
