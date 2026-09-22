# 取消批次完成后的自动暂停

已发布版本：`b862901`，分支 `codex/migration-idle-queues-20260922`。

控制器镜像：`qy-allpachong/qybullmq:pachongsys-b862901-migration-idle-20260922`。

## 行为

受控迁移批次自然完成（completed，调度器停止原因为 pipeline_complete）后，采集、Agent 和已启用的 Data API 消费队列保持开启，等待后续任务；Finalize 保持运行。采集和 Agent 的队列控制原因标记为 migration_idle。

批次仍会正常结算并停止生成该批次的新任务，不将调度器强行改回 running。人工暂停、人工结束、代理容量限制、详情积压控制和已禁用的 API 策略继续生效。发现队列和独立详情队列沿用当前执行拓扑，不因本次更新启用。

## 验证与发布

- 先增加回归断言，确认旧实现会在自然完成后暂停消费队列；修改后通过。
- 独立 PostgreSQL/Redis 回归及最终实际发布镜像测试：15 项通过，0 失败，0 跳过。
- `scripts/verify.sh` 通过。
- 06:36:11 UTC：旧控制器正常退出，退出码 0。
- 06:36:12 UTC：新控制器启动；06:37:14 检查为 running、重启次数 0。
- 持久化重建配置、运行环境和资源配置核验通过；本次只替换控制器。
- 06:38:14 UTC：实际迁移 API HTTP 200，当前批次仍 running；频道采集、Agent、Finalize 均未暂停，独立控制循环继续更新版本。
- 在运行中的控制器只读调用 completed 状态策略，确认采集、Agent、启用的 Data API、Finalize 均返回 paused=false。未为验证而改动生产批次状态。
- 06:45:41 UTC 复查全量节点：10/10 deployed、connected、allowed、ready，10 个自然空闲；所有容器仍 running、重启次数 0。06:39 的 full-crawl-6 短暂中心心跳未就绪已自动恢复，未发生容器重启。

验收时当前批次还有 1 个 started 频道，未宣称已在生产观察到整批完成。自然完成后的行为通过真实数据库/Redis 回归及已部署代码的策略核验确认。

## 证据与回退

证据位于 `runtime/migration-idle-queues-20260922/`，包括修改前后测试、镜像测试、构建、发布和 API 快照。受限重建配置与原容器快照位于该目录的 `private/`，不纳入 Git。

旧容器 `qy-newcrawler-fresh-controller-fullcrawl-canary-1-before-idle-queues-20260922` 已停止，自动重启关闭。回退会恢复上一版“完成后自动暂停”的策略。

原工作区修改保留；隔离发布工作树 `/tmp/pachongsys-migration-idle-queues-20260922` 干净。
