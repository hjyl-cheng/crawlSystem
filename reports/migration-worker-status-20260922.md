# 迁移批次、Worker 与队列检查

采样窗口：2026-09-22 05:38–05:42 UTC。本次只读检查，未重启服务、调整接单或修改队列暂停状态。

## 新批次

批次 `migration-8cbefdcd-7f08-46af-b04f-4d548410f547` 于 05:31:53 UTC 创建，计划迁移 100 个频道，控制批次状态为 running。

05:40:35 UTC 数据库及 Redis 复查：97 个频道 pending，已进入频道采集队列的 prioritized 集合，全部属于此批次；3 个已开始且完成详情采集，其中 2 个已按 dormant 结算，另 1 个 waiting_agent。该批次有 1 个 Agent Job 滞留在暂停队列。第一轮和第二轮采样之间，97 个待采集任务数量未下降。

调度批次表中的 total_channel_count=3 是当前已经物化的候选数，不能据此把用户此次选择的 100 个频道误报成 3 个。

## Worker

- 全量节点：连续三次采样均为部署/允许/连接/就绪 10/10，active=0、idle=10。full-crawl-1 未暂停且已启用。05:39:54 UTC 远程容器核验：10 个全部 running、重启次数均为 0。
- 增量节点：05:38:35 UTC 为 82/82 就绪且执行中；05:40:53 UTC 虽仍 82/82 连接，但 69 个暂报 network_unready，仅 13 个 readyForTasks；05:41:50 UTC 恢复至 82/82 就绪、82 个执行中。因此本次确有网络就绪状态波动，不应将全程在线等同于全程可接单。
- 中心、Redis、Dashboard 健康检查通过；控制器容器 running；上述容器重启次数均为 0。

## 队列

05:40:35 UTC 采样：

| 队列 | 暂停 | 等待 | 执行 | 延迟 |
| --- | --- | ---: | ---: | ---: |
| youtube-channel-crawl（普通队列） | 是 | 97（prioritized） | 0 | 0 |
| youtube-agent-batch | 是 | 1（本批次） | 0 | 0 |
| youtube-data-api-batch | 是 | 1 | 0 | 0 |
| youtube-content-enrich | 否 | 0 | 1 | 0 |
| youtube-finalize | 否 | 0 | 1 | 3 |

频道采集队列中的 192 个 failed 和 Agent 队列中的 645 个 failed 为保留的历史队列记录，不能当作此次批次新增失败。当前批次未观察到失败频道。

## 阻塞证据

控制器持久化 tick 50778（05:37:42 UTC）明确记录：`pause youtube-channel-crawl`，原因为 `query_scheduler_stopped`；容器日志给出相同决策。

但实际 `crawler.settings.query_scheduler` 当前为 finishing，pipeline_cycle_id 指向新批次，数据库 updated_at 为 05:31:53 UTC。当前状态与控制器暂停依据不一致；邻近两条主 tick 间隔约 7 分 56 秒。应继续检查耗时控制周期是否使用了迁移启动前的旧状态。现有证据足以确认暂停阻塞，但不足以把该旧状态竞争推断宣称为最终根因。

另有近期 finalize_changes 周期出现 PostgreSQL lock timeout；Finalize 队列仍有执行活动，未证明它导致频道采集暂停。

证据目录：`runtime/migration-worker-status-20260922/`。本次未执行恢复或修复，因此未以“队列已恢复”验收。
