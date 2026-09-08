# 迁移与增量重构最终版本（2026-09-08）

用户确认以 `agent/incremental-migration-release` 为本轮重构最终版本。
业务代码基线为 `70f81c9`；本次归档提交补充版本说明和历史审计报告。

## 已部署范围

20 个 Full Crawl worker、20 个增量 worker 及 API、API-batch、finalize、controller 共 44 个服务已运行 `pachongsys-70f81c9-web-ios`。
共享视频采集使用 YouTubeJS，客户端顺序为 WEB → IOS；视频详情兜底使用共享 API-batch，Full Crawl checkpoint 恢复入口已移除 yt-dlp 回退。
修复详情见 `WEB_IOS_MIGRATION_RECOVERY_20260908.md` 及此前专题记录。历史文档记录当时的方案与部署状态；发生调整时以最终代码为准。

## 最后迁移核验

批次 `legacy-results-canary-1788847458015-51a54aca`：200 个频道，129 ready_auto、71 ready_partial，全部结束；耗时约 8 分 46 秒。
5,824 条候选全部处理：2,245 条入库、3,579 条排除，无待确认项。
正常频道核心字段、视频重复、评论首屏结构检查未发现异常；有 69 条点赞采用既定未公开记零策略。
2 个频道的预算耗尽恢复已完成。5 条视频通过自动 API-batch 完成兜底，分为 5 个单视频批次，均为排除项；批量聚合效率仍可优化。

## 今日 Clock 与增量核验

2026-09-08 原有 250 个到期频道但没有今日 plan。通过运行中 feature-ingest 镜像中的 `feature-schedule-day` 临时执行一次规划，生成 250 个 plan，由正式派发和增量 worker 执行。

- 231 个 About 更新、17 个视频 plan 成功。
- 另外 2 个视频扫描同样完成，但频道转休眠时 plan 被记为 `cancelled / channel_dormant`：Relaxing Nature Soothing、Jell。因此页面显示 248 成功、2 取消，不能解释为 2 个未执行。
- 250 个底层 Run 均 done，无错误；19 个视频检查点 finalized，250 个派发 outbox published。
- 13 条首次发现视频详情、4 条近期指标刷新全部 captured，核心字段及采集值与存储一致，评论首屏共 181 条。
- 首次发现的 13 条中，6 条在 90 天窗口内并发布，7 条 2025 年历史视频仅保留在采集库。4 条近期指标刷新已发布；这 10 条业务视频的计数、类型、时长及发布哈希与采集库一致。
- 231 个 About 的订阅数、总播放量、视频数与业务库逐项一致。
- ASMR Paula Franssinett 达到追赶上限，扫描 150 条后按既定策略选择最新 30 条；本次不声称历史缺口全部补齐。

以上为落库与业务发布一致性核验，不等于重新逐条人工检查 YouTube。

## 运维待办

常驻 `feature-scheduler-daily` 尚未部署。部署脚本在创建容器前的凭据挂载检查处停止，未创建 scheduler；后续对话优先完成数据统计及本版本归档。
因此今日临时增量已经完成，但不能据此声称后续日期已恢复自动生成 plan。常驻部署须沿用已验证的 feature 镜像、数据库与正常 UTC 调度窗口，不能固定 `SCHEDULER_PLAN_DAY`。

`reports/` 内报告属于各自注明批次和时间的历史审计，不应与本节最新批次统计混用。
