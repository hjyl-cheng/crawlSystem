# WEB → IOS 与迁移恢复修复（2026-09-08）

本轮批次：`legacy-results-canary-1788841923798-0e9433a7`。

## 问题与最终行为

- 视频详情客户端顺序收敛为 WEB → IOS；移除后续 ANDROID、额外 WEB 和 Android Basic 探测。网络错误仍交由 Rota 处理，业务网络预算未增加。
- 后续客户端缺少 microformat 或返回非法 JSON 时，保留先前已确认的 WEB 类型证据。
- 网络预算耗尽后，未知视频类型不再阻止共享 API-batch 请求。API 结果持久化后可恢复消费，不重复发起网络采集；类型未知时保留 unresolved，交给现有 deferred 策略，不能猜测类型后发布。
- 终局系统失败对已接收迁移频道释放旧任务占用。恢复记录使用原任务 ID/attempt，不能使用释放后的空值；迟到的旧任务不能清除新代次的占用。
- checkpoint 补采完成后，验证冻结的 uploads/target 哈希、全部候选身份和终态，生成 Full Crawl 完成凭据，再允许最终收尾。
- checkpoint 恢复入口使用同一 YouTubeJS/API-batch 模块，禁用该入口原先的 yt-dlp 回退。

## 线上恢复

| 频道 | Candidate | 原完成详情 | 补采 |
|---|---:|---:|---:|
| Atividades para brincar / UCg1JrFCXdT3xN2kg0sCiQmg | 2635 | 24/30 | 6 |
| Alana / UCRrftr0EY_pt64yABQN1b7g | 2666 | 26/30 | 4 |

原任务各使用 9/9 个 Rota Task，实际为 9 个不同路由代次、3 个队列 execution；没有据此声称 9 个不同出口 IP。

失败任务被精确释放后，原业务预算正常终结，控制器建立独立 checkpoint 修复任务，仅补剩余项，未清零原 Rota 预算。两者均恢复为 done / ready_partial，30 条均超出 90 天窗口，无待确认或未处理项。

最终来源核验发现旧 checkpoint 入口使用了 yt-dlp，因此进一步修正该入口，并通过与 API-batch 相同的 `fetchVideoDataApiDetails` 官方批量接口，对这 10 条做一次受控维护请求。该维护请求核对了原网络预算 9/9、视频 ID、频道归属、发布时间和超龄状态，计入日配额；不是一次队列自动兜底执行。10/10 返回，发布时间均在 2020–2023 年，替换未发布排除项的当前详情来源为 `youtube_data_api_videos_list`。未以 yt-dlp 的类型证据冒充 API 类型，也没有写入已发布视频。

本批 200 个频道于 05:27:17 UTC 自动 completed：143 ready_auto、55 ready_partial、2 community_guidelines 拒绝；恢复项全部 resolved，调度器 stopped / pipeline_complete。143 正常频道有 2909 条视频，先前完整性审计未发现核心字段缺失或重复。

## 验证

- 主相关回归 179 项通过；后续 WEB 类型证据和 API 评论条件回归 94 项通过（与前者有重叠，不相加）。
- PostgreSQL 精简隔离 schema 中真实执行失败收尾 CTE：先复现未释放，占用释放修复后通过；重复失败事件不新增恢复项，旧事件不能清除新代次占用。
- 完成凭据复现用例先报 `Full Crawl scan checkpoint is incomplete or conflicting`，修复后通过；缺失、排队和身份冲突均拒绝写入。
- checkpoint 共享采集及详情路径回归，覆盖 API 未知类型持久化、不调用第二种抓取器、冻结清单校验。

历史失败记录保留。原 API-batch 队列中 2026-08-25 的缺少 key 失败项不属于本轮，未擅自删除。
