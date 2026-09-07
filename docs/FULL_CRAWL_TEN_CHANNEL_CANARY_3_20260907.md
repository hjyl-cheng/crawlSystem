# Full Crawl 第三批 10 频道灰度验收（2026-09-07）

## 结论

按用户授权顺序完成：部署可选评论修复、原 7 条历史内容补发、另选 10 个新频道迁移。第三批于 08:45:47 UTC 自动收敛，08:46 至 08:47 UTC 完成业务库、搜索和逐频道发布包核验。

- 新批次 10 个抓取 Job 均一次执行 completed，无失败和重试；9 个新 Run 均 done 且使用 v2，另 1 个频道在准入前因社区准则被拒绝。
- 5 个活跃频道，抓取 / 发布包 / 业务库 / 搜索均为 113 条，逐频道数量一致，实际发布包 exclusions 为空。
- 4 个频道因 no_published_content_within_90_days 休眠，Agent 按规则 skipped，Finalize 为 ready_partial；不是字段缺失造成的部分发布。
- 15 条三域发布消息全部 delivered，各一次投递；业务搜索投影全部 delivered。
- 调度器 stopped / pipeline_complete，批次 completed，agentOpen=0、finalOpen=0、publicationOpen=0、open=false，未手工修改收敛状态。
- 评论正文仍为尽力获取，不保证全部可见评论均返回；本批有 1 条正数总量但无正文，视频正常发布。

## 部署范围

镜像：`qy-allpachong/qybullmq:fullcrawl-youtubejs-canary-20260907-6`。

更新的三个容器：

- `qy-newcrawler-fresh-worker-fullcrawl-canary-1`：独立灰度队列，concurrency=1，youtubejs full，slot=bullmq-channel-10，route_generation=5。
- `qy-newcrawler-fresh-worker-finalize-1`：保留公共 youtube-finalize 单队列及 concurrency=1。
- `qy-newcrawler-fresh-controller-fullcrawl-canary-1`：保留原控制器配置，兼容 v1/v2 恢复。

三个新容器检查时 running，RestartCount=0。旧容器正常停止并以原名加 `-before-optional-comments` 保留。所有容器保留 SKIP_SCHEMA_MIGRATION=true；未部署增量 Worker、Rota 或业务消费者，未扩大 Query。

镜像内 60 项针对性测试全部通过，无失败/跳过，覆盖 v1/v2 契约、Store、Factory、评论排序、可选评论取消、发布包及业务契约。此前全量与真实隔离 PG 回归见评论诊断文档。

数据库仅使用官方受控脚本更新 comments_first_page 形状约束，允许 TOP_COMMENTS / NEWEST_FIRST。目标数据库 newcrawler_crawler，变更时 contents=21690，其他结构未迁移。首次操作辅助脚本因应用使用组合连接配置、未显式传 DATABASE_URL 而在执行 DDL 前退出；改用现有 databaseUrl() 后成功，保留两次日志。

## 历史 7 条补发

固定第二批的 WP CLIPS 5 条、Futebol Doc 2 条，原 Run 不变。执行前按真实 classifyYoutubeCommentPage 验证已保存 header 总数、原候选详情和内容数值一致，无 comments_error；按正式发布 item 构建器检查修正后 ready=true。

事务内仅更新 7 条 comment_count_status 为 exact，调用官方 refreshVideoPublicationItemHashes。对其余内容字段、raw、评论首屏、候选、Run 做前后相等断言；不重抓，不回填正文，不改 v1 契约。两次正式 processFinalizeV2 均 ready_auto / revised，稳定 Job ID 保留在独立补发队列。

- WP CLIPS：video sequence=2，25 -> 30。
- Futebol Doc：video sequence=2，5 -> 7。
- agent/channel sequence 仍为 1；第二批真实业务与搜索 89 -> 96。
- 08:40:48 UTC 原业务核验脚本通过，所有投影 delivered；第一次检查恰遇 Futebol 搜索投影 leased，等待后正常一致，未修改投影状态。
- 收尾再次确认原 7 条状态 exact、hash 非空、Run 不变，两补发 Job completed，补发队列无未结任务。

补发一次性容器在动作完成后仍保持运行，确认 Job 和队列终态后单独停止并保留日志，不将容器存活误判为业务补发未完成。

## 冻结名单与结果

批次：`fullcrawl-youtubejs-canary-20260907-ten-3`。

从正式迁移源选择，排除 crawler.channels、migration_channel_intents、channel_candidates 中所有已有频道。先冻结源候选 ID、频道 ID、snapshot_sha256；投递前逐一验证源快照未变。候选 ID 为 33830、33831、33832、33833、33835 至 33840，不推定连续 ID，不补投第 11 个频道。

| 频道 | Channel ID | 结果 | 抓取 / 业务 / 搜索 |
| --- | --- | --- | ---: |
| André Hernan | `UCEKt1YRDsDWL_fIr5KVAnOQ` | 90 天无内容 / 休眠 | 0 |
| Payciúma | `UCr0D5RXI0yntYYTaK70lPKw` | 社区准则拒绝 | 0 |
| Kleberiano Games | `UCjG6OU846XUdPNu_Ke9MNPg` | 活跃 / 已发布 | 22 |
| Batalha da Matrix | `UCxS7ccRXIrJyq53KZ5OId3A` | 活跃 / 已发布 | 30 |
| Will.M | `UCXnnWAdZNBQvjArq3P6gthw` | 活跃 / 已发布 | 30 |
| Centro das Rimas | `UC52N9y_hmKvUuPYShpTwK2A` | 90 天无内容 / 休眠 | 0 |
| roddrigo nobre | `UCOsGR5IzffhThOwB0ai15vA` | 90 天无内容 / 休眠 | 0 |
| ManyRimas HD | `UCYXlPezDXt28FkOvGvZSd1g` | 90 天无内容 / 休眠 | 0 |
| Zen Mc | `UCtisfWlqPNpKikTR0KbIQDA` | 活跃 / 已发布 | 1 |
| Kagibre KGB | `UCd09ybW4y-Ot_wEGJKwC-RQ` | 活跃 / 已发布 | 30 |

休眠和拒绝行的 0 表示本批没有发布内容，不声称这些频道在 YouTube 上没有历史视频。Kagibre KGB 为源标题，当前业务/搜索标题为 Kagibre。

## 视频字段与范围

候选详情终态共 236 条：113 stored，122 outside_content_window，1 upcoming_live。未来直播按规则排除，不当作缺失视频。

| 字段 | 实测 |
| --- | --- |
| 类型 | 53 video、55 short、5 live |
| 标题 | 113 present |
| 发布时间 | 113 exact |
| 时长 | 113 exact |
| 播放数 | 113 exact |
| 点赞数 | 113 exact |
| 评论数 | 82 exact、31 zero_from_surface，0 unresolved |
| 描述 | 61 exact、52 empty |
| 访问状态 | 113 public |
| 首屏对象 | 113 已保存，0 missing_page |
| 评论正文 | 81 个视频共 902 条顶层首屏正文 |

描述 empty 是已识别的空描述，不伪造非空内容。首屏正文数量不要求等于评论总数，后者可能包含回复、其他页或不可见内容。所有主体必需字段继续严格核验。

## 本次真实评论回退

两个视频触发 positive_total_empty_top，实际保存 NEWEST_FIRST 及诊断：

| 视频 | 总数 / 状态 | 回退首屏正文 | 结果 |
| --- | --- | ---: | --- |
| `iHRoYUBHiy0` | 540 / exact | 20 | fallback_status=collected |
| `hCz0C9PlNZo` | 614 / exact | 0 | fallback_status=empty |

两条均无 comments_error。第二条在两种排序下归一化首屏仍为空，保留可信总数，已包含在 Kleberiano Games 的 22 条发布内容内；未因评论阻止发布，也未把 614 改成零。本轮没有对该新视频进行额外原始响应探针，不能仅凭保存的归一化页面进一步断定是上游响应差异还是该次结构解析问题。旧 7 条中的两条原始响应对照诊断结论不自动推广到这个新样本。

## Run 身份

- `UCEKt1YRDsDWL_fIr5KVAnOQ`: `run:12517887-1a59-4d39-9278-148ec952a0fb`
- `UCjG6OU846XUdPNu_Ke9MNPg`: `run:4618ff28-ca4f-46ae-8e95-5163f82d8eae`
- `UCxS7ccRXIrJyq53KZ5OId3A`: `run:f32d425e-f606-4604-8ef4-485dfd41dba8`
- `UCXnnWAdZNBQvjArq3P6gthw`: `run:e9f138ba-3ab6-49d5-807a-dee9ee181cc3`
- `UC52N9y_hmKvUuPYShpTwK2A`: `run:58988506-c889-4c50-b60c-d7dbad107207`
- `UCOsGR5IzffhThOwB0ai15vA`: `run:e7964169-2970-402e-a931-6852b59e5bb9`
- `UCYXlPezDXt28FkOvGvZSd1g`: `run:8c4bdaf5-2079-4e07-b3c9-c2c009cf3080`
- `UCtisfWlqPNpKikTR0KbIQDA`: `run:3fce1748-5f75-4a49-a80e-762a133ff46a`
- `UCd09ybW4y-Ot_wEGJKwC-RQ`: `run:a0f49b48-e819-4b6a-97de-b59474439283`

所有新 Run 的 executor_version=2，contract_hash=`sha256:cdd2d1843c89057feb0638f6d6fc4b801dd76832c6effde0a551ca3103fcf911`。社区准则拒绝的频道没有 Run。

## 操作与复核

以下是本次临时操作记录，不是可盲目重复运行的上线入口：

- `/tmp/fullcrawl-optional-deploy-20260907.mjs`：三个容器定向更新及一次性脚本容器。
- `/tmp/fullcrawl-optional-schema-20260907.mjs`：受控评论排序约束更新。
- `/tmp/fullcrawl-optional-history-20260907.mjs`：固定历史 7 条计划、备份和补发。
- `/tmp/fullcrawl-canary-ten-3-20260907.mjs`：第三批冻结及投递。
- `/tmp/fullcrawl-canary-ten-3-observe.mjs`：字段、评论、任务终态。
- `/tmp/fullcrawl-canary-ten-3-verify.mjs`：CANARY_FINAL_ASSERT=true 时逐频道发布与 v2 契约断言。
- `/tmp/fullcrawl-canary-ten-3-business.mjs`：真实业务及搜索一致性断言。
- `/tmp/fullcrawl-canary-ten-3-state.mjs`：自动收敛及发布阻塞核验。
- `/tmp/fullcrawl-optional-final-audit-20260907.mjs`：三队列空闲、历史 Job 和 7 条哈希复核。

计划容器 `qy-fullcrawl-canary-ten-3-plan-20260907`、投递容器 `qy-fullcrawl-canary-ten-3-dispatch-20260907`、成功 schema 容器和历史计划容器均退出码 0。补发执行日志保留在 `qy-fullcrawl-optional-history-execute-20260907`，不要重启该已执行一次性容器。

当前灰度 Worker 继续运行，独立抓取队列及公共 Finalize 无 active/waiting/prioritized/delayed/paused 任务。此结论仅覆盖本次固定名单灰度，不等同于批准全量部署或无限评论补抓。
