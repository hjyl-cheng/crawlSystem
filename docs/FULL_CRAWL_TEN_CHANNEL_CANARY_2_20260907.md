# Full Crawl 第二批十频道迁移灰度（2026-09-07）

## 验收结论

后续诊断与本地修复见 [评论缺失诊断](FULL_CRAWL_COMMENT_DIAGNOSIS_20260907.md)：已证实两条热门排序为空而最新评论可见，修复后的真实请求已为原 7 条全部获取正文。本次诊断没有部署修复或补发历史内容，以下 96/89 的线上验收差额仍保留，不能视为已解决。

部署和这 10 个新频道的测试已执行完成，但内容完整性验收未通过：抓取库 96 条，业务库及活动搜索快照只有 89 条。7 条内容因评论字段契约不满足被发布层排除。批次自动 completed 不代表全部抓取内容均已发布；不据此扩大灰度或宣称所有字段完整。

上一批时长状态及扫描证据断接没有在本批复现：96 条时长均直接为 exact，6 个活跃频道均自动 ready_auto，没有人工补正或修复 Finalize。其余 4 个频道近期无内容，正常 dormant / ready_partial。

## 部署和范围

- 批次：`fullcrawl-youtubejs-canary-20260907-ten-2`。仅 Migration，未扩大到 Query；每频道最多 30 条、90 天窗口，评论仅首屏。
- 从当前工作区构建镜像 `qy-allpachong/qybullmq:fullcrawl-youtubejs-canary-20260907-5`，Config SHA `66406ab1c9c7da18e5c0b6c033be68021d4f75dfd9d00e37f5189e0924d64918`。构建的源码层命中缓存，本轮没有修改业务源码。
- 仅替换 `qy-newcrawler-fresh-worker-fullcrawl-canary-1`，容器 ID `3e4b990b064c4229f2b82b703d1e1a0b748198970403426b81a927d458140dff`。保持专用抓取前缀 `bull-fullcrawl-youtubejs-v1`、并发 1、full 提取模式、Rota 槽位 `bullmq-channel-10`，启动时 route_generation=5。
- 旧容器正常退出码 0、无 OOM，保留为 `qy-newcrawler-fresh-worker-fullcrawl-canary-1-before-ten-2`，镜像为 -4。新容器正常运行。
- 公共 Finalize Worker 仍为上一批已部署的 -4，容器身份不变且正常运行。本轮未更新 Controller、公共 Finalize、增量 Worker 或 Rota 服务。
- 镜像内离线回归 15 项通过、零失败/跳过，覆盖灰度路由、抓取契约、新执行器模型及依赖范围；本轮未重新运行整个测试套件。
- 预检确认上一批 completed、调度器 stopped、无遗留阻塞。排除目标 channels、migration_channel_intents、channel_candidates 中已有频道后，冻结来源 Candidate 33820 至 33829 及 snapshot_sha256；投递前再次逐一校验来源哈希和目标不存在。
- 目标 Candidate / Intent 为 1412 至 1421；恰好 10 个 created=true，无第 11 个频道，没有重复上一批频道。
- 预检容器 `qy-fullcrawl-canary-ten-2-plan-20260907`、投递容器 `qy-fullcrawl-canary-ten-2-dispatch-20260907` 均正常退出码 0，冻结名单及投递身份保留在容器日志。

## 频道结果

抓取观察时间：`2026-09-07T07:43:28.429Z`。业务及活动搜索快照观察时间：`2026-09-07T07:44:31.804Z`。表中“业务/搜索”两边数量一致。

| 来源 / 目标 Candidate | 频道 | Channel ID | 抓取入库 | 业务/搜索 | Finalize |
| --- | --- | --- | ---: | ---: | --- |
| 33820 / 1412 | Uniters Brasil | `UCVL9YQ2GXfa1Fc08VjpqN4Q` | 0 | 0 | ready_partial |
| 33821 / 1413 | Marlon Mendes | `UCsrW6mjW8NfchqRnPu98iXw` | 0 | 0 | ready_partial |
| 33822 / 1414 | Alves | `UCcYp2yuP5N0Dg9dtrb8dztw` | 0 | 0 | ready_partial |
| 33823 / 1415 | WP CLIPS | `UCVc6nznH7w4tRUHpeWyuCcg` | 30 | 25 | ready_auto |
| 33824 / 1416 | Edgard Scandurra | `UCiW6yavjyTTF5_4yVXENnxQ` | 1 | 1 | ready_auto |
| 33825 / 1417 | Central de Fãs Joelma | `UC5O9OeG7GOiAJ-s-HUFBWtA` | 0 | 0 | ready_partial |
| 33826 / 1418 | Miguel Kenji | `UC9ZjaiY_PwIEv7mEO6WvZ4g` | 21 | 21 | ready_auto |
| 33827 / 1419 | Futebora | `UCawyV41xFMtbhMW2RIuwBsg` | 29 | 29 | ready_auto |
| 33828 / 1420 | BASTIDORES DO CINEMA | `UCuFb1zbChX2T4m-ZmFYhbag` | 8 | 8 | ready_auto |
| 33829 / 1421 | Futebol Doc | `UC8SHRvlu6XX66sfXMsiFjCQ` | 7 | 5 | ready_auto |
| 合计 | | | 96 | 89 | |

10 个 Run 全部 done，新执行器契约均为 youtubejs_full。10 个抓取 Job 全部 completed、attemptsMade=1，无重试；300 条目标中 96 stored、201 outside_content_window、3 upcoming_live，没有 deferred。

Uniters Brasil、Marlon Mendes、Alves、Central de Fãs Joelma 的 dormant_reason 均为 no_published_content_within_90_days，Agent skipped；没有强制发布。

6 个活跃频道的 agent/channel/video 三域均已应用 sequence=1；18 条发布 Outbox 全部 delivered、各尝试 1 次，6 条搜索投影 Outbox 全部 delivered、各尝试 1 次。投递成功仅证明实际选入发布包的 89 条内容到达业务端，不包括被排除的 7 条。

## 字段观察

| 字段 | 抓取库实际状态（96 条） |
| --- | --- |
| 类型 | 56 video、40 short |
| 标题 | 96 有值 |
| 发布时间 | 96 exact |
| 时长 | 96 exact |
| 播放量 | 96 exact |
| 点赞 | 96 exact |
| 评论数 | 67 exact、22 zero_from_surface、7 unresolved |
| 描述 | 88 exact、8 empty |
| 访问状态 | 96 public |
| 评论首屏 | 96 有首屏对象；67 条非空，共 478 条首屏评论 |

7 条 unresolved 均有评论数数值，但首屏对象 surface=absent、returned_count=0，不能据此宣称抓到了对应评论正文，也不能将其当成真实零评论。

## 未通过项：评论状态与发布契约断接

只读业务核验命令：

```sh
docker exec -i qy-newcrawler-fresh-business-publication-projector-1 node --input-type=module < /tmp/fullcrawl-canary-ten-2-business.mjs
```

该脚本要求每个活跃频道的业务及搜索内容数量等于抓取数量，实际退出码 1，首个失败断言为 WP CLIPS 的 `25 !== 30`。完整输出同时显示 Futebol Doc 为 `5 / 7`。保留此失败标准，不通过修改预期数量把验收变绿。

用真实 `buildVideoReadiness` 对数据库现存证据只读重建发布包，确认恰好以下 7 条被排除，reason_code=item_contract_incomplete，唯一 issue 为 video_item_field_incomplete / comment_count：

| Channel ID | 内容 ID | 保存的评论数 |
| --- | --- | ---: |
| `UC8SHRvlu6XX66sfXMsiFjCQ` | `Jh85jFIXxB4` | 3 |
| `UC8SHRvlu6XX66sfXMsiFjCQ` | `f1H73sVcwfk` | 3 |
| `UCVc6nznH7w4tRUHpeWyuCcg` | `vYP05rbV4hA` | 3 |
| `UCVc6nznH7w4tRUHpeWyuCcg` | `dYU_ocZk0y0` | 1 |
| `UCVc6nznH7w4tRUHpeWyuCcg` | `RDD-SUVDIzA` | 17 |
| `UCVc6nznH7w4tRUHpeWyuCcg` | `daDsCLAGcXM` | 4 |
| `UCVc6nznH7w4tRUHpeWyuCcg` | `fp9ZeBRzzbU` | 1 |

其余 4 个活跃频道的发布包没有排除项。WP CLIPS 和 Futebol Doc 的包仍 ready=true，选中分别为 25 和 5 条；因此不是业务消费或搜索投影丢消息。

按 diagnosing-bugs 的证据复现流程，对保存的首屏对象调用实际 `classifyYoutubeCommentPage`，7 条均复现原 unresolved 状态和原数值，断言全部通过；候选入库详情中也已经是同样状态，`detail.youtubejs_comments_error` 为 null。这排除了“入库时才丢失状态”的解释。注意此证据是已归一化的详情，不是完整原始网络响应，不能据此排除上游响应结构未被解析的可能。

当前分支与 `pachongsys-incremental-refactor` 的分类器在“total_count>0、returned_count=0”这个分支一致：保留数值，状态 unresolved。而发布层 `countCurrent` 仅在未确认状态的原值为 null 时接受该字段，故正数加 unresolved 会排除整条内容。已定位到可复现的契约不一致，但尚未确定首屏缺失究竟来自 YouTube 响应还是解析逻辑。

本轮未补零、未将 unresolved 强改 exact、未重抓、未修改发布规则或历史数据。后续应先针对这 7 条的响应/解析证据和上述契约建立回归，再决定修复方式；同时验证“抓取完成但部分内容未发布”能被明确观察到。未经验证不得把批次 completed 当作完整性通过。

## 批次收敛

Controller 于 `2026-09-07T07:42:04.304Z` 自动将批次 status/outcome 标为 completed；调度器 status=stopped、stop_reason=pipeline_complete。统计 total=10、accepted=10、rejected=0、failed=0，解析失败统计为 0。没有手工强改批次状态。

只读检查返回 agentOpen=0、finalOpen=0、publicationOpen=0、hasOpenPipelineCrawlerWork=false，publication_gap_repair.status=required 记录为空。这些渠道级指标未反映上述 7 条内容排除，已单独记录为本轮验收未通过项。

## 操作证据

临时操作脚本不作为可重复投递入口；名单已创建，不能盲目重跑投递或替换步骤。

- `/tmp/fullcrawl-canary-ten-2-20260907.mjs`：部署、冻结名单及正式投递。
- `/tmp/fullcrawl-canary-ten-2-observe.mjs`：抓取、字段、发布消息和 Job 观察。
- `/tmp/fullcrawl-canary-ten-2-state.mjs`：批次及阻塞只读核验。
- `/tmp/fullcrawl-canary-ten-2-business.mjs`：完整性断言，当前预期失败。
- `/tmp/fullcrawl-canary-ten-2-comments.mjs`：已存评论首屏分类回放。
- `/tmp/fullcrawl-canary-ten-2-publication.mjs`：真实发布包排除原因及原详情状态核验。

## 后续评论修复补发（08:40 UTC）

用户批准后，部署镜像 `fullcrawl-youtubejs-canary-20260907-6` 并应用首屏排序兼容约束。固定上述 7 个 ID，在原 Run 内使用实际评论分类器核验已保存 header 总数；只修正这 7 条 `comment_count_status` 并刷新 `publication_item_hash`，原评论正文、数值、原始详情、候选和 Run 不变。

通过正式 Finalize，WP CLIPS 的视频包由 25 补到 30，Futebol Doc 由 5 补到 7；两者 video sequence=2，agent/channel 仍为 sequence=1。08:40:48 UTC 执行原业务核验脚本成功，6 个活跃频道合计业务 96 / 搜索 96，所有投影 delivered。原 7 条排除问题已关闭；正文未回填，不把探针的只读结果当作生产正文。

上文“当前预期失败”是补发前的记录。补发工具：`/tmp/fullcrawl-optional-history-20260907.mjs`，执行日志容器 `qy-fullcrawl-optional-history-execute-20260907`；固定名单已执行，不可盲目重跑。

## Run 身份

- `UCVL9YQ2GXfa1Fc08VjpqN4Q`: `run:2e23f3d4-2ba4-43aa-91c6-19069e151f0a`
- `UCsrW6mjW8NfchqRnPu98iXw`: `run:24a501e6-9599-4544-8e0b-0b38f0614355`
- `UCcYp2yuP5N0Dg9dtrb8dztw`: `run:c96a3bfc-c2ce-4a2e-84b8-c7b6c642cb28`
- `UCVc6nznH7w4tRUHpeWyuCcg`: `run:063cf9f6-9a3a-48c9-aa46-41cfc1026261`
- `UCiW6yavjyTTF5_4yVXENnxQ`: `run:59116ba5-1b30-400a-bd23-613a5181d231`
- `UC5O9OeG7GOiAJ-s-HUFBWtA`: `run:22c4cdac-3c6e-4b8b-ad5e-1036a3c383dc`
- `UC9ZjaiY_PwIEv7mEO6WvZ4g`: `run:a3777abc-cd51-4a60-9b0f-0a74a8e08521`
- `UCawyV41xFMtbhMW2RIuwBsg`: `run:8b26207d-1ccf-4b5c-83b4-957e9d4ce9c4`
- `UCuFb1zbChX2T4m-ZmFYhbag`: `run:68fee2b2-f367-4ed3-af4e-8377fd56469b`
- `UC8SHRvlu6XX66sfXMsiFjCQ`: `run:7580cd20-38af-4d35-a913-6502f50af1ba`
