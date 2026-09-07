# Full Crawl 评论缺失诊断与可选评论修复（2026-09-07）

## 后续上线状态（08:40 UTC 更新）

用户随后批准灰度部署、补发历史 7 条并测试新 10 频道。灰度抓取 Worker、公共 Finalize、灰度 Controller 已更新为 `fullcrawl-youtubejs-canary-20260907-6`，镜像内 60 项针对性测试全部通过。生产首屏约束已兼容 `NEWEST_FIRST`，变更时内容数保持 21690；没有部署增量 Worker 或扩大 Query。

历史 7 条仅依据已保存、与原候选详情一致的可信 header 总数重新分类为 exact，并刷新发布 item hash；原 Run、v1 契约、数值、评论正文、raw 和候选详情均未改动。通过正式 Finalize 补发两频道 video sequence=2，08:40:48 UTC 真实业务库及搜索均由 89 条补齐到 96 条，投影全部 delivered。旧 7 条的评论正文没有回填，不能把之前只读探针取得正文等同于生产已保存正文。

下文“未部署/未补发”描述保留为诊断完成时的历史状态，以上线更新为当前状态；后续第三批结果见 `FULL_CRAWL_TEN_CHANNEL_CANARY_3_20260907.md`。

## 结论与边界

用户手工确认评论可见，并明确要求评论正文尽力获取、拿不到不能阻止视频发布。按 diagnosing-bugs 流程完成本地复现、受控真实响应对照、代码修复和隔离 PostgreSQL 回归。

已经证实：至少两条异常视频不是网络请求失败，也不是“正文返回后解析丢失”，而是 YouTube 热门评论排序返回零正文；同一受控身份改用响应提供的最新评论入口，HTTP 200 且各得到 3 条原始评论，解析也各得到 3 条。

其余五条重新请求时原热门入口已经正常。历史抓取仅保存归一化详情，缺少当时的完整原始响应，不能倒推出历史那次究竟是短暂响应差异、会话/排序差异或其他因素；不得把这五条全部归因为网络错误或解析错误。

修复后的真实请求已对原 7 条全部获得评论正文。代码及测试已在本地完成，但没有部署新 Worker、没有修改生产表结构、没有回填抓取内容或补发这 7 条历史内容。原第二批抓取 96 / 业务与搜索 89 的线上差额尚未消除。

## 真实响应对照

探针 `qy-fullcrawl-comment-diagnosis-20260907-v3` 于 08:03:41 至 08:03:57 UTC 执行，退出码 0。同一任务受控身份依次请求原入口，以及 YouTube 返回的 Top / Newest 菜单入口；未绕开 Rota 直连，也未使用其他抓取引擎。下表 Top / Newest 请求均 HTTP 200、success=true，原始 commentThreadRenderer、commentEntityPayload 数量与解析正文数量一致。

| 视频 ID | 总数 | Top 原始/解析条数 | Newest 原始/解析条数 |
| --- | ---: | ---: | ---: |
| `RDD-SUVDIzA` | 17 | 12 / 12 | 13 / 13 |
| `Jh85jFIXxB4` | 3 | 0 / 0 | 3 / 3 |
| `f1H73sVcwfk` | 3 | 0 / 0 | 3 / 3 |
| `vYP05rbV4hA` | 3 | 2 / 2 | 2 / 2 |
| `dYU_ocZk0y0` | 1 | 1 / 1 | 1 / 1 |
| `daDsCLAGcXM` | 4 | 4 / 4 | 4 / 4 |
| `fp9ZeBRzzbU` | 1 | 1 / 1 | 1 / 1 |

零正文的 Top 响应仍有 commentsHeaderRenderer 和总数，但没有 commentThreadRenderer、commentViewModel、commentRenderer 或 commentEntityPayload，也没有失败/关闭评论信号。最新评论返回实际正文后，同一归一化函数能全部识别。没有证据证明需要额外等待页面加载或修复正文解析器；已经验证有效的是更换排序入口。总数可能包含回复，而本功能仅保存顶层首屏，不要求正文条数等于总数。

## 原发布缺口

原分类器将“total_count>0、returned_count=0”归为正数加 unresolved；发布层只接受“未确认状态加 null”，因而排除整条视频。这把可选正文是否获取成功错误地绑定到评论数及视频发布上。

已运行的本地红测试包括：

- `videoPublicationCurrent.test.js`：仅将评论改成 17/unresolved，就得到 ready=false，与“视频应可发布”断言冲突。
- `youtubeCommentPage.test.js`：可信 header total=17、零正文被分类为 unresolved，与“数值不依赖正文”断言冲突。
- `youtubeJsStrictDetail.test.js` 和 `fullCrawlYoutubeJsModel.test.js`：可选评论超时/429 仍导致整个完整视频失败。
- 热门零正文、最新评论有正文的最小响应 fixture：原代码只调用一次，预期最多两次的回退断言失败。

这些回归已经修复后通过，没有通过修改生产状态或补零规避问题。

## 本地修改

- `youtubeJs.js`：Full Crawl 可选评论优先请求热门；正数 total 且零正文时，只沿响应的只读 Watch Next 菜单入口追加一次最新评论。记录实际排序、回退原因及 collected/empty/failed；回退失败保留初次可信总数，取消仍抛出原始原因。其他调用默认不启用该回退。
- `youtubeCommentPage.js`：可信正数 total 的状态不再由正文条数决定；支持 NEWEST_FIRST 首屏并保留抓取诊断。缺少可信 total 仍保持未知，明确关闭评论仍保持 disabled/0。
- `videoPublicationCurrent.js`：仅对未确认评论数允许保留 crawler 中的 provisional 数值，业务 payload 仍输出 null/unresolved。不会排除视频，不伪造数值；其他必需字段、非法数值及 disabled 状态一致性检查不放宽。
- `fullCrawlYoutubeJsFactory.js` / `fullCrawlYoutubeJsModel.js`：v2 评论失败不阻止主体完整的视频提交；v1 保留原规则。主体字段、取消及执行 Fence 仍严格处理。
- `schema.sql`：首屏约束兼容 TOP_COMMENTS / NEWEST_FIRST，不能把最新评论冒充热门。只在隔离测试事务中应用了该约束，生产未应用。

评论正文仍不包含在业务发布包中；评论总数是独立的业务字段。真实 header 数字可正常发布，旧的未确认数值则在业务侧保持未知，而不是推断成零或 exact。

## 冻结契约

评论从必需变可选、增加排序回退属于语义变化，使用新契约，不能原地修改旧 Run：

- v1：`youtubejs_full_v1`，原 hash 保持 `sha256:a80e2f3c6e94042b4c12df5ff1dd94a06afd36d395e238904f4d4ca8f575dd80`。
- v2：`youtubejs_full_v2`，hash 为 `sha256:cdd2d1843c89057feb0638f6d6fc4b801dd76832c6effde0a551ca3103fcf911`；增加 comments_required=false、comments_fallback=empty_top_to_newest_once。
- 新灰度 Run 选择 v2；普通任务仍按既有配置选择，未打开全量默认值。v1 / v2 均可识别、恢复，Job 与 binding/Run 跨版本冲突仍拒绝。

## 修复后实测

探针 `qy-fullcrawl-comment-diagnosis-20260907-v4` 于 08:17:25 至 08:17:36 UTC 运行，挂载本地修复源码，只调用实际 fetchYoutubeJsVideoDetail 的可选评论模式，不写内容。退出码 0，断言 7 条全部 exact 且有正文。

| 视频 ID | 评论数状态 | 首屏正文数 | 实际排序 |
| --- | --- | ---: | --- |
| `RDD-SUVDIzA` | 17 / exact | 12 | TOP_COMMENTS |
| `Jh85jFIXxB4` | 3 / exact | 3 | NEWEST_FIRST，自动回退 |
| `f1H73sVcwfk` | 3 / exact | 3 | NEWEST_FIRST，自动回退 |
| `vYP05rbV4hA` | 3 / exact | 2 | TOP_COMMENTS |
| `dYU_ocZk0y0` | 1 / exact | 1 | TOP_COMMENTS |
| `daDsCLAGcXM` | 4 / exact | 4 | TOP_COMMENTS |
| `fp9ZeBRzzbU` | 1 / exact | 1 | TOP_COMMENTS |

隔离 PostgreSQL 的 16 项回归全部通过，无跳过，覆盖真实详情获取与 NEWEST_FIRST 入库、Store 检查点及 Finalize 发布。可选评论场景中 30 条原始 17/unresolved 的视频全部生成真实发布 Outbox 消息，业务 payload 的评论数为 null/unresolved，原 crawler 数值仍为 17，重复 reconcile 不重复发布。业务消费者契约另有回归接受此 payload，不需要分发评论正文。

最终 Node 26 全量回归共 1682 项，1515 通过、167 跳过、零失败/取消；未配置集成环境的测试按条件跳过，并显式排除 `real curl_cffi|persistent yt-dlp`。上述 16 项数据库测试另行在真实隔离 PostgreSQL 环境实际通过。`git diff --check` 通过。

## 运行状态及后续部署

诊断启动时 40 个 channel 槽位均已占用。先只读确认灰度队列完全空闲，再暂时正常停止唯一灰度 Worker，使用释放的槽位运行官方受控诊断流程；诊断后原 Worker 已恢复。未调整 Rota 容量、策略或增量 Worker。

现有灰度 Worker 仍运行 `fullcrawl-youtubejs-canary-20260907-5`，公共 Finalize 仍为 -4。所有一次性诊断容器均已退出；隔离 PostgreSQL 已停止并保留。诊断仅产生正常受控运行审计，没有写入 crawler.contents 或 publication 消息。

正式上线前需要单独受控执行：兼容首屏排序约束；构建、更新所需 Worker 及契约读取/恢复端，确保能识别 v2；然后验证新 Run。旧 7 条若需补齐评论和补发，应使用固定名单、保留原 Run 的专门补正流程，不重跑整个频道、不把 v1 binding 改成 v2。生产结构修改和历史补发不包含在本轮已完成动作中。

临时证据与工具：`/tmp/fullcrawl-comment-probe-20260907.mjs`、`/tmp/fullcrawl-comment-fixed-probe-20260907.mjs`、`/tmp/fullcrawl-comment-probe-deploy-20260907.mjs`；诊断容器日志保留实际结构摘要。首个旧诊断脚本因缺少新版 attemptsStarted 在发出 YouTube 请求前失败，后续临时探针补齐身份后运行成功；不把该工具错误当作评论网络错误。
