# Full Crawl 十频道灰度字段观察（2026-09-07）

## 最新结论（2026-09-07 07:26 UTC）

本批次已完整收尾：7 个活跃频道的 123 条内容已发布到业务库并进入活动搜索快照，时长状态全部为 `exact`；3 个近期无内容的频道按规则休眠，没有强制发布。Controller 自动将批次标记为 `completed`，调度器以 `pipeline_complete` 正常停止。抓取 Worker 和公共 Finalize Worker 均已更新至 `20260907-4`。

以下按时间保留各阶段证据；早期的“未部署”“尚未恢复”“93 unresolved”“finishing”等描述是当时快照，不是当前待办。最终部署和验收见“整批收尾完成”。本次仅完成这 10 个频道的迁移灰度，不代表 Query 实流量验收或全量推广；范围仍为每频道最多 30 条、90 天窗口，评论仅首屏。

## 执行范围

用户授权更新现有一个灰度 Worker，新增迁移 10 个频道并观察字段。未扩大到 Query、未更新 Controller、未调整增量 Worker 或 Rota、未修复首轮历史数据。

- 镜像：`qy-allpachong/qybullmq:fullcrawl-youtubejs-canary-20260907-2`，当前 dirty worktree 构建。
- 镜像 Config SHA：`f0ad02d235e8af2efa317b5765fe5c2e78b6f6c938836f3df9f6b37b3001c5e8`。
- 灰度 Worker：`qy-newcrawler-fresh-worker-fullcrawl-canary-1`，容器 ID `13df56c1745fe7406d6e3fcbcf41db72d51b9ccb464573847219a5a75950afbe`。
- 旧 Worker 正常停机保留为 `qy-newcrawler-fresh-worker-fullcrawl-canary-1-before-detail-fix`。
- 保持 `YOUTUBEJS_EXTRACTOR_MODE=full`、专用抓取前缀 `bull-fullcrawl-youtubejs-v1`、并发 1、槽位 `bullmq-channel-10`。旧的两个普通 Worker 仍暂停。
- Controller 仍为 `qy-newcrawler-fresh-controller-fullcrawl-canary-1`，镜像仍为首轮 `20260907-1`，保留灰度恢复路由。
- 新镜像内 71 项相关测试通过，运行容器内断言 full 模式启用且点赞 7 的状态为 exact。
- 批次：`fullcrawl-youtubejs-canary-20260907-ten-1`。冻结来源 Candidate 33810 至 33819，目标 Candidate / Intent 1402 至 1411。再次核验来源哈希及目标不存在后逐个正式投递，无替换名单或第 11 个频道。
- 工具记录：`/tmp/fullcrawl-canary-ten-20260907.mjs`、`/tmp/fullcrawl-canary-ten-observe.mjs`；预检和投递容器分别为 `qy-fullcrawl-canary-ten-plan-20260907`、`qy-fullcrawl-canary-ten-dispatch-20260907`，保留容器日志。

## 抓取与发布结果

抓取约于 06:07:51 至 06:15:34 UTC 执行。最终只读快照：2026-09-07T06:18:30.866Z。

10 个 Job 均 completed、attemptsMade=1，无失败或重试；10 个 Run 均 done。处理 300 条目标，其中 123 条 stored、176 条因 outside_content_window 排除、1 条 upcoming_live 排除；没有 deferred。范围仍为当前每频道最多 30 条、90 天窗口，不代表抓取全部历史内容。

| 来源 / 目标 Candidate | 频道（投递时名称） | Channel ID | 入库内容 | Finalize |
| --- | --- | --- | ---: | --- |
| 33810 / 1402 | Porteirabrasil | `UCY9tQhFW1eAKxmO1Y0QOdlw` | 0 | ready_partial |
| 33811 / 1403 | Podcast Bastidores da Vitória | `UCj8R0AzTziZrMLPU5x4zHQQ` | 30 | ready_partial |
| 33812 / 1404 | Hospital e Maternidade Santa Joana | `UCQsJ3UMMUwo_xCW4z5ba_EA` | 3 | ready_auto |
| 33813 / 1405 | Grupo Medtask \| Residência Médica | `UCXzwIIrAWeKbNSktZBtUDfg` | 0 | ready_partial |
| 33814 / 1406 | Basticast | `UC52fDblxmL-cmHagp-THjAQ` | 30 | ready_partial |
| 33815 / 1407 | Daniel Silva | `UCxCI3kRKfjXMnGKaKYaBqmQ` | 21 | ready_auto |
| 33816 / 1408 | Watch Brasil | `UC9ZhqYrRld8-7u2frDaW2-A` | 4 | ready_auto |
| 33817 / 1409 | CineMasters Podcast | `UCWfuzq9IgS0KJ8lkQ7vBwEA` | 5 | ready_auto |
| 33818 / 1410 | TV ENTORNO | `UCn08aBjCWkHkIia2FNYZu7w` | 30 | ready_partial |
| 33819 / 1411 | Raylla Monção | `UCSatLxdCd8NEU5PSP_mkEew` | 0 | ready_partial |

Watch Brasil 的当前 YouTube 标题为 Watch TV。

- 4 个 ready_auto 频道：Hospital、Daniel Silva、Watch TV、CineMasters。12 条发布 Outbox 全部 delivered，各 1 次尝试；正式业务端各有 3 个已应用域，4 条投影 Outbox 全部 delivered。
- Porteirabrasil、Grupo Medtask、Raylla 因近期无内容进入 dormant，Agent skipped，保持 ready_partial，不强制发布。
- Podcast Bastidores、Basticast、TV ENTORNO 的选中详情已经全部完成、Agent done，但初始视频观察 partial，仍为 ready_partial，未自动发布。具体证据断接见下节。
- 最后检查调度器仍为 finishing、cycle 为本批次。没有强制改成 completed/stopped，没有绕过自动发布门槛。不能把“10 个抓取 Job 完成”描述为“10 个频道全部发布且批次收尾”。

## 字段观察

123 条入库内容：44 个 video、78 个 short、1 个 live（回放），全部 public。

| 字段 | 实际状态 |
| --- | --- |
| 标题 | 123 条有值 |
| 发布时间 | 123 条 exact |
| 时长 | 123 条有数值，但状态全部 unresolved，属于缺陷 |
| 播放量 | 123 条有值、exact |
| 点赞 | 93 条 exact；30 条 zero_from_empty（非公开点赞的策略零，不是真实零的证明） |
| 评论数 | 46 条 exact；44 条 zero_from_surface；33 条 disabled |
| 描述 | 102 条 exact；21 条明确 empty |
| 评论首屏 | 123 条都有首屏对象；46 条非空，共 328 条首屏评论 |

发布时间、时长、播放量、点赞数、评论数数值字段没有 NULL。评论只抓首屏，不是全部评论。以上是已有入库内容统计，不将 177 条排除目标混入分母。

## 新发现的缺陷

### 时长状态丢失

解析器输出 duration_seconds 和 duration_source，但没有 duration_status；Full Crawl 入库默认 unresolved。实际抽样原始秒数 305、68、78 都存在，raw detail 中 status 缺失。不是没抓到时长，而是状态传递未闭合。检查点与现有业务记录均保留，本轮没有修复或补写。

### 扫描证据与 Finalize 断接

10 个 Run 的 `result_json.upload_scan` 均为空；新执行器把扫描文档保存在 `result_json.full_crawl.uploads.document`，旧 Finalize 的初始视频观察仍从 `run.result_json.upload_scan` 读取。

三个受影响活跃频道的观察为 `initial_full_video_partial`，discovery 表明 items=30、detail_success_count=30、detail_failure_count=0、stop_reason=max_items。现有策略的 VIDEO_WINDOW_MAX_ITEMS 本身就是 30，并且支持带完整扫描证据的候选上限终止；因此不能简单归因于“必须提高 30 条上限”。应在后续修复中补齐新检查点到观察器的证据传递，并验证正确终止条件，不能伪造 complete 或直接放宽发布门槛。

本轮仅部署已经验证过的点赞/评论修复并观察；未临时修改这两处代码、重建 Run、删除检查点或强行发布三个部分就绪频道。下一步应先补对应回归，再讨论用已存证据恢复这三个 Run 的发布。

## 后续本地修复进展

时长状态缺失已在 `youtubeJs.js` 修复：有效正数时长输出 `duration_status=exact`，无有效时长输出 `unresolved`。不改变入库默认规则，也不放宽普通视频详情完整性要求。

已通过隔离 PostgreSQL 复现修复前的错误（60 秒入库后状态为 unresolved），修复后视频详情相关 66 个测试全部通过，无跳过；覆盖正数时长，以及直播时长为零、缺失、非法数值的未确定状态。`git diff --check` 通过。

扫描证据与 Finalize 的断接已在 `initialFullObservations.js` 本地修复：新执行器的最终观察从 `full_crawl.uploads.document` 读取扫描证据，使用 Run 冻结的条数上限与时间窗口，并校验扫描/目标哈希、候选身份及抓取完成回执；不另写一份 `upload_scan`。旧执行器仍读取旧字段。缺失或冲突的新检查点会拒绝处理，不退回旧字段或按内容数量猜测完成。

同时兼容 `outside_content_window` 年龄排除证据，并阻止新执行器的 deferred 详情被扫描终止条件误判成完整。不提高 30 条上限、不放宽 90 天窗口规则。

新增 `fullCrawlInitialObservations.postgres.integration.test.js`，从真实 Store 的准入、扫描提交、详情提交、抓取关闭一路验证真实初始观察记录。修复前已分别复现 30 候选误判、年龄边界漏认，以及 deferred 误放行；修复后 10 个数据库回归全部通过，包括列表到尾、解析缺口、较短窗口、证据不足、损坏检查点、同 Run 修复与重复执行。

最终验证：Node 20 针对性测试 110 个通过、零失败、零跳过，包含真实隔离 PostgreSQL；Node 26 全量回归共 1671 个，1507 通过、164 跳过、零失败/取消。全量测试未配置集成环境，并明确排除 `real curl_cffi|persistent yt-dlp`；新增的数据库测试已在前述针对性运行中实际通过。`git diff --check` 通过。

本次尚未部署，线上已有 123 条内容的时长状态没有回填，三个部分就绪频道及调度批次也没有修改。隔离数据库证明：普通重复 Finalize 会保留既有 partial 观察；使用正式 `revisionType=repair` 和稳定 `repairId` 可以在同一 Run 上复用检查点/内容生成 complete 视频观察，重复修复复用同一观察 ID，无需重新抓取。此验证仅覆盖观察层，不等于线上最终发布已经恢复。后续需确认实际执行 Finalize 的 Worker 也加载修复代码，再受控运行修复并观察发布、投递和批次收敛；不能只更新抓取 Worker 后就宣称完成。

## 单频道恢复实测（2026-09-07 06:49 UTC）

用户授权先更新灰度代码、恢复一个频道。已构建并部署 `qy-allpachong/qybullmq:fullcrawl-youtubejs-canary-20260907-3`，镜像 Config SHA 为 `c0a16eca55619fac5dde4be6be2efde8e6a18c45083d4da87f8838dbb50bdcf8`。镜像内 105 个针对性测试通过、无跳过。

- 现有灰度 Worker 已更新，容器 ID 为 `cda0293eb32a54ca2a97f2a1901e6c65d69586b78a15ba3865834e37865988c2`，仍仅消费专用抓取队列、并发 1、使用原槽位 `bullmq-channel-10`。
- 上一版本保留为停止的 `qy-newcrawler-fresh-worker-fullcrawl-canary-1-before-scan-fix`，正常退出码 0。
- 公共 `qy-newcrawler-fresh-worker-finalize-1` 没有替换。使用新镜像的一次性 Finalize 容器，只消费专用 Redis 前缀 `bull-fullcrawl-finalize-repair-20260907-one-1` 的固定单频道任务，未影响其他队列。
- 目标为 Podcast Bastidores da Vitoria，频道 `UCj8R0AzTziZrMLPU5x4zHQQ`，Run 仍为 `run:364fc956-d4bd-4b14-9af2-e503068d8981`。
- 修复 ID 为 `fullcrawl-scan-evidence-20260907-one-1`，Job 为 `finalize-scan-evidence-one-1`，调用正式 `processFinalizeV2`，Job completed，一次性容器正常退出码 0。
- Finalize 从 ready_partial 升为 ready_auto，About/Video/Agent 观察均为 complete，原 partial 观察保留。
- 扫描检查点、全部候选详情、内容原始记录及 Agent 结果的前后指纹完全一致。十个原抓取 Job 仍全部 completed、各执行 1 次，总内容仍为 123 条。没有重抓、换 Run 或回填时长状态。

**尚未发布成功。** 正式发布返回 `not_owned`，没有生成目标频道的发布 Outbox。Run 已由正式流程记录 `publication_gap_repair.status=required`，唯一缺口是视频域的 `video_window_termination_unproven`。

根因进一步定位到 `videoPublicationCurrent.js` 的 `processedInitialCandidateLimit()`：发布层也直接读取旧 `run.result_json.upload_scan`，尚未适配新检查点。它已经允许 `repair_video_complete`，并非修复观察类型被禁止。此前修复/测试仅贯穿初始观察，没有覆盖到发布包就绪判断，不能将“观察 complete”当成发布完成。

下一步应将“新检查点 -> 修复观察 -> 视频发布校验/发布消息”纳入回归，并让发布层与观察层使用一致、经过校验的扫描证据，不能手工填旧字段或绕过终止证明。然后针对该频道已有发布缺口走受控恢复流程；当前修复脚本要求 ready_partial，不能原样再次启动或换名盲目重试。

Basticast、TV ENTORNO 仍 ready_partial，未恢复；本批次仍 finishing。增量 Worker、Rota、Controller 与公共 Finalize Worker 没有部署变更。

操作记录保留于 `/tmp/fullcrawl-single-deploy-20260907.mjs`、`/tmp/fullcrawl-single-finalize-20260907.mjs`、`/tmp/fullcrawl-single-observe-20260907.mjs`。预检与执行容器分别为 `qy-fullcrawl-single-finalize-20260907-plan`、`qy-fullcrawl-single-finalize-20260907-repair`，均已退出并保留日志。

## 发布层修复与内容预检

用户确认继续修复发布读取并恢复同一频道后，新增真实数据库回归，复现了“repair 观察 complete，但实际发布包仍报 video_window_termination_unproven”。现已新增 `fullCrawlScanEvidence.js`，让 `initialFullObservations.js` 和 `videoPublicationCurrent.js` 共用经过哈希、候选身份和完成回执校验的扫描证据；`publicationReadinessReport.js` 为新执行器的视频源加载对应候选证据。旧执行器继续使用旧字段，损坏证据不能通过候选上限证明。

验证包括真实视频发布消息写入与重复执行去重、消息中的实际内容数量、观察完成后检查点损坏或候选身份改变仍拒绝发布。针对性 155 个测试通过，无跳过；全量共 1673 个，1507 通过、166 跳过、零失败/取消。加强消息内容断言后，12 个数据库回归再次全部通过。待部署镜像内另有 40 个相关测试通过。

镜像 `qy-allpachong/qybullmq:fullcrawl-youtubejs-canary-20260907-4` 已构建，Config SHA 为 `c1c600716f5e99f5974f2f3c63e1f265013ea278ee4d4b1b9528ffffa7b4a572`，但**没有替换运行中的 Worker，也没有执行生产恢复**。当前灰度 Worker 仍是上一节的 `-3`。

只读预检发现：新的扫描证明可通过，但 Podcast 现有 30 条内容的时长状态仍为 unresolved；它们全部被发布 Item 校验以 duration 不完整排除。因此原发布策略会得到“扫描完整、视频包为空”的 ready 结果。新增单频道操作断言要求实际发布包包含全部 30 条，已在任何发布写入前拦截此情况，未修改全局发布策略。

进一步只读核验，30 条内容的正整数秒数均与候选详情中保存的 `youtubejs_player` 来源一致。在内存中仅补正 `duration_status=exact` 并重新计算 Item 哈希后，发布包 qualified/selected 均为 30，excluded 为 0。数据库没有修改。

待确认范围：仅补正 Podcast 这一个频道的 30 条历史时长状态及发布哈希，秒数、原始抓取证据和其他 93 条内容不动，然后继续同 Run 发布。此前计划明确暂不回填时长，因此这一步没有擅自执行。公共 Finalize、其他频道、Rota 和增量 Worker 均未变更。

只读预检容器 `qy-fullcrawl-single-publication-20260907-plan` 和 `qy-fullcrawl-single-publication-duration-20260907-plan` 保留日志；均因“实际包为 0 而预期 30”的保护性断言退出，非抓取失败。`/tmp/fullcrawl-single-deploy-20260907.mjs`、`/tmp/fullcrawl-single-finalize-20260907.mjs` 已更新到 `-4` 预检版本，不能将此前操作记录中的版本参数与当前文件混用。

## 单频道补正与发布完成（2026-09-07 07:15 UTC）

用户明确授权仅补正 Podcast Bastidores 这个频道的 30 条历史时长状态及发布哈希，然后继续同频道发布。已完成，未处理其他频道。

- 灰度 Worker 更新为 `fullcrawl-youtubejs-canary-20260907-4`，容器 ID `a0190a688f7666dd15e55a56dfbfb695a4eec5fb9291da63ef37548f56ae91ad`。仍仅监听原专用抓取队列，并发 1、原 Rota 槽位。上一版本保留为停止的 `qy-newcrawler-fresh-worker-fullcrawl-canary-1-before-publication-scan-fix`。
- 同频道事务先锁定恢复身份、Run、频道与目标内容，逐条验证正整数秒数与候选原始播放器证据一致，再只更新 `duration_status`，通过正式 Item 哈希接口重算哈希。恰好 30 条状态及 30 个哈希发生变化。发布包在提交前验证 selected=30、excluded=0；任何不符均回滚。
- 事务断言其他内容字段、候选详情和 Run 均未变化。秒数、原始抓取证据、Agent 结果未修改。补正前的状态和哈希保留在一次性容器的 `duration_correction_before` 日志中。
- 使用原 Run `run:364fc956-d4bd-4b14-9af2-e503068d8981`，正式修复 Job `finalize-publication-evidence-one-1` completed，返回 ready_auto、publication_status=revised。没有重抓，也没有新建 Run。
- 频道、视频、Agent 三条发布消息全部 delivered，各尝试 1 次。发布归属 owned、bootstrap seed complete。
- 业务端三个域均已应用 sequence=1；`result.content_current` 收到 30 条，活动搜索快照也包含 30 条内容。搜索投影 Outbox delivered，尝试 1 次。业务端频道名为 Podcast Bastidores da Vitoria。
- 一次性容器 `qy-fullcrawl-single-publication-duration-20260907-repair` 正常退出码 0，日志保留。公共 Finalize Worker、Controller、Rota 和增量 Worker 未更换。

本批次内容总数仍为 123，时长状态现在 30 exact、93 unresolved；另外 93 条未补正。十个原抓取 Job 仍全部 completed、各执行 1 次。Basticast 和 TV ENTORNO 仍 ready_partial，尚未恢复，调度批次仍 finishing，不能将本次单频道完成描述为整批完成。

原 Run 的历史 `publication_gap_repair.status=required` 记录未手工清除；当前发布已成功、业务端已应用，后续若治理历史缺口记录，应根据实际发布证据处理，不要因此重新抓取。只读业务核验脚本为 `/tmp/fullcrawl-single-business-observe-20260907.mjs`。补正脚本要求目标原状态为 unresolved，不能原样重复启动。

## 整批收尾完成（2026-09-07 07:26 UTC）

用户授权继续完成剩余收尾后，已补正其余 6 个活跃频道的 93 条时长状态及发布哈希，恢复 Basticast、TV ENTORNO 的发布，并验证所有活跃频道的业务落库和搜索投影。

### 部署与修复边界

- 灰度抓取 Worker 和公共 `qy-newcrawler-fresh-worker-finalize-1` 均使用 `qy-allpachong/qybullmq:fullcrawl-youtubejs-canary-20260907-4`，Config SHA 为 `c1c600716f5e99f5974f2f3c63e1f265013ea278ee4d4b1b9528ffffa7b4a572`。灰度抓取 Worker 保持上一节的容器身份、专用队列、并发 1 和原槽位。
- 公共 Finalize 队列确认空闲后正常停旧换新，新容器 ID 为 `f505f3a7ab005b5377830d44eba9400c4f249073428fea7468d957a6bd9e65d6`，仅消费 `youtube-finalize`、并发 1、managed=false，原环境、配置及重启策略保留。旧容器正常退出码 0，保留为 `qy-newcrawler-fresh-worker-finalize-1-before-fullcrawl-scan-fix`。
- 剩余修复通过一次性容器 `qy-fullcrawl-closeout-20260907-execute` 执行，专用 BullMQ 前缀为 `bull-fullcrawl-closeout-20260907`。固定 6 个频道逐个事务校验原始候选的 `youtubejs_player` 证据及正整数秒数，仅补正时长状态，再调用 `refreshVideoPublicationItemHashes`。其他内容字段、候选记录及 Run 保持不变，补正前状态和哈希保留在 `closeout_duration_backup` 日志中。
- 六个修复 Job 使用 `closeout-<channelId>` 和稳定修复 ID `closeout-20260907-<channelId>`，调用正式 `processFinalizeV2`，全部 completed、ready_auto、publication_status=revised；一次性容器退出码 0，日志保留。
- 本次未更新 Controller、增量 Worker 或 Rota，未扩大频道名单，未重新抓取、创建替代 Run 或放宽发布门槛。

### 最终业务与字段核验

业务端只读核验时间为 `2026-09-07T07:24:31.691Z`。以下 7 个频道均有 `agent`、`channel`、`video` 三个已应用域，业务活动内容和活动搜索快照数量一致，时长状态全部为 `exact`。

| 频道 | 业务活动内容 | 搜索快照内容 |
| --- | ---: | ---: |
| Podcast Bastidores da Vitória | 30 | 30 |
| Basticast | 30 | 30 |
| TV ENTORNO | 30 | 30 |
| Hospital e Maternidade Santa Joana | 3 | 3 |
| Daniel Silva | 21 | 21 |
| Watch TV | 4 | 4 |
| CineMasters Podcast | 5 | 5 |
| 合计 | 123 | 123 |

最初已发布的 4 个频道视频域更新为 sequence=2，其他域仍为 sequence=1；恢复发布的 3 个频道所有域均为 sequence=1。25 条发布 Outbox 全部 delivered、各尝试 1 次，包括 21 条初始域发布和 4 条视频修订；11 条搜索投影 Outbox 全部 delivered、各尝试 1 次。

最终批次快照时间为 `2026-09-07T07:26:15.622Z`：123 条内容仍为 44 个 video、78 个 short、1 个 live，全部 public、有标题，发布时间、时长及播放量状态全部 exact。点赞仍为 93 exact、30 zero_from_empty；评论数为 46 exact、44 zero_from_surface、33 disabled；描述为 102 exact、21 empty。策略零、关闭评论、空描述的含义没有因本次修复改变。

Porteirabrasil、Grupo Medtask、Raylla Monção 三个频道仍为 dormant / ready_partial、零内容。这是近期无内容的正常结果，不是待恢复的发布故障。10 个原抓取 Job 仍全部 completed、各执行 1 次，Run 身份保持不变，已提交内容没有重抓。

### 自动收敛与历史标记

Controller 于 `2026-09-07T07:23:48.888Z` 自动完成批次 `fullcrawl-youtubejs-canary-20260907-ten-1`：批次 status/outcome 均为 completed，调度器 status=stopped、stop_reason=pipeline_complete。统计为 total=10、accepted=10、rejected=0、failed=0，解析失败为 0。没有手工强改批次完成状态。

最终检查 `loadPipelineFinalizeBlockers` 返回 agentOpen=0、finalOpen=0、publicationOpen=0，`hasOpenPipelineCrawlerWork=false`。

Podcast 原 Run 的历史发布缺口标记于 `2026-09-07T07:25:31.561Z` 按实际发布证据置为 resolved：先锁定 Run/频道，校验仍为原 Run、done、ready_auto，当前三个域的发布包均 ready、哈希与当前发布一致、对应消息均 delivered。保留原 reason、readiness_domains、detected_at，增加 resolved_by=`fullcrawl-closeout-20260907`、resolution=`same_run_evidence_repair_published`、resolved_at 及各域 publication_evidence。最终本批次不存在 status=required 的历史发布缺口，未因此修改内容或再次抓取。

操作及只读核验脚本保留于 `/tmp/fullcrawl-closeout-20260907.mjs`、`/tmp/fullcrawl-closeout-business-20260907.mjs`、`/tmp/fullcrawl-closeout-state-20260907.mjs`、`/tmp/fullcrawl-closeout-gap-20260907.mjs`；部署操作使用 `/tmp/fullcrawl-single-deploy-20260907.mjs`。修复入口要求补正前恰好 93 条 unresolved，现已不满足，不能原样再次执行；历史标记修复也不得盲目重复运行。

## Run 身份明细

- `UCY9tQhFW1eAKxmO1Y0QOdlw`: `run:7246d96d-d1a3-4033-87eb-71b6d16df206`
- `UCj8R0AzTziZrMLPU5x4zHQQ`: `run:364fc956-d4bd-4b14-9af2-e503068d8981`
- `UCQsJ3UMMUwo_xCW4z5ba_EA`: `run:40f0f2f6-7040-40ac-97c9-1ec3888068b3`
- `UCXzwIIrAWeKbNSktZBtUDfg`: `run:de89b89c-6666-41e7-9e0b-635772b3e937`
- `UC52fDblxmL-cmHagp-THjAQ`: `run:c722d044-e515-4e99-81f4-3f1cfac2102e`
- `UCxCI3kRKfjXMnGKaKYaBqmQ`: `run:33d82931-fab0-4b3c-8c0e-9d6efc02222a`
- `UC9ZhqYrRld8-7u2frDaW2-A`: `run:583317ed-5c1d-423b-a8e8-6693f2fe3b9a`
- `UCWfuzq9IgS0KJ8lkQ7vBwEA`: `run:06236137-943f-48a3-b449-fe36bef25f2c`
- `UCn08aBjCWkHkIia2FNYZu7w`: `run:6458fa89-8fe9-442e-ac4d-c1a87346a9ce`
- `UCSatLxdCd8NEU5PSP_mkEew`: `run:8aeffe0e-17f4-40e6-8815-3b781f136de1`
