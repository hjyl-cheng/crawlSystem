# Migration Full Crawl 单 Worker 灰度记录

## 当前状态

最新进展：用户随后授权更新灰度 Worker 并新增迁移 10 个频道，已执行。新镜像为 `fullcrawl-youtubejs-canary-20260907-2`；10 个抓取任务完成，123 条内容入库，4 个频道已正式发布，另发现时长状态缺失和扫描证据未传给 Finalize 的问题。调度器仍为 `finishing`。完整结果见 [十频道字段观察](FULL_CRAWL_TEN_CHANNEL_CANARY_20260907.md)。下文保留首轮单频道与字段修复的历史记录，其中“尚未部署”描述的是当时状态。

用户已允许：只测试迁移，新增一个并发为 1 的 Full Crawl Worker，结果进入正式业务库并发布；允许补齐抓取投递和恢复隔离并更新相关服务。Query 和现有普通抓取 Worker 不切流量。

单频道真实迁移已完成，并已在正式业务端完成投影。用户追加允许总共临时暂停两个空闲普通 Worker 后，已启动一个并发为 1 的灰度 Worker，切换为支持灰度路由的新 Controller。首次抓取因继承的 YouTubeJS 模式为 `channel` 而在详情阶段失败；仅修正灰度容器为 `full` 后，通过 BullMQ 重试原 Job，沿用原 Run 和检查点完成抓取及发布。未新增第二个频道，未测试 Query，未重启 Rota 或调整增量服务。当前普通 Worker 1、2 保持暂停，新 Controller 和单个灰度 Worker 保持运行。

最终验证：隔离 PostgreSQL / Redis 定向回归 `52 passed / 0 skipped / 0 failed`；全量回归 `1657 tests / 1504 passed / 153 skipped / 0 failed`。全量命令未配置集成连接变量，因此集成用例在该命令中跳过，但已在独立定向运行实际通过。仍按既定模式排除缺少 Python 依赖的两个测试。`git diff --check` 和修改模块语法检查通过。日志位于 `/tmp/fullcrawl-canary-integration.log` 和 `/tmp/fullcrawl-canary-regression.log`。

真实集成用例使用生产路由 Adapter：普通 Query Job 只存在原抓取队列，灰度 Migration Job 只存在专用前缀队列；Migration 跨进程恢复和重复投递保持该隔离，Finalize 仍出现在原前缀队列。Controller 原任务重试和缺失时阻断分支另以数据库 / 队列 Adapter 测试覆盖；本次生产验证使用操作员调用 BullMQ 原 Job 的 retry API，并未故意触发 Controller 自动恢复分支或生产 SIGKILL。

## 隔离规则

- 专用批次名：`fullcrawl-youtubejs-canary-` 加不可变批次标识。仅普通 `channel-snapshot` 迁移任务可使用；要求 `dispatch_batch_id` 与 `pipeline_cycle_id` 一致、`crawl_mode=full`、`reject_if_no_recent_content=true`、`query_id=null`。
- 逻辑抓取队列仍是 `youtube-channel-crawl`，物理 Redis 前缀为 `<原前缀>-fullcrawl-youtubejs-v1`，原前缀未配置时使用 `bull`。不改全局 `BULLMQ_PREFIX`。
- 新代码的 `createQueues()` 在抓取队列上统一选择批次路由；Outbox、单频道投递和 Controller 重投递使用相同选择规则。普通任务仍走原物理队列。
- 灰度 Worker 配置 `FULL_CRAWL_CANARY_WORKER=true`、`WORKER_QUEUES=youtube-channel-crawl`、`YOUTUBEJS_EXTRACTOR_MODE=full`。启动时限制只消费该队列，执行并发强制为 1；在业务写入之前验证任务所属路由。不能直接继承普通 Worker 的 `channel` 模式；正式投递前必须断言 `youtubeJsDetailEnabled() === true`，仅确认槽位就绪不够。
- 批次规则选择 `youtubejs_full_v1`，不修改全局默认契约。Repair / `channel-snapshot-recovery` 不能借灰度批次绕过旧契约限制。
- Agent / Finalize / Publication 继续使用原库、原前缀和原服务；新执行器的下游投递不使用灰度前缀。
- BullMQ stalled / retry 保留原队列。系统恢复遇到 YouTubeJS Run 的未完成 Detail 时，重试原 `channel-snapshot` Job；原任务缺失、身份冲突或存在 Data API 状态时记录 `youtubejs_full_recovery_blocked` 并停止该恢复分支，禁止生成旧详情任务。
- 灰度批次不执行旧版自动完整性补抓及旧版自动修复，以免重置已冻结检查点。需要额外修复时由操作员确认，不自动降级。
- Controller 的普通队列暂停/恢复操作不自动开启或关闭灰度队列；停止灰度接单需停灰度 Worker 或显式暂停灰度物理队列。

## 部署前检查

2026-09-07 只读检查：

- 正式抓取 Worker 20 个；已有增量 Worker 20 个。现有抓取 Worker 消费 `youtube-channel-crawl,youtube-content-detail`。
- 正式 `query_scheduler` 为 `stopped`。投递前必须重新检查并遵守现有调度器互斥 admission，不绕过它。
- Rota `channel` 槽位：`desired=40 / provisioned=40 / claimed=40`，无空闲槽位。代理 reserve 为 1234，限制是 Worker 槽位，不是代理不足。
- 槽位数来自 `ROTA_CHANNEL_SLOTS` 启动配置。新增槽位需要改配置并重启共享 Rota；不能直接插入槽位绕过资源管理器。
- 经迁移源只读查询并排除目标库全部既有 Channel / Migration Intent 后，候选为 `UCeZwMe_IU4j-OaSdfbwh_Vg`，源 Candidate 为 `33809`。这不是已投递任务，执行前必须重新检查仍未迁移。
- 现有 Controller 镜像为 `pachongsys-23b93bf-comment-merge`，尚无灰度路由。必须先更新负责 Outbox / 恢复投递的 Controller，不能先投递后补路由。

用户随后允许临时停止一个空闲普通迁移 Worker；不停止增量 Worker、不重启 Rota。该授权下的实际尝试如下。

## 首次部署尝试与阻断

2026-09-07 UTC：

- 镜像：`qy-allpachong/qybullmq:fullcrawl-youtubejs-canary-20260907-1`。版本标记为当前 dirty worktree，未冒用提交版本；镜像 Config SHA 为 `9ca6a51d7bd18ca0325fecd530aa3e48e7ce655f718c552d59e480f4a09c8aaa`。使用镜像内 Node 20 的补充验证为 `36 passed`。
- 单频道只读预检通过：频道 `UCeZwMe_IU4j-OaSdfbwh_Vg`、Candidate `33809`、拟用批次 `fullcrawl-youtubejs-canary-20260907-1`。没有执行 dispatch；再次尝试前必须重新预检。
- 保留预检容器 `qy-fullcrawl-canary-plan-20260907`，退出码为 0；部署脚本为本机临时文件 `/tmp/fullcrawl-canary-deploy-20260907.mjs`，不是可跨机器复用的部署产物。
- 创建 `qy-newcrawler-fresh-controller-fullcrawl-canary-1`，未启动；原 `qy-newcrawler-fresh-controller-1` 始终运行，未切换。
- 原 `qy-newcrawler-fresh-worker-channel-1` 正常停机，退出码 0，无 OOM。Rota 租约确认于 `2026-09-07 05:25:06.769521+00` 释放，`release_reason=worker_shutdown`。
- 释放的 `bullmq-channel-07` 被增量 Worker `qy-newcrawler-fresh-worker-incremental-11`（Worker ID `752630081a03`）接走，其日志确认 `rota_slot_ready`。不是旧 Worker 租约未释放。
- 实际 40 个名额归属为：19 个普通抓取 Worker、20 个增量 Worker、1 个 `worker-content-enrich-1`。此前仅统计普通与增量容器数量，遗漏了内容补全 Worker 也占用 `channel` 槽位；容器运行不等于已取得槽位。
- 新 `qy-newcrawler-fresh-worker-fullcrawl-canary-1` 启动后始终没有 `rota_slot_ready`，因此没有切换 Controller，也没有投递迁移任务。
- 已停止灰度 Worker，并重新启动原 `qy-newcrawler-fresh-worker-channel-1`；原进程恢复不代表槽位已恢复，满额时仍需等待。没有停止第二个普通 Worker，没有调整增量 Worker，也没有重启 Rota。新旧容器均保留，未删除数据或检查点。

当时因只停一个的授权不足而暂停，用户随后明确允许总共暂停两个空闲普通 Worker；执行结果如下。没有扩容共享 Rota。

## 真实迁移结果

2026-09-07 UTC，仅一个频道：

- Channel：`UCeZwMe_IU4j-OaSdfbwh_Vg`，标题 `Maike Medeiros`，Handle `@MaikeMedeiros`。
- Source Candidate：`33809`；目标 Candidate / Migration Intent：`1401`。
- Batch：`fullcrawl-youtubejs-canary-20260907-1`。
- Run：`run:7178956b-92aa-4332-9c67-87b9ba8f4c6a`。
- Job：`channel-snapshot__fullcrawl-youtubejs-canary-20260907-1__UCeZwMe_IU4j-OaSdfbwh_Vg__g1`，抓取前缀 `bull-fullcrawl-youtubejs-v1`。下游 `youtube-finalize` 前缀仍为 `bull`，全局默认仍为 `legacy_full_v2`。
- 频道检查点提交于 `05:36:55.236`，内容清单提交于 `05:36:56.147`，冻结 30 条目标。详情配置错误导致前三次尝试失败，频道和清单检查点保留。
- 停止灰度 Worker，保留其错误配置容器为 `qy-newcrawler-fresh-worker-fullcrawl-canary-1-channel-mode`，以同镜像和其他原配置重建灰度 Worker，仅添加 `YOUTUBEJS_EXTRACTOR_MODE=full`。同一无网络断言由 `false` / 退出码 1 变为 `true` / 退出码 0。
- 使用 `job.retry('failed')` 重试原 Job；成功结果 `attemptsMade=4`、`resumed=true`，执行阶段仅为 `detail,close_fetch,handoff`，耗时约 48 秒。Run ID 未变，没有重新执行频道和清单阶段。
- 抓取于 `05:41:07.095` 完成：30 条详情均为 `done`；1 条 `stored`，29 条 `terminal_excluded`，0 条 deferred。实际 `crawler.contents` 为 1 条，且为近期内容；迁移活跃度判断 `passed`。
- 内容清单哈希保持 `sha256:cbc8a251e52513db18cd605da5576ae5dd784c1620941c9e93ab7c42e01410e4`，目标哈希保持 `sha256:4e61dfed60df730b42feab724289c83e0a37ea9dea86a95dc09b1cb2a729d225`。
- Run 最终 `status=done`、`detail_status=done`、`error_message=null`；`publication_finalized_status=ready_auto`，于 `05:41:27.824` 完成 Finalize。
- 正式发布 `agent/channel/video` 三域均为 `ready`、序列 1；Outbox 全部 `delivered`，各 1 次交付尝试，时间为 `05:41:28.224` 至 `05:41:28.231`。
- 正式业务库 `public.channels` 已存在该频道；三域 `consumer_cursor.active_sequence=1`，于 `05:41:29.053` 激活；业务投影 Outbox 为 `delivered`、attempts=1、无错误，于 `05:41:30.029222` 完成。

本次证明单频道真实迁移抓取、同 Run 配置故障恢复、已提交频道/清单不重抓、下游正式发布可用。不等价于大规模稳定性验证，也没有在生产已提交部分详情后再次强杀来验证细粒度续跑；后者已有隔离集成测试覆盖，不能冒充本次生产证据。

## 当前服务安排

- 正在运行：`qy-newcrawler-fresh-worker-fullcrawl-canary-1`，容器 ID `69bbff11d47c97c63e4d38a225e698217f4299a2f4254339896a241b654d0b28`，槽位 `bullmq-channel-10`，并发 1；`qy-newcrawler-fresh-controller-fullcrawl-canary-1`。
- 正常停机保留：`qy-newcrawler-fresh-worker-channel-1`、`qy-newcrawler-fresh-worker-channel-2`、`qy-newcrawler-fresh-controller-1`。两个普通 Worker 均退出码 0、无 OOM。
- 当前名额安排为 18 个普通抓取、20 个增量、1 个内容补全、1 个灰度，共 40；没有扩大灰度、调整增量服务或重启 Rota。
- 新 Controller 是独立容器，未保留 Compose 所有权标签。后续不得直接对旧 Compose 服务执行启动而造成双 Controller；下一次标准部署前应将当前灰度容器/配置纳入明确交接方案。
- 测试完成不自动撤销正式发布结果。撤除灰度时应先复核没有未完成任务，再停灰度并恢复普通资源；两个原 Worker 都恢复进程时仍可能有一个等待名额，需按全部角色总数安排容量。

## 视频详情字段复核与修复

单频道跑通不等于全部字段完整。后续只读核对发现，正式入库的 `Table saw`（Short，28 秒，发布时间 `2026-07-15T19:21:01Z`）有播放量 1391、点赞数 7，但点赞状态为 `unresolved`。描述由 Player 明确观察为空；评论首屏为空，按 `zero_from_surface` 记录 0，不是评论关闭。29 条排除原因均为 `outside_content_window`。

对照增量分支 `54c3ebf` / `d126ca6`，本分支工作区已补齐：

- 点赞主字段缺失时读取按钮备用值；明确数字包括 0 使用 `exact`，仅明确匹配非公开 Like 按钮时使用 `zero_from_empty` 和独立来源，未观察到则保持 `unresolved`。策略零不是实际点赞数为零的证明。
- 读取解析后 Next 消息中的明确评论关闭证据；空 continuation 响应本身不能证明关闭。
- 公开直播/预约直播在评论请求成功但评论区域缺失时，保留独立的策略零状态与来源；网络失败不制造零值。
- 现有 Full Crawl 入库层已能保留状态、来源和评论首屏，无需改数据库结构。新增真实详情入口到隔离 PostgreSQL 的联合测试，验证明确点赞、明确零、策略零、未知值、评论关闭及非空评论首屏。

按已确认的详情抓取入口和 Full Crawl 入库边界进行 TDD：点赞状态、Next 评论关闭、直播评论状态均先复现失败后修复；相关 101 项回归全部通过，含真实隔离 PostgreSQL，0 skipped / 0 failed。

最终补充验证：宿主 Node 26 全量 `1661 tests / 1507 passed / 154 skipped / 0 failed / 0 cancelled`，仍排除缺少依赖的 `real curl_cffi|persistent yt-dlp` 测试，未配置连接的集成测试在全量命令中跳过。上述定向 101 项使用镜像内 Node 20，已单独实际执行数据库用例，不以跳过代替验证。Node 20 全量容器另有缺少 git 的环境失败和既有超时用例取消；超时取消在未修改的旧镜像也可复现，宿主环境该用例通过，未为此改动生产超时逻辑。`git diff --check` 与语法检查通过，隔离测试 PostgreSQL 已恢复停止状态。

这些是工作区修复，尚未构建或部署新镜像，也未修复已发布历史数据、追加迁移频道或扩大灰度。当前生产镜像仍是上述 `fullcrawl-youtubejs-canary-20260907-1`。上线后仍需选取更多真实内容样本验证；本次没有扩大“最近 30 条 / 90 天”范围，没有抓取全部历史视频或全部评论，也没有将可选字段未知一律定义为请求成功或完整。

## 验证和上线顺序

1. 在隔离 PostgreSQL / Redis 中验证 Query 普通路由和 Migration 灰度路由的跨进程崩溃、重复投递；确认原物理抓取队列看不到灰度 Job，下游原队列能收到 Finalize。
2. 完成普通任务回归和 Controller 的 YouTubeJS 原任务恢复 / 缺失时阻断测试。
3. 获得 Rota 资源变更授权并安排可用槽位，构建包含当前工作区变更的独立镜像，记录可审计版本，不冒用已提交版本标签。
4. 保持环境和其他服务不变，更新路由相关服务，启动且仅启动一个灰度 Worker。确认其前缀、角色、并发及 Rota 槽位正确。
5. 使用单频道迁移投递接口及专用新批次，先重新核验候选，再投递一个频道；不调用默认最小 100 个频道的批量迁移 CLI，不扩大源选择规模。
6. 观察 Channel / Uploads / Detail、Agent、Finalize 和正式 Publication，记录 Run、Job、来源版本、终态及实际字段完整性。出现错误先停止追加任务，不扩大灰度。

## 停止与回退

先停止灰度新投递，再暂停灰度 Worker；不删除 Run、检查点或队列任务。有未完成 YouTubeJS Run 时，保留识别其路由的 Controller 和恢复代码，不直接回滚为旧 Controller，更不能把灰度队列任务搬到普通抓取队列。全部灰度 Run 已终态且没有恢复/待投递任务后，才评估撤销灰度服务和专用槽位。正式业务结果的撤回不包含在停止 Worker 操作中。
