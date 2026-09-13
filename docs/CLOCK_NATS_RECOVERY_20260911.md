# 2026-09-11 Clock 恢复与正式 NATS 流程核验

截至 2026-09-11 12:10 UTC，初始 18 个内部恢复失败的 Plan 已全部结束：9 个 succeeded、9 个 partial；18 个原 BullMQ Job 均 completed。另修复 Emanuely Liberato 已完成 Video 检查点但 Run 未收尾的问题，原 Job 已完成；重放 Animal Planet Brasil 已保存的 Video 完成事件后，频道按空上传列表进入休眠，Plan cancelled、Run done。

## 精确恢复范围

- 最初今日 21 个 failed Plan 中，18 个保存着 running Run、失败 remote task、已结束的旧执行及已完成 API 请求；3 个是明确不存在/终止的频道，保持 removed，不伪造采集成功。
- 先保存 Plan、Run、原 Job、413 个检查点、85 份 API 请求和执行记录的私有快照；逐项验证 frozen Plan hash、无更新执行、无存活网络绑定。恢复原 Plan/Job，不新建替代任务，不改采集策略或代次栅栏。
- 清除已失效的原 Job API 续跑标记后，最初一次恢复被 Rota 的已耗尽预算拒绝。18 个 Run 原有效尝试计数均为 9、上限 9。仅为这些已备份的 Run 各补 3 次额度，上限改为 12；全局上限、尝试序号和历史任务记录保留。Rota 仍拥有重试、代理切换及 API 交接。
- Emanuely 从真实 finalized Batch 和 complete Observation 修复 Video 域并完成 Run；原 Job 重新领取后由原 preparer 的 terminal 分支收尾，不发起采集。其历史 failed transport 记录保留。
- 用户接单配置保持中心 0、节点 01 为 20；迁移 Worker 未重建。

## 恢复中发现并修复的代码问题

原 `recordIncrementalTerminalFailure` 对同一 Run/Domain 固定使用一个失败幂等键。人工恢复后的下一次失败，其 attempt_count 和 observed_at 已变化，却仍使用旧键，触发 `CrawlObservationIdempotencyConflict`，覆盖原始 Rota 预算错误。

现在失败幂等键包含尝试次数，同一尝试重放使用已提交时间；不同尝试各自留存事件。未放宽通用 Observation 写入器的 hash 校验，已完成域仍不会被写为失败。

独立 PostgreSQL 回归测试先复现失败，再验证：两次不同尝试各生成一条 Observation；同一次重复提交不重复写；域完成后不追加失败。测试文件：`services/qybullmq/test/incrementalTerminalFailure.postgres.integration.test.js`。

已部署中心镜像 `qy-allpachong/remote-node-center:clock-recovery-20260911`，基于已运行的 NATS 镜像，仅覆盖失败记录模块。其余 Worker 保持原镜像；共享源码中的修复将在后续构建中包含。本次不提交或推送整个已有工作区。

部署须同时加载 `deploy/compose.remote-node-center.yml` 和 `deploy/compose.remote-node-services.yml`；第二个文件提供 Rota 网络和内部 CA。此次第一次重建遗漏第二个文件，Worker 无法准入、验证任务停在等待；已补齐，未在缺失配置下执行采集。

## 最终数据与传输证据

核对范围为上述 18 个恢复频道，加 Emanuely 的原检查点，共 19 个频道、413 个采集目标：

- 404 个 captured、9 个 settled_error；无 pending/claimed。
- 359 条主表视频的标题、发布日期、播放量、点赞量、评论数均非空。
- 33 条保留 deferred，原因为 authoritative_type_unresolved；21 条按直播中/尚未开播排除。所有 54 条未进主表的目标都有明确处置，无去向不明的目标。9 条 settled_error 位于直播内容的排除记录中；不能据此说所有视频详情都成功。
- 原先 228 条 captured 的 detail_json 和 captured_at 全部保持一致。
- 原 85 份 API 请求仍为 done；最终这批 Run 共有 151 份 done 请求，无 pending（增加 66 份，不能解释为 66 次 HTTP 请求）。
- 19 个 finalized Observation 均已被 Feature inbox applied；19 个原 Job 均 completed。
- 18 个远程任务收到 212 条持久化传输回执。采集期间观测到等待 API 的 Job 进入 delayed，释放 Worker；随后继续处理结果和后续视频。
- NATS 最终 pending/unacknowledged 为 0；RPC、心跳、等待通道均无 rejected/timedOut。20/20 Worker 在线且可接单；队列 active/waiting/delayed 均为 0，未暂停。

“partial”涉及类型不能可靠确认的新视频，保持后续复查，不能视作全部数据已完整采集。

## 仍然存在的失败

结束核对时，今日 failed Plan 为 4 个：

- AlineCPBorghi、CANAL DO RONNY：频道不存在，已 removed。
- DavidCdsOriginal Channel：版权投诉导致账号终止，已 removed。
- Carol Janiro：恢复成功的是原 Video Plan `72fef71f-5814-58db-b7fc-bc7db3874587`。调度器于 12:03:59 UTC 新生成 About Plan `ee574094-8440-5144-beb5-f39d7bdf6fd4`，随后 9 次执行均在 YouTube browse 的 WEB 请求上报 `FingerprintGatewayError: SSLError curl_code=35`，耗尽原有网络预算。该新 Plan 仍失败，未无限补额度重试；TLS 根因尚未完全定位，不能称整个 About 流程已验证成功。

BullMQ 总 failed 为 5，包含以上今日 4 个及 1 个更早的失败任务。原恢复清单中的 Job 已全部完成。

完整前后数据、Rota 预算快照保存在受限且被 git 忽略的 `runtime/remote-center-production/clock-nats-recovery/`。操作脚本是本次一次性工具，带精确数量/状态校验，不应重复运行。
