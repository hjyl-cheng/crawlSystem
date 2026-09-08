# 空上传列表的国家复核与休眠

## 行为

TIMES BRASIL 的上传列表在一个出口正常返回空，在 Rota 的 BR 备用节点返回了 100 条。
因此，频道有视频数量/内容标签但上传列表为空时，不再抛出普通错误反复重试。

- 只有正常终止、没有解析缺口的空列表才能触发此策略。实际 HTTP 响应还须包含明确的空播放列表提示，且不能存在被解析漏掉的视频 ID。
- About 没有国家：休眠。
- 当前出口与 About 国家一致：本次已经完成国家复核，仍空则休眠。
- 有国家且出口不同或未知：请求 Rota 对该国家复核一次。国家来自 About，不来自请求的 `gl`/语言，也不代表已证明地区封锁。
- 有可用同国家备用节点：在当前请求全部结束后换路；列表恢复则继续在该路由采集详情。
- 无同国家备用节点：保持健康路由，返回 `NO_COUNTRY_RESERVE`；频道休眠，不再对原出口重复请求列表，worker 不等待备用节点。
- 超时、风控、分页异常及未确认的空响应继续走错误处理，不作为正常空列表。

Full Crawl 与增量使用同一个国家决策模块。国家复核意图和结果保存在 job 的 `uploads_country_recheck`，防止恢复后无限换国家；不修改业务代次或候选任务栅栏。

频道休眠原因为 `uploads_empty`，与“90 天没有发布”分开。`source_json.uploads_recheck` 保存候选国家及休眠原因。下一次休眠 Video probe 使用该国家复核；无节点继续休眠，休眠 probe 不打开 YouTube 频道会话。成功读取非空列表后清除这条空列表标记，后续活跃/休眠沿用已有活动策略。

增量遇到这种休眠结果时，不派发存量视频详情，不推进或清空已有视频锚点及 source cursor，不改写存量视频。Full Crawl 的空列表提交也不重置已有内容的 `is_recent`。

## Rota 边界

扩展现有任务完成接口，增加可选 `recheck_country`，不新增绕过 Rota 的直连代理流程。
请求仍校验 lease、task、route generation、quiesced 和幂等键。只有 channel role 且业务尚未完成可请求国家复核。
从健康、未分配的备用节点中严格匹配国家，沿用原有凭据轮换和路由激活流程。国家复核不创建代理故障 Observation，不隔离原健康代理。
省略该字段时，旧请求和原有幂等哈希保持兼容。

## 上线顺序（初次验证阶段未部署；后续授权上线见文末）

1. 在 crawler 数据库执行 `services/qybullmq/sql/uploadsEmptyDormancy.sql`。
2. 在 feature-clock 数据库执行 `services/feature-engine/sql/uploads_empty_dormancy.sql`。
3. 更新 feature-engine 消费者/相关服务，使事件契约接受 `uploads_empty`；更新 Rota，使完成接口支持指定国家复核。
4. 更新 Full Crawl 和增量 worker。

SQL 只扩展休眠原因约束，不批量改写频道。不要先部署 worker：旧消费者不能识别新的休眠原因，旧 Rota 不支持严格国家复核。
已有 `uploads_empty` 数据/事件后，回退时应保留扩展的约束和消费者兼容性。

## 验证

- Node 相关回归：219 项通过，包括正常非空分页、取消/超时、国家复核、无备用节点、任务恢复与执行边界。
- 隔离 PostgreSQL：8 项通过，覆盖 Full Crawl 正常/空列表完成、增量正常/补采/空列表及原有视频和锚点保留。
- Rota `go test ./internal/proxycontrol`：在专用 PostgreSQL 上通过；新增精确国家选择、无匹配节点保留原路由、原代理保持 active、未 quiesce 拒绝换路及幂等重放测试。
- feature-engine：69 项事件契约与休眠计划相关测试通过。
- 未改动批量派发、并发数量、视频详情客户端切换及 API fallback。

## 真实频道补测（2026-09-08 10:38–10:44 UTC）

对象：`UCY-FPXguTtLHSTK5YpQ-sgQ`，TIMES BRASIL。
使用当前工作区代码的 `acquireYoutubeJs`、`openYoutubeJsChannel`、Full/增量列表函数和 `executeManagedWorkerAttempt`；未部署、未连接生产采集队列、未写生产数据库或修改 Rota 槽位。

- 美国备用节点 80666：Cloudflare trace 返回 US（不记录 IP/凭据）。使用与生产一致的 `YOUTUBE_COUNTRY=BR` 请求参数，Full 和增量均读取到 About 国家 Brazil、24,365 videos，真实空列表均触发 `country_recheck: BR`，job 回执变成 requested。不是预先绑定巴西出口。更早的美国节点 81866 连接失败，未计作通过。
- 无备用分支：在隔离 job 注入 unavailable 回执后，Full 返回 `no_country_reserve` 且新增 HTTP 请求为 0；增量 dormant preflight 返回同一原因。**回执为测试夹具，不是生产 Rota 返回。**
- BR 标记节点 82107（未领取的 discover 槽位）：真实列表仍空，Full 返回 `country_checked` 休眠；其实际出口国家未得到独立验证，不能将 BR 库存标签当作 YouTube 可访问保证。
- BR 标记节点 83171（测试时未领取的 discover 槽位）：Full 得到 30 条，增量扫描得到 130 条（不同扫描上限，不代表数据不一致）。抽测详情 `ltpLAh13KLA` 连接失败。之后只读查询发现该节点已被 Rota 健康检查置为 failed、从槽位移除，停止使用。该节点独立出口国家查询超时。
- 额外尝试用真实 `RotaSlotAdapter` 串起一次美国起步与 BR 重执行：控制回执/代理绑定使用隔离夹具；过程中目标节点已不满足健康/空闲条件，测试未完成，**不能报告自动换路端到端通过**。

结论：真实非巴西起步的空列表识别与国家复核请求已验证；真实 BR 标记出口取得非空列表已验证。当前仍缺少“新 Rota 正式控制接口 + worker 自动换路 + 视频详情完成”的完整验收。生产旧 Rota 不支持本次新字段，且查询时没有可用未分配 BR 备用；未为测试占用正式 worker 的节点，也未部署更新来绕过此限制。

本次只读补测日志（本机临时文件）：`/tmp/uploads-country-live-us.log`、`/tmp/uploads-country-live-br.log`。不含代理地址和认证信息。


## 后续授权上线

用户明确要求所有已提交改动按最新分支上线后，代码版本 `aa939bf` 已部署：Rota、feature-ingest、daily scheduler、API、controller、dashboard、20 个 Full Crawl worker、20 个增量 worker。先应用本文两份约束升级，再更新事件消费者/Rota，最后更新 workers。保留原容器配置和回滚备份。

最终检查：全部 40 个 worker 均为该代码镜像且 Rota 租约有效；相关服务运行、无重启异常；API/Rota/feature-ingest 健康。没有新建生产迁移批次。空列表国家复核的完整真实详情验收限制仍如上述补测记录，部署完成不等于该验收已通过。
