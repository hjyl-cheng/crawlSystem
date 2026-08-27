# Migration / Query 链路修复最终方案

## 1. 文档状态

- 状态：**FINAL / 本地代码修复与验证完成，生产上线门禁未完成**
- 定稿日期：2026-08-26
- 实现仓库：本仓库（临时 Worktree 不构成第二套维护源码）
- 实现基线：`main @ 4ada615`
- 本文记录位置：`docs/MIGRATION_QUERY_REPAIR_FINAL_PLAN_20260826.md`
- 当前记录分支：`agent/query-migration`

本文记录已经完成评审的修复方案及其实现结果。`agent/query-migration` 已 fast-forward 到 `main @ 4ada615` 后实施，不再基于旧的 `74574b3` 代码判断或修改。

## 2. 最终结论

本轮处理四类问题：

1. 年龄限制视频被泛化的 `needs_auth` 错误覆盖，最终落成 `login_required`；
2. 发布时间证据在 Uploads、详情合并、持久化和 Observation 传输中被拆散，导致来源、状态和精度失配；
3. 90 天窗口存在多份实现，Migration、Content Window 和 Publication 对边界及精度的处置不一致；
4. 历史数据需要区分证据纠错与策略重分类，不能整体重写 `exact/date_only`。

执行顺序已经批准：

1. 修年龄限制；
2. 修发布时间证据四元组；
3. 接入共享五态时间证据内核；
4. 分批修复历史数据；
5. 国家门禁本轮不动。

依赖关系：第 3 步依赖第 2 步；第 1 步与第 2、3 步正交，可以并行实施。

## 3. 明确不做的事情

- 不修改国家门禁或 Agent 国家推断模型；
- 不把合法的 `exact/date_only` 整体降级；
- 不原地改变 `detailAgeDays()` 的既有日历日返回语义；
- 不改变 Publication 当前 `video-window-v1` 对 date-only 截止日的处置；
- 不保留 Migration Gate 的第二份 SQL 时间比较实现；
- 不新增与现有 `relative / estimated / unavailable / unresolved` 重复的状态枚举；
- 不直接修改生产数据，不绕过正常 Crawler / Publication 链路写业务结果。

## 4. 发布时间证据模型

### 4.1 证据四元组

发布时间证据必须作为一个不可拆分的整体传递和选择：

```text
published_at
published_at_status
published_at_precision
published_at_source
```

字段语义：

- `published_at`：可解析的时间值；`date_only` 当前仍以 UTC 零点时间戳承载；
- `published_at_status`：证据如何获得及其可信状态；
- `published_at_precision`：时间分辨率，取值 `second | date_only | unknown`；
- `published_at_source`：产生该证据的具体 Extractor 来源。

`status` 与 `precision` 正交。`exact/date_only` 是合法且重要的组合，表示“确切知道 UTC 日历日期，但不知道具体时分秒”。它不是不确定日期。

### 4.2 Uploads Adapter 映射

Uploads 来源必须按下表构造证据：

| 来源 | status | precision | 含义 |
|---|---|---|---|
| `yt_dlp_flat_timestamp` | `exact` | `second` | yt-dlp 提供原始秒级 timestamp |
| `yt_dlp_flat_upload_date` | `exact` | `date_only` | yt-dlp 提供确切 upload date |
| `youtube_uploads_relative_time` | `relative` | `date_only` | YouTubeJS 相对时间推算出的日期 |
| 缺失或无效 | `unresolved` | `unknown` | 不能作为时间窗口事实 |

`estimated` 保留给其他明确的估算来源，不与 `relative` 混用。

该表只定义 Uploads Adapter。详情侧的 `yt_dlp_timestamp`、`yt_dlp_upload_date`、`youtubejs_player_microformat`、`youtubei_player_microformat`、`youtube_data_api_snippet` 等来源，必须由各自 Adapter 按实际解析结果产出完整四元组。共享模块负责验证四元组，不能把所有状态重新简化成一份 source 字符串白名单。

### 4.3 合法性不变量

- `exact/second` 必须有有效时间和非空来源；
- `exact/date_only` 必须有有效 UTC 日期承载值和非空来源；
- `relative` 或 `estimated` 不得被窗口内核当成 exact 事实；
- Uploads 未提供、非法或无法证明来源的值归 `unresolved/unknown`；Adapter 明确证明字段不可用时可以保留 `unavailable/unknown`，两者在窗口内核中均映射为 `unresolved`；
- 四元组更新时必须四个字段一起替换，禁止分别 `COALESCE` 后形成混合来源；
- 历史上的通用来源 `youtube_uploads` 不能直接推断为 yt-dlp 或 YouTubeJS。

### 4.4 证据选择规则

证据选择必须同时考虑 `(status, precision)`，不能只比较 precision。

必须满足的最小顺序：

```text
exact/second > exact/date_only > non-exact evidence > unresolved
```

其中：

- `exact/date_only` 必须覆盖已有的 `relative/date_only`；
- `relative/date_only` 不得覆盖已有的 `exact/date_only`；
- `exact/second` 必须覆盖 `exact/date_only`；
- `relative` 与 `estimated` 的关系必须由显式表定义，不能依赖字符串或枚举顺序；
- 同 status、同 precision、同值时必须幂等；
- 同 status、同 precision但值冲突时，必须使用确定的来源优先级或保留既有值并产生冲突 reason/metric，禁止由函数调用顺序静默决定。

共享模块应隐藏具体 rank 表。调用方只消费“选择后的完整证据 + decision/reason_code”，不能各自重写比较规则。

## 5. 共享时间证据内核

### 5.1 模块接口

建议在一个纯计算模块中提供小接口：

```text
normalizePublicationEvidence(input) -> evidence
selectPublicationEvidence(current, candidate) -> selection
classifyPublicationWindow(evidence, { asOf, maxAgeDays }) -> classification
```

模块不执行数据库或网络 I/O。调用方和测试通过同一接口验证行为，rank、UTC 比较和边界处理留在模块实现内部。

`classification` 至少返回：

```text
relation
reason_code
basis
precision
source
classifier_version
```

### 5.2 五态关系

`relation` 只能是：

```text
inside
outside
after_as_of
cutoff_overlap
unresolved
```

只有 `published_at_status = exact` 可以产生前四态。`relative / estimated / unavailable / unresolved` 一律产生 `unresolved`。

判定规则：

- `exact/second` 使用精确时间戳与 `asOf - maxAgeDays * 24h` 比较；
- 秒级时间 `<=` 精确截止时刻时为 `outside`；
- `exact/date_only` 使用 UTC civil date；
- date-only 日期位于精确截止时刻所在 UTC 日期时为 `cutoff_overlap`；
- 明确晚于 `asOf` 的 exact 证据为 `after_as_of`；
- 无效时间、非法组合或非 exact 状态为 `unresolved`。

### 5.3 消费者处置矩阵

共享内核只返回证据关系，不直接替消费者决定业务动作。

| relation | Video Publication | `contentWindow` | Migration Activity Policy / Gate |
|---|---|---|---|
| `inside` | 纳入窗口候选 | 不排除 | 计为近期内容 |
| `outside` | 排除 | 判定超窗 | 计为旧内容，不增加 uncertain |
| `after_as_of` | 排除为未来证据 | 不按“过旧”排除 | 计为 uncertain，除非已有 upcoming 专用结论 |
| `cutoff_overlap` | 映射为 outside，保持 `video-window-v1` | 不提前排除，继续详情 | 计为 uncertain，不能提前判 dormant |
| `unresolved` | 不作为可信窗口事实，并影响 readiness | 不提前排除 | 计为 uncertain，不能提前判 dormant |

这样可以让 Publication 的既有契约与 Migration 的保守抓取策略共存，不在调用点重新实现边界规则。

## 6. 批次一：年龄限制修复

修改 `detailFromYtDlpResult()` 中 yt-dlp availability 与结构化 playability 的合并规则。

当前错误路径：

```text
yt-dlp anonymous availability = needs_auth
playability reason = age_restricted
generic needs_auth 覆盖明确 age_restricted
access_status = login_required
availability = needs_auth
```

目标规则：

- 明确识别出的 `age_restricted` playability 是内容限制事实，优先级高于泛化 `needs_auth`；
- 输出 `access_status = public`；
- 保留 `availability = age_restricted` 及 playability reason code；
- 视频不因年龄限制而被丢弃，在其他存储条件满足时正常写入；
- `unlisted`、`private`、`members_only` 等更具体的内容事实保持现有优先级；
- 不把所有 `LOGIN_REQUIRED` 都改成 public，只有明确年龄限制证据命中该规则。

回归测试：

1. 保留现有 Unlisted 测试；
2. 新增 `needs_auth + age-restricted reason`，断言 public + age_restricted；
3. 新增普通 `needs_auth`，仍断言 login_required；
4. 明确 private/members-only 不受影响。

真实回归样本：`oy7l_ybAng8`。

## 7. 批次二：修复四元组链路

### 7.1 五处硬编码状态赋值点

以下位置不得再以“`published_at` 非空即 exact”推导状态：

1. `incrementalVideo.js` 内容 upsert 的 `EXCLUDED.published_at` 状态赋值；
2. `incrementalVideo.js` 新内容 insert 参数中的状态赋值；
3. `incrementalVideo.js` discovery scan 更新已有 Content 的状态赋值；
4. `incrementalVideo.js`详情更新 Content 的状态赋值；
5. `pipelineV2.js` 的 `normalizeResolvedDetail()` 状态赋值。

这些位置必须接收或保存 Adapter 已经产生并通过共享模块验证的完整证据。

### 7.2 三处合并点

以下位置必须调用同一证据选择规则：

1. `incrementalVideo.js::mergeIncrementalVideoDetail()`；
2. `pipelineV2.js::mergeDetail()`；
3. `fullVideoContentStore.js` 的 Content upsert 合并语句。

数据库 upsert 必须原子选择整组字段。不能出现新时间配旧 source、旧 status 配新 precision，或 `relative/date_only` 阻止 `exact/date_only` 升级。

### 7.3 来源及传输丢失点

至少修复：

- `incrementalUploadEntry()` 不再把所有输入先切成日期并丢弃 source/precision；
- `uploadsPublishedFacts()` 不再把所有来源硬编码为 `youtube_uploads/date_only`；
- `detailFacts()` 返回完整四元组；
- discovery `scanInput` / `jsonb_to_recordset` 传递完整四元组；
- Observation `commandEntries` 传递完整四元组，不能只保存 `published_at`；
- 后续持久化 Adapter 原子写入四元组。

30 天 Incremental Sampling 是独立策略，不自动改成 90 天 Migration 规则；但它读取发布时间时不得破坏四元组或把 non-exact 重新标成 exact。

## 8. 批次三：接入四处消费者

### 8.1 Video Publication

从 `videoPublicationCurrent.js` 提取现有比较内核并参数化，保持 Publication 现有 `video-window-v1` 输出行为，特别是 date-only 截止日仍映射为 outside。

### 8.2 Content Window

`contentWindow.js` 不再独立使用日历日 `> maxAgeDays` 判断。所有调用点消费共享分类结果；只有明确 `outside` 才按过旧排除。

`detailAgeDays()` 保留为证据或展示值，不原地改变返回类型和历史落库量纲。

### 8.3 Migration Activity Policy

`migrationActivityPolicy.js` 不再调用 `localizedPublishedUtcDay()` 抹掉已经存在的秒级时间。它消费原始四元组：

- yt-dlp 秒级证据可以直接精确判断；
- yt-dlp upload date 可以形成 exact/date-only 边界关系；
- YouTubeJS 相对日期产生 unresolved，必须继续抓详情；
- `cutoff_overlap` 不得提前判 dormant。

### 8.4 Migration Activity Gate

Gate 在 `activityEvidence.complete !== true` 时，按 Run 查询全部候选时间证据后在 JavaScript 中调用共享内核。

要求：

- 不写死 `LIMIT 30`；Run 的 `channel_content_limit` 可配置到 100；
- 不保留 SQL `BETWEEN` 时间比较镜像；
- SQL 只负责读取候选行及四元组，不负责复刻分类算法；
- 聚合结果记录 `inside/outside/after_as_of/cutoff_overlap/unresolved` 计数；
- `cutoff_overlap_count` 和 `unresolved_by_status_count` 可查询；
- `inside_count > 0` 时继续 active；
- `inside_count = 0` 且 `uncertain_count > 0` 时必须返回 inconclusive，继续详情或证据修复；
- 只有证据完整且 `inside_count = 0`、`uncertain_count = 0` 时才可 dormant；这同时覆盖“全部可信证据均 outside”和“完整列表没有符合类型的已发布内容”。

## 9. 版本与审计字段

三类版本/原因必须分开：

- `classifier_version`：共享时间证据内核版本；
- `policy_version`：各消费者自己的动作策略版本；
- `repair_kind`：本次数据写入原因。

`repair_kind` 取值：

```text
evidence_correction
policy_reclassification
```

Publication 继续拥有自己的 `video-window-v1`。Migration 使用独立 policy version，不能与 Publication 版本绑定。

这些字段必须落在可查询的 Run/Gate 结果和修复清单或审计记录中，不能只写进进程日志。

## 10. 上线前影响量化

不能只按 `crawler.contents(published_at_source, published_at_precision)` 分组估算影响面，原因是：

1. 来源已被部分路径拍平成 `youtube_uploads`；
2. 被提前判 dormant 的 Run 在候选入库前返回，本 Run 不产生 Content 行；
3. 因此 Content 样本同时存在来源误分和选择偏差。

上线前按以下方式量化：

1. 按 Run 统计 `crawler.raw_objects` 中 `youtube_channel_uploads_flat_json` 覆盖率；
2. 区分“有 metadata 行”和“S3 object 实际存在且可读”；
3. 对可读 raw object 在只读副本执行新分类器 dry-run；
4. 缺 raw 的 Run 单独计数，不从已拍平 Content source 反推；
5. 增量侧使用 `crawl_observations.extractor_versions` 估算实时路径占比，但不声称可恢复历史时间；
6. 灰度按实时 Extractor 路径或可验证 lineage，不按通用 `youtube_uploads` source；
7. 若 YouTubeJS unresolved 占比过高，先灰度 yt-dlp 路径，再分批扩大 YouTubeJS 路径。

只读审计应使用单连接或低并发，避免再次触发只读角色连接上限。

## 11. 历史数据三分类

历史修复必须按证据可恢复性分组：

| 分类 | 识别条件 | 修复动作 |
|---|---|---|
| A. 原始 timestamp 可用 | 全量迁移 Run 有可读 raw object，且包含 yt-dlp timestamp | 离线恢复 `exact/second`，不重抓 |
| B. 明确为 YouTubeJS 相对时间 | raw/lineage 能证明 `youtube_uploads_relative_time` | 改为 `relative/date_only`；窗口需要时抓详情 |
| C. yt-dlp 或来源未知且 raw 已丢 | 当前仅剩通用 `youtube_uploads/date_only`，无法恢复原始证据 | 不猜测，安排重抓 |

禁止执行：

- `UPDATE ... SET published_at_status='relative' WHERE precision='date_only'`；
- 按通用 `youtube_uploads` source 整体降级；
- 用新策略结果覆盖历史行却不记录版本和 repair kind；
- 直接修改 Business Publication 结果表。

## 12. 数据修复执行安全

数据修复脚本必须：

1. 默认 dry-run；
2. 先在只读副本输出总数、按 Run/来源/类别分组及样本 ID；
3. 对比 ITDP、Boxe 和年龄限制真实样本；
4. 生成稳定输入清单或哈希，Apply 时重新验证；
5. 按小批次执行，可幂等重放；
6. 每批记录 `classifier_version / policy_version / repair_kind`；
7. 通过正常 Crawler / Publication 路径传播，不手改下游业务结果；
8. 每批完成后复核行数、不变量、失败数和 Publication readiness；
9. 任一批次出现证据冲突或数量超出 dry-run 预期即停止。

## 13. 契约测试矩阵

### 13.1 年龄限制

- 泛化 `needs_auth` + 明确 age-restricted reason -> public + age_restricted；
- 普通 `needs_auth` -> login_required；
- Unlisted 保持 unlisted；
- Private / members-only 保持原结论。

### 13.2 Adapter 映射

- yt-dlp flat timestamp -> exact/second；
- yt-dlp flat upload date -> exact/date_only；
- YouTubeJS relative time -> relative/date_only；
- 缺失或非法值 -> unresolved/unknown；
- 详情 Adapter 的合法 exact/date-only 不被误降级。

### 13.3 四元组合并

- exact/date-only 双向压过 relative/date-only；
- relative/date-only 双向不得覆盖 exact/date-only；
- exact/second 压过 exact/date-only；
- status、precision、source、value 永不撕裂；
- 同等级冲突行为确定且有 reason/metric；
- SQL upsert 与纯函数选择结果一致。

### 13.4 五态内核

- 秒级 cutoff 精确边界；
- date-only cutoff -> cutoff_overlap；
- exact future -> after_as_of；
- relative / estimated / unavailable / unresolved -> unresolved；
- 非法时间和非法组合 -> unresolved；
- `maxAgeDays` 参数化，不硬编码 90。

真实边界样本：

| 样本 | 发布时间 | as-of | 预期关系 |
|---|---|---|---|
| ITDP `ooPL-Tk-6qI` | `2026-05-28T17:20:53Z` | `2026-08-26T05:36:44Z` | inside |
| Boxe `hEBA7Jg5oVQ` | `2026-05-28T01:33:12Z` | `2026-08-26T05:36:44Z` | outside |
| date-only cutoff day | `2026-05-28` | `2026-08-26T05:36:44Z` | cutoff_overlap |

### 13.5 消费者

- Publication 将 cutoff_overlap 映射为 outside；
- Content Window 对 cutoff_overlap/unresolved 不提前排除；
- Migration 对 cutoff_overlap/unresolved/after_as_of 增加 uncertain；
- Gate 查询完整 Run，不出现 `LIMIT 30`；
- Gate SQL 不包含独立日期窗口比较；
- 全部可信 outside 且 uncertain=0 才能 dormant；
- 任一 inside 证据必须阻止 dormant。

## 14. 可观测性与灰度验收

上线后至少观察：

- `relation_count`：五态分别计数；
- `unresolved_by_status_count`；
- `cutoff_overlap_count`；
- `details_requested_due_to_unresolved_count`；
- `dormant_decision_count`；
- `dormant_reversed_after_detail_count`；
- 按 Extractor 的详情请求增量、错误率和耗时；
- age-restricted public 存储数及 login-required 变化；
- Publication readiness 变化。

灰度期间如果额外详情请求量显著超出 dry-run 估算，应暂停扩大灰度，而不是恢复旧的错误休眠判断。

## 15. Definition Of Done

满足以下全部条件才算完成：

1. 年龄限制回归测试通过，真实样本不再落为 login_required；
2. Adapter、合并点、传输点和持久化点全程保留完整四元组；
3. 不再存在“时间非空即 exact”的五处硬编码规则；
4. 三处合并点不再只按 precision 选择；
5. 四处窗口消费者全部通过共享内核接口；
6. Migration Gate 不再包含 SQL 时间分类镜像；
7. ITDP 判 inside，Boxe 判 outside，date-only 截止日判 cutoff_overlap；
8. Publication 的 `video-window-v1` 既有测试保持通过；
9. Unlisted、Private、members-only 回归无变化；
10. 上线前 raw 覆盖率和 unresolved 增量已经 dry-run 量化；
11. 历史数据按 A/B/C 三类分批处理，没有整体降级 exact/date-only；
12. 每次重放均记录 classifier version、policy version 和 repair kind；
13. 国家门禁代码和数据模型本轮没有变化；
14. 生产 Apply 前在只读副本完成行数与样本比对。

## 16. 最终批准

本方案已完成设计评审，可以进入实现。实现中出现新的证据来源、同等级冲突形态或 dry-run 数量明显超出预期时，应停止对应批次并补充证据；除此之外不再重新讨论已经定稿的窗口语义和执行顺序。

## 17. 实现记录

2026-08-26 已完成代码与契约测试部分：

1. `detailFromYtDlpResult()` 保留明确年龄限制证据，不再让泛化 `needs_auth` 覆盖 `age_restricted`；Unlisted、Private、members-only 与普通 login-required 回归保持原结论。
2. 新增 `publicationTimeEvidence.js`，统一四元组规范化、`(status, precision)` 联合选择、PostgreSQL 原子选择条件和五态窗口分类。
3. yt-dlp flat timestamp/upload-date、YouTubeJS relative Uploads、yt-dlp/YouTubeJS/Data API 详情均在 Adapter 处产生完整四元组。
4. Incremental 的 discovery、candidate、Observation、detail refresh 与 Content upsert 已整组传输和选择四元组；通用历史来源 `youtube_uploads` 不做来源猜测。
5. Migration Full、Content Window、Video Publication 和 Pipeline 的四个实际窗口判断点已接入共享内核。
6. Migration Gate 已删除 SQL `BETWEEN` 窗口镜像，按 Run 读取全部候选证据后在 JavaScript 分类，并记录 `reference_at / classifier_version / policy_version / relation_counts / unresolved_by_status_counts`。
7. Publication 继续把 `cutoff_overlap` 映射为 outside，原有 `video-window-v1` 行为未改变。
8. `publicationTimeEvidence.js` 已阻止 `null`、`undefined` 和空字符串进入 `new Date(...)`；空发布时间不再变成 `1970-01-01`，统一归为 `unresolved`。
9. Incremental Video 生命周期已删除旧 SQL 日粒度分类，改为读取完整四元组并消费共享五态内核；relative、estimated、future、cutoff overlap、unresolved 与未结束 Live 均不能触发 dormant。
10. Migration Gate 会把 `scope=excluded / reason=live_in_progress` 保留为不确定活动证据；详情批处理只排除当前明确 outside 的候选，不再把首个旧视频之后的候选批量标旧。
11. 旧数据兼容推断已集中到 `publicationEvidenceFromFields()`；Pipeline、Incremental、Full Video Store 与 Content Window 不再各自猜测缺失 status/precision/source。
12. 已形成两个可查询的 Run/Gate 指标：
    - `result_json#>>'{migration_activity_metrics,details_requested_due_to_unresolved_count}'` 按候选去重，记录初始 flat 时间为 unresolved 且实际进入详情处理的数量；
    - `result_json#>>'{migration_activity_metrics,dormant_reversed_after_detail_count}'` 每个 Run 只取 `0/1`，仅在完整初始证据没有 inside、存在 unresolved、确实请求详情且最终 Gate 为 passed 时记 `1`；同值也保存在 `migration_activity_gate` 中。
13. 国家门禁未修改；未修改生产数据库、队列、运行配置或 Business Publication 数据。
14. Incremental 会把本轮 Uploads 和本轮 disposition 中的 `live_in_progress` 作为临时活动证据直接传给生命周期分类器；视频仍不写入 `crawler.contents`。即使已有 Live Candidate 处于 24 小时节流期、当前轮不重新抓详情，当前 Uploads 的直播信号仍会阻止频道休眠。既有 24 小时 Candidate 复查策略本轮未修改。
15. Migration Gate 不再把扫描完整性硬编码为 `true`。它保留 `migration_activity_initial_evidence.evidence_complete`：不完整扫描即使详情均为旧视频也只能得到 `inconclusive`；若详情已确认至少一个 inside，则仍可得到 `passed`。
16. 同质量发布时间四元组发生冲突时，纯函数和 PostgreSQL upsert 均保留当前值，不再按时间字符串选择较早值；冲突以 `equal_quality_publication_conflict` 和 `equal_quality_conflict_current_retained` 记录在 Candidate/Detail 或 `crawler.contents.raw_json.publication_evidence_conflict`。
17. Migration Gate 会从年龄排除 Candidate 已保存的 `scope.relation=outside` 恢复统计，`relation_counts.outside` 不再漏掉未入 Content 的明确旧视频。
18. Candidate 重试恢复发布时间时，既有 Detail 是 current、Uploads flat 是 candidate；同质量冲突保留既有 Detail，并同步按该方向选择 `published_text`，避免在联网复查前被旧 flat 错误排除。
19. 发布时间幂等按 `published_at + status + precision` 判断；来源不同但三者相同不构成冲突，纯函数与 PostgreSQL 冲突条件保持一致。来源仍随被选中的完整证据四元组原子保留。

2026-08-27 针对提交 `0c0fa16` 的复审补充：

20. yt-dlp flat Uploads 只接受正整数 Unix 秒级时间戳；非法 timestamp 会回退到通过 UTC 日历回读校验的 `YYYYMMDD` Upload Date。两者均非法时产出完整的 `unresolved/unknown/null` 四元组，`timestamp="0"` 不再被 JavaScript 日期解析成 2000 年，`20260230` 也不再自动进位成 3 月 2 日。
21. Incremental 历史 Video Activity 证据改为按 `(channel_id, source_content_id)` 现有唯一索引做键集分页，策略版本升级为 `incremental-video-activity-v3`。默认最多读取 1000 行、每页 200 行、分页墙钟预算 500ms；可分别通过 `INCREMENTAL_VIDEO_ACTIVITY_EVIDENCE_ROW_LIMIT`、`INCREMENTAL_VIDEO_ACTIVITY_EVIDENCE_PAGE_SIZE`、`INCREMENTAL_VIDEO_ACTIVITY_EVIDENCE_TIME_BUDGET_MS` 调整。超过任一预算即令 `evidence_complete=false`，禁止据此判 dormant。
22. Incremental Observation 的 `result_summary_json.activity` 与 Payload `activity_evidence` 同步保存扫描完整性、行数、页数、耗时、停止原因和预算值。`evidence_scan_truncated_count` 是通过多取一条哨兵行确认的已观测截断下界，并由 `evidence_scan_truncated_count_is_lower_bound=true` 明示，不执行会抵消性能收益的全量 `COUNT(*)`。
23. 本轮已经抓到的 Uploads/Detail 发布时间四元组直接并入生命周期证据；因此大频道历史扫描被截断时，明确的近期证据仍可恢复 active，未结束 Live 仍可阻止错误休眠。
24. `incremental-video-activity-v4` 用单个 PostgreSQL `NO SCROLL` 游标替代 v3 的多语句键集分页；外围事务继续使用 `READ COMMITTED`，游标在声明时固定证据快照，避免并发 Worker 更新导致不同页读取不同版本。
25. 每次游标声明和 `FETCH` 都按剩余墙钟预算设置事务局部 `statement_timeout`；`57014` 通过 SAVEPOINT 恢复为不完整证据，禁止休眠，同时保持外围 Incremental 事务可继续使用。慢速末页即使已经返回全部行，也不能在预算耗尽后被标成完整。
26. 当前轮 Detail 证据保留 `live_ended_at` 与 `duration_seconds`，已结束直播按回放参与发布时间判断；页外 Candidate 重试和 Recent Sampling 成功取得的 Detail 也直接进入本轮生命周期证据，历史扫描截断时不会遗漏明确近期内容。
27. 隔离 PostgreSQL 16.14 已实际验证参数化游标、`READ COMMITTED` 跨 `FETCH` 一致快照、锁等待触发 `statement_timeout`、SAVEPOINT 恢复以及恢复后同一事务继续查询；对应集成测试不依赖生产数据库。

复审后的本地验证结果：聚焦逻辑与链路测试全部通过；完整 `npm test` 共 256 个测试文件，252 个通过，4 个既有环境失败。失败原因分别为 sandbox 禁止 `spawnSync git`、两个本地监听 `EPERM`、以及当前 Python 环境缺少 `yt_dlp`，与本轮改动无关。隔离 PostgreSQL 16.14 已实际执行 Video Activity 游标快照与数据库超时恢复集成测试，结果 2/2 通过、无跳过；复审方此前另行执行的 Full Video SQL 与 Migration Gate 集成测试也均为 1/1 通过、无跳过。

尚未声称完成的数据工作：

1. 当前工作树没有生产只读数据库和 S3 凭据，因此尚未统计 `youtube_channel_uploads_flat_json` 的实际可读覆盖率，也未对 100 个 Run 执行 raw-object dry-run 重分类。
2. 尚未生成 A/B/C 历史修复清单，未执行 `evidence_correction` 或 `policy_reclassification` Apply。
3. 上述数据步骤必须继续遵守第 10-12 节：单连接或低并发、先只读副本、稳定清单/哈希、小批次、正常 Crawler/Publication 传播。
4. 在 raw object 可读覆盖率、A/B/C 历史清单和只读 dry-run 完成前，本分支不得合并或部署；经明确批准可以创建本地 checkpoint commit，但不得据此宣称生产门禁完成。

本轮复审新增的回归覆盖包括：空时间不变 1970、Incremental 当前直播与节流期直播均阻止休眠、不完整扫描不能休眠但近期详情仍可通过、年龄排除项计入 outside、同质量冲突双向保留当前值并生成冲突记录、非法 yt-dlp timestamp 回退、非法 Upload Date 归 unresolved、历史证据完整分页、行数/时间预算截断、截断时本轮近期证据恢复 active，以及扫描指标写入 Observation。PostgreSQL 冲突集成用例仍要求生产前在可控只读测试库执行。
