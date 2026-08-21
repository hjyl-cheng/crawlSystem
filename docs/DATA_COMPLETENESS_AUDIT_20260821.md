# 今日 Incremental Clock 数据完整性审计

**快照时点**：2026-08-21 16:31 北京时间（08:31 UTC）。Clock 仍在执行，终态数字会继续增长。
**统计范围**：仅今日 Incremental Clock（`plan_day = 2026-08-21`，`observation_kind = 'video' / 'about'`）。不含 Migration、Query、Full Crawl。
**数据来源**：`bullmq_crawler_migration` 库，全程 `READ ONLY` 事务，未做任何写入。
**校验方式**：所有门禁判定按 `services/qybullmq/src/videoPublicationCurrent.js` 的实际代码逐条在 SQL 中复现，不使用估算口径。

---

## 1. Clock 执行进度

| 状态 | 频道数 | 说明 |
|---|---:|---|
| 成功 | 9,388 | 本次计划完成 |
| 部分完成 | 650 | 至少一个采集环节不完整 |
| 失败 | 4 | 详见第 5 节 |
| 取消 | 59 | 休眠、移除等正常终止 |
| 执行中 | 17 | Worker 正在处理 |
| 已派发待执行 | 28 | 已进入队列 |
| 尚未派发 | 2,853 | 今日计划尚未开始 |
| **总计** | **12,999** | 已终态 10,101，约 **77.7%** |

起跑时间 03:40 UTC。前两日均为 00:30 UTC 起跑，今日晚 3 小时 10 分（Scheduler 当日刚完成接管）。

**Agent Clock 今日排程数为 0**，`run_agent` 全为 false，该域完全未执行。

---

## 2. 发现环节总账（今日增量）

| 指标 | 数值 |
|---|---:|
| 扫描 uploads 条目 | 38,403 |
| 成功入库（first_seen） | 26,686 |
| **未入库（unresolved）** | **3,195** |
| 详情抓取成功 | 29,881 |
| 详情抓取失败 | **0** |
| 受影响频道 | 648 |

### 关键恒等式

```
detail_success_count (29,881) = first_seen_count (26,686) + unresolved_count (3,195)
detail_failure_count = 0
```

这个等式精确成立，且详情失败数为零。含义是：**这 3,195 个视频的详情已经全部成功抓取到了，网络请求、代理配额、解析都已消耗完毕，数据拿到手之后被丢弃，没有写入数据库。** 这不是"抓不到"，是"抓到了没存"。

---

## 3. 完整问题清单

今日增量触达视频 **93,764 个**（新 26,686 / 存量 67,078），覆盖 **7,169 个频道**。

### P0 — 硬缺失：视频完全不在库中

| 问题 | 视频数 | 频道数 | 新视频 | 存量 | 影响 |
|---|---:|---:|---:|---:|---|
| Uploads 已发现且详情已抓到，但没有写入 `crawler.contents` | **3,195** | **648** | 3,195 | 0 | 视频不存在于 Current，无法显示与分发；且增量游标因此不前进 |

数据库中对这 3,195 个视频的留存情况：

| 留存项 | 数量 | 说明 |
|---|---:|---|
| `crawler.contents` 行 | 0 | 无内容行 |
| `crawler.content_candidates` 行 | 0 | 未进入 detail 补救路径 |
| `crawler.content_enrich_tasks` 行 | 0 | 未生成补全任务 |
| 观测中的 `unresolved_video_ids` | **0 条观测有该字段** | 视频 ID 本身未落库 |
| 失败原因 | 无 | 代码直接 `return null`，未记录 `classified_only` 还是 `unresolved` |

**这批视频在数据库中不留任何可追溯痕迹——ID 没有，原因没有，重试入口也没有。**

### P1 — 硬缺失：已入库但过不了发布门禁

按 `buildVideoPublicationItem()` 的 13 项 checks 逐条复现：

| 问题 | 视频数 | 频道数 | 新视频 | 存量 | 实际形态 |
|---|---:|---:|---:|---:|---|
| **任一硬校验失败（去重）** | **76** | **22** | 0 | 76 | |
| ├ `published_at` 时间证据不完整 | 62 | 10 | 0 | 62 | `status=unresolved`, `precision=unknown`, `source` 为空 |
| ├ `access_status=unlisted` 不被门禁接受 | 9 | 7 | 0 | 9 | 见下方契约冲突 |
| ├ `duration` 溯源缺失 | 4 | 4 | 0 | 4 | `status=exact` 且**有值**，但 `duration_source` 为空 |
| └ `like_count` 溯源缺失 | 1 | 1 | 0 | 1 | `status=exact`, `value=0`，但 `like_count_source` 为空 |

> **契约冲突（代码级缺陷）**：`fullVideoContentStore.js` 的 `NEW_CONTENT_ACCESS_STATUSES` 允许 `unlisted` 入库，但 `videoPublicationCurrent.js` 的门禁白名单是 `public / members_only / private / unavailable / login_required / unknown`，**不含 `unlisted`**。因此 unlisted 视频能存进来，却永远无法发布，且不会产生任何告警。

> **注意**：`duration` 和 `like_count` 这 5 条不是"数值与状态矛盾"——数值和状态是一致的，缺的是 `_source` 溯源字段。属于写入端字段遗漏，不是采集失败。

### P2 — 软缺失：可发布，但数据不完整

| 问题 | 视频数 | 频道数 | 新视频 | 存量 |
|---|---:|---:|---:|---:|
| 点赞数不可用或未解析 | 3,628 | 772 | 2,200 | 1,428 |
| 明确有评论（`count>0`）但未采到任何正文 | 1,741 | 1,585 | 112 | 1,629 |
| 评论总数未知且无正文 | 283 | 186 | 7 | 276 |
| 时长不可用或未解析 | 96 | 27 | 0 | 96 |
| 播放量不可用或未解析 | 63 | 11 | 0 | 63 |
| 视频描述不可用或未解析 | 62 | 10 | 0 | 62 |
| `access_status=unknown`（无法证明可否公开） | 51 | 9 | 0 | 51 |
| 已有正文但评论总数未知 | 29 | 16 | 18 | 11 |

三类评论问题互斥，合计影响 **2,053 个视频 / 1,730 个频道**：

| 评论总数 | 评论正文 | 视频数 | 含义 |
|---|---|---:|---|
| 已知 > 0 | 没有 | 1,741 | 最明确的评论采集失败 |
| 未知 | 没有 | 283 | 数量与正文都未解决 |
| 未知 | 已有 | 29 | 正文成功，只缺总数 |

参考量（**不计入缺失**）：`comments_disabled` 终态 5,588 个视频 / 3,595 频道，作者主动关闭评论，属永久终态。

---

## 4. 去重后的真实规模

| 范围 | 唯一视频/ID | 唯一频道 | 新视频 | 存量视频 |
|---|---:|---:|---:|---:|
| 硬缺失 · 完全未入库 | 3,195 | 648 | 3,195 | 0 |
| 硬缺失 · 已入库但过不了门禁 | 76 | 22 | 0 | 76 |
| 软缺失 | 5,650 | 2,389 | 2,331 | 3,319 |
| 硬 ∩ 软 重叠（已入库部分） | 65 | 13 | 0 | 65 |
| **已入库问题合计（去重）** | **5,661** | **2,394** | 2,331 | 3,330 |
| **全部问题合计** | **8,856** | **2,760** | 5,526 | 3,330 |

频道口径说明：648 个"未入库"频道与 2,394 个"已入库有问题"频道之间重叠 209 个，并集为 **2,760 个频道**，占今日触达频道（7,169）的 **38.5%**。

---

## 5. 频道 About 域问题

| 问题 | 频道数 | 说明 |
|---|---:|---|
| `total_video_count` 当次观测为 `unavailable` | 3 | 导致 About 结果为 partial |
| `total_view_count` 当次观测为 `unavailable` | 2 | 包含在上面 3 个频道中 |
| `subscriber_count` 缺失 | 0 | 本次无此问题 |
| 真正运行失败 | 1 | 重试 5 次后仍报 `cannot execute INSERT in a read-only transaction` |
| 频道已被 YouTube 移除 | 1 | `community_guidelines`，正常终态 |

**重要区分**：这 3 个频道的**当次快照**（`channel_about_metric_snapshots`）确实是 `unavailable`，但 `crawler.channels` 持久化行仍保留上一次的 `exact` 值。因此**持久化数据没有丢失**，只是本次刷新未取到。

**`cannot execute INSERT in a read-only transaction`（频道 `UCLGNJYRIp1fY2l7KQq-uAxA`）的根因已经确认**：外部审计脚本在共享 PgBouncer 上执行了 session 级 `SET default_transaction_read_only=on`。PgBouncer 使用 transaction pooling，受污染的 PostgreSQL 后端连接随后被 Worker 复用，导致写入被拒绝。这不是 YouTube、只读副本或 Worker 数据库权限问题。

---

## 6. 根因分析（代码级）

### 6.1 视频被丢弃的完整链路

`services/qybullmq/src/incrementalVideo.js:402` `upsertFirstSeenContent()`：

```js
const storageAction = fullVideoStorageAction({
  candidate: {},                                        // ← 恒为空对象
  classification,
  access: { access_status: facts?.access_status ?? "unknown" },
});
if (storageAction.kind !== "upsert") return null;       // ← 唯一早退，无日志、无原因
```

`services/qybullmq/src/fullVideoContentStore.js:28` `fullVideoStorageAction()` 的判定顺序：

1. `update_access` 分支需要 `existingKey`——但调用方恒传 `candidate: {}`，**此分支永不命中**；
2. `upsert` 需要 `classification.authoritative === true` **且** `access_status ∈ {public, unlisted, members_only}`；
3. 否则返回 `classified_only` 或 `unresolved` → 调用方 `return null` → **丢弃**。

`classification` 来自 `resolveYoutubeContentType()`（`youtubeContentType.js:185`）：

- `resolveFromDetail()`：只有命中 watch 信号分支（`youtubei|youtubejs|youtube_watch|yt_dlp`）才返回 `authoritative: true`；结构化信号存在但无分支命中时，第 138 行直接 `return null`。
- `resolveFromUpload()`：第 181 行要求 `content_type ∈ {short, live}`——**普通 `video` 直接返回 null**；即便命中，第 182 行也固定返回 `authoritative: false`。

**结论**：任何新发现的 upload，只要详情未能产出 authoritative 的 watch 信号分类，就会被静默丢弃，即使详情抓取本身完全成功。这解释了第 2 节那个恒等式。

### 6.2 增量游标因此卡死

`incrementalVideo.js:1256-1258`：

```js
anchorVideoIds: discovery.outcome === "complete" ? mergedDiscoveryAnchorIds(...) : null,
sourceCursor:   discovery.outcome === "complete" ? { ... } : null,
```

而 `discovery.outcome`（第 747 行）：

```js
outcome: scan.complete && unresolvedVideoIds.length === 0 ? "complete" : "partial"
```

**只要有 1 个视频未入库，游标和锚点就都不更新。** 实测验证：

| 频道分组 | 频道数 | 今日 `latest_complete_observed_at` 前进 |
|---|---:|---:|
| 有未入库视频 | 648 | **0** |
| 全部入库 | 6,524 | 6,523 |

后果是双重的：数据缺口无法自愈，且**下一次 Clock 会重新扫描并重新抓取同一批 uploads**——已经付出的详情抓取成本会被再付一次。

### 6.3 重复浪费的实测

今日 648 个受影响频道中，在过去 7 天内重复出现的分布：

| 近 7 天出现天数 | 频道数 |
|---:|---:|
| 1 天（仅今日） | 225 |
| 2 天 | 404 |
| 3 天 | 12 |

**65% 的受影响频道是重复命中**，说明该问题会持续复现而非一次性。

### 6.4 历史趋势

| 日期 | 受影响频道 | 未入库视频 | 扫描 uploads | 未入库率 |
|---|---:|---:|---:|---:|
| 08-14 | 1,064 | 6,991 | 8,627 | 81.0% |
| 08-15 | 5,813 | 41,265 | 57,985 | 71.2% |
| 08-16 | 3,840 | 31,418 | 42,406 | 74.1% |
| 08-17 | 1,340 | 11,632 | 27,776 | 41.9% |
| 08-18 | 316 | 1,834 | 83,617 | **2.2%** |
| 08-19 | 296 | 2,709 | 55,386 | 4.9% |
| 08-20 | 89 | 413 | 11,642 | 3.5% |
| **08-21（进行中）** | **648** | **3,195** | **38,403** | **8.3%** |

08-17→08-18 之间有一次显著改善（41.9% → 2.2%）。但今日 8.3% 相对昨日 3.5% **有明显回升**，需要确认是否与今日 Scheduler/Dispatch 接管为 pachongsys 镜像相关。

---

## 7. 今日未入库最严重的频道（Top 15）

| 频道 | 订阅数 | 扫描 | 未入库 | 已入库 | 详情成功 |
|---|---:|---:|---:|---:|---:|
| Fute Resenha | 91,900 | 84 | 83 | 0 | 83 |
| MULTIVERSO POLÍTICO | 27,900 | 76 | 68 | 7 | 75 |
| Voz do Esporte | 319,000 | 87 | 64 | 18 | 82 |
| Bandsports | 1,620,000 | 60 | 57 | 0 | 57 |
| Carranza Cursos | 362,000 | 96 | 48 | 0 | 48 |
| Pânico Jovem Pan | 4,250,000 | 42 | 41 | 0 | 41 |
| Isabela e Maitê Catunda | 3,170,000 | 42 | 41 | 0 | 41 |
| Sistema Diário de Comunicação | 349,000 | 39 | 38 | 0 | 38 |
| MARCOS EDUARDO | 557,000 | 44 | 36 | 7 | 43 |
| Folha TV | 200,000 | 46 | 35 | 7 | 42 |
| Diário de Santa Maria | 107,000 | 49 | 34 | 12 | 46 |
| HFNew Atualidades | 505,000 | 76 | 33 | 42 | 75 |
| TH+ Record | 32,800 | 48 | 33 | 14 | 47 |
| Jovem Pan Maringá | 365,000 | 44 | 33 | 10 | 43 |
| Christian Silva - Assessoria | 2,880 | 34 | 32 | 1 | 33 |

注意 `详情成功 = 未入库 + 已入库` 在每一行都成立。多个频道 `已入库 = 0`——整轮抓取的产出全部丢弃。

---

## 8. 硬标准 / 软标准的划分建议

系统已经用 `_status` 字段把"永久终态"和"暂时未取得"区分开了，但目前的重试与告警口径没有利用这个区分。建议按下表固化：

### 应判定为「已完成」的终态（不应重试、不应计入缺失）

| 字段 | 终态值 | 今日规模 | 理由 |
|---|---|---:|---|
| `comment_count` | `disabled` | 5,588 | 作者关闭评论，永远拿不到 |
| `description` | `empty` | — | 作者确实未填写 |
| `like_count` | `zero_from_empty` | — | 确认为 0 |
| `comment_count` | `zero_from_surface` | — | 确认为 0 |
| `subscriber_count` | `estimated` | 全量 100% | YouTube 只提供约数，不存在 exact |
| `access_status` | `private` / `members_only` / `login_required` | — | 权限终态，重试无意义 |

### 硬标准（缺失则不得发布，必须重试至成功）

| 字段 | 判定 | 今日违反 |
|---|---|---:|
| 视频入库本身 | 必须存在 `contents` 行 | **3,195** |
| `published_at` | `status=exact` + `precision ∈ {second,date_only}` + `source` 非空 | 62 |
| `access_status` | 必须在门禁白名单内 | 9（unlisted） |
| `title` / `url` / `kind` | 非空且合法 | 0 |
| 各计数字段的 `_source` | 状态为已解析时必须有溯源 | 5 |

### 软标准（不阻断发布，按成本决定重试上限）

| 字段 | 今日缺失 | 建议 |
|---|---:|---|
| `like_count` | 3,628 | 允许 `unresolved`，设重试上限 |
| 评论正文 | 2,053 | 成本最高的一项，建议按频道价值分级 |
| `duration` / `view_count` / `description` | 221 | 现状已很低，维持即可 |

**核心判断**：从数据上看，"一直请求也拿不到、纯烧性能"的情况**目前几乎不存在**——`content_candidates` 全量 1,058,693 条中只有 122 条带任何 `missing_fields`（0.01%），平均尝试次数 0.57，上限 3。今日详情抓取失败数为 0。因此性能与完整性此刻**并不冲突**，真正的损耗来自第 6.2 节的重复抓取，而不是难取字段的反复重试。

---

## 9. 建议优先级

| 优先级 | 动作 | 依据 |
|---|---|---|
| 1 | 在 `upsertFirstSeenContent` 丢弃分支落库失败原因与 `video_id`（区分 `classified_only` / `unresolved`） | 当前 0 条可追溯记录，无法定位也无法重试 |
| 2 | 排查 `resolveFromUpload` 对普通 `video` 返回 null、以及 `authoritative:false` 是否为预期 | 直接决定 3,195 条的去向 |
| 3 | 修复 `fullVideoStorageAction` 调用处恒传 `candidate: {}` | 导致 `update_access` 分支永久不可达 |
| 4 | 统一 `unlisted` 在存储策略与发布门禁之间的口径 | 存得进、发不出，且无告警 |
| 5 | 清除 PgBouncer 已污染连接，并禁止审计脚本设置 session 级默认只读 | 已确认是共享连接状态污染，非 YouTube 侧 |
| 6 | 确认 Agent Clock 今日 0 排程是否预期 | 整域未执行 |
| 7 | 补齐 `duration_source` / `like_count_source` 写入 | 5 条因溯源缺失被门禁拒绝 |

> 游标不前进这一项**不建议单独"修"**：它是 6.1 的下游症状，也是当前唯一阻止数据缺口被静默固化的保护机制。应先修 6.1，再评估游标策略。

---

*本报告全部数字由只读 SQL 独立复算，门禁判定逐条对齐源码实现。Clock 仍在执行中，终态数字会继续增长。*
