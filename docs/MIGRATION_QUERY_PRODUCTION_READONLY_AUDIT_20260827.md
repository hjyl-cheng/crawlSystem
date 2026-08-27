# Migration / Query 100 Run Production Read-only Audit

审计日期：2026-08-27 UTC

代码基线：`agent/query-migration @ 3582a03`

生产批次：`qy-migration-v1 / legacy-results-canary-1787722516493-b0c4f370`

## 结论

代码层复审通过，当前分支可以交由主 Agent 合并并安排上线。生产历史数据尚未修复完，不能仅因代码合并或部署就宣称这批 100 个频道已经完成数据闭环。

本次只读审计确认：

1. 目标批次确实是 100 个不同频道、100 个 Candidate、100 个一一对应的 Full Run；全部 Run 已结束。
2. raw metadata 覆盖率和 MinIO 实际可读率都是 100/100；对象内容、压缩、SHA-256、JSON 和 channel identity 全部通过。
3. 这 100 个 raw object 全部来自 YouTubeJS，2,985 条 Uploads entry 全部没有可用发布时间；初始窗口证据必须全部归 `unresolved`。
4. 旧生产结果是 `70 passed / 30 dormant`；按新五态口径只读重算为 `71 passed / 29 inconclusive / 0 dormant`。
5. ITDP Brasil 有一条精确近期 Detail，明确应从 dormant 改为 passed。
6. 另外 29 个 dormant Run 只抓到了第一条明确旧视频，后续 826 条候选没有 Detail。不能继续借播放列表顺序推断它们也旧，因此只能是 inconclusive，必须重抓后再决定。

本次没有修改 PostgreSQL、MinIO、队列、频道状态或下游 Publication 数据。

## 执行安全

- PostgreSQL：单连接，`REPEATABLE READ READ ONLY`，每次查询设置 `statement_timeout`。
- MinIO：仅执行 `statObject/getObject`；没有 put、delete、copy 或 bucket mutation。
- raw 校验：逐对象 gzip 解压、JSON parse，并用未压缩 payload 重算 SHA-256。
- dry-run：不相信旧的 `after_chronological_age_cutoff` 批量结论；没有 Detail 的候选按 `unresolved` 处理。
- 没有读取或输出任何数据库、MinIO 或代理密钥。

## 稳定清单

清单文件：`docs/MIGRATION_QUERY_AUDIT_MANIFEST_20260826_100_RUNS.tsv`

格式，每行八列且无表头：

```text
source_id<TAB>batch_id<TAB>source_candidate_id<TAB>channel_id<TAB>target_candidate_id<TAB>run_id<TAB>raw_object_id<TAB>raw_content_hash<LF>
```

- 行数：100
- SHA-256：`b59bb133092f50d15928f3bd88173b7c4fda6468247d4ba50ae9ac4744de713c`

任何数据 Apply 或重抓前必须重新生成同格式清单并核对该哈希；不一致时停止执行并重新审计批次身份。

## Raw 覆盖率

| 检查项 | 结果 |
|---|---:|
| migration intent / distinct channel | 100 / 100 |
| target Candidate accepted | 100 / 100 |
| Candidate -> Run 一一对应 | 100 / 100 |
| Full Run done / latest Run | 100 / 100 |
| raw metadata 行 | 100 / 100 |
| MinIO stat 成功 | 100 / 100 |
| MinIO get 成功 | 100 / 100 |
| gzip 解压成功 | 100 / 100 |
| SHA-256 一致 | 100 / 100 |
| JSON parse 成功 | 100 / 100 |
| payload channel_id 一致 | 100 / 100 |
| raw / Candidate video identity 差异 | 0 |
| Uploads parse gap = 0 | 100 / 100 |

对象来源全部为 `youtubejs_uploads_playlist_v2`，payload engine 全部为 `youtubei.js@17.2.0`，且都只读取了 1 个 Uploads page。总原始大小 2,916,574 bytes，gzip 后 349,116 bytes。99 个对象包含 30 条 entry，1 个对象包含 15 条，共 2,985 条。

## 历史证据分类

生产 raw 暴露了原设计 A/B/C 之外的第四种实际形态：对象可读且 Extractor lineage 明确，但 payload 中根本没有发布时间值。

| 分类 | Run 数 | Entry 数 | 处置 |
|---|---:|---:|---|
| A. raw 有合法 yt-dlp timestamp | 0 | 0 | 无可离线恢复的 `exact/second` |
| 合法 yt-dlp upload date | 0 | 0 | 无 `exact/date_only` 样本 |
| B. 有明确 YouTubeJS relative date | 0 | 0 | 无值可降为 `relative/date_only` |
| C. raw 缺失或不可读 | 0 | 0 | 本批不存在 |
| D. raw 可读、YouTubeJS lineage 明确、发布时间缺失 | 100 | 2,985 | 归 `unresolved/unknown`，按需抓 Detail |

D 不能伪装成 B：没有日期值时不能写 `relative/date_only`。它也不是 C：raw object 完整存在且可读。代码已经能安全处理 D，但历史修复清单和上线门禁必须显式记录该类。

## 五态 Dry-run

初始 raw 五态：

| inside | outside | after_as_of | cutoff_overlap | unresolved |
|---:|---:|---:|---:|---:|
| 0 | 0 | 0 | 0 | 2,985 |

已保存的 Candidate Detail / Content 证据：

| inside | outside | after_as_of | cutoff_overlap | unresolved | upcoming excluded |
|---:|---:|---:|---:|---:|---:|
| 1,145 | 75 | 0 | 0 | 1,727 | 38 |

- 已有可用 Detail：1,258。
- 旧流程批量跳过、没有 Detail：1,727。
- 1,727 条中，826 条属于 29 个仍无法定性的 dormant Run，28 条属于 ITDP，另外 873 条属于旧 Gate 已 passed 的 70 个 Run。
- 70 个旧 passed Run 已有 inside 证据，所以频道活动决策成立；但其 873 条未抓 Detail 的候选仍未满足“视频数据完整且正确”的验收要求。
- 如果用新分支完整重跑，预计 `details_requested_due_to_unresolved_count` 总量为 2,985；在相同频道状态下，71 个最终 passed Run 会满足 `dormant_reversed_after_detail_count=1` 的条件。

Run 决策变化：

| 决策 | 旧生产结果 | 新口径 dry-run |
|---|---:|---:|
| passed | 70 | 71 |
| dormant | 30 | 0 |
| inconclusive | 0 | 29 |

`inconclusive` 不等于 active，也不等于 dormant；它表示现有证据不足，系统必须继续抓 Detail，而不能提前休眠频道。

## 真实样本

| 样本 | 精确发布时间年龄 | 新关系 | 数据结论 |
|---|---:|---|---|
| ITDP [`ooPL-Tk-6qI`](https://www.youtube.com/watch?v=ooPL-Tk-6qI) | 89.5109 天 | inside | 旧 dormant 错误；新 Gate 为 passed |
| Boxe [`hEBA7Jg5oVQ`](https://www.youtube.com/watch?v=hEBA7Jg5oVQ) | 90.1699 天 | outside | 单条视频应为 outside；频道另有 inside 证据，仍为 passed |
| MATRIA [`oy7l_ybAng8`](https://www.youtube.com/watch?v=oy7l_ybAng8) | 29.5284 天 | inside | 旧数据仍是 `login_required/deferred`；明确 playability reason 为 `age_restricted`，分支修复后应按 public + restriction 处理 |

生产现存 MATRIA 行尚未被修复；本次只读审计没有回写它。

## 需要重抓的 29 个频道

以下频道旧值都是 dormant，新口径都是 inconclusive。除最后一个频道缺 14 条 Detail 外，其余各缺 29 条。

1. [Aline Supino](https://www.youtube.com/channel/UCNxlFnkJi_79d1tZA-n_W0A)
2. [ATIVIDADES EDUCATIVAS DA SASSA](https://www.youtube.com/channel/UCftXagSBC4K_prhWAhtN4pQ)
3. [BTFF - BRASIL TRADING FITNESS FAIR](https://www.youtube.com/channel/UCg6DSvbx1UA-CD21ojm87sA)
4. [by Carla Soares](https://www.youtube.com/channel/UCLfPV97MDFj0x0P3Wdt2L4Q)
5. [Cantinho Pedagogico](https://www.youtube.com/channel/UCC8q8h3zVWHgip2wCdMfujA)
6. [ClickTube Variedades](https://www.youtube.com/channel/UC6wsPyAwtt9wSwyN9J7G-aw)
7. [Floresta Ativista](https://www.youtube.com/channel/UCSZqkeQb78fE9nN98L8dq6g)
8. [Fred Novaes](https://www.youtube.com/channel/UCqHkfBtBYQajwHnUPWjfGDw)
9. [Gabriela Waleska](https://www.youtube.com/channel/UCu1ulVm5es9a7jU5hk4NBZg)
10. [GS Select Cars](https://www.youtube.com/channel/UCuA2EaAvFuhx521EPqOVg3w)
11. [Historia 77](https://www.youtube.com/channel/UCL1uEYnyMPYLU34IVtnIdvw)
12. [Humanas.Com! Prof. Felipe Eller](https://www.youtube.com/channel/UCmp0xxvG4aIY8uz4YoZ8hDA)
13. [IREETV](https://www.youtube.com/channel/UC3085NTJcFdI5D3M0Un9RmA)
14. [KATON Podcast](https://www.youtube.com/channel/UCN-ZJYG97S-Iqr6sxNquw1A)
15. [Maozinhapontocom](https://www.youtube.com/channel/UCKuph-gwfHANDLl6NLQs-pg)
16. [marina mattos](https://www.youtube.com/channel/UCVHVco5ALDMiutKgPiVnJYw)
17. [Momento Educacao](https://www.youtube.com/channel/UCMa3vUL4mDZ1zrtoB0RqY9Q)
18. [Momentos Flow](https://www.youtube.com/channel/UCgeBlu7cpnc6vW5VmOjiNMQ)
19. [Nutricao e Endurance](https://www.youtube.com/channel/UC2QCzVQh043NKo4Pr9t9ahw)
20. [Observatorio de Politica Ambiental](https://www.youtube.com/channel/UCvoC4O-itLvvYLrzGNd5o6A)
21. [Ola, criancas!](https://www.youtube.com/channel/UCghsc7y0BJYDi-mGU2pXXVg)
22. [OSistematico](https://www.youtube.com/channel/UCwSCPKW_xBK71iqD0ftS5Ew)
23. [Redacandro](https://www.youtube.com/channel/UC8WF3BuY5FbZnH89ICIboKg)
24. [Roju Soares](https://www.youtube.com/channel/UCRxJP6a-ZisisT-Xknobgng)
25. [Sting](https://www.youtube.com/channel/UCdvDOk6cNRhrjWzfAt8SP5A)
26. [Thais Calixto](https://www.youtube.com/channel/UCIZh93fiIWETlgEql0ybhXg)
27. [TODXS](https://www.youtube.com/channel/UCgBPwHTAOWHE_hFO0Sp8T3Q)
28. [XO DUVIDA - prof. Luciana](https://www.youtube.com/channel/UCQQbzlF4mBLDi2AQu0ZHX2g)
29. [Videos de Animes](https://www.youtube.com/channel/UCoqI93npRf56np3UtFsmg_w) - 缺 14 条 Detail

ITDP Brasil 不在该重抓清单中，因为已有 inside Detail 足以确定 passed；它应进入证据纠错清单。

## 上线执行与数据闭环

以下事项不阻断代码交付给主 Agent，但必须纳入上线 canary 和历史数据修复流程：

1. 基于稳定 manifest 生成只读 repair plan，明确 ITDP 的 `evidence_correction` 与 1,727 条缺失 Detail 的重抓动作。
2. 在分支构建的受控 canary 中先处理 ITDP 和 29 个 inconclusive 频道，再处理旧 Gate 已 passed 的 70 个频道所缺的 873 条 Detail；不直接修改 Business Publication 表。
3. 重抓后重新运行五态审计；每个 Run 写入 `classifier_version / policy_version / repair_kind`。
4. 验证现有 30 个 dormant 状态全部有新证据支持：ITDP 应 passed，其余 29 个必须得到真实的 passed 或 dormant，不能保留 inconclusive 后宣称修复完成；同时复核全部 100 Run 的 30 条候选处置完整性。
5. 单独重放 MATRIA 年龄限制样本，确认 access 为 public 且 restriction 保留。

国家门禁和数据库版本统一不在本次审计范围内，均未修改。
