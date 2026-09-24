# 9 月 22 日增量完成情况核查

核查时间：2026-09-23 06:55–06:57 UTC（北京时间 14:55–14:57）。仅只读检查，未重试或修改任务。

## 结论

尚未全部完整完成。系统每日计划按 UTC 日期记录，plan_day=2026-09-22 共 74,069 个计划；对应北京时间 9 月 22 日 08:00 至 9 月 23 日 08:00 的计划日，不等同于按北京时间自然日筛选 scheduled_at。

| 计划状态 | 数量 |
| --- | ---: |
| cancelled | 2,646 |
| dispatched | 26 |
| failed | 255 |
| partial | 105 |
| running | 2 |
| succeeded | 71,035 |

succeeded 由所需领域的 applied 事件归并得到；Run 的 running/done 或队列 completed 单独不能代替这个判定。105 个 partial 不计为完整成功。

## 28 个状态未收尾的失败任务

26 个 dispatched、2 个 running 的计划对应 Redis Job 全部为 failed，错误均为 `job stalled more than allowable limit`，没有 waiting/active/delayed。这 28 个频道均没有 9 月 23 日计划。采样未执行重试，不能保证自行继续。

## 取消与后续计划

- 2,378 个 cancelled 原因为 superseded_by_daily_plan，均没有原 Run；今日计划为 2,311 succeeded、48 failed、6 partial、13 cancelled。今日成功表示后续 Plan 满足自身需求，不证明原日所有字段/目标均已补齐，也不能重建昨日时间点快照。
- 268 个 cancelled 原因为 channel_dormant；均有 done Run，其中 265 个所需 about/video 域为 complete、3 个 about partial/video complete。这是频道休眠分支，不能混为排队未抓取。
- 昨日 255 个 failed 中，92 个频道今日计划 succeeded、16 partial、134 failed、13 没有今日计划。昨日 105 partial 中，今日 3 succeeded、43 planned、59 没有今日计划；不能据此推断未到期或应立即重试，需检查 Clock 与领域要求。

## 事件与状态一致性

- 昨日 Plan 关联的 inbox 中，未发现 received/waiting_gap/rejected；这不证明缺失、尚未投递的事件不存在。
- 昨日 succeeded 中还有 28 个 Run 显示 running，failed 中有 201 个 Run 显示 running；存在 Run 状态残留，不能把这些数当作真实在执行数。

## 北京时间自然日辅助口径

按 scheduled_at ∈ [2026-09-21 16:00 UTC, 2026-09-22 16:00 UTC) 查询：67,465 succeeded、255 failed、94 partial、281 cancelled，无活动状态。该口径跨两个 UTC plan_day，且不包含 scheduled_at 尚为空的计划，不能作为每日计划总覆盖率分母。此口径同样不能回答为“全部完整成功”。

## 待收尾清单

| Plan ID | 频道 | 计划状态 | 实际队列状态 |
| --- | --- | --- | --- |
| 091f5cfe-adcc-5c04-9e4a-014d5971aadb | UCDbumiPike3qLJeARBWrPFA | dispatched | failed |
| 146bf28f-a2b7-58d5-b6ff-da09a7cf27d4 | UCGi_mAdy_Vp1x8RaUnmViqg | dispatched | failed |
| 1a9c504a-73b0-572c-9899-ba19eb0fa3ec | UCtKzd9GiOQxvGAqNg1aJBfQ | running | failed |
| 23c71101-7f68-5079-a784-4b293f635f93 | UCWn6F8sIOuFSjo97ycgSpDg | dispatched | failed |
| 29d563c2-d45c-5537-8100-e9b5b9fd6593 | UC47oEXGl7Yi4IlFVVCVTvCA | dispatched | failed |
| 3eebbc3e-3b05-55df-a630-80c3f4fd946b | UCh3dAEDLI0XvyTRg-S6U9ag | dispatched | failed |
| 51e644dc-1e7d-53ae-a9eb-991c8b7529db | UCBjW3vrM-BwGTpMkemqoz9Q | dispatched | failed |
| 5b687090-0575-5463-b59e-62383a34f25e | UC4xNPxlvJrggYUKY4PO9Y9A | dispatched | failed |
| 6a86a13d-b95d-5a75-bb82-d173841e20a5 | UCG-8zVxifyMld8Ir2zLtKWg | dispatched | failed |
| 88fad762-7998-5845-9794-59554fc26420 | UCkoMrknasSdZa_AWA8H8E7Q | dispatched | failed |
| 89dcc276-fdcb-5bc0-8d65-2f9314a30fa8 | UC_cbgP056_Hg3SOdTS0DhxA | dispatched | failed |
| 89f72b49-57d5-5411-9dbf-9e39d6401b34 | UCL42p3xsCKoXhaP2v2g_zvA | dispatched | failed |
| 8bdd1ff1-cd96-5e81-95b7-b6f5437f6dc4 | UCPZuvR8EeIjVdbISGqpTgbw | dispatched | failed |
| 8be7ea44-5455-5720-8774-4f0a68ed8e90 | UCkoPtv_ox0BorisSe8aymlQ | dispatched | failed |
| 8fc92df2-ce37-56e0-9dd8-2b9d811def9d | UCgiWipiQ_efANCX-0cev-6A | dispatched | failed |
| a14b91d0-8ada-53c6-ad86-328dc00140ee | UCnCMBvcsxznlKfHOYqCu80Q | dispatched | failed |
| a91aa864-f8da-5f53-86bf-fca0970a78ff | UChAB5ZBjKa0oJzp7QP4JF_A | running | failed |
| b1197ee5-4b7c-5a4f-b8d9-1a60d5c37f72 | UCPZ_HuiPxVs4kyCuW9riBTQ | dispatched | failed |
| b7efaac4-ea34-5b58-b90f-df37002232ca | UC9_S4Lv-72TjmQBR79bAyDg | dispatched | failed |
| c4b82fc7-8adc-5e34-9774-1683a0ece3ca | UCTHs2EYw7eUCiAZdrztxYEA | dispatched | failed |
| cf3cb233-c678-5e75-a509-530db5f467d3 | UCm4zi9d7yy3esJQ5Ff4Ullw | dispatched | failed |
| d44d9fae-7e8e-5065-b064-8d01f32f7033 | UCMDzQYbGcp5BRjhZBBVYoxQ | dispatched | failed |
| d9b60e3c-62be-56eb-9c71-1fc7e8ab2eda | UCSt1TJNmKgdBWC-RLgX497g | dispatched | failed |
| e165d720-5d83-51cd-9ec9-b23e7e0cae1c | UCDOBBxOU2tqajmVnGM2OKOg | dispatched | failed |
| e1670544-947f-541a-8c49-d87b1c40cee5 | UC_JjfmtOpnhLDyZVRo-88zg | dispatched | failed |
| e40b256a-4fa0-5cd8-be36-b4ed24bdff95 | UCPT5cUYl38ng3C4aPNGjl1g | dispatched | failed |
| e7cda6eb-8f21-5fb5-be61-ce561ef03767 | UCL6OHgi2_x9UvEgkGFN3UgA | dispatched | failed |
| ea8a305d-c6ba-587b-a14d-4cb40c0468d9 | UCf7faX6qSeom5OWpG2p7hGA | dispatched | failed |

原始只读统计与脚本暂存于 `/tmp/yesterday-incremental-20260923/`。多次查询并非跨 PostgreSQL/Redis 的原子快照；计数以采样时间为准。
