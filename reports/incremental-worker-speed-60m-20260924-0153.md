# 近一小时增量 worker 完成速度

统计窗口：UTC 2026-09-24T00:53:08.453000+00:00 ≤ 完成时间 < 2026-09-24T01:53:08.453000+00:00；北京时间 2026-09-24 08:53:08–09:53:08。

口径：crawler.task_events 中 youtube-channel-incremental 的 completed 事件，按 job_id 去重，按完成事件的 remote_node_id/remote_slot 归属。每分钟速度 = 60 分钟内完成任务数 / 60，未扣除停机或空闲时间。该指标是队列任务完成数，不是视频数，也不保证全部业务域 complete。

合计 3,077 个，51.28 个/分钟；当前启用的 55 个 worker 均有完成记录，平均每 worker 0.93 个/分钟。原始完成事件和去重任务数一致；3,070 个 session_opened=true、1 个 false、6 个未带字段。

## 增量主节点 node01：2,612 个，43.53 个/分钟

| Worker | 完成数 | 个/分钟 |
| --- | ---: | ---: |
| incremental-3 | 44 | 0.73 |
| incremental-4 | 83 | 1.38 |
| incremental-5 | 60 | 1.00 |
| incremental-6 | 38 | 0.63 |
| incremental-7 | 59 | 0.98 |
| incremental-8 | 63 | 1.05 |
| incremental-9 | 53 | 0.88 |
| incremental-10 | 45 | 0.75 |
| incremental-11 | 54 | 0.90 |
| incremental-12 | 47 | 0.78 |
| incremental-13 | 25 | 0.42 |
| incremental-14 | 68 | 1.13 |
| incremental-15 | 65 | 1.08 |
| incremental-16 | 30 | 0.50 |
| incremental-17 | 60 | 1.00 |
| incremental-18 | 64 | 1.07 |
| incremental-19 | 52 | 0.87 |
| incremental-20 | 79 | 1.32 |
| incremental-21 | 68 | 1.13 |
| incremental-22 | 75 | 1.25 |
| incremental-23 | 42 | 0.70 |
| incremental-24 | 22 | 0.37 |
| incremental-25 | 63 | 1.05 |
| incremental-26 | 32 | 0.53 |
| incremental-27 | 57 | 0.95 |
| incremental-28 | 39 | 0.65 |
| incremental-29 | 64 | 1.07 |
| incremental-30 | 65 | 1.08 |
| incremental-31 | 40 | 0.67 |
| incremental-32 | 70 | 1.17 |
| incremental-33 | 78 | 1.30 |
| incremental-34 | 56 | 0.93 |
| incremental-35 | 51 | 0.85 |
| incremental-36 | 60 | 1.00 |
| incremental-37 | 55 | 0.92 |
| incremental-38 | 73 | 1.22 |
| incremental-39 | 37 | 0.62 |
| incremental-40 | 62 | 1.03 |
| incremental-41 | 65 | 1.08 |
| incremental-42 | 61 | 1.02 |
| incremental-43 | 64 | 1.07 |
| incremental-44 | 66 | 1.10 |
| incremental-45 | 37 | 0.62 |
| incremental-47 | 51 | 0.85 |
| incremental-48 | 63 | 1.05 |
| incremental-49 | 48 | 0.80 |
| incremental-50 | 59 | 0.98 |

## 增量节点02：465 个，7.75 个/分钟

| Worker | 完成数 | 个/分钟 |
| --- | ---: | ---: |
| incremental-1 | 34 | 0.57 |
| incremental-2 | 73 | 1.22 |
| incremental-3 | 62 | 1.03 |
| incremental-4 | 44 | 0.73 |
| incremental-6 | 78 | 1.30 |
| incremental-7 | 61 | 1.02 |
| incremental-8 | 54 | 0.90 |
| incremental-9 | 59 | 0.98 |

其他未启用远端注册 worker 共 31 个（含备用节点 20 个），本地 worker 20 个，本窗口均为 0 个/分钟；完整明细见同名 CSV。

只读查询与原始数据：runtime/incremental-worker-60m-20260924-0153/count.mjs、counts.jsonl。查询使用 REPEATABLE READ READ ONLY 事务和 8 秒单条超时，无生产修改。
