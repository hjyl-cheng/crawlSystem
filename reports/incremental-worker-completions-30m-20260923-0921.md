# 增量 worker 最近半小时完成量

统计窗口：2026-09-23T08:51:39.776Z ≤ UTC 时间 < 2026-09-23T09:21:39.776Z。北京时间 2026-09-23 16:51:39.776–17:21:39.776。

按 youtube-channel-incremental 的 completed 事件统计，job_id 去重，再按完成事件所属 node_id/slot 归属。合计 2,319 个；本窗口原始 completed 事件数量与去重数一致，duplicate=true 为 0，全部记录 session_opened=true。该指标不是新增视频数，也不保证全部业务域 complete。

## 增量主节点 node01：2,035 个

| Worker | 完成任务数 |
| --- | ---: |
| incremental-3 | 50 |
| incremental-4 | 49 |
| incremental-5 | 43 |
| incremental-6 | 52 |
| incremental-7 | 54 |
| incremental-8 | 32 |
| incremental-9 | 51 |
| incremental-10 | 51 |
| incremental-11 | 27 |
| incremental-12 | 38 |
| incremental-13 | 37 |
| incremental-14 | 57 |
| incremental-15 | 54 |
| incremental-16 | 48 |
| incremental-17 | 28 |
| incremental-18 | 43 |
| incremental-19 | 42 |
| incremental-20 | 51 |
| incremental-21 | 31 |
| incremental-22 | 58 |
| incremental-23 | 42 |
| incremental-24 | 43 |
| incremental-25 | 39 |
| incremental-26 | 47 |
| incremental-27 | 31 |
| incremental-28 | 36 |
| incremental-29 | 48 |
| incremental-30 | 57 |
| incremental-31 | 33 |
| incremental-32 | 51 |
| incremental-33 | 40 |
| incremental-34 | 54 |
| incremental-35 | 50 |
| incremental-36 | 47 |
| incremental-37 | 52 |
| incremental-38 | 0 |
| incremental-39 | 43 |
| incremental-40 | 47 |
| incremental-41 | 29 |
| incremental-42 | 57 |
| incremental-43 | 42 |
| incremental-44 | 40 |
| incremental-45 | 43 |
| incremental-47 | 31 |
| incremental-48 | 41 |
| incremental-49 | 43 |
| incremental-50 | 53 |

## 增量节点02：284 个

| Worker | 完成任务数 |
| --- | ---: |
| incremental-1 | 29 |
| incremental-2 | 0 |
| incremental-3 | 40 |
| incremental-4 | 52 |
| incremental-6 | 0 |
| incremental-7 | 55 |
| incremental-8 | 57 |
| incremental-9 | 51 |

## 零完成与其他注册 worker

当前启用的 55 个远端增量 worker 中，52 个有完成记录；以下 3 个为零：

- 增量主节点 node01 / incremental-38：0，当前在线且允许接单。
- 增量节点02 / incremental-2：0，当前在线且允许接单。
- 增量节点02 / incremental-6：0，当前在线且允许接单。

其余未启用远端注册槽位共 31 个（含备用节点 20 个），以及本地增量 worker 20 个，本窗口完成量均为 0；完整逐 worker 状态见同名 CSV。

证据及只读查询：`runtime/incremental-worker-30m-20260923-0921/counts.jsonl`、`count.mjs`。查询使用 PostgreSQL REPEATABLE READ READ ONLY 事务，单条超时 8 秒，无生产修改。
