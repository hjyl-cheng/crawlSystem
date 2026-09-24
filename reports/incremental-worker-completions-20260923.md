# 增量 worker 最近四小时完成量

统计窗口：2026-09-23 04:59:10.331 ≤ UTC 时间 < 08:59:10.331；北京时间 12:59:10.331–16:59:10.331。

口径：crawler.task_events 中 youtube-channel-incremental 队列的 completed 事件，按 job_id 去重，以完成事件的 remote_node_id + remote_slot 归属 worker。共 13,418 条事件、13,418 个任务，duplicate=true 为 0。该指标为队列任务完成量，不代表新增视频条数或全部业务域成功的 Plan 数。

其中 13,367 个任务在本次完成执行中记录 session_opened=true；9 个为 false，42 个未带该字段。不能把这些字段缺失任务直接判断为未抓取。

窗口内包含节点启用、维护暂停和服务恢复，不能直接据此比较 worker 的单位在线时间效率。

## 增量主节点（node01）：47 个 worker，完成 11,742 个

| Worker | 完成任务数 |
|---|---:|
| incremental-3 | 210 |
| incremental-4 | 355 |
| incremental-5 | 270 |
| incremental-6 | 263 |
| incremental-7 | 275 |
| incremental-8 | 234 |
| incremental-9 | 256 |
| incremental-10 | 193 |
| incremental-11 | 282 |
| incremental-12 | 258 |
| incremental-13 | 241 |
| incremental-14 | 229 |
| incremental-15 | 338 |
| incremental-16 | 286 |
| incremental-17 | 266 |
| incremental-18 | 199 |
| incremental-19 | 255 |
| incremental-20 | 194 |
| incremental-21 | 248 |
| incremental-22 | 345 |
| incremental-23 | 250 |
| incremental-24 | 207 |
| incremental-25 | 206 |
| incremental-26 | 271 |
| incremental-27 | 224 |
| incremental-28 | 247 |
| incremental-29 | 269 |
| incremental-30 | 253 |
| incremental-31 | 241 |
| incremental-32 | 179 |
| incremental-33 | 260 |
| incremental-34 | 304 |
| incremental-35 | 210 |
| incremental-36 | 275 |
| incremental-37 | 260 |
| incremental-38 | 143 |
| incremental-39 | 211 |
| incremental-40 | 218 |
| incremental-41 | 247 |
| incremental-42 | 370 |
| incremental-43 | 231 |
| incremental-44 | 206 |
| incremental-45 | 240 |
| incremental-47 | 298 |
| incremental-48 | 213 |
| incremental-49 | 180 |
| incremental-50 | 332 |

## 增量节点02：8 个 worker，完成 1,676 个

| Worker | 完成任务数 |
|---|---:|
| incremental-1 | 179 |
| incremental-2 | 75 |
| incremental-3 | 209 |
| incremental-4 | 228 |
| incremental-6 | 152 |
| incremental-7 | 268 |
| incremental-8 | 277 |
| incremental-9 | 288 |

当前启用接单的 55 个远端增量 worker 均有完成记录；备用节点 20 个、未启用的历史槽位及本地 20 个 worker 在此窗口内完成数均为 0。完整注册信息和零值见同名 CSV。

证据：runtime/worker-counts-20260923/counts.jsonl；查询：同目录 count.mjs。使用直连 PostgreSQL 的只读事务，单条查询超时 8 秒，无生产修改。
