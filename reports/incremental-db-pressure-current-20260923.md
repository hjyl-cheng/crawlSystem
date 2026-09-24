# 当前增量抓取数据库读写压力

2026-09-23 11:42:16–11:43:17 UTC（北京时间 19:42–19:43），直连采集数据库，只读采样 7 次、约 61 秒，并采集容器 cgroup 和宿主机 iostat。没有更改生产配置或业务数据。

结论：当前共享采集数据库读取压力明显，增量任务存在行锁竞争；写盘负载相对小，尚无写入饱和证据。不能将全库压力全部归因于增量，也不能将数据库容器的全部实际读盘归因于某一张表。

| 指标 | 实测 | 解释 |
| --- | ---: | --- |
| PostgreSQL shared buffer 未命中读取块折合 | 197.35 MiB/s | 包含由 OS 缓存满足的读取，不是物理磁盘吞吐 |
| 窗口 shared buffer 命中率 | 82.71% | 本窗口有明显缓存外读取 |
| 采集 PostgreSQL 容器实际块设备读取 | 75.93 MiB/s，6,229 次/s | cgroup I/O 计数差分 |
| 同容器实际块设备写入 | 7.81 MiB/s，613 次/s | 约为读取字节量的十分之一；不能据此排除短时 WAL/提交等待 |
| 数据库行写入统计 | 插入 198/s、更新 159/s、删除 42/s | 全库口径，不是增量业务独占 |
| PostgreSQL CPU | 平均 2.70 核 | 不等于整个 16 核宿主机耗尽 |
| PostgreSQL I/O pressure | 末次 some avg60=11.77%、full avg60=10.43% | 容器任务存在实际 I/O 阻塞 |
| 宿主机 iowait | cgroup 观察同期平均 9.10% | 全主机口径，含其他业务；iostat 窗口略有偏移 |
| 数据库连接 | 39–42 | 未采集 PgBouncer 排队，不能推断连接池完全无等待 |
| 临时文件新增 | 294.72 MiB | 存在临时文件落盘，未完成 SQL 归因 |
| 新增死锁 | 0 | 不代表无普通行锁等待 |

7 次活动查询快照共捕获 11 个 DataFileRead 会话样本、7 个 transactionid 锁等待样本。这些是离散样本数量，不是独立请求数或等待占比。所有锁等待样本都在 remote-node-center-gateway 的 remote_ingestion.tasks SELECT FOR UPDATE；被捕获语句已经运行最长约 1.96 秒，该时间含语句执行，不是精确锁等待时长。

当前读取热点（表、索引、TOAST 读取块折合，非物理读盘独立归因）：

| 表 | MiB/s |
| --- | ---: |
| crawler.channel_runs | 65.18 |
| remote_ingestion.tasks | 27.05 |
| crawler.channel_candidates | 18.16 |
| crawler.channel_execution_attempts | 17.32 |
| publication.channel_stream_state | 16.75 |
| crawler.channels | 14.71 |
| crawler.content_candidates | 13.80 |
| crawler.finalized_profiles | 10.64 |

增量自身可见负担包括：remote_ingestion.tasks 约 28.07 次更新/s、channel_runs 约 12.83 次更新/s、incremental_youtubejs_video_items 约 10.39 次更新/s，以及心跳、绑定和租约状态更新。tasks 同时支持多种远端任务，channel_runs 为共享业务表，不能把这些速率全部当成增量独占，也不能直接除以新任务数得到单任务成本。

共享后台负载同样明显：首个快照的 publication-auto-onboarding:backlog 查询已运行约 233 秒；最后快照的 publication-reconciler-v1 状态收敛 UPDATE 已运行约 43 秒。Finalize recovery 也被捕获在 DataFileRead 等待中。查询耗时说明慢路径存在，但尚未用执行计划或 SQL 累计统计证明各自读取贡献。

优化建议：

1. 优先检查自动发布补偿、Run 状态收敛及 Finalize 恢复查询的扫描范围、索引适用性和频率，减少它们与增量争用缓存及 I/O。
2. 对增量 tasks 行锁分解持锁区间，减少重复查询、无必要续约/更新及事务往返，保留 lease/fence 和原子提交保证。
3. 维持已经配置的 outbox 自动清理。本次 crawler_outbox 读取块折合仅约 0.058 MiB/s，autovacuum_count=3，最近一次自动清理为 10:47:53 UTC；早间自动清理尚未运行的记录已经过时，当前也不应再把该表列为主要读取来源。
4. 当前证据不支持优先扩 Worker 或提高数据库连接数，也不足以要求更换磁盘。宿主机 vda 本次 util 63.7–65.8%、读 await 0.33–0.44 ms、写 await 0.63–1.02 ms；这些指标不能独立证明磁盘达到吞吐极限。

以上是约一分钟的当前快照，足以确认读取和竞争值得优化，不能替代至少 30 分钟、多时段的容量基线。上一轮测得中心主导阶段 23.58 秒还包含节点领取和停止确认等等待，不能全部算作数据库耗时。

只读脚本与原始结果：runtime/incremental-db-pressure-current-20260923/ 下的 probe.mjs、db.jsonl、host.py、host.jsonl、iostat.txt、summarize.py、summary.json。查询连接明确使用 crawler-postgres:5432；每条查询使用 BEGIN READ ONLY、SET LOCAL statement_timeout=8s、lock_timeout=1s。没有注入运行中主进程或使用会话级只读设置。
