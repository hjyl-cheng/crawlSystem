# 迁移吞吐修复发布记录

修复慢速 Finalize 恢复占用串行 Controller、导致 40 个抓取 Worker 间歇断粮的问题。补队列、API、结算、Finalize、统计已拆为独立循环；同一 Run / 数据 generation 的已分发任务跟踪已有 Job。

状态：代码与服务发布完成，已观测至 2026-09-10 11:10 UTC。整体发布观察约 62 分钟，其中最终恢复优先级/容量版本运行约 38 分钟；不是同一最终版本的整小时稳态测试。

## 最终运行版本

| 角色 | 镜像版本 | 行为 |
| --- | --- | --- |
| Controller | `pachongsys-292211a` | 独立循环、持久游标、变化记录、恢复容量与优先级 |
| API | `pachongsys-b83be07` | 从索引采样读取进度，区分抓取和结算 |
| Dashboard | `pachongsys-acbc650` | 15/60 分钟指标、样本不足及过期提示 |
| 40 个抓取 Worker、Agent、Finalize Worker | 保留原镜像与数量 | Finalize 核心契约哈希已与新源码核对一致 |

Controller/API 延续原有独立容器部署方式，运行版本以本表和容器配置为准；原 runtime.env 通用镜像 pin 本来就与部分独立角色不同，本次没有为升级 API 而统一更换 40 个 Worker。

Controller 的 `CONTROLLER_THROUGHPUT_ENABLED` 和 `FINALIZE_CHANGE_RECOVERY_ENABLED` 均为 true。控制器交接顺序是先停止旧进程再启动新进程，四次交接的旧 Controller 都以 exit 0 结束。

目标数据库 `newcrawler_crawler` 的兼容迁移成功，库存表前后均为 401325 行。原运行批次 `migration-19eff406-4ec3-442e-a448-882b14caae69` 的总量为 376998、控制版本为 7，未重新创建批次。

## 数据与调度约束

- 事务触发器覆盖 channels、channel_runs、content_candidates、contents、agent_profiles、crawl_observations；回滚时恢复意图一并回滚，纯 Finalize 写回不自触发。
- 未齐数据只结束本次检查，不把 Run 标为业务完成；后续数据变化会重新登记。上线后新建且 detail_status 非 done 的 Run，抽查 publication_finalized_at 非空数量为 0。
- 已分发同版本跟踪已有 Job，数据变化或 Job 失败/丢失才重新评估；最终写入仍使用原源版本保护。
- 历史审计先取最多 200 个频道再筛选，超时减半页面并重试原游标；失败不跳过数据。首个全局轮次于 10:44:35 UTC 完成并进入一小时休息期，该轮包含发布交接及冷缓存阶段。
- 补漏发现 waiting + prioritized 达到 200 时停止领取/派发，保留 PostgreSQL 意图；正常流水线直接收尾不受该恢复上限限制。每轮最多 40 项，因此该阈值不是对所有生产者总排队数的硬上限。
- Controller 补漏使用 priority 100，正常流水线保留 priority 0。10:32 UTC 将本次已排队的 2717 个补漏 Job 调整到相同优先级，未删除或重建任务；交接后复核无遗漏。
- 已结束批次保留最终采样，活动批次每 30 秒采样；页面请求不重算大清单。

## 已完成验证

- `./scripts/test.sh` 和 `./scripts/verify.sh` 通过；最终受影响 Node 全集 1863 项中 1680 通过、183 因外部依赖未配置跳过、0 失败。
- 专用 PostgreSQL / Redis 验证与最终镜像 smoke：恢复 11 项，Controller / 统计 5 项，源版本保护 5 项，Dashboard 6 项通过；这些定向集成没有跳过。
- 额外 Agent / 增量 / 发布兼容测试 13 项中 12 项通过。增量视频测试期待抓取 uploads-dated-video 的断言失败，旧生产镜像同样重现；该既有问题未计为通过，本次未替换增量 Worker。
- 40 并发消费者在 pg_sleep(120) 尚未结束时持续接纳 250 个冻结频道，并验证 pause / resume；取得结果后主动取消阻塞查询。它证明隔离和接纳屏障，不等于持续运行了 120 秒。
- 既有 40 万冻结清单测试通过：首次 reconcile 594 毫秒；stop 24.7 秒，大批次 stop 仍有一次性清理成本。
- 实际 API 进度请求约 145 毫秒，Dashboard 转发约 316 毫秒；公开队列路径返回正常登录重定向 302，未绕过认证。
- 最近一次 10 分钟线上统计：补队列 P95 253 毫秒、API 检查 P95 108 毫秒；结算出现 2 次锁超时，后续重试成功。整页审计周期 P95 2242 毫秒（包括恢复意图写入），不能与纯查询基准混淆。

专用测试容器、匿名数据卷和测试网络已清理；生产回退容器保留。

## 基准的适用范围

[完整 CSV](benchmarks/finalize-query-20260910.csv)。测试 PostgreSQL 18.4，2 CPU / 2 GiB；每个已完成频道各有 3 个候选和 3 个内容，启用捕获触发器；每规模抽取 10 页。

| 合成频道数 | 旧全局筛选 | 新 200 频道单页 P95 |
| --- | --- | --- |
| 5 万 | 3920 ms | 50 ms |
| 10 万 | 6656 ms | 76 ms |
| 40 万 | 48125 ms | 227 ms |

旧查询和新单页工作量不同，不能称作“全库查询从 48 秒变成 227 毫秒”。旧查询从大量频道中找遗漏，每个频道内部检查自己的 Run、候选、内容和 Agent；不是每个频道重新扫描其他频道全部视频。新正常路径按变化触发，分页审计只负责历史回填和异常补漏。

## 观测与剩余限制

[完整聚合采样 CSV](benchmarks/migration-throughput-20260910.csv)；[约 2 秒供给采样 CSV](benchmarks/migration-supply-20260910.csv)。11:10 UTC 的页面数据如下：

| 指标 | 最近 60 分钟 | 最近 15 分钟 |
| --- | --- | --- |
| 批次最终结算频道 | 2937 | 1162 |
| 结算 / 有效小时 | 2910 | 4648 |
| 详情抓取完成频道 | 2941 | 713 |
| 抓取 / 有效小时 | 2914 | 2852 |

60 分钟样本的实际有效间隔为 3633 秒，因 30 秒采样对齐略超一小时；小时速率按实际有效时间计算。早期窗口有镜像切换、冷缓存和历史积压释放，短窗结算高于抓取不代表稳态吞吐可无限提高。此前 06/07/08 UTC 完整小时终态量分别为 1618/1352/1489，仅作同批次历史参考，不能当作相同样本的严格对照。

10:52:41–11:10:01 的高频供给验证共 517 个样本，活跃抓取任务最少 39、最多 40，全体空闲样本为 0，最大采样间隔 2.107 秒。此前 30 秒级连续记录也未见全体空闲，但不能用较粗采样排除更短的停顿。

最终三个发布角色重启次数均为 0，API/Dashboard 健康；原批次仍 running、version=7。未齐详情的新 Run 被误标 Finalize 完成的复核数量仍为 0。最终 Finalize 队列普通等待 47、较低优先级补漏 2364、active=1、failed=0；补漏仍在消化，未声称历史积压全部清空。两项只读观测进程均正常退出。

API 原有的每日上限与 next_retry_at 没有被绕过。365 个早期待补任务的最早允许重试时间为 2026-09-11 00:05 UTC，其中一项明确记录 daily_request_limit_reached / 每日上限 500，其余任务的原因字段为空；不能把每一项都描述为已独立核实配额原因。

## 回退

保留以下停止的原生产容器，可用原配置和精确旧镜像恢复：

- `qy-newcrawler-fresh-controller-fullcrawl-canary-1-before-throughput`
- `qy-newcrawler-fresh-qybullmq-api-1-before-throughput`
- `qy-newcrawler-fresh-dashboard-1-before-throughput`

同时保留 `-before-finalize-events`、`-before-metrics-cache`、`-before-recovery-capacity` 三个 Controller 中间版本。回退必须先停止当前 Controller，再恢复选定实例，确保只有一个控制器；不删除兼容表、generation、游标或业务数据。回退到原版本会重新暴露原串行慢查询问题。
