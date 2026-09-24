# 后台补偿扫描收缩发布记录（2026-09-23）

## 实施与发布

已将自动发布补偿和终态 Run 对账改为持久游标分页，并于 **2026-09-23 13:27:38 UTC** 替换生产中心控制器。旧控制器收到 SIGTERM 后排空约 150 秒，正常退出（exit 0），保留为回退容器。执行节点无需此次替换。

- 提交：`3a6eb67f3f6d19f8d4372c85d86fd503762b0635`，隔离分支 `perf/background-reconciliation-20260923`。
- 镜像：`qy-allpachong/qybullmq:pachongsys-3a6eb67-bounded-reconciliation`。
- 镜像 ID：`sha256:5de3b4b398738d62effc20f467fb95d0da972dff0d26bdd2d4be9d446858e3cc`；OCI revision 与提交一致。
- 生产容器：`qy-newcrawler-fresh-controller-fullcrawl-canary-1`。
- 仅新增小表 `crawler.background_reconciliation_scans`，未新增业务热表索引或触发器。
- 镜像来自隔离工作树的已提交代码，没有包含主工作区其他未提交修改。

| 路径 | 归属 | 新的扫描边界与节奏 |
| --- | --- | --- |
| 自动发布后台补偿 | 中心控制器 | 按主键每页最多 200 频道，轻量预筛后最多处理 25 个；默认每 2 秒一页，完整一轮后暂停 60 秒 |
| 全局终态 Run 对账 | 中心控制器 | 每页最多 100 个当前频道/Run，默认每 2 秒一页；完整一轮后暂停 1 小时 |
| 当前周期 Run 对账 | 中心控制器 | 有 dispatch batch 时限定该批次候选，分批审计；必须完成一轮后才继续原有周期完成判断 |

发布候选预筛已去掉全量 delivery/owner 路由关联。实际发布事务仍检查在线路由、所有权、抓取边界及完整 Initial Package。正常 Finalize 的即时收敛路径保留。

游标持久化，业务事务成功后原子推进；固定每轮上界保证持续插入时仍能结束一轮。超过处理预算的候选留给下一页。出错不跨过失败候选，之前已提交进度保留；锁与租约 token 阻止过期扫描者提交。历史数据仍逐轮覆盖，没有简单按最近几天截断。

## 验证

- 最终版本新增 PostgreSQL 集成测试及发布单测：**25/25 通过**，覆盖并发、租约过期、回滚、重启、固定上界、游标后新增、65 个候选按 25 个分批、故障恢复、当前周期及全局 Run 修复。
- 既有发布 PostgreSQL 集成测试：**1/1 通过**，验证在线 Delivery 和完整 Initial Package。
- QYBullMQ 全量测试在正确 Python 虚拟环境下：1844 通过、281 跳过、0 失败。最终全量尝试中 2 个测试因 Python 解释器缺 aiohttp/yt_dlp 失败，切换正确虚拟环境后 2/2 补跑通过；最终 SQL/计数变更另经上述 25 项回归。
- dashboard：99 通过；feature-dispatch：7 通过；local-agent：135 通过、4 跳过；feature-engine：214 通过、17 跳过；Rota Go 测试通过。`scripts/verify.sh` 通过。
- 全仓检查仍有既有 auth 失败：`services/auth/test/gatewayConfig.test.js:73`，HTTP/1.1 location 模板与断言不一致，在原工作区也复现；本次未修改该模块，不能称全仓全绿。
- 隔离完整数据库中的源码控制器、实际构建的 Node 20 镜像均冒烟通过，无 tick/cycle 错误，正常退出。

## 查询优化证据

隔离的 2 万历史频道数据集，旧候选 SQL 的 shared block hits 为 201,754，新预筛为 2,411；三轮热缓存下耗时从 418–590 ms 到 5–7 ms。这是候选选择的合成基准：旧查询返回 25 个，新查询预筛返回 199 个，不能当作端到端加速或生产实际读盘降幅。

生产仅执行 EXPLAIN，没有 ANALYZE：新发布预筛限定 200 个频道并使用索引点查，唯一顺序扫描是小型 stream 目录；终态 Run UPDATE 限定 100 个频道并使用主键查找。cost 是规划估算，不是毫秒。

## 上线观察

控制器日志窗口为 13:27:39–13:35:00 UTC：运行中、重启 0 次，`controller_tick_failed` / `controller_work_cycle_failed` 均为 0。

| 补偿路径 | 完成批次 | 累计检查频道 | 每批平均耗时 | P95 | 扫描失败 |
| --- | ---: | ---: | ---: | ---: | ---: |
| 自动发布补偿 | 221 | 44,200 | 458.57 ms | 584 ms | 0 |
| 全局 Run 对账 | 221 | 22,100 | 157.93 ms | 237 ms | 0 |

上述批次尚未命中实际发布候选或需修复的 Run，耗时代表扫描开销，不代表每次发布事务成本。两条全局游标持续前进，但 completed_rounds 仍为 0。当前周期的小范围审计已完成 29 轮，不能据此替代全局历史覆盖。

最后一次检查（13:34:58 UTC）之前 5 分钟内，Plan 成功 252 个、取消 1 个；增量队列 completed 事件 259 个。正常 Finalize 变更路径在日志窗口内派发 595 次。队列同时有 40 条 failed 事件（33 个不同 job），初始快照也有 36 条 failed（34 个不同 job）；这是事件计数，包含重试，既不是最终失败率，也不能宣称业务端零失败。本次确认的是补偿循环无异常、增量成功产出持续存在。

负载采样使用数据库只读统计和容器 cgroup。上线前窗口为 13:17:25–13:18:26 UTC（约 61 秒）；上线后数据库窗口为 13:28:39–13:33:43 UTC（约 303 秒、31 个样本），容器窗口约 300 秒，起止相差数秒。

| 指标 | 上线前 | 上线后 |
| --- | ---: | ---: |
| PostgreSQL 容器实际块设备读取 | 47.48 MiB/s | 29.67 MiB/s |
| PostgreSQL 容器实际块设备写入 | 5.73 MiB/s | 5.79 MiB/s |
| PostgreSQL shared buffer 未命中读取块折合速率 | 46.92 MiB/s | 62.89 MiB/s |
| PostgreSQL shared buffer 命中率 | 93.19% | 90.01% |
| PostgreSQL 容器 CPU | 2.47 核 | 2.39 核 |
| 主机 iowait | 8.62% | 6.96% |

实际读盘在观察窗口下降，但 PG 缓存未命中读取上升，不能合并成“全库读取下降”的结论。PG 块读取可能由操作系统缓存满足，和容器实际读盘不是同一指标；容器统计还覆盖该实例的其他数据库/后台活动。前后窗口时长、任务组成、缓存状态不同，且前窗口可能受镜像构建影响，不是受控 A/B，不能归因出本改动的确定百分比收益。

上线后无新增数据库死锁；活动采样未捕获旧的无界自动发布候选 SQL。仍有 Finalize DataFileRead 和增量 tasks 事务锁等待。表级缓存未命中读取主要见于 content_candidates（14.78 MiB/s）、channel_execution_attempts（11.86）、tasks（11.15）及 contents（5.99），这些包含多种调用者，不能全部归给补偿。

## 覆盖与后续限制

本次只完成短窗口上线验证，尚未完成全局历史扫描一整轮；不能承诺历史积压已清空或长期吞吐提高。游标之后才发生的新变化由下一轮覆盖，未实现专用事件驱动发布请求表。持续硬错误会阻止该页前进，应通过 failed、游标更新时间和 completed_rounds 监控并修复；不能静默跳过。

Finalize 恢复仍会读 content_candidates/contents，增量任务状态与心跳仍有锁竞争。若需进一步降低全库读取，应分别量化这些路径；此次改动不取消业务执行权、所有权或完整性检查。

## 回退与证据

保留的旧镜像为 `qy-allpachong/qybullmq:pachongsys-b862901-migration-idle-20260922`；旧容器为 `qy-newcrawler-fresh-controller-fullcrawl-canary-1-before-bounded-reconciliation-20260923`。在仓库根目录执行已准备的脚本可恢复原容器及重启策略：

```sh
python3 runtime/background-reconciliation-release-20260923/controller-release.py rollback
```

本次按原容器配置替换运行实例；后续通过 Compose/其他编排重新创建控制器时，需显式选用上述新镜像，避免恢复旧版本。

回退保留新增游标表。Docker 私有配置快照在证据目录的 private 子目录（0700，文件 0600），含运行配置，不应公开或提交。

实现说明：[BOUNDED_BACKGROUND_RECONCILIATION_20260923.md](../docs/BOUNDED_BACKGROUND_RECONCILIATION_20260923.md)。发布、查询计划、基准、测试与采样证据位于 `runtime/background-reconciliation-release-20260923/`；关键文件包括 deployment.jsonl、schema-apply.json、controller-summary.json、before-summary.json、after-summary.json、progress-final.jsonl 及 tests-final-integration.log。
