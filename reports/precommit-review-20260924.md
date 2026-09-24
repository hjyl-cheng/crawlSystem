# 2026-09-24 工作区检查与提交验证

范围：相对 `570328b93e24f3f2d359be30467ae3966330b83b` 的当前源码、测试、部署覆盖文件、数据库维护脚本和相关方案/报告。覆盖节点接单容量、增量契约、后台有界恢复、中心事务与静止确认、预算终态和网络诊断。

## Standards

静态审查依据 CONTEXT.md、.gitignore、scripts/verify.sh 和相关模块既有契约。未发现需要阻止提交的标准违规。敏感内容扫描命中均核对为隔离测试的固定凭据或测试 URL；运行目录、私有配置及本地频道输入不进入提交。

## Spec

发现并修复一项 P2：历史预算补偿脚本只有 LIMIT，没有方案要求的分页游标；首批长期 deferred/not_exhausted 时，后续记录可能一直得不到处理。

修复增加限定时间窗口的 `(started_at,run_id)` keyset 游标，每页输出 `next_cursor`。即使记录被延后也能继续后页；一次扫描结束后，从无游标的新一轮重新检查延后记录。时间戳保留 PostgreSQL 微秒，Run ID 使用真实 `incremental:<uuid>` TEXT 类型。复核时发现初版游标错误使用 UUID 类型，已更正并由真实数据库回归验证。

脚本仍默认为 dry-run。续页时保持相同的 `--since`、`--until` 和执行模式，将输出值通过 `--cursor <next_cursor>` 传入；`next_cursor=null` 表示当前扫描结束。确认后执行 apply 必须从第一页开始，不能把 dry-run 末页游标直接带入 apply。晚到或游标之前新出现的候选由下一轮重新扫描覆盖。

本次新增分页修复仅写入源码并验证，没有执行生产补偿或发布。

## 本次验证

| 检查 | 结果 |
|---|---|
| scripts/verify.sh、git diff --check | 通过 |
| QYBullMQ 相关 JavaScript 单元测试 | 158 通过，0 失败，0 跳过 |
| Dashboard 回归 | 103 通过，0 失败，12 项环境条件跳过 |
| feature-engine 增量域事件 | 50 通过 |
| Python CONNECT/TLS 诊断 | 4 通过 |
| 独立 PostgreSQL/Redis/NATS/PgBouncer 集成 | 19 通过，0 失败，0 跳过 |

本次合计 334 项通过，12 项跳过。集成覆盖预算终态重投/回滚/Agent 与 API 保护、55 Plan 事务隔离、直连与事务池连接设置、连接池观测接口、节点容量，以及补偿游标跨页、同毫秒微秒顺序、上一页记录消失和窗口错配拒绝。

首次使用系统 Python 缺少依赖，已改为现有 `.venv`。受限环境中的本地 HTTP 测试未正常完成，允许临时 loopback 后回归通过。首次合并集成发现新增测试夹具未清理导致下一测试建表失败，已将该夹具封装在事务内回滚，完整重跑通过。没有隐藏首次失败，也没有将其计为最终通过。

Rota Go 和其他历史完整回归结果见对应发布报告；本次没有重新运行全仓全部测试。既有发布报告记录的 auth 模块历史失败不在本次修改范围，不宣称全仓全绿。

详细日志仅保存在忽略目录 `runtime/precommit-20260924/`。提交排除 `bug0923.txt`、`channel_ids_main_table.txt`、`channelid_brazil.sql`、`db_repaire.txt` 四个本地输入文件。
