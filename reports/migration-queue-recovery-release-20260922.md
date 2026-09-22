# 批量迁移队列恢复修复发布

已发布版本：`4a74edc`，分支 `codex/migration-queue-recovery-20260922`。

API 和 Controller 镜像：`qy-allpachong/qybullmq:pachongsys-4a74edc-migration-queue-20260922`。

## 故障与修复

使用旧 Controller 的实际 setPaused 函数和独立 Redis 复现：旧循环读到 stopped，新批次恢复队列并入队，旧循环随后暂停队列，断言失败。生产历史日志也记录了新批次启动后以 query_scheduler_stopped 暂停队列的操作。

迁移队列现在由独立循环每 2 秒核对。批次创建、恢复、暂停或结束先提交原有持久批次状态，再通知该循环；API 不再直接改变队列开关。API 不持有代理容量凭据，所有容量判断在 Controller 进行。

独立循环在调度器行锁保护下读取当前批次及其版本，再计算整条处理链所需队列。旧主循环和恢复后台任务不能绕过该入口改写受控迁移队列。新增 PostgreSQL 序列生成不可回滚的队列控制版本，并在 Redis 原子执行 BullMQ 暂停/恢复脚本时拒绝旧版本，防止失联进程的迟到命令覆盖新操作。

采集、Agent、Finalize 自动协调；Data API 按启用策略和任务需求运行。保留人工暂停、代理容量限制、详情积压限制、批次收尾与历史恢复规则。未改变远程节点 Worker 数量、维护暂停、增量接单或金丝雀队列。

批次提交后的通知失败返回 pending，不撤销批次或诱导重复创建；周期循环负责补偿，进程重启后重新核对原有状态。应用结果保存在 `crawler.settings.migration_queue_control`，迁移批次 API 返回批次版本、控制版本、各队列状态及原因和应用时间。

## 验证

- 干净发布工作树完整采集套件：1811 通过，278 按环境跳过，0 失败。
- 真实 PostgreSQL/Redis 测试：旧决策竞争、同批次暂停恢复、人工暂停优先、失联命令、部分应用失败补偿、主循环阻塞、优先级与延迟任务、API 任务消费均通过。
- 既有真实迁移控制测试和 40 万频道库存回归通过。
- 最终实际 Node 20 发布镜像专项：13 通过，0 失败，0 跳过。
- `scripts/verify.sh` 通过。

原工作区首次完整测试出现历史 runtime 脚本违反事务只读规范、系统 Python 缺少 aiohttp/yt_dlp 的环境问题。干净发布工作树配合既有 Python 虚拟环境运行完整套件通过；未修改这些历史脚本或将其纳入发布。

## 发布与验收（UTC）

- 06:09:53：验证生产数据库身份后，仅新增队列控制序列，保存受限回退配置。
- 06:10:48：旧 Controller 正常退出，退出码 0。
- 06:11:07：旧 API 正常退出，退出码 0；新 API 完成替换并通过健康检查。
- 06:12:43：新 Controller 应用当前批次控制版本 1。
- 06:13:57：实际迁移 API 返回 HTTP 200、控制版本 38 和最新应用时间，证明独立循环持续协调。
- 06:14:55：API、Controller 均运行新镜像，重启次数 0；环境、资源配置与受限持久化重建文件一致。
- 06:14:56：全量 10/10 连接、允许和就绪，其中 9 个执行、1 个空闲；增量三个节点分别 15/15、20/20、47/47 就绪。
- 06:15:25：频道采集队列未暂停，10 个活动 Job；Agent、Finalize 均未暂停。批次待启动数从发布前 51 降至 35，Agent 完成计数从 57 增至 58。观察期间无 migration_queue_control_failed。

该批次仍在执行，未宣称整批迁移已完成。最终采样为 35 pending、17 started、31 dormant、9 success、3 rejected、5 failed。5 个失败在发布前已存在且此次未增加；它们是业务结果，不应与队列控制恢复混为一谈。本次未清空队列、重建频道任务或重试这些失败。

## 回退与证据

旧容器已停止，自动重启关闭：

- `qy-newcrawler-fresh-qybullmq-api-1-before-queue-recovery-20260922`
- `qy-newcrawler-fresh-controller-fullcrawl-canary-1-before-queue-recovery-20260922`

新容器通过 `qy.runtime.create_spec` 指向其持久重建配置。配置及原容器快照位于 `runtime/migration-queue-recovery-20260922/private/`，权限受限，不纳入 Git。该目录外的运行证据包括复现日志、测试日志、镜像构建日志、发布前后批次快照及 `release-verified.json`。

回退须先停止新 Controller，避免新旧队列控制同时写入。新增序列可保留，不需要删除业务数据；旧版仍存在旧决策误暂停风险。

隔离发布工作树 `/tmp/pachongsys-migration-queue-recovery-20260922` 干净。原工作区既有修改完整保留，本次源码修复也同步保留供审阅。
