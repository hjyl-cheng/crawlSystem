# Rota / BullMQ Known Bugs and Controlled Recovery

记录日期：2026-08-26

冻结源码基线：`agent/rota-fix`，`a8e07f1bd51f766d96c173590940c213c36efc1d`

实现工作树：`agent/rota-fix` 基于上述提交的未提交改动，尚无实现 commit 或镜像 digest。

文档状态：故障、解决方案和实施边界冻结基线。本文同时保留 `a8e07f1` 的原始故障行为，
并记录 2026-08-26 当前工作树中的实现。工作树实现不等于已提交、已构建、已部署或已完成
数据恢复，本文不能作为部署证明。

适用范围：

- `services/qybullmq` 的 Channel Worker、BullMQ 调度与 Business Run Binding；
- `services/rota/core/internal/proxycontrol` 的 Lease、Task、Observation、Completion 和
  Reconcile；
- `legacy-results-manual-v1` 频道迁移批次；
- Fingerprint Gateway 管理的 YouTube 网络请求。

安全边界：本文只允许在独立测试数据库和独立 Redis 中执行复现。修复部署完成前，不得对
当前运行数据库、Rota Task 历史或 Redis 队列执行删除、清空、伪造计数等写操作。

## 1. 冻结结论

当前确认四个相互关联但可以独立触发的缺陷：

| 编号 | 优先级 | 问题 | 冻结基线 / 当前工作树状态 |
| --- | --- | --- | --- |
| BUG-1 | P0 | Execution 预算错误被无限 delayed，Business Run 终态被掩盖 | 基线未修复；工作树已实现并测试，未提交/部署 |
| BUG-2 | P0 | 活动 Task 的 Lease 确定消失后，Worker 不会自动重新 Claim | 基线未修复；工作树已重做并测试，未提交/部署 |
| BUG-3 | P1 | Fingerprint `proxy_transport` 结构化错误被分类为 `unknown` | 基线未修复；工作树已实现并测试，未提交/部署 |
| BUG-4 | P1 | failed Proxy 仍绑定 leased-idle Slot，Lease 永久续期但不换路 | 基线未修复；ADR 已接受，工作树已实现并测试，未提交/部署 |

### 1.1 当前工作树实施记录

当前未提交工作树已完成以下代码边界：

- BUG-1：Worker 三类错误分派、materialized/reserved Business Run 原子终止、failed
  listener 终态门禁、Rota Business Run 预算优先检查；
- BUG-2：只对确定性 `LEASE_GONE` 清理失去服务端权威的 Task/Completion/Runtime，并自动
  Claim 新 Lease；不确定 Completion 会持续 Fence，即使后续 Renew 成功也不能重新开放
  Slot；
- BUG-3：沿受限 cause 链提取结构化 kind/code/source，识别 Fingerprint proxy transport，
  保留普通非 Fingerprint TLS 分类；
- BUG-4：已接受 `docs/adr/0002-health-driven-slot-route-transitions.md`；idle Slot 使用同
  Lease Route Transition，active Task 允许当前 Attempt 收尾，Health Incident 不冒充 Task
  Observation，BeginTask 执行最终 Proxy eligibility 检查；
- 跨问题边界：有界 Execution ID、持久 dispatch generation、Recovery Intent、PostgreSQL
  Outbox、白名单恢复 Job Data 和默认只读的受控 Recovery Intent CLI；Outbox 对
  `queue.add()` 直接返回或模糊成功都校验确定性 Job ID、name 和 data，Controller 周期补偿
  Recovery Intent 遗失的异步终态事件。

当前仍未完成：创建实现 commit、固定和核对运行镜像 digest、构建镜像、部署、生产冒烟、
Dashboard 预算展示，以及任何真实频道恢复。两个卡住频道和 18 个历史 Fingerprint 失败频道
均未被重试或修改。

在以下条件全部满足前，不恢复两个卡住的频道，也不批量重试 18 个 Fingerprint 历史失败
频道：

1. 未物化 Business Run 的预算终止具备原子事务和回归测试；
2. Worker `failed` 监听器不会覆盖预算终态；
3. BUG-2 热修已经进入 Git，并能从固定源码重建出可追溯镜像；
4. Execution 预算不再进入 `moveToDelayed()`；
5. 新恢复调度使用新的 Business Run、Job ID 和 dispatch generation；
6. BUG-3 分类已修复；
7. BUG-4 的 idle Route Transition 和 active Task 协议已经实现并验证。

## 2. 关键术语和预算边界

### 2.1 BullMQ attempt

BullMQ attempt 是一次 Job Processor 执行。普通失败会增加 `attemptsMade`，而
`moveToDelayed()` 不增加 `attemptsMade`。因此 delayed 不能用于表示“当前执行预算已经
耗尽”。

### 2.2 Rota Execution

Rota 使用 `job_execution_id` 把同一次 BullMQ attempt 中的换路 Task 归在一起。当前上限
为：

```text
1 + max_route_switches_per_execution
```

当 `max_route_switches_per_execution=2` 时，一个 Execution 最多创建 3 个 Rota Task。

### 2.3 Rota Business Run

Business Run 是一个业务抓取意图的持久预算和审计边界。当前
`max_network_attempts_per_business_run=9`，即一个 Business Run 最多创建 9 个 Rota
Task，不因 BullMQ Job 重建或容器重启而重置。

### 2.4 Binding

`crawler.business_run_bindings` 将稳定业务意图键映射到 Rota Business Run ID：

- `reserved`：Run ID 已分配，但 `crawler.channel_runs` 尚未物化；
- `materialized`：Channel Run 已生成；
- `terminal`：Binding 已明确终止，不得被普通重新调度复用。

### 2.5 dispatch generation

dispatch generation 表示同一逻辑任务的一次持久调度代次。BullMQ 内部 retry 保持相同
generation；人工恢复或重新创建 Job 必须获得新 generation。它不能只存在于 Redis Job
Data，必须有数据库审计和唯一约束。

## 3. 四个问题的依赖关系

```text
Fingerprint 运行时错误
  -> BUG-3 分类和 source 提取
  -> Worker Observation
  -> Rota Route Transition

Health Check Verdict
  -> BUG-4 持久 Health Incident / eligibility
  -> idle 或 active Route Transition

Lease 确定消失
  -> BUG-2 清理失权威本地状态
  -> Release/Reclaim 或直接 Claim 新 Lease

Execution / Business Run 预算耗尽
  -> BUG-1 正确错误分类
  -> BullMQ attempt 或 Business Run 明确终止
  -> 受控 Recovery Intent 创建新预算边界
```

BUG-3 解决运行时代理故障没有上报的问题；BUG-4 解决健康平面已经判定失效、但控制平面
没有迁移 Slot 的问题。两者不能互相替代。BUG-2 又是服务端 Fence 或 Lease 过期之后
Worker 能否继续工作的前提。

## 4. BUG-1：频道迁移永久卡在 Execution Route Budget Exhausted

优先级：P0

### 4.1 现场对象

记录中有两个频道永久停留在 queued，BullMQ Job 每 5 秒在可执行状态和 delayed 之间
循环：

| Candidate | Channel | Business Run |
| --- | --- | --- |
| `1` | `UC6_vYxOf3POVEnufqugPyPg` | `run:2d7153ac-7e9a-418a-b42c-dee94678dbe5` |
| `4` | `UChHM17akWu34e_viXKtd0XA` | `run:9426e637-bdfc-40a4-882b-f4fe88a63e84` |

Worker 重复输出：

```text
event=rota_job_deferred
reason=execution_route_budget_exhausted
delay_ms=5000
```

现场代理容量充足，问题在 BeginTask 预算检查之前或之内发生，不是代理不足：

```text
active=33
channel-ready=31
cooldown=0
```

### 4.2 数据证据

两个 Business Run 均满足：

```text
next_attempt_number=10
max_network_attempts_per_business_run=9
max_route_switches_per_execution=2
```

每个 Run 已经真实创建 9 个 Task。`:1`、`:2`、`:3` 三个 Execution 各自已经使用 3 个
Task。Binding 仍为 `reserved`，没有对应的 `crawler.channel_runs`。

Crawler 最近记录的错误为 `LEASE_CONFLICT`，Candidate 的 `snapshot_attempts=3`。18 个
已完成 Rota Task 没有 Observation。缺少旧 Worker 日志和 Observation，不能把所有 Task
失败都归因于同一个错误。

### 4.3 当前源码证据

1. `services/qybullmq/src/rotaSlotAdapter.js:66` 的 `jobExecutionId()` 只使用：

   ```text
   queue_name:job_id:(attemptsMade+1)
   ```

   Job 恢复、重建或尝试历史丢失后，`attemptsMade` 可能重新为 0，从而复用旧 `:1`。

2. `services/rota/core/internal/proxycontrol/task.go:126` 先检查 Execution Task 数，再在
   `:129` 检查 Business Run 总预算。因此 Execution 上限掩盖了 9/9 的真实终态。

3. `services/qybullmq/src/rotaSlotAdapter.js:469` 把 Execution 和 Business Run 预算错误都
   包装成 `RotaSlotDeferredError`。

4. `services/qybullmq/src/rotaSlotAdapter.js:348` 的本地 Route Switch 上限也抛出
   `RotaSlotDeferredError("execution_route_budget")`。

5. `services/qybullmq/src/worker.js:1477` 除 Business Run 预算特例外，会把所有
   `RotaSlotDeferredError` 交给 `deferJobForSlotPause()`。该操作不增加 `attemptsMade`。

6. `services/qybullmq/src/businessRunBudgetRecovery.js:40` 只更新 `channel_runs`。当 Run 尚未
   物化时，UPDATE 返回 0 行，`:99` 抛出普通 Error，而不是 `UnrecoverableError`。

7. `services/qybullmq/src/worker.js:1692` 的 `failed` 监听器不识别 Business Run 预算终态。
   当 `attemptsMade < maxAttempts` 时，它会把非 accepted/rejected Candidate 写回 queued。

8. `services/qybullmq/src/proxyBusinessRun.js:154` 对所有带 Candidate 的任务优先使用
   `full-candidate:<candidate_id>`，会挡住未显式优先处理的 Recovery Intent。

9. 现场记录中的 Channel Job 带有 `dispatch_generation=1`，但冻结源码
   `services/qybullmq/src/worker.js:640` 的 `channel-snapshot` 入队 Data 没有该字段。这是除
   BUG-2 热修之外的另一处源码/运行时版本差异，不能把现场字段当成当前 Git 已实现的能力。

### 4.4 三阶段行为必须分开描述

| 阶段 | 实际行为 |
| --- | --- |
| 当前 `a8e07f1` | Execution budget 先返回，Job 进入不消耗 `attemptsMade`、但永久阻塞并持续制造队列和日志负载的 delayed 循环 |
| 只调整 Rota 检查顺序 | Business budget 暴露；未物化 Run 抛普通 Error 并消耗 retries；Candidate 抖动，Binding 保持 reserved |
| 完整 BUG-1 | Candidate、Binding 和可选 Channel Run 原子终止，随后抛 Unrecoverable，`failed` 监听器保持数据库终态 |

第二行是“Business Run 预算错误暴露后”的潜在行为，不是两个频道在当前源码下已经发生的
行为。这个区别锁定了部署顺序：必须先部署能安全处理 Business Run 终态的 Worker，再
调整 Rota 的预算检查顺序。

### 4.5 根因链路

```text
attemptsMade 丢失或重置
  -> jobExecutionId 复用 :1
  -> :1 已使用 3 个 Task
  -> Rota 先返回 Execution budget
  -> Adapter 把终止错误包装为 Deferred
  -> Worker moveToDelayed 5 秒
  -> attemptsMade 不变
  -> 再次复用 :1
```

即使交换 Rota 检查顺序，未物化 Run 仍会进入另一条不完整路径：

```text
Business Run 9/9
  -> BUSINESS_RUN_BUDGET_EXHAUSTED
  -> channel_runs UPDATE 0 行
  -> 普通 Error
  -> BullMQ retries
  -> Candidate 中间状态回到 queued
  -> Binding 一直 reserved
```

### 4.6 永久解决方案

#### 4.6.1 先部署 Worker 兼容逻辑

错误语义必须分成三类。具体类名可以调整，但行为不能合并：

| 类型 | 示例 | BullMQ 行为 |
| --- | --- | --- |
| 暂时不可执行 | `slot_not_ready`、`waiting_capacity`、`no_reserve`、`route_not_ready` | delayed，不增加 `attemptsMade` |
| 当前 Execution 已结束 | 服务端 `EXECUTION_ROUTE_BUDGET*`、本地 Route Switch 上限 | 结束当前 Processor 执行，让 BullMQ 增加 `attemptsMade` |
| Business Run 已终止 | `BUSINESS_RUN_BUDGET*` | 原子写入业务终态后抛 `UnrecoverableError` |

`processJob()` 当前未导出，导入 `worker.js` 还会产生 Worker 初始化副作用。实现时应先把
Rota 错误分派提取为无副作用、可注入依赖的函数，例如接收 `defer`、`terminate` 和
`throwExecutionFailure` 回调。Worker 边界测试必须通过这个真实分派 seam 断言是否调用
`deferJobForSlotPause()`。

本地 Route Switch 上限是在失败 Task 已经 Complete、服务端可能已经开始 Route Rotation
之后触发。Adapter 在结束 BullMQ attempt 前，必须完成以下二选一：等待并接受更高 Route
generation，或把本地 Slot 明确 Fence 为 not ready 并由 Renew 完成同步。不得带着旧
generation 的 `slotReady=true` 直接进入下一次 BullMQ attempt。

#### 4.6.2 原子终止 Business Run

新增或扩展预算终止事务，至少执行以下步骤：

1. 根据 Job Data 中的 `business_run_key`、`run_id` 和 `candidate_id` 定位 Binding；缺少
   `run_id` 时不得直接失败，应在锁内通过稳定 Binding 身份解析；
2. `FOR UPDATE` 锁定 Binding 和 Candidate；存在 Channel Run 时同时锁定；
3. 再次验证 Binding、Candidate、Channel 和 Business Run ID 的对应关系；
4. 将 Binding 标记为 `terminal`，写入
   `terminal_reason=proxy_control_business_run_budget_exhausted`；
5. Channel Run 存在且尚未成功终止时，标记 `status=failed`、`detail_status=failed`，写入
   结构化 `result_json.proxy_control` 证据；
6. Channel Run 不存在时，仍完成 Binding 终止；
7. Candidate 尚未 accepted/rejected/existing 时，标记 `failed`，清空 `next_retry_at`，写入
   `validation_finished_at` 和结构化预算证据；
8. 已经成功终止的 Candidate 或 Run 不得被预算错误降级；矛盾状态应作为不变量错误报警；
9. 提交事务后再抛带稳定 code 的 `UnrecoverableError`。

当前 `terminateBusinessRunBinding()` 只会把 reserved/terminal Binding 更新为 terminal，
materialized Binding 会被原样返回。实现原子终止时必须明确扩展 materialized 预算终态，
同时保留 `materialized_at` 审计；不能让已失败 Channel Run 对应一个仍可被解释为运行中的
Binding。

事务必须幂等。重复收到相同 Business Run 的预算错误时，只允许重放相同终态和证据，不得
改变终态时间或产生第二个恢复意图。

#### 4.6.3 修复 `failed` 监听器终态覆盖

`worker.on("failed")` 必须显式识别 Business Run 预算终态。不能只依赖
`attemptsMade >= maxAttempts`，也不能只依赖错误文案。至少应同时具备：

- 稳定错误 code/cause 链识别；
- 数据库中 Binding/Candidate 的持久终态门禁；
- 幂等写入，避免异步 failed listener 把 `failed` 改成 `queued`。

建议把 Channel Job 失败回写从内联事件监听器提取成可单测函数，测试真实 SQL 状态转移。

#### 4.6.4 再调整 Rota 预算检查顺序

Worker 已能处理两种预算终态后，再修改 `task.go`：

1. 保留 Business Run 行的 `FOR UPDATE`；
2. 先检查 `nextAttempt > maxAttempts`；
3. 总预算耗尽时幂等写入 `budget_exhausted_at` 并返回 Business Run 预算错误；
4. 总预算未耗尽时再检查当前 `job_execution_id` 的 Execution 预算。

幂等的 `attempt_request_id` 重放检查必须继续位于预算检查之前，避免已成功创建的 Task 因
后续预算变化而无法重放。

#### 4.6.5 使用持久且不可碰撞的 dispatch generation

每次逻辑调度生成持久 generation：

- 同一次 BullMQ 内部 retry 保持 generation；
- Job 被人工重新调度、重新创建或进入 Recovery Intent 时获得新 generation；
- generation 在数据库事务中原子分配，不能由内存或 `attemptsMade` 推断；
- Job Data、调度审计和 Recovery Intent 保存相同 generation；
- 同一逻辑任务的 generation 具有唯一约束。

Execution 身份的规范输入可以是：

```text
queue_name + job_id + dispatch_generation + bullmq_attempt_number
```

但不能直接拼成 Rota 字段。`safeJobId()` 对未哈希输入允许 256 字符，哈希分支也可达到
254 字符；Rota BeginTask 对单字段限制为 255 字节。实现必须在 `jobExecutionId()` 合成
边界使用版本化、有界的 ASCII 哈希，例如：

```text
exec:v1:<fixed-width digest>
```

完整 queue、Job ID、generation 和 attempt 分别保存在审计字段中，不能只保留不可解释的
digest。

#### 4.6.6 新增 Recovery Intent 和可靠投递

建议表 `crawler.migration_retry_intents` 至少包含：

```text
retry_intent_id
request_key
candidate_id
previous_business_run_id
new_business_run_id
new_business_run_key
new_job_id
dispatch_generation
reason
status
dispatch_status
requested_at
dispatched_at
finished_at
last_error
```

约束要求：

- `request_key` 唯一，重复操作只能返回同一 Intent；
- `(candidate_id, dispatch_generation)` 唯一；
- 新 Business Run Key 为
  `full-candidate:<candidate_id>:recovery:<retry_intent_id>`；
- `bindingIdentity()` 必须在普通 `candidate_id` 分支之前识别 `retry_intent_id`；
- 新 Job ID 与 dispatch generation 必须关联 Intent；
- 数据库提交和 Redis 入队之间使用 outbox/dispatcher 或等价的可重放机制；
- Redis Job ID 固定且唯一，使重复投递幂等。

`queue.add()` 可能已在 Redis 成功、但客户端在收到响应前断线。Outbox 不能直接把这种
错误计为死信；应按确定性 Job ID 回查，且只有 Job ID、name 和规范化 data 全部一致时才
标记 sent。相同 ID 对应不同 name/data 时必须 fail-closed。Worker 的异步 completed/failed
listener 也可能在进程退出时丢失，因此 Controller 需要周期读取 active Recovery Intent 和
BullMQ 终态，并使用 Intent ID、Job ID、dispatch generation 三重 Fence 幂等重放终态。

恢复 Job Data 必须从字段白名单重新构造，不能展开并复制旧 Job Data。当前 Candidate 分支
会忽略旧 `run_id`，但旧 `business_run_key` 会在 cached-key 检查中与新 key 冲突；
`full_intent_id`、Job name、Policy 和 Intent JSON 的变化还可能在检查 terminal 之前触发
`BUSINESS_RUN_KEY_CONFLICT`。运行时缓存字段不属于新恢复意图。

旧 key 上存在三种不符合恢复目标的结果：

1. Binding 为 reserved/materialized 且 Intent 相同：复用旧 Business Run；
2. Binding 为 terminal 且 Intent 相同：Job 被 skip；
3. Intent、Policy 或 `full_intent_id` 不同：抛 `BUSINESS_RUN_KEY_CONFLICT`。

因此 Recovery Intent 必须使用新 key，而不是试图改变旧 Binding 的不可变 Intent。

#### 4.6.7 区分 Observation 和服务端终态证据

不能要求所有 failed/abandoned Task 都有 Worker Observation：

- `Observe()` 只接受 active Task；
- Lease 已过期时返回 `LEASE_GONE`；
- `LEASE_CONFLICT` 可能在 BeginTask 创建 Task 之前发生。

冻结验收要求应写为：所有失败或 abandoned Task 都有可审计证据。Worker 在持有有效 Task
和 Lease 时写 Observation；Lease 过期、服务端 abandon、BeginTask 冲突等由 Rota 写入
服务端 Task/Lease 终态事件。不得伪造一个不可能被协议接受的 Observation。

#### 4.6.8 Dashboard 预算可见性

Dashboard 至少显示：

- Business Run 已用 Task 数和总上限；
- 当前 Execution 已用 Task 数和上限；
- Binding 状态和 terminal reason；
- dispatch generation；
- 当前 BullMQ attempt；
- Business Run `budget_exhausted_at`。

页面和日志不得显示 Redis 密码、数据库密码、Rota Token、代理认证信息或带认证的代理
URL。

### 4.7 必补回归

1. Rota：Business Run 总预算优先于 Execution 预算；
2. Adapter：服务端 Execution 预算结束当前 attempt，不返回 Deferred；
3. Adapter：本地 Route Switch 上限与服务端 Execution 预算行为相同；
4. Worker：Execution 预算不调用 `deferJobForSlotPause()`；
5. Worker：容量等待和无 Reserve 仍 delayed，且不增加 `attemptsMade`；
6. Budget Recovery：materialized Binding、Candidate 和 Channel Run 原子终止；
7. Budget Recovery：reserved Binding 且无 Channel Run 仍能原子终止；
8. Worker failed handler：预算 Unrecoverable 不把 Candidate 改回 queued；
9. Execution ID：最长 Job ID 仍生成小于等于 255 字节的稳定 ID；
10. dispatch generation：Job 重建后不复用旧 Execution；
11. Recovery Intent：重复请求和重复投递只产生一个新 Run/Job；
12. 集成：每个 BullMQ attempt 最多 3 个 Task，3 次 attempt 最多 9 个 Task，第 10 次稳定
    进入 Business Run failed 终态；
13. 重启：终态和 generation 不回退。

### 4.8 不可采用的处理方式

- 重启容器；
- 只修改 `snapshot_attempts`；
- 只修改 BullMQ `attemptsMade`；
- 修改 `next_attempt_number` 或删除旧 Task；
- 清空 Redis 队列或 Rota 数据库；
- 增加代理数量；
- 复用已经耗尽或 terminal 的 `full-candidate:<candidate_id>` Binding。

## 5. BUG-2：活动 Task 的 Lease 过期后永久 Slot Not Ready

优先级：P0

### 5.1 现场现象

电脑休眠、虚拟机暂停或 Node.js 事件循环停止超过 Rota Lease TTL 后：

- Rota 将旧 Lease 标记 expired；
- 活动 Task 被原子标记为 abandoned/cancelled，`failed_stage=lease_expired`；
- Slot 已变为 unleased，并准备了新凭据 generation；
- Worker 本地仍保留旧 Task、Completion 和 Runtime；
- 后续 Job 持续得到 `slot_not_ready` 并每 5 秒 delayed；
- 代理容量足够，但 Worker 没有第二次 Claim。

BullMQ Lock 也可能在休眠期间过期，出现 `could not renew lock` 或 `Missing lock`。这会影响
当前 Job 的 BullMQ 状态转换，但不是 Adapter 永久不重新 Claim 的根因。

### 5.2 `a8e07f1` 基线源码证据

- `rotaSlotAdapter.js:536` 在发送 CompleteTask 前保存 `pendingCompletion`；
- 只有 CompleteTask 成功后才在 `:539` 清理 Completion，在执行主路径 `:324` 清理
  `activeTask`；
- CompleteTask 失败时 `:542` 将 Slot Fence 为 `COMPLETE_UNCERTAIN`；
- `:688` 的周期 Renew 对 `retryable=false` 错误停止重新调度；
- 当前源码没有 `#reclaimAssignment()` 或等价恢复路径；
- 当前测试没有“活动 Task 的 Renew 和 CompleteTask 都返回 LEASE_GONE 后第二次 Claim”
  用例。

第二份现场报告声称该修复已经部署且 41 个测试通过，但修复不在当前 Git 或可见历史中。
因此这是源码与运行镜像不一致问题。任何 qybullmq 镜像重建前必须先解析现场镜像 digest，
归档实际补丁或在当前仓库重新实现。

### 5.3 根因链路

```text
周期 Renew 返回确定性 LEASE_GONE
  -> Adapter Fence 并中止 Attempt
  -> Worker 使用旧 Lease CompleteTask
  -> CompleteTask 同样 LEASE_GONE
  -> pendingCompletion / activeTask / Runtime 保留
  -> 非 retryable Renew 不再调度
  -> Assignment 永久 not ready
  -> 所有 Job 只能 delayed
```

### 5.4 永久解决方案

仅当控制面明确返回 `LEASE_GONE` 时允许丢弃旧本地状态。建议恢复顺序：

1. 捕获发生错误时的 frozen Assignment，包括 Slot、Lease 和 route generation；
2. 中止当前 Attempt；
3. 等待当前 `activeJobCompletion` 结束，避免与执行主路径并发清理；
4. 再次确认当前 Assignment 仍与 frozen Lease 相同；
5. 只清理属于该 frozen Lease 的 `pendingCompletion` 和 `activeTask`；
6. quiesce 并 retire `activeRuntime` 和 `idleRuntime`，确保旧网络身份不再使用；
7. 清空旧 Assignment 和 Lease 安全窗口，进入明确的 `RECLAIMING` 本地状态；
8. 使用新的 Claim request ID 执行 Claim；
9. 接受新的 Lease、凭据和 Assignment 后恢复周期 Renew；
10. 继续消费原队列，不要求删除或重新创建 BullMQ Job。

以下错误必须继续 fail-closed，不能触发上述清理：

- Rota 请求超时；
- 临时网络错误；
- `retryable=true` 的错误；
- CompleteTask 结果不确定但 Lease 仍可能有效；
- 无法证明错误属于当前 frozen Assignment。

工作树使用独立的 `completionUncertain` 本地状态记录“CompleteTask 请求可能已经在服务端
生效，但 Worker 没拿到确定回执”。该状态存在时，周期 Renew 仍可延长同一 Lease，避免
服务端过早回收，但 Renew 的 `ready=true` 不能重新开放 Slot：Adapter 接收续租结果后必须
立即恢复 `COMPLETE_UNCERTAIN` Fence，后续 Job 不得 BeginTask。只有以下两个确定性结果
可以清除该 Fence：

1. 使用原 `completion_request_id` 幂等补发 CompleteTask 并获得成功回执；
2. Rota 明确返回 `LEASE_GONE`，随后按 frozen Lease 执行 Reclaim。

普通 Renew 成功、控制面超时或其他网络恢复都不能推断原 Completion 未生效，也不能清除
`pendingCompletion`。

`close()`、周期 Renew 和 Reclaim 必须互斥或共享同一控制命令 lane，避免关闭过程和后台
Claim 同时创建新 Lease。

### 5.5 必补回归

1. idle Lease 的确定性 `LEASE_GONE` 会重新 Claim；
2. active Attempt 的 Renew 返回 `LEASE_GONE` 时 AbortSignal 被中止；
3. 旧 CompleteTask 同样返回 `LEASE_GONE` 后本地残留被清理；
4. active/idle Runtime 各自只 retire 一次；
5. 第二次 Claim 得到不同 Lease，新 Job 可 BeginTask、CompleteTask 和 Release；
6. 超时、retryable Renew 和不确定 CompleteTask 不会清理权威状态；
7. 不确定 CompleteTask 之后即使 Renew 成功，Slot 仍保持 fenced，不能 BeginTask；
8. 使用相同 Completion 幂等键补发成功，或确定性 `LEASE_GONE` Reclaim 后才清除 Fence；
9. Reclaim 期间调用 close 不会留下新 Lease；
10. BullMQ Lock 已丢失时，Adapter 仍可恢复 Slot，且不会伪造 Job 完成状态。

### 5.6 部署阻断

在完成以下操作前，不得构建或部署新的 Worker 镜像：

1. 记录当前运行 worker-channel 镜像 digest 和源码 revision label；
2. 判断现场镜像是否包含未入库 BUG-2 热修；
3. 将补丁以源码和回归测试形式纳入当前分支；
4. 从固定 Git revision 重建镜像；
5. 验证新镜像同时包含 BUG-1、BUG-2 和 BUG-3 所需 Worker 兼容逻辑。

## 6. BUG-3：Fingerprint Proxy Transport 没有触发 Rota 换路

优先级：P1

### 6.1 现场现象

三个 Channel Worker 扩容后，18 个 Job 均在相同 Route 上耗尽 3 次 BullMQ attempt：

```text
fingerprint gateway proxy_transport: SSLError curl_code=35
code=FINGERPRINT_PROXY_TRANSPORT
```

Crawler 中对应 18 个 Candidate 为 failed，没有 Channel Run。Rota Task 结果为 failed，但
控制状态为 `READY_KEEP_ROUTE`，代理仍为 active/healthy。Rota 没有收到
`proxy_transport` Observation。

### 6.2 当前源码证据

`services/qybullmq/src/fingerprintGateway.js:32` 已构造结构化错误：

```text
failureKind=proxy_transport
code=FINGERPRINT_PROXY_TRANSPORT
curlCode=35
youtube_failure_evidence.source=fingerprint_gateway
```

但 `services/qybullmq/src/youtubeFailurePolicy.js:90` 的 `decideYoutubeFailure()`：

- 不读取 `failureKind` 或 `failure_kind`；
- `errorCode` 只读取根 Error 或一层 cause；
- Proxy 正则不匹配 `proxy_transport`；
- `youtube_failure_evidence` 只从根 Error 读取。

因此最小结构化复现稳定返回：

```json
{
  "kind": "unknown",
  "retry_mode": "default",
  "proxy_action": "none",
  "client_action": "none",
  "terminal": false,
  "status": null
}
```

`services/qybullmq/src/managedWorkerExecution.js:82` 只把
`proxy_transport`、`youtube_rate_limited` 和 `youtube_challenge` 转成 Rota 网络失败。
`unknown` 会直接抛给 BullMQ，不会持久化网络 Checkpoint、写 Observation 或请求换路。

### 6.3 根因链路

```text
Fingerprint Gateway 提供结构化 proxy_transport
  -> 统一分类器忽略 failureKind/code
  -> 文本正则未匹配
  -> unknown/default
  -> retryableRotaFailure() 返回 null
  -> 无 Observation / 无 pending_action
  -> Rota READY_KEEP_ROUTE
  -> BullMQ 在同一 Route 上重试三次
  -> Job 和 Candidate failed
```

扩容只提高了并发和暴露速度，不是根因。

### 6.4 永久解决方案

实现一个有循环保护和深度上限的结构化错误提取器，遍历 Error、`cause` 和受支持的聚合
错误。提取 `kind`、`code`、`source`、HTTP status 和受限证据时必须来自同一个可信节点，
不能从不同 cause 拼出虚假的组合。

白名单映射：

| 可信字段 | Decision |
| --- | --- |
| `failureKind=proxy_transport` 或 `code=FINGERPRINT_PROXY_TRANSPORT` | `proxy_transport`、`new_identity`、`cooldown_network` |
| `failureKind=upstream_transient` 或 `code=FINGERPRINT_UPSTREAM_TRANSIENT` | `upstream_transient`、`same_identity`、不切代理 |

必须保留结构化错误所在 cause 节点的 `source=fingerprint_gateway`。只修 kind、不修 source
会让审计退化成 `youtube_managed_request`。

文本兜底只能覆盖有限兼容格式：

- `proxy_transport` / `fingerprint_proxy_transport`；
- 来源明确为 Fingerprint Gateway 的 `SSLError curl_code=35`；
- 已有明确代理连接、Tunnel、SOCKS 或 407 证据。

不得把所有 SSL/TLS 错误无条件判为代理失效。

分类后 Managed Worker 必须先持久化 Checkpoint，再返回：

```text
kind=retryable_network_failure
observation=proxy_transport
source=fingerprint_gateway
failedStage=channel_full
checkpointPersisted=true
```

Rota 现有 Observation/CompleteTask 原则上无需为 BUG-3 单独修改：有 Reserve 时进入
`PENDING_NEW_ROUTE`，无 Reserve 时进入 `PAUSED_NO_RESERVE`。如果失败 Observation 后仍
返回 `READY_KEEP_ROUTE`，Adapter 应把它视为协议错误。

### 6.5 必补回归

1. 结构化 `failureKind=proxy_transport` 不依赖文本即可识别；
2. `FINGERPRINT_PROXY_TRANSPORT` 不依赖文本即可识别；
3. 相同错误包装在多层 cause 中仍保留 kind 和 `source=fingerprint_gateway`；
4. `FINGERPRINT_UPSTREAM_TRANSIENT` 保持 same identity；
5. 普通未知 TLS 文本没有可信来源时不切代理；
6. Checkpoint 在返回 retryable network failure 之前持久化；
7. `proxy_transport` Observation 使 CompleteTask 进入新 Route 或无 Reserve 状态；
8. 403、429、Challenge、404、Parser 和数据库分类保持不变。

### 6.6 历史 18 个失败 Job

历史 Job 已经 failed，不会因分类代码部署而自动恢复。不得直接修改 attempts、
`snapshot_attempts` 或旧 Rota Task。完成 Recovery Intent 后，应为它们创建新调度代次，
先恢复 3 个样本，确认 Route Rotation 后再处理剩余 15 个。

## 7. BUG-4：健康检查判定 failed 后 Leased-Idle Slot 不换路

优先级：P1。冻结基线存在协议设计阻断；当前工作树 ADR 已接受并实现，尚未部署。

### 7.1 现场现象

健康检查将 `proxy_id=785` 明确更新为 failed：

```text
failure_kind=soft_unreachable
conclusive=true
control_path_healthy=true
base_health_status=failed
```

但 `bullmq-channel-04` 仍保持：

```text
proxy_id=785
control_state=leased_idle
active_task_id=NULL
Lease 持续 Renew
assignment_version 不变
```

### 7.2 当前源码的精确行为

1. `proxy/healthcheck.go:216` 调用 `ApplyHealthVerdict()`；
2. `repository/proxy_lifecycle_repository.go:71` 更新 Proxy 生命周期和健康证据，但不更新
   Slot、Lease、Route generation，也不递增或返回该 Verdict 的 `health_generation`；
3. `proxycontrol/reconcile.go:78` 只要 Lease 有效就把 Slot 标记 Locked；
4. `proxycontrol/policy.go:45` 无条件保留 Locked Slot 的旧 Proxy；
5. `proxycontrol/reconcile.go:84` 对所有有效 Lease 直接 continue；
6. `proxycontrol/lease.go:250` 的 Renew 先延长 Lease，再在 `:306` 加载 Assignment；
7. `proxycontrol/lease.go:775` 的 Assignment SQL 已检查 Proxy status、健康和
   `revalidation_required`，所以 failed Proxy 返回的是 `ready=false`，不是 `ready=true`；
8. Adapter 对非 ready Assignment 执行 Fence，但 Renew 请求本身成功，周期仍会继续，
   因此旧 Lease 可以永久延长；
9. leased-idle 没有 active Task，不能通过 Observation + CompleteTask 换路；
10. `proxycontrol/task.go:55` 的 BeginTask 只校验 Slot/Lease/leased_idle，不 JOIN Proxy
    校验当前 Route 仍 eligible。

所以根因应表述为：健康生命周期已经失效，Renew 也能看见 `ready=false`，但有效 Lease
没有 idle Route Transition；不是 Renew 一直返回 ready，也不是等待某个 CompleteTask。

### 7.3 active 和 idle 不能使用同一 Locked 语义

| Slot 状态 | 约束 |
| --- | --- |
| `active_task` | 不能静默改变当前 Attempt 的出口 IP；必须明确允许收尾或通过 Fence 中止 |
| `leased_idle` | 没有业务请求，可执行同 Lease Idle Route Transition |
| `pending_new_route` | 旧 Runtime 不得继续使用，等待新数据面和更高 generation |
| `paused_no_reserve` | 不得 BeginTask，Job delayed 且不消耗 attempts |
| `unleased` | 允许 Reconcile 分配并由 Worker Claim |

当前一个 `ready` 布尔值不足以同时表达“当前 active Attempt 是否仍有权继续”和“是否允许
开始下一个 Task”。当前 ADR 通过 Execution-Locked/Lease-Owned Idle 状态、最终 BeginTask
资格门禁和 Idle Route Transition 分开表达这两个能力。

### 7.4 已接受的 ADR 决议

`docs/adr/0002-health-driven-slot-route-transitions.md` 已接受以下决议：

1. conclusive Health Verdict 发生在 active Task 上时，当前 Attempt 保持原 Route，允许
   quiesce 并按现有 Completion 合同收尾；
2. Health Incident 是独立的服务端健康证据，不替代 Worker Task Observation，也不为 active
   Task 写现有 `pending_action`；
3. `health_generation` 对实际应用的 Verdict 单调递增，过期证据不递增；
4. leased-idle 使用同 Lease Idle Route Transition；
5. 无 Reserve 时清除不可用 Route，保留 Lease，并进入 `PAUSED_NO_RESERVE`；
6. Worker 接受更高 Route generation 前先 retire 旧 idle Runtime；
7. 提交后的 Health 通知只是加速 Reconcile 的提示；持久数据库状态和周期 Reconcile 是通知
   丢失及多实例场景的权威兜底。

### 7.5 永久解决方案

#### 7.5.1 持久 Health Event 和 Reconcile 触发

当前工作树让已应用 Health Verdict 在数据库中递增 `health_generation`；过期 Evidence 不
递增。Repository 目前不返回该 generation。事务提交后，HealthChecker 产生进程内
`HealthVerdictEvent`，字段只有：

```text
proxy_id
resulting_status
failure_kind
conclusive
checked_at
```

Server 适配器当前只把其中的 `proxy_id` 传给 `NotifyHealthIncident()` 以唤醒 Reconcile；
通知不携带 `previous_status`、`health_generation` 或持久 Event ID，也不作为迁移授权。
周期 Reconcile 继续作为通知丢失的兜底。跨实例不会把本地通知当作权威，仍从数据库中的
已提交 Proxy/Slot 状态恢复。

#### 7.5.2 Reconcile 读取真实 Task 状态

`loadRunningSlots()` 至少增加：

```text
active_task_id
current_lease_id
pending_action
proxy_id
assignment_version
control_state
lease_until
```

Planner 必须区分 `ExecutionLocked` 和 `LeaseOwnedIdle`。只有 active Task 的当前 Route 可以
按执行一致性暂时冻结；idle Slot 的 Proxy 已不 eligible 时必须进入受控迁移。

#### 7.5.3 BeginTask 最终资格门禁

BeginTask 在同一事务和锁范围中检查当前 Proxy：

- `status=active`；
- `revalidation_required=false`；
- cooldown 已结束；
- Role/Policy 和健康字段仍符合条件；
- Slot 仍为 `leased_idle` 且没有 active Task。

Health Verdict 提交后、下一次 Renew 前，BeginTask 也不得在 failed Proxy 上启动新请求。

#### 7.5.4 leased-idle Route Transition

当前工作树实现同 Lease Rotation：

1. 锁定 Slot、Lease 和候选 Proxy；
2. 再次确认 `active_task_id IS NULL`；
3. 选择符合 Policy 的 Reserve；
4. retire 旧数据面凭据；
5. 增加 assignment/route 和 credential generation；
6. 切换 Proxy 和 network identity；
7. 激活新数据面后返回更高 generation 的 ready Assignment。

没有 Reserve 时，事务清除不可用 Proxy/identity，轮换凭据、递增 Route generation 并进入
`paused_no_reserve`；增加 Reserve 后同一 Lease 幂等恢复。重复 Health 通知或 Reconcile
不能为同一次状态变化重复递增 generation。

#### 7.5.5 active Task Route Transition

Health Verdict 不写现有 `pending_action=rotate_route`。Execution-Locked Slot 在 Renew 时仍向
当前 Task 返回其原 Route，使 Attempt 可以 quiesce 并按原 Completion 合同收尾；同一事务
内的 BeginTask Proxy eligibility 门禁禁止 failed Route 开始下一个 Task。Task 收尾后 Slot
成为 Lease-Owned Idle。Completion 的锁定快照会读取当前 Proxy eligibility；若 Route 已
失效且没有 Task Observation 产生的 pending action，提交 Completion 后立即请求一次
Reconcile，再由同 Lease Transition 换路。周期 Reconcile 仍是最终兜底。Worker 不伪造
Observation，Rota 也不在 active Task 中途静默改写 endpoint。

### 7.6 必补回归

1. failed Proxy 上的 leased-idle Slot 自动迁移到健康 Reserve；
2. 无 Reserve 时进入 paused 状态且不允许 BeginTask；
3. 增加 Reserve 后 paused Slot 自动恢复；
4. Renew 不会把 failed Proxy 返回为 ready 的 leased-idle Assignment；
5. Renew 不会在没有迁移进度时永久延长失效 idle Route；
6. BeginTask 在 Health Verdict 与下一次 Renew 的窗口内拒绝 failed Proxy；
7. active Task 不会被后台静默改写 endpoint；
8. active Task 保持原 Route 收尾，且不会被 Health Incident 强制 Abort；
9. Health Incident 不创建 Task pending action，也不要求伪造 Observation；
10. assignment 和 credential generation 每次迁移只增加一次；
11. 并发 Renew、Health Verdict、Reconcile 和 BeginTask 不产生双重绑定；
12. 重复 Health Event/Reconcile 幂等；
13. 一个 Proxy 不会同时绑定多个 Slot。

## 8. 统一测试矩阵

所有数据库和 Redis 集成测试必须使用独立实例，不得连接当前运行数据。

| 测试 ID | 层级 | 验证目标 |
| --- | --- | --- |
| T01 | Rota PostgreSQL | Business Run 预算优先于 Execution 预算 |
| T02 | Adapter | 服务端 Execution 预算不返回 capacity Deferred |
| T03 | Adapter | 本地 Route Switch 上限结束当前 BullMQ attempt |
| T04 | Worker boundary | Execution 预算不调用 `deferJobForSlotPause()` |
| T05 | Worker boundary | capacity/no-reserve 仍 delayed |
| T06 | Crawler PostgreSQL | materialized Run 的 Candidate/Binding/Run 原子终止 |
| T07 | Crawler PostgreSQL | reserved Binding 且无 Run 的原子终止 |
| T08 | Worker failure | failed listener 不覆盖预算终态 |
| T09 | Identity | 最长 Job ID 的 Execution digest 不超过 255 字节 |
| T10 | Dispatch | 新 generation 不复用旧 Execution |
| T11 | Recovery | 重复 Recovery Intent/投递保持一个新 Run 和 Job |
| T12 | Adapter | active Lease 确定 gone 后自动第二次 Claim |
| T13 | Adapter | 不确定 CompleteTask 继续 fail-closed |
| T14 | Failure policy | Fingerprint structured kind/code/cause/source 分类 |
| T15 | Managed Worker | Checkpoint 先于 retryable network failure |
| T16 | Rota Observation | proxy transport 进入新 Route 或 no-reserve |
| T17 | Proxy Control | failed Proxy 的 leased-idle Route Transition |
| T18 | Proxy Control | active Task 保持 Route，Health Incident 不替代 Observation |
| T19 | Proxy Control | BeginTask 最终 eligibility 门禁 |
| T20 | Concurrency | Renew/Verdict/Reconcile/BeginTask 并发幂等 |
| T21 | 端到端 | 每 Execution 最多 3 Task，每 Run 最多 9 Task，第 10 次终止 |
| T22 | 重启 | 终态、generation 和 Recovery 进度不回退 |
| T23 | Outbox | `queue.add()` 模糊成功只接受 name/data 一致的确定性 Job |
| T24 | Recovery | Controller 重放丢失的 Recovery Intent completed/failed 终态 |

计划执行的静态和定向命令至少包括：

```text
node --check services/qybullmq/src/rotaSlotAdapter.js
node --check services/qybullmq/src/youtubeFailurePolicy.js
node --test services/qybullmq/test/youtubeFailurePolicy.test.js
node --test services/qybullmq/test/managedWorkerExecution.test.js
node --test services/qybullmq/test/rotaSlotAdapter.test.js
node --test services/qybullmq/test/businessRunBudgetRecovery.test.js
git diff --check
```

Rota 需要在 `services/rota/core` 下执行相关 Go 单元测试和 PostgreSQL 集成测试。测试名称
和 package 以最终实现为准，不能用单纯编译成功代替状态机断言。

当前工作树验证记录：

- `npm test` 共运行 251 个 Node 测试文件，247 个直接通过；其中 `buildImages.test.js` 和
  `businessPublicationIngress.test.js` 仅受文件系统/Docker或本地监听沙箱限制，授权后分别
  1/1 和 4/4 通过；剩余两个环境失败是宿主 Python 缺少 Fingerprint Gateway 所需的
  `aiohttp` 和 yt-dlp Session 所需的 `yt_dlp`，未安装或修改宿主 Python 环境；
- 四个 Bug、Recovery Intent、Outbox 和 Worker 合同的 12 个定向 Node 测试文件全部通过；
- `services/qybullmq/src`、`scripts` 和 `test` 下 508 个 JavaScript/MJS 文件全部通过
  `node --check`；
- 独立 `rota-fix-test-postgres/rota_test` 中，reserved/materialized/late-completion 三种预算
  终止事务、Managed Outbox/schema 和 Recovery Intent 事务测试全部通过；Recovery Intent
  覆盖阈值冲突、缺 Outbox fail-closed，以及直接返回或模糊成功的 BullMQ Job 身份消歧；
- 固定 `golang:1.25.3` 容器中的 `go test ./... -count=1` 全部通过，包括 Proxy Control 和
  Proxy Health PostgreSQL 集成用例；本次修改和新增的 14 个 Go 文件无 `gofmt` 差异；
- 新增 CLI 的 plan/execute 单元测试已通过，`git diff --check` 和敏感信息扫描仍需在最终
  静态检查后保持通过。

上述是工作树测试证据，不是镜像构建、部署或生产冒烟证据。

## 9. 实施与部署顺序

### 9.1 阶段 0：建立源码与镜像可追溯性

1. 记录当前三个 worker-channel、Rota Core 和 Fingerprint 相关镜像 digest；
2. 读取镜像 revision label，并与 Git commit 对照；
3. 比对现场 `dispatch_generation` Job Data 与冻结源码入队路径；
4. 归档或重做现场声称已部署的 BUG-2 热修；
5. 在 Git 中加入 BUG-2 精确回归；
6. 在此之前不得重建 Worker，避免覆盖热修或其他未入库运行时能力。

### 9.2 阶段 1：先部署 Worker 兼容逻辑

同一批可追溯 Worker 改动包含：

- BUG-1 三类错误分派；
- materialized/reserved Business Run 原子终止；
- failed listener 终态门禁；
- BUG-2 确定性 Lease Gone Reclaim；
- BUG-3 结构化分类和 cause source；
- 对旧 Rota 响应保持向后兼容。

先部署 Worker 的原因：如果先交换 Rota 预算检查顺序，当前两个未物化 Run 会从永久
delayed 转成普通 Error、BullMQ retries 和 Candidate 状态抖动。

### 9.3 阶段 2：再部署 Rota 预算和 BeginTask 门禁

- Business Run 预算优先检查；
- `budget_exhausted_at` 幂等写入；
- BeginTask Proxy eligibility 最终门禁；
- 保持现有 idempotency replay 和 CompleteTask fail-closed 行为。

部署后先观察旧 Job 是否离开 Execution delayed 循环并进入受控 Business Run 终止，但仍
不得创建恢复调度。

### 9.4 阶段 3：Execution Identity 和 Recovery Intent

- 数据库 dispatch generation；
- 有界 `jobExecutionId` digest；
- Recovery Intent schema、唯一约束和投递 outbox；
- 新 Business Run Key 和白名单 Job Data；
- Dashboard 预算字段。

### 9.5 阶段 4：BUG-4 协议部署

1. 使用已接受 ADR 中的 active finish 和同 Lease idle Transition；
2. 先部署能理解新 Renew/Health Incident 状态的 Worker；
3. 再部署会发出新状态的 Rota；
4. 验证同 Lease Idle Route Transition；
5. 验证无 Reserve 和恢复 Reserve；
6. 完成并发与重启测试。

### 9.6 阶段 5：受控数据恢复

只有全部阶段验收完成后，才进入第 10 节。

## 10. 当前两个频道的受控恢复程序

### 10.1 恢复前置条件

每项都必须满足：

- 所有相关修复已经提交、测试并部署；
- 运行镜像 digest 与 Git revision 一致；
- Worker 已通过 BUG-2 Lease Reclaim 冒烟；
- Fingerprint proxy transport 能产生 Observation 和 Route Rotation；
- failed Proxy 的 idle Slot 能迁移或进入 no-reserve；
- Recovery Intent 和 outbox 已部署；
- Dashboard/SQL 能显示 Business 和 Execution 预算；
- 已暂停 worker-channel 接收新任务；
- `youtube-channel-crawl active=0`；
- 没有控制面命令仍在执行；
- 已对两个 Candidate、Binding、Business Run、18 个 Task 和 Redis Job 做只读快照；
- 快照不包含明文密码、Token 或代理认证 URL。

### 10.2 受控终止旧状态

1. 对两个旧 Business Run 执行幂等预算终止程序；
2. 保留 18 个旧 Task 和全部审计历史；
3. 将两个旧 Binding 标记 terminal；
4. 将两个 Candidate 标记 failed 并写结构化证据；
5. 若发现 Channel Run，则按事务规则终止；当前预期为不存在；
6. 验证修复后的 Worker 已将旧 Job 终止；若仍为 delayed，才使用受控、幂等的 BullMQ
   终止程序将其置为 failed/terminal；
7. 验证不再出现同 Job 的 5 秒 delayed 日志；
8. 任一对象身份或计数与快照不一致时立即停止。

### 10.3 创建新恢复边界

对每个 Candidate：

1. 创建唯一 `migration_retry_intent`；
2. 分配新 dispatch generation；
3. 分配新 Business Run ID；
4. 使用 `full-candidate:<candidate_id>:recovery:<retry_intent_id>`；
5. 从白名单构造全新 Job Data；
6. 使用关联 Intent 的新 Job ID；
7. 通过 outbox 幂等入队；
8. 验证旧 Binding 仍 terminal，新 Binding 为 reserved 且指向新 Run；
9. 验证新 Execution digest 与旧 Task 的所有 `job_execution_id` 不同。

受控创建入口为：

```text
npm run recover:migration-retry-intent -- \
  --candidate-id <id> \
  --previous-business-run-id <run-id> \
  --request-key <operator-request-key> \
  --reason <auditable-reason> \
  --min-subscriber-count <count>
```

默认命令只验证 Crawler 数据库身份并输出只读计划及参数绑定的确认串。只有追加
`--execute --confirm <exact-plan-token>` 才会提交 Candidate generation、Recovery Intent 和
PostgreSQL Outbox；该 CLI 不导入 BullMQ Queue，也不直接 Retry 旧 Job。

### 10.4 小流量恢复

1. 只恢复这两个任务；
2. 恢复 worker-channel，初始并发限制为 1；
3. 确认 Worker 使用有效新 Lease 和新 dispatch generation；
4. 监控 Candidate 进入 validating 后明确到 accepted 或 failed；
5. accepted 时继续检查 Channel Run 的 waiting_detail、waiting_agent 和最终状态；
6. 检查 `finalized_profiles`、Publication 和频道列表晋升；
7. failed 时保留新 Run 和 Task 证据，不自动创建第三个 Intent。

### 10.5 立即停止条件

出现任一条件立即暂停恢复，不得通过清空数据继续：

- 新 Job 使用旧 Business Run ID、旧 Binding key 或旧 Execution ID；
- 同一 Intent 创建两个 Job 或两个 Business Run；
- Candidate 从 terminal failed 自动回到 queued；
- `execution_route_budget_exhausted` 或 `slot_not_ready` 再次无限 delayed；
- Lease conflict、Missing lock 或数据面 generation 不一致持续出现；
- proxy transport 后 Rota 仍返回 `READY_KEEP_ROUTE`；
- failed Proxy 仍允许 BeginTask；
- 日志或数据库出现密码、Token 或代理认证信息；
- Task 数超过冻结预算。

## 11. 最终验收标准

### 11.1 BUG-1

- 相同 Job 不再每 5 秒无限输出 Execution budget；
- 每个 Execution 最多 3 个 Task；
- 每个 Business Run 最多 9 个 Task；
- 第 10 次 BeginTask 稳定返回 Business Run 终态；
- reserved 且无 Channel Run 也能原子终止；
- failed listener 不回滚 Candidate；
- 重启后终态不回退。

### 11.2 BUG-2

- idle 和 active 场景的确定性 `LEASE_GONE` 都能自动重新 Claim；
- 旧 Runtime 在新 Lease 前 retire；
- 不确定 Completion 在成功 Renew 后仍 fail-closed，且复用同一幂等键补发；
- 不需要删除 BullMQ Job 即可继续运行。

### 11.3 BUG-3

- 第一次 `FINGERPRINT_PROXY_TRANSPORT` 即产生持久 Observation；
- 有 Reserve 时使用不同 Proxy 和更高 Route generation；
- 无 Reserve 时 delayed 且不消耗 BullMQ attempt；
- cause 包装不丢失 `source=fingerprint_gateway`；
- 不把普通 TLS 错误误判为代理失效。

### 11.4 BUG-4

- conclusive Health Verdict 能驱动控制面协调；
- failed/archived/revalidation Proxy 不长期绑定 leased-idle Slot；
- active Task 不被静默切换 IP；
- 无 Reserve 时不使用旧 failed Proxy；
- BeginTask 关闭 Health Verdict 到 Renew 之间的竞态窗口；
- generation、Credential 和 Health Incident 审计完整；
- 并发和重复事件保持幂等且无双重代理绑定。

### 11.5 数据恢复

- 两个旧 Business Run 保留完整 9/9 Task 历史并明确终止；
- 两个旧 delayed Job 离开 delayed；
- 两个新恢复任务拥有新 Business Run、Job ID 和 generation；
- 两个频道最终明确 accepted 或 failed；
- 任何容器重启后恢复进度不回退；
- 历史 18 个 Fingerprint 失败频道只通过新 Recovery Intent 分批恢复。

## 12. 禁止事项

- 不删除 Rota Task、Lease、Observation、Binding 或健康检查历史；
- 不清空 Redis 队列；
- 不重置整个 Rota 数据库；
- 不手工伪造 `attemptsMade`、`snapshot_attempts` 或 `next_attempt_number`；
- 不通过增加代理数量掩盖 BeginTask 预算错误；
- 不把 failed Proxy 手工改回 active；
- 不在修复前重试历史失败 Job；
- 不把所有 SSL/TLS 错误无条件归类为 Proxy 故障；
- 不在 active Task 未 quiesce 时静默切换出口 IP；
- 不用部署日志代替 Git、镜像 digest 和自动化测试证据。

## 13. 文档维护规则

后续实现应在本文顶部更新状态，但不得覆盖原始事实：

- 每个修复记录 Git commit、镜像 digest、部署时间和验证范围；
- “源码已修复”“已部署”“数据已恢复”是三个独立状态；
- 测试通过必须记录具体测试文件和结果；
- 现场行为与仅部分部署后的行为必须分开描述；
- BUG-4 实现不得偏离已接受 ADR；变更 active finish 或同 Lease idle Transition 必须新增
  ADR；
- 恢复完成后仍保留旧 Run、Task、Binding 和 Intent 审计。
