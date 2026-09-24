# 增量抓取中心优化实施方案

日期：2026-09-24。状态：代码核对后的实施方案，未修改或部署生产代码。

实施进度：以上为制定方案时的状态；后续 A/B 已发布并完成观察，C 批根据隔离 CPU profile 增加了事务上下文优化，具体部署、验证与实测结果见 `center-optimization-release-20260924.md`。

目标：保持 55 个增量 worker，降低中心连接池等待、主线程 CPU 消耗和任务周转时间；在相近任务构成下提高有效完成吞吐，再评估 worker 扩容。依据见 `incremental-capacity-assessment-20260924.md`。

## 1. 当前基线与范围

- 中心主池 max=12；已有进度事件中 idle=0，waiting=23–87。此为事件采样，实施前须补充固定周期采样与实际 acquire 等待时长。
- 中心 Node 主线程约 0.959 核，容器已有 4 核 CPU 配额；增加 CPU 配额本身不能解决单线程工作量。
- 276 个完整成功样本：准入 2.14 秒、准备 5.76 秒、采集传输 14.58 秒、应用 5.52 秒、收尾 6.83 秒；中心主导部分约 20.25 秒。
- 最新 15 分钟 completed 为 84.07 个/分钟；这不是长期稳定产能承诺，验收前要固定任务构成并纳入失败和未完成任务。
- `runRemoteNodeCenter.mjs` 已有主池 12、心跳池 4、结果池 8；还有独立直连的监督锁连接池和 LISTEN 会话。不能将已存在的池隔离当作新优化，也不能把监督锁连接移到事务池。
- `waitClaim`、`awaitCommand`、`waitQuiesced` 已使用通知唤醒，并以 5 秒兜底重查；100ms 只是无 signals 时的 fallback。先记录真实唤醒来源，不把线上现象误判成每 worker 每 100ms 固定查库。
- 工作区存在其他未提交修改，实施时须保留既有改动；发布前记录当前线上镜像及相关文件版本，明确最终补丁范围。

第一批保持 worker 数、调度 buffer、代理策略、预算和数据库表结构不变。采用可分别回退的小补丁，避免多项同时变化导致无法归因。

## 2. P0：补齐决定优化方向的计量

改动位置：`remoteNodes/store.js`、`boundedPostgresRead.js`、`managedIncrementalRuntime.js`、`incrementalCoordinator.js`、`runRemoteNodeCenter.mjs`。

1. 对主池、心跳池、结果池分别每秒采样 total/idle/waiting，按 10 秒输出聚合值；不按每条 SQL 输出日志。
2. 为 `pool.connect` 和直接 `pool.query` 路径分别计量 acquire wait、连接占用、SQL 执行与事务总时长；同一事务内统计 SQL 次数，明确是否含 BEGIN/设置/COMMIT。不能只包 transaction 而漏掉直接查询。
3. 给事务标注有限集合的操作名：admission、claim、bind、snapshot、command_wait、receipt、apply、stop、checkpoint、finish。不要把 job/channel ID 作为指标标签，以免形成海量时间序列。
4. 主进程记录 event loop utilization、event loop delay、CPU、GC 与输入/结果字节数；区分 About、Video、About+Video、视频数量和重试次数。
5. 函数级 CPU profile 先在隔离运行环境使用受控样本复现；若与线上负载不一致，再安排有时限、可关闭的进程内采样入口。不要向当前生产进程通过 inspector 动态注入或求值代码；本仓库曾有该操作造成进程退出的记录。
6. 收尾单独记录 requestStop、等待 durable release receipt、checkpoint 读取/应用、finishAttempt、Rota completion 的耗时，避免把 6.83 秒全部归为 stop ACK。

产物：一份同版本、55 worker、至少 30 分钟基线；包含单任务 SQL/事务次数、池 acquire P50/P95/P99、每阶段时间以及成功/partial/失败/未完成任务。观测补丁本身与关闭观测比较开销，建议以 CPU/吞吐变化小于 2% 为工程目标，超过则降低采样率。

## 3. P1-A：合并事务参数设置，直接减少 SQL 往返

已确认代码：`RemoteNodeStore.transactionAttempt()` 每次 BEGIN 后分别发送 writer_version、synchronous_commit、statement_timeout、lock_timeout 四条设置语句，然后才执行实际业务。

实施：保持 BEGIN 独立，在一条参数化 SELECT 中调用四个 `set_config(..., true)`，保留原值：writer version、on、15s、3s。业务 action 和 COMMIT/ROLLBACK 顺序不变。由四次设置往返减为一次，每个该类事务确定少三次客户端往返；这不等于事务耗时一定下降相同比例。

不把 `BEGIN; SELECT ...` 拼成含参数的多语句扩展查询；不改成 session 级设置；不移除同步提交、超时或 writer_version。

验证：用真实 PostgreSQL 验证事务内设置正确、提交和回滚后不泄漏、repeatable-read 重试行为不变；PgBouncer transaction pooling 环境也要验证设置不会串到其他事务。成功、异常、取消、连接断开均需正确释放或销毁连接。

验收：设置 SQL 从 4→1；同负载事务连接占用和 acquire 等待下降；写入结果及失败分类不变。若只减少 SQL 次数、总体吞吐没提升，记录真实收益并继续定位，不能直接宣称整体提速。

## 4. P1-B：收尾去重，保留唯一的完成责任

已确认调用链：`RemoteManagedIncrementalRuntime.quiesce()` 对查到的 binding 执行 requestStop/waitQuiesced，然后 `handle.adapter.quiesce(handle.inner)` 又对 inner binding 调用这两步。后者还负责读取 YouTube checkpoint 并应用 profile，因此不能直接删掉整次 adapter 调用。

改动位置：`managedIncrementalRuntime.js`、`rotaChannelRuntimeAdapter.js`、`channelRouteStore.js`，必要时调整 `youtubeProfileCheckpoint.js` 的内部接口。

实施方式：

- 按精确 binding ID 合并同一 attempt 内的停止请求与静止确认，产生包含 task/generation/binding 身份的内部结果；只有持久化 `retired` 且 `release_receipt.in_flight=0` 的证据才能复用。
- 当前 inner binding 走一次完整 quiesce；数据库扫描发现的其他 binding 仍逐一清理，保留 bind 提交成功但响应丢失的恢复路径。
- 把“网络已静止”和“checkpoint 已处理”分清：adapter 可复用同一精确 binding 的静止结果，但仍执行 checkpoint 与 profile 的事务校验及应用。
- 同一清理 promise 在成功前保持唯一责任；调用方超时不能释放槽位，重试不能并发启动第二个不确定写操作。失败后按现有可恢复规则继续。
- 不跨 attempt 缓存授权状态，不并行化未知锁顺序的 finish/checkpoint，也不在收到普通 ACK 后提前开始下一任务。

验证场景：正常结束、stop 早于 grant、grant 在途、release ACK 丢失、bind COMMIT 响应丢失、部分 binding 已退休、checkpoint 未就绪、清理超时后晚到成功、中心重启与旧 generation 回传。验证 profile 和 attempt 不重复应用，旧 owner 无法完成新任务。

验收：正常单 binding 完成路径只进行一次必要的 stop/等待链；收尾 SQL 次数和耗时下降，恢复语义与静止保障不变。

## 5. P1-C：按载荷证据优化整频道编码与分片

先由 P0 统计载荷分布。若多数任务只有一个很小分片，本项排序低于事务和收尾，不能以“大对象很昂贵”预设显著收益。

### 输入发送

已确认：`WholeChannelStore.input()` 在持 task 锁时调用 `wholeChannelParts(input_json)`；每取一个分片都重新规范化、哈希并生成整个输入的全部分片。

实施分两步：

1. 同一 command/generation/input_sha256 的不可变输入只编码一次，仅将请求分片转为传输格式，避免为每片创建全部 base64 字符串。
2. 经独立测试后，将纯编码计算移到持锁区间之外：取不可变快照、释放事务、编码、发送前按既有规则再校验 task/lease/generation/command。缓存只缓存字节和 manifest，不能缓存“仍有执行权限”的结论。增加往返的方案必须用净收益决定是否保留。

缓存按字节预算而非条数限制，可从 32 MiB 上限开始试验；超限回退原路径，同一 key 仅启动一次编码，任务结束或归档后释放。缓存命中不能绕过租约、节点身份及归档检查，不能记录敏感载荷。

### 结果接收和应用

已确认：每收到一个 chunk，`receive()` 都调用 `parts()` 读取当前所有 payload 并转 base64；最后一个 chunk 解码完整结果；之后 `result()` 在应用事务内再次拼接解码。

第一步只消除确定的多余工作：非最终片仅查 metadata/count 判断是否齐备，不加载之前全部 payload；在最终片收齐时才读取完整内容。数据库内部 Buffer 验证走 bytea 路径，省掉 base64 往返，但保持单片长度/hash、总 manifest/hash 和身份校验。

最终拼接解码从锁内移出属于后续高风险子项：先取得不可变、完整候选；锁外计算；重新进入事务核验同一 manifest、全部分片、live lease/generation、业务 fence 和未归档状态后，才原子标记 received。外部 durable/complete 的既有语义不变，绝不能先标 complete 再异步验证。完整结果应用与业务 Observation/领域状态的原子提交仍保留。

验证：单片/多片、大对象、乱序、重复、冲突、缺片、损坏、最终片并发、历史 owner 重放、归档竞态、解码时切换 generation。衡量每任务编码次数、读出字节、锁持有时间、CPU 与内存净变化。

## 6. P2：让状态等待不反复占用业务写锁

已确认：`incrementalCoordinator.awaitCommand()` 通过通用 query→withTransaction→channelStore.transaction 读取命令状态；没有现存外层事务时，会获取 task FOR UPDATE、执行业务 fence 并更新 coordinator_until。即使只是等待，也走完整写事务。

本项在 P1 后仍有明显 command_wait 开销时实施，不是直接删除 fence：

1. 独立的有界状态读取只用于判断“是否值得进入消费事务”，以同一快照核对 task/command/generation/coordinator/lease。读取不能触发业务写、授权网络或认定完成。
2. 收到 received 后，通过原有事务重新核对所有权和业务 fence，再读取/采用结果；不能直接使用锁外状态写业务。
3. coordinator 续约与读操作解耦，采用条件 UPDATE 校验当前 owner/generation/live lease，不能复活已过期 owner。60 秒租约下，15–20 秒只是初始续约间隔候选，还须包含池等待、事件循环延迟、数据库时钟和安全裕量；续约失败停止新副作用并走原恢复路径。
4. 继续先订阅通知再读状态，保留通知丢失、LISTEN 断线、超时重查和取消行为。网络等待期间不持数据库连接。

验证：通知先到/后到/丢失、双协调器、旧 generation、续约失败、事件循环停顿、中心重启、API replay、国家切换、read timeout 后连接晚到。用相同场景比较锁获取次数与 heartbeat 延迟。

## 7. P3：连接池与进程规模只做条件实验

### 主池小步调节

把主池 max=12 改为有范围校验的配置项，默认仍为 12。先确认 PgBouncer 实际 backend 配额、各池总预算和等待位置，再在保持 55 worker 下单独试 12→16；只有有效吞吐和等待同时改善且数据库锁/I/O 不恶化时，才考虑 20。

心跳池=4、结果池=8 和监督锁直连会话单独计量，不能为缓解主池排队任意挪用。主池 busy 且 CPU 高时，更多连接可能反而增加主线程处理量和数据库竞争。

### 主线程仍满时

由 profile 决定是否将纯编码、哈希、解码交给有界 worker_threads；传输 Buffer 而非克隆大对象，计算期间不持 SQL 连接，队列按任务数和字节双重限额，取消结果不得越过 generation fence。没有 profile 支持时不默认新增线程池。

多中心进程属于后续架构项目：必须明确槽位归属、监督锁、NATS 消费、gateway、后台维护和部署控制的唯一责任。不能仅把同一中心容器复制两份来获得双核吞吐。

## 8. 测试、发布和回退

建议拆为四批：

| 批次 | 内容 | 放行条件 |
| --- | --- | --- |
| A | 低开销计量＋合并事务设置 | 事务设置等价、隔离库/PgBouncer 验证通过，额外观测开销可接受 |
| B | 收尾单次确认 | stop/checkpoint/recovery 故障场景全部通过，收尾成本有实测下降 |
| C | 载荷与等待热点中经 profile 确认的一项 | 单项前后对照有净收益、所有权及原子提交测试通过 |
| D | 主池 12→16，必要时 20；随后才试 worker 55→60 | 参数一次只改一个，中心/数据库/吞吐指标同步改善 |

可复用测试入口与测试文件：`test:remote-center`、`test:remote-progress`、`test:remote-progress:docker`、`test:remote-handoffs`、`test:remote-fence`，以及 `remoteChannelStop.postgres.integration.test.js`、`remoteWholeChannel.wan.integration.test.js`、`wholeChannelRecovery.test.js`。涉及真实事务/锁的行为使用隔离 PostgreSQL，不用模拟 client 替代验证；修改到 whole-channel 协议时补充乱序/崩溃重放用例。实施时以实际 package.json 命令为准，生产只运行只读验收。

先在隔离环境使用可重复的 About/Video 混合样本跑基线与故障注入，再发布到同一中心的受控配置；逻辑支持按槽位启用时先选 5 个槽位，事务基础设施等进程级变更按整个中心回退，不能伪称仅影响 5 个槽位。新旧分组共享数据库和 CPU，分组比较只是初筛，还要做切换前后窗口及必要的回切验证。

每次至少观察 30 分钟，使用同类任务、相近视频数/载荷/节点/失败比例，并独立记录下游 backlog。以独立 Plan cohort 对账：包含最终成功、partial、handoff、失败、仍在执行，不只看最快完成的样本。

第一阶段工程目标（不是收益承诺）：中心主导平均时间从 20.25 秒降至 15–16 秒；在相近工作量下有效吞吐提升 10–15%，或显著增加中心资源余量。20.25→16 且采集 14.58 秒不变时，执行段由 34.83 降至 30.58 秒，理想槽位周转增幅约 13.9%；真实效果仍需实测。单补丁可因明显降低资源成本而保留，但不能据此宣称吞吐已提升。

建议的性能回退判据：可比窗口吞吐持续下降超过 5%、执行 P95 上升超过 20%、数据库/租约超时增加，或池等待与主线程负载恶化而无完成量收益。阈值需用 P0 自然波动校准；普通任务复杂度变化不能触发误回退。任何重复业务应用、失去所有权后写入、网络未静止即复用槽位等正确性问题立即停止扩大范围并按既有暂停/排空流程回退。

第一批不做破坏性 schema 变更。保存旧镜像、配置和基线；回退关闭优化开关或恢复旧镜像，使用正常排空和恢复机制，不强杀正在持有执行权的任务。新旧路径必须可读取相同持久化结果与 checkpoint。

## 9. 不纳入本轮收益承诺的事项

路由/业务预算耗尽、STALE_LEASE 的根因排查另列专项，与中心优化共用重试和超时指标，但不假设全部由中心排队引起。降低 dispatch buffer 主要改变等待位置；提高预算、延长超时、删安全校验或增加 worker 数不作为本轮中心优化的替代方案。
