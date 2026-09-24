# 增量重复失败修复发布记录

2026-09-24，时间均为 UTC。中心确定性修复已发布；原功能灰度完成 30 分钟观察后，经用户明确授权，node01/node02 的 55 个 worker 已全量部署并恢复就绪。业务总预算误重试的确定性缺陷已修复，网络根因尚未全部解决。

## 30 分钟验收结果

发布前固定窗口 06:07:00–06:37:00；中心 B 固定窗口 06:48:45.080–07:18:45.080，排除两次发布暂停/排空时间。最终 07:19:24 中心 healthy、0 次重启；两队列未暂停，增量 active=55、waiting=54、delayed=0。远端 55 个容器的最后一次清单核查均为 0 次重启。

| 指标 | 发布前 | 中心 B + 两槽灰度 |
| --- | ---: | ---: |
| 去重 completed Job | 4,814 | 5,249 |
| completed/分钟 | 160.47 | 174.97 |
| 完成 cohort 中查询时 succeeded Plan | 4,772 | 5,216 |
| succeeded Plan/窗口分钟 | 159.07 | 173.87 |
| completed 执行段 P95 秒数 | 30.08 | 28.37 |
| 队列失败尝试 | 357 | 87 |
| 单次执行预算失败 | 347 | 83 |
| 队列 STALE_LEASE 失败 | 0 | 0 |
| 底层网络执行 SSL35 / Proxy56 失败 | 898 / 143 | 57 / 218 |

completed 吞吐观察值提高约 9.0%，但任务和网络条件不是严格对照，不能全部归因于补丁。成功 Plan 的 About/Video/About+Video 数量分别为前 3,190/496/1,086，后 3,540/528/1,148，任务构成略有变化。底层 SSL 与 Proxy 故障构成变化很大，本次仅改了两个远端槽，不能把全体 SSL 错误下降认定为已修复 TLS 根因。completed 执行段时长不含队列退避，不是端到端 Plan 延迟；Plan 状态为查询时投影状态，并非截点状态。前 cohort 另有 42 个 cancelled；后 cohort 另有 31 个 cancelled、2 个 partial，故 completed 不等同于成功 Plan。

中心 A/B 合并观察中，1 笔自然发生的业务总预算错误正确归类 `business_run_budget_exhausted / none`，只产生 1 次队列失败事件。B 窗口本身没有新增自然总预算耗尽样本；不能将“误重试为 0”包装成大量同类线上样本的证明。另有 1 笔历史补偿，二者均已成为 failed Run / terminal binding，预算失败 Observation 每个应结算领域各 1 条，终态后网络执行为 0。该结论同时有真实 PostgreSQL、BullMQ、Rota 隔离回归支撑。

灰度 node01 / incremental-38 完成 102 个（3.40/分钟），出现 2 次单次执行预算失败；node02 / incremental-1 完成 106 个（3.53/分钟），无队列失败。每个 worker 的固定窗口对照见 `incremental-worker-speed-repeat-fix-20260924.csv`。未因灰度观察结束而越过源码传输审批扩大部署。

日志保留的最后约 5 分钟（07:13:20–07:18:40）有 33 份性能样本：中心进程平均 CPU 约 1.06 核、事件循环利用率均值 98.85%，主池排队峰值 80，心跳/结果池排队峰值均为 0。这里的 CPU 是进程采样，不能当作主线程精确计量。仍无充分中心余量支持直接增加 worker；消除无效预算重试并不等于中心瓶颈已解除。

## 已修复及实际部署范围

1. 共享错误分类遍历完整 cause/errors 链且防环，优先隔离失去所有权的旧执行。业务总预算耗尽返回 `business_run_budget_exhausted / retry_mode=none`，不会再被 `ProxyControlRequestError` 类名覆盖为系统重试。单次执行预算耗尽仍在业务总预算内使用原队列退避机制。
2. 增量专用终结操作校验冻结 Plan、dispatch、binding、Run 和 attempt 身份，检查旧网络执行已结束且 release receipt 为零在途。数据库短事务写入 Run 终态、幂等领域 Observation 和 binding terminal；提交后才向 BullMQ 返回不可恢复错误。结算失败保留可重试语义。
3. `IncrementalRunStore.claim()` 读取持久化终止标记及 terminal binding，阻止旧 Job/重复投递唤醒已耗尽预算的业务运行。普通可恢复失败仍可续跑；已有 complete/partial/queued 领域、合法 Agent 和 API 后续工作受保护。
4. `copyright_removed` 纳入内容终态分类，避免因已确认版权移除反复尝试客户端和换路。该改动已进入中心镜像及全部 55 个远端 worker。
5. 新增脱敏传输诊断（curl code、固定错误签名、已知阶段、端点类别、耗时、session reset），以及租约拒绝的 owner/generation/state/reason 证据。中心诊断已部署；网络诊断及补充的 CONNECT 数字响应码已在全部 55 个远端 worker 启用。

未扩大网络预算、队列预算、worker 数量或连接池。Rota 仍是业务网络预算唯一权威。预算终止针对本次业务运行，不永久禁用频道。

## 用户授权后的 55 个 worker 全量发布

用户回复“1”明确授权后，07:22 开始继续发布。四个补丁文件与本地已测试源码哈希一致，在两台节点分别保留既有两种基础镜像构建候选；无网络导入检查通过。V2 仅在已完成 30 分钟灰度的功能补丁上补充 CONNECT 数字响应码。两个 V2 槽恢复后先完成 21 个任务、无队列失败，再扩大覆盖。

分批更新数量为 2、6、11、11、10、9、6，共 55 个。每批保存原领取选择，暂停目标槽的新领取，等待无未结算任务、无活跃网络绑定，并检查 spool 无待回执及网络已关闭；正常停止旧容器并确认退出码 0，保留旧容器和配置，再启动候选及恢复原选择。后续批次增加“中心确认登记了新实例身份”作为恢复领取前置条件，避免过早为旧实例创建执行 owner。没有强杀或手工绕过围栏。

最后一批于 07:53:18.251 全部就绪，07:53:46 完成逐槽核验。07:54:50–51 最终中心接口确认：node01 47/47、node02 8/8 已连接、启用、请求领取且 readyForTasks=true，原 selected_slots 精确恢复；两队列未暂停，增量 active=55。中心 B healthy、重启 0 次。

55 个旧进程退出码全部为 0，55 个新容器逐槽应用健康检查通过，四文件共 220 项 SHA-256 校验全部一致。最终容器清单再次核对镜像和 V2 标签，55 个均匹配、重启 0 次。各槽现有 Compose 的 image 已更新到固定候选 ID；原配置和旧容器保留在两节点 `/var/lib/qy-repeat-fix-20260924-v2/<slot>/`，本轮未执行回滚。

| 节点 | V2 镜像 ID | worker 数 |
| --- | --- | ---: |
| node01 | `sha256:1b07d99e74d6cd5f981cee52ca35788ac45adaf66431fa8d6d6e6cd9b5a1949d` | 35 |
| node01 | `sha256:850af44ae200957c157e2de81aa1eb65268784bfd8007869bcdf19f5ed71bb2e` | 12 |
| node02 | `sha256:a9d2edaf6fa04781ed5ea9aff72af0ad5d5b61d125c1e27ba682bde923398f71` | 6 |
| node02 | `sha256:e93a83506f5e7808d0ddb7e59458a0a6d7483c916ed3f697de21f305c9866cc2` | 2 |

截至 07:58:55，53/55 个槽在各自恢复后已有 completed 记录，合计 2,628 个完成、106 次失败尝试。各槽恢复时间不同，此数只用于证明执行恢复，不计算吞吐对照。node01 / incremental-26、incremental-50 在各自恢复后的窗口仍未出现 completed，存在代理/TLS 失败，不能把“55 个槽就绪”当成“55 个槽都抓取成功”。针对槽 26 的限定查询中，14 次队列失败对应 14 个不同 Job；诊断包含 SSL35 与 CONNECT 502，而不是预算耗尽后的同业务唤醒。槽 50 的单独查询含发布前成功样本，因此最终完成判断采用各自恢复时间的全槽查询。

V2 观察区间从 07:27:31.958 持续到约 07:59，包含滚动维护时间；两个灰度槽分别完成 108 和 90 个任务，后者有 4 次失败。全队列该区间记录 191 次单次执行预算失败、5 次上游暂时失败，没有业务总预算误判系统重试或队列 STALE_LEASE 失败。新增诊断包含 136 条 CONNECT 502、178 条 curl35，以及少量其他传输诊断；这些是诊断条目，不是全部请求的失败率。

`nodeforward/relay.go` 的 502 来自建立上游连接失败分支。这进一步定位了部分代理错误发生的阶段，但没有确认具体代理供应商、上游认证响应或 TLS/profile 根因。原 Rota 隔离、冷却及健康检查机制保持生效；本轮未为消除错误计数而放大预算、重置 Business Run 或增加 worker。

07:54:54 再次复核中心两笔预算终态：每个待失败领域仍只有 1 条预算 Observation，终态后网络执行仍为 0；线上自然预算耗尽 Job 仍只有 1 次不可重试失败。核心预算缺陷修复和远端补丁覆盖完成，代理/TLS 根因修复仍未完成，不能宣称所有重复失败已消失。

远端回退使用 `remote-rollback-v2.py/.mjs`，先按同样的中心槽位暂停、排空流程执行；仅恢复目标槽的原容器及 Compose image，保留其他槽的版本，随后恢复原选择。部署脚本、每批正常退出/恢复/核验、最终清单和诊断证据均在 `runtime/incremental-repeat-fix-20260924/v2-*`。

## 发布状态

- 中心 A 恢复队列：06:39:44.729。首笔线上总预算耗尽错误已正确不可重试结算，Run/binding/Plan 状态收敛，没有后续网络执行。
- 中心 B 在 A 基础上使用已有 channel 索引约束失败结算/历史查询，并明确写入未完成领域的 failed 状态。06:44:57 暂停领取并自然排空，旧中心退出码 0；新中心 06:48:44 healthy，06:48:45.080 恢复队列。
- 当前中心镜像：`qy-allpachong/remote-node-center:incremental-repeat-fix-20260924-b`。
- 镜像 ID：`sha256:b2efd7890424b696b3d49a702d95617b1e106db57183d4bb214f74b00200cd48`。
- 07:05 核对中心 20 个修复文件 SHA-256 全部一致；07:04:39 健康检查 healthy、0 次重启。
- 两个远端灰度：node01 / incremental-38，node02 / incremental-1，约 06:47 完成正常退出及替换，无强杀。分别保留既有两种基础镜像的其他行为；原容器、spool、精确创建配置及 Compose 快照均保留。
- worker 总数维持 55（node01=47，node02=8）。中心持久化镜像引用及 Compose overlay 已更新到 B。

## 验证

- 直接运行的相关 JavaScript 单元回归共 201 项通过，含真实 ProxyControlRequestError 分类及版权移除的真实 fallback 入口；修复前失败证据已保存。
- PostgreSQL/BullMQ/NATS 预算和围栏集成 42 项通过，零跳过。
- 真实 Go Rota 九次业务预算、中心 SIGKILL/API/whole receipt 恢复集成 33 项通过，零跳过。九次额度耗尽后拒绝第十次网络执行。
- 最终增量预算集成 10 项通过，零跳过，包含提交回滚、持久终态重投、真实中心/BullMQ 不再排下一次重试、普通失败续跑、旧 dispatch 拒绝、Agent 结果保留、待完成 API 保护、租约与零在途回执保护。
- 最新 CONNECT 数字响应诊断：JavaScript 7 项及 Python 4 项通过。此补丁已随 V2 完成远端全量发布。
- 上述测试集合有重复覆盖，不将各批次数量相加宣称独立测试总数。线上 30 分钟观察期间约 07:03 运行过一批小型隔离集成测试，属于共享主机负载干扰因素。

## 历史补偿

工具 `services/qybullmq/scripts/reconcileIncrementalBudgets.mjs` 默认 dry-run，要求显式不超过 24 小时时间窗、每批最多 50 条。仅处理有明确总预算耗尽事件、Run failed、binding materialized、队列 failed/missing 且 Rota 权威预算确实耗尽的记录；不重置预算、不修改队列状态。

本次范围 04:00–06:37，dry-run 确认 1 笔，06:49:20.868 提交结算：Plan `7c170ed7-00a2-5f01-9691-2ec76dea3fe4`。07:01:17 同范围复查 `examined=0`。这表示该限定范围的可补偿记录收敛，不代表所有历史失败已清空。

## 网络与租约证据及限制

两灰度槽初期 6 笔新增诊断中，5 笔为 CONNECT tunnel failure，1 笔为 TLS 握手错误。逐 Task 关联 Rota Observation/report/lifecycle：6 笔均选择 rotate_route，且都存在 task_observation_quarantine 事件；部分代理通过后续健康检查恢复后，再次被真实任务隔离。不能据此说隔离链路没有执行，也不能只凭 curl 35/56 判断供应商、认证或指纹根因。

补充的数字 CONNECT 响应码有助于区分本地 relay 的 407、429、502、409；自动审批两次拒绝向远端构建节点传输源码，因此未再次尝试或绕过。用户随后回复“1”明确授权向现有 node01、node02 传输部署补丁；审批通过后已完成 55 个 worker 覆盖，未绕过原拒绝。

部分 `remote_stale_lease` 日志来自 state=applied/failed 后的迟到 RPC，同 owner/generation，属于任务已结束的拒绝；不能把日志条数等同于租约过期导致的抓取失败。另有 pending 状态 owner 已清空的拒绝，尚不足以判定发生真实租约过期。任务失败以 PostgreSQL 的持久事件为准，Docker 日志轮转可能只保留请求观察窗口的一部分。

B 窗口另 4 次 unknown/default 失败均来自同一个 Job 返回 `This channel is not available.`。该信息不等同于已证实的版权移除或频道永久删除，未扩大永久终态规则；它仍是现有策略下的有限重试，需单独取得目标内容证据。本次不能宣称所有重复失败都已消失。

07:20:21 最终复查两笔预算终态、幂等领域事件及终态后网络执行，结果与上文一致。该时点的远端覆盖和 CONNECT 响应码发布阻塞已在用户授权后解决。目前仍需根据更多诊断证据确定代理/TLS 根因及必要修正；租约真实过期根因仍未证实，不凭本窗口零失败宣布永久修复。

## 回滚

回滚前暂停新领取，重新确认两队列 paused 且 active=0，并在有效的近期排空证据下正常关闭中心。B 回退 A 保留本次终态保护；若需要回到前一次中心优化代码，使用已构建的兼容镜像 `qy-allpachong/remote-node-center:incremental-repeat-fix-20260924-rollback` 及 `runtime/incremental-repeat-fix-20260924/rollback-compatible.py`。

不得直接恢复不识别 budget terminal marker 的原 C 镜像，也不得使用旧 A 发布脚本的普通 rollback 分支退回原 C。持久终态标记出现后，回滚也必须保留 claim 读取保护。本次未执行回滚。

## 证据

- `runtime/incremental-repeat-fix-20260924/`：修复前复现、201 项单元回归、围栏/Rota/崩溃恢复集成、A 发布、两槽灰度、55 槽 V2 全量发布及逐槽核验、历史 dry-run/apply、兼容回滚。
- `runtime/incremental-repeat-fix-20260924-b/`：B 发布、源码核对、健康、持续观察、Rota 隔离关联、历史收敛、10 项最终预算集成。
- 凭据和完整容器配置只留在 private 备份，不写入此报告。
