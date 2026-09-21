# P2 第二批：全量节点注册与准入

日期：2026-09-20。承接第一批身份和 schema，本批实现共享中心入口中的全量节点登记、心跳、就绪、启用/撤销和受校验的任务领取。P2 整体仍未完成；生产全量节点未部署或启用。

后续更新：本文保留第二批实现与验证范围。真实业务校验、专用续租与详情事务已在 [P2 第三批](FULL_CRAWL_REMOTE_P2_BUSINESS_FENCE_20260920.md)实现；完整阶段消息与中心 processor 仍待接齐。

## 实现行为

`RemoteWorkerActivationStore` 根据第一批 `collectingWorkload` 合约验证 role、mode、slot 和 runtime revision。全量必须使用专用 capability `youtube.full-crawl.v1`；注册为全量的节点不能同时带增量或未知 capability，也不能复用已有另一种 role 的节点记录。

全量新注册默认为 `enabled=false / activation_requested=false`。相同登记可以重放，改变 deployment/config/role/mode 会被拒绝。心跳只能更新已登记且身份匹配的连接，不能自行登记或启用。

全量就绪检查使用独立的 `fullCrawlExecution` Adapter：

```js
new RemoteWorkerActivationStore({
  store,
  verifyExecution: incrementalSupervisorVerifier,
  fullCrawlExecution: {
    verifyExecution: async (client, connection) => { /* exact full supervisor readiness */ },
    assertTask: async (client, task, connection) => { /* lock and check full business ownership; throw on failure */ },
  },
});
```

该 Adapter 默认 `null`，同时要求提供两个方法。缺少 Adapter 时全量心跳只返回等待启用，启用报 `CENTRAL_EXECUTION_NOT_CONFIGURED`；不会调用增量 verifier。当前生产中心入口没有装配该 Adapter。W04/W06 必须实现真正的 supervisor readiness 和业务 fence 后才能装配，不能将固定返回 `true` 的测试回调用于生产。

启用事务核验节点状态、capability、slot、deployment、config hash、instance、relay boot、runtime revision、连接有效期和 accepting 状态，并在同一连接锁内调用专属 readiness 检查。需要操作员请求的调用可保留 `requireRequested=true` 校验。

## 领取和兼容入口

通用 `RemoteNodeStore.claim` 的授权回调现在同时收到已锁定的节点行，并可提供单一 capability 和事务内任务校验。全量领取必须经过全量 Adapter：

- 按 capability 限定候选任务，增量／未装配授权的通用领取排除全量任务。
- 任务必须明确指定当前 node/slot；未定向或错误定向不能交付。
- 对新领取和相同 claim ID 重放都在 task 锁内调用 `assertTask`。失败回滚，不能推进运输 generation 或写入 claim。
- 暂停关闭新领取；确切在途 claim 可以重放，但仍须通过当前业务校验。capability 撤销或实例身份变化不能重放。

完整的 candidate/run/attempt/dispatch generation 业务检查仍是 W06，本批测试通过注入可控的拒绝回调验证其强制调用与事务回滚，不声称已经实现真实业务检查。

通用工作 heartbeat 暂时拒绝全量 task，通用 results 入口报 `FULL_CRAWL_REQUIRES_CENTRAL_COMPLETION`。后续必须接入带完整业务和实例校验的续租/结果路径，不能复用目前只核对 task/node/generation 的旧路径。

旧 `connect_only` 入口不能刷新已处于采集模式或全量模式的连接，不能复活 retired 记录。

## 实例接管与撤销

活跃实例不能被另一个 instance/relay boot 覆盖。连接过期仍不代表工作已结束：已有 lease、未退役网络绑定，或全量 slot 的 pending/leased/received task 都阻止接管。两个新实例并发竞争时只能有一个成功。

允许接管时清除 `enabled`，新实例需要重新通过中心准入。节点 draining/disabled、accepting=false、显式 drain、中心 readiness 丢失和退役状态均不能产生新的合法领取。

## 验证入口与范围

新增 `remoteFullCrawlActivation.postgres.integration.test.js`，在专用 PostgreSQL 和真实 HTTP gateway 中覆盖五组场景：

1. 登记幂等、默认暂停、capability 和专用节点约束。
2. 错误节点/模式/slot/runtime/deployment/config、重复实例、旧连接协议绕过。
3. 独立 readiness、任务校验失败回滚、capability 隔离、暂停与重放、旧结果和续租入口拒绝。
4. 精确定向、中心 readiness 丢失、停止接单和节点撤销。
5. 未完成证据阻止接管、并发实例竞争、退役后不能复活。

`npm run test:remote-full-crawl` 已包含该测试；需要显式配置 `REMOTE_NODE_TEST_DATABASE_URL`，否则按现有测试约定跳过集成文件。完整回归入口：

```bash
runtime/fullcrawl-p2-preflight-20260920/run-preflight.sh
runtime/fullcrawl-p2-preflight-20260920/run-supervisor-regression.sh
```

第二个脚本在内部网络创建临时 Redis，使用 fixture password 和独立内存上限，测试执行器共享其网络命名空间以满足现有 supervisor 测试的 loopback 地址要求；不暴露宿主机端口，退出时自动移除临时 Redis。未改变原测试 Redis 或生产 Redis 的配置。

最终验证：**68 项单元测试、85 项 PostgreSQL/Redis 集成测试、3 项增量 supervisor/接单控制集成测试全部通过，共 156 项，0 失败、0 跳过**。本批五项全量准入集成场景包含在 85 项中。日志为上述 runtime 目录的 `logs/unit.log`、`logs/integration.log`、`logs/supervisor.log`；`git diff --check` 通过。所有数据库修改均在专用测试数据库内，工作区仍为未提交的 P1/P2 变更。

扩大回归时修正了两个既有测试夹具，未改变对应生产控制逻辑：

- `remoteNodeConcurrency.postgres.integration.test.js`：20 个并发领取按现有协议对 `CLAIM_BUSY` 使用相同 claim ID 有界重试，并等待所有请求结束后清理；仍断言最终只产生节点额度允许的两个租约。
- `remoteNodeExecutionControl.postgres.integration.test.js`：暂停后的额度调整先断言仍暂停，再显式启动。`intakeControl.js` 和既有 `remoteIntakeContention` 测试已明确“配置数量”和“开始/暂停”独立，原夹具误将额度调整当作启动。

本批 HTTP 测试不等于 NATS/WSS 的全量执行验收。完整业务采集、全量续租、结果入库和 API 接续仍未接入。

## 下一步

第二批之后的 W06 核心业务 fence 与详情事务已完成，见第三批交接。当前下一步是补齐 W05 的阶段消息与中心解码，再装配 W04 中心 processor。生产部署继续留在后续验收阶段。
