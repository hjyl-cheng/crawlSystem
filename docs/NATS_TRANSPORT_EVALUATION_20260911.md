# NATS / JetStream 独立验证：2026-09-11

后续正式实现与部署已完成，见 [部署记录](NATS_REMOTE_DEPLOYMENT_20260911.md)。下文保留最初隔离实验时的结论与范围。

建议采用官方 NATS + JetStream 作为远程节点通信层的候选实现。独立验证已经通过，但尚未接入正式 Worker；当前页面 503、Clock 历史任务恢复问题不能据此宣称已修复。

这次只新增隔离实验文件，没有重启生产服务、修改生产表、派发实际频道或改变原有增量/迁移策略。实验容器、数据库和消息卷已在结束后清理。

## 实测结果

执行：`cd services/remote-node/experiments/nats-transport && npm ci && npm test`。

使用官方 NATS Server 2.12.15（镜像固定 digest）、官方 `@nats-io/transport-node` 和 `@nats-io/jetstream` 3.4.0。NATS 与独立 PostgreSQL 各限制为 0.5 CPU、256 MiB，两个模拟节点连接承载 100 个并发逻辑 Worker。使用回环网络和合成结果，每条 gzip 结果约 16.7 KB；复用实际的 `RemoteChannelPlanStore.receive` / `RemoteNodeStore` 及其 PostgreSQL 表和结果协议。

| 项目 | 实测 |
| --- | ---: |
| 100 个逻辑 Worker 指令—结果—接收入库 | 1.34 秒 |
| 后续 1,000 条不同结果、100 个并发发布者：消息接收确认速度 | 2,534 条/秒 |
| 同一批结果：原有 SQL 接收代码处理速度，8 个并发消费者 | 96.7 条/秒 |
| 消息接收确认 P95 | 59.5 ms |
| 负载期间实际 SQL 心跳续租次数 | 138 |
| 心跳续租 P95 / 最大延迟 | 84.8 / 530.2 ms |
| 心跳失败 | 0 |
| 最终不同结果记录数 | 1,100，符合预期 |
| 实验客户端事件循环 P99 延迟 | 14.8 ms |
| 数据库连接池最多等待请求数 | 1 |

消息接收确认只表示 JetStream 接收，不表示 SQL 已处理或频道已完成。不能把以上数字换算为频道/小时，也不能作为最大容量或相对 HTTP 的性能倍数：这里未进行相同条件下的 HTTP 对照，PostgreSQL 也受到 0.5 CPU 限制。NATS 约 9.6 MiB 的内存读数来自测试结束及重启后，不是峰值内存。

## 故障与完整性验证

全部通过：

- 相同消息 ID 重发：JetStream 识别重复发布。
- SQL 已提交、消费者在 ACK 前断开：同一持久化消费者重新收到结果，原有接收代码幂等处理，不产生第二份记录。
- 旧执行代次：原有接收代码仍拒绝 `STALE_LEASE`。
- 相同结果身份、不同内容：仍拒绝 `BATCH_CONFLICT`。
- 节点 1 冒充节点 2 上报或读取其指令：NATS subject 权限拒绝。
- 消费停止后的 40 条积压，在 NATS 进程 SIGKILL / 重启后仍可处理；官方客户端重新连接，原有本地 spool 保留并重放未确认结果。
- 消息流达到容量上限：使用 `DiscardNew` 拒绝新消息，不挤掉已经接收的旧结果。

最后一轮完整测试返回 PASS，详细数值见 [results.json](../services/remote-node/experiments/nats-transport/results.json)，可运行代码与边界说明见 [README](../services/remote-node/experiments/nats-transport/README.md)。

## 正式接入的范围

继续由 BullMQ 管频道任务与重试，原来的 Plan runner 管采集策略，Rota 管采集网络。NATS 提供官方长连接、权限和状态通信，JetStream 提供持久化指令及结果队列。不要再实现一套频道分配器。

需要编写的是有限的业务适配：

1. 原有指令 SQL 提交后，由持久化 relay/outbox 投递 JetStream。不能把“写 SQL、发消息”当作一个不会失败的双写操作。
2. 节点接收指令时仍核对有效的任务、执行代次、接单和网络归属，使用现有采集器、本地 spool 与稳定结果 ID。
3. 中心消费者并发调用原有结果接收代码，提交成功后才 ACK；原有增量业务写入、Clock 收尾、BullMQ 完成确认仍按其各自栅栏执行。
4. 单独处理心跳续租，监控积压量、最老消息年龄和入库延迟；结果积压过大时降低接单量，不把数据库压力转成无界内存或磁盘占用。
5. 节点初始化时配置独立凭据、TLS 和权限，接入现有页面与镜像部署；退出节点时撤销权限。

尚未验证完整 Clock 生命周期经过 NATS、公网 TLS/弱网、多实例消费扩容曲线、主机掉电或多副本故障。此次单副本 SIGKILL 测试只代表进程重启恢复，不能代表磁盘丢失/断电也不丢数据。生产启用前需要明确副本、存储同步及保留策略，并验证新旧通信方式对同一执行只能有一个所有者。
