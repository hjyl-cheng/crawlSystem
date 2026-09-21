# P3：节点采集、传输与恢复验收

日期：2026-09-20。范围：执行计划 W07～W11；仅隔离环境。状态：P3 已完成隔离验收；下一阶段为 P4。

## 实现

| 工作项 | 实现与边界 |
| --- | --- |
| W07 节点执行 | `runRemoteNodeFullCrawl.mjs` / `nodeFullCrawlRuntime.js` 使用 `full_crawl_collect`、`full-crawl-N` 与独立运行时版本。复用连接就绪门禁；一个进程、一个 spool、一个浏览器会话。 |
| W07 本地日志 | `fullCrawlNode.js` 逐视频登记稳定 start ID，中心确认后才请求详情。结果先 fsync，再分块上传；全 spool 共用额度。磁盘不足时在详情开始前停止，未完成的日志尾部可恢复。 |
| W08 传输与入库 | 独立 JetStream `QY_REMOTE_FULL_CRAWL_RESULTS`，File / Workqueue / DiscardNew；节点仅能发布自身全量结果主题。TLS NATS 和 WSS 都使用同一协议。SQL 持久回执确认后才释放本地证据；业务结果与 applied 标记仍在同一事务提交。 |
| W09 网络与 profile | `fullCrawlTransport.js` 将原 Rota Task、全量所有权和原 BrowserProfile 接到共享路由/session 存储；节点用签名授权配置真实本地 relay。close_fetch 只在浏览器请求清空、checkpoint 交付、relay 退役确认后返回。Cookie 回写检查原候选、任务、attempt 与 profile revision。 |
| W10 API 与国家交接 | v3 节点保留原错误、部分详情与实际 parser 重试；中心原 fallback 决定是否允许 API，节点不创建 API 请求。API / country 回执持久化为 received，实际停网后释放槽位。API 稳定 request ID 可恢复丢失的 BullMQ continuation；国家复查在 SQL 已提交而控制确认丢失时重放原请求，不伪造 checked/unavailable；纯 API 回放不发起采集、不重复增加详情尝试。剩余网络步骤返回原 managed 流程。本地 Worker 已创建的 continuation 保留显式本地无网络回放。v1/v2 不获得 v3 权限。 |
| W11 恢复 | 旧执行只可补交绑定原 task / generation / node / connection 的原始证据，不能续租、登记新 started、申请网络或写业务。中心恢复必须持有原 supervisor advisory lock；只在实际退役或从未签发授权时结束 attempt。新所有者复用同合同、同目标集合的已持久化详情；缓存通过有界 RPC 分块读取，不塞入阶段命令。未取得原始结果的 started 仍保留已消耗的次数。 |

`fullCrawlCoordinator.js` 继续驱动原 `createFullCrawlYoutubeJsExecutor`，资格判断、内容类型、时间窗口、内容写入和完成交接没有搬到节点。原本地 Collector 保留。共享完整性修正使同一所有者恢复时不会把节点已经 started 的候选重置为 queued。

## 隔离装配

生产入口没有导入这些全量装配；没有修改生产 schema、队列、节点或配置。

1. 在已有 crawler 测试 schema 上执行 `REMOTE_NODE_TEST_DATABASE_URL=... node scripts/prepareFullCrawlP3Schema.mjs --apply`。脚本验证数据库必须为隔离测试库，没有生产 `DATABASE_URL` 回退。
2. `createFullCrawlCenter` 接收 `transportOptions`：原 Rota reader、签名私钥、加密密钥，以及实际 NATS 连接健康谓词 `isReady`；保留原 policy、profileSecret、Rota client、handoff 和兼容 processor。配置 `createApiFallback` 为原 `createVideoDetailApiFallback` 与中心 API 设置读取器。
3. `startRemoteNatsCenter` 的 `fullCrawls` 使用装配返回的 `transport.service`。全量 supervisor 仍负责原 `youtube-channel-crawl` BullMQ 消费和 Rota 生命周期。启用顺序仍受中心就绪与节点 activation 门禁约束。
4. 节点运行 `npm run node-full-crawl`。relay 必须显式登记 `-slots full-crawl-1` 等全量槽位。WSS 使用既有 `/node-messages` 路径；没有 HTTP 或直接网络回退。

本次复用 P2 内部 Docker 网络和测试 PostgreSQL/Redis，另建独立 TLS/WSS NATS。P2 基础容器实际存在随机 **loopback** 发布端口；P3 新 NATS 没有发布主机端口。P2 早期文档的“无主机端口”不能视为当前实际配置。

## 验证

| 验证集 | 通过 | 失败 / 跳过 |
| --- | ---: | --- |
| P3 与共享运行时 | 53 | 0 / 0 |
| P2 单元回归 | 161 | 0 / 0 |
| P2 数据库集成回归 | 119 | 0 / 0 |
| 真实 BullMQ / supervisor | 4 | 0 / 0 |
| 最终国家恢复与本地 continuation 兼容复测 | 15 | 0 / 0 |

各组含重复共享回归，不作为去重用例总数。验证记录和源文件摘要见 [验证记录](../reports/fullcrawl-p3-validation-20260920.json)。`git diff --check` 通过。

边界：阶段输入 64 KiB；结果分块 512 KiB；单批 8 MiB；一组详情最多 20 项；节点默认 spool 上限 256 MiB。测试使用超过一个分块的大结果，并注入磁盘压力，确认没有在无法保存结果时先增加详情尝试。

新增验证位于：

- `test/remoteFullCrawlNats.postgres.integration.test.js`：真实 NATS/WSS、SQL、中心 managed runtime、加密 profile/session、签名授权、Go relay 与节点 executor；大结果分块、SQL ACK 丢失、中心 consumer 重启、重复结果、分块冲突、主题 ACL；API / 国家交接前实际停网及槽位释放。
- `test/remoteFullCrawlNode.postgres.integration.test.js`：逐视频计数、预检查跳过、spool 背压、截断日志恢复、过期证据重传、错误链保留、中心所有权恢复、已抓详情复用、同所有者续跑、API continuation 恢复与无网络回放。
- 新旧 Collector 对相同受控观察得到相同候选、频道、Run、内容、详情状态与尝试次数。
- 共享节点进程、YouTube runtime、增量网络/session、原完整频道业务和 P2 supervisor/processor 回归。

验收使用受控 YouTube 观察值，不是公网 YouTube 抽样。NATS/WSS、数据库、签名、relay、节点/中心生命周期和业务入库是真实组件；真实 YouTubeJS 会话及 parser 的受控传输测试在共享运行时回归中运行。此验收不代表生产部署或公网容量验收。

可复现测试脚本与日志：`runtime/fullcrawl-p3-20260920/`（本地忽略目录）；P2 回归日志保存在 `runtime/fullcrawl-p2-preflight-20260920/logs/`。

## 后续阶段

P4 才处理全量镜像/安装包、部署登记、资源计数、管理页预览、接单与排空/删除。上线仍需在 P4 检查部署形态和生产配置；本次没有开启生产全量远程接单。
