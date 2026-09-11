# Release 与 bugfix 分支归并记录

将 `agent/bugfix-optimization` 的 `ad76f5e19bb46870e5c6670de3023e718d6cf46a`（相对共同基点 `1bc2edd` 的六个提交）合入 `agent/incremental-migration-release`。归并前先保存 release 的既有成果：`8bf80ef` 保存受限搜索副本清理工具，`cd4d410` 保存多服务器与节点部署功能。运行凭据、镜像数据和数据库数据未提交。

唯一双方同时修改的文件为 Dashboard `server.js`：节点管理接线与迁移库存读取修复位于不同位置，Git 自动合并成功。此前生产 Dashboard 使用叠加镜像保留的迁移修复现已进入统一源码；后续可直接从本分支普通 Dockerfile 构建，不必继续依赖叠加安装脚本。

归并调整限于两处测试及说明：

- 网关配置测试同时识别 Beszel WebSocket 和节点执行 API 的独立 HTTP/1.1 配置，继续拒绝与公共代理参数重复定义。
- 远程监督器集成测试在空测试库中补齐原业务 schema。最小复现返回 `42P01: relation crawler.channel_execution_attempts does not exist`；补齐业务 schema 后同一查询正常返回。原测试依赖其他用例预先初始化数据库，生产恢复逻辑没有修改。

## 验证

- 主采集 Node 测试：1712 通过、195 因外部依赖未配置跳过；Dashboard：59 通过、5 跳过；Auth：11 通过；Feature Dispatch：26 通过、2 跳过。
- 本地 Agent Python 测试：135 通过、4 跳过，另有 2 个子用例通过；Feature Engine：208 通过、17 跳过，另有 90 个子用例通过。Go 全包测试命令通过，未配置数据库的集成用例仍按原规则跳过。
- 定向真实 PostgreSQL/Redis 集成覆盖迁移持续补任务、暂停、恢复容量与 generation、API 全局批次数限制、中心独占、API 回放以及强杀恢复。首轮 39 项中 38 通过，1 项因上述空库准备缺失失败；修正后在重新创建的空库单独重跑该项通过。另两项 Dashboard 数据库集成通过。因此定向覆盖最终 41 项均通过，没有跳过。
- `scripts/verify.sh` 与差异空白检查通过；上游 CSV 保留原 CRLF 换行，暂存区检查使用 `core.whitespace=blank-at-eol,blank-at-eof,space-before-tab,cr-at-eol`，未改写原始测量数据。完整测试脚本在 Feature Dispatch 阶段因本地缺少 pg 依赖中止；按锁文件补齐依赖后续跑该阶段及 Python、Go，未重复运行已通过的前面阶段。

测试复用现有 Python 开发依赖，主要测试进程降低优先级并限制在两个 CPU。数据库与 Redis 使用本轮单独创建、仅绑定 loopback 的临时容器，没有连接正式数据库或队列。测试结束清理临时容器与本轮开发环境链接。

日志位于 `/tmp/release-merged-full-tests-20260911.log`、`/tmp/release-merged-remaining-tests-20260911.log`、`/tmp/release-merge-integrations-20260911.log`、`/tmp/release-merge-fixture-and-dashboard-tests-20260911.log` 与 `/tmp/release-remote-fixture-diagnosis-20260911.log`。

## 当前迁移与部署

本轮仅提交、归并、测试及只读核对，不重新部署正在运行的服务。线上代码比对确认 Dashboard 与合并源码的业务逻辑一致，入口仅两个 import 的位置不同；Controller 的吞吐修复已经上线。Controller 中的剩余差异为共享 API 设置读取的等价提取与函数 export，API/中心镜像还包含各自非入口模块的旧副本，没有据此统一重启所有角色。

后续如需统一镜像版本，应保留现有角色配置与 schema 开关，仅部署实际需要变更的服务。Controller 先停止补任务并等待当前循环退出，再启动唯一新实例；需要更换采集 Worker 时先停止新领取、等待在途任务完成。不要直接全栈 `compose up` 或在旧库上重放 bootstrap 快照。
