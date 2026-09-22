# 全量远程节点容量与暂停策略上线

验收时间：2026-09-22 02:25 UTC（北京时间 10:25）。

## 已生效

- 取消中心对远程全量 Worker 的固定槽位上限。每个节点的接单容量跟随已部署 Worker 数量，维护暂停的 Worker 不参与接单。
- 取消全量节点白名单，允许多个已登记、处于 active 状态的全量远程节点参与执行。节点认证和部署归属校验继续生效。
- 指定 `43.172.77.209` 的 `full-crawl-1` 维护暂停：容器 stopped/exited、自动重启关闭、Compose 设置 paused profile，中心状态显示 `paused=true`、`requested=false`、`readyForTasks=false`。节点扩容、重新开启接单及中心重启不会解除暂停。身份和暂存数据保留。
- 线上环境已移除 `REMOTE_NODE_FULL_CRAWL_TOTAL_SLOTS` 和 `REMOTE_NODE_FULL_CRAWL_NODE_IDS`。实际配置读取结果：`maxSlots=null`、`dashboardManaged=true`、`allowedNodeIds=[]`。
- 本地兼容执行器仍保留 1 个槽位；共享 Rota 网络容量按已登记节点申请，继续受其整体资源预算约束。

## 版本与上线结果

- 源码：`f3848c0d87fed0989275ab169319bff5e0b9847f`，发布分支 `codex/fullcrawl-open-20260922`；构建使用独立工作树，原工作区未提交改动保留。
- 中心镜像：`qy-allpachong/qybullmq:pachongsys-f3848c0-fullcrawl-open-20260922`。
- 面板镜像：`qy-allpachong/dashboard:pachongsys-f3848c0-fullcrawl-open-20260922`。
- 中心于 02:20:58 UTC 完成替换，旧中心平滑退出，退出码 0；没有强制杀死在途任务。
- 中心和面板均 healthy，重启次数均为 0。82 个增量 Worker 已全部恢复就绪（15 + 20 + 47）。
- 镜像中中心 472 个、面板 81 个源码文件与发布提交哈希完全一致。
- 中心配置已写回原持久化 Compose 文件。面板原部署无 Compose 标签，完整创建配置保存在此次发布的 private 目录，路径记录在容器 `qy.runtime.create_spec` 标签中。
- 旧中心与旧面板容器保留，名称后缀为 `-before-fullcrawl-open-20260922`，自动重启关闭，供回滚使用。回滚旧中心会恢复旧容量/白名单行为；不要将其当作维持新策略的常规重启方式。

## 验证

- 实际新中心镜像：9 项集成测试通过，无跳过；覆盖多节点合计 10 个就绪 Worker、扩容后 11 个、重启及旧接单请求不能解除指定暂停、未登记连接不被接纳、完整全量采集和增量监督器回归。
- 中心测试：1797 通过，276 项依赖外部夹具的用例跳过。直接 npm 运行缺失 Python 环境变量的两项失败，使用项目规定 Python 环境复核后均通过。
- 面板测试：99 通过，12 项依赖外部数据库的用例跳过；安装器暂停、部署和接单相关定向测试共 12 项通过；Python 安装器原有测试 7 项通过。
- 源码校验及 Compose 模板检查通过。
- 全仓库脚本在未改动的 auth 测试 `gatewayConfig.test.js:73` 处遇到既有 Nginx 配置断言失败；本次没有修改或发布 auth/Nginx。原工作区历史 runtime 脚本还会触发会话只读设置扫描，干净发布源码通过该扫描。

## 当前仍存在的运行问题

`full-crawl-3` 仍反复退出，日志错误为 `NODE_EXECUTOR_STOPPED`。全量节点原有部署失败记录仍为目标 10、已确认 3；本次没有重试节点部署或开启全量接单，因此该节点当前接单数量为 0。解除容量限制不等于已修复该 Worker 故障。

完整采样及校验结果见 [JSON 证据](fullcrawl-open-nodes-release-20260922.json)。
