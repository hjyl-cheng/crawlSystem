# 节点运行环境准备（2026-09-10）

本轮把节点环境准备做成 Dashboard 手动触发的固定脚本操作。代码尚未部署；没有连接两台真实执行节点、启动远程 Worker、修改生产数据库或派发正式任务。

## 页面流程与边界

1. 用户添加服务器，按已有流程完成 SSH、专用密钥与 Beszel 初始化。
2. 在已初始化的执行节点卡片点击“准备运行环境”。SSH 使用已配置密钥；sudo 需要密码时，在本次窗口输入，密码不写入节点登记或日志。
3. 页面显示连接、系统检查、Docker/Compose、运行目录、最终校验五个步骤。后台执行期间可以关闭窗口，重新打开或刷新后继续查看进度。全部完成会自动关闭；主动重新打开完成记录会保留窗口，方便核实版本。
4. 可保存 Worker 类型与数量计划。环境就绪不代表 Worker 已部署，实际镜像部署、节点启动入口和接收正式频道任务仍待接线。

当前按钮是独立手动操作，不随页面加载、保存节点或保存 Worker 数量自动执行。下一阶段接通部署时可以复用该准备入口，不需要让浏览器拼装任意命令。

采集、Clock、迁移派发、代理预算、休眠、API 兜底和业务落库流程均不在本次改动中。节点运行中的删除校验仍未接通，已初始化节点继续禁止直接删除登记。

## 固定脚本与执行方式

- 脚本源：`services/dashboard/src/nodeRuntime/bootstrap.sh`，随 Dashboard 的 `COPY src ./src` 进入镜像。
- 调用方式：`sh bootstrap.sh check|docker|layout|verify NODE_UUID`，必须以 root 执行。
- Dashboard 经 SFTP 上传脚本，安装至 root 所有的 `/opt/qy-node/runtime/bootstrap/<SHA256>.sh`，权限 700，并核实文件内容摘要。同一摘要的文件复用，不覆盖执行中的文件。
- 服务器使用 `/run/lock/qy-node-runtime.lock` 防止环境操作并行冲突；`/etc/qy-node/runtime/node-id` 防止同一主机被另一节点登记接管。
- 支持检查范围：使用 systemd 的 Ubuntu 22.04/24.04、Debian 12/13，x86_64/arm64。
- 已有 Docker 与 Compose 直接检查复用。首次安装从 Docker 官方 apt 源安装指定软件包，源密钥校验固定指纹；不会执行全系统升级或自动移除冲突运行时。
- Docker 不可用时尝试启动，设置开机启动；不会重启已经可用的 Docker。脚本不拉取镜像、不创建 Worker 容器、不修改 Docker daemon 配置或防火墙。
- 目录：`/opt/qy-node/runtime`、`/var/lib/qy-node/runtime/spool`、`/etc/qy-node/runtime/secrets`。暂存与凭据目录权限 700，重复准备保留已有内容。
- 各 SSH 操作有超时，Docker 步骤上限 900 秒，远程 timeout 带 TERM/KILL 收尾。错误按固定代码转为中文，不返回原始 apt 输出。

## 状态与重试

状态继续保存在既有 `crawler.settings` 的 `dashboard_server_nodes_v1` JSON 中，新增节点字段 `runtime`，无需新表。该字段由服务端管理，普通配置接口不接受注入运行状态。

`POST /api/server-nodes/:id/prepare-runtime` 仅接受 `version` 和本次 sudo `password`。先持久写入操作 ID、30 分钟期限及步骤状态，再连接节点。并发提交通过整个登记文档的 CAS 和操作 ID 拒绝重复启动，最多同时准备三台节点；进行中禁止编辑该节点。仅已完成 SSH 与监控初始化的执行节点可以使用，有部署记录时拒绝此入口。

每一步在远程动作前确认当前操作身份，完成后保存结果。全部五步完成才可记为 ready，并保存脚本摘要与检测到的 Docker/Compose 版本。失败可重试；Dashboard 重启造成的中断要等原操作期限结束后重试，已安装的环境和数据目录会复用。旧操作不能写入新操作的进度，准备脚本本身不依赖浏览器保持连接。

Controlled migration 的写入许可仅增加这个确切的 POST 路径；`/deploy` 和队列控制仍未开放。

## SSH 回归发现

现有 ssh2 1.17 的 Ed25519 生成器在转换 DER 公钥时移除所有前导零，偶尔把合法 32 字节公钥缩短，导致刚生成的私钥也无法解析。隔离 Node 22 中连续生成 100 对，观察到 1 对解析失败；故障发生在创建测试 SSH 服务器前，与网络或 Docker 无关。

新增 `generateNodeKey` 在保存新密钥前验证私钥、公钥可解析且相互匹配。遇到无效新密钥最多重新生成四次，失败明确报错；已有登记密钥不自动更换。测试 SSH 服务器使用相同生成检查，并验证无效/不匹配密钥被拒绝、失败次数有上限。另行生成 1,000 对验证输出为 0 对无效密钥。

## 验证方法

隔离 SSH 夹具位于 `services/dashboard/test-fixtures/node-runtime`，基于已有 onboarding 夹具。使用真实 OpenSSH、SFTP、密码 sudo、密钥登录及真实文件权限；apt、下载密钥和 Docker/systemctl 动作用 fixture 模拟。没有挂载宿主机 Docker socket，不代表已经验证真实 apt 安装或真实远程 Worker。

```bash
docker build -t qy-node-onboarding-sshd-test:latest services/dashboard/test-fixtures/node-onboarding
docker build -t qy-node-runtime-sshd-test:20260910 services/dashboard/test-fixtures/node-runtime
docker run -d --name qy-node-runtime-fixture --cpus 0.5 --memory 256m \
  -p 127.0.0.1:58023:22 -e 'TEST_SSH_PASSWORD=runtime-fixture-only!$' \
  qy-node-runtime-sshd-test:20260910
cd services/dashboard
SERVER_NODES_TEST_DATABASE_URL=postgres://postgres@127.0.0.1:55447/server_nodes_dashboard_test \
NODE_RUNTIME_TEST_SSH_PORT=58023 \
node --test --test-concurrency=1 \
  src/serverNodeRuntime.test.js src/serverNodeRuntimeRoutes.test.js \
  src/serverNodeRuntime.ssh.integration.test.js src/serverNodes.test.js \
  src/serverNodes.postgres.integration.test.js src/serverNodeOnboarding.test.js \
  src/serverNodeSsh.test.js src/controlledWritePolicy.test.js
docker rm -f qy-node-runtime-fixture
```

使用独立的 `server_nodes_dashboard_test` 数据库（事先创建），测试在修改数据前核实真实库名；会清理该测试库的 `crawler.settings`，切勿与其他用例并行共用。SSH 测试只接受 loopback 端口，并先验证 fixture 标识才进行写入。每次完整复跑要重新创建空白 SSH 夹具。未提供两个测试变量时相关集成测试会跳过，不能视为通过。

本机验证记录：

- Dashboard Node 22 镜像运行针对性测试：**25 项通过，0 失败，0 跳过**，日志 `/tmp/node-runtime-validation-final-20260910.log`。
- 实际脚本覆盖首次准备、重复执行保留暂存数据、错误节点、apt 失败重试、错误下载密钥、其他运行时冲突、不支持系统、目录权限不正确；核实没有启动容器、拉镜像或重启现有 Docker。
- 数据库测试覆盖并发启动、未初始化拒绝、编辑/删除保护、旧操作拒绝写入、超时后重试、不能提前报 ready、Worker 计划保留。
- 浏览器使用本机内存接口，覆盖手动启动、密码清空、刷新进度、失败重试、完成自动关闭、重开详情、手机布局和保留 Worker 计划；无浏览器脚本错误。该浏览器测试不连接 SSH，SSH 另由上述集成测试验证。
- 浏览器脚本与截图位于 `/tmp/qy-node-runtime-ui-test/`，其中 `browser.log` 为通过记录。

后续先完成节点 Worker 启动入口、运行镜像和中心接入配置，再从页面选择一台手工登记的节点做真实环境与采集验收。不能把目前的环境 ready 用作接收正式任务的依据。

后续进展：已完成独立节点接入镜像及真实 Docker/HTTPS 连接验收，并在 Worker 配置窗口增加只读部署方案。该镜像目前仅验证连接，完整采集与页面实际部署动作仍未开放，见 [节点接入镜像说明](../services/remote-node/README.md)。
