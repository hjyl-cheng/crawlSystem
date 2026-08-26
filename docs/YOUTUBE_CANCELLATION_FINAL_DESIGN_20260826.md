# YouTube 抓取取消链路最终设计

## 1. 文档状态

- 状态：**已实施并完成本地验证；初始实现已提交，审查修复随本文所在后续提交落地；未部署**
- 定稿日期：2026-08-26
- 实施日期：2026-08-26
- 起始代码基线：`aacd03870921e336c28d71f6dbb0faeb4edfccc4`
- 初始实现提交：`49a92ade9ab78cb27a35bb1bb7e7c12c2e02f34f`
- 审查修复：本文所在的独立后续提交，不 amend `49a92ad`
- 工作分支：`agent/incremental`
- 范围：`services/qybullmq` 的 Incremental、Content Enrich、Channel Crawl/Migration、YouTube.js、yt-dlp 与频道执行记账
- 验证摘要：15 个取消重点测试文件全部通过；依赖完整环境全量 1249 项中 1173 项通过、76 项跳过、0 项失败、0 项取消
- 验证环境：Node.js 26.7.0；Python 使用仓库 `.venv`，已包含 `aiohttp==3.14.3` 与 `yt-dlp==2026.07.04`

本文是本次取消问题的单一方案来源和实施记录。下文设计行号均以起始代码基线为准，实施后允许漂移；函数名、接口不变式和验收条件不得因行号漂移而省略。当前结论只覆盖本地提交与验证，不表示生产已经部署。

## 2. 目标

当 Rota/任务取消或 Content Enrich 丢失租约时，正在执行的 YouTube 抓取必须迅速停止，并满足以下结果：

1. 正在运行的 YouTube.js 请求、持久 yt-dlp 请求和一次性 Python 请求收到取消；
2. 持久 yt-dlp 子进程被终止后不会被 recovery、release、stop 竞态或 one-shot fallback 重新拉起；
3. 取消原因原样向上传播，不被包装成聚合错误、降级结果或普通失败；
4. 取消不新增代理故障、`upstream_transient` 或请求失败证据；取消前已经真实发生的失败证据保留；
5. 正常完成路径仍回收 `cookie_state`，持久会话仍跨频道复用；
6. 取消到任务退出的耗时明显低于现有 30/90/180 秒内部超时。

## 3. 已确认的根因

### 3.1 两个外部取消源彼此独立

| 信号 | 来源 | 可见范围 | 语义 |
|---|---|---|---|
| 显式信号 | `contentEnrichExecution.js` 的 heartbeat `AbortController` | 由函数参数向下传递 | Content Enrich 租约失效 |
| 环境信号 | `ChannelExecutionRuntime` 写入的 `abort_signal` | `channelExecutionContext` / proxy identity ALS | Rota、任务或频道执行取消 |
| 内部超时信号 | 各 transport 自己创建 | 单次请求内部 | 真实请求超时，不是外部取消 |

Content Enrich 的 heartbeat 信号和频道执行环境信号不是同一个对象。只读取环境信号会漏掉本次修复的主要目标，即 Enrich 丢失租约后立即停止详情抓取。

### 3.2 Incremental 两条路径的断点不同

- Uploads 已在 `incrementalVideo.js:2822-2825` 把 `signal` 传给 `fetchUploads()`，但 `youtube.js` 的 Uploads adapter 没有接住并继续传递。
- Detail 的 `captureDetails()` 当前调用 `fetchDetail(entry.id)`，没有传 `signal`。因此 Incremental 的详情抓取无法使用 `fetchIncrementalVideoDetail()` 内现有的取消检查。

### 3.3 yt-dlp recovery 会抵消取消

`ytdlpSession.js:321-340` 当前只直接放行 `ProxyIdentityChangedError` 和带 `.remote` 的错误。取消导致的本地错误会进入 `restartDaemon()` 并重发同一命令。

杀死 daemon 还会让同进程内其他 pending 请求收到 `#onExit()` 创建的普通本地错误。这些兄弟错误本身没有取消身份，因此 recovery 必须以传入 `signal.aborted` 为判据，而不是检查错误类型。

### 3.4 四条进程复活路径

1. `releasePersistentYtDlp()` 调用允许 `start()` 的 `request()`；
2. `requestWithRecovery()` 调用 `restartDaemon()`；
3. `stop()` 在旧 child 已 `killed`、`close` 事件尚未清理状态的窗口中调用 `request("shutdown")`，从而先启动新进程；
4. persistent adapter 返回 `null` 后，Uploads、Video Detail、Channel Metadata 三处 adapter 启动 `runPythonJson()` one-shot fallback。

仅把模块级 `daemon` 设为 `null` 无法证明 OS 进程已经退出。进程验收必须观察 spawn、PID/child 退出与后续 acquire 行为。

### 3.5 启动链路当前不可取消

`requestWithRecovery()` 的 `ensureDaemon()` 当前位于 `try` 外。`start()` 最长等待 15 秒，`configure()` 最长等待 30 秒；恢复 acquire 还可能再等待 15 秒。`acquirePersistentYtDlp()` 又会把所有错误降级成 `{ enabled:false, mode:"one_shot" }`，导致取消身份丢失。

### 3.6 YouTube.js 主路径没有收到外部信号

- `fetchYoutubeJsVideoDetail(videoId)` 当前丢弃第二个参数；
- `youtubei.js@17.2.0` 的 `getInfo()` 没有业务 `signal` 参数，必须通过 operation-level `AsyncLocalStorage` 把显式信号送到自定义 fetch；
- `createFetch()` 当前只合并 youtubei.js 传入信号与内部 timeout，没有逐请求读取环境信号；
- 评论、About、分页和 Migration 多处 catch 会把取消洗成错误字段或 fallback。

共享 YouTube.js runtime 是模块级单例，操作信号不得挂在 runtime 上，也不得在创建 fetch 闭包时缓存。`createFetch()` 返回的函数必须在每次请求执行时读取 operation ALS 与环境信号。

### 3.7 取消会污染失败记账

污染点彼此独立：

- `ytdlpSession.recordCommandFailure()`；
- `youtube.js:785-795` 的 `annotateYtDlpFailure()`；
- `youtube.js:1148-1163` 的 HTTP transport catch；
- `youtubeJs.js:883-901` 的 transport catch 和 `stats.failures`；
- `channelExecutionRuntime.js:270-274` 把取消错误加入 `failureDecisions()`，并把 attempt 写成 `failed`。

失败分类器中的 `abort` 正则不是取消判定机制。内部 timeout 也会产生 AbortError，不能按错误名称过滤。

## 4. 设计不变式

### 4.1 信号合并

所有合并先按引用去重，只有两个以上不同信号时才调用 `AbortSignal.any()`：

```js
const parts = [...new Set([explicitSignal, ambientSignal].filter(Boolean))];
const effectiveSignal = parts.length <= 1
  ? (parts[0] ?? null)
  : AbortSignal.any(parts);
```

这样保证：

- 只有显式信号时，`effectiveSignal === explicitSignal`；
- 显式信号与环境信号为同一对象时，不创建 composite；
- 两个不同信号时，composite 的 `reason` 保持触发方的原始引用；
- 不为每个详情请求无意义地向同一长生命周期信号追加 composite 监听。

### 4.2 三种信号职责

- **有效合并信号**决定当前操作是否停止；
- **原始外部信号**决定当前层是否属于主动取消、是否跳过失败记账；
- **内部 timeout 信号**仍按真实超时记账。

取消原因原样传播，不新增全局 `CancelledError`，不修改 `signal.reason`，不依赖失败分类器识别取消。只有将来出现真正丢失 `signal` 的异步 seam 时，才在该 seam 单独设计带 `cause` 的错误类型。

若外部取消与内部超时几乎同时发生，记账采用外部取消优先。这可能少记一个极少发生的真实超时点，属于接受的假阴性。

### 4.3 取消监听器生命周期

所有手工注册的 abort listener 必须：

1. 在入口先检查 `signal.aborted`；
2. 只注册一次；
3. 在 resolve、reject、timeout、child error、child close 和同步写入失败等所有 settle 路径清理；
4. composite signal 同样执行清理，不能因为对象短命而省略。

### 4.4 Ordered Prefetch 不变式

只要 `signal` 在 `processWithOrderedPrefetch()` 完成前已经 aborted，runner 就必须：

1. 排空当前窗口中所有已启动的 prefetch；
2. 以原始取消原因拒绝；
3. 不写入取消后得到的结果；
4. 不调用 `stopAfter()` 形成正常 stop result；
5. 不产生 `unhandledRejection`；
6. 任何路径都不得正常返回。

`detailConcurrency.js` 是通用模块，只接收可选显式 `signal`，内部不读取 ALS。

## 5. 最终实施方案

### A. 双信号合并与透传

#### A1. yt-dlp 链路

显式信号与环境信号合并后，贯穿以下接口：

1. `YtDlpDaemon.start()`；
2. `YtDlpDaemon.configure()`；
3. `YtDlpDaemon.request()`；
4. `ensureDaemon()`；
5. `restartDaemon()`；
6. `requestWithRecovery()`；
7. `acquirePersistentYtDlp()`；
8. `persistentChannelMetadata()`；
9. `persistentChannelUploads()`；
10. `persistentVideoDetail()`；
11. `youtube.js` 的 `runPythonJson()`；
12. Uploads、Video Detail、Channel Metadata adapters。

环境信号在 adapter/会话入口读取，显式参数优先参与合并。调用方没有显式信号时保持兼容。

#### A2. Incremental Detail

`fetchIncrementalVideoDetail()` 把显式信号与 `currentChannelExecutionAbortSignal()` 合并，函数内所有 `throwIfAborted()` 和下游调用统一使用有效信号。

#### A3. 引用身份

所有合并使用第 4.1 节的引用去重规则。禁止无条件 `AbortSignal.any()` 后通过放宽既有引用相等测试来掩盖行为变化。

完成条件：仅显式、显式与环境为同一对象、两个不同对象三种场景均通过身份测试。

### B. `captureDetails()` 停止吞取消

修改 `incrementalVideo.js:622-632`：

- 接收 `signal`；
- 每次调用改为 `fetchDetail(entry.id, { signal })`；
- 循环入口和 catch 内检查取消；
- 取消立即抛出，不写 `{ detail, error }` 后继续下一个视频；
- `incrementalVideo.js:2342` 和 `2896` 两个调用点都传入有效信号。

测试必须从 `executeIncrementalVideo()` 穿过真实 `captureDetails()`。仅直接测试注入的宽签名 fake 不能覆盖参数丢弃回归。

### C. yt-dlp 启动链路可取消

- `ensureDaemon()`、`start()`、`configure()`、acquire command 全部接收并检查信号；
- `requestWithRecovery()` 把 `ensureDaemon()` 纳入受取消保护的 `try` 范围；
- `acquirePersistentYtDlp()` 识别取消并原样抛出，不降级成 `{ enabled:false }`；
- acquire command 返回后、写 `activeLease` 前再次检查信号。

若最后一次检查命中，Python 侧已经建立租约而 Node 侧尚未记录。此时必须执行直接 `terminate()` 并清空两侧状态，不能只跳过 `activeLease = lease`。

### D. `request()` 取消当前 Python 请求

- 入口预检 `signal.aborted`；
- 请求期间监听 abort；
- abort 时拒绝当前请求并直接 `SIGKILL` 子进程，不发送 daemon 命令；
- 清理 pending timer 与 abort listener；
- child 退出导致的兄弟 pending 拒绝交给 E2 的环境信号判断处理。

相同规则用于 `runPythonJson()`：入口预检、abort listener、`SIGKILL`、所有 settle 路径清理。

### E. recovery 两处取消闸门

#### E1. 函数入口

`requestWithRecovery()` 的取消检查放在 `activeLease`、`enabledByEnvironment()` 和 strict 判断之前。否则 pre-aborted + persistent unavailable 会返回 `null` 并拉起 one-shot fallback。

#### E2. catch 最前面

catch 首行检查传入有效信号：

- 若 `signal.aborted`，直接执行取消清理并抛原始 reason；
- 不进入 `restartDaemon()`；
- 不 retry；
- 不调用 `recordCommandFailure()`；
- 该检查位于 `.remote` 和 `ProxyIdentityChangedError` 分支之前。

该判据同时覆盖 child 退出后兄弟 pending 收到的普通本地错误。

### F. yt-dlp adapter 取消闸门

三个 adapter 在入口、persistent 返回后、one-shot fallback 前、catch 以及 parsed-invalid 分支统一检查外部信号。

#### F1. 禁止错误记账

Uploads 和 Video Detail 的四个 `annotateYtDlpFailure()` 调用点在取消时全部绕开，禁止调用 `recordChannelExecutionFailure()`。

#### F2. 禁止 one-shot fallback

覆盖三个完整入口：

- Uploads：persistent `null` 后不得调用 `runPythonJson(YTDLP_UPLOADS_PY)`；
- Video Detail：persistent `null` 后不得调用 `runPythonJson(YTDLP_QUICK_DETAIL_PY)`；
- Channel Metadata：persistent `null` 后不得调用 `runPythonJson(YTDLP_CHANNEL_METADATA_PY)`。

Channel Metadata 不经过 `annotateYtDlpFailure()`，因此只属于 F2，不属于 F1。

### G. 固化 daemon 方法语义

把“是否允许启动子进程”固化到方法接口：

| 方法 | 语义 |
|---|---|
| `request()` | 业务请求，允许调用 `start()` |
| `requestExisting()` | 只操作现存健康进程，禁止调用 `start()` |
| `terminate()` | 直接终止，不经过 stdin，不启动进程，幂等；返回值只在真实 `exit`/`close` 后完成 |

原 `stop()` 不得再通过允许启动的 `request()` 发送 shutdown。

`terminate()` 发出 `SIGKILL` 后只立即拒绝 pending work，不得同步伪造退出并清空 child。被 terminate 的 daemon 实例永久不可再次 `start()`。模块级退出屏障覆盖 cancel、restart、profile change 与 close；任何 replacement spawn 必须等待旧 child 的真实 `exit`/`close`。

#### G1. 三态清理

| 状态 | 动作 |
|---|---|
| 取消或会话已失效 | 先清模块级 `daemon` 与 `activeLease`，再对捕获的旧实例调用并等待 `terminate()`；后续 release 自动短路 |
| 正常成功完成 | `requestExisting("release", ..., { signal })`，回收 `cookie_state`，保留 daemon 和 PID 供下个频道复用 |
| close | 不继承已经取消的业务 signal；健康进程使用不允许 start 的优雅 shutdown，死亡/终止中进程直接清理 |

正常 release 的返回值由 `channelExecutionRuntime.js` 提取 `cookie_state` 并写回 profile store。禁止把正常 release 改成每频道直接 kill。

正常 release 开始时必须收到频道执行的原始 `abortSignal`。release 期间取消时，`requestExisting()` 立即拒绝、终止 child、等待真实退出并原样抛出取消原因；未取消的正常完成仍回收 Cookie 并复用 PID。

#### G2. 命令超时证据

内部 yt-dlp command timeout 使用稳定错误码 `YTDLP_SESSION_COMMAND_TIMEOUT`。`requestWithRecovery()` 在取消检查之后、restart/retry 之前记录第一次真实 timeout；如果 recovery 成功，指标同时保留第一次失败证据和第二次成功结果。外部取消仍优先，不能因为 timeout 标记而新增取消失败证据。

### H. 评论首页回填停止吞取消

`commentFirstPageBackfill.js:243` 在 catch 中识别环境取消并原样抛出，不能返回 `comment_detail_error` 后继续批次。

真实调用方是 `services/qybullmq/scripts/backfillCommentFirstPages.mjs`。它是手动 `--apply` 脚本，不阻塞 Incremental 上线，但共享 adapter 开始响应环境信号时必须同时修复。

### I. 频道执行层记账

该项只处理 Rota/任务环境信号：

```text
attemptAborted = identityChanged || abortSignal?.aborted
```

- `finishAttempt.status` 写为 `aborted`；
- 取消错误不作为 `attemptError` push 进 `failureDecisions()`；
- `metrics.failure_evidence` 中取消前已经真实发生的网络失败继续保留；
- 不新增 `upstream_transient` 或代理处罚决策。

Enrich 租约失效在 `contentEnrichExecution` 内以 `heartbeat.lease_lost=true`、`skipped=claimed.length` 和 `error=null` 收敛，不会作为异常抛到本层。它的脏证据由 E/F/K 阻断。

### J. 取消判定约束

- 有效合并信号决定是否停止；
- 原始外部信号决定是否跳过本层失败记账；
- 内部 timeout 继续作为真实失败；
- 原始取消 reason 直接传播；
- 不按 `AbortError`、错误消息或 youtube failure policy 的正则判断主动取消。

### K. transport 记账

#### K1. `youtube.js` HTTP transport

`youtube.js:1148` 的 catch 分别观察 `init.signal`、`currentManagedAbortSignal()` 和内部 timeout controller：

- 外部信号 aborted：抛原始 reason，不调用失败记账；
- 仅内部 timeout aborted：保持现有 `upstream_transient` 记账。

K1 不依赖 L，可与 A 同时实施。

#### K2. YouTube.js transport

`youtubeJs.js:883` 的 catch 分别观察 operation signal、环境信号、youtubei caller signal 与内部 `timeoutSignal`：

- 外部取消不增加 `stats.failures`，不写 `recordChannelExecutionRequest(ok:false)`；
- 内部 timeout 仍增加失败计数并写真实失败证据。

`youtubeJsState().stats.requests` 在进入 fetch 时计数。被取消请求继续计入请求尝试次数是预期行为；频道执行指标不把它记成失败。这是两个不同口径。

### L. 打通 YouTube.js 取消

#### L1. 真实导出接口接收信号

`fetchYoutubeJsVideoDetail(videoId, { signal } = {})` 必须真正消费第二个参数。测试不得只验证一个签名更宽的注入 fake。

#### L2. operation-level ALS

- 新增模块内 operation signal storage，只保存单次 Detail 调用的显式信号；
- `getRuntime()` 在 operation ALS 作用域外执行；
- `current.client.getInfo()` 和同一详情操作的评论请求在 operation ALS 作用域内执行；
- 信号不挂在共享 runtime、client、stats 或 player surface Map 上。

#### L3. `createFetch()` 每次请求动态合并

在 `createFetch()` 返回的 async 函数体内逐请求读取：

1. youtubei.js caller signal；
2. operation ALS 中的显式信号；
3. `currentManagedAbortSignal()`；
4. 内部 timeout signal。

外部信号按引用去重；禁止在 `createFetch()` 创建闭包时读取并缓存 operation/环境信号。

#### L4. Discovery 与 About

Discovery 的 `scanUploads()` 和 About 的 `openYoutubeJsChannel()` 不需要额外 operation ALS。它们运行在频道执行上下文中，由 L3 每次请求读取环境信号。

`incrementalAbout.js` 不是网络入口；真正的 About 请求位于 `youtubeJs.js` 的 `openYoutubeJsChannel()`。

#### L5. 三处吞取消

以下 catch 在外部信号取消时原样抛出：

- 评论：`youtubeJs.js:1578`；
- About：`youtubeJs.js:1099`；
- Uploads 分页：`youtubeJs.js:449`。

#### L6. runtime bootstrap

`Innertube.create()` 使用以下四个 flag：

```text
retrieve_player: false
generate_session_locally: true
retrieve_innertube_config: false
enable_session_cache: false
```

该配置下 bootstrap 零网络，因此 operation ALS 不会用单个调用方的取消毒化共享 `runtimePromise`。仍保持 `getRuntime()` 在 operation ALS 外，固化这一不变式。

### M. Channel Crawl/Migration fallback 与并发详情

#### M1. 只在频道抓取流程读取环境信号

集中提供 `throwIfChannelExecutionAborted()`，在以下四个只属于 channel-crawl 的 catch 进入 fallback 前调用：

- `pipelineV2.js:1279`，YouTube.js Channel；
- `pipelineV2.js:1296`，Legacy Header；
- `pipelineV2.js:1360`，yt-dlp Metadata；
- `pipelineV2.js:1782`，YouTube.js Uploads。

#### M2. 详情流程使用显式 signal 链

共享详情流程可能由无 ALS 的源码形态到达，因此内部不读取环境 ALS。四个 `processContentDetailRun()` 调用点统一传：

```js
signal: currentChannelExecutionAbortSignal()
```

调用点及 execution mode：

| 基线行号 | execution mode | 当前运行形态 |
|---|---|---|
| 1190 | `channel_inline_resume` | channelCrawl，有 ALS |
| 1938 | `channel_inline` | channelCrawl，有 ALS |
| 2925 | `detail_queue` | channel-detail-repair 有 ALS；独立 contentDetail 入口休眠 |
| 2960 | `checkpoint_repair` | channelCrawl，有 ALS |

完整链路必须闭合：

```text
processContentDetailRun({ signal })
  -> processWithOrderedPrefetch({ signal })
  -> captureYoutubeJsDetail(videoId, { signal })
  -> fetchYoutubeJsVideoDetail(videoId, { signal })
  -> processOneCandidate(..., { signal })
  -> downstream yt-dlp / YouTube.js calls receive the same signal
  -> throwIfAborted(signal)
```

`captureYoutubeJsDetail()` 保持 `{ detail, error }` 返回契约，不能直接抛出并破坏 ordered prefetch 的排空语义；但它必须把信号传给真实 `fetchYoutubeJsVideoDetail()`。

`processOneCandidate()` 在取得 `youtubeJsResult` 后立即检查信号，在正常详情、yt-dlp fallback 和 YouTube.js comment fallback 中都传递同一信号。`pipelineV2.js:2281` 与 `2299` 的 catch 使用显式信号，不读取 ALS。

#### M3. Ordered Prefetch 七处检查

`processWithOrderedPrefetch({ signal })` 在七处检查：

1. runner 入口，`for` 循环之外；
2. 每个新窗口创建 prefetch Promise 之前；
3. 每次 `process()` 调用之前；
4. `process()` 返回后、写入 `processed/results` 之前；
5. stop 路径 `Promise.allSettled()` 之后、return 之前；
6. catch 路径 `Promise.allSettled()` 之后、throw 之前，外部取消优先于原错误；
7. 最终正常 return 之前。

第 1 和第 7 处覆盖 `items=[]` 及没有窗口可排空的路径。空集合的真实来源是上游查询没有待处理候选；`shouldPrefetch()` 只决定是否启动预取，不过滤 row。

#### M4. sibling rejection 保护

每个 prefetch Promise 创建后立即执行：

```js
void promise.catch(() => {});
```

这只把早到的 sibling rejection 标记为已处理；后续 `await promise` 仍按原语义拒绝。发生 stop 或 error 时，runner 继续通过 `Promise.allSettled(prefetched)` 排空整个当前窗口。

#### M5. 休眠 contentDetail 入口

`worker.js` 保留 `contentDetail` dispatch 分支，但 `managedWorkerExecution.js:66-68` 禁止独立消费，默认 inline 模式也暂停该队列。因此它是源码中的休眠入口，不是当前活跃生产路径。

可选 `signal` 只让依赖显式化，不会自动赋予无 signal 调用方取消能力。将来恢复独立 contentDetail Worker 时，必须先设计真实取消来源。

## 6. 实施顺序

主依赖顺序：

```text
A -> L -> M -> K2 -> B/C/D/E/F/G/I -> H
```

K1 只依赖现有 HTTP transport，可与 A 同期完成。

每一阶段完成条件：该阶段的真实导出接口已消费 signal，取消不再进入该阶段后续 fallback/记账，相关成对回归测试通过后才进入下一阶段。

## 7. 验收矩阵

### 7.1 信号合并与真实接口

1. 仅显式信号：有效信号与原对象引用相等；
2. 显式和环境是同一对象：去重后仍与原对象引用相等；
3. 两个不同信号：生成 composite，抛出的 reason 与触发方原对象引用相等；
4. 每个 signal seam 至少一条测试经过真实导出函数，不能只测试注入 fake。

### 7.2 Incremental/Enrich 链路

1. `executeIncrementalVideo()` 的详情抓取真实经过 `captureDetails()`，取消后不抓下一个视频；
2. Uploads 已有调用方信号继续到 adapter、persistent/one-shot 层；
3. Enrich heartbeat signal 中断正在进行的 YouTube.js Detail；
4. Enrich 丢租结果保持 `status=success`、`heartbeat.lease_lost=true`、`skipped=claimed.length`，且本次取消不产生 failure evidence。

### 7.3 YouTube.js 取消，共五类

1. Enrich 显式信号中断 Detail；
2. Rota 环境信号中断 Discovery；
3. 评论阶段取消不被 `commentsError` 吞掉；
4. About 取消不被 `aboutError` 吞掉；
5. 分页取消不被 `pagination_error` 吞掉。

### 7.4 外部取消与内部超时，成对测试

1. 外部取消：HTTP/YouTube.js transport 不记失败、不新增 evidence、不增加 `stats.failures`；
2. 内部 timeout：仍记录真实 `upstream_transient` 和失败指标；
3. 被取消请求仍计入 YouTube.js `stats.requests`，这是预期尝试次数口径。
4. yt-dlp 第一次 command timeout 后 recovery 成功：最终结果成功，同时保留一条带 `YTDLP_SESSION_COMMAND_TIMEOUT` 的失败证据。

### 7.5 yt-dlp 启动、请求与 acquire

1. pre-aborted 的 start/configure/request/acquire 不 spawn、不写 stdin；
2. start、configure、acquire 等待期间取消会终止原 child 并迅速拒绝；
3. acquire command 返回后、写 `activeLease` 前取消，会终止 Python 侧孤儿租约并清空 Node 状态；
4. abort listener 在成功、失败、超时和 child close 后全部清理；
5. acquire 不把取消降级为 one-shot unavailable。

### 7.6 四类复活入口

1. 取消后的 `releasePersistentYtDlp()` 不 spawn；
2. 取消后的 recovery 不进入 `restartDaemon()`；
3. killed child 到 `close` 事件之间调用清理，不通过旧 `stop()` 竞态 spawn；
4. persistent 返回 `null` 的三个 adapter 均不启动 `runPythonJson()`。

进程断言必须包括：原 PID/child 已触发真实 `exit`/`close`、abort 到 finally 结束之间没有第二次 spawn、下一次正常 acquire 也只能在旧 child 退出后出现新 PID。`child.killed` 只表示 kill signal 已发送，不是退出证据；禁止只用它或 `persistentYtDlpState().process_pid === null` 代替 OS/child 断言。

### 7.7 正常路径回归

1. 正常 release 仍返回并落盘 `cookie_state`；
2. 持久 daemon 跨频道复用，正常 release 前后 PID 不变；
3. close 对健康进程优雅关闭，对死亡/终止中进程不启动 replacement。
4. 正常 release 进行中收到取消时，在明显低于 15 秒 command timeout 的上界内终止，等待 child 退出并保留原始 reason。

### 7.8 频道执行记账

1. Rota 取消后 attempt `status=aborted`；
2. 取消错误不新增 `upstream_transient`、代理处罚或 attempt failure evidence；
3. 取消前已经记录的真实网络失败 evidence 保留；
4. Enrich 丢租按第 7.2 节的成功/跳过语义收敛。

### 7.9 Ordered Prefetch 取消，共七类

1. 窗口中途取消：无 `unhandledRejection`，当前窗口完整排空；
2. `process()` 执行期间取消：结果不写入 `results`，不触发 `stopAfter()` 正常返回；
3. stop 路径排空期间取消：以原始 reason 拒绝；
4. catch 路径排空期间取消：以原始 reason 拒绝，外部取消优先于原错误；
5. 靠后 sibling prefetch 在消费前 reject，靠前 process 跨过至少一个事件循环周期：无 `unhandledRejection`，窗口完整排空；
6. 已取消且 `items=[]`：以原始 reason 拒绝，不正常返回；
7. 最后一项的 `stopAfter()` 内同步 abort 并返回 `null`：最终 return 前仍以原始 reason 拒绝。

### 7.10 跨 L/M 挂钟验收，阻塞项

测试必须经过真实链路：

```text
processContentDetailRun
  -> captureYoutubeJsDetail
  -> fetchYoutubeJsVideoDetail
  -> controlled transport
```

只在最底层 transport 使用测试内部 seam。transport 除非收到 abort，否则永不 resolve。禁止使用会主动响应 signal 的 fake prefetch 替代真实 capture/export 链。

测试同时断言：

1. 底层 transport 实际收到 abort；
2. operation 拒绝值与原始 `signal.reason` 引用相等；
3. 取消到 runner 拒绝的耗时有明确上界，且明显低于 `YOUTUBEJS_TIMEOUT_MS` 默认 30000ms；
4. deadline 失败分支在 `finally` 中清理 transport、计时器与未决 Promise。

该测试用于阻止“检查点命中、错误类型正确，但仍在 drain 中空等 30 秒”的假性修复。

### 7.11 评论回填

真实经过 `backfillOneCommentFirstPage()` 和批次循环，环境取消后立即拒绝，不生成 `comment_detail_error` 后继续下一视频。

## 8. 已接受的边界与非目标

- 持久/one-shot Python 配置为 `skip_download=True` / `download=False`，不会启动 ffmpeg；本次不处理 SIGKILL 后的媒体子进程树问题。
- YouTube.js runtime bootstrap 已确认零网络，本次不新增 runtimePromise 取消共享策略。
- `youtubeJsState().stats.requests` 与频道执行失败指标保持不同统计口径。
- 本次不启用独立 contentDetail Worker，也不为休眠入口设计新的取消来源。
- 测试 transport 是内部 seam，不扩大生产接口。

## 9. 完成定义

方案只有在以下条件全部满足后才可标记为“已实现”：

1. A 至 M 的代码项全部落地，无被跳过的真实调用点；
2. 第 7 节所有验收通过，包括成对的取消/超时测试和跨 L/M 挂钟测试；
3. 原 Python PID 已退出且取消期间没有 replacement spawn；
4. 正常 cookie 回收与 PID 复用回归通过；
5. Rota 与 Enrich 两种取消源的状态和记账语义分别符合设计；
6. 全量 qybullmq 测试无新增失败；
7. 文档状态从“方案定稿，待实施”更新为实际实现提交与验证结果，不提前声称生产完成。

## 10. 实施与验证记录

### 10.1 已落地范围

- A 至 M 的信号合并、YouTube.js/yt-dlp 透传、fallback 闸门、daemon 三态清理、Ordered Prefetch 取消和频道执行记账均已落地；
- 正常 yt-dlp release 仍返回 `cookie_state`，持久进程跨频道复用；
- 取消路径覆盖 release、recovery、killed-child stop 竞态和三个 one-shot fallback；
- 实施后审查额外覆盖了 transport 与取消同时完成时的成功记账竞态，以及三个 one-shot adapter 在 child 快速 close 时的 reason 身份。
- 频道 cleanup 中发生的代理漂移或 Rota abort 会重新核对状态并终止已 release 的旧 yt-dlp daemon；字符串、空字符串和不可扩展 Error 形式的取消 reason 均保持原值。
- `49a92ad` 后续审查修复增加了真实 child 退出屏障、release 期间取消和首次 command timeout 证据；修复作为独立提交落地，没有 amend 初始实现。

### 10.2 验证结果

- 取消重点矩阵：15 个测试文件全部通过；
- 全量命令：`node --test test/*.test.js`，等价于 package 的 `npm test` 脚本；
- 全量统计：1249 tests，1173 passed，76 skipped，0 failed，0 cancelled；
- Python 集成测试使用仓库依赖完整 `.venv`，`fingerprintGateway.integration.test.js` 与 `ytdlpSession.integration.test.js` 均通过；
- `buildImages.test.js` 与 `businessPublicationIngress.test.js` 在允许临时 git 进程和 localhost 端口的完整环境中通过；
- 本次新增 4 条回归测试：真实退出屏障、release signal 透传、release 中途取消、timeout recovery 失败证据；
- `git diff --check` 与本次修改生产模块的 `node --check` 通过。

### 10.3 尚未声明完成的事项

- 未执行生产部署或线上验证。
