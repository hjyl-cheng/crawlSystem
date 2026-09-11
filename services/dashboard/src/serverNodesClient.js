const $ = id => document.getElementById(id);
const escapeHtml = value => String(value ?? "").replace(/[&<>"']/g, char => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[char]));
const roles = {
  fullcrawl: ["迁移 / Full Crawl", "频道首次采集与批量迁移"],
  incremental: ["增量采集", "跟进频道内容与视频数据更新"],
  discover: ["Query / 发现", "搜索与发现候选频道"],
  query_quality: ["Query 质量评估", "评估关键词及发现结果"],
};
const serverIcon = '<svg width="27" height="27" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.5" aria-hidden="true"><rect x="3" y="3" width="18" height="7" rx="2"/><rect x="3" y="14" width="18" height="7" rx="2"/><path d="M7 6.5h.01M7 17.5h.01M11 6.5h6M11 17.5h6" stroke-linecap="round"/></svg>';
let registry = null;
let editor = null;
let detailId = null;
let refreshing = false;
let deletion = null;
let onboardingAvailable = false;
let initializeId = null;
let closeInitializationOnSuccess = false;
let workerEditor = null;
let workerDeploymentAvailable = false;
let deploymentNodeId = null;
let closeDeploymentOnSuccess = false;
let runtimeAvailable = false;
let runtimeId = null;
let closeRuntimeOnSuccess = false;
const observations = new Map();
const isReady = node => node?.provisioning?.state === "ready";
const isRunning = node => node?.provisioning?.state === "running" && Date.parse(node.provisioning.deadline) > Date.now();
const runtimeRunning = node => node?.runtime?.state === "running" && Date.parse(node.runtime.deadline) > Date.now();
const deploymentRunning = node => node?.deployment?.state === 'running' && Date.parse(node.deployment.deadline) > Date.now();
const deploymentState = node => deploymentRunning(node) ? '正在部署' : node?.deployment?.state === 'connected' ? '部署和连接检查通过' : node?.deployment ? '部署未完成，可查看或重试' : '尚未部署';
const runtimeState = node => node?.runtime?.state === "ready" ? "运行环境就绪" : runtimeRunning(node) ? "运行环境准备中" : node?.runtime?.state === "failed" ? "环境准备失败，可重试" : node?.runtime?.state === "running" ? "环境准备中断，可重试" : "运行环境尚未准备";
const nodeState = node => isReady(node) ? "已初始化" : isRunning(node) ? "初始化中" : node.provisioning?.state === "failed" ? "初始化失败" : node.provisioning?.state === "running" ? "初始化中断，可重试" : "待初始化";

function announce(message, error = false) {
  const element = $("nodes-message");
  element.textContent = message;
  element.dataset.error = String(error);
  element.hidden = !message;
}

async function request(path, options = {}, expectRegistry = true) {
  const response = await fetch(path, { ...options, headers: { "Accept": "application/json", ...options.headers }, signal: AbortSignal.timeout(15000) });
  if (response.redirected || !response.headers.get("content-type")?.includes("application/json")) throw new Error("读取失败或登录已过期，请刷新页面后重试");
  const result = await response.json();
  if (!response.ok) throw new Error(result.error || "保存失败，请稍后重试");
  if (expectRegistry && (!Number.isInteger(result.version) || !Array.isArray(result.nodes))) throw new Error("服务器列表格式异常，请稍后刷新");
  return result;
}

function metrics(node) {
  const observation = observations.get(node?.id);
  const data = observation?.metrics;
  const pct = value => `${Number(value).toFixed(1)}%`;
  const capacity = (used, total) => `${Number(used).toFixed(1)} / ${Number(total).toFixed(1)} GiB`;
  const rate = value => `${(Number(value) / 1024).toFixed(1)} KiB/s`;
  const values = data ? [["CPU", pct(data.cpuPercent), observation.online ? "在线" : "历史数据"],
    ["内存", pct(data.memoryPercent), capacity(data.memoryUsedGiB, data.memoryTotalGiB)],
    ["磁盘", pct(data.diskPercent), capacity(data.diskUsedGiB, data.diskTotalGiB)],
    ["网络", `↓ ${rate(data.downloadBytesPerSecond)}`, `↑ ${rate(data.uploadBytesPerSecond)}`]]
    : ["CPU", "内存", "磁盘", "网络"].map(label => [label, "—", node?.provisioning?.systemId ? "等待数据" : "未接入"]);
  return `<div class="nodes-metrics">${values.map(([label, value, help]) => `<div class="nodes-metric"><span>${label}</span><strong>${escapeHtml(value)}</strong><small>${escapeHtml(help)}</small></div>`).join("")}</div>`;
}

function card(node) {
  const total = node.workers.reduce((sum, worker) => sum + worker.count, 0);
  const environmentAction = isReady(node) && node.kind === "execution";
  const runtimeButton = `<button type="button" class="nodes-button primary" data-runtime="${escapeHtml(node.id)}">${runtimeRunning(node) ? "环境准备详情" : node.runtime?.state === "ready" ? "检查运行环境" : "准备运行环境"}</button>`;
  return `<article class="nodes-card">
    <div class="nodes-card-main"><div class="nodes-card-top"><div class="nodes-card-icon">${serverIcon}</div><div class="nodes-card-title"><h3>${escapeHtml(node.name)}</h3><div class="nodes-address">${escapeHtml(node.host)} · ${node.port}</div></div><details class="nodes-card-menu"><summary aria-label="${escapeHtml(node.name)}的更多操作">⋯</summary><div><button type="button" data-edit="${escapeHtml(node.id)}">编辑服务器</button><button type="button" class="danger" data-delete="${escapeHtml(node.id)}">删除服务器</button></div></details></div>
      <div class="nodes-card-tags"><span class="nodes-badge ${node.kind === "center" ? "center" : ""}">${node.kind === "center" ? "中心节点" : "执行节点"}</span><span class="nodes-badge ${isReady(node) ? "center" : "pending"}">${nodeState(node)}</span>${observations.has(node.id) ? `<span class="nodes-badge">${observations.get(node.id).online ? "监控在线" : "监控暂无新数据"}</span>` : ""}</div>
      ${metrics(node)}<div class="nodes-card-workers"><span>Worker 计划</span><strong>${total ? total + " 个" : "未配置"}</strong>${total ? `<span class="nodes-badge pending">${deploymentState(node)}</span>` : ""}</div><p class="nodes-worker-gate">${environmentAction ? `${runtimeState(node)}。可保存 Worker 计划。` : node.provisioning?.error ? escapeHtml(node.provisioning.error) : "完成 SSH 与监控接入后，可配置 Worker。"}</p>
    </div><div class="nodes-card-footer"><button type="button" class="nodes-text-button" data-detail="${escapeHtml(node.id)}">查看详情</button><div><button type="button" class="nodes-button" data-workers="${escapeHtml(node.id)}" ${isReady(node) && !runtimeRunning(node) ? "" : "disabled"}>配置 Worker</button>${environmentAction && node.runtime?.state === 'ready' && total ? `<button type="button" class="nodes-button primary" data-deploy="${escapeHtml(node.id)}">${node.deployment ? 'Worker 部署详情' : '部署 Worker'}</button>` : ''}${environmentAction ? runtimeButton : `<button type="button" class="nodes-button primary" data-initialize="${escapeHtml(node.id)}">${isReady(node) || isRunning(node) ? "初始化详情" : node.provisioning?.state === "failed" ? "重试初始化" : "初始化"}</button>`}</div></div>
  </article>`;
}

function render() {
  if (!registry) return;
  $("nodes-total").textContent = registry.nodes.length;
  $("nodes-online").textContent = [...observations.values()].filter(value => value.online).length;
  $("nodes-planned").textContent = registry.nodes.reduce((sum, node) => sum + node.workers.reduce((n, worker) => n + worker.count, 0), 0);
  $("nodes-add").disabled = false;
  const search = $("nodes-search").value.trim().toLowerCase();
  const kind = $("nodes-kind").value;
  const nodes = registry.nodes.filter(node => (kind === "all" || node.kind === kind) && `${node.name} ${node.host}`.toLowerCase().includes(search));
  $("nodes-count").textContent = registry.nodes.length ? `${nodes.length} / ${registry.nodes.length}` : "";
  if (!registry.nodes.length) {
    $("nodes-list").innerHTML = `<div class="nodes-empty"><div class="nodes-empty-icon">${serverIcon}</div><h3>添加你的第一台服务器</h3><p>填写服务器地址与密码，自动验证 SSH、配置密钥并接入监控，节点就绪后再添加 Worker。</p><button type="button" class="nodes-button primary" data-add>＋ 添加服务器</button><div class="nodes-steps"><b>01 添加服务器</b><span>02 初始化与监控</span><span>03 配置 Worker</span></div></div>`;
  } else if (!nodes.length) {
    $("nodes-list").innerHTML = '<div class="nodes-empty"><h3>没有匹配的服务器</h3><p>换一个名称、IP 地址或节点类型试试。</p><button type="button" class="nodes-button" data-clear>清除筛选</button></div>';
  } else {
    $("nodes-list").innerHTML = nodes.map(card).join("");
  }
}

async function refresh(quiet = false) {
  if (refreshing || (quiet && (document.querySelector('.nodes-card-menu[open], .nodes-dialog[data-saving="true"]') || [...document.querySelectorAll('.nodes-dialog[open]')].some(dialog => !["node-initialize", "node-runtime", "node-deploy"].includes(dialog.id))))) return;
  refreshing = true;
  $("nodes-refresh").disabled = true;
  $("nodes-refresh").textContent = "刷新中…";
  try {
    registry = await request("/api/server-nodes");
    onboardingAvailable = registry.capabilities?.onboarding === true;
    runtimeAvailable = registry.capabilities?.runtime === true;
    workerDeploymentAvailable = registry.capabilities?.workerDeployment === true;
    if (!quiet) announce("");
    render();
    if ($("node-initialize").open) renderInitialization();
    if ($("node-runtime").open) renderRuntime();
    if ($("node-deploy").open) renderDeployment();
    for (const id of observations.keys()) if (!registry.nodes.some(node => node.id === id)) observations.delete(id);
    await Promise.allSettled(registry.nodes.filter(node => node.provisioning?.systemId).map(async node => {
      try { observations.set(node.id, await request(`/api/server-nodes/${encodeURIComponent(node.id)}/monitoring`, {}, false)); }
      catch { const last = observations.get(node.id); observations.set(node.id, { ...last, online: false }); }
    }));
    // Avoid replacing an open card menu during a background refresh.
    if (!document.querySelector(".nodes-card-menu[open]")) render();
  }
  catch (error) {
    if (!quiet) announce(error.message, true);
    if (!registry) $("nodes-list").innerHTML = '<div class="nodes-empty"><h3>暂时无法读取服务器</h3><p>已有配置不会丢失，请稍后重试。</p><button type="button" class="nodes-button" data-retry>重新加载</button></div>';
  } finally { refreshing = false; $("nodes-refresh").disabled = false; $("nodes-refresh").textContent = "刷新"; }
}

function openEditor(node = null) {
  if (!registry) return;
  editor = { node, version: registry.version };
  const form = $("node-form");
  form.reset();
  for (const field of ["name", "host", "port", "username", "kind", "notes"]) {
    form.elements[field].value = node?.[field] ?? ({ port: 22, kind: "execution" }[field] ?? "");
  }
  $("node-editor-title").textContent = node ? "编辑服务器" : "添加服务器";
  $("node-editor-intro").textContent = node ? "更新服务器的登记信息。已初始化节点的连接信息固定，备注和名称可以修改。" : "填写登录信息，点击添加并初始化，自动完成 SSH 与 Beszel 接入。";
  for (const key of ["host", "port", "username", "kind"]) form.elements[key].disabled = !!node && (node.kind === "center" || ![undefined, "not_started"].includes(node.provisioning?.state));
  $("node-password").disabled = !onboardingAvailable;
  $("node-add-initialize").disabled = !onboardingAvailable;
  $("node-add-initialize").title = onboardingAvailable ? "验证连接并接入监控" : "中心初始化服务尚未配置";
  $("node-password-section").hidden = !!node;
  $("node-register-help").hidden = !!node;
  $("node-add-initialize").hidden = !!node;
  $("node-save").textContent = node ? "保存修改" : "仅保存信息";
  $("node-form-error").hidden = true;
  $("node-editor").showModal();
  form.elements.name.focus();
}

function openDetail(id) {
  const node = registry.nodes.find(item => item.id === id);
  if (!node) return;
  detailId = id;
  $("node-detail-title").textContent = node.name;
  const fields = [["节点类型", node.kind === "center" ? "中心节点" : "执行节点"], ["服务器地址", node.host], ["SSH 用户名", node.username], ["SSH 端口", node.port], ["SSH 配置引用", node.sshAlias || "初始化时自动配置"], ["配置更新时间", new Date(node.updatedAt).toLocaleString("zh-CN")]];
  $("node-detail-content").innerHTML = `<div class="nodes-detail-section"><dl class="nodes-detail-meta">${fields.map(([key, value]) => `<div><dt>${key}</dt><dd>${escapeHtml(value)}</dd></div>`).join("")}</dl>${node.notes ? `<p class="nodes-detail-notes">${escapeHtml(node.notes)}</p>` : ""}</div>
    <section class="nodes-detail-section"><h3>资源监控 <span class="nodes-badge">${observations.get(node.id)?.online ? "在线" : "暂无新数据"}</span></h3>${metrics(node)}<p class="nodes-footnote">${observations.get(node.id)?.sampleAt ? `最近数据：${escapeHtml(new Date(observations.get(node.id).sampleAt).toLocaleString("zh-CN"))}` : "等待接入 Beszel 监控"}</p></section>
    <section class="nodes-detail-section"><h3>Worker 配置</h3>${node.workers.length ? node.workers.map(worker => `<div class="nodes-worker-summary"><span>${escapeHtml(roles[worker.role]?.[0] ?? worker.role)}</span><span><strong>计划 ${worker.count} 个</strong><span class="nodes-badge pending">尚未部署</span></span></div>`).join("") : '<p class="nodes-footnote">尚未配置 Worker。</p>'}<p class="nodes-worker-gate">请先完成初始化并接入监控，再配置 Worker。已有计划保留，当前不可调整。</p><button type="button" class="nodes-button" disabled>配置 Worker · 节点尚未就绪</button></section>
    <p class="nodes-footnote">初始化状态：${nodeState(node)}。实际 Worker 部署和运行记录尚未接入。</p>`;
  if (isReady(node)) {
    const section = $("node-detail-content").querySelectorAll(".nodes-detail-section")[2];
    section.querySelector(".nodes-worker-gate").textContent = "初始化已完成，可以保存 Worker 计划。";
    const button = section.querySelector("button"); button.disabled = false; button.textContent = "配置 Worker"; button.dataset.workers = node.id;
  }
  $("node-detail").showModal();
}

function openInitialize(id) {
  const node = registry.nodes.find(item => item.id === id);
  if (!node) return;
  initializeId = id;
  // Keep completed details available when explicitly reopened for inspection.
  closeInitializationOnSuccess = !isReady(node);
  $("node-initialize-name").textContent = node.name;
  $("node-initialize-auth").textContent = node.sshAlias ? `已登记 SSH 引用 ${node.sshAlias}，初始化时优先验证其是否可用。` : "首次接入时使用登录密码配置专用密钥；若已有可用密钥，将优先复用。";
  $("node-initialize-password").value = "";
  renderInitialization();
  $("node-initialize").showModal();
}

function renderInitialization() {
  const node = registry.nodes.find(item => item.id === initializeId);
  if (!node) return;
  const running = isRunning(node);
  const ready = isReady(node);
  const labels = { pending: "待执行", running: "执行中…", completed: "已完成", failed: "失败" };
  [...$("node-initialize-steps").children].forEach((element, index) => {
    const step = ["ssh", "key", "monitoring", "metrics"][index];
    element.lastElementChild.textContent = labels[node.provisioning?.steps?.[step]] ?? "待执行";
  });
  $("node-initialize-credentials").hidden = running || ready;
  $("node-initialize-password").disabled = !onboardingAvailable || running || ready;
  $("node-initialize-start").disabled = !onboardingAvailable || running || ready || node.kind === "center" || !!$("node-initialize").dataset.saving;
  $("node-initialize-start").textContent = ready ? "初始化完成" : running ? "初始化中…" : "开始 / 重试初始化";
  $("node-initialize-status").textContent = node.provisioning?.error || (ready ? "SSH 密钥与监控均已验证，可以配置 Worker。" : running ? "初始化在后台执行，可以关闭窗口；刷新页面仍可查看进度。" : node.provisioning?.state === "running" ? "上次初始化未在时限内完成，可重新输入密码重试。已完成的密钥和监控配置会复用。" : !onboardingAvailable ? "中心初始化服务尚未配置" : "点击开始后自动执行以上步骤，密码只用于本次初始化。");
  if (ready && closeInitializationOnSuccess && $("node-initialize").open) {
    closeInitializationOnSuccess = false;
    $("node-initialize").close();
    announce(`${node.name} 初始化完成，可以配置 Worker。`);
  }
}

async function startInitialization(initialPassword = null) {
  const dialog = $("node-initialize");
  if (dialog.dataset.saving) return;
  const node = registry.nodes.find(item => item.id === initializeId);
  if (!node) return;
  dialog.dataset.saving = "true";
  $("node-initialize-start").disabled = true;
  let password = initialPassword ?? $("node-initialize-password").value;
  $("node-initialize-password").value = "";
  try {
    registry = await request(`/api/server-nodes/${encodeURIComponent(node.id)}/initialize`, { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify({ version: registry.version, password }) });
    announce("初始化已启动，可以在节点卡片查看进度。");
  } catch (error) { announce(error.message, true); }
  finally { password = ""; delete dialog.dataset.saving; render(); renderInitialization(); }
}

function openRuntime(id) {
  const node = registry.nodes.find(item => item.id === id);
  if (!isReady(node) || node.kind !== "execution") return;
  runtimeId = id;
  closeRuntimeOnSuccess = runtimeRunning(node);
  $("node-runtime-name").textContent = node.name;
  $("node-runtime-password").value = "";
  renderRuntime();
  $("node-runtime").showModal();
}

function renderRuntime() {
  const node = registry.nodes.find(item => item.id === runtimeId);
  if (!node) return;
  const running = runtimeRunning(node);
  const ready = node.runtime?.state === "ready";
  const labels = { pending: "待执行", running: "执行中…", completed: "已完成", failed: "失败" };
  [...$("node-runtime-steps").children].forEach((element, index) => {
    const step = ["ssh", "check", "docker", "layout", "verify"][index];
    const state = node.runtime?.steps?.[step];
    element.lastElementChild.textContent = state === "running" && !running ? "已中断" : labels[state] ?? "待执行";
  });
  $("node-runtime-password-field").hidden = running;
  $("node-runtime-password").disabled = !runtimeAvailable || running;
  $("node-runtime-start").disabled = !runtimeAvailable || running || !!node.deployment || !!$("node-runtime").dataset.saving;
  $("node-runtime-start").textContent = running ? "正在准备…" : ready ? "重新检查环境" : "开始 / 重试准备";
  const details = node.runtime?.details;
  $("node-runtime-status").textContent = !runtimeAvailable ? "中心运行环境准备服务尚未配置" : node.deployment ? "已有部署记录，请先核实运行状态。" : node.runtime?.error || (ready ? `环境已就绪：Docker ${details?.dockerVersion ?? "—"} · Compose ${details?.composeVersion ?? "—"}。Worker 尚未部署。` : running ? "正在后台准备环境，可以关闭窗口；刷新页面仍可查看进度。" : node.runtime?.state === "running" ? "上次操作未在时限内完成，可重试。已安装的环境会复用。" : "使用已配置的 SSH 密钥连接，点击开始后准备运行环境。");
  if (ready && closeRuntimeOnSuccess && $("node-runtime").open) {
    closeRuntimeOnSuccess = false;
    $("node-runtime").close();
    announce(`${node.name} 运行环境已就绪，Worker 尚未部署。`);
  }
}

async function startRuntime() {
  const dialog = $("node-runtime");
  if (dialog.dataset.saving) return;
  const node = registry.nodes.find(item => item.id === runtimeId);
  if (!node) return;
  dialog.dataset.saving = "true";
  $("node-runtime-start").disabled = true;
  let password = $("node-runtime-password").value;
  $("node-runtime-password").value = "";
  try {
    registry = await request(`/api/server-nodes/${encodeURIComponent(node.id)}/prepare-runtime`, { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify({ version: registry.version, password }) });
    closeRuntimeOnSuccess = true;
    announce("环境准备已启动，可以在节点卡片查看进度。");
  } catch (error) { closeRuntimeOnSuccess = false; announce(error.message, true); }
  finally { password = ""; delete dialog.dataset.saving; render(); renderRuntime(); }
}

function openWorkers(id) {
  const node = registry.nodes.find(item => item.id === id);
  if (!isReady(node)) return;
  workerEditor = { node, version: registry.version };
  $("node-detail").close();
  $("node-workers-runtime-state").textContent = runtimeState(node);
  $("node-workers-deployment-status").hidden = true;
  $("node-workers-fields").innerHTML = Object.entries(roles).map(([role, [label]]) => `<label class="nodes-field">${label}<input type="number" name="${role}" min="0" max="100" required value="${node.workers.find(worker => worker.role === role)?.count ?? 0}"></label>`).join("");
  $("node-workers-error").hidden = true;
  $("node-workers").showModal();
}

async function openDelete(id) {
  const node = registry.nodes.find(item => item.id === id);
  if (!node) return;
  const current = { id };
  deletion = current;
  $("node-delete-name").textContent = node.name;
  $("node-delete-reason").textContent = "正在检查登记与初始化状态…";
  $("node-delete-checks").hidden = true;
  $("node-delete-confirm").disabled = true;
  $("node-delete-confirm").textContent = "正在检查…";
  $("node-delete").showModal();
  try {
    const check = await request(`/api/server-nodes/${encodeURIComponent(id)}/deletion-check`, {}, false);
    if (deletion !== current || !$("node-delete").open) return;
    if (check.id !== id || !Number.isInteger(check.version) || typeof check.allowed !== "boolean") throw new Error("删除条件返回异常，请重新打开窗口检查");
    Object.assign(current, check);
    $("node-delete-reason").textContent = check.reason;
    $("node-delete-checks").hidden = check.allowed || node.kind === "center";
    $("node-delete-confirm").disabled = !check.allowed;
    $("node-delete-confirm").textContent = check.allowed ? "确认删除登记" : "暂时不能删除";
  } catch (error) {
    if (deletion !== current || !$("node-delete").open) return;
    $("node-delete-reason").textContent = error.message;
    $("node-delete-confirm").textContent = "检查失败，请重新打开";
  }
}

async function confirmDelete() {
  const dialog = $("node-delete");
  if (!deletion?.allowed || dialog.dataset.saving) return;
  const current = deletion;
  const controls = [...dialog.querySelectorAll("button")];
  controls.forEach(control => { control.disabled = true; });
  dialog.dataset.saving = "true";
  $("node-delete-confirm").textContent = "正在删除…";
  try {
    registry = await request(`/api/server-nodes/${encodeURIComponent(current.id)}`, {
      method: "DELETE", headers: { "Content-Type": "application/json" }, body: JSON.stringify({ version: current.version }),
    });
    render();
    dialog.close();
    announce("服务器登记已删除。");
  } catch (error) {
    current.allowed = false;
    $("node-delete-reason").textContent = `${error.message}。请关闭窗口、刷新列表后重新确认。`;
    $("node-delete-confirm").textContent = "请重新检查";
  } finally {
    controls.forEach(control => { control.disabled = control.id === "node-delete-confirm"; });
    delete dialog.dataset.saving;
  }
}

async function saveForm({ form, dialog, errorId, edit, node, message }) {
  const controls = [...form.querySelectorAll("button,input,textarea,select")].filter(control => !control.disabled);
  controls.forEach(control => { control.disabled = true; });
  dialog.dataset.saving = "true";
  $(errorId).hidden = true;
  try {
    registry = await request(edit.node ? `/api/server-nodes/${encodeURIComponent(edit.node.id)}` : "/api/server-nodes", {
      method: edit.node ? "PUT" : "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ version: edit.version, node }),
    });
    render();
    dialog.close();
    announce(message);
    return registry;
  } catch (error) {
    $(errorId).textContent = error.message;
    $(errorId).hidden = false;
  } finally {
    controls.forEach(control => { control.disabled = false; });
    delete dialog.dataset.saving;
  }
}

$("node-form").addEventListener("submit", async event => {
  event.preventDefault();
  if (!editor || $("node-editor").dataset.saving) return;
  // Only metadata is submitted. A password must never enter the registry payload.
  const values = Object.fromEntries(["name", "host", "port", "username", "kind", "notes"].map(key => [key, event.currentTarget.elements[key].value]));
  const initialize = event.submitter?.id === "node-add-initialize";
  let password = initialize ? $("node-password").value : "";
  if (initialize && (!onboardingAvailable || values.kind !== "execution")) { announce("自动初始化只适用于执行节点", true); return; }
  const saved = await saveForm({ form: event.currentTarget, dialog: $("node-editor"), errorId: "node-form-error", edit: editor,
    node: { ...values, port: Number(values.port), sshAlias: editor.node?.sshAlias ?? "", workers: editor.node?.workers ?? [] },
    message: "服务器配置已保存。" });
  $("node-password").value = "";
  if (saved && initialize) {
    const node = saved.nodes.find(node => node.host === values.host.trim().toLowerCase() && node.port === Number(values.port));
    openInitialize(node.id);
    await startInitialization(password);
  }
  password = "";
});
$("node-workers-form").addEventListener("submit", event => {
  event.preventDefault();
  if (!workerEditor || $("node-workers").dataset.saving) return;
  const node = Object.fromEntries(["name", "host", "port", "username", "kind", "notes", "sshAlias"].map(key => [key, workerEditor.node[key]]));
  node.workers = Object.keys(roles).map(role => ({ role, count: Number(event.currentTarget.elements[role].value) })).filter(worker => worker.count > 0);
  void saveForm({ form: event.currentTarget, dialog: $("node-workers"), errorId: "node-workers-error", edit: workerEditor, node, message: "Worker 计划已保存，尚未部署。" });
});
document.addEventListener("click", event => {
  const target = event.target.closest("button");
  if (!target || target.disabled) return;
  if (target.hasAttribute("data-close") && !$(target.dataset.close).dataset.saving) $(target.dataset.close).close();
  if (target.hasAttribute("data-add")) openEditor();
  if (target.hasAttribute("data-detail")) openDetail(target.dataset.detail);
  if (target.hasAttribute("data-initialize")) openInitialize(target.dataset.initialize);
  if (target.hasAttribute("data-deploy")) openDeployment(target.dataset.deploy);
  if (target.hasAttribute("data-runtime")) openRuntime(target.dataset.runtime);
  if (target.hasAttribute("data-workers")) openWorkers(target.dataset.workers);
  if (target.hasAttribute("data-edit")) openEditor(registry.nodes.find(node => node.id === target.dataset.edit));
  if (target.hasAttribute("data-delete")) openDelete(target.dataset.delete);
  target.closest(".nodes-card-menu")?.removeAttribute("open");
  if (target.hasAttribute("data-retry")) void refresh();
  if (target.hasAttribute("data-clear")) { $("nodes-search").value = ""; $("nodes-kind").value = "all"; render(); }
});
for (const dialog of document.querySelectorAll(".nodes-dialog")) {
  dialog.addEventListener("close", () => { for (const input of dialog.querySelectorAll('input[type="password"]')) input.value = ""; });
  dialog.addEventListener("cancel", event => { if (dialog.dataset.saving) event.preventDefault(); });
  dialog.addEventListener("click", event => { if (dialog.dataset.saving && event.target === dialog) event.stopPropagation(); });
}
$("nodes-add").addEventListener("click", () => openEditor());
$("nodes-refresh").addEventListener("click", () => void refresh());
$("nodes-search").addEventListener("input", render);
$("nodes-kind").addEventListener("change", render);
$("node-detail-edit").addEventListener("click", () => { $("node-detail").close(); openEditor(registry.nodes.find(node => node.id === detailId)); });
$("node-detail-initialize").addEventListener("click", () => { $("node-detail").close(); openInitialize(detailId); });
$("node-detail-delete").addEventListener("click", () => { $("node-detail").close(); openDelete(detailId); });
$("node-delete").addEventListener("close", () => { deletion = null; });
$("node-delete-confirm").addEventListener("click", () => void confirmDelete());
$("node-initialize-start").addEventListener("click", () => void startInitialization());
$("node-runtime-start").addEventListener("click", () => void startRuntime());
$("node-workers-deployment-preview").addEventListener("click", async () => {
  if (!workerEditor) return;
  const current = workerEditor;
  const button = $("node-workers-deployment-preview");
  const status = $("node-workers-deployment-status");
  button.disabled = true; status.hidden = false; status.textContent = "正在检查已保存的部署计划…";
  try {
    const preview = await request(`/api/server-nodes/${encodeURIComponent(current.node.id)}/worker-deployment`, {}, false);
    if (workerEditor !== current || !$("node-workers").open) return;
    status.textContent = preview.available
      ? `已保存 ${preview.count} 个增量 Worker 计划，${preview.mode === 'incremental_collect' ? '完整采集' : '接入验证'}容器内存上限合计 ${preview.memoryLimitMiB} MiB。部署后先核实中心连接，中心启用后才接收任务。当前仅查看方案，尚未执行部署。`
      : preview.reason;
  } catch (error) { if (workerEditor === current) status.textContent = error.message; }
  finally { button.disabled = false; }
});
setInterval(() => { if (!document.hidden) void refresh(true); }, 5000);
void refresh();

function openDeployment(id) {
  const node = registry.nodes.find(row => row.id === id); if (!node) return;
  deploymentNodeId = id; closeDeploymentOnSuccess = deploymentRunning(node);
  $("node-deploy-password").value = ""; $("node-deploy").showModal(); renderDeployment();
}
function renderDeployment() {
  const node = registry.nodes.find(row => row.id === deploymentNodeId); if (!node) return;
  const running = deploymentRunning(node); const count = node.workers.find(row => row.role === 'incremental')?.count ?? 0;
  $("node-deploy-name").textContent = node.name;
  $("node-deploy-plan").textContent = `使用已保存计划：${count} 个增量 Worker。每个容器内存上限 768 MiB，合计 ${count * 768} MiB。`;
  const steps = ['center','ssh','files','start','verify','connection'];
  [...$("node-deploy-steps").children].forEach((element,index) => {
    const state = node.deployment?.steps?.[steps[index]] ?? 'pending'; element.dataset.state = state;
    element.querySelector('span').textContent = ({pending:'待执行',running:'执行中',completed:'已完成',failed:'失败'})[state] ?? '待执行';
  });
  $("node-deploy-start").disabled = !workerDeploymentAvailable || running || !!$("node-deploy").dataset.saving || !count;
  $("node-deploy-start").textContent = running ? '正在部署…' : node.deployment ? '部署 / 重试已保存计划' : '部署已保存的计划';
  $("node-deploy-password-field").hidden = running;
  $("node-deploy-status").textContent = !workerDeploymentAvailable ? '中心尚未配置 Worker 部署服务；保存的计划已保留。' : node.deployment?.error || (node.deployment?.state === 'connected' ? `已完成 ${node.deployment.appliedCount} 个 Worker 的部署和连接检查。中心启用后自动接收任务。` : running ? '正在后台部署，可关闭窗口，稍后重新查看。' : '点击后将实际下发配置、启动容器并检查连接。');
  if (closeDeploymentOnSuccess && node.deployment?.state === 'connected' && $("node-deploy").open) {
    closeDeploymentOnSuccess = false; $("node-deploy").close(); announce(`${node.name}：Worker 部署和连接检查完成。`);
  }
}
$("node-deploy-start").addEventListener('click', async () => {
  if (!deploymentNodeId || $("node-deploy").dataset.saving) return;
  const dialog = $("node-deploy"); dialog.dataset.saving = 'true'; $("node-deploy-start").disabled = true;
  let password = $("node-deploy-password").value; $("node-deploy-password").value = '';
  let accepted = false;
  try {
    registry = await request(`/api/server-nodes/${encodeURIComponent(deploymentNodeId)}/deploy-workers`, {method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({version:registry.version,password})});
    accepted = true; closeDeploymentOnSuccess = true; render();
  } catch (error) { $("node-deploy-status").textContent = error.message; closeDeploymentOnSuccess = false; }
  finally {
    password = undefined; delete dialog.dataset.saving;
    if (accepted) renderDeployment();
    else $("node-deploy-start").disabled = !workerDeploymentAvailable || deploymentRunning(registry.nodes.find(row => row.id === deploymentNodeId));
  }
});
