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
let workerEditor = null;
let detailId = null;
let refreshing = false;

function announce(message, error = false) {
  const element = $("nodes-message");
  element.textContent = message;
  element.dataset.error = String(error);
  element.hidden = !message;
}

async function request(path, options = {}) {
  const response = await fetch(path, { ...options, headers: { "Accept": "application/json", ...options.headers }, signal: AbortSignal.timeout(15000) });
  if (response.redirected || !response.headers.get("content-type")?.includes("application/json")) throw new Error("读取失败或登录已过期，请刷新页面后重试");
  const result = await response.json();
  if (!response.ok) throw new Error(result.error || "保存失败，请稍后重试");
  if (!Number.isInteger(result.version) || !Array.isArray(result.nodes)) throw new Error("服务器列表格式异常，请稍后刷新");
  return result;
}

function metrics() {
  return `<div class="nodes-metrics">${["CPU", "内存", "磁盘", "网络"].map(label => `<div class="nodes-metric"><span>${label}</span><strong>—</strong><small>未接入</small></div>`).join("")}</div>`;
}

function card(node) {
  const total = node.workers.reduce((sum, worker) => sum + worker.count, 0);
  return `<article class="nodes-card">
    <div class="nodes-card-main"><div class="nodes-card-top"><div class="nodes-card-icon">${serverIcon}</div><div class="nodes-card-title"><h3>${escapeHtml(node.name)}</h3><div class="nodes-address">${escapeHtml(node.host)} · ${node.port}</div></div><span class="nodes-badge">已登记</span></div>
      <div class="nodes-card-tags"><span class="nodes-badge ${node.kind === "center" ? "center" : ""}">${node.kind === "center" ? "中心节点" : "执行节点"}</span><span class="nodes-badge pending">监控未接入</span></div>
      ${metrics()}<div class="nodes-card-workers"><span>Worker 计划</span><strong>${total ? total + " 个" : "未配置"}</strong>${total ? '<span class="nodes-badge pending">尚未部署</span>' : ""}</div>
    </div><div class="nodes-card-footer"><span>SSH 尚未验证</span><div><button type="button" class="nodes-text-button" data-detail="${escapeHtml(node.id)}">查看详情</button><button type="button" class="nodes-text-button" data-workers="${escapeHtml(node.id)}">配置 Worker</button></div></div>
  </article>`;
}

function render() {
  if (!registry) return;
  $("nodes-total").textContent = registry.nodes.length;
  $("nodes-planned").textContent = registry.nodes.reduce((sum, node) => sum + node.workers.reduce((n, worker) => n + worker.count, 0), 0);
  $("nodes-add").disabled = false;
  const search = $("nodes-search").value.trim().toLowerCase();
  const kind = $("nodes-kind").value;
  const nodes = registry.nodes.filter(node => (kind === "all" || node.kind === kind) && `${node.name} ${node.host}`.toLowerCase().includes(search));
  $("nodes-count").textContent = registry.nodes.length ? `${nodes.length} / ${registry.nodes.length}` : "";
  if (!registry.nodes.length) {
    $("nodes-list").innerHTML = `<div class="nodes-empty"><div class="nodes-empty-icon">${serverIcon}</div><h3>添加你的第一台服务器</h3><p>从中心服务器或一个采集节点开始。添加后可以配置 Worker 计划，后续再接入监控和部署。</p><button type="button" class="nodes-button primary" data-add>＋ 添加服务器</button><div class="nodes-steps"><b>01 登记服务器</b><span>02 配置 Worker</span><span>03 接入与部署</span></div></div>`;
  } else if (!nodes.length) {
    $("nodes-list").innerHTML = '<div class="nodes-empty"><h3>没有匹配的服务器</h3><p>换一个名称、IP 地址或节点类型试试。</p><button type="button" class="nodes-button" data-clear>清除筛选</button></div>';
  } else {
    $("nodes-list").innerHTML = nodes.map(card).join("");
  }
}

async function refresh() {
  if (refreshing) return;
  refreshing = true;
  $("nodes-refresh").disabled = true;
  $("nodes-refresh").textContent = "刷新中…";
  try { registry = await request("/api/server-nodes"); announce(""); render(); }
  catch (error) {
    announce(error.message, true);
    if (!registry) $("nodes-list").innerHTML = '<div class="nodes-empty"><h3>暂时无法读取服务器</h3><p>已有配置不会丢失，请稍后重试。</p><button type="button" class="nodes-button" data-retry>重新加载</button></div>';
  } finally { refreshing = false; $("nodes-refresh").disabled = false; $("nodes-refresh").textContent = "刷新"; }
}

function openEditor(node = null) {
  if (!registry) return;
  editor = { node, version: registry.version };
  const form = $("node-form");
  form.reset();
  for (const field of ["name", "host", "port", "username", "sshAlias", "kind", "notes"]) {
    form.elements[field].value = node?.[field] ?? ({ port: 22, kind: "execution" }[field] ?? "");
  }
  $("node-editor-title").textContent = node ? "编辑服务器" : "添加服务器";
  $("node-form-error").hidden = true;
  $("node-editor").showModal();
  form.elements.name.focus();
}

function openDetail(id) {
  const node = registry.nodes.find(item => item.id === id);
  if (!node) return;
  detailId = id;
  $("node-detail-title").textContent = node.name;
  const fields = [["节点类型", node.kind === "center" ? "中心节点" : "执行节点"], ["服务器地址", node.host], ["SSH 用户名", node.username], ["SSH 端口", node.port], ["SSH 配置别名", node.sshAlias || "未填写"], ["配置更新时间", new Date(node.updatedAt).toLocaleString("zh-CN")]];
  $("node-detail-content").innerHTML = `<div class="nodes-detail-section"><dl class="nodes-detail-meta">${fields.map(([key, value]) => `<div><dt>${key}</dt><dd>${escapeHtml(value)}</dd></div>`).join("")}</dl>${node.notes ? `<p class="nodes-detail-notes">${escapeHtml(node.notes)}</p>` : ""}</div>
    <section class="nodes-detail-section"><h3>资源监控 <span class="nodes-badge">尚未接入</span></h3>${metrics()}<div class="nodes-monitor-placeholder"><strong>等待接入 Beszel 监控</strong>接入后可查看 CPU、内存、磁盘和网络历史曲线。</div></section>
    <section class="nodes-detail-section"><h3>Worker 配置</h3>${node.workers.length ? node.workers.map(worker => `<div class="nodes-worker-summary"><span>${escapeHtml(roles[worker.role]?.[0] ?? worker.role)}</span><span><strong>计划 ${worker.count} 个</strong><span class="nodes-badge pending">尚未部署</span></span></div>`).join("") : '<p class="nodes-footnote">尚未配置 Worker。可以先保存类型和数量，后续再部署。</p>'}</section>
    <p class="nodes-footnote">SSH 连通性、实际运行状态和部署记录尚未接入。</p>`;
  $("node-detail").showModal();
}

function openWorkers(id) {
  const node = registry.nodes.find(item => item.id === id);
  if (!node) return;
  workerEditor = { node, version: registry.version };
  $("node-workers-name").textContent = node.name;
  $("node-workers-error").hidden = true;
  $("node-worker-fields").innerHTML = Object.entries(roles).map(([role, [label, description]]) => `<div class="nodes-worker-row"><div><label for="worker-${role}">${label}</label><small>${description}</small></div><div class="nodes-worker-count"><input id="worker-${role}" name="${role}" type="number" min="0" max="100" required value="${node.workers.find(worker => worker.role === role)?.count ?? 0}"><span>个</span></div></div>`).join("");
  $("node-workers").showModal();
}

function nodeInput(node) {
  return Object.fromEntries(["name", "host", "port", "username", "sshAlias", "kind", "notes", "workers"].map(key => [key, node[key]]));
}

async function saveForm({ form, dialog, errorId, edit, node, message }) {
  const controls = [...form.querySelectorAll("button,input,textarea,select")];
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
  } catch (error) {
    $(errorId).textContent = error.message;
    $(errorId).hidden = false;
  } finally {
    controls.forEach(control => { control.disabled = false; });
    delete dialog.dataset.saving;
  }
}

$("node-form").addEventListener("submit", event => {
  event.preventDefault();
  if (!editor || $("node-editor").dataset.saving) return;
  const values = Object.fromEntries(new FormData(event.currentTarget));
  void saveForm({ form: event.currentTarget, dialog: $("node-editor"), errorId: "node-form-error", edit: editor,
    node: { ...values, port: Number(values.port), workers: editor.node?.workers ?? [] },
    message: "服务器配置已保存，尚未连接或部署。" });
});
$("node-workers-form").addEventListener("submit", event => {
  event.preventDefault();
  if (!workerEditor || $("node-workers").dataset.saving) return;
  const workers = [...new FormData(event.currentTarget)].map(([role, count]) => ({ role, count: Number(count) })).filter(worker => worker.count > 0);
  void saveForm({ form: event.currentTarget, dialog: $("node-workers"), errorId: "node-workers-error", edit: workerEditor,
    node: { ...nodeInput(workerEditor.node), workers }, message: "Worker 计划已保存，尚未部署；实际运行数量未改变。" });
});

document.addEventListener("click", event => {
  const target = event.target.closest("button");
  if (!target || target.disabled) return;
  if (target.hasAttribute("data-close")) $(target.dataset.close).close();
  if (target.hasAttribute("data-add")) openEditor();
  if (target.hasAttribute("data-detail")) openDetail(target.dataset.detail);
  if (target.hasAttribute("data-workers")) openWorkers(target.dataset.workers);
  if (target.hasAttribute("data-retry")) void refresh();
  if (target.hasAttribute("data-clear")) { $("nodes-search").value = ""; $("nodes-kind").value = "all"; render(); }
});
for (const dialog of document.querySelectorAll(".nodes-dialog")) {
  dialog.addEventListener("cancel", event => { if (dialog.dataset.saving) event.preventDefault(); });
  dialog.addEventListener("click", event => { if (dialog.dataset.saving && event.target === dialog) event.stopPropagation(); });
}
$("nodes-add").addEventListener("click", () => openEditor());
$("nodes-refresh").addEventListener("click", () => void refresh());
$("nodes-search").addEventListener("input", render);
$("nodes-kind").addEventListener("change", render);
$("node-detail-edit").addEventListener("click", () => { $("node-detail").close(); openEditor(registry.nodes.find(node => node.id === detailId)); });
$("node-detail-workers").addEventListener("click", () => { $("node-detail").close(); openWorkers(detailId); });
void refresh();
