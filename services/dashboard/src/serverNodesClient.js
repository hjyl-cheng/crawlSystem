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
let workerDeploymentAvailable = false;
let workerRemovalAvailable = false;
let runtimeAvailable = false;
let runtimeId = null;
let closeRuntimeOnSuccess = false;
const observations = new Map();
const executionStates = new Map();
const executionActions = new Set();
const isReady = node => node?.provisioning?.state === "ready";
const isRunning = node => node?.provisioning?.state === "running" && Date.parse(node.provisioning.deadline) > Date.now();
const runtimeRunning = node => node?.runtime?.state === "running" && Date.parse(node.runtime.deadline) > Date.now();
const deploymentRunning = node => node?.deployment?.state === 'running' && Date.parse(node.deployment.deadline) > Date.now();
const runtimeState = node => node?.runtime?.state === "ready" ? "运行环境就绪" : runtimeRunning(node) ? "运行环境准备中" : node?.runtime?.state === "failed" ? "环境准备失败，可重试" : node?.runtime?.state === "running" ? "环境准备中断，可重试" : "运行环境尚未准备";
const nodeState = node => isReady(node) ? "已初始化" : isRunning(node) ? "初始化中" : node.provisioning?.state === "failed" ? "初始化失败" : node.provisioning?.state === "running" ? "初始化中断，可重试" : "待初始化";

function announce(message, error = false) {
  const element = $("nodes-message");
  element.textContent = message;
  element.dataset.error = String(error);
  element.hidden = !message;
}

async function request(path, options = {}, expectRegistry = true) {
  const response = await fetch(path, { ...options, headers: { "Accept": "application/json", ...options.headers }, signal: AbortSignal.timeout(options.method === "DELETE" ? 240000 : 15000) });
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

let workerManager = null;
const lastIntakeCounts=new Map();
function rememberIntake(id,count){
  if(!Number.isInteger(count)||count<1||lastIntakeCounts.get(id)===count)return;
  lastIntakeCounts.set(id,count);
  try{localStorage.setItem('qy-worker-intake-'+id,String(count));}catch{}
}
function resumeIntakeCount(id,installed){
  let remembered=lastIntakeCounts.get(id);
  if(!remembered){try{remembered=Number(localStorage.getItem('qy-worker-intake-'+id));}catch{}}
  return Math.min(installed,Number.isInteger(remembered)&&remembered>0?remembered:installed);
}
const installedCount = node => executionStates.get(node.id)?.counts?.deployed ?? node.deployment?.appliedCount ?? 0;
function executionPanel(node) {
  if (!node.deployment && !node.localIntake) return '<p class="nodes-worker-gate">尚未部署 Worker</p>';
  const state = executionStates.get(node.id), known = state?.counts;
  const counts = state?.counts, allowed = state?.allowedCount ?? 0;
  const label = !known ? state?.error ? '接任务状态暂不可用' : '正在读取接任务状态'
    : state.adjusting ? '接单设置已保存，正在调整'
    : counts.draining ? '正在调整，当前频道完成后待命'
    : allowed ? counts.ready ? '正在接任务' : '正在准备接任务' : '已暂停接单';
  return `<div class="nodes-execution"><div><strong>${label}${known && state.error ? '（上次状态）' : ''}</strong>${known ? `<small>已部署 ${counts.deployed} · 允许接任务 ${allowed} · 已连接 ${counts.connected}</small><small>执行 ${counts.running ?? counts.active} · 空闲 ${counts.idle} · 收尾 ${counts.draining} · 待命 ${counts.standby}</small>` : ''}${known && state.error ? '<small>状态更新失败，正在重试；以上为上次读取结果。</small>' : ''}</div></div>`;
}
function workerActions(node) {
  const state=executionStates.get(node.id);
  const enabled=node.localIntake || (isReady(node) && node.runtime?.state==='ready');
  if(state&&!state.error)rememberIntake(node.id,state.allowedCount);
  const installed=installedCount(node),pause=state?.allowedCount>0,busy=executionActions.has(node.id);
  const toggle=installed?`<button type="button" class="nodes-button" data-toggle-intake="${escapeHtml(node.id)}" title="${pause?'停止接新任务，已领取频道继续收尾':`允许 ${resumeIntakeCount(node.id,installed)} 个 Worker 接任务，可在管理窗口调整`}"
    ${busy||!state||state.error||!state.executionAvailable?'disabled':''}>${busy?'正在切换…':pause?'暂停接任务':'开始接任务'}</button>`:'';
  return `<button type="button" class="nodes-button primary" data-manage="${escapeHtml(node.id)}" ${enabled?'':'disabled'}>${node.localIntake || installed>0 || node.deployment?'管理 Worker':'部署 Worker'}</button>${toggle}`;
}
function deploymentNotice(node) {
  const d=node.deployment;
  if(!d)return '';
  if(deploymentRunning(node))return `<p class="nodes-deployment-notice">正在部署至 ${d.desiredCount} 个 · 已确认部署 ${installedCount(node)} 个</p>`;
  if(d.state==='failed'||d.state==='running')return `<p class="nodes-deployment-notice error">本次部署未完成 · 已确认部署 ${installedCount(node)} 个。请在管理 Worker 中查看原因或重试。</p>`;
  if(d.intakeSync?.state==='failed')return '<p class="nodes-deployment-notice error">部署已完成，接单数量未自动同步，请在管理 Worker 中核实。</p>';
  return '';
}
function openWorkerManager(id) {
  const node=registry.nodes.find(n=>n.id===id);if(!node)return;
  $('node-detail').close();
  workerManager={id,version:registry.version,expectedInstalledCount:installedCount(node),deploymentDirty:false,intakeDirty:false,lastOperation:null,lastState:null};
  $('worker-deployment-count').value=1;
  $('worker-deployment-role').value='incremental';
  $('worker-intake-count').value=executionStates.get(id)?.allowedCount??0;
  $('worker-sync-intake').checked=!node.deployment;
  $('worker-deployment-password').value='';
  $('worker-removal-password').value='';
  $('worker-removal-error').hidden=true;
  for(const name of ['worker-deployment-error','worker-intake-error']){$(name).hidden=true;$(name).textContent='';}
  $('worker-intake-result').textContent='';
  $('worker-deployment-history').open=false;
  renderWorkerManager();$('node-worker-manager').showModal();$('node-worker-manager').scrollTop=0;
}
function renderWorkerManager() {
  if(!workerManager)return;
  const node=registry.nodes.find(n=>n.id===workerManager.id);if(!node){$('node-worker-manager').close();return;}
  const state=executionStates.get(node.id),counts=state?.counts,known=!!counts&&!state.error;
  const d=node.deployment,removing=node.workerRemoval&&!['completed','rejected'].includes(node.workerRemoval.state),running=deploymentRunning(node),busy=!!$('node-worker-manager').dataset.saving||!!removing;
  const installed=installedCount(node);
  $('worker-manager-name').textContent=node.name;
  $('worker-manager-summary').innerHTML=executionPanel(node);
  $('worker-deployment-section').hidden=!!node.localIntake;
  $('worker-center-note').hidden=!node.localIntake;
  $('worker-installed-label').textContent=`已确认部署 ${installed} 个`;
  if(!workerManager.deploymentDirty){
    workerManager.version=registry.version;
    workerManager.expectedInstalledCount=installed;
  }
  const additional=Number($('worker-deployment-count').value),target=workerManager.expectedInstalledCount+additional;
  const valid=Number.isSafeInteger(additional)&&additional>0&&Number.isSafeInteger(target);
  $('worker-deployment-count').removeAttribute('max');
  $('worker-deployment-count').disabled=busy||running;
  $('worker-deployment-role').disabled=busy||running;
  $('worker-sync-intake').disabled=busy||running||!!d&&(!known||!state.executionAvailable);
  $('worker-deployment-password-field').hidden=running;
  $('worker-deployment-impact').textContent=running?`正在部署至 ${d.desiredCount} 个。已确认部署 ${installed} 个，进度会自动更新。`
    :valid?`已有 ${workerManager.expectedInstalledCount} 个增量 Worker ＋ 本次新增 ${additional} 个 ＝ 新增后共 ${target} 个。现有 Worker 继续运行。`
    :'请输入有效的新增数量（正整数）。';
  const memoryRequired=(target*256+1536)/1024,memoryTotal=observations.get(node.id)?.metrics?.memoryTotalGiB;
  $('worker-deployment-memory').textContent=valid
    ?`内存参考：按每个 Worker 256 MiB ＋ 系统预留 1.5 GiB 估算，${target} 个约需 ${memoryRequired.toFixed(2)} GiB。${Number.isFinite(memoryTotal)?` 本机总内存约 ${Number(memoryTotal).toFixed(2)} GiB。`:''}仅供参考，不限制新增数量，请按实际运行情况自行安排。`:'';
  $('worker-deployment-save').disabled=busy||running||!workerDeploymentAvailable||node.runtime?.state!=='ready'||!valid
    ||$('worker-sync-intake').checked&&!!d&&(!known||!state.executionAvailable);
  $('worker-deployment-save').textContent=running?'正在新增…':`新增 ${Number.isInteger(additional)&&additional>0?additional:'—'} 个增量 Worker`;
  if(!workerManager.intakeDirty&&known){$('worker-intake-count').value=state.allowedCount;workerManager.expectedAllowedCount=state.allowedCount;}
  $('worker-intake-count').max=installed;
  $('worker-intake-count').disabled=busy||!known||!state.executionAvailable||!installed;
  $('worker-allowed-label').textContent=known?`当前允许 ${state.allowedCount} 个 · 可设 0–${installed}`:'等待接单状态';
  $('worker-intake-save').disabled=busy||!known||!state.executionAvailable||!installed;
  renderWorkerRemoval(node,state);
  const history=$('worker-deployment-history');history.hidden=!d||!!node.localIntake;
  if(d){
    const changed=workerManager.lastOperation!==d.operationId||workerManager.lastState!==d.state;
    if(changed)history.open=d.state!=='connected'||d.intakeSync?.state==='failed';
    workerManager.lastOperation=d.operationId;workerManager.lastState=d.state;
    $('worker-deployment-history-title').textContent=running?`部署进度 · 目标 ${d.desiredCount} 个`:d.state==='connected'?'最近部署记录 · 已完成':'最近部署记录 · 未完成';
    $('worker-deployment-status').textContent=d.error||d.intakeSync?.error||(running?'部署在后台执行，可以关闭窗口。':d.state==='connected'?`已确认部署 ${d.appliedCount} 个 Worker。${d.intakeSync?.state==='completed'?'接单数量已同步。':''}`:'上次部署未完成，可重试。');
    const steps=[['ssh','连接与资源检查'],['center','准备中心接入'],['files','准备部署文件'],['start','启动 Worker'],['verify','检查运行状态'],['connection','确认连接中心']];
    if(d.intakeSync)steps.push(['intake','同步接单数量']);
    $('worker-deployment-steps').innerHTML=steps.map(([key,label],i)=>{const status=key==='intake'?d.intakeSync.state:d.steps?.[key]??'pending';return `<li data-state="${escapeHtml(status)}"><b>${i+1}</b><div><strong>${label}</strong></div><span>${({pending:'待执行',running:'执行中',completed:'已完成',failed:'未完成'})[status]??'待执行'}</span></li>`;}).join('');
  }
}
function renderWorkerRemoval(node,state){
  $('worker-removal-section').hidden=!!node.localIntake||!workerRemovalAvailable||!node.deployment;
  const removal=node.workerRemoval;
  const inProgress=removal?.state==='running'&&Date.parse(removal.deadline)>Date.now();
  const pending=removal&&!['completed','rejected'].includes(removal.state);
  const known=!!state&&!state.error;
  const busy=!!$('node-worker-manager').dataset.saving||deploymentRunning(node)||inProgress;
  $('worker-removal-status').textContent=removal?inProgress?`正在核实并删除 ${removal.slot}，可以关闭窗口等待。`
    :removal.state==='completed'?`${removal.slot} 已删除。`:removal.error||'上次删除中断，可重试继续。':'';
  const installedSlots=new Set(node.deployment?.slots??Array.from({length:installedCount(node)},(_,i)=>`incremental-${i+1}`));
  const rows=known?[...(state.workers??[])].filter(w=>installedSlots.has(w.slot)).sort((a,b)=>a.slot.localeCompare(b.slot,'en',{numeric:true})):[];
  $('worker-removal-list').innerHTML=known?rows.map(w=>{
    const retry=pending&&removal.slot===w.slot&&!inProgress;
    const idle=w.connected&&!w.active;
    const disabled=busy||node.deployment.state!=='connected'||(!retry&&(!idle||pending));
    const label=w.retiring?'正在移除':!w.connected?'状态未知':w.active?'执行 / 恢复中':w.requested?'空闲，可接任务':'空闲，待命';
    return `<div class="nodes-worker-row"><span><strong>${escapeHtml(w.slot)}</strong><small>${label}</small></span><button type="button" class="nodes-button danger" data-remove-worker="${escapeHtml(w.slot)}" ${disabled?'disabled':''}>${retry?'重试删除':'删除'}</button></div>`;
  }).join('')||'<p class="nodes-manager-help">暂无已部署的 Worker。</p>':'<p class="nodes-manager-help">正在读取 Worker 状态，暂时不能删除。</p>';
  $('worker-removal-form').hidden=!workerManager.removalSlot;
  $('worker-removal-title').textContent=`删除 ${workerManager.removalSlot??''}？`;
  const target=rows.find(w=>w.slot===workerManager.removalSlot);
  $('worker-removal-confirm').disabled=busy||!target||(!target.connected||target.active)&&!(pending&&removal.slot===target.slot);
}
async function removeIdleWorker(event){
  event.preventDefault();const manager=workerManager,dialog=$('node-worker-manager');
  if(!manager?.removalSlot||dialog.dataset.saving)return;
  dialog.dataset.saving='true';$('worker-removal-error').hidden=true;
  let password=$('worker-removal-password').value;$('worker-removal-password').value='';
  try{
    registry=await request(`/api/server-nodes/${encodeURIComponent(manager.id)}/remove-worker`,{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({version:registry.version,slot:manager.removalSlot,password})});
    manager.removalSlot=null;manager.deploymentDirty=false;manager.intakeDirty=false;announce('已开始检查并删除空闲 Worker，进度会自动更新。');
  }catch(error){$('worker-removal-error').textContent=error.message;$('worker-removal-error').hidden=false;}
  finally{password='';delete dialog.dataset.saving;render();renderWorkerManager();}
}
async function saveIntake(id,allowedCount,expectedAllowedCount) {
  if(executionActions.has(id))return;
  executionActions.add(id);
  render();
  try{
    const result=await request(`/api/server-nodes/${encodeURIComponent(id)}/execution`,{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({version:registry.version,allowedCount,expectedAllowedCount})},false);
    executionStates.set(id,result);return result;
  }finally{executionActions.delete(id);render();}
}
$('worker-deployment-count').addEventListener('input',()=>{if(workerManager){workerManager.deploymentDirty=true;renderWorkerManager();}});
$('worker-intake-count').addEventListener('input',()=>{if(workerManager)workerManager.intakeDirty=true;});
$('worker-deployment-form').addEventListener('submit',async event=>{
  event.preventDefault();if(!workerManager||$('worker-deployment-save').disabled)return;
  const manager=workerManager,dialog=$('node-worker-manager'),node=registry.nodes.find(n=>n.id===manager.id);
  let password=$('worker-deployment-password').value;$('worker-deployment-password').value='';
  const syncIntake=$('worker-sync-intake').checked;
  const expectedAllowedCount=node.deployment?executionStates.get(node.id)?.allowedCount:0;
  dialog.dataset.saving='true';$('worker-deployment-error').hidden=true;renderWorkerManager();
  try{
    registry=await request(`/api/server-nodes/${encodeURIComponent(manager.id)}/deploy-workers`,{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({version:manager.version,additionalCount:Number($('worker-deployment-count').value),role:$('worker-deployment-role').value,expectedInstalledCount:manager.expectedInstalledCount,password,syncIntake,...(syncIntake?{expectedAllowedCount}:{})})});
    manager.deploymentDirty=false;$('worker-deployment-count').value=1;announce('新增 Worker 已开始，完成并核实连接后才更新已部署数量。');
  }catch(error){$('worker-deployment-error').textContent=error.message;$('worker-deployment-error').hidden=false;}
  finally{password=undefined;delete dialog.dataset.saving;render();renderWorkerManager();void refresh(true);}
});
$('worker-intake-form').addEventListener('submit',async event=>{
  event.preventDefault();if(!workerManager||$('worker-intake-save').disabled)return;
  const manager=workerManager,dialog=$('node-worker-manager');
  dialog.dataset.saving='true';$('worker-intake-error').hidden=true;renderWorkerManager();
  try{
    const count=Number($('worker-intake-count').value);
    await saveIntake(manager.id,count,manager.expectedAllowedCount);
    manager.intakeDirty=false;$('worker-intake-result').textContent=`允许接任务数量已保存为 ${count} 个。正在执行的频道会先完成，运行状态自动更新。`;
  }catch(error){$('worker-intake-error').textContent=error.message;$('worker-intake-error').hidden=false;}
  finally{delete dialog.dataset.saving;renderWorkerManager();void refresh(true);}
});
async function toggleIntake(id){
  const state=executionStates.get(id);if(!state||state.error)return;
  const pause=state.allowedCount>0;
  if(pause)rememberIntake(id,state.allowedCount);
  const count=pause?0:resumeIntakeCount(id,state.counts.deployed);
  try{await saveIntake(id,count,state.allowedCount);announce(pause?'已暂停接新任务，正在执行的频道完成后待命。':`已允许 ${count} 个 Worker 接任务，正在准备接单。`);}
  catch(error){announce(error.message,true);}
  void refresh(true);
}

function card(node) {
  if(node.localIntake)return `<article class="nodes-card"><div class="nodes-card-main"><div class="nodes-card-top"><div class="nodes-card-icon">${serverIcon}</div><div class="nodes-card-title"><h3>中心服务器</h3><div class="nodes-address">${escapeHtml(node.host)}</div></div></div><div class="nodes-card-tags"><span class="nodes-badge center">中心节点</span><span class="nodes-badge">增量采集</span></div>${executionPanel(node)}<p class="nodes-worker-gate">管理本机现有增量 Worker 的接单数量。</p></div><div class="nodes-card-footer">${workerActions(node)}</div></article>`;
  const ready=isReady(node),environmentAction=ready&&node.kind==='execution';
  const runtimeButton=`<button type="button" class="nodes-button primary" data-runtime="${escapeHtml(node.id)}">${runtimeRunning(node)?'查看环境准备进度':'准备运行环境'}</button>`;
  return `<article class="nodes-card"><div class="nodes-card-main"><div class="nodes-card-top"><div class="nodes-card-icon">${serverIcon}</div><div class="nodes-card-title"><h3>${escapeHtml(node.name)}</h3><div class="nodes-address">${escapeHtml(node.host)} · ${node.port}</div></div><details class="nodes-card-menu"><summary aria-label="${escapeHtml(node.name)}的更多操作">⋯</summary><div><button type="button" data-detail="${escapeHtml(node.id)}">服务器详情</button><button type="button" data-edit="${escapeHtml(node.id)}">编辑服务器</button><button type="button" class="danger" data-delete="${escapeHtml(node.id)}">删除服务器</button></div></details></div>
    <div class="nodes-card-tags"><span class="nodes-badge ${node.kind==='center'?'center':''}">${node.kind==='center'?'中心节点':'执行节点'}</span><span class="nodes-badge ${ready?'center':'pending'}">${nodeState(node)}</span>${observations.has(node.id)?`<span class="nodes-badge">${observations.get(node.id).online?'监控在线':'监控暂无新数据'}</span>`:''}</div>
    ${metrics(node)}${executionPanel(node)}${deploymentNotice(node)}${!ready?`<p class="nodes-worker-gate">${escapeHtml(node.provisioning?.error||'完成初始化后，准备环境并部署 Worker。')}</p>`:node.runtime?.state!=='ready'?`<p class="nodes-worker-gate">${runtimeState(node)}</p>`:''}
    </div><div class="nodes-card-footer">${environmentAction?node.runtime?.state==='ready'?workerActions(node):runtimeButton:`<button type="button" class="nodes-button primary" data-initialize="${escapeHtml(node.id)}">${isRunning(node)?'查看初始化进度':node.provisioning?.state==='failed'?'重试初始化':'初始化服务器'}</button>`}</div></article>`;
}

function render() {
  if (!registry) return;
  $("nodes-total").textContent = registry.nodes.length;
  const states=[...executionStates.values()].filter(state=>state.counts&&!state.error);
  $('nodes-planned').textContent=states.length?states.reduce((sum,s)=>sum+s.counts.deployed,0):'—';
  $('nodes-allowed').textContent=states.length?states.reduce((sum,s)=>sum+s.allowedCount,0):'—';
  $('nodes-active').textContent=states.length?states.reduce((sum,s)=>sum+s.counts.active,0):'—';
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
  if (refreshing || (quiet && (document.querySelector('.nodes-card-menu[open], .nodes-dialog[data-saving="true"]') || [...document.querySelectorAll('.nodes-dialog[open]')].some(dialog => !["node-initialize", "node-runtime", "node-worker-manager", "node-detail"].includes(dialog.id))))) return;
  refreshing = true;
  $("nodes-refresh").disabled = true;
  $("nodes-refresh").textContent = "刷新中…";
  try {
    registry = await request("/api/server-nodes");
    onboardingAvailable = registry.capabilities?.onboarding === true;
    runtimeAvailable = registry.capabilities?.runtime === true;
    workerDeploymentAvailable = registry.capabilities?.workerDeployment === true;
    workerRemovalAvailable = registry.capabilities?.workerRemoval === true;
    if (!quiet) announce("");
    render();
    if ($("node-initialize").open) renderInitialization();
    if ($("node-runtime").open) renderRuntime();
    for (const id of observations.keys()) if (!registry.nodes.some(node => node.id === id)) observations.delete(id);
    for (const id of executionStates.keys()) if (!registry.nodes.some(node => node.id === id)) executionStates.delete(id);
    await Promise.allSettled([...registry.nodes.filter(node => node.provisioning?.systemId).map(async node => {
      try { observations.set(node.id, await request(`/api/server-nodes/${encodeURIComponent(node.id)}/monitoring`, {}, false)); }
      catch { const last = observations.get(node.id); observations.set(node.id, { ...last, online: false }); }
    }), ...registry.nodes.filter(node => node.deployment || node.localIntake).map(async node => {
      try { executionStates.set(node.id, await request(`/api/server-nodes/${encodeURIComponent(node.id)}/execution`, {}, false)); }
      catch { executionStates.set(node.id, { ...executionStates.get(node.id), error:true }); }
    })]);
    // Avoid replacing an open card menu during a background refresh.
    if (!document.querySelector(".nodes-card-menu[open]")) render();
    if ($("node-detail").open) renderDetail();
    if ($("node-worker-manager").open) renderWorkerManager();
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
  if (!registry.nodes.some(item => item.id === id)) return;
  detailId = id;
  renderDetail();
  $("node-detail").showModal();
}

function renderDetail() {
  const node = registry.nodes.find(item => item.id === detailId);
  if (!node) { $("node-detail").close(); return; }
  $("node-detail-title").textContent = node.name;
  const fields = [["节点类型", node.kind === "center" ? "中心节点" : "执行节点"], ["服务器地址", node.host], ["SSH 用户名", node.username], ["SSH 端口", node.port], ["SSH 配置引用", node.sshAlias || "初始化时自动配置"], ["配置更新时间", new Date(node.updatedAt).toLocaleString("zh-CN")]];
  $("node-detail-content").innerHTML = `<div class="nodes-detail-section"><dl class="nodes-detail-meta">${fields.map(([key, value]) => `<div><dt>${key}</dt><dd>${escapeHtml(value)}</dd></div>`).join("")}</dl>${node.notes ? `<p class="nodes-detail-notes">${escapeHtml(node.notes)}</p>` : ""}</div>
    <section class="nodes-detail-section"><h3>资源监控 <span class="nodes-badge">${observations.get(node.id)?.online ? "在线" : "暂无新数据"}</span></h3>${metrics(node)}<p class="nodes-footnote">${observations.get(node.id)?.sampleAt ? `最近数据：${escapeHtml(new Date(observations.get(node.id).sampleAt).toLocaleString("zh-CN"))}` : "等待接入 Beszel 监控"}</p></section>
    <p class="nodes-footnote">初始化状态：${nodeState(node)}。${runtimeState(node)}。</p>`;
  $('node-detail-initialize').textContent=isReady(node)?'初始化记录':'初始化服务器';
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

async function openDelete(id) {
  const node = registry.nodes.find(item => item.id === id);
  if (!node) return;
  const current = { id };
  deletion = current;
  $("node-delete-password").value = "";
  $("node-delete-password-field").hidden = true;
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
    $("node-delete-password-field").hidden = !check.requiresRemoteCheck;
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
      method: "DELETE", headers: { "Content-Type": "application/json" }, body: JSON.stringify({ version: current.version, password: $("node-delete-password").value }),
    });
    render();
    dialog.close();
    announce("服务器登记已删除。");
  } catch (error) {
    current.allowed = false;
    $("node-delete-reason").textContent = `${error.message}。请关闭窗口、刷新列表后重新确认。`;
    $("node-delete-confirm").textContent = "请重新检查";
  } finally {
    $("node-delete-password").value = "";
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
document.addEventListener("click", event => {
  for (const menu of document.querySelectorAll('.nodes-card-menu[open]')) {
    if (!menu.contains(event.target)) menu.removeAttribute('open');
  }
  const target = event.target.closest("button");
  if (!target || target.disabled) return;
  if(target.hasAttribute('data-remove-worker')){workerManager.removalSlot=target.dataset.removeWorker;renderWorkerManager();$('worker-removal-form').scrollIntoView({block:'nearest'});}
  if(target.hasAttribute('data-cancel-worker-removal')){workerManager.removalSlot=null;renderWorkerManager();}
  if (target.hasAttribute("data-manage")) openWorkerManager(target.dataset.manage);
  if (target.hasAttribute("data-toggle-intake")) void toggleIntake(target.dataset.toggleIntake);
  if (target.hasAttribute("data-close") && !$(target.dataset.close).dataset.saving) $(target.dataset.close).close();
  if (target.hasAttribute("data-add")) openEditor();
  if (target.hasAttribute("data-detail")) openDetail(target.dataset.detail);
  if (target.hasAttribute("data-initialize")) openInitialize(target.dataset.initialize);
  if (target.hasAttribute("data-runtime")) openRuntime(target.dataset.runtime);
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
document.addEventListener('keydown', event => {
  if (event.key === 'Escape') for (const menu of document.querySelectorAll('.nodes-card-menu[open]')) menu.removeAttribute('open');
});
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
setInterval(() => { if (!document.hidden) void refresh(true); }, 5000);
void refresh();

$('worker-removal-form').addEventListener('submit',removeIdleWorker);
