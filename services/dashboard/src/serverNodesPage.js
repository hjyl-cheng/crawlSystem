export function renderServerNodesPage() {
  return `
  <link rel="stylesheet" href="/assets/server-nodes.css">
  <section class="nodes-page" aria-labelledby="nodes-title">
    <header class="nodes-header">
      <div><div class="nodes-eyebrow">基础设施 / 节点管理</div><h1 id="nodes-title">服务器节点</h1><p>管理服务器与 Worker 配置，逐步接通采集能力。</p></div>
      <div class="nodes-actions"><button type="button" class="nodes-button" id="nodes-refresh">刷新</button><button type="button" class="nodes-button primary" id="nodes-add" disabled><span aria-hidden="true">＋</span> 添加服务器</button></div>
    </header>
    <div id="nodes-message" role="status" aria-live="polite" hidden></div>
    <div class="nodes-summary" aria-label="节点概览">
      <div class="nodes-stat"><span>已登记服务器</span><strong id="nodes-total">—</strong><small>手工添加的节点</small></div>
      <div class="nodes-stat"><span>在线服务器</span><strong>—</strong><small>等待接入监控</small></div>
      <div class="nodes-stat"><span>计划 Worker</span><strong id="nodes-planned">—</strong><small>已保存，尚未部署</small></div>
      <div class="nodes-stat"><span>实际运行 Worker</span><strong>—</strong><small>等待接入运行状态</small></div>
    </div>
    <div class="nodes-stage"><span class="nodes-stage-icon" aria-hidden="true">i</span><div><strong>先配置，再接入</strong><p>当前支持登记服务器和保存 Worker 计划。保存不会连接服务器或部署容器；实时监控和执行操作将在后续接入。</p></div></div>
    <div class="nodes-list-heading"><h2>服务器列表 <span id="nodes-count"></span></h2><div class="nodes-filters"><label class="nodes-search"><span class="nodes-sr-only">搜索服务器名称或地址</span><input id="nodes-search" type="search" placeholder="搜索名称或 IP 地址" autocomplete="off"></label><label><span class="nodes-sr-only">筛选节点类型</span><select id="nodes-kind"><option value="all">全部类型</option><option value="center">中心节点</option><option value="execution">执行节点</option></select></label></div></div>
    <div id="nodes-list" class="nodes-list" aria-live="polite"><div class="nodes-empty"><span class="nodes-loading">正在读取服务器列表…</span></div></div>
    <noscript><p class="nodes-stage">请启用 JavaScript 以加载和管理服务器。</p></noscript>
  </section>

  <dialog id="node-editor" class="nodes-dialog" aria-labelledby="node-editor-title">
    <form id="node-form">
      <div class="nodes-dialog-heading"><div><div class="nodes-eyebrow">节点配置</div><h2 id="node-editor-title">添加服务器</h2></div><button class="nodes-icon-button" type="button" data-close="node-editor" aria-label="关闭">×</button></div>
      <p class="nodes-dialog-intro">填写连接信息并保存。此步骤不会测试 SSH，也不会安装任何服务。</p>
      <div class="nodes-form-grid">
        <label class="nodes-field wide">服务器名称 <input name="name" required maxlength="80" placeholder="例如：增量采集节点 01" autocomplete="off"></label>
        <label class="nodes-field">节点类型<select name="kind"><option value="execution">执行节点</option><option value="center">中心节点</option></select></label>
        <label class="nodes-field">SSH 用户名<input name="username" required maxlength="64" placeholder="ubuntu" autocomplete="off"></label>
        <label class="nodes-field">IP 地址 / 主机名<input name="host" required maxlength="253" placeholder="服务器 IP 或主机名" autocomplete="off" spellcheck="false"></label>
        <label class="nodes-field">SSH 端口<input name="port" type="number" required min="1" max="65535" value="22"></label>
        <label class="nodes-field wide">SSH 配置别名 <span class="nodes-optional">可选</span><input name="sshAlias" maxlength="80" placeholder="例如：qy-node-1" autocomplete="off" spellcheck="false"><small>记录中心服务器已有的 SSH 别名，不会创建或修改 SSH 配置。</small></label>
        <label class="nodes-field wide">备注 <span class="nodes-optional">可选</span><textarea name="notes" rows="3" maxlength="500" placeholder="用途、机房或其他需要记录的信息"></textarea></label>
      </div>
      <p id="node-form-error" class="nodes-form-error" role="alert" hidden></p>
      <div class="nodes-dialog-footer"><button type="button" class="nodes-button" data-close="node-editor">取消</button><button type="submit" class="nodes-button primary" id="node-save">保存服务器</button></div>
    </form>
  </dialog>

  <dialog id="node-detail" class="nodes-dialog nodes-detail-dialog" aria-labelledby="node-detail-title">
    <div class="nodes-dialog-heading"><div><div class="nodes-eyebrow">服务器详情</div><h2 id="node-detail-title"></h2></div><button class="nodes-icon-button" type="button" data-close="node-detail" aria-label="关闭">×</button></div>
    <div id="node-detail-content"></div>
    <div class="nodes-dialog-footer"><button type="button" class="nodes-button" data-close="node-detail">关闭</button><button type="button" class="nodes-button" id="node-detail-edit">编辑服务器</button><button type="button" class="nodes-button primary" id="node-detail-workers">配置 Worker</button></div>
  </dialog>

  <dialog id="node-workers" class="nodes-dialog" aria-labelledby="node-workers-title">
    <form id="node-workers-form">
      <div class="nodes-dialog-heading"><div><div class="nodes-eyebrow" id="node-workers-name"></div><h2 id="node-workers-title">Worker 计划</h2></div><button class="nodes-icon-button" type="button" data-close="node-workers" aria-label="关闭">×</button></div>
      <p class="nodes-dialog-intro">选择 Worker 类型和计划数量。保存仅更新配置，不会启动、停止或调整正在运行的 Worker。</p>
      <div id="node-worker-fields"></div>
      <p class="nodes-footnote">填 0 表示不配置该类型。实际并发和运行数量会在部署能力接入后显示。</p>
      <p id="node-workers-error" class="nodes-form-error" role="alert" hidden></p>
      <div class="nodes-dialog-footer"><button type="button" class="nodes-button" data-close="node-workers">取消</button><button type="submit" class="nodes-button primary" id="node-workers-save">保存计划</button></div>
    </form>
  </dialog>
  <script type="module" src="/assets/server-nodes.js"></script>`;
}
