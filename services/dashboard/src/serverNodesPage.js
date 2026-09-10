export function renderServerNodesPage() {
  return `
  <link rel="stylesheet" href="/assets/server-nodes.css">
  <section class="nodes-page" aria-labelledby="nodes-title">
    <header class="nodes-header">
      <div><div class="nodes-eyebrow">基础设施 / 节点管理</div><h1 id="nodes-title">服务器节点</h1><p>添加服务器，完成连接与监控接入，再配置采集 Worker。</p></div>
      <div class="nodes-actions"><button type="button" class="nodes-button" id="nodes-refresh">刷新</button><button type="button" class="nodes-button primary" id="nodes-add" disabled><span aria-hidden="true">＋</span> 添加服务器</button></div>
    </header>
    <div id="nodes-message" role="status" aria-live="polite" hidden></div>
    <div class="nodes-summary" aria-label="节点概览">
      <div class="nodes-stat"><span>已登记服务器</span><strong id="nodes-total">—</strong><small>手工添加的节点</small></div>
      <div class="nodes-stat"><span>在线服务器</span><strong>—</strong><small>等待接入监控</small></div>
      <div class="nodes-stat"><span>计划 Worker</span><strong id="nodes-planned">—</strong><small>已保存，尚未部署</small></div>
      <div class="nodes-stat"><span>实际运行 Worker</span><strong>—</strong><small>等待接入运行状态</small></div>
    </div>
    <ol class="nodes-journey" aria-label="服务器接入流程"><li class="current"><b>01</b><div><strong>添加服务器</strong><small>地址与登录信息</small></div></li><li><b>02</b><div><strong>初始化与监控</strong><small>验证 SSH · 配置密钥 · 接入 Beszel</small></div></li><li><b>03</b><div><strong>配置 Worker</strong><small>节点就绪后添加</small></div></li></ol>
    <div class="nodes-stage"><span class="nodes-stage-icon" aria-hidden="true">i</span><div><strong>当前可保存服务器信息</strong><p>自动初始化正在接入，密码输入暂未开放。SSH 与监控验证成功后才能配置 Worker；删除前也需要核实运行和派发状态。</p></div></div>
    <div class="nodes-list-heading"><h2>服务器列表 <span id="nodes-count"></span></h2><div class="nodes-filters"><label class="nodes-search"><span class="nodes-sr-only">搜索服务器名称或地址</span><input id="nodes-search" type="search" placeholder="搜索名称或 IP 地址" autocomplete="off"></label><label><span class="nodes-sr-only">筛选节点类型</span><select id="nodes-kind"><option value="all">全部类型</option><option value="center">中心节点</option><option value="execution">执行节点</option></select></label></div></div>
    <div id="nodes-list" class="nodes-list" aria-live="polite"><div class="nodes-empty"><span class="nodes-loading">正在读取服务器列表…</span></div></div>
    <noscript><p class="nodes-stage">请启用 JavaScript 以加载和管理服务器。</p></noscript>
  </section>

  <dialog id="node-editor" class="nodes-dialog" aria-labelledby="node-editor-title">
    <form id="node-form">
      <div class="nodes-dialog-heading"><div><div class="nodes-eyebrow">节点配置</div><h2 id="node-editor-title">添加服务器</h2></div><button class="nodes-icon-button" type="button" data-close="node-editor" aria-label="关闭">×</button></div>
      <p class="nodes-dialog-intro" id="node-editor-intro">初始化接通后，添加服务器将自动验证登录、配置 SSH 密钥并接入 Beszel；完成后再配置 Worker。</p>
      <div class="nodes-form-grid">
        <label class="nodes-field wide">服务器名称 <input name="name" required maxlength="80" placeholder="例如：增量采集节点 01" autocomplete="off"></label>
        <label class="nodes-field">节点类型<select name="kind"><option value="execution">执行节点</option><option value="center">中心节点</option></select></label>
        <label class="nodes-field">SSH 用户名<input name="username" required maxlength="64" placeholder="ubuntu" autocomplete="off"></label>
        <label class="nodes-field">IP 地址 / 主机名<input name="host" required maxlength="253" placeholder="服务器 IP 或主机名" autocomplete="off" spellcheck="false"></label>
        <label class="nodes-field">SSH 端口<input name="port" type="number" required min="1" max="65535" value="22"></label>
        <div class="nodes-field wide" id="node-password-section"><label for="node-password">服务器密码 <span class="nodes-badge">初始化待接入</span></label><input id="node-password" type="password" autocomplete="new-password" disabled placeholder="自动初始化接通后填写" aria-describedby="node-password-help"><small id="node-password-help">首次接入使用密码，后续通过专用 SSH 密钥连接。SSH 别名由系统自动配置，无需手工填写。</small></div>
        <label class="nodes-field wide">备注 <span class="nodes-optional">可选</span><textarea name="notes" rows="3" maxlength="500" placeholder="用途、机房或其他需要记录的信息"></textarea></label>
      </div>
      <p id="node-form-error" class="nodes-form-error" role="alert" hidden></p>
      <p class="nodes-footnote" id="node-register-help">目前只能保存基本信息，密码不会收集或提交。</p>
      <div class="nodes-dialog-footer"><button type="button" class="nodes-button" data-close="node-editor">取消</button><button type="submit" class="nodes-button" id="node-save">仅保存信息</button><button type="button" class="nodes-button primary" id="node-add-initialize" disabled title="自动初始化功能尚未接入">添加并初始化</button></div>
    </form>
  </dialog>

  <dialog id="node-detail" class="nodes-dialog nodes-detail-dialog" aria-labelledby="node-detail-title">
    <div class="nodes-dialog-heading"><div><div class="nodes-eyebrow">服务器详情</div><h2 id="node-detail-title"></h2></div><button class="nodes-icon-button" type="button" data-close="node-detail" aria-label="关闭">×</button></div>
    <div id="node-detail-content"></div>
    <div class="nodes-dialog-footer"><button type="button" class="nodes-button danger" id="node-detail-delete">删除服务器</button><button type="button" class="nodes-button" id="node-detail-edit">编辑信息</button><button type="button" class="nodes-button primary" id="node-detail-initialize">初始化</button></div>
  </dialog>

  <dialog id="node-initialize" class="nodes-dialog" aria-labelledby="node-initialize-title">
    <div class="nodes-dialog-heading"><div><div class="nodes-eyebrow" id="node-initialize-name"></div><h2 id="node-initialize-title">初始化服务器</h2></div><button class="nodes-icon-button" type="button" data-close="node-initialize" aria-label="关闭">×</button></div>
    <p class="nodes-dialog-intro">验证登录后自动配置 SSH 密钥与 Beszel 监控。以下步骤全部完成后，才开放 Worker 配置。</p>
    <div class="nodes-auth-choice"><strong>登录凭据</strong><p id="node-initialize-auth"></p><label class="nodes-field" id="node-initialize-password-field">首次登录密码<input type="password" id="node-initialize-password" disabled autocomplete="new-password" placeholder="初始化功能接通后填写"></label></div>
    <ol class="nodes-initialization-steps"><li><b>1</b><div><strong>验证 SSH 与 sudo 权限</strong><small>确认服务器可连接，并具有初始化所需权限</small></div><span>未执行</span></li><li><b>2</b><div><strong>配置专用 SSH 密钥</strong><small>验证密钥登录，供后续部署使用</small></div><span>未执行</span></li><li><b>3</b><div><strong>安装并注册 Beszel Agent</strong><small>连接中心监控服务</small></div><span>未执行</span></li><li><b>4</b><div><strong>等待首份监控数据</strong><small>确认 CPU、内存、磁盘与网络数据可用</small></div><span>未执行</span></li></ol>
    <p class="nodes-footnote">自动初始化尚未接通，当前没有任务在执行。接通后，中途失败会显示具体步骤和重试入口。</p>
    <div class="nodes-dialog-footer"><button type="button" class="nodes-button" data-close="node-initialize">关闭</button><button type="button" class="nodes-button primary" disabled>开始初始化 · 待接入</button></div>
  </dialog>

  <dialog id="node-delete" class="nodes-dialog" aria-labelledby="node-delete-title">
    <div class="nodes-dialog-heading"><div><div class="nodes-eyebrow" id="node-delete-name"></div><h2 id="node-delete-title">删除服务器</h2></div><button class="nodes-icon-button" type="button" data-close="node-delete" aria-label="关闭">×</button></div>
    <p class="nodes-dialog-intro">仅登记、尚未通过系统初始化或部署的执行节点，可以直接删除登记。已经开始初始化或部署的节点，需要先核实运行与任务状态。</p>
    <div class="nodes-deletion-reason" id="node-delete-reason" role="status"></div>
    <ul class="nodes-deletion-checks" id="node-delete-checks" hidden><li><span>节点已停止接收新任务</span><b>无法核实</b></li><li><span>没有运行中的 Worker（包括空闲实例）</span><b>无法核实</b></li><li><span>没有执行、排队或等待重试的已分配任务</span><b>无法核实</b></li><li><span>没有进行中的初始化或部署</span><b>无法核实</b></li></ul>
    <p class="nodes-footnote">删除仅移除本页的服务器登记和保存的 Worker 计划，不会卸载远程服务、删除 SSH 密钥或清除已采集的业务数据。已初始化节点的运行校验尚未接入，暂不开放删除。</p>
    <div class="nodes-dialog-footer"><button type="button" class="nodes-button" data-close="node-delete">返回</button><button type="button" id="node-delete-confirm" class="nodes-button danger" disabled>正在检查…</button></div>
  </dialog>
  <script type="module" src="/assets/server-nodes.js"></script>`;
}
