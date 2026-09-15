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
      <div class="nodes-stat"><span>已登记服务器</span><strong id="nodes-total">—</strong><small>中心与执行节点</small></div>
      <div class="nodes-stat"><span>已部署增量 Worker</span><strong id="nodes-planned">—</strong><small>实际部署数量</small></div>
      <div class="nodes-stat"><span>允许接任务</span><strong id="nodes-allowed">—</strong><small>各服务器设置的合计</small></div>
      <div class="nodes-stat"><span>正在执行 / 收尾</span><strong id="nodes-active">—</strong><small>已领取任务的 Worker</small></div>
    </div>
    <ol class="nodes-journey" aria-label="服务器接入流程"><li class="current"><b>01</b><div><strong>添加并初始化</strong><small>验证 SSH · 配置密钥 · 接入 Beszel</small></div></li><li><b>02</b><div><strong>准备运行环境</strong><small>Docker · Compose · 运行目录</small></div></li><li><b>03</b><div><strong>部署与接单</strong><small>部署实例 · 调整接单数量</small></div></li></ol>
    <div class="nodes-stage"><span class="nodes-stage-icon" aria-hidden="true">i</span><div><strong>按步骤完成服务器接入</strong><p>添加并初始化后，在服务器卡片点击“准备运行环境”，查看各步骤进度。环境就绪后，在“管理 Worker”中部署实例并调整接单数量。</p></div></div>
    <div class="nodes-list-heading"><h2>服务器列表 <span id="nodes-count"></span></h2><div class="nodes-filters"><label class="nodes-search"><span class="nodes-sr-only">搜索服务器名称或地址</span><input id="nodes-search" type="search" placeholder="搜索名称或 IP 地址" autocomplete="off"></label><label><span class="nodes-sr-only">筛选节点类型</span><select id="nodes-kind"><option value="all">全部类型</option><option value="center">中心节点</option><option value="execution">执行节点</option></select></label></div></div>
    <div id="nodes-list" class="nodes-list" aria-live="polite"><div class="nodes-empty"><span class="nodes-loading">正在读取服务器列表…</span></div></div>
    <noscript><p class="nodes-stage">请启用 JavaScript 以加载和管理服务器。</p></noscript>
  </section>

  <dialog id="node-editor" class="nodes-dialog" aria-labelledby="node-editor-title">
    <form id="node-form">
      <div class="nodes-dialog-heading"><div><div class="nodes-eyebrow">节点配置</div><h2 id="node-editor-title">添加服务器</h2></div><button class="nodes-icon-button" type="button" data-close="node-editor" aria-label="关闭">×</button></div>
      <p class="nodes-dialog-intro" id="node-editor-intro">填写登录信息，系统将验证 SSH、配置专用密钥并接入 Beszel。初始化成功后再配置 Worker。</p>
      <div class="nodes-form-grid">
        <label class="nodes-field wide">服务器名称 <input name="name" required maxlength="80" placeholder="例如：增量采集节点 01" autocomplete="off"></label>
        <label class="nodes-field">节点类型<select name="kind"><option value="execution">执行节点</option><option value="center">中心节点</option></select></label>
        <label class="nodes-field">SSH 用户名<input name="username" required maxlength="64" placeholder="ubuntu" autocomplete="off"></label>
        <label class="nodes-field">IP 地址 / 主机名<input name="host" required maxlength="253" placeholder="服务器 IP 或主机名" autocomplete="off" spellcheck="false"></label>
        <label class="nodes-field">SSH 端口<input name="port" type="number" required min="1" max="65535" value="22"></label>
        <div class="nodes-field wide" id="node-password-section"><label for="node-password">服务器密码 <span class="nodes-optional">首次初始化使用</span></label><input id="node-password" type="password" autocomplete="new-password" disabled placeholder="首次登录密码；已有系统密钥时可留空" aria-describedby="node-password-help"><small id="node-password-help">首次接入使用密码，后续通过专用 SSH 密钥连接。SSH 别名由系统自动配置，无需手工填写。</small></div>
        <label class="nodes-field wide">备注 <span class="nodes-optional">可选</span><textarea name="notes" rows="3" maxlength="500" placeholder="用途、机房或其他需要记录的信息"></textarea></label>
      </div>
      <p id="node-form-error" class="nodes-form-error" role="alert" hidden></p>
      <p class="nodes-footnote" id="node-register-help">密码仅在点击“添加并初始化”时用于本次连接，不保存到节点登记中。</p>
      <div class="nodes-dialog-footer"><button type="button" class="nodes-button" data-close="node-editor">取消</button><button type="submit" class="nodes-button" id="node-save">仅保存信息</button><button type="submit" class="nodes-button primary" id="node-add-initialize" disabled title="正在检查初始化服务">添加并初始化</button></div>
    </form>
  </dialog>

  <dialog id="node-detail" class="nodes-dialog nodes-detail-dialog" aria-labelledby="node-detail-title">
    <div class="nodes-dialog-heading"><div><div class="nodes-eyebrow">服务器详情</div><h2 id="node-detail-title"></h2></div><button class="nodes-icon-button" type="button" data-close="node-detail" aria-label="关闭">×</button></div>
    <div id="node-detail-content"></div>
    <div class="nodes-dialog-footer"><button type="button" class="nodes-button danger" id="node-detail-delete">删除服务器</button><button type="button" class="nodes-button" id="node-detail-edit">编辑信息</button><button type="button" class="nodes-button primary" id="node-detail-initialize">初始化</button></div>
  </dialog>

  <dialog id="node-initialize" class="nodes-dialog" aria-labelledby="node-initialize-title">
    <div class="nodes-dialog-heading"><div><div class="nodes-eyebrow" id="node-initialize-name"></div><h2 id="node-initialize-title">初始化服务器</h2></div><button class="nodes-icon-button" type="button" data-close="node-initialize" aria-label="关闭">×</button></div>
    <p class="nodes-dialog-intro">验证登录后自动配置 SSH 密钥与 Beszel 监控。以下步骤全部完成后，开放 Worker 配置。密码不会长期保存。</p>
    <div class="nodes-auth-choice" id="node-initialize-credentials"><strong>登录凭据</strong><p id="node-initialize-auth"></p><label class="nodes-field" id="node-initialize-password-field">首次登录密码<input type="password" id="node-initialize-password" disabled autocomplete="new-password" placeholder="首次登录密码；已配置系统密钥时可留空"></label></div>
    <ol class="nodes-initialization-steps" id="node-initialize-steps"><li><b>1</b><div><strong>验证 SSH 与 sudo 权限</strong><small>确认服务器可连接，并具有初始化所需权限</small></div><span>未执行</span></li><li><b>2</b><div><strong>配置专用 SSH 密钥</strong><small>验证密钥登录，供后续部署使用</small></div><span>未执行</span></li><li><b>3</b><div><strong>安装并注册 Beszel Agent</strong><small>连接中心监控服务</small></div><span>未执行</span></li><li><b>4</b><div><strong>等待首份监控数据</strong><small>确认 CPU、内存、磁盘与网络数据可用</small></div><span>未执行</span></li></ol>
    <p class="nodes-footnote" id="node-initialize-status" role="status"></p>
    <div class="nodes-dialog-footer"><button type="button" class="nodes-button" data-close="node-initialize">关闭</button><button type="button" class="nodes-button primary" id="node-initialize-start" disabled>开始初始化</button></div>
  </dialog>

  <dialog id="node-delete" class="nodes-dialog" aria-labelledby="node-delete-title">
    <div class="nodes-dialog-heading"><div><div class="nodes-eyebrow" id="node-delete-name"></div><h2 id="node-delete-title">删除服务器</h2></div><button class="nodes-icon-button" type="button" data-close="node-delete" aria-label="关闭">×</button></div>
    <p class="nodes-dialog-intro">未部署 Worker 的执行节点可以删除。已初始化的节点会先检查中心任务和远程环境，检查通过后清理本系统监控及节点登记。</p>
    <div class="nodes-deletion-reason" id="node-delete-reason" role="status"></div>
    <ul class="nodes-deletion-checks" id="node-delete-checks" hidden><li><span>节点已停止接收新任务</span><b>无法核实</b></li><li><span>没有运行中的 Worker（包括空闲实例）</span><b>无法核实</b></li><li><span>没有执行、排队或等待重试的已分配任务</span><b>无法核实</b></li><li><span>没有进行中的初始化或部署</span><b>无法核实</b></li></ul>
    <p class="nodes-footnote">保留 Docker、SSH 接入和采集数据。已有 Worker 部署记录或远程容器的节点仍需先完成退役检查。删除可能需要约 1–2 分钟。</p>
    <label class="nodes-field" id="node-delete-password-field" hidden>服务器密码（仅 sudo 需要时填写）<input id="node-delete-password" type="password" autocomplete="new-password"><small>仅用于本次操作，不保存。</small></label>
    <div class="nodes-dialog-footer"><button type="button" class="nodes-button" data-close="node-delete">返回</button><button type="button" id="node-delete-confirm" class="nodes-button danger" disabled>正在检查…</button></div>
  </dialog>
  <dialog id="node-runtime" class="nodes-dialog" aria-labelledby="node-runtime-title">
    <div class="nodes-dialog-heading"><div><div class="nodes-eyebrow" id="node-runtime-name"></div><h2 id="node-runtime-title">准备 Worker 运行环境</h2></div><button type="button" class="nodes-icon-button" data-close="node-runtime" aria-label="关闭">×</button></div>
    <p class="nodes-dialog-intro">自动安装或复用 Docker 与 Compose，准备节点目录并检查环境。完成后再部署 Worker。</p>
    <label class="nodes-field" id="node-runtime-password-field">sudo 密码（按需填写）<input type="password" id="node-runtime-password" autocomplete="new-password" placeholder="SSH 使用已配置密钥；sudo 免密时留空"></label>
    <ol class="nodes-initialization-steps" id="node-runtime-steps"><li><b>1</b><div><strong>连接服务器</strong><small>验证专用密钥与 sudo 权限</small></div><span>待执行</span></li><li><b>2</b><div><strong>检查系统</strong><small>核实系统版本、架构与节点身份</small></div><span>待执行</span></li><li><b>3</b><div><strong>准备 Docker 与 Compose</strong><small>已有环境直接复用，首次安装可能需要几分钟</small></div><span>待执行</span></li><li><b>4</b><div><strong>准备运行目录</strong><small>建立配置、凭据与采集结果暂存目录</small></div><span>待执行</span></li><li><b>5</b><div><strong>检查环境可用性</strong><small>检查容器引擎、Compose 与目录权限</small></div><span>待执行</span></li></ol>
    <p class="nodes-footnote" id="node-runtime-status" role="status"></p>
    <div class="nodes-dialog-footer"><button type="button" class="nodes-button" data-close="node-runtime">关闭</button><button type="button" class="nodes-button primary" id="node-runtime-start">开始准备</button></div>
  </dialog>
  <dialog id="node-worker-manager" class="nodes-dialog nodes-worker-manager" aria-labelledby="node-worker-manager-title">
    <div class="nodes-dialog-heading"><div><div class="nodes-eyebrow" id="worker-manager-name"></div><h2 id="node-worker-manager-title">管理 Worker</h2></div><button type="button" class="nodes-icon-button" data-close="node-worker-manager" aria-label="关闭">×</button></div>
    <p class="nodes-dialog-intro">先选择 Worker 类型，再填写本次新增数量；下方可以单独调整允许接任务数量。</p>
    <div id="worker-manager-summary" class="nodes-manager-summary" aria-live="polite"></div>
    <section class="nodes-manager-section" id="worker-deployment-section">
      <form id="worker-deployment-form">
        <div class="nodes-section-heading"><h3>新增 Worker</h3><span id="worker-installed-label"></span></div>
        <label class="nodes-field">Worker 类型<select id="worker-deployment-role"><option value="incremental">增量 Worker</option><option value="query" disabled>Query Worker（远程部署暂未开放）</option></select></label>
        <div class="nodes-manager-control"><label class="nodes-field" for="worker-deployment-count">本次新增几个<input id="worker-deployment-count" type="number" min="1" step="1" required></label><button type="submit" class="nodes-button primary" id="worker-deployment-save">部署 Worker</button></div>
        <p id="worker-deployment-impact" class="nodes-manager-help" role="status"></p>
        <p id="worker-deployment-memory" class="nodes-manager-help"></p>
        <label class="nodes-check"><input type="checkbox" id="worker-sync-intake">新增成功后，将允许接任务数量同步为新增后的总数</label>
        <p class="nodes-manager-help">关闭窗口后部署仍会继续。若期间手动修改了接单设置，将保留较新的设置。</p>
        <label class="nodes-field" id="worker-deployment-password-field">sudo 密码（按需填写）<input id="worker-deployment-password" type="password" autocomplete="new-password" placeholder="已配置免密 sudo 时留空"></label>
        <p id="worker-deployment-error" class="nodes-form-error" role="alert" hidden></p>
      </form>
    </section>
    <p id="worker-center-note" class="nodes-manager-help" hidden>中心现有增量 Worker 的部署数量暂不在此调整；可以直接设置允许接任务数量。</p>
    <section class="nodes-manager-section">
      <form id="worker-intake-form">
        <div class="nodes-section-heading"><h3>允许接任务数量</h3><span id="worker-allowed-label"></span></div>
        <div class="nodes-manager-control"><label class="nodes-field" for="worker-intake-count">最多同时参与采集的 Worker<input id="worker-intake-count" type="number" min="0" step="1" required></label><button type="submit" class="nodes-button" id="worker-intake-save">保存接单数量</button></div>
        <p id="worker-intake-impact" class="nodes-manager-help">设为 0 暂停接单；调小后，多出的 Worker 完成当前频道再待命。</p>
        <p id="worker-intake-result" class="nodes-manager-help" role="status"></p>
        <p id="worker-intake-error" class="nodes-form-error" role="alert" hidden></p>
      </form>
    </section>
    <section id="worker-removal-section" class="nodes-manager-section">
      <h3>已部署的 Worker</h3>
      <p class="nodes-manager-help">只能删除空闲 Worker。执行中、恢复中或状态未知时不能删除；删除前会再次检查并停止接单。已采集的数据会保留。</p>
      <p id="worker-removal-status" class="nodes-manager-help" role="status"></p>
      <div id="worker-removal-list" class="nodes-worker-list"></div>
      <form id="worker-removal-form" class="nodes-worker-confirm" hidden>
        <strong id="worker-removal-title"></strong>
        <p class="nodes-manager-help">确认后移除此 Worker 的容器，释放运行资源。需要时可以重新新增 Worker。</p>
        <label class="nodes-field">sudo 密码（按需填写）<input id="worker-removal-password" type="password" autocomplete="new-password" placeholder="免密 sudo 时留空"></label>
        <div class="nodes-manager-control"><button type="submit" id="worker-removal-confirm" class="nodes-button danger">确认删除空闲 Worker</button><button type="button" class="nodes-button" data-cancel-worker-removal>取消</button></div>
      </form>
      <p id="worker-removal-error" class="nodes-form-error" role="alert" hidden></p>
    </section>
    <details id="worker-deployment-history" class="nodes-deployment-history">
      <summary id="worker-deployment-history-title">最近部署记录</summary>
      <p id="worker-deployment-status" class="nodes-manager-help" role="status"></p>
      <ol id="worker-deployment-steps" class="nodes-initialization-steps"></ol>
    </details>
    <div class="nodes-dialog-footer"><button type="button" class="nodes-button" data-close="node-worker-manager">关闭</button></div>
  </dialog>
  <script type="module" src="/assets/server-nodes.js"></script>`;
}
