const escape = (value) =>
  String(value ?? "").replace(
    /[&<>"']/g,
    (c) =>
      ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" })[
        c
      ],
  );
export function migrationBatchPanel({ pendingCount = 0 } = {}) {
  return `<style>
.migration-console{margin:20px 0;padding:24px;border:1px solid #dce3ec;border-radius:16px;background:var(--panel,#fff);box-shadow:0 4px 20px #152c4c06}
.migration-console-head,.migration-controls,.migration-progress-head{display:flex;align-items:center;justify-content:space-between;gap:16px;flex-wrap:wrap}
.migration-console h2{margin:0 0 6px;font-size:20px}.migration-console p{margin:6px 0;color:#64748b}.migration-controls{justify-content:flex-start;margin:18px 0}.migration-controls select{min-width:170px;padding:10px;border:1px solid #cbd5e1;border-radius:8px}.migration-controls button:disabled{opacity:.5;cursor:wait}
.migration-progress-track{height:10px;border-radius:8px;background:#e9eef5;overflow:hidden;margin:14px 0}.migration-progress-fill{height:100%;background:var(--primary,#127849);transition:width .4s}.migration-metrics{display:grid;grid-template-columns:repeat(6,minmax(85px,1fr));gap:12px;margin:20px 0}.migration-metric{background:var(--soft,#f7faf8);border-radius:10px;padding:13px}.migration-metric strong{display:block;font-size:23px;font-variant-numeric:tabular-nums}.migration-metric span{color:#64748b;font-size:12px}.migration-status{background:#e9f5ee;color:#127849;padding:6px 12px;border-radius:20px;font-weight:600}.migration-danger{color:#b42318;border-color:#f0b4ae}.migration-history{margin-top:20px;border-top:1px solid #e2e8f0;padding-top:14px}.migration-history-row{display:flex;justify-content:space-between;gap:12px;padding:9px 0;font-size:13px}.migration-message{min-height:22px;color:#b42318}.migration-confirm{border:0;border-radius:14px;padding:26px;max-width:430px;box-shadow:0 20px 80px #0003}.migration-confirm::backdrop{background:#0f172a66}
@media(max-width:700px){.migration-console{padding:16px}.migration-metrics{grid-template-columns:repeat(3,1fr)}.migration-controls{gap:8px}}
</style>
<section class="migration-console" id="migration-console" aria-label="批量迁移控制台">
 <div class="migration-console-head"><div><h2>批量迁移</h2><p>选择频道数量，后台按容量持续采集。暂停后可从原批次继续。</p></div><span class="migration-status" id="migration-status">读取批次状态…</span></div>
 <form class="migration-controls" id="migration-start-form" method="post" action="/migration-channels/batch-migrate">
  <label for="batch-migration-selection">迁移数量</label><select id="batch-migration-selection" name="selection">${[100, 200, 500, 1000, 2000].map((n) => `<option value="${n}">${n.toLocaleString()} 个频道</option>`).join("")}<option value="all">All · 全部待迁移</option></select>
  <button type="submit" class="btn btn-primary" id="migration-start" disabled>开始迁移</button><span class="note">当前列表待迁移约 ${escape(Number(pendingCount).toLocaleString())} 个；实际范围在启动时固定</span>
 </form>
 <div id="migration-batch-progress" hidden></div>
 <div class="migration-controls" id="migration-batch-actions"></div>
 <p class="migration-message" id="migration-batch-message" role="status" aria-live="polite"></p>
 <details class="migration-history"><summary>最近批次</summary><div id="migration-batch-history">正在读取…</div></details>
</section>
<dialog class="migration-confirm" id="migration-stop-confirm"><h3>结束本批迁移？</h3><p id="migration-stop-explanation"></p><p>已完成的数据会保留。结束后不能恢复此批次，但剩余频道可以重新选择迁移。</p><div class="migration-controls"><button class="btn" id="migration-stop-cancel">返回</button><button class="btn migration-danger" id="migration-stop-submit">结束本批</button></div></dialog>
<script>(${migrationBatchClient.toString()})();</script>`;
}
function migrationBatchClient() {
  const byId = (id) => document.getElementById(id),
    fmt = (n) => Number(n || 0).toLocaleString(),
    esc = (v) =>
      String(v ?? "").replace(
        /[&<>"']/g,
        (c) =>
          ({
            "&": "&amp;",
            "<": "&lt;",
            ">": "&gt;",
            '"': "&quot;",
            "'": "&#39;",
          })[c],
      );
  const labels = {
    preparing: "准备中",
    running: "运行中",
    pausing: "暂停中",
    paused: "已暂停",
    stopping: "结束中",
    ended: "已结束",
    completed: "已完成",
  };
  let active = null,
    busy = false,
    loading = false,
    stopTarget = null;
  const message = (t) => {
    byId("migration-batch-message").textContent = t;
  };
  const terminal = (b) =>
    ["success", "dormant", "rejected", "failed", "existing"].reduce(
      (n, k) => n + Number(b.counts[k] || 0),
      0,
    );
  async function request(url, body) {
    const response = await fetch(url, {
      method: body ? "POST" : "GET",
      headers: body ? { "content-type": "application/json" } : {},
      body: body ? JSON.stringify(body) : undefined,
      cache: "no-store",
    });
    const data = await response.json();
    if (!response.ok || data.ok === false)
      throw Error(data.error || "请求失败");
    return data;
  }
  function render(data) {
    active = data.active;
    const b = active;
    byId("migration-status").textContent = b ? labels[b.status] : "当前无运行批次，可以开始迁移";
    byId("migration-start").disabled = busy || !!active;
    byId("batch-migration-selection").disabled = busy || !!active;
    const box = byId("migration-batch-progress");
    box.hidden = !b;
    if (!b) box.replaceChildren();
    if (b) {
      const c = b.counts,
        done = terminal(b),
        total = Number(b.total_count),
        percent = total ? Math.min(100, (done / total) * 100) : 0;
      const metrics = [
        ["success", "成功"],
        ["dormant", "休眠 / 部分完成"],
        ["rejected", "拒绝"],
        ["failed", "最终失败"],
        ["started", "执行 / 恢复中"],
        ["pending", "尚未开始"],
      ];
      const rate =
        Number(b.active_seconds) > 60 && done >= 10
          ? (done / Number(b.active_seconds)) * 3600
          : null;
      const eta =
        rate && active && b.status === "running"
          ? ` · 预计剩余 ${(total - done) / rate >= 24 ? ((total - done) / rate / 24).toFixed(1) + " 天" : (total - done) / rate >= 1 ? ((total - done) / rate).toFixed(1) + " 小时" : Math.ceil(((total - done) / rate) * 60) + " 分钟"}`
          : "";
      const rolling = [15, 60].map(minutes => {
        const value = b.rolling_rates?.[`minutes_${minutes}`];
        return value ? `最近 ${minutes} 分钟结算 ${fmt(value.completed)} 个 · ${fmt(Math.round(value.per_hour))} 个/有效小时${value.fetch_completed != null ? `；抓取完成 ${fmt(value.fetch_completed)} 个` : ""}` : `最近 ${minutes} 分钟：样本积累中`;
      }).join("；");
      const freshness = Number(b.statistics_age_seconds) > 90 ? ` · 统计延迟 ${Math.floor(b.statistics_age_seconds)} 秒` : "";
      box.innerHTML = `<div class="migration-progress-head"><strong>${b.frozen_at || ["ended", "completed"].includes(b.status) ? `已处理 ${fmt(done)} / ${fmt(total)} 个频道` : "正在固定本批频道清单…"}</strong><span>${percent.toFixed(1)}%</span></div><div class="migration-progress-track" role="progressbar" aria-label="批次进度" aria-valuenow="${percent.toFixed(1)}" aria-valuemin="0" aria-valuemax="100"><div class="migration-progress-fill" style="width:${percent}%"></div></div><div class="migration-metrics">${metrics.map(([k, label]) => `<div class="migration-metric"><strong>${fmt(c[k])}</strong><span>${label}</span></div>`).join("")}</div><p>${Number(b.publishing_count) ? `待发布 ${fmt(b.publishing_count)} 项 · ` : ""}有效运行 ${Math.floor(Number(b.active_seconds) / 60)} 分钟${rate ? ` · 平均每小时 ${fmt(Math.round(rate))} 个` : ""}${eta}</p><p>${b.status === "pausing" ? `等待 ${fmt(c.started)} 个已启动频道及发布任务收尾，随后暂停。` : b.status === "stopping" ? `已停止启动新频道，等待当前频道及发布任务收尾。` : b.status === "paused" ? "现场已保留，点击继续迁移即可恢复。" : b.status === "ended" ? `${fmt(c.released)} 个未开始频道保留在待迁移列表。` : "完成数包含成功、休眠、拒绝和最终失败；重试不重复计数。"}</p><p>${rolling}${freshness}。统计每 30 秒更新；结算数按频道去重，等待恢复和重试不重复计数。</p><p class="migration-message">${b.control_error ? `源数据读取失败，已暂停派发：${esc(b.control_error)}` : ""}</p><div class="note">批次 ${esc(b.batch_id)}</div>`;
    }
    const actions = byId("migration-batch-actions");
    actions.replaceChildren();
    if (active) {
      for (const [action, label] of ["running", "preparing"].includes(
        active.status,
      )
        ? [
            ["pause", "暂停"],
            ["stop", "结束本批"],
          ]
        : ["paused", "pausing"].includes(active.status)
          ? [
              ["resume", "继续迁移"],
              ["stop", "结束本批"],
            ]
          : []) {
        const btn = document.createElement("button");
        btn.className =
          "btn " +
          (action === "resume"
            ? "btn-primary"
            : action === "stop"
              ? "migration-danger"
              : "");
        btn.textContent = label;
        btn.disabled = busy;
        btn.onclick = () =>
          action === "stop" ? confirmStop() : control(action);
        actions.append(btn);
      }
    }
    byId("migration-batch-history").innerHTML = data.batches.length
      ? data.batches
          .map(
            (h) =>
              `<div class="migration-history-row"><span>${esc(new Date(h.created_at).toLocaleString())}</span><span>${esc(labels[h.status])} · 已处理 ${fmt(terminal(h))} / ${fmt(h.total_count)}${h.status === "ended" ? ` · ${fmt(h.counts.released)} 个保留待迁移` : ""}</span></div>`,
          )
          .join("")
      : "暂无批次记录";
  }
  async function refresh() {
    if (loading || busy) return;
    loading = true;
    try {
      render(await request("/migration-channels/batches/progress"));
    } catch (e) {
      message("状态更新失败：" + e.message);
    } finally {
      loading = false;
    }
  }
  async function control(action, target = active) {
    if (!target || busy) return;
    const b = target;
    busy = true;
    message("正在处理…");
    try {
      await request(
        `/migration-channels/batches/${encodeURIComponent(b.batch_id)}/${action}`,
        { version: b.version },
      );
      message("");
    } catch (e) {
      message(e.message);
    } finally {
      busy = false;
      await refresh();
    }
  }
  function confirmStop() {
    if (!active || busy) return;
    stopTarget = { ...active };
    const c = active.counts;
    byId("migration-stop-explanation").textContent =
      `已启动的 ${fmt(c.started)} 个频道会继续完成；尚未开始的 ${fmt(c.pending)} 个频道保留在待迁移列表。`;
    byId("migration-stop-confirm").showModal();
  }
  byId("migration-stop-cancel").onclick = () =>
    byId("migration-stop-confirm").close();
  byId("migration-stop-submit").onclick = () => {
    byId("migration-stop-confirm").close();
    control("stop", stopTarget);
  };
  byId("migration-start-form").onsubmit = async (e) => {
    e.preventDefault();
    if (busy || active) return;
    busy = true;
    byId("migration-start").disabled = true;
    message("正在创建批次…");
    try {
      await request("/migration-channels/batches/start", {
        selection: byId("batch-migration-selection").value,
      });
      message("");
    } catch (error) {
      message(error.message);
    } finally {
      busy = false;
      await refresh();
    }
  };
  refresh();
  setInterval(() => {
    if (!document.hidden) refresh();
  }, 4000);
  document.addEventListener("visibilitychange", () => {
    if (!document.hidden) refresh();
  });
}
