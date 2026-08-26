import express from "express";
import morgan from "morgan";
import { nanoid } from "nanoid";
import { createBullBoard } from "@bull-board/api";
import { BullMQAdapter } from "@bull-board/api/bullMQAdapter";
import { ExpressAdapter } from "@bull-board/express";
import {
  createAgentTemplateDraft,
  ensureDefaultAgentConfig,
  getActiveAgentConfig,
  publishAgentTemplate,
  updateDefaultAgentConfig,
} from "./agentConfig.js";
import { closeDb, ensureSchema, pool, query, withTransaction } from "./db.js";
import {
  dispatchManualMigrationBatch,
  dispatchManualMigrationChannel,
} from "./manualMigrationDispatch.js";
import { assertMigrationChannelInventorySchema } from "./migrationInventorySchema.js";
import {
  migrationInventoryForceSyncEnabled,
  migrationInventorySyncConfigured,
  syncMigrationChannelInventory,
} from "./migrationInventorySync.js";
import { closeMigrationSourcePool } from "./migrationSource.js";
import {
  ManagedJobOutboxDispatcher,
  PostgresManagedJobDispatchRepository,
} from "./managedJobDispatchOutbox.js";
import {
  ManagedJobIntentStore,
  PostgresManagedJobIntentRepository,
} from "./managedJobIntentStore.js";
import { dispatchManagedQueryQualityBatch } from "./managedJobApi.js";
import { loadIdentityPolicyCatalog } from "./identityPolicyCatalog.js";
import { scoreQueryBatch } from "./queryQuality.js";
import { closeQueues, createQueues, getQueueStats, queueNames, queuesByRole, safeJobId } from "./queues.js";

const port = Number(process.env.PORT || 3000);
const basePath = process.env.BULL_BOARD_BASE_PATH || "/queues";
const queues = createQueues();
const managedIdentityPolicies = [...loadIdentityPolicyCatalog().policies.values()];
const managedJobIntentStore = new ManagedJobIntentStore({
  repository: new PostgresManagedJobIntentRepository({ withTransaction }),
  policies: managedIdentityPolicies,
});
const managedJobOutboxDispatcher = new ManagedJobOutboxDispatcher({
  repository: new PostgresManagedJobDispatchRepository({ withTransaction }),
  queues,
});
const stronglyTypedManagedQueues = new Set([
  queuesByRole.queryQuality,
  queuesByRole.discoverPage,
  queuesByRole.channelCrawl,
  queuesByRole.channelIncremental,
  queuesByRole.contentEnrich,
]);
await ensureSchema();
if (migrationInventorySyncConfigured()) {
  await assertMigrationChannelInventorySchema({ query: pool.query.bind(pool) });
  const inventorySync = await syncMigrationChannelInventory({
    targetPool: pool,
    force: migrationInventoryForceSyncEnabled(),
    onProgress: ({ source_id: sourceId, eligible_count: eligibleCount }) => {
      console.log(JSON.stringify({
        event: "migration_channel_inventory_sync_progress",
        source_id: sourceId,
        eligible_count: eligibleCount,
      }));
    },
  });
  console.log(JSON.stringify({
    event: "migration_channel_inventory_sync_ready",
    source_id: inventorySync.source_id,
    eligible_count: inventorySync.eligible_count,
    skipped: inventorySync.skipped,
  }));
}
await ensureDefaultAgentConfig();

const app = express();
app.disable("x-powered-by");
app.use(express.json({ limit: "2mb" }));
app.use(morgan("combined"));
app.use((req, res, next) => {
  const controlled = String(process.env.CONTROLLED_MIGRATION_ONLY || "").toLowerCase() === "true";
  const readMethod = req.method === "GET" || req.method === "HEAD" || req.method === "OPTIONS";
  if (!controlled || readMethod || req.path.startsWith("/api/migration/channels")) return next();
  return res.status(423).json({
    ok: false,
    code: "controlled_migration_only",
    error: "non-Migration writes are disabled during the controlled canary",
  });
});

const asyncRoute = (fn) => (req, res, next) => Promise.resolve(fn(req, res, next)).catch(next);

const serverAdapter = new ExpressAdapter();
serverAdapter.setBasePath(basePath);

createBullBoard({
  queues: queueNames.map((name) => new BullMQAdapter(queues[name])),
  serverAdapter,
});

app.get("/", (_req, res) => {
  res.redirect(basePath);
});

app.get("/settings", (_req, res) => {
  res.redirect("/settings/agent");
});

function agentSettingsPage() {
  return `<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>BullMQ Crawler Agent Settings</title>
  <style>
    :root { color-scheme: light; --border:#d7dde8; --muted:#667085; --ink:#101828; --bg:#f7f8fb; --accent:#1f6feb; }
    * { box-sizing: border-box; }
    body { margin:0; font:14px/1.45 system-ui,-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif; color:var(--ink); background:var(--bg); }
    header { background:#fff; border-bottom:1px solid var(--border); padding:14px 24px; display:flex; align-items:center; gap:22px; }
    header h1 { font-size:18px; margin:0; font-weight:650; }
    nav { display:flex; gap:12px; }
    nav a { color:#344054; text-decoration:none; padding:6px 8px; border-radius:6px; }
    nav a[aria-current="page"] { color:#fff; background:var(--accent); }
    main { max-width:1280px; margin:0 auto; padding:22px 24px 48px; }
    section { background:#fff; border:1px solid var(--border); border-radius:8px; margin-bottom:18px; }
    .section-head { padding:14px 16px; border-bottom:1px solid var(--border); display:flex; align-items:center; justify-content:space-between; gap:16px; }
    .section-head h2 { margin:0; font-size:15px; }
    .section-body { padding:16px; }
    .grid { display:grid; grid-template-columns:repeat(4, minmax(0, 1fr)); gap:12px; }
    label { display:block; font-weight:600; font-size:12px; color:#344054; margin-bottom:5px; }
    input, textarea { width:100%; border:1px solid var(--border); border-radius:6px; padding:9px 10px; font:inherit; background:#fff; color:var(--ink); }
    textarea { min-height:430px; resize:vertical; font-family:ui-monospace,SFMono-Regular,Menlo,monospace; font-size:12px; line-height:1.5; }
    .tools textarea { min-height:96px; }
    .actions { display:flex; gap:10px; flex-wrap:wrap; align-items:center; }
    button { border:1px solid var(--border); background:#fff; color:#1d2939; border-radius:6px; padding:8px 12px; font-weight:650; cursor:pointer; }
    button.primary { background:var(--accent); border-color:var(--accent); color:#fff; }
    button:disabled { opacity:.55; cursor:not-allowed; }
    .muted { color:var(--muted); }
    .status { white-space:pre-wrap; border-top:1px solid var(--border); padding:12px 16px; color:#344054; background:#fbfcff; min-height:42px; }
    .mono { font-family:ui-monospace,SFMono-Regular,Menlo,monospace; font-size:12px; }
    @media (max-width: 900px) { .grid { grid-template-columns:1fr 1fr; } header { align-items:flex-start; flex-direction:column; } }
    @media (max-width: 560px) { .grid { grid-template-columns:1fr; } main { padding:16px; } }
  </style>
</head>
<body>
  <header>
    <h1>BullMQ Crawler</h1>
    <nav>
      <a href="/queues">Queues</a>
      <a href="/settings/agent" aria-current="page">Agent Settings</a>
    </nav>
  </header>
  <main>
    <section>
      <div class="section-head">
        <h2>Agent Config</h2>
        <span id="configMeta" class="muted mono">loading</span>
      </div>
      <div class="section-body">
        <div class="grid">
          <div><label for="provider">Provider</label><input id="provider" value="rules"></div>
          <div><label for="model">Model</label><input id="model" value="rules-agent-v1"></div>
          <div><label for="batchSize">Batch Size</label><input id="batchSize" type="number" min="1" max="50"></div>
          <div><label for="timeoutMs">Timeout Ms</label><input id="timeoutMs" type="number" min="5000" max="3600000" step="1000"></div>
          <div><label for="maxRetries">Max Retries</label><input id="maxRetries" type="number" min="0" max="10"></div>
          <div><label for="endpoint">Endpoint</label><input id="endpoint" placeholder="future LLM endpoint"></div>
          <div><label for="secretRef">Secret Ref</label><input id="secretRef" placeholder="env:OPENAI_API_KEY"></div>
        </div>
        <div class="tools" style="margin-top:12px">
          <label for="toolsJson">Tools JSON</label>
          <textarea id="toolsJson" spellcheck="false"></textarea>
        </div>
        <div class="actions" style="margin-top:12px">
          <button id="saveConfig" class="primary" type="button">Save Config</button>
          <span class="muted">Current worker still uses rules-agent-v1; prompt/config version is recorded for traceability.</span>
        </div>
      </div>
      <div id="configStatus" class="status"></div>
    </section>

    <section>
      <div class="section-head">
        <h2>Prompt Template</h2>
        <span id="promptMeta" class="muted mono">loading</span>
      </div>
      <div class="section-body">
        <div style="margin-bottom:12px">
          <label for="templateName">Template Name</label>
          <input id="templateName">
        </div>
        <label for="templateText">Template Text</label>
        <textarea id="templateText" spellcheck="false"></textarea>
        <div class="actions" style="margin-top:12px">
          <button id="saveDraft" type="button">Save Draft</button>
          <button id="publishDraft" class="primary" type="button" disabled>Publish Saved Draft</button>
          <span id="draftMeta" class="muted mono"></span>
        </div>
      </div>
      <div id="promptStatus" class="status"></div>
    </section>
  </main>
  <script>
    let state = { config: null, template: null, savedDraftId: null };
    const $ = (id) => document.getElementById(id);
    const status = (id, text) => { $(id).textContent = text; };
    async function api(path, options = {}) {
      const res = await fetch(path, { headers: { "content-type": "application/json" }, ...options });
      const text = await res.text();
      const data = text ? JSON.parse(text) : {};
      if (!res.ok || data.ok === false) throw new Error(data.error || text || res.statusText);
      return data;
    }
    function fill(data) {
      state.config = data.active_config;
      state.template = data.active_template;
      $("provider").value = data.active_config.provider || "rules";
      $("model").value = data.active_config.model || "rules-agent-v1";
      $("batchSize").value = data.active_config.batch_size || 50;
      $("timeoutMs").value = data.active_config.timeout_ms || 120000;
      $("maxRetries").value = data.active_config.max_retries || 2;
      $("endpoint").value = data.active_config.endpoint || "";
      $("secretRef").value = data.active_config.secret_ref || "";
      $("toolsJson").value = JSON.stringify(data.active_config.tools_json || [{ type: "web_search" }], null, 2);
      $("templateName").value = data.active_template.name || "YouTube Agent Prompt";
      $("templateText").value = data.active_template.template_text || "";
      $("configMeta").textContent = "config #" + data.active_config.config_id + " updated " + data.active_config.updated_at;
      $("promptMeta").textContent = "template #" + data.active_template.template_id + " v" + data.active_template.version + " hash " + data.active_config.prompt_hash.slice(0, 12);
    }
    async function load() {
      const data = await api("/api/agent/config");
      fill(data);
      status("configStatus", "Loaded active agent config.");
      status("promptStatus", "Loaded active prompt template.");
    }
    $("saveConfig").addEventListener("click", async () => {
      try {
        const payload = {
          provider: $("provider").value,
          model: $("model").value,
          endpoint: $("endpoint").value,
          secret_ref: $("secretRef").value,
          batch_size: Number($("batchSize").value),
          timeout_ms: Number($("timeoutMs").value),
          max_retries: Number($("maxRetries").value),
          tools_json: JSON.parse($("toolsJson").value || "[]")
        };
        const data = await api("/api/agent/config", { method: "POST", body: JSON.stringify(payload) });
        fill(data);
        status("configStatus", "Saved config #" + data.active_config.config_id + ".");
      } catch (error) {
        status("configStatus", "Error: " + error.message);
      }
    });
    $("saveDraft").addEventListener("click", async () => {
      try {
        const data = await api("/api/agent/templates/draft", {
          method: "POST",
          body: JSON.stringify({ name: $("templateName").value, template_text: $("templateText").value })
        });
        state.savedDraftId = data.template.template_id;
        $("publishDraft").disabled = false;
        $("draftMeta").textContent = "draft #" + data.template.template_id + " v" + data.template.version;
        status("promptStatus", "Saved draft. Publish it to make workers use this template version.");
      } catch (error) {
        status("promptStatus", "Error: " + error.message);
      }
    });
    $("publishDraft").addEventListener("click", async () => {
      if (!state.savedDraftId) return;
      try {
        const data = await api("/api/agent/templates/" + state.savedDraftId + "/publish", { method: "POST" });
        fill(data);
        $("publishDraft").disabled = true;
        $("draftMeta").textContent = "";
        status("promptStatus", "Published template #" + state.savedDraftId + " as active.");
        state.savedDraftId = null;
      } catch (error) {
        status("promptStatus", "Error: " + error.message);
      }
    });
    load().catch((error) => {
      status("configStatus", "Error: " + error.message);
      status("promptStatus", "Error: " + error.message);
    });
  </script>
</body>
</html>`;
}

app.get("/settings/agent", (_req, res) => {
  res.type("html").send(agentSettingsPage());
});

app.get("/health", async (_req, res) => {
  await query("SELECT 1");
  res.json({ ok: true, queues: await getQueueStats(queues), db: { ok: true } });
});

app.get("/api/queues", async (_req, res) => {
  const stats = await getQueueStats(queues);
  const rows = Object.entries(stats).map(([name, counts]) => ({ name, counts }));
  res.json({ ok: true, queues: rows });
});

app.post("/api/migration/channels/batch", asyncRoute(async (req, res) => {
  const result = await dispatchManualMigrationBatch({ selection: req.body?.selection });
  res.status(result.created ? 201 : 200).json(result);
}));

app.post("/api/migration/channels/:channelId", asyncRoute(async (req, res) => {
  const result = await dispatchManualMigrationChannel({
    channelId: req.params.channelId,
    candidateId: req.body?.candidate_id,
    queue: queues[queuesByRole.channelCrawl],
  });
  res.status(result.created ? 201 : 200).json(result);
}));

app.get("/api/agent/config", asyncRoute(async (_req, res) => {
  await ensureDefaultAgentConfig();
  const activeConfig = await getActiveAgentConfig();
  const templates = await query(
    `SELECT template_id, name, version, status, is_default, created_at, updated_at
     FROM crawler.agent_prompt_templates
     ORDER BY is_default DESC, updated_at DESC
     LIMIT 30`,
  );
  res.json({
    ok: true,
    active_config: activeConfig,
    active_template: {
      template_id: activeConfig.prompt_template_id,
      name: activeConfig.prompt_name,
      version: activeConfig.prompt_version,
      template_text: activeConfig.template_text,
      output_schema_json: activeConfig.output_schema_json,
      status: activeConfig.prompt_status,
    },
    templates: templates.rows,
  });
}));

app.post("/api/agent/config", asyncRoute(async (req, res) => {
  const updated = await updateDefaultAgentConfig(req.body ?? {});
  const activeConfig = await getActiveAgentConfig();
  res.json({
    ok: true,
    config: updated,
    active_config: activeConfig,
    active_template: {
      template_id: activeConfig.prompt_template_id,
      name: activeConfig.prompt_name,
      version: activeConfig.prompt_version,
      template_text: activeConfig.template_text,
      output_schema_json: activeConfig.output_schema_json,
      status: activeConfig.prompt_status,
    },
  });
}));

app.post("/api/agent/templates/draft", asyncRoute(async (req, res) => {
  let outputSchemaJson = req.body?.output_schema_json;
  if (typeof outputSchemaJson === "string" && outputSchemaJson.trim()) {
    outputSchemaJson = JSON.parse(outputSchemaJson);
  }
  const template = await createAgentTemplateDraft({
    name: req.body?.name,
    templateText: req.body?.template_text,
    outputSchemaJson,
  });
  res.status(201).json({ ok: true, template });
}));

app.post("/api/agent/templates/:templateId/publish", asyncRoute(async (req, res) => {
  await publishAgentTemplate(req.params.templateId);
  const activeConfig = await getActiveAgentConfig();
  res.json({
    ok: true,
    active_config: activeConfig,
    active_template: {
      template_id: activeConfig.prompt_template_id,
      name: activeConfig.prompt_name,
      version: activeConfig.prompt_version,
      template_text: activeConfig.template_text,
      output_schema_json: activeConfig.output_schema_json,
      status: activeConfig.prompt_status,
    },
  });
}));

app.post("/api/jobs/:queueName", async (req, res) => {
  const queue = queues[req.params.queueName];
  if (!queue) {
    res.status(404).json({ ok: false, error: "unknown queue", valid_queues: queueNames });
    return;
  }
  if (stronglyTypedManagedQueues.has(req.params.queueName)) {
    res.status(409).json({
      ok: false,
      error: "managed YouTube queues require a persistent typed Intent",
      code: "MANAGED_QUEUE_REQUIRES_TYPED_PRODUCER",
    });
    return;
  }

  const name = String(req.body?.name || req.params.queueName);
  const payload = req.body?.payload ?? {};
  const opts = req.body?.opts ?? {};
  const job = await queue.add(name, payload, {
    jobId: safeJobId(opts.jobId || name, opts.jobId ? null : nanoid()),
    priority: opts.priority,
    delay: opts.delay,
  });

  res.status(201).json({ ok: true, queue: req.params.queueName, job_id: job.id, name: job.name });
});

app.post("/api/queries", async (req, res) => {
  const queryText = String(req.body?.query_text ?? req.body?.query ?? "").trim();
  if (!queryText) {
    res.status(400).json({ ok: false, error: "query_text is required" });
    return;
  }
  const scored = (await scoreQueryBatch([queryText], {
    language: req.body?.language || process.env.YOUTUBE_LANGUAGE || "pt-BR",
    country: req.body?.country || process.env.YOUTUBE_COUNTRY || "BR",
    minSubscriberCount: Number(req.body?.min_subscriber_count || process.env.MIN_SUBSCRIBER_COUNT || 1000),
    fallbackOnly: true,
  }))[0];
  const qualityScore = Number(scored?.quality_score);
  const qualityStatus = String(scored?.quality_status || "scored_fallback");
  const row = await query(
    `INSERT INTO crawler.query_terms (
       query_text, language, country, category, priority,
       quality_score, quality_status, quality_json, quality_checked_at,
       metadata_json, updated_at
     )
     VALUES ($1, $2, $3, $4, $5, $6::numeric, $7, $8::jsonb, $9::timestamptz, $10::jsonb, now())
     ON CONFLICT (lower(query_text), COALESCE(language, ''), COALESCE(country, ''), COALESCE(category, ''))
     DO UPDATE SET status = 'active',
                   priority = GREATEST(crawler.query_terms.priority, EXCLUDED.priority),
                   quality_score = EXCLUDED.quality_score,
                   quality_status = EXCLUDED.quality_status,
                   quality_json = EXCLUDED.quality_json,
                   quality_checked_at = EXCLUDED.quality_checked_at,
                   metadata_json = crawler.query_terms.metadata_json || EXCLUDED.metadata_json,
                   updated_at = now()
     RETURNING *`,
    [
      queryText,
      req.body?.language ?? null,
      req.body?.country ?? null,
      req.body?.category ?? null,
      Number(req.body?.priority ?? 100),
      Number.isFinite(qualityScore) ? qualityScore : 0,
      qualityStatus,
      JSON.stringify(scored || {}),
      scored?.checked_at || new Date().toISOString(),
      JSON.stringify(req.body?.metadata ?? {}),
    ],
  );
  res.status(201).json({ ok: true, query: row.rows[0] });
});

app.post("/api/query-quality/score", asyncRoute(async (req, res) => {
  const rawQueries = Array.isArray(req.body?.queries) ? req.body.queries : [];
  const queries = Array.from(new Set(rawQueries.map((value) => String(value ?? "").trim()).filter(Boolean)));
  if (queries.length === 0) {
    res.status(400).json({ ok: false, error: "queries is required" });
    return;
  }
  if (queries.length > 200) {
    res.status(400).json({ ok: false, error: "queries max is 200 per request" });
    return;
  }
  if (req.body?.fallback_only !== true) {
    res.status(409).json({
      ok: false,
      error: "network Query Quality scoring requires a persistent Batch and Rota-managed Worker",
      code: "QUERY_QUALITY_REQUIRES_MANAGED_BATCH",
    });
    return;
  }
  const results = await scoreQueryBatch(queries, {
    language: req.body?.language || process.env.YOUTUBE_LANGUAGE || "pt-BR",
    country: req.body?.country || process.env.YOUTUBE_COUNTRY || "BR",
    minSubscriberCount: Number(req.body?.min_subscriber_count || process.env.MIN_SUBSCRIBER_COUNT || 1000),
    topVideos: Number(req.body?.top_videos || process.env.QUERY_QUALITY_TOP_VIDEOS || 20),
    concurrency: Number(req.body?.concurrency || process.env.QUERY_QUALITY_CONCURRENCY || 3),
    includeVideoSearch: req.body?.include_video_search === true,
    fallbackOnly: req.body?.fallback_only === true,
    fallbackOnRateLimit: req.body?.fallback_on_rate_limit !== false,
  });
  res.json({
    ok: true,
    count: results.length,
    results,
  });
}));

app.post("/api/query-quality/batches/:qualityBatchId/dispatch", asyncRoute(async (req, res) => {
  const response = await dispatchManagedQueryQualityBatch({
    qualityBatchId: req.params.qualityBatchId,
    intentStore: managedJobIntentStore,
    outboxDispatcher: managedJobOutboxDispatcher,
  });
  res.status(response.status).json(response.body);
}));

app.get("/api/queries/due", async (_req, res) => {
  const rows = await query(
    `SELECT *
     FROM crawler.query_terms
     WHERE next_crawl_at <= now()
       AND quality_score IS NOT NULL
       AND quality_status NOT IN ('unscored', 'failed')
       AND COALESCE(quality_score, 0) >= COALESCE((
         SELECT NULLIF(value_json->>'query_quality_min_score', '')::numeric
         FROM crawler.settings
         WHERE setting_key = 'query_scheduler'
         LIMIT 1
       ), 0)
     ORDER BY priority DESC, next_crawl_at ASC, query_id ASC
     LIMIT 100`,
  );
  res.json({ ok: true, queries: rows.rows });
});

app.post("/api/discover/seed-due", async (req, res) => {
  const limit = Math.max(1, Math.min(Number(req.body?.limit ?? 1), 100));
  const rows = await query(
    `SELECT *
     FROM crawler.query_terms
     WHERE next_crawl_at <= now()
       AND quality_score IS NOT NULL
       AND quality_status NOT IN ('unscored', 'failed')
       AND COALESCE(quality_score, 0) >= COALESCE((
         SELECT NULLIF(value_json->>'query_quality_min_score', '')::numeric
         FROM crawler.settings
         WHERE setting_key = 'query_scheduler'
         LIMIT 1
       ), 0)
     ORDER BY priority DESC, next_crawl_at ASC, query_id ASC
     LIMIT $1`,
    [limit],
  );
  const pages = [];
  const pipelineCycleId = String(req.body?.pipeline_cycle_id || `pipeline:manual:${nanoid(12)}`);
  for (const row of rows.rows) {
    const pageNo = 1;
    const discoveryRunId = `query:${row.query_id}:run:${Date.now()}:${nanoid(8)}`;
    const pageId = `${discoveryRunId}:page:${pageNo}`;
    const prepared = await managedJobIntentStore.prepareDiscoverPage({
      queryId: row.query_id,
      queryText: row.query_text,
      language: row.language,
      country: row.country,
      category: row.category,
      pageNo,
      pageId,
      discoveryRunId,
      pipelineCycleId,
      dispatchBatchId: pipelineCycleId,
      priority: row.priority,
      searchFilter: "video",
      sort: "popularity",
      timeWindow: "this_year",
    });
    pages.push({
      query_id: row.query_id,
      discovery_run_id: discoveryRunId,
      page_id: pageId,
      created: prepared.created,
    });
  }
  const dispatch = await managedJobOutboxDispatcher.dispatchAvailable({ limit: 500 });
  res.status(201).json({ ok: true, created_count: pages.length, pages, dispatch });
});

app.post("/api/demo", async (req, res) => {
  const runId = nanoid();
  const channelId = String(req.body?.channel_id || `UCdemo${nanoid(8)}`);
  const queryText = String(req.body?.query_text || `demo-${runId}`);
  const language = String(req.body?.language || "pt-BR");
  const country = String(req.body?.country || "BR");
  const queryRow = await query(
    `WITH inserted AS (
       INSERT INTO crawler.query_terms (
         query_text,language,country,category,status,priority,metadata_json,updated_at
       ) VALUES ($1,$2,$3,'demo','active',100,$4::jsonb,now())
       ON CONFLICT (
         lower(query_text),COALESCE(language,''),COALESCE(country,''),COALESCE(category,'')
       ) DO NOTHING
       RETURNING query_id
     )
     SELECT query_id FROM inserted
     UNION ALL
     SELECT query_id
     FROM crawler.query_terms
     WHERE lower(query_text)=lower($1)
       AND COALESCE(language,'')=COALESCE($2,'')
       AND COALESCE(country,'')=COALESCE($3,'')
       AND COALESCE(category,'')='demo'
     LIMIT 1`,
    [queryText, language, country, JSON.stringify({ demo_run_id: runId })],
  );
  const pipelineCycleId = `pipeline:demo:${runId}`;
  const pageId = `demo:${runId}:page:1`;
  await managedJobIntentStore.prepareDiscoverPage({
    queryId: queryRow.rows[0].query_id,
    queryText,
    language,
    country,
    pageNo: 1,
    pageId,
    discoveryRunId: `demo:${runId}`,
    pipelineCycleId,
    dispatchBatchId: pipelineCycleId,
    priority: 100,
    demo: true,
    channelId,
  });
  const dispatch = await managedJobOutboxDispatcher.dispatchAvailable({ limit: 100 });
  res.status(201).json({
    ok: true,
    run_id: runId,
    page_id: pageId,
    channel_id: channelId,
    dispatch,
  });
});

app.use(basePath, serverAdapter.getRouter());

app.use((error, _req, res, _next) => {
  console.error(JSON.stringify({ event: "http_error", error: error?.message || String(error) }));
  const requestedStatus = Number(error?.statusCode);
  const status = Number.isInteger(requestedStatus) && requestedStatus >= 400 && requestedStatus <= 599
    ? requestedStatus
    : ["MANAGED_JOB_INTENT_CONFLICT", "MANAGED_POLICY_UNAVAILABLE"].includes(error?.code)
      ? 409
      : error instanceof TypeError
        ? 400
        : 500;
  res.status(status).json({
    ok: false,
    error: error?.message || String(error),
    code: error?.code || "internal_error",
    ...(error?.details == null ? {} : { details: error.details }),
  });
});

const server = app.listen(port, "0.0.0.0", () => {
  console.log(`BullMQ crawler UI listening on :${port}${basePath}`);
});

async function shutdown(signal) {
  console.log(`received ${signal}, shutting down`);
  server.close(async () => {
    await closeQueues(queues);
    await Promise.all([closeDb(), closeMigrationSourcePool()]);
    process.exit(0);
  });
}

process.on("SIGTERM", () => shutdown("SIGTERM"));
process.on("SIGINT", () => shutdown("SIGINT"));
