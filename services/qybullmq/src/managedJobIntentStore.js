import {
  buildDiscoverPageIntent,
  buildQueryQualityChunkIntents,
  MANAGED_JOB_INTENT_SCHEMA_VERSION,
  managedJobIntentHash,
} from "./managedJobIntents.js";
import { queuesByRole, safeJobId } from "./queues.js";

export class ManagedJobIntentConflictError extends Error {
  constructor(aggregateKind, aggregateId) {
    super(`${aggregateKind} Intent conflicts with persisted aggregate ${aggregateId}`);
    this.name = "ManagedJobIntentConflictError";
    this.code = "MANAGED_JOB_INTENT_CONFLICT";
    this.aggregateKind = aggregateKind;
    this.aggregateId = aggregateId;
  }
}

function requiredText(value, field) {
  const normalized = String(value ?? "").trim();
  if (!normalized) throw new TypeError(`${field} is required`);
  return normalized;
}

function dispatchId(aggregateKind, aggregateId, intentHash) {
  const digest = managedJobIntentHash({ aggregateKind, aggregateId, intentHash })
    .replace(/^sha256:/, "")
    .slice(0, 32);
  return `managed-dispatch:${aggregateKind}:${digest}`;
}

function discoverOutbox(intent) {
  return {
    dispatch_id: dispatchId("discover_page", intent.pageId, intent.intentHash),
    aggregate_kind: "discover_page",
    aggregate_id: intent.pageId,
    intent_hash: intent.intentHash,
    queue_registry_key: queuesByRole.discoverPage,
    deterministic_job_id: safeJobId("discover-page", intent.pageId),
    payload_json: intent.jobPayload,
    status: "queued",
  };
}

function qualityOutbox(intent) {
  return {
    dispatch_id: dispatchId("query_quality_chunk", intent.qualityChunkId, intent.chunkIntentHash),
    aggregate_kind: "query_quality_chunk",
    aggregate_id: intent.qualityChunkId,
    intent_hash: intent.chunkIntentHash,
    queue_registry_key: queuesByRole.queryQuality,
    deterministic_job_id: safeJobId("query-quality", intent.qualityChunkId),
    payload_json: intent.jobPayload,
    status: "pending",
  };
}

function pageRow(intent) {
  return {
    page_id: intent.pageId,
    query_id: intent.queryId,
    query_text: intent.queryText,
    page_no: intent.pageNo,
    status: "queued",
    priority: intent.priority,
    dispatch_batch_id: intent.dispatchBatchId,
    page_intent_hash: intent.intentHash,
    intent_schema_version: MANAGED_JOB_INTENT_SCHEMA_VERSION,
    request_language: intent.language,
    request_country: intent.country,
    identity_policy_id: intent.policy.id,
    identity_policy_version: intent.policy.version,
    identity_policy_hash: intent.policy.hash,
    continuation_parent_page_id: intent.continuationParentPageId,
    continuation_token_hash: intent.continuationTokenHash,
    managed_fetch_status: "pending",
    qualification_status: "not_required",
    dispatch_status: "pending",
    result_json: { managed_intent: intent.managedIntent },
  };
}

function qualityChunkRow(intent) {
  return {
    quality_chunk_id: intent.qualityChunkId,
    quality_batch_id: intent.qualityBatchId,
    chunk_revision: 1,
    chunk_intent_hash: intent.chunkIntentHash,
    intent_schema_version: MANAGED_JOB_INTENT_SCHEMA_VERSION,
    effective_language: intent.effectiveLanguage,
    effective_country: intent.effectiveCountry,
    identity_policy_id: intent.policy.id,
    identity_policy_version: intent.policy.version,
    identity_policy_hash: intent.policy.hash,
    scoring_options: intent.scoringOptions,
    scoring_options_hash: intent.scoringOptionsHash,
    status: "pending",
    dispatch_status: "pending",
    quality_task_ids: [...intent.qualityTaskIds],
  };
}

function assertPageMatches(row, expected) {
  if (!row || row.page_intent_hash !== expected.page_intent_hash
      || Number(row.intent_schema_version) !== expected.intent_schema_version
      || row.identity_policy_id !== expected.identity_policy_id
      || Number(row.identity_policy_version) !== expected.identity_policy_version
      || row.identity_policy_hash !== expected.identity_policy_hash) {
    throw new ManagedJobIntentConflictError("discover_page", expected.page_id);
  }
}

function assertChunkMatches(row, expected) {
  if (!row || row.chunk_intent_hash !== expected.chunk_intent_hash
      || Number(row.intent_schema_version) !== expected.intent_schema_version
      || row.quality_batch_id !== expected.quality_batch_id
      || row.identity_policy_id !== expected.identity_policy_id
      || Number(row.identity_policy_version) !== expected.identity_policy_version
      || row.identity_policy_hash !== expected.identity_policy_hash) {
    throw new ManagedJobIntentConflictError("query_quality_chunk", expected.quality_chunk_id);
  }
}

export class ManagedJobIntentStore {
  constructor({ repository, policies, qualityChunkSize = 3 } = {}) {
    if (!repository || typeof repository.transaction !== "function") {
      throw new TypeError("repository.transaction is required");
    }
    this.repository = repository;
    this.policies = policies ?? [];
    this.qualityChunkSize = qualityChunkSize;
  }

  async prepareDiscoverPage(input) {
    const intent = buildDiscoverPageIntent(input, { policies: this.policies });
    return this.repository.transaction(async (transaction) => {
      const persisted = await transaction.persistDiscoverPage(pageRow(intent), discoverOutbox(intent));
      assertPageMatches(persisted.page, pageRow(intent));
      return persisted;
    });
  }

  async prepareQueryQualityBatch(qualityBatchId) {
    const normalizedBatchId = requiredText(qualityBatchId, "qualityBatchId");
    return this.repository.transaction(async (transaction) => {
      const batch = await transaction.loadUnassignedQueryQualityBatch(normalizedBatchId);
      if (!batch) throw new TypeError(`Query Quality Batch does not exist: ${normalizedBatchId}`);
      if (!["queued", "running"].includes(batch.status)) return { chunks: [], terminal: true };
      const intents = buildQueryQualityChunkIntents({
        qualityBatchId: normalizedBatchId,
        batchOptions: batch.options_json ?? {},
        tasks: batch.tasks ?? [],
        chunkSize: this.qualityChunkSize,
      }, { policies: this.policies });
      const chunks = [];
      for (const intent of intents) {
        const expected = qualityChunkRow(intent);
        const persisted = await transaction.persistQueryQualityChunk(
          expected,
          intent.members,
          qualityOutbox(intent),
        );
        assertChunkMatches(persisted, expected);
        chunks.push(persisted);
      }
      return { chunks, terminal: false };
    });
  }
}

class PostgresManagedJobIntentTransaction {
  constructor(client) {
    this.client = client;
  }

  async persistDiscoverPage(page, outbox) {
    await this.client.query("SELECT pg_advisory_xact_lock(hashtext($1))", [
      `discover-page-intent:${page.page_id}`,
    ]);
    if (page.dispatch_batch_id) {
      const pipelineCycleId = page.result_json?.managed_intent?.pipeline_cycle_id
        || page.dispatch_batch_id;
      await this.client.query(
        `INSERT INTO crawler.query_dispatch_batches (
           dispatch_batch_id,pipeline_cycle_id,status,result_json,updated_at
         ) VALUES ($1,$2,'running',$3::jsonb,now())
         ON CONFLICT (dispatch_batch_id) DO NOTHING`,
        [page.dispatch_batch_id, pipelineCycleId, JSON.stringify({ source: "managed_discover_intent" })],
      );
    }
    const inserted = await this.client.query(
      `INSERT INTO crawler.query_pages (
         page_id,query_id,query_text,page_no,status,priority,dispatch_batch_id,result_json,
         page_intent_hash,intent_schema_version,request_language,request_country,
         identity_policy_id,identity_policy_version,identity_policy_hash,
         continuation_parent_page_id,continuation_token_hash,managed_fetch_status,
         qualification_status,dispatch_status,updated_at
       ) VALUES (
         $1,$2,$3,$4,$5,$6,$7,$8::jsonb,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,now()
       ) ON CONFLICT (page_id) DO NOTHING
       RETURNING *`,
      [
        page.page_id,
        page.query_id,
        page.query_text,
        page.page_no,
        page.status,
        page.priority,
        page.dispatch_batch_id,
        JSON.stringify(page.result_json),
        page.page_intent_hash,
        page.intent_schema_version,
        page.request_language,
        page.request_country,
        page.identity_policy_id,
        page.identity_policy_version,
        page.identity_policy_hash,
        page.continuation_parent_page_id,
        page.continuation_token_hash,
        page.managed_fetch_status,
        page.qualification_status,
        page.dispatch_status,
      ],
    );
    const persisted = inserted.rows[0] ?? (await this.client.query(
      "SELECT * FROM crawler.query_pages WHERE page_id=$1 FOR UPDATE",
      [page.page_id],
    )).rows[0];
    assertPageMatches(persisted, page);
    await this.#persistOutbox(outbox);
    return { created: inserted.rowCount === 1, page: persisted };
  }

  async loadUnassignedQueryQualityBatch(qualityBatchId) {
    const batchRows = await this.client.query(
      "SELECT * FROM crawler.query_quality_batches WHERE quality_batch_id=$1 FOR UPDATE",
      [qualityBatchId],
    );
    const batch = batchRows.rows[0];
    if (!batch) return null;
    if (!["queued", "running"].includes(batch.status)) return { ...batch, tasks: [] };
    const tasks = await this.client.query(
      `SELECT task.quality_task_id,task.query_id,task.status,term.language,term.country
       FROM crawler.query_quality_tasks task
       JOIN crawler.query_terms term ON term.query_id=task.query_id
       LEFT JOIN crawler.query_quality_chunk_members member
         ON member.quality_batch_id=task.quality_batch_id
        AND member.quality_task_id=task.quality_task_id
       WHERE task.quality_batch_id=$1 AND task.status='queued'
         AND member.quality_task_id IS NULL
       ORDER BY task.quality_task_id
       FOR UPDATE OF task`,
      [qualityBatchId],
    );
    return { ...batch, tasks: tasks.rows };
  }

  async persistQueryQualityChunk(chunk, members, outbox) {
    const inserted = await this.client.query(
      `INSERT INTO crawler.query_quality_chunks (
         quality_chunk_id,quality_batch_id,chunk_revision,chunk_intent_hash,
         intent_schema_version,effective_language,effective_country,
         identity_policy_id,identity_policy_version,identity_policy_hash,
         scoring_options,scoring_options_hash,status,dispatch_status,updated_at
       ) VALUES (
         $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11::jsonb,$12,$13,$14,now()
       ) ON CONFLICT (quality_chunk_id) DO NOTHING
       RETURNING *`,
      [
        chunk.quality_chunk_id,
        chunk.quality_batch_id,
        chunk.chunk_revision,
        chunk.chunk_intent_hash,
        chunk.intent_schema_version,
        chunk.effective_language,
        chunk.effective_country,
        chunk.identity_policy_id,
        chunk.identity_policy_version,
        chunk.identity_policy_hash,
        JSON.stringify(chunk.scoring_options),
        chunk.scoring_options_hash,
        chunk.status,
        chunk.dispatch_status,
      ],
    );
    const persisted = inserted.rows[0] ?? (await this.client.query(
      "SELECT * FROM crawler.query_quality_chunks WHERE quality_chunk_id=$1 FOR UPDATE",
      [chunk.quality_chunk_id],
    )).rows[0];
    assertChunkMatches(persisted, chunk);
    if (inserted.rowCount === 1) {
      for (let index = 0; index < members.length; index += 1) {
        await this.client.query(
          `INSERT INTO crawler.query_quality_chunk_members (
             quality_batch_id,quality_chunk_id,quality_task_id,member_ordinal
           ) VALUES ($1,$2,$3,$4)`,
          [chunk.quality_batch_id, chunk.quality_chunk_id, members[index].qualityTaskId, index + 1],
        );
      }
    }
    const frozen = await this.client.query(
      `SELECT quality_task_id
       FROM crawler.query_quality_chunk_members
       WHERE quality_chunk_id=$1
       ORDER BY member_ordinal`,
      [chunk.quality_chunk_id],
    );
    const frozenIds = frozen.rows.map((row) => Number(row.quality_task_id));
    if (frozenIds.length !== chunk.quality_task_ids.length
        || frozenIds.some((value, index) => value !== chunk.quality_task_ids[index])) {
      throw new ManagedJobIntentConflictError("query_quality_chunk", chunk.quality_chunk_id);
    }
    await this.#persistOutbox(outbox);
    return { ...persisted, quality_task_ids: frozenIds };
  }

  async #persistOutbox(outbox) {
    const inserted = await this.client.query(
      `INSERT INTO crawler.proxy_job_dispatch_outbox (
         dispatch_id,aggregate_kind,aggregate_id,intent_hash,queue_registry_key,
         deterministic_job_id,payload_json,status,updated_at
       ) VALUES ($1,$2,$3,$4,$5,$6,$7::jsonb,'pending',now())
       ON CONFLICT (aggregate_kind,aggregate_id,intent_hash) DO NOTHING
       RETURNING dispatch_id`,
      [
        outbox.dispatch_id,
        outbox.aggregate_kind,
        outbox.aggregate_id,
        outbox.intent_hash,
        outbox.queue_registry_key,
        outbox.deterministic_job_id,
        JSON.stringify(outbox.payload_json),
      ],
    );
    if (inserted.rowCount === 1) return;
    const existing = await this.client.query(
      `SELECT dispatch_id,queue_registry_key,deterministic_job_id,payload_json
       FROM crawler.proxy_job_dispatch_outbox
       WHERE aggregate_kind=$1 AND aggregate_id=$2 AND intent_hash=$3`,
      [outbox.aggregate_kind, outbox.aggregate_id, outbox.intent_hash],
    );
    const row = existing.rows[0];
    if (!row || row.dispatch_id !== outbox.dispatch_id
        || row.queue_registry_key !== outbox.queue_registry_key
        || row.deterministic_job_id !== outbox.deterministic_job_id
        || JSON.stringify(row.payload_json) !== JSON.stringify(outbox.payload_json)) {
      throw new ManagedJobIntentConflictError(outbox.aggregate_kind, outbox.aggregate_id);
    }
  }
}

export class PostgresManagedJobIntentRepository {
  constructor({ withTransaction } = {}) {
    if (typeof withTransaction !== "function") throw new TypeError("withTransaction is required");
    this.withTransaction = withTransaction;
  }

  transaction(action) {
    return this.withTransaction((client) => action(new PostgresManagedJobIntentTransaction(client)));
  }
}

class InMemoryManagedJobIntentTransaction {
  constructor(repository) {
    this.repository = repository;
  }

  async persistDiscoverPage(page, outbox) {
    const existing = this.repository.pages.get(page.page_id);
    if (existing) {
      assertPageMatches(existing, page);
      this.repository.persistOutbox(outbox);
      return { created: false, page: structuredClone(existing) };
    }
    this.repository.pages.set(page.page_id, structuredClone(page));
    this.repository.persistOutbox(outbox);
    return { created: true, page: structuredClone(page) };
  }

  async loadUnassignedQueryQualityBatch(qualityBatchId) {
    const batch = this.repository.qualityBatches.get(qualityBatchId);
    if (!batch) return null;
    return {
      ...structuredClone(batch),
      tasks: batch.tasks
        .filter((task) => task.status === "queued"
          && !this.repository.qualityMembers.has(Number(task.quality_task_id)))
        .map((task) => structuredClone(task)),
    };
  }

  async persistQueryQualityChunk(chunk, members, outbox) {
    const existing = this.repository.qualityChunks.get(chunk.quality_chunk_id);
    if (existing) assertChunkMatches(existing, chunk);
    for (const member of members) {
      const taskId = Number(member.qualityTaskId);
      const assigned = this.repository.qualityMembers.get(taskId);
      if (assigned && assigned !== chunk.quality_chunk_id) {
        throw new ManagedJobIntentConflictError("query_quality_task", String(taskId));
      }
    }
    const persisted = existing ?? structuredClone(chunk);
    this.repository.qualityChunks.set(chunk.quality_chunk_id, persisted);
    members.forEach((member) => {
      this.repository.qualityMembers.set(Number(member.qualityTaskId), chunk.quality_chunk_id);
    });
    this.repository.persistOutbox(outbox);
    return structuredClone(persisted);
  }
}

export class InMemoryManagedJobIntentRepository {
  constructor({ qualityBatches = [] } = {}) {
    this.pages = new Map();
    this.outbox = new Map();
    this.qualityChunks = new Map();
    this.qualityMembers = new Map();
    this.qualityBatches = new Map(qualityBatches.map((batch) => [
      batch.quality_batch_id,
      structuredClone(batch),
    ]));
  }

  async transaction(action) {
    return action(new InMemoryManagedJobIntentTransaction(this));
  }

  persistOutbox(outbox) {
    const key = `${outbox.aggregate_kind}:${outbox.aggregate_id}:${outbox.intent_hash}`;
    const existing = this.outbox.get(key);
    if (existing && (existing.dispatch_id !== outbox.dispatch_id
        || existing.deterministic_job_id !== outbox.deterministic_job_id)) {
      throw new ManagedJobIntentConflictError(outbox.aggregate_kind, outbox.aggregate_id);
    }
    if (!existing) this.outbox.set(key, structuredClone(outbox));
  }
}
