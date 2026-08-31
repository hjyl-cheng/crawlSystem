import { observationFactsHash, recordCrawlerObservation } from "./crawlObservationStore.js";
import { classifyAgentFailure } from "./agentRetryPolicy.js";
import { buildAgentPublicationRun } from "./agentPublicationCurrent.js";
import { AGENT_TAXONOMY_VERSION } from "./publicationContract.js";
import { reconcilePublication } from "./publicationReconciler.js";

function text(value) {
  const output = String(value ?? "").trim();
  return output || null;
}

function dateOnly(value) {
  if (value == null) return null;
  const raw = String(value);
  if (/^\d{4}-\d{2}-\d{2}$/.test(raw)) return raw;
  const parsed = new Date(value);
  return Number.isNaN(parsed.getTime()) ? null : parsed.toISOString().slice(0, 10);
}

function positiveInteger(value, fallback) {
  const parsed = Number.parseInt(String(value ?? ""), 10);
  return Number.isSafeInteger(parsed) && parsed > 0 ? parsed : fallback;
}

export function incrementalAgentFailureDisposition({
  attempts,
  error,
  maxAttempts = 8,
}) {
  const attemptCount = positiveInteger(attempts, 1);
  const maximum = positiveInteger(maxAttempts, 8);
  const failure = classifyAgentFailure(error);
  return {
    ...failure,
    attempts: attemptCount,
    max_attempts: maximum,
    terminal: failure.retryable !== true || attemptCount >= maximum,
  };
}

function groupRequests(rows) {
  const grouped = new Map();
  for (const row of rows) {
    const current = grouped.get(row.channel_id) ?? {
      channel_id: row.channel_id,
      plan_ids: [],
      run_ids: [],
      plan_day: dateOnly(row.plan_day),
      scheduled_at: row.scheduled_at == null ? null : new Date(row.scheduled_at).toISOString(),
      attempts: 0,
    };
    current.plan_ids.push(String(row.plan_id));
    if (row.run_id) current.run_ids.push(String(row.run_id));
    current.attempts = Math.max(current.attempts, Number(row.attempts ?? 0));
    grouped.set(row.channel_id, current);
  }
  return [...grouped.values()];
}

function evidenceCount(value) {
  if (Array.isArray(value)) return value.reduce((total, item) => total + evidenceCount(item), 0);
  if (!value || typeof value !== "object") return 0;
  return Object.entries(value).reduce((total, [key, item]) => (
    total + (key === "evidence" && Array.isArray(item) ? item.length : evidenceCount(item))
  ), 0);
}

function normalizedTopicToken(prefix, value) {
  const normalized = text(value)?.toLocaleLowerCase("en").replace(/\s+/g, " ");
  return normalized ? `${prefix}:${normalized}` : null;
}

function topicTokens(profile) {
  const categories = valueAt(profile, "channel_categories");
  const tags = valueAt(profile, "channel_tags")?.tags;
  return [...new Set([
    normalizedTopicToken("l1", categories?.level_1),
    ...(Array.isArray(categories?.level_2)
      ? categories.level_2.map((value) => normalizedTopicToken("l2", value))
      : []),
    ...(Array.isArray(tags)
      ? tags.map((value) => normalizedTopicToken("tag", value))
      : []),
  ].filter(Boolean))].sort();
}

function collectEvidenceFingerprints(value, output) {
  if (Array.isArray(value)) {
    for (const item of value) collectEvidenceFingerprints(item, output);
    return;
  }
  if (!value || typeof value !== "object") return;
  for (const [key, item] of Object.entries(value)) {
    if (key === "evidence" && Array.isArray(item)) {
      for (const evidence of item) output.add(observationFactsHash(evidence));
    } else {
      collectEvidenceFingerprints(item, output);
    }
  }
}

function evidenceFingerprints(metrics) {
  const output = new Set();
  collectEvidenceFingerprints(metrics, output);
  return [...output].sort().slice(0, 256);
}

function valueAt(profile, name) {
  return profile?.[name]?.value ?? null;
}

export function incrementalAgentEventPayload(metrics, versionContext = {}) {
  const profile = metrics?.audience_profile_agent ?? {};
  const categories = valueAt(profile, "channel_categories");
  const tags = valueAt(profile, "channel_tags")?.tags;
  return {
    output_hash: observationFactsHash(metrics ?? {}),
    category_level_1: text(categories?.level_1),
    category_level_2: Array.isArray(categories?.level_2)
      ? categories.level_2.map(text).filter(Boolean)
      : [],
    tag_count: Array.isArray(tags) ? tags.length : 0,
    evidence_count: evidenceCount(metrics),
    active_subscriber_ratio: valueAt(profile, "active_subscriber_ratio"),
    topic_tokens: topicTokens(profile),
    evidence_fingerprints: evidenceFingerprints(metrics),
    agent_version_hash: versionContext.agentVersionHash ?? observationFactsHash({
      executor_mode: "basic",
      provider: versionContext.provider ?? null,
      model: versionContext.model ?? null,
      agent_config_id: versionContext.agentConfigId ?? null,
      prompt_template_id: versionContext.promptTemplateId ?? null,
      prompt_hash: versionContext.promptHash ?? null,
      prompt_variant: versionContext.promptVariant ?? null,
      taxonomy_version: versionContext.taxonomyVersion ?? "qy-taxonomy-v1",
      tools: versionContext.tools ?? [],
    }),
  };
}

export function isIncrementalAgentJob(job) {
  return text(job?.data?.batch_id)?.startsWith("incremental-agent:") === true
    && job?.data?.force_refresh === true
    && Array.isArray(job?.data?.incremental_agent_requests);
}

export async function markAgentChannelsRunning({
  withTransaction,
  channelIds,
  forceRefresh = false,
  transactionGuard = null,
}) {
  if (typeof withTransaction !== "function") throw new TypeError("withTransaction is required");
  if (transactionGuard != null && typeof transactionGuard !== "function") {
    throw new TypeError("transactionGuard must be a function");
  }
  const ids = [...new Set((channelIds ?? []).map(text).filter(Boolean))];
  if (ids.length === 0) return { rows: [], rowCount: 0 };
  return withTransaction(async (client) => {
    if (transactionGuard && await transactionGuard(client) !== true) {
      return { rows: [], rowCount: 0, fenceRejected: true };
    }
    return client.query(
      `UPDATE crawler.channels
       SET agent_status='running',agent_error_message=NULL,updated_at=now()
       WHERE channel_id=ANY($1::text[]) AND status='active'
         AND ($2::boolean OR agent_status<>'done')`,
      [ids, forceRefresh === true],
    );
  });
}

export async function persistAgentChannelSuccess({
  withTransaction,
  channelId,
  inputUrl,
  metrics,
  publicationRun,
  transactionGuard = null,
}) {
  if (typeof withTransaction !== "function") throw new TypeError("withTransaction is required");
  if (transactionGuard != null && typeof transactionGuard !== "function") {
    throw new TypeError("transactionGuard must be a function");
  }
  return withTransaction(async (client) => {
    if (transactionGuard && await transactionGuard(client) !== true) {
      return { rows: [], rowCount: 0, fenceRejected: true };
    }
    await client.query(
      `INSERT INTO crawler.agent_profiles (
         channel_id,agent_mode,input_url,status,metrics_json,agent_model,agent_config_id,
         prompt_template_id,prompt_hash,prompt_variant,input_content_ids,input_content_hash,
         taxonomy_version,agent_version_hash,attempts,error_message,updated_at
       ) VALUES (
         $1,'basic',$2,'success',$3::jsonb,$4,$5,$6,$7,$8,$9::text[],$10,$11,$12,
         1,NULL,now()
       )
       ON CONFLICT (channel_id,agent_mode) DO UPDATE
       SET input_url=EXCLUDED.input_url,status='success',metrics_json=EXCLUDED.metrics_json,
           agent_model=EXCLUDED.agent_model,agent_config_id=EXCLUDED.agent_config_id,
           prompt_template_id=EXCLUDED.prompt_template_id,prompt_hash=EXCLUDED.prompt_hash,
           prompt_variant=EXCLUDED.prompt_variant,input_content_ids=EXCLUDED.input_content_ids,
           input_content_hash=EXCLUDED.input_content_hash,
           taxonomy_version=EXCLUDED.taxonomy_version,
           agent_version_hash=EXCLUDED.agent_version_hash,
           attempts=crawler.agent_profiles.attempts+1,
           error_message=NULL,updated_at=now()`,
      [
        channelId,
        inputUrl,
        JSON.stringify(metrics),
        publicationRun.agent_model,
        publicationRun.agent_config_id,
        publicationRun.prompt_template_id,
        publicationRun.prompt_hash,
        publicationRun.prompt_variant,
        publicationRun.input_content_ids,
        publicationRun.input_content_hash,
        publicationRun.taxonomy_version,
        publicationRun.agent_version_hash,
      ],
    );
    return client.query(
      `UPDATE crawler.channels
       SET agent_status='done',agent_attempts=agent_attempts+1,agent_next_retry_at=NULL,
           agent_error_message=NULL,updated_at=now()
       WHERE channel_id=$1`,
      [channelId],
    );
  });
}

export async function persistAgentChannelFailure({
  withTransaction,
  channelId,
  inputUrl,
  agentModel,
  agentConfigId,
  promptTemplateId,
  promptHash,
  promptVariant,
  errorMessage,
  transactionGuard = null,
}) {
  if (typeof withTransaction !== "function") throw new TypeError("withTransaction is required");
  if (transactionGuard != null && typeof transactionGuard !== "function") {
    throw new TypeError("transactionGuard must be a function");
  }
  return withTransaction(async (client) => {
    if (transactionGuard && await transactionGuard(client) !== true) {
      return { rows: [], rowCount: 0, fenceRejected: true };
    }
    await client.query(
      `INSERT INTO crawler.agent_profiles (
         channel_id,agent_mode,input_url,status,metrics_json,agent_model,agent_config_id,
         prompt_template_id,prompt_hash,prompt_variant,attempts,error_message,updated_at
       ) VALUES ($1,'basic',$2,'failed','{}'::jsonb,$3,$4,$5,$6,$7,1,$8,now())
       ON CONFLICT (channel_id,agent_mode) DO UPDATE
       SET status='failed',attempts=crawler.agent_profiles.attempts+1,
           error_message=EXCLUDED.error_message,updated_at=now()`,
      [
        channelId,
        inputUrl,
        agentModel,
        agentConfigId,
        promptTemplateId,
        promptHash,
        promptVariant,
        errorMessage,
      ],
    );
    return client.query(
      `UPDATE crawler.channels
       SET agent_status='failed',agent_attempts=agent_attempts+1,
           agent_next_retry_at=now()+make_interval(secs=>LEAST(3600,30*power(2,LEAST(agent_attempts,7)))::int),
           agent_error_message=$2,updated_at=now()
       WHERE channel_id=$1`,
      [channelId, errorMessage],
    );
  });
}

export class IncrementalAgentResultStore {
  constructor({ withTransaction, maxAttempts = 8 }) {
    if (typeof withTransaction !== "function") throw new TypeError("withTransaction is required");
    this.withTransaction = withTransaction;
    this.maxAttempts = positiveInteger(maxAttempts, 8);
  }

  async claim(job) {
    if (!isIncrementalAgentJob(job)) throw new TypeError("incremental Agent job contract is required");
    const batchId = text(job.data.batch_id);
    const channelIds = [...new Set(job.data.channel_ids.map(text).filter(Boolean))];
    if (channelIds.length === 0) return { batchId, requests: [] };
    return this.withTransaction(async (client) => {
      const selected = await client.query(
        `SELECT request.* FROM crawler.agent_refresh_requests request
         JOIN crawler.channels channel ON channel.channel_id=request.channel_id
         WHERE request.batch_id=$1
           AND request.channel_id=ANY($2::text[])
           AND request.status IN ('queued','running','failed')
           AND channel.status='active'
         ORDER BY request.created_at,request.plan_id
         FOR UPDATE`,
        [batchId, channelIds],
      );
      if (selected.rows.length === 0) return { batchId, requests: [] };
      const planIds = selected.rows.map((row) => String(row.plan_id));
      const claimed = await client.query(
        `UPDATE crawler.agent_refresh_requests
         SET status='running',attempts=attempts+1,started_at=now(),last_error=NULL,updated_at=now()
         WHERE plan_id=ANY($1::uuid[])
         RETURNING *`,
        [planIds],
      );
      const requests = groupRequests(claimed.rows);
      await client.query(
          `UPDATE crawler.channels
           SET agent_status='running',agent_error_message=NULL,updated_at=now()
           WHERE channel_id=ANY($1::text[]) AND status='active'`,
        [requests.map((request) => request.channel_id)],
      );
      return { batchId, requests };
    });
  }

  async complete({ batchId, request, resolved, row, agentConfig }) {
    const observedAt = new Date().toISOString();
    const localExecution = resolved.execution_variant === "local_offline";
    const promptVariant = localExecution
      ? "local_offline"
      : resolved.country_required ? "country_required" : "country_resolved";
    const publicationRun = buildAgentPublicationRun({
      agentConfig,
      agentModel: resolved.agent_model ?? agentConfig.model,
      promptVariant,
      executionVariant: resolved.execution_variant ?? null,
      runtimeIdentity: localExecution ? resolved.metrics?.profile_processing_context : null,
      inputContentIds: resolved.input_content_ids ?? [],
      taxonomyVersion: process.env.AGENT_TAXONOMY_VERSION || AGENT_TAXONOMY_VERSION,
    });
    const payload = {
      ...incrementalAgentEventPayload(resolved.metrics, {
        provider: agentConfig.provider ?? null,
        model: publicationRun.agent_model,
        agentConfigId: publicationRun.agent_config_id,
        promptTemplateId: publicationRun.prompt_template_id,
        promptHash: publicationRun.prompt_hash,
        promptVariant: publicationRun.prompt_variant,
        taxonomyVersion: publicationRun.taxonomy_version,
        tools: agentConfig.tools_json ?? [],
        agentVersionHash: publicationRun.agent_version_hash,
      }),
      input_content_count: publicationRun.input_content_ids.length,
      input_content_hash: publicationRun.input_content_hash,
    };
    const runId = request.run_ids[0] ?? null;
    const planId = request.plan_ids[0] ?? null;
    return this.withTransaction(async (client) => {
      const recorded = await recordCrawlerObservation(client, {
        idempotencyKey: `agent:${batchId}:${request.channel_id}:attempt:${request.attempts}`,
        observationKind: "agent",
        channelId: request.channel_id,
        runId,
        observedAt,
        planId,
        planDay: request.plan_day,
        triggerReason: "clock_due",
        scheduledAt: request.scheduled_at,
        startedAt: observedAt,
        finishedAt: observedAt,
        crawlerVersion: String(process.env.CRAWLER_VERSION || "qy-v16"),
        extractorVersions: { agent: resolved.agent_model ?? agentConfig.model ?? null },
        command: {
          batch_id: batchId,
          fulfilled_plan_ids: request.plan_ids,
          output_hash: payload.output_hash,
          input_content_hash: publicationRun.input_content_hash,
          agent_version_hash: publicationRun.agent_version_hash,
        },
        prepare: async ({ client: transactionClient, observationId, sequence }) => {
          const active = await transactionClient.query(
            `SELECT plan_id,run_id FROM crawler.agent_refresh_requests
             WHERE batch_id=$1 AND channel_id=$2 AND status='running'
             ORDER BY created_at,plan_id
             FOR UPDATE`,
            [batchId, request.channel_id],
          );
          const planIds = active.rows.map((item) => String(item.plan_id));
          const runIds = [...new Set(active.rows.map((item) => text(item.run_id)).filter(Boolean))];
          if (planIds.length === 0) throw new Error(`no running Agent refresh request for ${request.channel_id}`);
          await transactionClient.query(
            `INSERT INTO crawler.agent_profiles (
               channel_id,agent_mode,input_url,status,metrics_json,agent_model,agent_config_id,
               prompt_template_id,prompt_hash,prompt_variant,input_content_ids,input_content_hash,
               taxonomy_version,agent_version_hash,attempts,error_message,
               last_observation_id,last_observed_at,current_output_hash,updated_at
             ) VALUES (
               $1,'basic',$2,'success',$3::jsonb,$4,$5,$6,$7,$8,$9::text[],$10,$11,$12,
               1,NULL,$13,$14,$15,now()
             )
             ON CONFLICT (channel_id,agent_mode) DO UPDATE
             SET input_url=EXCLUDED.input_url,status='success',metrics_json=EXCLUDED.metrics_json,
                 agent_model=EXCLUDED.agent_model,agent_config_id=EXCLUDED.agent_config_id,
                 prompt_template_id=EXCLUDED.prompt_template_id,prompt_hash=EXCLUDED.prompt_hash,
                 prompt_variant=EXCLUDED.prompt_variant,input_content_ids=EXCLUDED.input_content_ids,
                 input_content_hash=EXCLUDED.input_content_hash,
                 taxonomy_version=EXCLUDED.taxonomy_version,
                 agent_version_hash=EXCLUDED.agent_version_hash,
                 attempts=crawler.agent_profiles.attempts+1,
                 error_message=NULL,last_observation_id=EXCLUDED.last_observation_id,
                 last_observed_at=EXCLUDED.last_observed_at,current_output_hash=EXCLUDED.current_output_hash,
                 updated_at=now()`,
            [
              request.channel_id,
              row.input_url,
              JSON.stringify(resolved.metrics),
              publicationRun.agent_model,
              publicationRun.agent_config_id,
              publicationRun.prompt_template_id,
              publicationRun.prompt_hash,
              publicationRun.prompt_variant,
              publicationRun.input_content_ids,
              publicationRun.input_content_hash,
              publicationRun.taxonomy_version,
              publicationRun.agent_version_hash,
              observationId,
              observedAt,
              payload.output_hash,
            ],
          );
          await transactionClient.query(
            `UPDATE crawler.channels
             SET agent_status='done',agent_attempts=agent_attempts+1,agent_next_retry_at=NULL,
                 agent_error_message=NULL,updated_at=now()
             WHERE channel_id=$1`,
            [request.channel_id],
          );
          await transactionClient.query(
            `UPDATE crawler.agent_refresh_requests
             SET status='done',finished_at=$2,last_error=NULL,updated_at=now()
             WHERE plan_id=ANY($1::uuid[])`,
            [planIds, observedAt],
          );
          if (runIds.length > 0) {
            const state = JSON.stringify({
              status: "complete",
              observation_id: observationId,
              kind_sequence: sequence,
              output_hash: payload.output_hash,
              updated_at: observedAt,
            });
            await transactionClient.query(
              `UPDATE crawler.channel_runs
               SET result_json=jsonb_set(
                     COALESCE(result_json,'{}'::jsonb),
                     ARRAY['domains','agent'],
                     $2::jsonb,
                     true
                   ),
                   status=CASE
                     WHEN status='waiting_agent'
                       OR (
                         status='failed'
                         AND detail_status='done'
                         AND COALESCE(result_json#>>'{domains,agent,status}','') IN ('failed','retrying')
                       )
                     THEN 'done'
                     ELSE status
                   END,
                   finished_at=CASE
                     WHEN status='waiting_agent'
                       OR (
                         status='failed'
                         AND detail_status='done'
                         AND COALESCE(result_json#>>'{domains,agent,status}','') IN ('failed','retrying')
                       )
                     THEN $3::timestamptz
                     ELSE finished_at
                   END,
                   error_message=NULL,updated_at=now()
               WHERE run_id=ANY($1::text[]) AND crawl_mode='incremental'`,
              [runIds, state, observedAt],
            );
          }
          return {
            outcome: "complete",
            outcomeReasonCode: "agent_refresh_complete",
            resultSummary: {
              fulfilled_plan_count: planIds.length,
              category_level_1: payload.category_level_1,
              evidence_count: payload.evidence_count,
              output_hash: payload.output_hash,
              input_content_hash: publicationRun.input_content_hash,
              agent_version_hash: publicationRun.agent_version_hash,
            },
            payload: { ...payload, fulfilled_plan_count: planIds.length },
          };
        },
      });
      await reconcilePublication(client, {
        channelId: request.channel_id,
        domains: ["agent"],
        asOf: observedAt,
      });
      return recorded;
    });
  }

  async fail({ batchId, request, row, agentConfig, error }) {
    const observedAt = new Date().toISOString();
    const message = String(error?.message || error || "agent returned no result").slice(0, 2000);
    const runId = request.run_ids[0] ?? null;
    const planId = request.plan_ids[0] ?? null;
    const disposition = incrementalAgentFailureDisposition({
      attempts: request.attempts,
      error,
      maxAttempts: this.maxAttempts,
    });
    const localExecution = String(agentConfig?.provider ?? "").toLocaleLowerCase("en")
      === "local-offline";
    const failurePromptTemplateId = localExecution ? null : agentConfig?.prompt_template_id ?? null;
    const failurePromptHash = localExecution ? null : agentConfig?.prompt_hash ?? null;
    const failurePromptVariant = localExecution
      ? "local_offline"
      : row.country_required ? "country_required" : "country_resolved";
    if (!disposition.terminal) {
      return this.withTransaction(async (client) => {
        const active = await client.query(
          `SELECT plan_id,run_id FROM crawler.agent_refresh_requests
           WHERE batch_id=$1 AND channel_id=$2 AND status='running'
           ORDER BY created_at,plan_id
           FOR UPDATE`,
          [batchId, request.channel_id],
        );
        const planIds = active.rows.map((item) => String(item.plan_id));
        const runIds = [...new Set(active.rows.map((item) => text(item.run_id)).filter(Boolean))];
        if (planIds.length === 0) throw new Error(`no running Agent refresh request for ${request.channel_id}`);
        await client.query(
          `INSERT INTO crawler.agent_profiles (
             channel_id,agent_mode,input_url,status,metrics_json,agent_model,agent_config_id,
             prompt_template_id,prompt_hash,prompt_variant,attempts,error_message,updated_at
           ) VALUES ($1,'basic',$2,'failed','{}'::jsonb,$3,$4,$5,$6,$7,1,$8,now())
           ON CONFLICT (channel_id,agent_mode) DO UPDATE
           SET status='failed',attempts=crawler.agent_profiles.attempts+1,
               error_message=EXCLUDED.error_message,updated_at=now()`,
          [
            request.channel_id,
            row.input_url,
            agentConfig?.model ?? null,
            agentConfig?.config_id ?? null,
            failurePromptTemplateId,
            failurePromptHash,
            failurePromptVariant,
            message,
          ],
        );
        await client.query(
          `UPDATE crawler.channels
           SET agent_status='failed',agent_attempts=agent_attempts+1,
               agent_next_retry_at=now()+make_interval(secs=>LEAST(3600,30*power(2,LEAST(agent_attempts,7)))::int),
               agent_error_message=$2,updated_at=now()
           WHERE channel_id=$1`,
          [request.channel_id, message],
        );
        await client.query(
          `UPDATE crawler.agent_refresh_requests
           SET status='failed',batch_id=NULL,queued_at=NULL,finished_at=$2,last_error=$3,
               next_retry_at=now()+make_interval(secs=>LEAST(3600,30*power(2,LEAST(attempts,7)))::int),
               updated_at=now()
           WHERE plan_id=ANY($1::uuid[])`,
          [planIds, observedAt, message],
        );
        if (runIds.length > 0) {
          const state = JSON.stringify({
            status: "retrying",
            attempt: disposition.attempts,
            max_attempts: disposition.max_attempts,
            failure_kind: disposition.kind,
            error: message,
            updated_at: observedAt,
          });
          await client.query(
            `UPDATE crawler.channel_runs
             SET result_json=jsonb_set(
                   COALESCE(result_json,'{}'::jsonb),
                   ARRAY['domains','agent'],
                   $2::jsonb,
                   true
                 ),
                 updated_at=now()
             WHERE run_id=ANY($1::text[]) AND crawl_mode='incremental'`,
            [runIds, state],
          );
        }
        return {
          outcome: "retrying",
          terminal: false,
          failure_kind: disposition.kind,
          attempts: disposition.attempts,
          max_attempts: disposition.max_attempts,
        };
      });
    }
    return this.withTransaction((client) => recordCrawlerObservation(client, {
      idempotencyKey: `agent:${batchId}:${request.channel_id}:attempt:${request.attempts}`,
      observationKind: "agent",
      channelId: request.channel_id,
      runId,
      observedAt,
      planId,
      planDay: request.plan_day,
      triggerReason: "clock_due",
      scheduledAt: request.scheduled_at,
      startedAt: observedAt,
      finishedAt: observedAt,
      crawlerVersion: String(process.env.CRAWLER_VERSION || "qy-v16"),
      extractorVersions: { agent: agentConfig?.model ?? null },
      command: { batch_id: batchId, failed_plan_ids: request.plan_ids, attempt: request.attempts },
      prepare: async ({ client: transactionClient, observationId, sequence }) => {
        const active = await transactionClient.query(
          `SELECT plan_id,run_id FROM crawler.agent_refresh_requests
           WHERE batch_id=$1 AND channel_id=$2 AND status='running'
           ORDER BY created_at,plan_id
           FOR UPDATE`,
          [batchId, request.channel_id],
        );
        const planIds = active.rows.map((item) => String(item.plan_id));
        const runIds = [...new Set(active.rows.map((item) => text(item.run_id)).filter(Boolean))];
        if (planIds.length === 0) throw new Error(`no running Agent refresh request for ${request.channel_id}`);
        await transactionClient.query(
          `INSERT INTO crawler.agent_profiles (
             channel_id,agent_mode,input_url,status,metrics_json,agent_model,agent_config_id,
             prompt_template_id,prompt_hash,prompt_variant,attempts,error_message,updated_at
           ) VALUES ($1,'basic',$2,'failed','{}'::jsonb,$3,$4,$5,$6,$7,1,$8,now())
           ON CONFLICT (channel_id,agent_mode) DO UPDATE
           SET status='failed',attempts=crawler.agent_profiles.attempts+1,
               error_message=EXCLUDED.error_message,updated_at=now()`,
          [
            request.channel_id,
            row.input_url,
            agentConfig?.model ?? null,
            agentConfig?.config_id ?? null,
            failurePromptTemplateId,
            failurePromptHash,
            failurePromptVariant,
            message,
          ],
        );
        await transactionClient.query(
          `UPDATE crawler.channels
           SET agent_status='failed',agent_attempts=agent_attempts+1,
               agent_next_retry_at=NULL,
               agent_error_message=$2,updated_at=now()
           WHERE channel_id=$1`,
          [request.channel_id, message],
        );
        await transactionClient.query(
          `UPDATE crawler.agent_refresh_requests
           SET status='failed',batch_id=NULL,queued_at=NULL,finished_at=$2,last_error=$3,
               next_retry_at='infinity'::timestamptz,
               updated_at=now()
           WHERE plan_id=ANY($1::uuid[])`,
          [planIds, observedAt, message],
        );
        if (runIds.length > 0) {
          const state = JSON.stringify({
            status: "failed",
            observation_id: observationId,
            kind_sequence: sequence,
            attempt: disposition.attempts,
            max_attempts: disposition.max_attempts,
            failure_kind: disposition.kind,
            error: message,
            updated_at: observedAt,
          });
          await transactionClient.query(
            `UPDATE crawler.channel_runs
             SET result_json=jsonb_set(
                   COALESCE(result_json,'{}'::jsonb),
                   ARRAY['domains','agent'],
                   $2::jsonb,
                   true
                 ),
                 status='failed',finished_at=$3::timestamptz,
                 error_message=$4,updated_at=now()
             WHERE run_id=ANY($1::text[]) AND crawl_mode='incremental'`,
            [runIds, state, observedAt, message],
          );
        }
        return {
          outcome: "failed",
          outcomeReasonCode: "agent_refresh_failed",
          resultSummary: { failed_plan_count: planIds.length },
          payload: { failed_plan_count: planIds.length },
          errorClass: error?.name ?? "AgentRefreshError",
          errorMessage: message,
        };
      },
    }));
  }
}
