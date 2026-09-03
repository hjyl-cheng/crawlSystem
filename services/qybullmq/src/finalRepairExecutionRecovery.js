import { finalRepairCandidateSql } from "./finalRepairCandidatePolicy.js";

const TERMINAL_JOB_STATES = new Set(["completed", "failed"]);
const SUCCESSFUL_FINALIZE_STATUSES = new Set(["ready_auto", "ready_partial"]);

function requiredText(value, field) {
  const normalized = String(value ?? "").trim();
  if (!normalized) throw new TypeError(`${field} is required`);
  return normalized;
}

function positiveInteger(value, field) {
  const normalized = Number(value);
  if (!Number.isSafeInteger(normalized) || normalized <= 0) {
    throw new TypeError(`${field} must be a positive integer`);
  }
  return normalized;
}

function nonNegativeInteger(value, field) {
  const normalized = Number(value);
  if (!Number.isSafeInteger(normalized) || normalized < 0) {
    throw new TypeError(`${field} must be a non-negative integer`);
  }
  return normalized;
}

export class FinalRepairExecutionBusyError extends Error {
  constructor(runId, jobId, state) {
    super(`Final Repair ${runId} cannot replace Detail owner ${jobId} in state ${state}`);
    this.name = "FinalRepairExecutionBusyError";
    this.code = "FINAL_REPAIR_EXECUTION_BUSY";
    this.runId = runId;
    this.jobId = jobId;
    this.state = state;
  }
}

export class FinalRepairExecutionConflictError extends Error {
  constructor(runId) {
    super(`Final Repair ${runId} Detail ownership changed during recovery`);
    this.name = "FinalRepairExecutionConflictError";
    this.code = "FINAL_REPAIR_EXECUTION_CONFLICT";
    this.runId = runId;
  }
}

function optionalText(value) {
  return String(value ?? "").trim() || null;
}

function optionalInteger(value) {
  if (value == null || value === "") return null;
  const normalized = Number(value);
  return Number.isSafeInteger(normalized) ? normalized : Number.NaN;
}

function sameExecutionState(row, expected) {
  return Number(row.detail_job_epoch) === expected.expectedJobEpoch
    && optionalText(row.detail_active_job_id) === expected.expectedActiveJobId
    && optionalInteger(row.detail_active_job_attempt) === expected.expectedActiveJobAttempt
    && (row.detail_active_scope_key ?? null) === expected.expectedActiveScopeKey
    && optionalInteger(row.detail_active_job_epoch) === expected.expectedActiveJobEpoch;
}

export class PostgresFinalRepairExecutionRecoveryRepository {
  constructor({ withTransaction } = {}) {
    if (typeof withTransaction !== "function") throw new TypeError("withTransaction is required");
    this.withTransaction = withTransaction;
  }

  loadExecutionState({ runId, channelId }) {
    return this.withTransaction(async (client) => {
      const result = await client.query(
        `SELECT run_id,channel_id,detail_job_epoch,detail_active_job_id,
                detail_active_job_attempt,detail_active_scope_key,detail_active_job_epoch
         FROM crawler.channel_runs
         WHERE run_id=$1 AND channel_id=$2`,
        [runId, channelId],
      );
      return result.rows[0] ?? null;
    });
  }

  loadBusinessState({ runId, channelId }) {
    return this.withTransaction(async (client) => {
      const result = await client.query(
        `SELECT run.detail_status,run.publication_finalized_status,
                run.publication_finalized_at,
                count(candidate.*) FILTER (
                  WHERE candidate.detail_status IN ('queued','running','failed','api_pending')
                     OR candidate.api_status IN ('pending','queued','running','failed')
                     OR ${finalRepairCandidateSql("candidate")}
                )::int AS open_candidate_count,
                (
                  channel.status='active'
                  AND channel.agent_status='done'
                  AND channel.registry_promotion_run_id IS NOT NULL
                  AND promotion_candidate.status='accepted'
                  AND promotion_candidate.accepted_at IS NOT NULL
                  AND NOT EXISTS (
                    SELECT 1
                    FROM publication.channel_stream_state owner
                    WHERE owner.channel_id=channel.channel_id
                      AND owner.status='owned'
                      AND (
                        owner.onboarding_mode='bootstrap'
                        AND owner.seed_status='pending'
                        AND owner.ownership_reference->>'onboarding_mode'='automatic_bootstrap'
                        AND owner.ownership_reference->>'initial_full_run_id'
                              =channel.registry_promotion_run_id
                      ) IS NOT TRUE
                  )
                  AND EXISTS (
                    SELECT 1
                    FROM publication.stream automatic_stream
                    WHERE automatic_stream.status='active'
                      AND automatic_stream.capture_enabled_at IS NOT NULL
                      AND promotion_candidate.accepted_at>=automatic_stream.capture_enabled_at
                      AND COALESCE(
                            automatic_stream.source_identity_json->>'stream_role',
                            ''
                          )<>'dead_letter_recovery'
                      AND COALESCE((
                        SELECT bool_and(delivery.mode='online')
                        FROM publication.channel_delivery_state delivery
                        JOIN publication.channel_stream_state route_owner
                          ON route_owner.publication_stream_id=delivery.publication_stream_id
                         AND route_owner.channel_id=delivery.channel_id
                        WHERE delivery.publication_stream_id=automatic_stream.publication_stream_id
                          AND route_owner.status='owned'
                      ),false)
                  )
                ) AS publication_open
         FROM crawler.channels channel
         JOIN crawler.channel_runs run
           ON run.run_id=$1 AND run.channel_id=channel.channel_id
         LEFT JOIN crawler.channel_candidates promotion_candidate
           ON promotion_candidate.candidate_id=channel.registry_promotion_candidate_id
          AND promotion_candidate.channel_id=channel.channel_id
         LEFT JOIN crawler.content_candidates candidate ON candidate.run_id=run.run_id
         WHERE channel.channel_id=$2
         GROUP BY channel.channel_id,channel.status,channel.agent_status,
                  channel.registry_promotion_run_id,promotion_candidate.status,
                  promotion_candidate.accepted_at,run.run_id`,
        [runId, channelId],
      );
      return result.rows[0] ?? null;
    });
  }

  prepareDetailDispatch(input) {
    return this.withTransaction(async (client) => {
      const locked = await client.query(
        `SELECT run_id,channel_id,detail_job_epoch,detail_active_job_id,
                detail_active_job_attempt,detail_active_scope_key,detail_active_job_epoch
         FROM crawler.channel_runs
         WHERE run_id=$1 AND channel_id=$2
         FOR UPDATE`,
        [input.runId, input.channelId],
      );
      const run = locked.rows[0];
      if (!run || !sameExecutionState(run, input)) {
        throw new FinalRepairExecutionConflictError(input.runId);
      }
      const leaseReplaced = input.expectedActiveJobId != null;
      const jobEpoch = input.expectedJobEpoch + (leaseReplaced ? 1 : 0);
      const updatedRun = await client.query(
        `UPDATE crawler.channel_runs
         SET detail_job_epoch=$3,
             detail_active_job_id=NULL,detail_active_job_attempt=NULL,
             detail_active_scope_key=NULL,detail_active_job_epoch=NULL,
             result_json=jsonb_set(
               COALESCE(result_json,'{}'::jsonb),
               '{final_repair_recovery_intent}',
               jsonb_build_object(
                 'status','prepared',
                 'repair_round',$4::int,
                 'job_id',$5::text,
                 'content_detail_job_epoch',$3::bigint,
                 'replaced_job_id',$6::text,
                 'replaced_job_attempt',$7::bigint,
                 'replaced_job_state',$8::text,
                 'prepared_at',now()
               ),
               true
             ),
             updated_at=now()
         WHERE run_id=$1 AND channel_id=$2
           AND detail_job_epoch=$9
           AND detail_active_job_id IS NOT DISTINCT FROM $6
           AND detail_active_job_attempt IS NOT DISTINCT FROM $7
           AND detail_active_scope_key IS NOT DISTINCT FROM $10
           AND detail_active_job_epoch IS NOT DISTINCT FROM $11
         RETURNING run_id`,
        [
          input.runId,
          input.channelId,
          jobEpoch,
          input.repairRound,
          input.jobId,
          input.expectedActiveJobId,
          input.expectedActiveJobAttempt,
          input.expectedActiveJobState,
          input.expectedJobEpoch,
          input.expectedActiveScopeKey,
          input.expectedActiveJobEpoch,
        ],
      );
      if (updatedRun.rowCount !== 1) {
        throw new FinalRepairExecutionConflictError(input.runId);
      }
      await client.query(
        `UPDATE crawler.content_candidates candidate
         SET content_key=COALESCE(candidate.content_key,stored.content_key),
             api_status=CASE
               WHEN candidate.detail_status='done'
                AND cardinality(
                      array_remove(
                        COALESCE(candidate.missing_fields,'{}'::text[]),
                        'comments_first_page'
                      )
                    )=0
                 THEN 'done'
               ELSE candidate.api_status
             END,
             missing_fields=array_remove(
               COALESCE(candidate.missing_fields,'{}'::text[]),
               'comments_first_page'
             ),
             error_message=CASE
               WHEN candidate.detail_status='done'
                AND cardinality(
                      array_remove(
                        COALESCE(candidate.missing_fields,'{}'::text[]),
                        'comments_first_page'
                      )
                    )=0
                 THEN NULL
               ELSE candidate.error_message
             END,
             result_json=jsonb_set(
               jsonb_set(
                 COALESCE(candidate.result_json,'{}'::jsonb),
                 '{detail}',
                 COALESCE(candidate.result_json->'detail','{}'::jsonb)
                   || jsonb_build_object(
                        'comments_first_page',stored.comments_first_page,
                        'comments_first_page_status','collected',
                        'comments_first_page_source','stored_content_reconciliation'
                      ),
                 true
               ),
               '{final_repair_dispatch}',
               jsonb_build_object(
                 'status','prepared',
                 'mode','detail',
                 'repair_round',$2::int,
                 'job_id',$3::text,
                 'content_detail_job_epoch',$4::bigint,
                 'prepared_at',now()
               ),
               true
             ),
             updated_at=now()
         FROM crawler.contents stored
         WHERE candidate.run_id=$1
           AND stored.run_id=candidate.run_id
           AND stored.channel_id=candidate.channel_id
           AND stored.source_content_id=candidate.source_content_id
           AND candidate.missing_fields @> ARRAY['comments_first_page']::text[]
           AND COALESCE((stored.comments_first_page->>'returned_count')::int,0)>0`,
        [input.runId, input.repairRound, input.jobId, jobEpoch],
      );
      const candidates = await client.query(
        `WITH crawl_settings AS (
           SELECT GREATEST(
                    0,
                    LEAST(
                      3650,
                      COALESCE(
                        (
                          SELECT (value_json->>'content_max_age_days')::int
                          FROM crawler.settings
                          WHERE setting_key='crawl'
                          LIMIT 1
                        ),
                        90
                      )
                    )
                  )::int AS content_max_age_days
         )
         UPDATE crawler.content_candidates candidate
         SET detail_status='queued',api_status='not_needed',attempts=0,
             missing_fields='{}'::text[],error_message=NULL,finished_at=NULL,
             result_json=jsonb_set(
               COALESCE(candidate.result_json,'{}'::jsonb),
               '{final_repair_dispatch}',
               jsonb_build_object(
                 'status','prepared',
                 'mode','detail',
                 'repair_round',$2::int,
                 'job_id',$3::text,
                 'content_detail_job_epoch',$4::bigint,
                 'prepared_at',now()
               ),
               true
             ),
             updated_at=now()
         FROM crawl_settings settings
         WHERE candidate.run_id=$1
           AND NOT (candidate.result_json ? 'parser_contract_error')
           AND COALESCE(candidate.result_json->'scope'->>'status','')<>'excluded'
           AND (
             settings.content_max_age_days=0
             OR NOT EXISTS (
               SELECT 1
               FROM crawler.contents repair_content
               WHERE repair_content.content_key=candidate.content_key
                 AND repair_content.run_id=candidate.run_id
                 AND repair_content.published_at IS NOT NULL
                 AND repair_content.published_at
                       <now()-(settings.content_max_age_days * interval '1 day')
             )
           )
           AND (
             ${finalRepairCandidateSql("candidate")}
             OR (
               candidate.result_json#>>'{final_repair_dispatch,status}'='prepared'
               AND candidate.result_json#>>'{final_repair_dispatch,mode}'='detail'
               AND candidate.result_json#>>'{final_repair_dispatch,repair_round}'=$2::text
               AND candidate.result_json#>>'{final_repair_dispatch,job_id}'=$3::text
               AND candidate.detail_status IN ('queued','running','failed')
             )
           )
         RETURNING candidate_id`,
        [input.runId, input.repairRound, input.jobId, jobEpoch],
      );
      return {
        content_detail_job_epoch: jobEpoch,
        lease_replaced: leaseReplaced,
      };
    });
  }
}

export class FinalRepairExecutionRecovery {
  constructor({ repository, findJob } = {}) {
    if (!repository || typeof repository.loadExecutionState !== "function"
        || typeof repository.prepareDetailDispatch !== "function") {
      throw new TypeError("a Final Repair execution recovery repository is required");
    }
    if (typeof findJob !== "function") throw new TypeError("findJob is required");
    this.repository = repository;
    this.findJob = findJob;
  }

  async isBusinessComplete({ runId, channelId } = {}) {
    if (typeof this.repository.loadBusinessState !== "function") {
      throw new TypeError("Final Repair recovery repository cannot load business state");
    }
    const normalizedRunId = requiredText(runId, "runId");
    const normalizedChannelId = requiredText(channelId, "channelId");
    const state = await this.repository.loadBusinessState({
      runId: normalizedRunId,
      channelId: normalizedChannelId,
    });
    if (!state) return false;
    return Number(state.open_candidate_count) === 0
      && state.detail_status === "done"
      && SUCCESSFUL_FINALIZE_STATUSES.has(state.publication_finalized_status)
      && state.publication_finalized_at != null
      && state.publication_open === false;
  }

  async prepareDetailDispatch({ runId, channelId, repairRound, jobId } = {}) {
    const normalizedRunId = requiredText(runId, "runId");
    const normalizedChannelId = requiredText(channelId, "channelId");
    const normalizedJobId = requiredText(jobId, "jobId");
    const normalizedRepairRound = positiveInteger(repairRound, "repairRound");
    const state = await this.repository.loadExecutionState({
      runId: normalizedRunId,
      channelId: normalizedChannelId,
    });
    if (!state) throw new Error(`Final Repair Run is missing: ${normalizedRunId}`);
    const expectedJobEpoch = nonNegativeInteger(state.detail_job_epoch, "detail_job_epoch");
    const expectedActiveJobId = String(state.detail_active_job_id ?? "").trim() || null;
    let expectedActiveJobState = null;
    if (expectedActiveJobId) {
      const activeJob = await this.findJob(expectedActiveJobId);
      if (!activeJob || String(activeJob.id ?? "") !== expectedActiveJobId
          || typeof activeJob.getState !== "function") {
        throw new FinalRepairExecutionBusyError(
          normalizedRunId,
          expectedActiveJobId,
          "missing",
        );
      }
      expectedActiveJobState = await activeJob.getState();
      if (!TERMINAL_JOB_STATES.has(expectedActiveJobState)) {
        throw new FinalRepairExecutionBusyError(
          normalizedRunId,
          expectedActiveJobId,
          expectedActiveJobState,
        );
      }
    }
    const prepared = await this.repository.prepareDetailDispatch({
      runId: normalizedRunId,
      channelId: normalizedChannelId,
      repairRound: normalizedRepairRound,
      jobId: normalizedJobId,
      expectedJobEpoch,
      expectedActiveJobId,
      expectedActiveJobAttempt: state.detail_active_job_attempt == null
        ? null
        : nonNegativeInteger(state.detail_active_job_attempt, "detail_active_job_attempt"),
      expectedActiveScopeKey: state.detail_active_scope_key ?? null,
      expectedActiveJobEpoch: state.detail_active_job_epoch == null
        ? null
        : nonNegativeInteger(state.detail_active_job_epoch, "detail_active_job_epoch"),
      expectedActiveJobState,
    });
    const jobEpoch = nonNegativeInteger(
      prepared?.content_detail_job_epoch,
      "content_detail_job_epoch",
    );
    return {
      data: { content_detail_job_epoch: jobEpoch },
      lease_replaced: prepared?.lease_replaced === true,
    };
  }
}
