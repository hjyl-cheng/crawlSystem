import {
  incrementalPlanHash,
  incrementalRunId,
  validateIncrementalPlan,
} from "./incrementalPlan.js";

const DOMAINS = new Set(["about", "video", "agent"]);

export class IncrementalPlanConflict extends Error {
  constructor(planId) {
    super(`plan_id ${planId} is already bound to a different incremental Plan`);
    this.name = "IncrementalPlanConflict";
    this.planId = planId;
  }
}

function domainSeed(plan) {
  return Object.fromEntries(
    ["about", "video", "agent"].map((domain) => [
      domain,
      {
        status: plan.task_mask[domain] === true ? "pending" : "not_due",
      },
    ]),
  );
}

function json(value) {
  return JSON.stringify(value);
}

function resultJson(row) {
  if (!row?.result_json) return {};
  return typeof row.result_json === "string" ? JSON.parse(row.result_json) : row.result_json;
}

export class IncrementalRunStore {
  constructor({ withTransaction }) {
    if (typeof withTransaction !== "function") throw new TypeError("withTransaction is required");
    this.withTransaction = withTransaction;
  }

  async claim(input) {
    const plan = validateIncrementalPlan(input);
    const runId = incrementalRunId(plan.plan_id);
    const planHash = incrementalPlanHash(plan);
    return this.withTransaction(async (client) => {
      const inserted = await client.query(
        `INSERT INTO crawler.channel_runs (
           run_id,channel_id,status,crawl_mode,content_limit,detail_status,
           plan_id,plan_day,trigger_reason,task_mask,scheduled_at,
           clock_version,policy_version,planner_config_version,capacity_version,
           crawler_version,started_at,result_json,updated_at
         )
         SELECT $1,$2,'running','incremental',0,'pending',
                $3,$4,'clock_due',$5::jsonb,
                $6::timestamptz,
                $7,$8,$9,$10,$11,now(),$12::jsonb,now()
         WHERE EXISTS (
           SELECT 1 FROM crawler.channels
           WHERE channel_id=$2
             AND (
               ($13='standard' AND status='active')
               OR (
                 $13='dormant_probe'
                 AND (
                   status='dormant'
                   OR (status='rejected' AND reject_reason='no_published_content_within_90_days')
                 )
               )
             )
         )
         ON CONFLICT (plan_id) WHERE plan_id IS NOT NULL DO NOTHING
         RETURNING *`,
        [
          runId,
          plan.channel_id,
          plan.plan_id,
          plan.plan_day,
          json(plan.task_mask),
          plan.scheduled_at,
          plan.clock_version,
          plan.policy_version,
          plan.planner_config_version,
          plan.capacity.version,
          String(process.env.CRAWLER_VERSION || "qy-v16"),
          json({
            job_id: plan.job_id,
            plan_payload_hash: planHash,
            capacity: plan.capacity,
            plan_mode: plan.plan_mode,
            domains: domainSeed(plan),
          }),
          plan.plan_mode,
        ],
      );
      if (inserted.rowCount === 1) {
        return { created: true, resumed: false, terminal: false, run: inserted.rows[0] };
      }

      const existing = await client.query(
        `SELECT * FROM crawler.channel_runs
         WHERE plan_id=$1
         FOR UPDATE`,
        [plan.plan_id],
      );
      const row = existing.rows[0];
      if (!row) {
        throw new Error(`Channel lifecycle does not match incremental Plan: ${plan.channel_id}`);
      }
      const stored = resultJson(row);
      if (row.run_id !== runId
          || row.channel_id !== plan.channel_id
          || stored.plan_payload_hash !== planHash) {
        throw new IncrementalPlanConflict(plan.plan_id);
      }
      if (["done", "waiting_agent"].includes(row.status)) {
        return { created: false, resumed: false, terminal: true, run: row };
      }
      const resumed = await client.query(
        `UPDATE crawler.channel_runs
         SET status='running',error_message=NULL,finished_at=NULL,updated_at=now()
         WHERE run_id=$1
         RETURNING *`,
        [runId],
      );
      return { created: false, resumed: true, terminal: false, run: resumed.rows[0] };
    });
  }

  async markDomain(runId, domain, status, detail = {}) {
    if (!DOMAINS.has(domain)) throw new TypeError(`unknown incremental domain: ${domain}`);
    const state = { status, ...detail, updated_at: new Date().toISOString() };
    const rows = await this.withTransaction((client) => client.query(
      `UPDATE crawler.channel_runs
       SET result_json=jsonb_set(
             COALESCE(result_json,'{}'::jsonb),
             ARRAY['domains',$2::text],
             $3::jsonb,
             true
           ),
           updated_at=now()
       WHERE run_id=$1 AND crawl_mode='incremental'
       RETURNING *`,
      [runId, domain, json(state)],
    ));
    if (rows.rowCount !== 1) throw new Error(`incremental run not found: ${runId}`);
    return rows.rows[0];
  }

  async finish(runId, { waitingForAgent = false } = {}) {
    const rows = await this.withTransaction((client) => client.query(
      `UPDATE crawler.channel_runs
       SET status=CASE
             WHEN $2::boolean
               AND COALESCE(result_json #>> '{domains,agent,status}','')<>'complete'
             THEN 'waiting_agent'
             ELSE 'done'
           END,
           detail_status='done',
           finished_at=CASE
             WHEN $2::boolean
               AND COALESCE(result_json #>> '{domains,agent,status}','')<>'complete'
             THEN NULL
             ELSE now()
           END,
           error_message=NULL,updated_at=now()
       WHERE run_id=$1 AND crawl_mode='incremental'
       RETURNING *`,
      [runId, waitingForAgent],
    ));
    if (rows.rowCount !== 1) throw new Error(`incremental run not found: ${runId}`);
    return rows.rows[0];
  }

  async fail(runId, error) {
    const message = String(error?.message || error).slice(0, 2000);
    await this.withTransaction((client) => client.query(
      `UPDATE crawler.channel_runs
       SET status='failed',detail_status='failed',error_message=$2,
           finished_at=now(),updated_at=now()
       WHERE run_id=$1 AND crawl_mode='incremental'`,
      [runId, message],
    ));
  }
}

export function incrementalDomainState(run, domain) {
  return resultJson(run)?.domains?.[domain]?.status ?? null;
}
