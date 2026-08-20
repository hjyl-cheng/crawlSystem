import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import test from "node:test";
import pg from "pg";
import { IncrementalAgentResultStore } from "../src/incrementalAgentResultStore.js";

const { Pool } = pg;
const integrationUrl = process.env.INCREMENTAL_POSTGRES_TEST_URL;

function agentJob(batchId, channelId, planId, runId) {
  return {
    data: {
      batch_id: batchId,
      channel_ids: [channelId],
      agent_mode: "basic",
      force_refresh: true,
      incremental_agent_requests: [{
        channel_id: channelId,
        plan_ids: [planId],
        run_ids: [runId],
      }],
    },
  };
}

async function seed(pool, { channelId, runId, planId, batchId }) {
  await pool.query(
    `INSERT INTO crawler.channels (
       channel_id,channel_url,title,status,subscriber_count,agent_status,ready_for_agent
     ) VALUES ($1,$2,'Incremental Agent integration','active',1000,'done',true)`,
    [channelId, `https://www.youtube.com/channel/${channelId}`],
  );
  await pool.query(
    `INSERT INTO crawler.channel_runs (
       run_id,channel_id,status,crawl_mode,content_limit,detail_status,
       plan_id,plan_day,trigger_reason,task_mask,scheduled_at,
       clock_version,policy_version,planner_config_version,capacity_version,
       crawler_version,started_at,result_json
     ) VALUES (
       $1,$2,'waiting_agent','incremental',0,'done',$3,'2026-07-20','clock_due',
       '{"agent":true}'::jsonb,'2026-07-20T00:00:00Z',7,'v16-rule-1',
       'video-plan-1','capacity-1','test',now(),
       '{"domains":{"profile":{"status":"not_due"},"about":{"status":"not_due"},"video":{"status":"not_due"},"agent":{"status":"queued"}}}'::jsonb
     )`,
    [runId, channelId, planId],
  );
  await pool.query(
    `INSERT INTO crawler.agent_refresh_requests (
       plan_id,plan_day,channel_id,run_id,status,batch_id,clock_version,policy_version,queued_at
     ) VALUES ($1,'2026-07-20',$2,$3,'queued',$4,7,'v16-rule-1',now())`,
    [planId, channelId, runId, batchId],
  );
}

async function seedAgentConfig(pool, suffix) {
  const templateText = "Analyze every input URL and return the V1 Agent facts.";
  const template = await pool.query(
    `INSERT INTO crawler.agent_prompt_templates (
       name,version,template_text,status,is_default
     ) VALUES ($1,1,$2,'active',false)
     RETURNING template_id`,
    [`integration-agent-template-${suffix}`, templateText],
  );
  const config = await pool.query(
    `INSERT INTO crawler.agent_configs (
       name,provider,model,prompt_template_id,tools_json,enabled,is_default
     ) VALUES ($1,'openai-compatible','agent-test',$2,$3::jsonb,true,false)
     RETURNING config_id,provider,model,prompt_template_id,tools_json`,
    [
      `integration-agent-config-${suffix}`,
      template.rows[0].template_id,
      JSON.stringify([{ type: "web_search" }]),
    ],
  );
  return {
    ...config.rows[0],
    template_text: templateText,
  };
}

test("incremental Agent force-refreshes Current and settles its waiting Run", {
  skip: !integrationUrl,
}, async () => {
  const pool = new Pool({ connectionString: integrationUrl, max: 4 });
  const suffix = randomUUID().replaceAll("-", "");
  const channelId = `UCv16agent${suffix}`;
  const runId = `incremental:agent:${suffix}`;
  const planId = randomUUID();
  const batchId = `incremental-agent:success:${suffix}`;
  let agentConfig;
  const withTransaction = async (action) => {
    const client = await pool.connect();
    try {
      await client.query("BEGIN");
      const result = await action(client);
      await client.query("COMMIT");
      return result;
    } catch (error) {
      await client.query("ROLLBACK");
      throw error;
    } finally {
      client.release();
    }
  };
  const store = new IncrementalAgentResultStore({ withTransaction, maxAttempts: 1 });

  try {
    agentConfig = await seedAgentConfig(pool, suffix);
    await seed(pool, { channelId, runId, planId, batchId });
    const claimed = await store.claim(agentJob(batchId, channelId, planId, runId));
    assert.equal(claimed.requests.length, 1);
    assert.equal(claimed.requests[0].attempts, 1);

    const completed = await store.complete({
      batchId,
      request: claimed.requests[0],
      row: { input_url: `https://www.youtube.com/channel/${channelId}` },
      agentConfig,
      resolved: {
        agent_model: "agent-test",
        country_required: false,
        input_content_ids: [],
        metrics: {
          audience_profile_agent: {
            channel_categories: {
              value: { level_1: "Tech", level_2: ["Artificial Intelligence"] },
              evidence: ["public channel evidence"],
            },
            channel_tags: {
              value: { tags: ["AI", "Software"] },
              evidence: ["public tag evidence"],
            },
            active_subscriber_ratio: { value: 35, evidence: ["public activity evidence"] },
          },
        },
      },
    });
    assert.equal(completed.outcome, "complete");

    const state = await pool.query(
      `SELECT channel.agent_status,profile.status AS profile_status,
              profile.current_output_hash,profile.input_content_ids,
              profile.input_content_hash,profile.taxonomy_version,
              profile.agent_version_hash,profile.prompt_hash,
              profile.agent_config_id,profile.prompt_template_id,
              request.status AS request_status,
              run.status AS run_status,run.result_json #>> '{domains,agent,status}' AS domain_status
       FROM crawler.channels channel
       JOIN crawler.agent_profiles profile ON profile.channel_id=channel.channel_id
       JOIN crawler.agent_refresh_requests request ON request.channel_id=channel.channel_id
       JOIN crawler.channel_runs run ON run.run_id=request.run_id
       WHERE channel.channel_id=$1`,
      [channelId],
    );
    assert.deepEqual({
      agent_status: state.rows[0].agent_status,
      profile_status: state.rows[0].profile_status,
      request_status: state.rows[0].request_status,
      run_status: state.rows[0].run_status,
      domain_status: state.rows[0].domain_status,
    }, {
      agent_status: "done",
      profile_status: "success",
      request_status: "done",
      run_status: "done",
      domain_status: "complete",
    });
    assert.match(state.rows[0].current_output_hash, /^sha256:[0-9a-f]{64}$/);
    assert.deepEqual(state.rows[0].input_content_ids, []);
    assert.match(state.rows[0].input_content_hash, /^sha256:[0-9a-f]{64}$/);
    assert.match(state.rows[0].agent_version_hash, /^sha256:[0-9a-f]{64}$/);
    assert.match(state.rows[0].prompt_hash, /^[0-9a-f]{64}$/);
    assert.equal(state.rows[0].taxonomy_version, "qy-taxonomy-v1");
    assert.equal(String(state.rows[0].agent_config_id), String(agentConfig.config_id));
    assert.equal(String(state.rows[0].prompt_template_id), String(agentConfig.prompt_template_id));

    const event = await pool.query(
      `SELECT observation.outcome,observation.plan_id,
              observation.result_summary_json,
              outbox.payload_json->>'plan_id' AS event_plan_id,
              outbox.payload_json->'payload' AS payload
       FROM crawler.crawl_observations observation
       JOIN crawler.crawler_outbox outbox USING (observation_id)
       WHERE observation.channel_id=$1 AND observation.observation_kind='agent'`,
      [channelId],
    );
    assert.equal(event.rows[0].outcome, "complete");
    assert.equal(event.rows[0].plan_id, planId);
    assert.equal(event.rows[0].event_plan_id, planId);
    assert.equal(event.rows[0].payload.category_level_1, "Tech");
    assert.deepEqual(event.rows[0].payload.topic_tokens, [
      "l1:tech",
      "l2:artificial intelligence",
      "tag:ai",
      "tag:software",
    ]);
    assert.equal(event.rows[0].payload.evidence_fingerprints.length, 3);
    assert.equal(event.rows[0].payload.input_content_count, 0);
    assert.equal(event.rows[0].payload.input_content_hash, state.rows[0].input_content_hash);
    assert.equal(event.rows[0].payload.agent_version_hash, state.rows[0].agent_version_hash);
    assert.equal(event.rows[0].result_summary_json.output_hash, state.rows[0].current_output_hash);
    assert.equal(
      event.rows[0].result_summary_json.input_content_hash,
      state.rows[0].input_content_hash,
    );
    assert.equal(
      event.rows[0].result_summary_json.agent_version_hash,
      state.rows[0].agent_version_hash,
    );
    assert.equal(event.rows[0].payload.metrics_json, undefined);
  } finally {
    await pool.query("DELETE FROM crawler.channels WHERE channel_id=$1", [channelId]).catch(() => {});
    if (agentConfig) {
      await pool.query("DELETE FROM crawler.agent_configs WHERE config_id=$1", [agentConfig.config_id]).catch(() => {});
      await pool.query(
        "DELETE FROM crawler.agent_prompt_templates WHERE template_id=$1",
        [agentConfig.prompt_template_id],
      ).catch(() => {});
    }
    await pool.end();
  }
});

test("terminal incremental Agent failure closes the Run and emits a failed Observation", {
  skip: !integrationUrl,
}, async () => {
  const pool = new Pool({ connectionString: integrationUrl, max: 4 });
  const suffix = randomUUID().replaceAll("-", "");
  const channelId = `UCv16agentfail${suffix}`;
  const runId = `incremental:agent-fail:${suffix}`;
  const planId = randomUUID();
  const batchId = `incremental-agent:failure:${suffix}`;
  const withTransaction = async (action) => {
    const client = await pool.connect();
    try {
      await client.query("BEGIN");
      const result = await action(client);
      await client.query("COMMIT");
      return result;
    } catch (error) {
      await client.query("ROLLBACK");
      throw error;
    } finally {
      client.release();
    }
  };
  const store = new IncrementalAgentResultStore({ withTransaction, maxAttempts: 1 });

  try {
    await seed(pool, { channelId, runId, planId, batchId });
    const claimed = await store.claim(agentJob(batchId, channelId, planId, runId));
    const failed = await store.fail({
      batchId,
      request: claimed.requests[0],
      row: {
        input_url: `https://www.youtube.com/channel/${channelId}`,
        country_required: true,
      },
      agentConfig: { config_id: null, model: "agent-test" },
      error: new Error("temporary agent failure"),
    });
    assert.equal(failed.outcome, "failed");

    const state = await pool.query(
      `SELECT request.status AS request_status,request.batch_id,run.status AS run_status,
              run.result_json #>> '{domains,agent,status}' AS domain_status,
              observation.outcome
       FROM crawler.agent_refresh_requests request
       JOIN crawler.channel_runs run ON run.run_id=request.run_id
       JOIN crawler.crawl_observations observation ON observation.run_id=run.run_id
       WHERE request.channel_id=$1 AND observation.observation_kind='agent'`,
      [channelId],
    );
    assert.deepEqual(state.rows[0], {
      request_status: "failed",
      batch_id: null,
      run_status: "failed",
      domain_status: "failed",
      outcome: "failed",
    });
  } finally {
    await pool.query("DELETE FROM crawler.channels WHERE channel_id=$1", [channelId]).catch(() => {});
    await pool.end();
  }
});
