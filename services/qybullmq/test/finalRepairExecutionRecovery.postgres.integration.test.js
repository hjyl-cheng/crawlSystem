import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import test from "node:test";
import pg from "pg";

import {
  claimContentDetailExecution,
  contentDetailExecutionFence,
} from "../src/contentDetailExecutionFence.js";
import {
  FinalRepairExecutionRecovery,
  PostgresFinalRepairExecutionRecoveryRepository,
} from "../src/finalRepairExecutionRecovery.js";
import { recoverablePreparedFinalDetailRepairSql } from "../src/finalRepairCandidatePolicy.js";

const { Client } = pg;
const databaseUrl = String(process.env.MANAGED_JOB_TEST_DATABASE_URL ?? "").trim();

function assertDedicatedLocalTestDatabase(value) {
  const url = new URL(value);
  assert.match(decodeURIComponent(url.pathname), /test/i);
  assert.ok(["127.0.0.1", "localhost", "::1"].includes(url.hostname));
}

async function transaction(client, action) {
  await client.query("BEGIN");
  try {
    const result = await action(client);
    await client.query("COMMIT");
    return result;
  } catch (error) {
    await client.query("ROLLBACK").catch(() => {});
    throw error;
  }
}

test("Final Repair atomically replaces a terminal Snapshot Detail lease", {
  skip: databaseUrl ? false : "MANAGED_JOB_TEST_DATABASE_URL is not configured",
  timeout: 30_000,
}, async (t) => {
  assertDedicatedLocalTestDatabase(databaseUrl);
  const client = new Client({ connectionString: databaseUrl, connectionTimeoutMillis: 3000 });
  await client.connect();
  t.after(async () => {
    await client.query("DROP SCHEMA IF EXISTS publication CASCADE").catch(() => {});
    await client.query("DROP SCHEMA IF EXISTS crawler CASCADE").catch(() => {});
    await client.end().catch(() => {});
  });
  await client.query("DROP SCHEMA IF EXISTS publication CASCADE");
  await client.query("DROP SCHEMA IF EXISTS crawler CASCADE");
  const schema = await readFile(new URL("../src/schema.sql", import.meta.url), "utf8");
  await client.query(schema);

  const channelId = "UCfinalRepairLease";
  const runId = "run:final-repair-lease";
  const cycleId = "cycle:final-repair-lease";
  const ownerJobId = "channel-snapshot__cycle__UCfinalRepairLease__g2";
  const repairJobId = "final-repair__run_final-repair-lease__1";
  await transaction(client, async (tx) => {
    await tx.query(
      `INSERT INTO crawler.channels (
         channel_id,channel_url,title,subscriber_count,status,ready_for_agent,
         agent_status,latest_run_id
       ) VALUES ($1,$2,'Final Repair lease',2000,'active',true,'done',$3)`,
      [channelId, `https://www.youtube.com/channel/${channelId}`, runId],
    );
    await tx.query(
      `INSERT INTO crawler.channel_runs (
         run_id,channel_id,status,crawl_mode,detail_status,expected_content_count,
         started_at,result_json,detail_job_epoch,detail_active_job_id,
         detail_active_job_attempt,detail_active_scope_key,detail_active_job_epoch
       ) VALUES (
         $1,$2,'waiting_detail','full','queued',1,now(),$3::jsonb,0,$4,1,$5,0
       )`,
      [runId, channelId, JSON.stringify({ pipeline_cycle_id: cycleId }), ownerJobId, "snapshot-scope"],
    );
  });
  const contentCandidateId = Number((await client.query(
    `INSERT INTO crawler.content_candidates (
       run_id,channel_id,source_content_id,position,title,source_url,
       content_type,type_status,type_source,detail_status,api_status,
       missing_fields,attempts,result_json,finished_at
     ) VALUES (
       $1,$2,'video-with-page',1,'Video with page',
       'https://www.youtube.com/watch?v=video-with-page',
       'video','resolved','youtube_watch_canonical','done','unavailable',
       ARRAY['comments_first_page']::text[],3,$3::jsonb,now()
     ) RETURNING candidate_id`,
    [
      runId,
      channelId,
      JSON.stringify({
        access: { access_status: "public" },
        detail: { comment_count: 1, comment_count_status: "exact" },
      }),
    ],
  )).rows[0].candidate_id);
  const storedContentKey = `${channelId}:video:stored-video`;
  await client.query(
    `INSERT INTO crawler.contents (
       content_key,channel_id,run_id,content_type,content_type_source,
       source_content_id,title,url,published_at,published_at_status,
       published_at_precision,duration_seconds,duration_status,view_count_text,
       view_count_status,like_count,like_count_status,comment_count,
       comment_count_status,comments_disabled,comments_first_page,access_status
     ) VALUES (
       $1,$2,$3,'video','youtube_watch_canonical','stored-video','Stored video',
       'https://www.youtube.com/watch?v=stored-video',now(),'exact','second',90,
       'exact','100','exact',3,'exact',1,'exact',false,$4::jsonb,'public'
     )`,
    [
      storedContentKey,
      channelId,
      runId,
      JSON.stringify({
        version: 1,
        sort: "TOP_COMMENTS",
        returned_count: 1,
        comments: [{ comment_id: "stored-comment", text: "Already stored" }],
      }),
    ],
  );
  const storedCandidateId = Number((await client.query(
    `INSERT INTO crawler.content_candidates (
       run_id,channel_id,source_content_id,position,title,source_url,content_key,
       content_type,type_status,type_source,detail_status,api_status,
       missing_fields,attempts,result_json,error_message,finished_at
     ) VALUES (
       $1,$2,'stored-video',2,'Stored video',
       'https://www.youtube.com/watch?v=stored-video',$3,
       'video','resolved','youtube_watch_canonical','done','unavailable',
       ARRAY['comments_first_page']::text[],3,$4::jsonb,'missing comments_first_page',now()
     ) RETURNING candidate_id`,
    [
      runId,
      channelId,
      storedContentKey,
      JSON.stringify({
        access: { access_status: "public" },
        detail: { comment_count: 1, comment_count_status: "exact" },
      }),
    ],
  )).rows[0].candidate_id);

  const repository = new PostgresFinalRepairExecutionRecoveryRepository({
    withTransaction: (action) => transaction(client, action),
  });
  const recovery = new FinalRepairExecutionRecovery({
    repository,
    findJob: async (jobId) => ({
      id: jobId,
      async getState() { return "completed"; },
    }),
  });
  const prepared = await recovery.prepareDetailDispatch({
    runId,
    channelId,
    repairRound: 1,
    jobId: repairJobId,
  });

  assert.deepEqual(prepared, {
    data: { content_detail_job_epoch: 1 },
    lease_replaced: true,
  });
  assert.equal(await recovery.isBusinessComplete({ runId, channelId }), false);
  const state = (await client.query(
    `SELECT run.detail_job_epoch,run.detail_active_job_id,
            run.result_json->'final_repair_recovery_intent' AS recovery_intent,
            candidate.detail_status,candidate.api_status,candidate.missing_fields,
            candidate.attempts,candidate.finished_at,
            candidate.result_json->'final_repair_dispatch' AS final_repair_dispatch
     FROM crawler.channel_runs run
     JOIN crawler.content_candidates candidate ON candidate.candidate_id=$2
     WHERE run.run_id=$1`,
    [runId, contentCandidateId],
  )).rows[0];
  assert.equal(Number(state.detail_job_epoch), 1);
  assert.equal(state.detail_active_job_id, null);
  assert.equal(state.recovery_intent.status, "prepared");
  assert.equal(state.recovery_intent.replaced_job_id, ownerJobId);
  assert.equal(state.detail_status, "queued");
  assert.equal(state.api_status, "not_needed");
  assert.deepEqual(state.missing_fields, []);
  assert.equal(Number(state.attempts), 0);
  assert.equal(state.finished_at, null);
  assert.equal(state.final_repair_dispatch.job_id, repairJobId);
  assert.equal(Number(state.final_repair_dispatch.content_detail_job_epoch), 1);
  const storedCandidate = (await client.query(
    `SELECT detail_status,api_status,missing_fields,attempts,error_message,finished_at,
            result_json#>'{detail,comments_first_page}' AS comments_first_page,
            result_json#>>'{detail,comments_first_page_source}' AS comments_first_page_source
     FROM crawler.content_candidates
     WHERE candidate_id=$1`,
    [storedCandidateId],
  )).rows[0];
  assert.equal(storedCandidate.detail_status, "done");
  assert.equal(storedCandidate.api_status, "done");
  assert.deepEqual(storedCandidate.missing_fields, []);
  assert.equal(Number(storedCandidate.attempts), 3);
  assert.equal(storedCandidate.error_message, null);
  assert.ok(storedCandidate.finished_at);
  assert.equal(storedCandidate.comments_first_page.comments[0].comment_id, "stored-comment");
  assert.equal(storedCandidate.comments_first_page_source, "stored_content_reconciliation");
  const storedDispatch = (await client.query(
    `SELECT result_json#>>'{final_repair_dispatch,status}' AS status,
            result_json#>>'{final_repair_dispatch,job_id}' AS job_id
     FROM crawler.content_candidates
     WHERE candidate_id=$1`,
    [storedCandidateId],
  )).rows[0];
  assert.equal(storedDispatch.status, "prepared");
  assert.equal(storedDispatch.job_id, repairJobId);

  const fence = contentDetailExecutionFence({
    id: repairJobId,
    attemptsStarted: 1,
    data: {
      run_id: runId,
      channel_id: channelId,
      pipeline_cycle_id: cycleId,
      content_detail_job_epoch: 1,
    },
  });
  const claimed = await transaction(client, (tx) => claimContentDetailExecution(tx, fence));
  assert.equal(claimed.runId, runId);
  await client.query(
    `UPDATE crawler.content_candidates
     SET detail_status='done',api_status='done',missing_fields='{}'::text[],finished_at=now()
     WHERE candidate_id=$1`,
    [contentCandidateId],
  );
  await client.query(
    `UPDATE crawler.channel_runs
     SET detail_status='done',publication_finalized_status='ready_auto',
         publication_finalized_at=now()
     WHERE run_id=$1`,
    [runId],
  );
  const recoverableAfterClosure = await client.query(
    `SELECT count(*)::int AS count
     FROM crawler.content_candidates candidate
     JOIN crawler.channel_runs run ON run.run_id=candidate.run_id
     WHERE run.run_id=$1
       AND ${recoverablePreparedFinalDetailRepairSql("candidate", "run")}`,
    [runId],
  );
  assert.equal(Number(recoverableAfterClosure.rows[0].count), 0);
  assert.equal(await recovery.isBusinessComplete({ runId, channelId }), true);
});
