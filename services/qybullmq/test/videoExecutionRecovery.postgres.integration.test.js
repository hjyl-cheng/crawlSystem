import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import { randomUUID } from 'node:crypto';
import test from 'node:test';
import pg from 'pg';
import { runWithChannelExecution } from '../src/channelExecutionContext.js';
import { executeIncrementalYoutubeJsVideo, incrementalYoutubeJsVideoTargetHash, INCREMENTAL_YOUTUBEJS_VIDEO_SETTLE_SQL } from '../src/incrementalYoutubeJsVideo.js';
import { claimIncrementalVideoExecution, lockIncrementalVideoExecution } from '../src/incrementalVideoExecution.js';
import { withVideoApiReplay } from '../src/videoApiContinuation.js';
import { incrementalYoutubeJsVideoCheckpointSchemaBlock } from '../src/incrementalYoutubeJsVideoSchema.js';

const url = process.env.VIDEO_EXECUTION_RECOVERY_TEST_DATABASE_URL;

async function fixture(t, { previousFinished = true, managed = true } = {}) {
  const client = new pg.Client({ connectionString: url });
  await client.connect();
  t.after(async () => { await client.query('ROLLBACK'); await client.end(); });
  assert.equal((await client.query('SELECT current_database() AS name')).rows[0].name, 'video_execution_recovery_test');
  await client.query('BEGIN');
  await client.query('CREATE SCHEMA crawler');
  await client.query(`CREATE TABLE crawler.channel_runs (
    run_id text PRIMARY KEY,channel_id text,plan_id uuid,crawl_mode text,task_mask jsonb,result_json jsonb,
    status text,detail_job_epoch bigint NOT NULL DEFAULT 0,detail_active_job_id text,
    detail_active_job_attempt bigint,detail_active_job_epoch bigint,detail_active_scope_key text,updated_at timestamptz);
    CREATE TABLE crawler.channel_execution_attempts (
      attempt_id text PRIMARY KEY,run_id text,business_run_id text,channel_id text,queue_name text,job_id text,
      attempt_number bigint,job_attempt bigint,dispatch_generation bigint,status text,finished_at timestamptz,
      workload_scope text,identity_changed boolean DEFAULT false,task_id text);
    CREATE TABLE crawler.crawl_observations (observation_id uuid PRIMARY KEY)`);
  await client.query(incrementalYoutubeJsVideoCheckpointSchemaBlock(await readFile(new URL('../src/schema.sql', import.meta.url), 'utf8')));
  const planId = randomUUID();
  const plan = { plan_id: planId,channel_id: 'UC-recovery',job_id: 'incremental-recovery',dispatch_generation: 1,
    plan_day: '2026-09-12',scheduled_at: '2026-09-12T01:00:00Z',planner_config_version: 'video-plan-1',
    task_mask: { about: false,video: true,agent: false },capacity: { version: 'test',factor: 1,player_cap: 3,next_cap: 0 } };
  const runId = `incremental:${planId}`;
  await client.query(`INSERT INTO crawler.channel_runs(run_id,channel_id,plan_id,crawl_mode,task_mask,result_json,status)
    VALUES($1,$2,$3,'incremental',$4,'{}','running')`, [runId,plan.channel_id,planId,plan.task_mask]);
  for (const [id,number,finished] of [['old',1,previousFinished],['new',2,false]]) {
    await client.query(`INSERT INTO crawler.channel_execution_attempts
      (attempt_id,run_id,business_run_id,channel_id,queue_name,job_id,attempt_number,job_attempt,dispatch_generation,status,finished_at,workload_scope)
      VALUES($1,$2,$2,$3,'youtube-channel-incremental',$4,$5::bigint,$5::bigint-1,1,$6,CASE WHEN $7 THEN now() ELSE NULL END,'qy-production')`,
    [id,runId,plan.channel_id,plan.job_id,number,finished ? 'aborted' : 'running',finished]);
  }
  if (managed) await client.query("UPDATE crawler.channel_execution_attempts SET task_id=attempt_id,attempt_id='channel-attempt:'||attempt_id");
  const identity = id => managed ? `channel-attempt:${id}` : id;
  const entries = ['already-saved','unfinished'].map((id,ordinal) => ({ id,position: ordinal + 1,title: id,content_type: 'video' }));
  const items = entries.map((entry,ordinal) => ({ phase: 'first_seen',ordinal,video_id: entry.id,target_json: entry }));
  await client.query(`INSERT INTO crawler.incremental_youtubejs_video_batches
    (run_id,cycle_key,plan_id,channel_id,status,cycle_observed_at,started_at,scan_json,anchors_json,discovery_entries_json,
    pending_deferred_video_ids,sampling_plan_json,sampling_config_json,target_hash,first_seen_checkpoint_status,first_seen_checkpoints_json)
    VALUES($1,'base',$2,$3,'fetching',now(),now(),$4,'[]',$5,'[]','{"rows":[]}','{}',$6,'pending','[]')`,
  [runId,planId,plan.channel_id,{ complete: true,entries },JSON.stringify(entries),incrementalYoutubeJsVideoTargetHash(items)]);
  await client.query(`INSERT INTO crawler.incremental_youtubejs_video_items(run_id,cycle_key,phase,ordinal,video_id,target_json)
    SELECT $1,'base',item.phase,item.ordinal,item.video_id,item.target_json FROM jsonb_to_recordset($2::jsonb)
    AS item(phase text,ordinal integer,video_id text,target_json jsonb)`, [runId,JSON.stringify(items)]);
  await client.query(`UPDATE crawler.incremental_youtubejs_video_items SET status='captured',detail_json='{"id":"already-saved"}',
    field_status_json='{}',captured_at=now() WHERE video_id='already-saved'`);
  const oldToken = randomUUID();
  await client.query(`UPDATE crawler.incremental_youtubejs_video_items SET status='claimed',claim_token=$1,
    claim_expires_at=now()+interval '5 minutes' WHERE video_id='unfinished'`, [oldToken]);
  const captured = (await client.query("SELECT * FROM crawler.incremental_youtubejs_video_items WHERE video_id='already-saved'")).rows[0];
  const fetched = [];
  const stop = new Error('test stops at first resumed network request');
  const execute = (attemptId = 'new', options = {}) => {
    const signal = options.signal ?? AbortSignal.timeout(2000);
    return runWithChannelExecution({ attempt_id: identity(attemptId),abort_signal: signal },
    () => executeIncrementalYoutubeJsVideo({ plan,runId,query: client.query.bind(client),
      withTransaction: action => { if (options.rejectCancelledTransactions) signal.throwIfAborted(); return action(client); },
      startedAt: '2026-09-12T01:00:00Z',getChannelSnapshot: async () => { throw Error('must reuse frozen uploads'); },
      fetchDetail: options.fetchDetail ?? (async id => { fetched.push(id); throw stop; }) }));
  };
  return { client,plan,runId,oldToken,captured,fetched,stop,execute,identity };
}

test('a new incremental execution recovers an unfinished five-minute claim immediately and preserves captured videos', { skip: !url }, async t => {
  const f = await fixture(t);
  await assert.rejects(f.execute(), error => error === f.stop);
  assert.deepEqual(f.fetched,['unfinished']);
  assert.deepEqual((await f.client.query("SELECT * FROM crawler.incremental_youtubejs_video_items WHERE video_id='already-saved'")).rows[0],f.captured);
});

test('without managed admission an unfinished previous execution is deferred without clearing its claim', { skip: !url }, async t => {
  const f = await fixture(t,{ previousFinished: false,managed: false });
  await assert.rejects(f.execute(), { code: 'VIDEO_EXECUTION_RECOVERY_PENDING' });
  assert.deepEqual(f.fetched,[]);
  assert.equal((await f.client.query("SELECT claim_token FROM crawler.incremental_youtubejs_video_items WHERE video_id='unfinished'")).rows[0].claim_token,f.oldToken);
});

test('a later Rota admission recovers claims after a hard exit left the old attempt running', { skip: !url }, async t => {
  const f = await fixture(t,{ previousFinished: false });
  const next = (await f.client.query('DELETE FROM crawler.channel_execution_attempts WHERE attempt_id=$1 RETURNING *',[f.identity('new')])).rows[0];
  await f.client.query("UPDATE crawler.incremental_youtubejs_video_items SET status='pending',claim_token=NULL,claim_expires_at=NULL WHERE video_id='unfinished'");
  const oldFence = await runWithChannelExecution({ attempt_id: f.identity('old') },()=>claimIncrementalVideoExecution(f.client,
    { plan: f.plan,runId: f.runId,cycleKey: 'base' }));
  await f.client.query("UPDATE crawler.incremental_youtubejs_video_items SET status='claimed',claim_token=$1,claim_expires_at=now()+interval '5 minutes' WHERE video_id='unfinished'",[f.oldToken]);
  // This is the persisted admission of the replacement. Deliberately never
  // finish the old PostgreSQL attempt, as happens on SIGKILL.
  await f.client.query('INSERT INTO crawler.channel_execution_attempts SELECT * FROM jsonb_populate_record(NULL::crawler.channel_execution_attempts,$1)',[next]);
  await assert.rejects(f.execute(),error => error === f.stop);
  assert.deepEqual(f.fetched,['unfinished']);
  await assert.rejects(lockIncrementalVideoExecution(f.client,oldFence),{ code: 'CONTENT_DETAIL_EXECUTION_FENCE_STALE' });
  assert.equal((await f.client.query(INCREMENTAL_YOUTUBEJS_VIDEO_SETTLE_SQL,
    [f.runId,'base','first_seen','unfinished',f.oldToken,'captured','{"id":"late"}','{}',null])).rowCount,0);
  assert.deepEqual((await f.client.query("SELECT * FROM crawler.incremental_youtubejs_video_items WHERE video_id='already-saved'")).rows[0],f.captured);
});

test('a later number in another Rota workload cannot authorize takeover', { skip: !url }, async t => {
  const f = await fixture(t,{ previousFinished: false });
  await f.client.query("UPDATE crawler.channel_execution_attempts SET workload_scope='unrelated' WHERE attempt_id=$1",[f.identity('new')]);
  await assert.rejects(f.execute(),{ code: 'VIDEO_EXECUTION_RECOVERY_PENDING' });
  assert.equal(f.fetched.length,0);
});

test('cancellation that blocks cleanup is recovered by the next execution and the old result cannot commit', { skip: !url }, async t => {
  const f = await fixture(t,{ previousFinished: false });
  await f.client.query("DELETE FROM crawler.channel_execution_attempts WHERE attempt_id=$1",[f.identity('new')]);
  await f.client.query("UPDATE crawler.incremental_youtubejs_video_items SET status='pending',claim_token=NULL,claim_expires_at=NULL WHERE video_id='unfinished'");
  const abort = new AbortController();
  const cancelled = new Error('remote execution cancelled');
  await assert.rejects(f.execute('old',{ signal: abort.signal,rejectCancelledTransactions: true,
    fetchDetail: async () => { abort.abort(cancelled); throw cancelled; } }),error => error === cancelled);
  const token = (await f.client.query("SELECT claim_token FROM crawler.incremental_youtubejs_video_items WHERE video_id='unfinished'")).rows[0].claim_token;
  assert.ok(token,'cancelled transaction really left an unexpired claim');
  await f.client.query("UPDATE crawler.channel_execution_attempts SET status='aborted',finished_at=now() WHERE attempt_id=$1",[f.identity('old')]);
  await f.client.query(`INSERT INTO crawler.channel_execution_attempts SELECT $1,run_id,business_run_id,channel_id,queue_name,job_id,
    2,1,dispatch_generation,'running',NULL,workload_scope,false,'new' FROM crawler.channel_execution_attempts WHERE attempt_id=$2`,[f.identity('new'),f.identity('old')]);
  await assert.rejects(f.execute(),error => error === f.stop);
  assert.deepEqual(f.fetched,['unfinished']);
  await assert.rejects(f.execute('old'), { code: 'CONTENT_DETAIL_EXECUTION_FENCE_STALE' });
  assert.equal((await f.client.query(INCREMENTAL_YOUTUBEJS_VIDEO_SETTLE_SQL,
    [f.runId,'base','first_seen','unfinished',token,'captured','{"id":"late"}','{}',null])).rowCount,0);
  assert.deepEqual((await f.client.query("SELECT * FROM crawler.incremental_youtubejs_video_items WHERE video_id='already-saved'")).rows[0],f.captured);
});

test('reentering the same live execution does not reset its active video claim', { skip: !url }, async t => {
  const f = await fixture(t);
  await assert.rejects(f.execute(),error => error === f.stop);
  const token = randomUUID();
  await f.client.query("UPDATE crawler.incremental_youtubejs_video_items SET status='claimed',claim_token=$1,claim_expires_at=now()+interval '5 minutes' WHERE video_id='unfinished'",[token]);
  await assert.rejects(f.execute(), { code: 'VIDEO_EXECUTION_RECOVERY_PENDING' });
  assert.equal(f.fetched.length,1);
  assert.equal((await f.client.query("SELECT claim_token FROM crawler.incremental_youtubejs_video_items WHERE video_id='unfinished'")).rows[0].claim_token,token);
});

test('ownership transfer and recovery of unfinished items roll back together', { skip: !url }, async t => {
  const f = await fixture(t);
  await f.client.query('SAVEPOINT takeover');
  await runWithChannelExecution({ attempt_id: f.identity('new') },()=>claimIncrementalVideoExecution(f.client,{ plan: f.plan,runId: f.runId,cycleKey: 'base' }));
  assert.equal((await f.client.query("SELECT status FROM crawler.incremental_youtubejs_video_items WHERE video_id='unfinished'")).rows[0].status,'pending');
  await f.client.query('ROLLBACK TO SAVEPOINT takeover');
  assert.equal((await f.client.query("SELECT claim_token FROM crawler.incremental_youtubejs_video_items WHERE video_id='unfinished'")).rows[0].claim_token,f.oldToken);
  assert.equal((await f.client.query('SELECT detail_active_job_id FROM crawler.channel_runs WHERE run_id=$1',[f.runId])).rows[0].detail_active_job_id,null);
});

test('local API replay reuses only the latest finished owner and rejects its delayed writes after takeover', { skip: !url }, async t => {
  const f = await fixture(t);
  await f.client.query("UPDATE crawler.channel_execution_attempts SET status='failed',finished_at=now() WHERE attempt_id=$1",[f.identity('new')]);
  const options = { plan: f.plan,runId: f.runId,cycleKey: 'base' };
  const fence = await withVideoApiReplay(()=>claimIncrementalVideoExecution(f.client,options));
  assert.equal(fence.attemptId,f.identity('new'));
  await lockIncrementalVideoExecution(f.client,fence);
  assert.equal((await f.client.query("SELECT status FROM crawler.incremental_youtubejs_video_items WHERE video_id='unfinished'")).rows[0].status,'pending');
  await f.client.query(`INSERT INTO crawler.channel_execution_attempts SELECT 'channel-attempt:third',run_id,business_run_id,channel_id,queue_name,job_id,
    3,2,dispatch_generation,'running',NULL,workload_scope,false,'third' FROM crawler.channel_execution_attempts WHERE attempt_id=$1`,[f.identity('new')]);
  await assert.rejects(withVideoApiReplay(()=>claimIncrementalVideoExecution(f.client,options)),{ code: 'CONTENT_DETAIL_EXECUTION_FENCE_STALE' });
  await runWithChannelExecution({ attempt_id: 'channel-attempt:third' },()=>claimIncrementalVideoExecution(f.client,options));
  await assert.rejects(lockIncrementalVideoExecution(f.client,fence),{ code: 'CONTENT_DETAIL_EXECUTION_FENCE_STALE' });
  await assert.rejects(runWithChannelExecution({ attempt_id: f.identity('new') },()=>withVideoApiReplay(()=>claimIncrementalVideoExecution(f.client,options))),
    { code: 'CONTENT_DETAIL_EXECUTION_FENCE_STALE' });
});
