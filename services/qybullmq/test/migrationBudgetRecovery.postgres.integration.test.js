import assert from 'node:assert/strict';
import test from 'node:test';
import {readFile} from 'node:fs/promises';
import pg from 'pg';
import {crawlerRuntimeSchema} from '../src/publicationCurrentSchema.js';
import {MigrationSystemRetryRecoveryReconciler} from '../src/migrationSystemRetryRecovery.js';
import {YOUTUBEJS_FULL_CRAWL_FETCH_CONTRACT} from '../src/fullCrawlFetchContract.js';
import {queuesByRole} from '../src/queues.js';
import {sharedCrawlerSchedulerActivationAdmission} from '../src/migrationSystemRetryAdmission.js';

const url = process.env.MIGRATION_BUDGET_TEST_URL;
test('budget-terminal recovery settles active and legacy completions without reopening the admission blocker', {skip: !url}, async () => {
  assert.equal(new URL(url).pathname, '/migration_budget_recovery_test');
  assert.ok(['localhost','127.0.0.1'].includes(new URL(url).hostname));
  const pool = new pg.Pool({connectionString:url});
  const query = pool.query.bind(pool);
  const withTransaction = async fn => {
    const c = await pool.connect();
    try {await c.query('BEGIN');const result=await fn(c);await c.query('COMMIT');return result;}
    catch(error){await c.query('ROLLBACK');throw error;}finally{c.release();}
  };
  let retried=0;
  const queues={[queuesByRole.channelCrawl]:{getJob:async()=>({name:'channel-snapshot',data:{run_id:'run-budget',candidate_id:1,dispatch_generation:2},getState:async()=>'completed',retry:async()=>{retried+=1;}})}};
  const reconciler = new MigrationSystemRetryRecoveryReconciler({query,withTransaction,queues});
  try {
    await query('DROP SCHEMA IF EXISTS crawler CASCADE');
    await query(crawlerRuntimeSchema(await readFile(new URL('../src/schema.sql',import.meta.url),'utf8')));
    await query(`
      INSERT INTO crawler.query_dispatch_batches(dispatch_batch_id,pipeline_cycle_id,status) VALUES('b','b','completed');
      INSERT INTO crawler.channel_candidates(candidate_id,dispatch_batch_id,pipeline_cycle_id,channel_id,channel_url,status,snapshot_dispatch_generation)
        VALUES(1,'b','b','UCbudget','https://www.youtube.com/channel/UCbudget','accepted',2);
      INSERT INTO crawler.migration_channel_intents(source_id,source_database,source_database_oid,source_candidate_id,channel_id,source_snapshot,snapshot_sha256,target_candidate_id,first_dispatch_batch_id)
        VALUES('test',current_database(),1,1,'UCbudget','{}',repeat('a',64),1,'b');
      INSERT INTO crawler.migration_system_retry_items(migration_intent_id,candidate_id,failed_dispatch_batch_id,failed_dispatch_generation,failed_job_id,failed_job_attempt,failure_code,failure_category,status)
        SELECT migration_intent_id,1,'b',2,'original',1,'LEASE_CONFLICT','lease','retrying' FROM crawler.migration_channel_intents;
    `);
    await withTransaction(async c => {
      await c.query(`INSERT INTO crawler.channels(channel_id,channel_url,status,latest_run_id) VALUES('UCbudget','https://www.youtube.com/channel/UCbudget','active','run-budget')`);
      await c.query(`INSERT INTO crawler.channel_runs(run_id,channel_id,candidate_id,status,crawl_mode,detail_status,finished_at,result_json)
        VALUES('run-budget','UCbudget',1,'failed','full','failed',now(),'{"dispatch_batch_id":"b","job_id":"original","proxy_control":{"status":"business_run_budget_exhausted","business_run_id":"run-budget"}}')`);
    });
    await query(`UPDATE crawler.channel_runs SET result_json=result_json||jsonb_build_object('fetch_contract',$1::jsonb)`,[JSON.stringify(YOUTUBEJS_FULL_CRAWL_FETCH_CONTRACT)]);
    for (const status of ['retrying','dispatched','resolved']) {
      await query(`UPDATE crawler.migration_system_retry_items SET status=$1,failed_dispatch_generation=CASE WHEN $1='dispatched' THEN 1 ELSE 2 END,retry_dispatch_generation=CASE WHEN $1='dispatched' THEN 2 ELSE NULL END,recovery_run_id=NULL,resolution=CASE WHEN $1='resolved' THEN 'job_completed' ELSE NULL END`,[status]);
      const result=await reconciler.reconcileAvailable({limit:10});
      assert.equal(retried,0,'must not retry a completed Job whose business Run exhausted its budget');
      assert.equal(result.resolved,1,status);
      assert.equal(result.legacyReopened,0);
      assert.equal(result.terminalJobsRequeued,0);
      assert.equal((await query('SELECT resolution FROM crawler.migration_system_retry_items')).rows[0].resolution,'recovery_business_run_budget_exhausted');
      assert.equal((await sharedCrawlerSchedulerActivationAdmission(pool)).allowed,true);
      assert.equal((await reconciler.reconcileAvailable({limit:10})).resolved,0);
    }
    // Each mutation happens AFTER loading: settlement must recheck persisted fences.
    const guards = [
      [`UPDATE crawler.channel_candidates SET snapshot_dispatch_generation=3`,`UPDATE crawler.channel_candidates SET snapshot_dispatch_generation=2`],
      [`UPDATE crawler.channel_candidates SET snapshot_active_job_id='live',snapshot_active_job_attempt=1`,`UPDATE crawler.channel_candidates SET snapshot_active_job_id=NULL,snapshot_active_job_attempt=NULL`],
      [`UPDATE crawler.channel_runs SET detail_active_job_id='live',detail_active_job_attempt=1,detail_active_scope_key='test',detail_active_job_epoch=0`,`UPDATE crawler.channel_runs SET detail_active_job_id=NULL,detail_active_job_attempt=NULL,detail_active_scope_key=NULL,detail_active_job_epoch=NULL`],
      [`UPDATE crawler.channel_runs SET result_json=result_json-'proxy_control'`,`UPDATE crawler.channel_runs SET result_json=result_json||'{"proxy_control":{"status":"business_run_budget_exhausted","business_run_id":"run-budget"}}'`],
      [`UPDATE crawler.channel_runs SET result_json=jsonb_set(result_json,'{dispatch_batch_id}','"new"')`,`UPDATE crawler.channel_runs SET result_json=jsonb_set(result_json,'{dispatch_batch_id}','"b"')`],
      [`UPDATE crawler.migration_system_retry_items SET failed_dispatch_generation=3`,`UPDATE crawler.migration_system_retry_items SET failed_dispatch_generation=2`],
      [`UPDATE crawler.channel_runs SET status='waiting_detail',detail_status='queued'`,`UPDATE crawler.channel_runs SET status='failed',detail_status='failed'`],
    ];
    for (const [mutate,restore] of guards) {
      await query(`UPDATE crawler.migration_system_retry_items SET status='retrying',resolution=NULL,recovery_run_id=NULL`);
      const [row]=await reconciler.loadRecoveries(10);
      await query(mutate);
      assert.equal(await reconciler.resolveBudgetExhaustedOutcome(row),false,mutate);
      assert.equal((await sharedCrawlerSchedulerActivationAdmission(pool)).allowed,false);
      await query(restore);
    }
    assert.equal((await reconciler.reconcileAvailable({limit:10})).resolved,1);
  } finally {await pool.end();}
});
