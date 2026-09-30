import test from 'node:test';
import assert from 'node:assert/strict';
import pg from 'pg';
import {candidateFilters,candidateListSql,loadCandidateList,loadCandidateStatistics,readCandidatesWithin} from './candidateChannels.js';
import {createStatisticsCache} from './statisticsCache.js';

const url=process.env.DASHBOARD_STATISTICS_TEST_URL;
test('candidate reads exclude legacy migration rows and use the batch index instead of scanning them',{skip:!url,timeout:60000},async t=>{
  const target=new URL(url);assert.ok(['127.0.0.1','localhost','[::1]'].includes(target.hostname));assert.match(target.pathname,/test/);
  const pool=new pg.Pool({connectionString:url,max:2});t.after(()=>pool.end());
  await pool.query(`DROP SCHEMA IF EXISTS crawler CASCADE;CREATE SCHEMA crawler;
    CREATE TABLE crawler.query_dispatch_batches(dispatch_batch_id text PRIMARY KEY,status text NOT NULL DEFAULT 'running',
      started_at timestamptz NOT NULL DEFAULT now(),finished_at timestamptz);
    CREATE TABLE crawler.query_pages(page_id text PRIMARY KEY,dispatch_batch_id text,status text,page_no int);
    CREATE INDEX idx_crawler_query_pages_dispatch_status ON crawler.query_pages(dispatch_batch_id,status,page_no);
    CREATE TABLE crawler.channel_candidates(candidate_id bigserial PRIMARY KEY,
      dispatch_batch_id text NOT NULL REFERENCES crawler.query_dispatch_batches(dispatch_batch_id),channel_id text NOT NULL,channel_url text NOT NULL,
      handle text,title text,avatar_url text,search_subscriber_count bigint,search_subscriber_count_text text,is_verified boolean,
      priority int NOT NULL DEFAULT 100,status text NOT NULL DEFAULT 'discovered',source_json jsonb NOT NULL DEFAULT '{}',
      reject_reason text,error_message text,validation_finished_at timestamptz,accepted_at timestamptz,created_at timestamptz NOT NULL DEFAULT now(),
      UNIQUE(dispatch_batch_id,channel_id));
    CREATE INDEX idx_crawler_channel_candidates_claim ON crawler.channel_candidates(dispatch_batch_id,status,priority DESC,created_at ASC);`);
  await pool.query(`INSERT INTO crawler.query_dispatch_batches(dispatch_batch_id,started_at) VALUES
      ('legacy-migration','2026-08-01Z'),('query-old','2026-09-28Z'),('query-new','2026-09-30Z');
    INSERT INTO crawler.query_pages VALUES ('p-old-1','query-old','done',1),('p-new-1','query-new','done',1),('p-new-2','query-new','running',2);
    INSERT INTO crawler.channel_candidates(dispatch_batch_id,channel_id,channel_url,status,source_json,created_at)
      SELECT 'legacy-migration','UClegacy'||g,'https://youtube.com/channel/UClegacy'||g,'discovered',
        jsonb_build_object('source','legacy_results_db','legacy',repeat('x',200)),'2026-08-01Z'
      FROM generate_series(1,40000) g;
    INSERT INTO crawler.channel_candidates(dispatch_batch_id,channel_id,channel_url,title,status,reject_reason,source_json,created_at) VALUES
      ('query-old','UCold','https://youtube.com/channel/UCold','Old','accepted',NULL,
        '{"source":"youtube_search_discovery","query_text":"receitas faceis","page_id":"p-old-1","rank_position":3}','2026-09-28 10:00Z'),
      ('query-new','UCnew1','https://youtube.com/channel/UCnew1','New one','queued',NULL,
        '{"source":"youtube_search_discovery","query_text":"futebol","page_id":"p-new-2","rank_position":1}','2026-09-30 02:00Z'),
      ('query-new','UCnew2','https://youtube.com/channel/UCnew2','New two','rejected','below_min_subscribers',
        '{"source":"youtube_search_discovery","query_text":"futebol","page_id":"p-new-1","rank_position":"bad"}','2026-09-30 03:00Z'),
      ('query-new','UCother','https://youtube.com/channel/UCother','Manual','discovered',NULL,'{"source":"manual"}','2026-09-30 04:00Z');
    ANALYZE crawler.query_dispatch_batches;ANALYZE crawler.query_pages;ANALYZE crawler.channel_candidates;`);
  const read=(sql,args)=>readCandidatesWithin(pool,sql,args);
  const list=async query=>(await loadCandidateList({read,filters:candidateFilters(query)}));

  const all=await list({});
  assert.deepEqual(all.candidates.map(c=>c.channel_id),['UCnew2','UCnew1','UCold']);
  assert.deepEqual(all.batches.map(b=>b.dispatch_batch_id),['query-new','query-old'],'legacy batches are not Query batches');
  assert.equal(all.candidates[1].page_no,2);assert.equal(all.candidates[1].rank_position,1);assert.equal(all.candidates[0].rank_position,null);
  assert.deepEqual((await list({batch:'query-old'})).candidates.map(c=>c.channel_id),['UCold']);
  assert.deepEqual((await list({status:'rejected'})).candidates.map(c=>c.reject_reason),['below_min_subscribers']);
  assert.deepEqual((await list({q:'FACEIS'})).candidates.map(c=>c.channel_id),['UCold']);
  assert.deepEqual((await list({q:'%'})).candidates,[],'LIKE wildcards are literal');
  // 2026-09-30 02:00Z is 10:00 Beijing on 09-30; 2026-09-28 10:00Z is 18:00 Beijing on 09-28.
  assert.deepEqual((await list({from:'2026-09-29'})).candidates.map(c=>c.channel_id),['UCnew2','UCnew1']);
  assert.deepEqual((await list({to:'2026-09-28'})).candidates.map(c=>c.channel_id),['UCold']);
  const paged=await list({limit:'2',offset:'0'});assert.equal(paged.hasNext,true);
  assert.equal((await list({limit:'2',offset:'2'})).hasNext,false);
  assert.equal((await list({batch:'legacy-migration'})).candidates.length,0);

  const readStatistics=(sql,args)=>readCandidatesWithin(pool,sql,args,'60s');
  const stats=await loadCandidateStatistics({readStatistics,cache:createStatisticsCache(),filters:candidateFilters({status:'queued'})});
  assert.equal(stats.all,3);assert.equal(stats.pending,1);assert.equal(stats.total,1);assert.equal(stats.byStatus.accepted,1);

  const plan=candidateListSql(candidateFilters({}),['query-new','query-old']);
  const explain=(await pool.query(`EXPLAIN ${plan.sql}`,plan.args)).rows.map(r=>r['QUERY PLAN']).join('\n');
  assert.doesNotMatch(explain,/Seq Scan on channel_candidates/,explain);
});
