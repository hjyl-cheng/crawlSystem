import test from 'node:test';
import assert from 'node:assert/strict';
import {readFile} from 'node:fs/promises';
import vm from 'node:vm';
import pg from 'pg';
const url=process.env.DASHBOARD_STATISTICS_TEST_URL;
test('aggregate Clock selection preserves day precedence, backlog, recovery and lifecycle scope', {skip:!url,timeout:30000},async()=>{
  const target=new URL(url);assert.ok(['127.0.0.1','localhost','[::1]'].includes(target.hostname));assert.match(target.pathname,/test/);
  const source=await readFile(new URL('./server.js',import.meta.url),'utf8');
  const start=source.indexOf('function dailyClockScopeSql('),end=source.indexOf('async function dailyClockListData',start);
  const scope=vm.runInNewContext(source.slice(start,end)+';dailyClockScopeSql');
  const c=new pg.Client({connectionString:url});await c.connect();
  try{
    await c.query('BEGIN');
    await c.query(`CREATE TEMP TABLE channel_clock_state (
      channel_id text,lifecycle_status text,about_due_day date,video_due_day date,agent_due_day date,
      about_tier text,video_tier text,agent_tier text,channel_next_run_day date,dispatch_slot int,
      estimated_request_cost int,clock_version int,policy_version int);
      CREATE TEMP TABLE channels (channel_id text,status text,channel_url text,handle text,title text,avatar_url text,subscriber_count bigint);
      CREATE TEMP TABLE daily_channel_plans (channel_id text,plan_id text,plan_day date,status text,created_at timestamptz,
        scheduled_at timestamptz,execution_deadline_at timestamptz,completed_at timestamptz,run_about boolean,run_video boolean,run_agent boolean,
        due_day date,estimated_request_cost int,source_clock_version int,policy_version int,planner_config_version int,capacity_version int);
      CREATE TEMP TABLE dispatch_outbox (plan_id text,status text);
      CREATE TEMP TABLE channel_runs (plan_id text,run_id text,status text,started_at timestamptz,finished_at timestamptz,result_json jsonb);
      INSERT INTO channel_clock_state(channel_id,lifecycle_status,channel_next_run_day,about_due_day,video_due_day,agent_due_day)
      VALUES ('precedence','active','2026-09-15','2026-09-15','2026-09-15','2026-09-15'),
        ('backlog','active','2026-09-15','2026-09-15','2026-09-15','2026-09-15'),
        ('unplanned','active','2026-09-12','2026-09-12','2026-09-14','2026-09-15'),
        ('tomorrow','active','2026-09-14','2026-09-15','2026-09-14','2026-09-15'),
        ('recovered','active','2026-09-14','2026-09-14','2026-09-14','2026-09-14'),
        ('removed','active','2026-09-12','2026-09-12','2026-09-12','2026-09-12'),
        ('dormant','dormant','2026-09-12','2026-09-12','2026-09-12','2026-09-12'),
        ('orphan','active','2026-09-13','2026-09-13','2026-09-14','2026-09-14');
      INSERT INTO channels(channel_id,status) SELECT channel_id,CASE WHEN channel_id='removed' THEN 'removed' ELSE 'active' END
        FROM channel_clock_state WHERE channel_id<>'orphan';
      INSERT INTO daily_channel_plans(channel_id,plan_id,plan_day,status,created_at,run_about,run_video,run_agent)
      VALUES ('precedence','previous','2026-09-12','running','2026-09-12',true,false,false),
        ('precedence','today-older','2026-09-13','failed','2026-09-13 01:00Z',true,true,false),
        ('precedence','today','2026-09-13','succeeded','2026-09-13 02:00Z',false,true,false),
        ('precedence','future','2026-09-14','planned','2026-09-13 03:00Z',false,false,true),
        ('backlog','old-active','2026-09-12','running','2026-09-12',true,false,false),
        ('recovered','recovered-plan','2026-09-13','failed','2026-09-13',true,true,false);
      INSERT INTO channel_runs(plan_id,run_id,status,started_at,finished_at)
        VALUES ('recovered-plan','run-recovered','done','2026-09-13 01:00Z','2026-09-13 02:00Z'),
          (NULL,'migration-unrelated','done','2026-09-13','2026-09-13');`);
    for(const day of ['2026-09-13','2026-09-14']){
      const values=[];
      for(const statistics of [false,true]){
        const sql=scope({statistics}).replaceAll(/(?:feature_clock|crawler)\./g,'pg_temp.').replace("(now() AT TIME ZONE 'UTC')::date","'2026-09-13'::date");
        values.push((await c.query(sql+' SELECT * FROM scope ORDER BY channel_id',[day])).rows);
      }
      assert.deepEqual(values[1],values[0]);
      if(day==='2026-09-13'){
        assert.deepEqual(values[1].map(r=>r.channel_id),['backlog','orphan','precedence','recovered','unplanned']);
        assert.equal(values[1].find(r=>r.channel_id==='precedence').plan_id,'today');
        assert.equal(values[1].find(r=>r.channel_id==='recovered').crawler_run_status,'done');
        assert.equal(values[1].find(r=>r.channel_id==='unplanned').run_video,false);
      }else{
        assert.deepEqual(values[1].map(r=>r.channel_id),['precedence','recovered','tomorrow']);
        assert.equal(values[1].find(r=>r.channel_id==='precedence').plan_id,'future');
      }
    }
  }finally{await c.query('ROLLBACK');await c.end();}
});
