// Explicit metadata-only upgrade; never invoked by collector startup.
import pg from 'pg';
import {readFile} from 'node:fs/promises';
const expected=process.env.EXPECTED_CRAWLER_DATABASE;
if(!expected || !process.env.REMOTE_NODE_DATABASE_URL)throw Error('explicit database identity required');
const pool=new pg.Pool({connectionString:process.env.REMOTE_NODE_DATABASE_URL,max:1,application_name:'intake-control-upgrade'});
const client=await pool.connect();
try{
  if((await client.query('SELECT current_database() AS name')).rows[0].name!==expected)throw Error('database identity mismatch');
  if(!process.argv.includes('--apply'))console.log(JSON.stringify({database:expected,applied:false}));
  else{
    await client.query("BEGIN; SET LOCAL lock_timeout='5s'; SET LOCAL statement_timeout='15s'");
    await client.query(await readFile(new URL('../src/remoteNodes/intakeControlSchema.sql',import.meta.url),'utf8'));
    // Initialize once from existing operator intent; never enable any slot.
    await client.query(`INSERT INTO remote_ingestion.intake_controls(node_key,configured_count,intake_enabled)
      SELECT d.node_id::text,CASE WHEN desired.n>0 THEN desired.n ELSE d.worker_count END,desired.n>0
      FROM remote_ingestion.node_deployments d LEFT JOIN remote_ingestion.node_intake_requests r USING(node_id)
      CROSS JOIN LATERAL (SELECT COALESCE(cardinality(r.selected_slots),(SELECT count(*)::int FROM remote_ingestion.worker_connections w
        WHERE w.node_id=d.node_id AND w.retired_at IS NULL AND w.activation_requested)) AS n) desired
      ON CONFLICT(node_key) DO NOTHING`);
    if((await client.query("SELECT to_regclass('remote_ingestion.local_incremental_workers') AS name")).rows[0].name){
      await client.query(`INSERT INTO remote_ingestion.intake_controls(node_key,configured_count,intake_enabled)
        SELECT 'local-center',CASE WHEN count(*) FILTER(WHERE activation_requested)>0
          THEN count(*) FILTER(WHERE activation_requested) ELSE count(*) END,count(*) FILTER(WHERE activation_requested)>0
        FROM remote_ingestion.local_incremental_workers ON CONFLICT(node_key) DO NOTHING`);
    }
    await client.query('COMMIT');console.log(JSON.stringify({database:expected,applied:true}));
  }
}catch(error){await client.query('ROLLBACK').catch(()=>{});throw error;}
finally{client.release();await pool.end();}
