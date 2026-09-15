// Targeted, opt-in upgrade. Never runs from a crawler Worker or startup hook.
import pg from 'pg';
import {readFile} from 'node:fs/promises';
const expected=process.env.EXPECTED_CRAWLER_DATABASE;
if(!expected || !process.env.REMOTE_NODE_DATABASE_URL)throw Error('explicit database identity required');
const pool=new pg.Pool({connectionString:process.env.REMOTE_NODE_DATABASE_URL,max:1,application_name:'remote-intake-upgrade'});
const client=await pool.connect();
try{
  if((await client.query('SELECT current_database() name')).rows[0].name!==expected)throw Error('database identity mismatch');
  const schema=await readFile(new URL('../src/remoteNodes/schema.sql',import.meta.url),'utf8');
  const indexes=[...schema.matchAll(/CREATE INDEX IF NOT EXISTS (remote_tasks_(?:target_pending|unassigned_claim|live_slot|live_lease_lookup))\s+[\s\S]*?;/g)];
  if(indexes.length!==4)throw Error('index upgrade manifest incomplete');
  if(!process.argv.includes('--apply')){console.log(JSON.stringify({database:expected,indexes:indexes.map(m=>m[1]),applied:false}));}
  else{
    // Concurrent builds may wait for old snapshots while allowing normal
    // writes. A short row-lock budget would abort that safe waiting phase.
    await client.query("SET lock_timeout='0'; SET statement_timeout='15min'");
    for(const [sql,name] of indexes){
      const existing=(await client.query('SELECT indisvalid FROM pg_index WHERE indexrelid=to_regclass($1)',[`remote_ingestion.${name}`])).rows[0];
      if(existing && !existing.indisvalid)throw Error(`invalid interrupted index: ${name}; inspect before retry`);
      await client.query(sql.replace('CREATE INDEX IF NOT EXISTS','CREATE INDEX CONCURRENTLY IF NOT EXISTS'));
      console.log(JSON.stringify({index:name,ready:true}));
    }
    const activation=await readFile(new URL('../src/remoteNodes/workerActivationSchema.sql',import.meta.url),'utf8');
    const table=activation.slice(activation.indexOf('CREATE TABLE IF NOT EXISTS remote_ingestion.node_intake_requests'));
    if(!table.startsWith('CREATE TABLE'))throw Error('intake schema missing');
    await client.query("SET lock_timeout='5s'");
    await client.query('BEGIN');
    await client.query(table);
    await client.query(await readFile(new URL('../src/remoteNodes/natsSchema.sql',import.meta.url),'utf8'));
    await client.query('COMMIT');
    console.log(JSON.stringify({database:expected,applied:true}));
  }
}catch(error){await client.query('ROLLBACK').catch(()=>{});throw error;}
finally{client.release();await pool.end();}
