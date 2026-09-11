import pg from 'pg';
import { readFile } from 'node:fs/promises';
const expected=process.env.EXPECTED_CRAWLER_DATABASE;
if(!expected || !process.env.REMOTE_NODE_DATABASE_URL)throw new Error('explicit database identity required');
const pool=new pg.Pool({connectionString:process.env.REMOTE_NODE_DATABASE_URL,max:1});
const client=await pool.connect();
try{
  const row=(await client.query('SELECT current_database() AS name')).rows[0];
  if(row.name!==expected)throw new Error('database identity mismatch');
  const files=['schema.sql','routeSchema.sql','youtubeSessionSchema.sql','workerConnectionSchema.sql','workerActivationSchema.sql'];
  if(process.argv.includes('--apply')){
    await client.query('BEGIN');
    await client.query("SET LOCAL lock_timeout='3s'");
    await client.query("SET LOCAL statement_timeout='15s'");
    await client.query('SELECT pg_advisory_xact_lock(781138013)');
    for(const file of files)await client.query(await readFile(new URL(`../src/remoteNodes/${file}`,import.meta.url),'utf8'));
    await client.query('COMMIT');
  }
  const tables=(await client.query("SELECT tablename FROM pg_tables WHERE schemaname='remote_ingestion' ORDER BY tablename")).rows;
  console.log(JSON.stringify({database:row.name,applied:process.argv.includes('--apply'),tables:tables.map(r=>r.tablename)}));
}catch(error){await client.query('ROLLBACK').catch(()=>{});throw error;}
finally{client.release();await pool.end();}
