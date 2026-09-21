// Isolated acceptance only; deliberately has no DATABASE_URL fallback.
import pg from 'pg';
import {readFile} from 'node:fs/promises';
import {assertIsolatedRemoteDatabase} from '../src/remoteNodes/isolation.js';
if(!process.env.REMOTE_NODE_TEST_DATABASE_URL||process.argv.slice(2).some(arg=>arg!=='--apply'))throw Error('Explicit isolated database and optional --apply required');
const pool=new pg.Pool({connectionString:process.env.REMOTE_NODE_TEST_DATABASE_URL,max:1});
try{
  await assertIsolatedRemoteDatabase(pool);
  if(process.argv.includes('--apply')){
    const client=await pool.connect();
    try{
      await client.query('BEGIN');await client.query("SET LOCAL lock_timeout='3s'");await client.query("SET LOCAL statement_timeout='15s'");
      await client.query('SELECT pg_advisory_xact_lock(781137981)');
      for(const file of ['schema.sql','routeSchema.sql','workerConnectionSchema.sql','workerActivationSchema.sql','fullCrawlSchema.sql',
        'fullCrawlBusinessSchema.sql','youtubeSessionSchema.sql','natsSchema.sql','fullCrawlTransportSchema.sql']){
        await client.query(await readFile(new URL(`../src/remoteNodes/${file}`,import.meta.url),'utf8'));
      }
      await client.query('COMMIT');
    }catch(error){await client.query('ROLLBACK');throw error;}finally{client.release();}
  }
  console.log(JSON.stringify({phase:'P3',isolated:true,applied:process.argv.includes('--apply')}));
}finally{await pool.end();}
