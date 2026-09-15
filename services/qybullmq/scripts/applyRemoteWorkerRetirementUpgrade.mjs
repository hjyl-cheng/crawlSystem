import pg from 'pg';
import {readFile} from 'node:fs/promises';
const expected=process.env.EXPECTED_CRAWLER_DATABASE;
if(!expected || !process.env.REMOTE_NODE_DATABASE_URL)throw Error('explicit database identity required');
const pool=new pg.Pool({connectionString:process.env.REMOTE_NODE_DATABASE_URL,max:1});
const client=await pool.connect();
try{
  if((await client.query('SELECT current_database() name')).rows[0].name!==expected)throw Error('database identity mismatch');
  const source=await readFile(new URL('../src/remoteNodes/workerActivationSchema.sql',import.meta.url),'utf8');
  const start=source.indexOf('-- Removed slots remain as tombstones');
  if(start<0)throw Error('retirement schema missing');
  if(process.argv.includes('--apply')){
    await client.query('BEGIN');
    await client.query("SET LOCAL lock_timeout='3s'; SET LOCAL statement_timeout='15s'");
    await client.query(source.slice(start));
    await client.query('COMMIT');
  }
  console.log(JSON.stringify({database:expected,applied:process.argv.includes('--apply')}));
}catch(error){await client.query('ROLLBACK').catch(()=>{});throw error;}
finally{client.release();await pool.end();}
