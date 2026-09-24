import { readFile } from 'node:fs/promises';
import { fileURLToPath } from 'node:url';
import pg from 'pg';
import { environmentValue } from '../src/runtimeEnvironment.js';

export function latestStorageConfig(environment=process.env,argv=process.argv.slice(2)) {
  const action=argv[0]??'inspect';
  if(!['inspect','install','enable'].includes(action)) throw new Error('action must be inspect, install or enable');
  const database=String(environment.EXPECTED_BUSINESS_DATABASE??'').trim();
  if(!database) throw new Error('EXPECTED_BUSINESS_DATABASE is required');
  if(action!=='inspect'&&(!argv.includes('--apply')||environment.CONFIRM_BUSINESS_LATEST_STORAGE!==database))
    throw new Error('writes require --apply and CONFIRM_BUSINESS_LATEST_STORAGE matching the database');
  if(action==='enable'&&(!argv.includes('--reader-ready')||!environment.EXPECTED_BUSINESS_WATERMARK
      ||!environment.BUSINESS_STORAGE_ACTOR||!environment.BUSINESS_STORAGE_REASON))
    throw new Error('enable requires --reader-ready, EXPECTED_BUSINESS_WATERMARK, BUSINESS_STORAGE_ACTOR and BUSINESS_STORAGE_REASON');
  return {action,database,databaseUrl:environmentValue('BUSINESS_DATABASE_URL',{environment}),
    watermark:environment.EXPECTED_BUSINESS_WATERMARK,actor:environment.BUSINESS_STORAGE_ACTOR,reason:environment.BUSINESS_STORAGE_REASON};
}

export async function manageLatestStorage(client,config) {
  await client.query(config.action==='inspect'?'BEGIN READ ONLY':'BEGIN');
  try {
    await client.query("SET LOCAL lock_timeout='3s'");
    await client.query("SET LOCAL statement_timeout='60s'");
    const identity=(await client.query(`SELECT current_database() AS database, database_kind,database_name
      FROM publication.database_identity WHERE singleton`)).rows[0];
    if(identity?.database!==config.database||identity.database_kind!=='business'||identity.database_name!==config.database)
      throw new Error('unexpected or uninitialized business database');
    if(config.action!=='inspect') {
      await client.query("SELECT pg_advisory_xact_lock(hashtext('kol_demo:creator-search-publish'))");
      if(config.action==='install') await client.query(await readFile(new URL('../src/businessLatestStorageSchema.sql',import.meta.url),'utf8'));
      else {
        const state=(await client.query(`SELECT a.watermark,s.read_mode,s.write_mode
          FROM public.creator_search_active a CROSS JOIN publication.creator_search_storage_state s
          WHERE a.singleton AND s.singleton FOR UPDATE OF a,s`)).rows[0];
        if(state?.watermark!==config.watermark||state.read_mode!=='live'||state.write_mode!=='incremental')
          throw new Error('business publication changed or Search is not incremental/live');
        const privileges=(await client.query(`SELECT bool_and(has_table_privilege(
          'business_publication_projector',target,privilege)) AS ready FROM (VALUES
          ('publication.business_storage_state','SELECT'),
          ('publication.latest_projection_state','SELECT'),('publication.latest_projection_state','INSERT'),
          ('publication.latest_projection_state','UPDATE'),('publication.channel_metric_history','SELECT'),
          ('publication.channel_metric_history','INSERT'),('publication.channel_metric_history','DELETE'),
          ('public.channel_snapshots','UPDATE'),('public.content_snapshots','UPDATE'),
          ('public.channel_links','DELETE'),('public.channel_links','UPDATE'),
          ('public.channel_profile_facts','DELETE'),('public.channel_profile_facts','UPDATE'),
          ('public.channel_metric_values','DELETE'),('public.channel_metric_values','UPDATE')
        ) required(target,privilege)`)).rows[0];
        if(!privileges?.ready) throw new Error('provision latest publisher permissions before activation');
        await client.query(`UPDATE publication.business_storage_state SET mode='latest',
          activated_at=COALESCE(activated_at,clock_timestamp()),actor=$1,reason=$2 WHERE singleton`,[config.actor,config.reason]);
      }
    }
    const installed=(await client.query("SELECT to_regclass('publication.business_storage_state') IS NOT NULL AS ready")).rows[0].ready;
    const state=installed?(await client.query('SELECT * FROM publication.business_storage_state WHERE singleton')).rows[0]:null;
    const counts=installed?(await client.query(`SELECT
      (SELECT count(*) FROM publication.latest_projection_state) AS adopted_channels,
      (SELECT count(*) FROM publication.channel_metric_history) AS trend_points`)).rows[0]:null;
    await client.query('COMMIT');return {database:config.database,installed,state,counts};
  } catch(error) {await client.query('ROLLBACK').catch(()=>{});throw error;}
}

if(process.argv[1]===fileURLToPath(import.meta.url)) {
  const client=new pg.Client({connectionString:latestStorageConfig().databaseUrl});
  try {await client.connect();console.log(JSON.stringify(await manageLatestStorage(client,latestStorageConfig())));}
  catch(error) {console.error(error.message);process.exitCode=1;}
  finally {await client.end().catch(()=>{});}
}
