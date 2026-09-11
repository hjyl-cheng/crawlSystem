import test from 'node:test';
import assert from 'node:assert/strict';
import {spawn} from 'node:child_process';
import {once} from 'node:events';
import {mkdtemp,writeFile,readFile,rm} from 'node:fs/promises';
import {join} from 'node:path';
import {tmpdir} from 'node:os';
import {randomUUID,randomBytes,generateKeyPairSync} from 'node:crypto';
import pg from 'pg';
import {assertIsolatedRemoteDatabase} from '../src/remoteNodes/isolation.js';
const url=process.env.REMOTE_NODE_TEST_DATABASE_URL;const port=Number(process.env.REMOTE_NODE_TEST_REDIS_PORT);
test('real central entry requires explicit config, starts gateway and shuts down without creating plans',{skip:!url||!port,timeout:30000},async t=>{
  const pool=new pg.Pool({connectionString:url,max:2});const guard=await pool.connect();const children=[];
  const folder=await mkdtemp(join(tmpdir(),'remote-center-entry-'));
  t.after(async()=>{for(const child of children)if(child.exitCode===null&&!child.killed)child.kill('SIGKILL');guard.release();await pool.end();await rm(folder,{recursive:true,force:true});});
  await assertIsolatedRemoteDatabase(pool);await guard.query('SELECT pg_advisory_lock(781137981)');
  for(const file of ['schema.sql','routeSchema.sql','youtubeSessionSchema.sql','workerConnectionSchema.sql','workerActivationSchema.sql'])await pool.query(await readFile(new URL(`../src/remoteNodes/${file}`,import.meta.url),'utf8'));
  const files={REMOTE_NODE_ROUTE_PRIVATE_KEY_FILE:generateKeyPairSync('ed25519').privateKey.export({type:'pkcs8',format:'pem'}),
    REMOTE_NODE_ENCRYPTION_KEY_FILE:randomBytes(32).toString('hex'),REMOTE_NODE_ADMIN_TOKEN_FILE:randomBytes(32).toString('hex'),
    REMOTE_NODE_ROTA_TOKEN_FILE:randomBytes(32).toString('hex'),REMOTE_NODE_PROFILE_SECRET_FILE:randomBytes(32).toString('hex'),
    REMOTE_NODE_ROTA_CONTROL_TOKEN_FILE:randomBytes(32).toString('hex')};
  const env={...process.env,DATABASE_URL:url,REMOTE_NODE_DATABASE_URL:url,REMOTE_NODE_EXECUTION_ENABLED:'true',
    YOUTUBEJS_EXTRACTOR_MODE:'full',YOUTUBEJS_VIDEO_API_BATCH_FALLBACK:'false',ROTA_IDENTITY_POLICY_ID:'qy-br-channel-anonymous-v1',
    ROTA_WORKLOAD_SCOPE_EXPECTED:'qy-production',REMOTE_NODE_REDIS_URL:`redis://:remote-center-fixture-only@127.0.0.1:${port}/0`,
    REMOTE_NODE_QUEUE_PREFIX:'remote-center-entry-test-'+randomUUID(),REMOTE_NODE_EXECUTION_NODE_IDS:randomUUID(),
    REMOTE_NODE_ROTA_ROUTE_URL:'http://127.0.0.1:9/internal/v1/remote-route',ROTA_PROXY_CONTROL_URL:'http://127.0.0.1:9',REMOTE_NODE_CENTER_PORT:'0',
    REMOTE_NODE_COLLECT_IMAGE:'fixture.example/collect@sha256:'+'a'.repeat(64),REMOTE_NODE_GATEWAY_URL:'https://fixture.example'};
  for(const [name,value] of Object.entries(files)){env[name]=join(folder,name);await writeFile(env[name],value,{mode:0o600});}
  const before=(await pool.query('SELECT count(*)::int AS n FROM remote_ingestion.tasks')).rows[0].n;
  for(let iteration=0;iteration<3;iteration++){
  const child=spawn(process.execPath,['scripts/runRemoteNodeCenter.mjs'],{env,stdio:['ignore','pipe','pipe']});children.push(child);
  let output='';let errors='';child.stdout.on('data',bytes=>{output+=bytes;});child.stderr.on('data',bytes=>{errors+=bytes;});
  const exited=once(child,'exit');
  await new Promise((resolve,reject)=>{const timeout=setTimeout(()=>reject(new Error('entry did not become ready: '+errors)),15000);
    const listen=()=>{if(output.includes('remote_node_center_listening')){clearTimeout(timeout);child.stdout.off('data',listen);resolve();}};
    child.stdout.on('data',listen);listen();child.once('exit',code=>{clearTimeout(timeout);if(!output.includes('remote_node_center_listening'))reject(new Error('entry exited '+code+': '+errors));});});
  assert.match(output,/explicit_node_allowlist/);child.kill('SIGTERM');assert.deepEqual(await exited,[0,null]);
  for(const value of Object.values(files))assert.ok(!(output+errors).includes(value));
  }
  assert.equal((await pool.query('SELECT count(*)::int AS n FROM remote_ingestion.tasks')).rows[0].n,before);
  const bad=spawn(process.execPath,['scripts/runRemoteNodeCenter.mjs'],{env:{...env,REMOTE_NODE_QUEUE_PREFIX:''},stdio:['ignore','pipe','pipe']});children.push(bad);bad.stdout.resume();bad.stderr.resume();
  assert.equal((await once(bad,'exit'))[0],1,'missing queue identity fails instead of using defaults');
});
