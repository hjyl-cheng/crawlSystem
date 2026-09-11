// Explicit opt-in local Docker integration; never uses production DB defaults.
import assert from 'node:assert/strict';
import { execFile } from 'node:child_process';
import { promisify } from 'node:util';
import { mkdtemp, readFile, writeFile, chown, chmod, rm, mkdir } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { createServer } from 'node:https';
import { once } from 'node:events';
import { randomUUID, randomBytes, generateKeyPairSync } from 'node:crypto';
import { setTimeout as delay } from 'node:timers/promises';
import pg from 'pg';
import { RemoteNodeStore } from '../src/remoteNodes/store.js';
import { RemoteWorkerActivationStore, REMOTE_RUNTIME_REVISION } from '../src/remoteNodes/workerActivationStore.js';
import { buildNodeCollectDeployment } from '../../dashboard/src/nodeRuntime/collectDeployment.js';
import { RemoteWorkerConnectionStore } from '../src/remoteNodes/workerConnectionStore.js';
import { createRemoteNodeGateway } from '../src/remoteNodes/gateway.js';
import { assertIsolatedRemoteDatabase } from '../src/remoteNodes/isolation.js';
import { buildNodeConnectionDeployment } from '../../dashboard/src/nodeRuntime/connectionDeployment.js';

const run = promisify(execFile);
const docker = async (...args) => (await run('docker', args, { timeout: 60000, maxBuffer: 1024*1024 })).stdout.trim();
const collecting=!!process.env.REMOTE_NODE_COLLECT_TEST_IMAGE;
const image = process.env.REMOTE_NODE_COLLECT_TEST_IMAGE || process.env.REMOTE_NODE_CONNECTION_TEST_IMAGE;
const entry=collecting?'runRemoteNodeIncremental.mjs':'runRemoteNodeConnection.mjs';
const url = process.env.REMOTE_NODE_TEST_DATABASE_URL;
if (!image || !url) throw new Error('explicit test image and isolated database URL required');
const inspection = JSON.parse(await docker('image','inspect',image))[0];
assert.equal(inspection.Config.Labels['qy.remote.mode'], collecting?'incremental_collect':'connect_only');
const pool = new pg.Pool({ connectionString: url, max: 4 });
await assertIsolatedRemoteDatabase(pool);
const directory = await mkdtemp(join(tmpdir(), 'qy-node-connection-docker-'));
const project = `qy-connection-test-${randomUUID().slice(0,8)}`;
const network = `${project}-network`;
const nodeId = randomUUID();
const token = randomBytes(32).toString('hex');
const composeFile = join(directory,'compose.json');
let server; let guard; let createdNetwork = false; let wroteCompose = false;
try {
  guard = await pool.connect();
  await guard.query('SELECT pg_advisory_lock(781137981)');
  for (const file of ['schema.sql','routeSchema.sql','workerConnectionSchema.sql','workerActivationSchema.sql']) await pool.query(await readFile(new URL(`../src/remoteNodes/${file}`,import.meta.url),'utf8'));
  await docker('network','create','--internal',network); createdNetwork = true;
  const networkInfo = JSON.parse(await docker('network','inspect',network))[0];
  const gatewayAddress = networkInfo.IPAM.Config[0].Gateway;
  const ca = join(directory,'ca.pem'); const tlsKey = join(directory,'tls.key');
  await run('openssl',['req','-x509','-newkey','rsa:2048','-nodes','-keyout',tlsKey,'-out',ca,
    '-days','1','-subj','/CN=connection-center.test','-addext','subjectAltName=DNS:connection-center.test'],{timeout:10000});
  const store = new RemoteNodeStore({pool});
  const workerConnections = collecting?new RemoteWorkerActivationStore({store,verifyExecution:async()=>true}):new RemoteWorkerConnectionStore({store});
  await store.registerNode({nodeId,token,capabilities:[`fixture.docker.${nodeId}`],maxLeases:2});
  if(!collecting)await store.setNodeState(nodeId,'draining');
  const gateway = createRemoteNodeGateway({store,workerConnections});
  const handle = gateway.listeners('request')[0];
  let unavailable = false; let claimRequests = 0;
  server = createServer({key:await readFile(tlsKey),cert:await readFile(ca)},(req,res)=>{
    if(req.url.includes('/work/claim'))claimRequests++;
    if(unavailable){req.resume();res.writeHead(503,{'content-type':'application/json'});res.end('{"error":"TEST_OFFLINE"}');return;}
    handle(req,res);
  });
  server.listen(0,gatewayAddress);await once(server,'listening');
  const plan = (collecting?buildNodeCollectDeployment:buildNodeConnectionDeployment)({ node:{id:nodeId,kind:'execution',provisioning:{state:'ready'},runtime:{state:'ready'},workers:[{role:'incremental',count:2}]},
    gatewayUrl:`https://connection-center.test:${server.address().port}`, image:'fixture.example/connection@sha256:'+'a'.repeat(64)});
  const routeKeys = generateKeyPairSync('ed25519');
  const values = {...plan.files, 'node-token':token, 'route-public.pem':routeKeys.publicKey.export({type:'spki',format:'pem'}),
    'incremental-1.relay-token':randomBytes(32).toString('hex'),'incremental-2.relay-token':randomBytes(32).toString('hex')};
  for(const [name,contents] of Object.entries(values)){
    const path=join(directory,name);await writeFile(path,contents,{mode:0o600});await chown(path,1000,1000);
  }
  await chmod(ca,0o644);
  for(const registration of plan.registrations){
    await pool.query('INSERT INTO remote_ingestion.network_slots(node_id,slot,rota_worker_id) VALUES($1,$2,$3)',[nodeId,registration.slot,`${project}-${registration.slot}`]);
    await workerConnections.register(registration);
  }
  const before = (await pool.query('SELECT task_id,state,generation,node_id FROM remote_ingestion.tasks ORDER BY task_id')).rows;
  const compose = plan.compose;
  compose.networks={default:{external:true,name:network}};
  for(const service of Object.values(compose.services)){
    service.image=inspection.Id; // The exact locally built image, no registry pull.
    service.extra_hosts=[`connection-center.test:${gatewayAddress}`];
    service.environment={NODE_EXTRA_CA_CERTS:'/run/secrets/test-ca.pem'};
    for(const mount of service.volumes){
      mount.source=join(directory,mount.source.split('/').at(-1));
      if(mount.target==='/var/lib/qy-node/spool'){await mkdir(mount.source,{mode:0o700});await chown(mount.source,1000,1000);}
    }
    service.volumes.push({type:'bind',source:ca,target:'/run/secrets/test-ca.pem',read_only:true});
  }
  await writeFile(composeFile,JSON.stringify(compose));wroteCompose=true;
  const dc=(...args)=>docker('compose','-p',project,'-f',composeFile,...args);
  await dc('config','--quiet');
  await dc('up','-d','--pull','never');
  async function waitFor(action,timeout=45000){const end=Date.now()+timeout;while(Date.now()<end){try{if(await action())return;}catch{}await delay(250);}throw new Error('connection fixture timed out');}
  await waitFor(async()=>Number((await pool.query('SELECT count(*) FROM remote_ingestion.worker_connections WHERE node_id=$1 AND connected_until>clock_timestamp()',[nodeId])).rows[0].count)===2);
  const containerIds=(await dc('ps','-q')).split('\n');assert.equal(containerIds.length,2);
  for(const id of containerIds)await docker('exec',id,'node',`scripts/${entry}`,'--healthcheck');
  const initial=(await pool.query('SELECT slot,instance_id FROM remote_ingestion.worker_connections WHERE node_id=$1 ORDER BY slot',[nodeId])).rows;
  assert.equal(claimRequests,0,'starting containers is not activation');
  if(collecting){
    await docker('exec',containerIds[0],'python3','-c','import aiohttp,curl_cffi,importlib.util; assert importlib.util.find_spec("yt_dlp") is None');
    for(const registration of plan.registrations){
      const row=(await pool.query('SELECT * FROM remote_ingestion.worker_connections WHERE node_id=$1 AND slot=$2',[nodeId,registration.slot])).rows[0];
      await workerConnections.activate({version:1,mode:'incremental_collect',node_id:nodeId,slot:row.slot,deployment_id:row.deployment_id,
        config_hash:row.config_hash,instance_id:row.instance_id,relay_boot_id:row.relay_boot_id,runtime_revision:REMOTE_RUNTIME_REVISION,accepting:true});
    }
    await waitFor(()=>claimRequests>0);
    await docker('exec',containerIds[0],'node','-e',"require('fs').writeFileSync('/var/lib/qy-node/spool/retained-fixture','retained')");
  }
  unavailable=true;
  await waitFor(async()=>{
    const result=JSON.parse(await docker('exec',containerIds[0],'cat','/run/qy-node/health.json'));
    return result.state==='disconnected';
  });
  unavailable=false;
  await waitFor(async()=>JSON.parse(await docker('exec',containerIds[0],'cat','/run/qy-node/health.json')).state===(collecting?'ready':'connected_waiting_activation'));
  assert.deepEqual((await pool.query('SELECT slot,instance_id FROM remote_ingestion.worker_connections WHERE node_id=$1 ORDER BY slot',[nodeId])).rows,initial,'reconnection preserves process ownership');
  compose.services.duplicate=structuredClone(compose.services['incremental-1']);
  await writeFile(composeFile,JSON.stringify(compose));
  await dc('up','-d','--pull','never','duplicate');
  const duplicate=await dc('ps','-q','duplicate');
  await waitFor(async()=>JSON.parse(await docker('exec',duplicate,'cat','/run/qy-node/health.json')).state==='disconnected');
  assert.deepEqual((await pool.query('SELECT slot,instance_id FROM remote_ingestion.worker_connections WHERE node_id=$1 ORDER BY slot',[nodeId])).rows,initial,'duplicate process cannot replace a live slot');
  if(collecting){
    for(const registration of plan.registrations)await workerConnections.drain(nodeId,registration.slot);
    await waitFor(async()=>{
      const states=await Promise.all(containerIds.map(id=>docker('exec',id,'cat','/run/qy-node/health.json')));
      return states.every(state=>JSON.parse(state).state==='connected_waiting_activation');
    });
    const atDrain=claimRequests;await delay(1800);assert.equal(claimRequests,atDrain,'drain stops new polling');
  }else assert.equal(claimRequests,0);
  assert.deepEqual((await pool.query('SELECT task_id,state,generation,node_id FROM remote_ingestion.tasks ORDER BY task_id')).rows,before);
  const logs=await dc('logs','--no-color');assert.ok(!logs.includes(token));
  for(const name of ['incremental-1.relay-token','incremental-2.relay-token'])assert.ok(!logs.includes(values[name]));
  await dc('stop','-t','25');
  const stopped=JSON.parse(await docker('inspect',...containerIds));
  assert.ok(stopped.every(container=>container.State.ExitCode===0));
  if(collecting)assert.equal(await readFile(join(directory,'incremental-1','retained-fixture'),'utf8'),'retained');
  console.log(JSON.stringify({mode:collecting?'incremental_collect':'connect_only',passed:['real Docker Compose','two isolated containers','real Go relay','HTTPS trust','central registration and heartbeats','health check','disconnect and reconnect','duplicate slot rejected','graceful SIGTERM',collecting?'activation and drain with real empty claims':'no task claims','no secret logging'],imageId:inspection.Id}));
}finally{
  if(wroteCompose)await docker('compose','-p',project,'-f',composeFile,'down','--remove-orphans','--timeout','10').catch(()=>{});
  if(server)await new Promise(resolve=>{server.close(resolve);server.closeAllConnections();});
  if(createdNetwork)await docker('network','rm',network).catch(()=>{});
  await pool.query('DELETE FROM remote_ingestion.worker_connections WHERE node_id=$1',[nodeId]).catch(()=>{});
  await pool.query('DELETE FROM remote_ingestion.network_slots WHERE node_id=$1',[nodeId]).catch(()=>{});
  await pool.query('DELETE FROM remote_ingestion.nodes WHERE node_id=$1',[nodeId]).catch(()=>{});
  guard?.release();await pool.end();await rm(directory,{recursive:true,force:true});
}
