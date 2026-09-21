import test from 'node:test';
import assert from 'node:assert/strict';
import {randomUUID,randomBytes,generateKeyPairSync} from 'node:crypto';
import {mkdir,writeFile,readFile,chown} from 'node:fs/promises';
import {setTimeout as delay} from 'node:timers/promises';
import {fullCrawlFixture} from './helpers/remoteFullCrawlFixture.js';
import {createFullCrawlDeploymentRuntime} from '../src/remoteNodes/fullCrawlDeploymentRuntime.js';
import {createRemoteDeploymentAdmin} from '../src/remoteNodes/deploymentAdmin.js';
import {RemoteChannelPlanStore} from '../src/remoteNodes/channelPlanStore.js';
import {RemoteWorkerActivationStore} from '../src/remoteNodes/workerActivationStore.js';
import {createTransportSignals} from '../src/remoteNodes/transportSignals.js';
import {startRemoteNatsCenter} from '../src/remoteNodes/natsCenter.js';
import {createNatsProvisioning} from '../src/remoteNodes/natsProvisioning.js';
import {buildNodeCollectDeployment} from '../../dashboard/src/nodeRuntime/collectDeployment.js';

// The shell harness starts the actual built image and isolated TLS broker.
// Files under this explicit fixture directory coordinate container lifecycle;
// the node mounts only its own credentials, certificate and durable spool.
const directory=process.env.FULL_CRAWL_PACKAGE_TEST_DIR;
test('dedicated full image connects over TLS, survives node/center restart, and retires with spool retained',
  {skip:!directory,timeout:180000},async t=>{
  assert.equal(directory,'/p4-package');
  const until=async(check)=>{for(let i=0;i<450;i++){if(await check())return;await delay(200);}throw Error('P4_PACKAGE_TIMEOUT');};
  const marker=name=>writeFile(`${directory}/${name}`,'ready\n');
  const exists=name=>readFile(`${directory}/${name}`).then(()=>true,()=>false);
  const f=await fullCrawlFixture(t,{createAttempt:false,activate:false});
  for(const file of ['natsSchema.sql','youtubeSessionSchema.sql','fullCrawlTransportSchema.sql'])await f.query(await readFile(new URL(`../src/remoteNodes/${file}`,import.meta.url),'utf8'));
  const nodeId=randomUUID(),deploymentId=randomUUID();
  const fullImage=process.env.FULL_CRAWL_PACKAGE_TEST_IMAGE;
  const full=createFullCrawlDeploymentRuntime({store:f.store,image:fullImage,privateKey:generateKeyPairSync('ed25519').privateKey,
    secretKey:randomBytes(32),readRotaRoute:()=>assert.fail('paused node cannot allocate a network route')});
  const args={store:f.store,routes:full.transport.routes,image:'registry.example/incremental@sha256:'+'a'.repeat(64),
    token:randomBytes(32).toString('hex'),gatewayUrl:'https://center.fixture',fullCrawl:full};
  let admin=createRemoteDeploymentAdmin(args),nats,signals;
  const control={nodeId,deploymentId};
  try{
    const plan=buildNodeCollectDeployment({node:{id:nodeId,workerRole:'fullcrawl',kind:'execution',workers:[{role:'fullcrawl',count:1}],
      provisioning:{state:'ready'},runtime:{state:'ready'}},deploymentId,image:fullImage,gatewayUrl:args.gatewayUrl,natsUrl:'tls://full-p4-nats:4222'});
    const credentials=await admin.prepare({...control,image:plan.image,files:plan.files});
    await mkdir(`${directory}/secrets`,{recursive:true,mode:0o755});
    for(const [name,content] of Object.entries({'node-config.json':plan.files['full-crawl-1.json'],'node-token':credentials.nodeToken,
      'relay-token':credentials.relayTokens['full-crawl-1'],'route-public.pem':credentials.publicKey})){
      const path=`${directory}/secrets/${name}`;await writeFile(path,content,{mode:0o600});await chown(path,1000,1000);
    }
    // Provision only this test deployment; unrelated fixture credentials use other encryption keys.
    const provisioning=createNatsProvisioning({pool:{query:(sql)=>f.query(sql.replace("WHERE n.state <> 'disabled'","WHERE n.node_id=$1"),[nodeId])},
      routes:full.transport.routes,file:`${directory}/auth.conf`,centerPassword:'p4-center-test-only-password-000000000000'});
    await provisioning.sync();await marker('registered');
    await until(()=>exists('broker-started'));
    signals=await createTransportSignals({connectionString:process.env.REMOTE_NODE_TEST_DATABASE_URL});
    const natsArgs={signals,resultMaxBytes:32*1024*1024,url:'tls://full-p4-nats:4222',password:'p4-center-test-only-password-000000000000',store:f.store,
      channelPlans:new RemoteChannelPlanStore({store:f.store}),workerConnections:new RemoteWorkerActivationStore({store:f.store,verifyExecution:async()=>false}),
      routes:full.transport.routes,youtubeSessions:full.transport.youtubeSessions,fullCrawls:full.transport.service};
    await until(async()=>{try{nats=await startRemoteNatsCenter(natsArgs);return true;}catch(error){if(error.message!=='connection refused')throw error;return false;}});await marker('center-started');
    await until(async()=>(await admin.status(control)).counts.connected===1);
    assert.equal((await admin.status(control)).allowedCount,0);assert.equal((await admin.status(control)).workers[0].readyForTasks,false);
    const connection=async()=>(await f.query('SELECT * FROM remote_ingestion.worker_connections WHERE node_id=$1',[nodeId])).rows[0];
    const first=await connection();assert.equal(first.runtime_revision,'youtubejs-full-crawl-v1');assert.equal(first.mode,'full_crawl_collect');
    await nats.close();nats=await startRemoteNatsCenter(natsArgs);admin=createRemoteDeploymentAdmin(args);
    assert.equal((await admin.status(control)).configuredCount,1);assert.equal((await admin.status(control)).allowedCount,0);
    await marker('restart-requested');await until(()=>exists('node-stopped'));
    // Simulate the 45-second heartbeat expiry after abrupt node loss.
    await f.query("UPDATE remote_ingestion.worker_connections SET connected_until='-infinity' WHERE node_id=$1",[nodeId]);
    await marker('restart-ready');
    await until(async()=>{const row=await connection();return row.instance_id!==first.instance_id&&row.connected_until>new Date();});
    assert.equal((await admin.status(control)).allowedCount,0);
    await assert.rejects(admin.setExecution({...control,workerCount:1,enabled:true,expectedRequested:false}),{code:'REMOTE_CENTER_EXECUTION_NOT_CONFIGURED'});
    assert.equal((await f.query('SELECT count(*)::int AS n FROM remote_ingestion.tasks WHERE target_node_id=$1',[nodeId])).rows[0].n,0);
    const retirement={...control,slot:'full-crawl-1',operationId:randomUUID()};
    await admin.retire({...retirement,phase:'reserve'});await admin.retire({...retirement,phase:'ready'});
    await marker('remove-requested');await until(()=>exists('node-removed'));
    await admin.retire({...retirement,phase:'finish'});
    assert.equal((await admin.status(control)).counts.deployed,0);
    assert.equal(await readFile(`${directory}/spool/retained-evidence`,'utf8'),'retained\n');
    await writeFile(`${directory}/acceptance.json`,JSON.stringify({nodeId,deploymentId,image:fullImage,mode:plan.mode,
      runtimeRevision:first.runtime_revision,connected:true,readyForTasks:false,allowedCount:0,nodeRestart:true,centerRestart:true,removed:true,spoolRetained:true},null,2)+'\n');
  }finally{
    await nats?.close();await signals?.close();
    await f.query('DELETE FROM remote_ingestion.intake_controls WHERE node_key=$1',[nodeId]);
    for(const table of ['node_intake_requests','node_deployments','worker_connections','network_slots','nodes'])await f.query(`DELETE FROM remote_ingestion.${table} WHERE node_id=$1`,[nodeId]);
  }
});
