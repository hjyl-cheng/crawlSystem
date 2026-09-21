import test from 'node:test';
import assert from 'node:assert/strict';
import {randomUUID,randomBytes,generateKeyPairSync} from 'node:crypto';
import {readFile} from 'node:fs/promises';
import {fullCrawlFixture} from './helpers/remoteFullCrawlFixture.js';
import {createRemoteDeploymentAdmin} from '../src/remoteNodes/deploymentAdmin.js';
import {createFullCrawlDeploymentRuntime} from '../src/remoteNodes/fullCrawlDeploymentRuntime.js';
import {createDeploymentCapacity} from '../src/remoteNodes/deploymentCapacity.js';
import {buildNodeCollectDeployment} from '../../dashboard/src/nodeRuntime/collectDeployment.js';

test('full deployment isolates identity, retries capacity, restores intake and retires only quiet work',
  {skip:!process.env.REMOTE_NODE_TEST_DATABASE_URL},async t=>{
  const f=await fullCrawlFixture(t);
  for(const file of ['workerCountSchema.sql','intakeControlSchema.sql']){
    // The shared schema names are deliberately explicit: no production startup migration.
    await f.query(await readFile(new URL(`../src/remoteNodes/${file}`,import.meta.url),'utf8'));
  }
  const image='registry.example/incremental@sha256:'+'a'.repeat(64);
  const fullImage='registry.example/full@sha256:'+'b'.repeat(64);
  const gatewayUrl='https://center.example',token=randomBytes(32).toString('hex');
  const fullRuntime=createFullCrawlDeploymentRuntime({store:f.store,image:fullImage,
    privateKey:generateKeyPairSync('ed25519').privateKey,secretKey:randomBytes(32),readRotaRoute:()=>assert.fail('deployment cannot allocate network')});
  const routes=fullRuntime.transport.routes;
  const nodeId=randomUUID(),incrementalId=randomUUID(),deploymentId=randomUUID();
  const node={id:nodeId,kind:'execution',workerRole:'fullcrawl',provisioning:{state:'ready'},runtime:{state:'ready'}};
  const plan=count=>buildNodeCollectDeployment({node:{...node,workers:[{role:'fullcrawl',count}]},deploymentId,image:fullImage,gatewayUrl,natsUrl:'tls://messages.example:4222'});
  const prepare=(admin,p)=>admin.prepare({nodeId:p.nodeId,deploymentId:p.deploymentId,image:p.image,files:p.files});
  let capacityFail=true,required;
  const capacity=createDeploymentCapacity({pool:f.pool,localChannelSlots:3,client:{async ensureCapacity(v){required=v.minimum_slots;if(capacityFail)throw Error('fixture unavailable');return {ok:true,role:'channel',provisioned:required};}}});
  const args={store:f.store,routes,image,token,gatewayUrl,capacity,fullCrawl:fullRuntime};
  let admin=createRemoteDeploymentAdmin(args);
  try{
    await assert.rejects(prepare(createRemoteDeploymentAdmin({...args,fullCrawl:null}),plan(1)),{code:'INVALID_DEPLOYMENT'});
    await assert.rejects(prepare(admin,plan(1)),{code:'REMOTE_NETWORK_CAPACITY_UNAVAILABLE'});
    assert.equal((await f.query('SELECT worker_count FROM remote_ingestion.node_deployments WHERE node_id=$1',[nodeId])).rows[0].worker_count,1);
    capacityFail=false;
    const first=await prepare(admin,plan(1));
    assert.deepEqual(await prepare(admin,plan(1)),first);
    const expanded=await prepare(admin,plan(2));
    assert.equal(expanded.nodeToken,first.nodeToken);assert.equal(expanded.relayTokens['full-crawl-1'],first.relayTokens['full-crawl-1']);
    assert.deepEqual((await f.query('SELECT capabilities FROM remote_ingestion.nodes WHERE node_id=$1',[nodeId])).rows[0].capabilities,['youtube.full-crawl.v1']);
    assert.equal(required,5);
    const incrementalPlan=buildNodeCollectDeployment({node:{...node,id:incrementalId,workerRole:'incremental',workers:[{role:'incremental',count:2}]},image,gatewayUrl,natsUrl:'tls://messages.example:4222'});
    await prepare(admin,incrementalPlan);assert.equal(required,7,'full and incremental share Rota channel capacity without double counting retries');
    const oldIncremental=(await f.query('SELECT * FROM remote_ingestion.worker_connections WHERE node_id=$1 ORDER BY slot',[incrementalId])).rows;
    await assert.rejects(prepare(admin,{...plan(2),image}),{code:'INVALID_DEPLOYMENT'});
    for(const registration of plan(2).registrations){
      await fullRuntime.activation.heartbeat(nodeId,{version:1,mode:'full_crawl_collect',node_id:nodeId,slot:registration.slot,
        deployment_id:deploymentId,config_hash:registration.configHash,instance_id:randomUUID(),relay_boot_id:'c'.repeat(48),runtime_revision:'youtubejs-full-crawl-v1',accepting:true});
    }
    const control={nodeId,deploymentId,workerCount:2};
    let status=await admin.status(control);
    assert.equal(status.counts.connected,2);assert.equal(status.allowedCount,0);assert.equal(status.executionAvailable,false);
    await assert.rejects(admin.setExecution({...control,enabled:true,expectedRequested:false}),{code:'REMOTE_CENTER_EXECUTION_NOT_CONFIGURED'});
    let processing=false;
    const execution={allowsNode:id=>id===nodeId,isProcessing:()=>processing};
    const enabledArgs={...args,fullCrawl:{...fullRuntime,execution}};
    admin=createRemoteDeploymentAdmin(enabledArgs);
    await admin.setExecution({...control,allowedCount:1,expectedAllowedCount:2});
    status=await admin.setExecution({...control,enabled:true,expectedRequested:false});
    assert.equal(status.allowedCount,1);assert.equal(status.counts.standby,1);
    admin=createRemoteDeploymentAdmin(enabledArgs);
    assert.equal((await admin.status(control)).configuredCount,1,'center restart reads durable configuration');
    processing=true;
    status=await admin.setExecution({...control,enabled:false,expectedRequested:true});
    assert.equal(status.allowedCount,0);assert.equal(status.configuredCount,1);assert.equal(status.counts.finishing,2);
    const retirement={nodeId,deploymentId,slot:'full-crawl-2',operationId:randomUUID()};
    await assert.rejects(admin.retire({...retirement,phase:'reserve'}),{code:'WORKER_NOT_IDLE'});
    processing=false;
    await admin.retire({...retirement,phase:'reserve'});
    await admin.retire({...retirement,phase:'ready'});
    await admin.retire({...retirement,phase:'finish'});
    assert.equal((await admin.retire({...retirement,phase:'finish'})).removed,true);
    assert.equal((await admin.status(control)).counts.deployed,1);
    await assert.rejects(prepare(admin,plan(2)),{code:'WORKER_RETIREMENT_CONFLICT'});
    assert.deepEqual((await f.query('SELECT * FROM remote_ingestion.worker_connections WHERE node_id=$1 ORDER BY slot',[incrementalId])).rows,oldIncremental);

    // A terminal API handoff keeps evidence in SQL but releases the collection slot.
    const request=await f.claim();
    await f.query(`INSERT INTO remote_ingestion.node_deployments(node_id,deployment_id,image,worker_count,credentials_cipher) VALUES($1,$2,$3,1,$4)`,
      [f.nodeId,f.connection.deployment_id,fullImage,routes.encrypt({nodeToken:'d'.repeat(64),relayTokens:{}},`node-deployment:${f.nodeId}`)]);
    const fixtureControl={nodeId:f.nodeId,deploymentId:f.connection.deployment_id};
    assert.equal((await admin.status(fixtureControl)).counts.awaiting,1);
    await f.query("UPDATE remote_ingestion.tasks SET state='received',last_error='VIDEO_API_WAIT' WHERE task_id=$1",[request.task_id]);
    assert.equal((await admin.status(fixtureControl)).counts.awaiting,1,'unfinished original attempt still blocks deletion');
    await f.query("UPDATE crawler.channel_execution_attempts SET status='success',finished_at=clock_timestamp() WHERE attempt_id=$1",[f.execution.execution_attempt_id]);
    assert.equal((await admin.status(fixtureControl)).counts.awaiting,0,'finished API handoff does not occupy collection capacity');
    const last={...fixtureControl,slot:f.slot,operationId:randomUUID()};
    await admin.retire({...last,phase:'reserve'});await admin.retire({...last,phase:'ready'});await admin.retire({...last,phase:'finish'});
    assert.equal((await admin.status(fixtureControl)).counts.deployed,0);
    assert.equal((await f.query('SELECT state FROM remote_ingestion.tasks WHERE task_id=$1',[request.task_id])).rows[0].state,'received','retiring the last worker preserves evidence for API continuation');
  }finally{
    await f.query('DELETE FROM remote_ingestion.node_deployments WHERE node_id=$1',[f.nodeId]);
    await f.query('DELETE FROM remote_ingestion.intake_controls WHERE node_key=ANY($1::text[])',[[nodeId,incrementalId]]);
    for(const id of [nodeId,incrementalId])for(const table of ['node_intake_requests','node_deployments','worker_connections','network_slots','nodes'])
      await f.query(`DELETE FROM remote_ingestion.${table} WHERE node_id=$1`,[id]);
  }
});
