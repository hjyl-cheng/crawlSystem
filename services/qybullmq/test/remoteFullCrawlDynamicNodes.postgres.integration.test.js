import test from 'node:test';
import assert from 'node:assert/strict';
import {randomUUID,randomBytes,generateKeyPairSync} from 'node:crypto';
import {readFile} from 'node:fs/promises';
import {setTimeout as delay} from 'node:timers/promises';
import pg from 'pg';
import {fullCrawlFixture} from './helpers/remoteFullCrawlFixture.js';
import {createRemoteDeploymentAdmin} from '../src/remoteNodes/deploymentAdmin.js';
import {createFullCrawlDeploymentRuntime} from '../src/remoteNodes/fullCrawlDeploymentRuntime.js';
import {RemoteCenterExecutionSupervisor} from '../src/remoteNodes/centerExecutionSupervisor.js';
import {buildNodeCollectDeployment} from '../../dashboard/src/nodeRuntime/collectDeployment.js';

const url=process.env.REMOTE_NODE_TEST_DATABASE_URL,port=Number(process.env.REMOTE_NODE_TEST_REDIS_PORT);
test('registered full nodes exceed five slots, preserve node-scoped pause and grow with deployed workers',
  {skip:!url||!port,timeout:60000},async t=>{
  const f=await fullCrawlFixture(t,{createAttempt:false,activate:false});
  await f.query(await readFile(new URL('../src/remoteNodes/intakeControlSchema.sql',import.meta.url),'utf8'));
  const ids=[randomUUID(),randomUUID()],deployments=ids.map(()=>randomUUID());
  const image='fixture/full@sha256:'+'b'.repeat(64),gatewayUrl='https://fixture.example';
  let supervisor;
  const runtime=createFullCrawlDeploymentRuntime({store:f.store,image,privateKey:generateKeyPairSync('ed25519').privateKey,
    secretKey:randomBytes(32),readRotaRoute:()=>assert.fail('no transport is used')});
  runtime.activation.fullCrawlExecution.verifyExecution=(client,row)=>supervisor.verifyExecution(client,row);
  const guardPool=new pg.Pool({connectionString:url,max:5,connectionTimeoutMillis:500});
  // Production keeps one release-wide lock in the same pool while all four
  // supervision groups own remote Workers. Expansion must preserve that slot.
  const releaseGuard=await guardPool.connect();
  await releaseGuard.query('SELECT pg_advisory_lock(781138015,1)');
  const options={store:f.store,channelStore:{store:f.store},activation:runtime.activation,guardPool,
    mode:'full_crawl_collect',dashboardManaged:true,allowedNodeIds:[],maxSlots:null,
    pausedWorkers:[ids[0]+'/full-crawl-1'],prefix:'full-dynamic-'+randomUUID(),
    connection:{host:'127.0.0.1',port,password:'remote-center-fixture-only',maxRetriesPerRequest:null},
    createRuntime:()=>({readyForTasks:()=>true}),createProcessor:()=>async()=>assert.fail('no jobs queued'),
    recoverSlot:async()=>({settled:true}),slotUnsettled:async()=>false,settleHandoffs:false,
    createRota:args=>({workerId:args.workerId,workerInstanceId:args.workerInstanceId,start:async()=>{},close:async()=>{},
      status:()=>({started:true,assignment:{ready:true}})})};
  const make=()=>{
    supervisor=new RemoteCenterExecutionSupervisor(options);
    return createRemoteDeploymentAdmin({store:f.store,routes:runtime.transport.routes,gatewayUrl,
      token:'fixture-admin-token-'+'a'.repeat(32),image:'fixture/incremental@sha256:'+'a'.repeat(64),
      fullCrawl:{...runtime,execution:supervisor}});
  };
  const plan=(i,count)=>buildNodeCollectDeployment({node:{id:ids[i],kind:'execution',workerRole:'fullcrawl',
    provisioning:{state:'ready'},runtime:{state:'ready'},workers:[{role:'fullcrawl',count}]},
    deploymentId:deployments[i],image,gatewayUrl,natsUrl:'tls://fixture.example:4222'});
  const prepare=(admin,p)=>admin.prepare({nodeId:p.nodeId,deploymentId:p.deploymentId,image:p.image,files:p.files});
  const heartbeats=new Map();
  const connect=async p=>{
    for(const r of p.registrations){
      const key=p.nodeId+'/'+r.slot;
      if(!heartbeats.has(key))heartbeats.set(key,{version:1,mode:'full_crawl_collect',node_id:p.nodeId,slot:r.slot,
        deployment_id:p.deploymentId,config_hash:r.configHash,instance_id:randomUUID(),relay_boot_id:'c'.repeat(48),
        runtime_revision:'youtubejs-full-crawl-v1',accepting:true});
      await runtime.activation.heartbeat(p.nodeId,heartbeats.get(key));
    }
  };
  const ready=async expected=>{
    for(let i=0;i<150;i++){
      await supervisor.tick();let count=0;
      for(const h of heartbeats.values())if((await runtime.activation.heartbeat(h.node_id,h)).ready_for_tasks)count++;
      if(count===expected)return;
      await delay(30);
    }
    assert.fail('workers did not reach expected readiness '+expected);
  };
  try{
    let admin=make();
    for(const [i,count] of [[0,7],[1,4]]){
      const p=plan(i,count),credentials=await prepare(admin,p);
      assert.deepEqual(credentials.pausedSlots,i===0?['full-crawl-1']:[]);
      await connect(p);
      const status=await admin.setExecution({nodeId:ids[i],deploymentId:deployments[i],workerCount:count,enabled:true,expectedRequested:false});
      assert.equal(status.allowedCount,count-(i===0?1:0));
    }
    // A live, activated connection with no deployment is not enrolled implicitly.
    await f.query('UPDATE remote_ingestion.worker_connections SET activation_requested=true WHERE node_id=$1',[f.nodeId]);
    await ready(10);
    assert.equal(supervisor.entries.size,10);
    assert.equal(supervisor.entries.has(ids[0]+'/full-crawl-1'),false);
    assert.equal(supervisor.entries.has(ids[1]+'/full-crawl-1'),true,'pause must not affect the same slot on another server');
    assert.equal(supervisor.entries.has(f.nodeId+'/'+f.slot),false,'only registered deployments are admitted');
    const larger=plan(0,8);await prepare(admin,larger);await connect(larger);await ready(11);
    assert.equal((await admin.status({nodeId:ids[0],deploymentId:deployments[0]})).allowedCount,7);
    await supervisor.stop();admin=make();await ready(11);
    assert.equal(supervisor.entries.has(ids[0]+'/full-crawl-1'),false,'center restart retains the pause');
    // Even a stale desired set cannot start the paused process.
    await f.query("UPDATE remote_ingestion.node_intake_requests SET selected_slots=array_append(selected_slots,'full-crawl-1') WHERE node_id=$1",[ids[0]]);
    await ready(11);
    const status=await admin.status({nodeId:ids[0],deploymentId:deployments[0]});
    assert.equal(status.workers.find(w=>w.slot==='full-crawl-1').paused,true);
    assert.equal(status.workers.find(w=>w.slot==='full-crawl-1').requested,false);
    assert.equal(supervisor.entries.has(ids[0]+'/full-crawl-1'),false);
  }finally{
    await supervisor?.stop();
    await releaseGuard.query('SELECT pg_advisory_unlock(781138015,1)');releaseGuard.release();await guardPool.end();
    for(const id of ids){
      await f.query('DELETE FROM remote_ingestion.intake_controls WHERE node_key=$1',[id]);
      for(const table of ['node_intake_requests','node_deployments','worker_connections','network_slots','nodes'])await f.query(`DELETE FROM remote_ingestion.${table} WHERE node_id=$1`,[id]);
    }
  }
});
