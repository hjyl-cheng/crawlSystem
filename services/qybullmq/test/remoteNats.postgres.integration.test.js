import test from 'node:test';
import assert from 'node:assert/strict';
import {randomUUID,randomBytes} from 'node:crypto';
import {readFile} from 'node:fs/promises';
import {setTimeout as delay} from 'node:timers/promises';
import pg from 'pg';
import {connectNats as connect} from '../src/remoteNodes/natsConnection.js';
import {RemoteNodeStore} from '../src/remoteNodes/store.js';
import {RemoteChannelPlanStore} from '../src/remoteNodes/channelPlanStore.js';
import {createRemoteNatsClient} from '../src/remoteNodes/natsClient.js';
import {startRemoteNatsCenter} from '../src/remoteNodes/natsCenter.js';
import {createTransportSignals} from '../src/remoteNodes/transportSignals.js';
import {assertIsolatedRemoteDatabase} from '../src/remoteNodes/isolation.js';
import {CHANNEL_PLAN_CAPABILITY} from '../src/remoteNodes/channelPlanContract.js';
import {INCREMENTAL_JOB_NAME,INCREMENTAL_QUEUE} from '../src/incrementalPlan.js';
import {encodeResult} from '../src/remoteNodes/protocol.js';
const url=process.env.REMOTE_NATS_TEST_URL;
test('NATS transport uses committed notifications, queued original receipts, and existing node gates',{skip:!url,timeout:60000},async t=>{
 const pool=new pg.Pool({connectionString:process.env.REMOTE_NODE_TEST_DATABASE_URL,max:8});await assertIsolatedRemoteDatabase(pool);
 const guard=await pool.connect();await guard.query('SELECT pg_advisory_lock(781137981)');
 let center,client,signals;const previousFetch=globalThis.fetch;
 t.after(async()=>{globalThis.fetch=previousFetch;await client?.close();await signals?.close();await center?.close();guard.release();await pool.end();});
 for(const f of ['schema.sql','routeSchema.sql','natsSchema.sql'])await pool.query(await readFile(new URL(`../src/remoteNodes/${f}`,import.meta.url),'utf8'));
 await pool.query('TRUNCATE remote_ingestion.nodes CASCADE');
 const store=new RemoteNodeStore({pool});const channels=new RemoteChannelPlanStore({store});
 const nodeId=process.env.REMOTE_NATS_TEST_NODE_ID,token=process.env.REMOTE_NATS_TEST_TOKEN;
 await store.registerNode({nodeId,token,capabilities:[CHANNEL_PLAN_CAPABILITY]});
 const tls={caFile:process.env.REMOTE_NATS_TEST_CA};
 signals=await createTransportSignals({connectionString:process.env.REMOTE_NODE_TEST_DATABASE_URL});
 let nodeHeartbeat=0,grant=0,release=0,checkpoint=0,abandon=0,session=0;
 center=await startRemoteNatsCenter({url,password:process.env.REMOTE_NATS_TEST_PASSWORD,tls,store,channelPlans:channels,signals,resultMaxBytes:32*1024*1024,
  workerConnections:{heartbeat:async(id,value)=>{assert.equal(id,nodeId);nodeHeartbeat++;return value;},claim:(id,p)=>store.claim(id,p.claim_id,p.slot??null)},
  routes:{grant:async()=>{grant++;return {};},release:async()=>{release++;return {};},abandon:async()=>{abandon++;return {};}},
  youtubeSessions:{get:async()=>{session++;return {};},checkpoint:async()=>{checkpoint++;return {};}}});
 client=await createRemoteNatsClient({url,token,nodeId,slot:'incremental-1',tls});
 globalThis.fetch=()=>assert.fail('NATS mode must not issue HTTP requests');
 await Promise.all([client.workerHeartbeat({accepting:true}),client.grantRoute({}),client.releaseRoute({}),client.abandonRoute({}),client.youtubeSession({}),client.youtubeCheckpoint({})]);
 assert.deepEqual([nodeHeartbeat,grant,release,abandon,session,checkpoint],[1,1,1,1,1,1]);
 const planId=randomUUID(),plan={schema_version:5,dispatch_generation:1,job_id:`nats_${planId}`,plan_id:planId,plan_mode:'standard',plan_day:'2026-09-11',scheduled_at:'2026-09-11T01:00:00.000Z',channel_id:`UC${randomUUID().replaceAll('-','').slice(0,22)}`,task_mask:{about:false,video:true,agent:false},capacity:{factor:1,player_cap:20,next_cap:8,version:'capacity-1'},clock_version:7,policy_version:'v16-rule-1',planner_config_version:'video-plan-1'};
 const queued=await channels.enqueue({id:plan.job_id,name:INCREMENTAL_JOB_NAME,queueName:INCREMENTAL_QUEUE,data:plan},{executionAttemptId:'fixture'});
 const lease=await client.claim(randomUUID());assert.equal(lease.task_id,queued.taskId);
 await client.heartbeat(lease);
 const task=(await pool.query('SELECT * FROM remote_ingestion.tasks WHERE task_id=$1',[lease.task_id])).rows[0];
 let pollCount=0;const poll=channels.poll.bind(channels);channels.poll=(...args)=>{pollCount++;return poll(...args);};
 let resolved=false;const pending=client.pollCommands(lease).then(value=>{resolved=true;return value;});
 await delay(350);assert.equal(resolved,false);assert.equal(pollCount,1,'no 100ms polling while waiting');
 const tx=await pool.connect();await tx.query('BEGIN');await channels.request(tx,task,'video_detail',{video_id:'rollback'},'rollback');await delay(100);assert.equal(resolved,false);await tx.query('ROLLBACK');tx.release();
 await delay(100);assert.equal(resolved,false,'rolled-back command must not wake node');
 const commandId=await store.transaction(c=>channels.request(c,task,'video_detail',{video_id:'accepted'},'accepted'));
 assert.equal((await pending).commands[0].command_id,commandId);assert.ok(pollCount<=3,'notification wakes the waiting node');
 const envelope={version:1,generation:lease.generation,batch_id:randomUUID(),command_id:commandId,outcome:'success',data:{title:'NATS video',body:randomBytes(16000).toString('hex')}};
 const payload=await encodeResult(envelope);
 const result=await client.uploadCommand(lease,payload);assert.equal(result.durable,true);assert.equal(result.command_id,commandId);
 assert.equal((await pool.query('SELECT state FROM remote_ingestion.channel_commands WHERE command_id=$1',[commandId])).rows[0].state,'received');
 assert.deepEqual(await client.uploadCommand(lease,payload),result);
 assert.equal((await pool.query('SELECT count(*)::int AS n FROM remote_ingestion.transport_receipts')).rows[0].n,1);
 await assert.rejects(client.uploadCommand(lease,await encodeResult({...envelope,data:{title:'conflict'}})),{code:'BATCH_CONFLICT'});
 await assert.rejects(client.uploadCommand(lease,await encodeResult({...envelope,generation:lease.generation+1})),{code:'STALE_LEASE'});
 // Restart the center consumer, keeping the broker and existing SQL receipt.
 await center.close();
 center=await startRemoteNatsCenter({url,password:process.env.REMOTE_NATS_TEST_PASSWORD,tls,store,channelPlans:channels,signals,resultMaxBytes:32*1024*1024});
 assert.deepEqual(await client.uploadCommand(lease,payload),result);
 const stats=await center.stats();assert.equal(stats.pending,0);
 // Broker namespace cannot be bypassed with another node's subject.
 const malicious=await connect({servers:url,user:nodeId,pass:token,tls,inboxPrefix:`_INBOX.${nodeId}.acl`});
 try{await assert.rejects(malicious.request(`qy.remote.rpc.${randomUUID()}.heartbeat`,Buffer.from('{}'),{timeout:500}),/permission|timeout/i);}finally{await malicious.close();}
 await store.setNodeState(nodeId,'disabled');await assert.rejects(client.heartbeat(lease),{code:'UNAUTHORIZED'});
});
