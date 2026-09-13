// ISOLATED EXPERIMENT. Real JetStream + the existing SQL receipt writer.
// This is not a production transport or a replacement Clock/Plan scheduler.
import assert from 'node:assert/strict';
import { randomUUID, randomBytes } from 'node:crypto';
import { readFile, writeFile, mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { execFileSync } from 'node:child_process';
import { setTimeout as delay } from 'node:timers/promises';
import { performance, monitorEventLoopDelay } from 'node:perf_hooks';
import pg from 'pg';
import { connect } from '@nats-io/transport-node';
import { jetstream, jetstreamManager, AckPolicy, DiscardPolicy, RetentionPolicy, StorageType } from '@nats-io/jetstream';
import { RemoteNodeStore } from '../../../qybullmq/src/remoteNodes/store.js';
import { RemoteChannelPlanStore } from '../../../qybullmq/src/remoteNodes/channelPlanStore.js';
import { RemoteResultSpool } from '../../../qybullmq/src/remoteNodes/spool.js';
import { assertIsolatedRemoteDatabase } from '../../../qybullmq/src/remoteNodes/isolation.js';
import { incrementalPlanHash, INCREMENTAL_JOB_NAME, INCREMENTAL_QUEUE } from '../../../qybullmq/src/incrementalPlan.js';
import { CHANNEL_PLAN_CAPABILITY } from '../../../qybullmq/src/remoteNodes/channelPlanContract.js';
import { encodeResult, decodeResult } from '../../../qybullmq/src/remoteNodes/protocol.js';
const ns = ms => ms * 1e6;
const json = value => Buffer.from(JSON.stringify(value));
const url = process.env.PROBE_NATS_URL;
if (!/^nats:\/\/127\.0\.0\.1:\d+$/.test(url ?? '') || !/^qy-nats-probe-\d+-\d+$/.test(process.env.PROBE_COMPOSE_PROJECT ?? '')) throw Error('Use npm test to create an isolated environment');
const pool = new pg.Pool({connectionString:process.env.PROBE_PG_URL,max:8,connectionTimeoutMillis:5000});
const connections = [];const reconnects = [];const expectedPermissionErrors = [];
const report = {at:new Date().toISOString(),scope:'single broker, loopback, 0.5 CPU broker + 0.5 CPU PostgreSQL; synthetic video payloads; existing receipt writer',checks:{},measurements:{}};
const root = await mkdtemp(join(tmpdir(),'qy-nats-spool-'));
const loop = monitorEventLoopDelay({resolution:10});loop.enable();
let timer;let maxPoolWaiting=0;let keepHeartbeats=false;
async function connection(user='center') {
 const nc=await connect({servers:url,user,pass:`isolated-${user}-only`,inboxPrefix:`_INBOX.${user}`,maxReconnectAttempts:40,reconnectTimeWait:100,reconnectJitter:100,timeout:1000});
 connections.push(nc);
 void (async()=>{for await (const status of nc.status()) {
  if(status.type==='reconnect') reconnects.push(user);
  if(status.type==='error') expectedPermissionErrors.push({user,error:String(status.error ?? status.data ?? status)});
 }})();
 return nc;
}
function mark(name, value=true) { report.checks[name]=value;console.log(JSON.stringify({check:name,result:value})); }
const percentile=(values,p)=>[...values].sort((a,b)=>a-b)[Math.min(values.length-1,Math.floor(values.length*p))] ?? 0;
const round=value=>Math.round(value*10)/10;
async function parallel(items,count,fn) {
 let index=0;await Promise.all(Array.from({length:Math.min(count,items.length)},async()=>{while(index<items.length){const i=index++;await fn(items[i],i);}}));
}
try {
 await assertIsolatedRemoteDatabase(pool);
 for(const path of ['schema.sql','routeSchema.sql']) await pool.query(await readFile(new URL(`../../../qybullmq/src/remoteNodes/${path}`,import.meta.url),'utf8'));
 const store=new RemoteNodeStore({pool,leaseSeconds:600});const channelStore=new RemoteChannelPlanStore({store});
 const center=await connection();const js=jetstream(center);const jsm=await jetstreamManager(center);
 report.server_version=center.info.version;
 const stream={storage:StorageType.File,retention:RetentionPolicy.Workqueue,discard:DiscardPolicy.New,max_bytes:32*1024*1024,max_msg_size:1024*1024,num_replicas:1,duplicate_window:ns(60000)};
 await jsm.streams.add({...stream,name:'PROBE_COMMANDS',subjects:['probe.commands.*']});
 await jsm.streams.add({...stream,name:'PROBE_RESULTS',subjects:['probe.results.*']});
 await jsm.consumers.add('PROBE_RESULTS',{durable_name:'center',ack_policy:AckPolicy.Explicit,ack_wait:ns(2000),max_ack_pending:32});
 const nodes=[];const fixtures=[];
 for(const user of ['node1','node2']) {
  const nodeId=randomUUID();await store.registerNode({nodeId,token:randomBytes(32).toString('hex'),capabilities:[CHANNEL_PLAN_CAPABILITY],maxLeases:100});
  await jsm.consumers.add('PROBE_COMMANDS',{durable_name:user,filter_subject:`probe.commands.${user}`,ack_policy:AckPolicy.Explicit,ack_wait:ns(30000),max_ack_pending:50});
  const nc=await connection(user);nodes.push({user,nodeId,nc,js:jetstream(nc)});
 }
 const payload=randomBytes(16384).toString('base64');
 for(let i=0;i<100;i++) {
  const node=nodes[Math.floor(i/50)];const planId=randomUUID();
  const plan={schema_version:5,dispatch_generation:1,job_id:`probe_${planId}`,plan_id:planId,plan_mode:'standard',plan_day:'2026-09-11',scheduled_at:'2026-09-11T01:00:00.000Z',channel_id:`UC${randomUUID().replaceAll('-','').slice(0,22)}`,task_mask:{about:false,video:true,agent:false},capacity:{factor:1,player_cap:20,next_cap:8,version:'capacity-1'},clock_version:7,policy_version:'v16-rule-1',planner_config_version:'video-plan-1'};
  await channelStore.enqueue({id:plan.job_id,name:INCREMENTAL_JOB_NAME,queueName:INCREMENTAL_QUEUE,data:plan},{executionAttemptId:`probe-${planId}`});
  const lease=await store.claim(node.nodeId,randomUUID());assert.ok(lease);
  const task=(await pool.query('SELECT * FROM remote_ingestion.tasks WHERE task_id=$1',[lease.task_id])).rows[0];
  const commandId=await store.transaction(client=>channelStore.request(client,task,'video_detail',{video_id:`fixture-${i}`},'once'));
  const envelope={version:1,generation:lease.generation,batch_id:randomUUID(),command_id:commandId,task_id:lease.task_id,outcome:'success',data:{title:`fixture-${i}`,payload}};
  fixtures.push({node,lease,commandId,envelope,bytes:await encodeResult(envelope)});
 }
 const byCommand=new Map(fixtures.map(f=>[f.commandId,f]));
 async function apply(msg) {
  const {value}=await decodeResult(msg.data);const node=nodes.find(n=>msg.subject===`probe.results.${n.user}`);assert.ok(node,'identity comes from broker-authorized subject');
  return channelStore.receive(node.nodeId,{task_id:value.task_id},Buffer.from(msg.data));
 }
 const receiver=await js.consumers.get('PROBE_RESULTS','center');
 async function drain(count,concurrency=8) {
  const messages=await receiver.fetch({max_messages:count,expires:15000});const work=new Set();let seen=0;
  for await (const msg of messages) {
   const p=(async()=>{await apply(msg);assert.equal(await msg.ackAck(),true);seen++;})();work.add(p);p.finally(()=>work.delete(p)).catch(()=>{});
   if(work.size>=concurrency)await Promise.race(work);
  }
  await Promise.all(work);assert.equal(seen,count);
 }
 // Each logical worker receives the exact command for its existing leased Plan.
 const start=performance.now();const receiveDone=drain(100);
 const nodeWork=nodes.map(async node=>{
  const consumer=await node.js.consumers.get('PROBE_COMMANDS',node.user);const messages=await consumer.fetch({max_messages:50,expires:15000});const pending=[];
  for await(const msg of messages){const command=msg.json();const f=byCommand.get(command.command_id);assert.equal(f.node.nodeId,node.nodeId);
   pending.push((async()=>{await node.js.publish(`probe.results.${node.user}`,f.bytes,{msgID:f.envelope.batch_id});await msg.ackAck();})());}
  await Promise.all(pending);assert.equal(pending.length,50);
 });
 await parallel(fixtures,32,async f=>js.publish(`probe.commands.${f.node.user}`,json({command_id:f.commandId,task_id:f.lease.task_id,generation:f.lease.generation}),{msgID:f.commandId}));
 await Promise.all([...nodeWork,receiveDone]);
 report.measurements.command_result_roundtrip={workers:100,node_connections:2,total_ms:round(performance.now()-start)};
 assert.equal((await pool.query("SELECT count(*)::int AS n FROM remote_ingestion.channel_commands WHERE state='received'")).rows[0].n,100);
 mark('100 concurrent logical workers: command delivery and existing SQL result writer');
 // Test transport-level deduplication, then business-level deduplication even
 // when the transport message ID changes (including outside its time window).
 const f=fixtures[0];const duplicate=await f.node.js.publish('probe.results.node1',f.bytes,{msgID:f.envelope.batch_id});assert.equal(duplicate.duplicate,true);
 mark('JetStream publication deduplication');
 const loadFixtures=[];
 await parallel(fixtures,8,async item=>{
  const task=(await pool.query('SELECT * FROM remote_ingestion.tasks WHERE task_id=$1',[item.lease.task_id])).rows[0];
  for(let i=0;i<10;i++) {
   const commandId=await store.transaction(client=>channelStore.request(client,task,'video_detail',{video_id:`load-${i}`},`load-${i}`));
   const envelope={...item.envelope,command_id:commandId,batch_id:randomUUID()};
   loadFixtures.push({...item,commandId,envelope,bytes:await encodeResult(envelope)});
  }
 });
 const receiveLatencies=[];const heartbeatLatencies=[];let heartbeatErrors=0;keepHeartbeats=true;
 const hbSub=center.subscribe('probe.heartbeat.*');
 const hbResponder=(async()=>{for await(const msg of hbSub){try{const node=nodes.find(n=>msg.subject===`probe.heartbeat.${n.user}`);const value=msg.json();await store.heartbeat(node.nodeId,value.task_id,value.generation);msg.respond(json({ok:true}));}catch{msg.respond(json({ok:false}));}}})();
 await center.flush();
 const heartbeatLoop=(async()=>{while(keepHeartbeats){const t=performance.now();try{assert.equal((await nodes[0].nc.request('probe.heartbeat.node1',json(f.lease),{timeout:2000})).json().ok,true);heartbeatLatencies.push(performance.now()-t);}catch{heartbeatErrors++;}await delay(25);}})();
 timer=setInterval(()=>{maxPoolWaiting=Math.max(maxPoolWaiting,pool.waitingCount)},10);
 const loadStart=performance.now();const samples=1000;
 const consumeLoad=drain(samples,8);
 await parallel(loadFixtures,100,async(item,index)=>{
  const t=performance.now();await item.node.js.publish(`probe.results.${item.node.user}`,item.bytes,{msgID:`load-${index}`});receiveLatencies.push(performance.now()-t);
 });
 const publishMs=performance.now()-loadStart;await consumeLoad;const totalMs=performance.now()-loadStart;
 keepHeartbeats=false;await heartbeatLoop;report.measurements.event_loop_p99_ms=round(loop.percentile(99)/1e6);loop.disable();hbSub.unsubscribe();await hbResponder;
 assert.equal(heartbeatErrors,0);assert.ok(heartbeatLatencies.length>0);assert.ok(Math.max(...heartbeatLatencies)<2000);
 assert.equal((await pool.query("SELECT count(*)::int AS n FROM remote_ingestion.channel_commands WHERE state='received'")).rows[0].n,1100);
 report.measurements.load={messages:samples,concurrent_publishers:100,compressed_bytes:f.bytes.length,publish_ack_per_second:round(samples*1000/publishMs),receipt_apply_per_second:round(samples*1000/totalMs),publish_ack_p95_ms:round(percentile(receiveLatencies,.95)),heartbeat_count:heartbeatLatencies.length,heartbeat_p95_ms:round(percentile(heartbeatLatencies,.95)),heartbeat_max_ms:round(Math.max(...heartbeatLatencies)),heartbeat_errors:heartbeatErrors};
 mark('1000 deliveries, bounded consumer concurrency, heartbeat SQL renewal, no duplicate result writes');
 // Deliberately commit SQL without queue acknowledgement, then close the
 // consumer connection. A replacement process must receive and apply safely.
 await f.node.js.publish('probe.results.node1',f.bytes,{msgID:'lost-ack'});
 const doomed=await connection();const doomedConsumer=await jetstream(doomed).consumers.get('PROBE_RESULTS','center');
 const lost=await doomedConsumer.next({expires:5000});assert.ok(lost);await apply(lost);await doomed.close();
 const redelivery=await receiver.next({expires:5000});assert.ok(redelivery);assert.ok(redelivery.info.deliveryCount>=2);await apply(redelivery);assert.equal(await redelivery.ackAck(),true);
 mark('SQL commit followed by consumer disconnect before ACK: replay without duplicate write');
 // Both a stale execution generation and altered data with the same receipt
 // identity must still be rejected by the unchanged production receipt writer.
 for(const [name,envelope,code] of [
  ['stale generation',{...f.envelope,generation:f.envelope.generation+1},'STALE_LEASE'],
  ['conflicting result',{...f.envelope,data:{title:'changed'}},'BATCH_CONFLICT'],
 ]) {
  await f.node.js.publish('probe.results.node1',await encodeResult(envelope),{msgID:randomUUID()});const msg=await receiver.next({expires:5000});
  await assert.rejects(apply(msg),{code});msg.term(code);await center.flush();mark(name+' rejected by existing writer');
 }
 // Subject ACLs forbid one node from impersonating another or consuming its commands.
 await assert.rejects(nodes[0].js.publish('probe.results.node2',f.bytes,{timeout:1000}),/permissions|timeout/i);
 await assert.rejects(nodes[0].js.consumers.get('PROBE_COMMANDS','node2'),/permissions|timeout/i);
 mark('node subject permissions prevent cross-node result publication and command access');
 // With the consumer stopped, results must accumulate durably and later drain.
 await parallel(fixtures.slice(0,40),16,item=>item.node.js.publish(`probe.results.${item.node.user}`,item.bytes,{msgID:randomUUID()}));
 assert.equal((await jsm.consumers.info('PROBE_RESULTS','center')).num_pending,40);
 const beforeRestart=(await jsm.streams.info('PROBE_RESULTS')).state.messages;assert.equal(beforeRestart,40);
 const spool=new RemoteResultSpool({directory:join(root,'node1')});await spool.init();
 await spool.save('pending.json',json({task_id:f.lease.task_id,payload:f.bytes.toString('base64')}));
 // Use SIGKILL so persistence/reconnection is checked beyond graceful shutdown.
 const compose=['compose','-f',new URL('./compose.yml',import.meta.url).pathname,'-p',process.env.PROBE_COMPOSE_PROJECT];
 execFileSync('docker',[...compose,'kill','-s','SIGKILL','broker'],{stdio:'pipe',timeout:15000});
 await assert.rejects(nodes[0].js.publish('probe.results.node1',f.bytes,{msgID:'offline-spool',timeout:300}));
 assert.ok(await spool.read('pending.json'),'failed publish must not remove local spool');
 execFileSync('docker',[...compose,'start','broker'],{stdio:'pipe',timeout:15000});
 for(let i=0;i<100&&!reconnects.includes('node1');i++)await delay(100);
 assert.ok(reconnects.includes('node1'));
 assert.ok((await jsm.streams.info('PROBE_RESULTS')).state.messages>=40);
 await drain(40);
 // The timed-out publish may have reached the server during reconnection:
 // replay uses the same ID and SQL remains idempotent either way.
 const pending=await spool.read('pending.json');await nodes[0].js.publish('probe.results.node1',Buffer.from(pending.payload,'base64'),{msgID:'offline-spool'});await spool.remove('pending.json');
 await drain(1);assert.equal(await spool.read('pending.json'),null);
 assert.equal((await jsm.consumers.info('PROBE_RESULTS','center')).num_pending,0);
 assert.equal((await jsm.consumers.info('PROBE_RESULTS','center')).num_ack_pending,0);
 mark('broker SIGKILL/restart: durable backlog, client reconnect, local spool replay');
 // A full stream refuses new publications instead of silently dropping old data.
 await jsm.streams.add({name:'PROBE_LIMIT',subjects:['probe.limit'],storage:StorageType.File,max_bytes:4096,max_msgs:2,discard:DiscardPolicy.New});
 await js.publish('probe.limit',json({i:1}));await js.publish('probe.limit',json({i:2}));await assert.rejects(js.publish('probe.limit',json({i:3})),/maximum|max messages/i);
 assert.equal((await jsm.streams.info('PROBE_LIMIT')).state.messages,2);
 mark('full queue rejects new message without evicting previously acknowledged data');
 report.measurements.max_pool_waiting=maxPoolWaiting;
 report.measurements.client_rss_mib=round(process.memoryUsage().rss/1024/1024);
 report.resources=execFileSync('docker',['stats','--no-stream','--format','{{.Name}} {{.MemUsage}}',...execFileSync('docker',[...compose,'ps','-q'],{encoding:'utf8'}).trim().split(/\s+/)],{encoding:'utf8'}).trim().split('\n');
 report.checks.final_sql_received=(await pool.query("SELECT count(*)::int AS n FROM remote_ingestion.channel_commands WHERE state='received'")).rows[0].n;
 assert.equal(report.checks.final_sql_received,1100);
 await writeFile(new URL('./results.json',import.meta.url),JSON.stringify(report,null,2)+'\n');console.log(JSON.stringify({verdict:'PASS',...report}));
} finally {
 keepHeartbeats=false;clearInterval(timer);loop.disable();await Promise.allSettled(connections.map(nc=>nc.close()));await pool.end();await rm(root,{recursive:true,force:true});
}
