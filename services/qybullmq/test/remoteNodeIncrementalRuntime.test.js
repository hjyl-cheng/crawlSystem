import test from 'node:test';
import assert from 'node:assert/strict';
import {randomUUID} from 'node:crypto';
import {setTimeout as delay} from 'node:timers/promises';
import {mkdtemp,writeFile,rm} from 'node:fs/promises';
import {tmpdir,uptime} from 'node:os';
import {join} from 'node:path';
import {RemoteIncrementalProcess,checkNodeIncrementalHealth} from '../src/remoteNodes/nodeIncrementalRuntime.js';
import {parseWorkerConfig} from '../src/remoteNodes/workerConfig.js';

const config=()=>parseWorkerConfig(Buffer.from(JSON.stringify({version:1,mode:'incremental_collect',role:'incremental',
  node_id:randomUUID(),slot:'incremental-1',deployment_id:randomUUID(),gateway_url:'https://center.example'})),{mode:'incremental_collect'});
const ack=(value,ready)=>({...value,state:!value.accepting?'draining':ready?'ready':'connected_waiting_activation',ready_for_tasks:ready&&value.accepting,
  server_time:new Date().toISOString(),connected_until:new Date(Date.now()+45000).toISOString()});
async function until(check){for(let i=0;i<200;i++){if(check())return;await delay(5);}assert.fail('expected state did not arrive');}

test('a real node process waits for activation, stops new claims on disconnect, and drains before exit',{timeout:6000},async()=>{
  const abort=new AbortController();const reports=[];const beats=[];let ready=false;let online=true;let calls=0;let stopped=false;let finishCurrent=false;
  const runner=new RemoteIncrementalProcess({config:config(),intervalMs:50,report:async value=>reports.push(value),
    localRota:{boot:async()=>({boot_id:'b'.repeat(48)})},client:{workerHeartbeat:async value=>{beats.push(value);if(!online)throw new Error('offline');return ack(value,ready);},
      claim:async(_id,_slot,connection)=>{calls++;assert.equal(connection.instance_id,runner.instanceId);return null;}},
    createWorker:({client,slot})=>({stop(){stopped=true;},async run(){while(!stopped){await client.claim(randomUUID(),slot);await delay(5);}while(!finishCurrent)await delay(5);}})});
  const run=runner.run({signal:abort.signal});
  await until(()=>reports.some(r=>r.state==='connected_waiting_activation'));assert.equal(calls,0);
  ready=true;await until(()=>calls>0);
  online=false;await until(()=>reports.at(-1)?.state==='disconnected');const before=calls;await delay(70);assert.equal(calls,before);
  online=true;await until(()=>reports.at(-1)?.state==='ready');
  abort.abort();await until(()=>beats.some(b=>b.accepting===false));
  assert.equal(reports.some(r=>r.state==='stopped'),false,'ongoing work may still finish while draining');
  finishCurrent=true;await run;assert.equal(reports.at(-1).state,'stopped');
});

test('connection-only config cannot enter collecting runtime; mismatched readiness never allows claims',async()=>{
  const c=config();assert.throws(()=>parseWorkerConfig(Buffer.from(JSON.stringify({...c,config_hash:undefined}))));
  assert.throws(()=>new RemoteIncrementalProcess({config:{...c,mode:'connect_only'}}));
  let claim;
  const runner=new RemoteIncrementalProcess({config:c,client:{workerHeartbeat:async value=>({...ack(value,true),config_hash:'f'.repeat(64)}),claim:()=>assert.fail('must not claim')},
    localRota:{boot:async()=>({boot_id:'b'.repeat(48)})},createWorker:({client})=>{claim=()=>client.claim(randomUUID(),c.slot);return {};}});
  await assert.rejects(runner.probe(),/NODE_CONNECTION_RECEIPT_MISMATCH/);assert.equal(await claim(),null);
});

test('health distinguishes ready, connected waiting, draining, disconnected and expired',async t=>{
  const dir=await mkdtemp(join(tmpdir(),'node-collect-health-'));t.after(()=>rm(dir,{recursive:true,force:true}));const file=join(dir,'health.json');
  for(const state of ['ready','connected_waiting_activation','draining']){
    await writeFile(file,JSON.stringify({version:1,state,ready_for_tasks:state==='ready',valid_until_uptime:uptime()+10}));await checkNodeIncrementalHealth(file);
  }
  for(const patch of [{state:'disconnected'},{valid_until_uptime:uptime()-1},{ready_for_tasks:false}]){
    await writeFile(file,JSON.stringify({version:1,state:'ready',ready_for_tasks:true,valid_until_uptime:uptime()+10,...patch}));
    await assert.rejects(checkNodeIncrementalHealth(file));
  }
});

test('health writer failure stops new intake immediately and waits for current work to drain',async()=>{
  let stopped=false;let finish=false;let calls=0;
  const runner=new RemoteIncrementalProcess({config:config(),intervalMs:50,
    client:{workerHeartbeat:async value=>ack(value,true),claim:async()=>{calls++;return null;}},
    localRota:{boot:async()=>({boot_id:'b'.repeat(48)})},report:async()=>{throw new Error('disk failure');},
    createWorker:({client,slot})=>({stop(){stopped=true;},async run(){while(!stopped){await client.claim(randomUUID(),slot);await delay(5);}while(!finish)await delay(5);}})});
  const run=runner.run({signal:new AbortController().signal});
  const rejection=assert.rejects(run);await until(()=>stopped);
  const before=calls;await delay(60);assert.equal(calls,before);finish=true;await rejection;
});

test('shutdown while a heartbeat is in flight validates the request that was actually sent',async()=>{
  const abort=new AbortController();let release;let entered;let stopped=false;
  const requested=new Promise(resolve=>{entered=resolve;});const pending=new Promise(resolve=>{release=resolve;});
  const runner=new RemoteIncrementalProcess({config:config(),intervalMs:50,
    client:{workerHeartbeat:async value=>{entered();await pending;return ack(value,false);}},localRota:{boot:async()=>({boot_id:'b'.repeat(48)})},
    createWorker:()=>({stop(){stopped=true;},async run(){while(!stopped)await delay(5);}})});
  const run=runner.run({signal:abort.signal});await requested;abort.abort();release();await run;
  assert.equal(runner.fatal,null);
});

test('paused worker sleeps between heartbeats, wakes on activation and can shut down while waiting', {timeout:6000}, async t=>{
  const {RemoteChannelPlanExecutor}=await import('../src/remoteNodes/channelPlanExecutor.js');
  const {RemoteResultSpool}=await import('../src/remoteNodes/spool.js');
  const directory=await mkdtemp(join(tmpdir(),'node-paused-wait-'));t.after(()=>rm(directory,{recursive:true,force:true}));
  let ready=false,beats=0,iterations=0,claims=0;const abort=new AbortController();
  const runner=new RemoteIncrementalProcess({config:config(),intervalMs:50,spool:new RemoteResultSpool({directory}),
    localRota:{boot:async()=>({boot_id:'b'.repeat(48)})},
    client:{transport:'nats',workerHeartbeat:async value=>{beats++;return ack(value,ready);},claim:async()=>{claims++;return null;}},
    createWorker:({client,spool})=>{
      const worker=new RemoteChannelPlanExecutor({client,spool,withSession:()=>assert.fail('no task'),
        youtube:{openChannel:()=>assert.fail('no task'),fetchDetail:()=>assert.fail('no task')}});
      const once=worker.runOnce.bind(worker);worker.runOnce=()=>{iterations++;return once();};return worker;
    }});
  t.after(()=>abort.abort());const run=runner.run({signal:abort.signal});
  await until(()=>beats>=2);await delay(1100);
  assert.equal(iterations,1,'paused loop must sleep, not scan the spool every second');assert.equal(claims,0);
  ready=true;await until(()=>claims>0);
  ready=false;await until(()=>runner.readyUntil===0);await delay(1100);
  const paused=iterations;await delay(1100);assert.equal(iterations,paused);
  const before=Date.now();abort.abort();await run;assert.ok(Date.now()-before<1500,'shutdown wakes a paused executor');
});
