import test from 'node:test';
import assert from 'node:assert/strict';
import {setTimeout as delay} from 'node:timers/promises';
import {RemoteManagedIncrementalRuntime} from '../src/remoteNodes/managedIncrementalRuntime.js';
import {RemoteCenterExecutionSupervisor} from '../src/remoteNodes/centerExecutionSupervisor.js';
import {ExecutionProgress} from '../src/remoteNodes/executionProgress.js';
import {incrementalProgressConfig} from '../src/remoteNodes/incrementalProgressConfig.js';
import {intakeStatus} from '../src/remoteNodes/intakeSelection.js';
import {createRemoteRotaChannelRuntime} from '../src/remoteNodes/rotaChannelRuntimeAdapter.js';

const deferred=()=>{let resolve;const promise=new Promise(r=>{resolve=r;});return {promise,resolve};};
async function runtimeFixture({prepare,stop,bindings=async()=>[],stopTimeoutMs=25}={}) {
  const runtime=new RemoteManagedIncrementalRuntime({channelStore:{store:{pool:{}}},youtubeSessions:{routes:{store:{}}},profileSecret:'test',nodeId:'node',slot:'slot',stopTimeoutMs});
  const calls=[];
  runtime.executions={pendingCountryHandoff:async()=>null,prepare:prepare??(async()=>({taskId:'task',attemptId:'channel-attempt:rota-task'})),
    waitClaim:async(admission,{signal})=>{signal.throwIfAborted();return new Promise((resolve,reject)=>signal.addEventListener('abort',()=>reject(signal.reason),{once:true}));},
    stop:stop??(async()=>calls.push('stop')),find:async()=>({taskId:'task',attemptId:'channel-attempt:rota-task'}),bindings,
    finish:async()=>calls.push('finish')};
  runtime.routes={requestStop:async()=>calls.push('network_stop'),waitQuiesced:async()=>{calls.push('quiet');return {active_managed_requests:0};}};
  const handle=await runtime.acquire({task:{task_id:'rota-task'},abortSignal:new AbortController().signal});
  const executing=handle.execute({job:{id:'job'},attempt:{resumeMode:'initial'}},()=>{}).catch(error=>error);
  await delay(0);
  return {runtime,handle,executing,calls};
}
test('attempt cancellation settles claim, preserves cleanup visibility and ignores stale IDs',async()=>{
  const f=await runtimeFixture();
  assert.equal(f.runtime.executionSnapshot().executionPhase,'awaiting_claim');
  assert.equal(f.runtime.requestAbort('old'),'superseded');
  assert.equal(f.runtime.requestAbort('channel-attempt:rota-task'),'requested');
  assert.equal(f.runtime.requestAbort('channel-attempt:rota-task'),'already_requested');
  assert.equal((await f.executing).code,'REMOTE_EXECUTION_OVERDUE');
  assert.equal(f.runtime.active,null);assert.ok(f.runtime.executionSnapshot());
  await f.runtime.quiesce(f.handle);assert.deepEqual(f.calls,['stop','finish']);
  assert.equal(f.runtime.executionSnapshot().executionPhase,'recovering');
  await f.runtime.retire(f.handle);assert.equal(f.runtime.executionSnapshot().executionPhase,'finished');
  assert.equal(f.runtime.requestAbort('channel-attempt:rota-task'),'no_execution');
});
test('cancellation during an uncertain admission waits for the original commit and does not bind',async()=>{
  const prepare=deferred();const f=await runtimeFixture({prepare:()=>prepare.promise});
  f.runtime.requestAbort('channel-attempt:rota-task');
  prepare.resolve({taskId:'task',attemptId:'channel-attempt:rota-task'});
  assert.equal((await f.executing).code,'REMOTE_EXECUTION_OVERDUE');assert.equal(f.handle.inner,null);
  await f.runtime.quiesce(f.handle);assert.deepEqual(f.calls,['stop','finish']);
});
test('a hung stop becomes blocked without a second cleanup or fabricated completion',async()=>{
  const pending=deferred();let stops=0;
  const f=await runtimeFixture({stop:()=>{stops++;return pending.promise;}});
  f.runtime.requestAbort('channel-attempt:rota-task');await f.executing;
  await assert.rejects(f.runtime.quiesce(f.handle),{code:'REMOTE_CLEANUP_TIMEOUT'});
  assert.equal(f.runtime.executionSnapshot().executionPhase,'blocked');assert.equal(stops,1);assert.deepEqual(f.calls,[]);
  await assert.rejects(f.runtime.quiesce(f.handle),{code:'REMOTE_CLEANUP_TIMEOUT'});
  pending.resolve();await f.handle.quiescence;await f.runtime.quiesce(f.handle);
  assert.equal(stops,1);assert.deepEqual(f.calls,['finish']);
});
test('a missing inner handle still requires persisted network retirement',async()=>{
  const f=await runtimeFixture({bindings:async()=>[{binding_id:'persisted',state:'active'}]});
  f.runtime.requestAbort('channel-attempt:rota-task');await f.executing;
  await f.runtime.quiesce(f.handle);assert.deepEqual(f.calls,['stop','network_stop','quiet','finish']);
});
test('network retirement failure never finishes the attempt',async()=>{
  const f=await runtimeFixture({bindings:async()=>[{binding_id:'persisted',state:'active'}]});
  f.runtime.routes.waitQuiesced=async()=>{throw new Error('network not quiet');};
  f.runtime.requestAbort('channel-attempt:rota-task');await f.executing;
  await assert.rejects(f.runtime.quiesce(f.handle),/network not quiet/);
  assert.equal(f.calls.includes('finish'),false);assert.equal(f.runtime.executionSnapshot().progressHealth,'blocked');
});
test('managed cleanup and its real adapter stop each persisted binding once while still applying checkpoint',async()=>{
  const bindings=[{binding_id:'inner',state:'active'},{binding_id:'uncertain',state:'active'}];
  const f=await runtimeFixture({bindings:async()=>bindings,stopTimeoutMs:1000});
  f.runtime.requestAbort('channel-attempt:rota-task');await f.executing;
  const stops=[],waits=[];let checkpoints=0;
  f.runtime.routes.requestStop=async id=>stops.push(id);
  f.runtime.routes.waitQuiesced=async id=>{waits.push(id);return {active_managed_requests:0};};
  f.handle.adapter=createRemoteRotaChannelRuntime({routes:f.runtime.routes,nodeId:'node',lease:{task_id:'task',generation:1},slot:'slot',
    quiesceBinding:f.handle.quiesceBinding,youtubeSessions:{result:async()=>({metrics:{requests:2}})},youtubeSession:{},
    youtubeCheckpointConsumer:{apply:async()=>{checkpoints++;}}});
  f.handle.inner={binding:bindings[0]};
  await f.runtime.quiesce(f.handle);await f.runtime.quiesce(f.handle);
  assert.deepEqual(stops,['inner','uncertain']);assert.deepEqual(waits,['inner','uncertain']);
  assert.equal(checkpoints,1);assert.equal(f.calls.filter(x=>x==='finish').length,1);
});
test('uncertain bind response is recovered without duplicate stop, and failed checkpoint retries only checkpoint',async()=>{
  const binding={binding_id:'persisted',state:'active'};
  const f=await runtimeFixture({bindings:async()=>[binding],stopTimeoutMs:1000});
  f.runtime.requestAbort('channel-attempt:rota-task');await f.executing;
  let stops=0,waits=0,checkpoints=0;
  f.runtime.routes.requestStop=async()=>{stops++;};
  f.runtime.routes.waitQuiesced=async()=>{waits++;return {active_managed_requests:0};};
  f.runtime.routes.bindingForExecution=async()=>binding;
  f.handle.adapter=createRemoteRotaChannelRuntime({routes:f.runtime.routes,nodeId:'node',lease:{task_id:'task',generation:1},slot:'slot',
    quiesceBinding:f.handle.quiesceBinding,youtubeSessions:{result:async()=>null},youtubeSession:{},
    youtubeCheckpointConsumer:{apply:async()=>{if(++checkpoints===1)throw new Error('checkpoint unavailable');}}});
  f.handle.inner={binding:null};
  await assert.rejects(f.runtime.quiesce(f.handle),/checkpoint unavailable/);
  assert.equal(f.calls.includes('finish'),false);
  await f.runtime.quiesce(f.handle);
  assert.equal(stops,1);assert.equal(waits,1);assert.equal(checkpoints,2);assert.equal(f.handle.finished,true);
});
test('progress uses monotonic stage time; long collection is not automatically aborted',()=>{
  let now=0;const p=new ExecutionProgress({attemptId:'a',claimTimeoutMs:30,stopTimeoutMs:45,now:()=>now});
  p.move('awaiting_claim','query');now=31;assert.equal(p.snapshot().canAbort,true);
  const seq=p.snapshot().progressSequence;p.snapshot();assert.equal(p.snapshot().progressSequence,seq);
  p.move('collecting');now=99999;assert.equal(p.snapshot().canAbort,false);
  p.cancel('timeout');now+=46;assert.equal(p.snapshot().progressHealth,'blocked');
});
function supervisorFixture(mode='enforce') {
  const row={node_id:'11111111-1111-4111-8111-111111111111',slot:'incremental-1',activation_requested:true,enabled:true};
  const config={...incrementalProgressConfig(),mode,allowlist:[`${row.node_id}/${row.slot}`]};
  const supervisor=new RemoteCenterExecutionSupervisor({store:{},channelStore:{},activation:{},guardPool:{},connection:{},prefix:'test',allowedNodeIds:[row.node_id],incrementalProgress:config});
  const snapshot={attemptId:'attempt',executionPhase:'awaiting_claim',progressHealth:'overdue',canAbort:true};
  let aborted=0,paused=0;
  const entry={row,owned:true,processing:true,queueReady:true,runtime:{executionSnapshot:()=>snapshot,requestAbort:()=>{aborted++;return 'requested';}},
    rota:{status:()=>({started:true})},worker:{pause:async()=>{paused++;},opts:{name:'test'},toKey:v=>v,client:Promise.resolve({set:async()=>{}})}};
  supervisor.entries.set(`${row.node_id}/${row.slot}`,entry);
  return {supervisor,entry,snapshot,aborted:()=>aborted,paused:()=>paused};
}
test('watchdog aborts an active overdue owner once, keeps cleanup readiness and blocks only new intake',async()=>{
  const f=supervisorFixture();f.supervisor.inspectProgress();f.supervisor.inspectProgress();await delay(0);
  assert.equal(f.aborted(),1);assert.equal(f.paused(),1);assert.equal(f.entry.blocked,undefined);
  assert.equal(await f.supervisor.ready(f.entry),false);
  f.snapshot.executionPhase='finished';f.snapshot.canAbort=false;f.entry.processing=false;
  f.supervisor.inspectProgress();assert.equal(f.entry.progressRecovery,null);
});
test('observe, non-allowlisted slots and lost owners cannot trigger cancellation',()=>{
  for(const kind of ['observe','other_slot','lost_owner']){
    const f=supervisorFixture(kind==='observe'?'observe':'enforce');
    if(kind==='other_slot')f.supervisor.incrementalProgress.allowlist=[];
    if(kind==='lost_owner')f.entry.owned=false;
    f.supervisor.inspectProgress();assert.equal(f.aborted(),0,kind);
  }
});
test('dashboard does not count an active blocked worker as normal processing',()=>{
  const status=intakeStatus([{requested:true,connected:true,processing:true,active:true,progressHealth:'blocked'}]);
  assert.equal(status.counts.blocked,1);assert.equal(status.counts.processing,0);assert.equal(status.counts.collecting,0);
});
test('enforce requires a valid explicit slot allowlist and validated deadlines',()=>{
  assert.throws(()=>incrementalProgressConfig({REMOTE_INCREMENTAL_PROGRESS_MODE:'enforce'}),/allowlist/);
  assert.throws(()=>incrementalProgressConfig({REMOTE_INCREMENTAL_READ_TIMEOUT_MS:'NaN'}),/invalid/);
  assert.equal(incrementalProgressConfig().mode,'observe');
});

test('late cancellation from an older attempt cannot affect the next handle',async()=>{
  const f=await runtimeFixture();f.runtime.requestAbort('channel-attempt:rota-task');await f.executing;
  await f.runtime.retire(f.handle);
  const next=await f.runtime.acquire({task:{task_id:'new-rota-task'},abortSignal:new AbortController().signal});
  f.runtime.executions.prepare=async()=>({taskId:'task',attemptId:'channel-attempt:new-rota-task'});
  const running=next.execute({job:{id:'job'},attempt:{}},()=>{}).catch(e=>e);await delay(0);
  assert.equal(f.runtime.requestAbort('channel-attempt:rota-task'),'superseded');assert.equal(next.controller.signal.aborted,false);
  f.runtime.requestAbort('channel-attempt:new-rota-task');await running;await f.runtime.retire(next);
});
test('local finalization cannot turn nonzero network activity into a cached quiet result',async()=>{
  const f=await runtimeFixture();f.runtime.requestAbort('channel-attempt:rota-task');await f.executing;
  f.handle.inner={};f.handle.adapter={quiesce:async()=>({active_managed_requests:1})};
  await assert.rejects(f.runtime.quiesce(f.handle),{code:'REMOTE_NETWORK_NOT_QUIESCED'});
  assert.equal(f.handle.finished,false);assert.equal(f.calls.includes('finish'),false);
});
test('finished cleanup does not reopen intake while original Rota completion is unresolved',()=>{
  const f=supervisorFixture();f.supervisor.inspectProgress();f.entry.processing=false;
  f.snapshot.executionPhase='finished';f.snapshot.canAbort=false;
  f.entry.rota.status=()=>({started:true,recovery_pending:true});f.supervisor.inspectProgress();
  assert.equal(f.entry.progressRecovery,'attempt');
});
test('an overdue collecting phase raises a diagnostic without automatic cancellation',()=>{
  let now=0;const p=new ExecutionProgress({attemptId:'a',claimTimeoutMs:30,stopTimeoutMs:45,now:()=>now});
  p.move('collecting');now=16*60*1000;
  assert.equal(p.snapshot().progressHealth,'overdue');assert.equal(p.snapshot().canAbort,false);
});
