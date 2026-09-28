import test from 'node:test';
import assert from 'node:assert/strict';
import {createRecoveringNatsConnection} from '../src/remoteNodes/natsRecovery.js';

const deferred=()=>{let resolve,reject;const promise=new Promise((a,b)=>{resolve=a;reject=b;});return {promise,resolve,reject};};
const client=()=>({closed:false,isClosed(){return this.closed;},flush:async()=>{}});
const setup=async(options={})=>{
  const first=client(),second=client(),reasons=[];let opens=0;
  const manager=await createRecoveringNatsConnection({connect:async()=>++opens===1?first:second,
    dispose:async nc=>{nc.closed=true;},healthTimeoutMs:5,onRecovery:r=>reasons.push(r),...options});
  return {first,second,manager,reasons,opens:()=>opens};
};
test('concurrent callers replace a permanently closed client exactly once',async()=>{
  const {first,second,manager,opens}=await setup();first.closed=true;
  assert.deepEqual(await Promise.all(Array.from({length:20},()=>manager.get())),Array(20).fill(second));
  assert.equal(opens(),2);await manager.close();
});
test('replacement waits until the old connection has been disposed',async()=>{
  const barrier=deferred();const {first,second,manager,opens}=await setup({dispose:()=>barrier.promise});
  first.closed=true;const pending=manager.get();await Promise.resolve();assert.equal(opens(),1);
  barrier.resolve();assert.equal(await pending,second);await manager.close();
});
test('a center timeout with a responsive broker keeps the existing connection',async()=>{
  const {first,manager,opens,reasons}=await setup();await manager.failed(first,{heartbeat:true});
  assert.equal(await manager.get(),first);assert.equal(opens(),1);assert.deepEqual(reasons,[]);await manager.close();
});
test('a failed broker ping replaces the connection without replaying a request',async()=>{
  const {first,second,manager,reasons}=await setup();first.flush=()=>new Promise(()=>{});
  await manager.failed(first,{heartbeat:true});assert.equal(first.closed,true);
  assert.equal(await manager.get(),second);assert.deepEqual(reasons,['broker_ping_failed']);await manager.close();
});
test('shutdown during reconnect disposes the new connection and prevents reopening',async()=>{
  const next=deferred();const old=client(),replacement=client();let calls=0;
  const {manager}=await setup({connect:async()=>++calls===1?old:next.promise});
  old.closed=true;const opening=manager.get();await Promise.resolve();await Promise.resolve();
  const closed=manager.close();next.resolve(replacement);
  await assert.rejects(opening,/NATS_CLIENT_CLOSED/);await closed;
  await assert.rejects(manager.get(),/NATS_CLIENT_CLOSED/);
  assert.ok(calls===1||replacement.closed); // shutdown may win before dialing
});
test('failed disposal cannot create overlapping live connections',async()=>{
  const {first,manager,opens}=await setup({dispose:async()=>{throw Error('dispose failed');}});
  first.closed=true;await assert.rejects(manager.get(),/dispose failed/);
  await assert.rejects(manager.get(),/dispose failed/);assert.equal(opens(),1);
  await assert.rejects(manager.close(),/dispose failed/);
});
