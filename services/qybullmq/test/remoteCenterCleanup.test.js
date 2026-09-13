import test from 'node:test';import assert from 'node:assert/strict';
import {RemoteCenterExecutionSupervisor} from '../src/remoteNodes/centerExecutionSupervisor.js';
for(const failure of ['drain','rota'])test(`slot cleanup survives ${failure} failure without leaking its queue consumer or crashing the supervisor`,async()=>{
 const events=[];let workerClosed=0,guardReleased=0;
 const entry={row:{node_id:'node',slot:'incremental-1'},owned:true,
  worker:{pause:async()=>{},client:Promise.resolve({set:async()=>{}}),opts:{name:'fixture'},toKey:x=>x,close:async()=>{workerClosed++;}},
  rota:{close:async()=>{if(failure==='rota')throw Object.assign(Error('quiesce failed'),{code:'55P03'});}},
  guard:{release:async()=>{guardReleased++;}}};
 const supervisor=Object.assign(Object.create(RemoteCenterExecutionSupervisor.prototype),{entries:new Map([['node/incremental-1',entry]]),report:event=>events.push(event),
  activation:{drain:async()=>{if(failure==='drain')throw Object.assign(Error('lock timeout'),{code:'55P03'});}}});
 await supervisor.closeEntry(entry,{abort:true});
 assert.equal(workerClosed,1);assert.equal(guardReleased,1);assert.equal(entry.owned,false);
 assert.ok(events.some(e=>e.event==='remote_center_cleanup_failed'&&e.code==='55P03'));
 await supervisor.closeEntry(entry,{abort:true});assert.equal(workerClosed,1);
});

test('session loss during the initial unsettled read cannot create an orphan consumer',async()=>{
 let lose,finish,started;const pending=new Promise(r=>finish=r);const entered=new Promise(r=>started=r);
 let created=0;
 const supervisor=Object.assign(Object.create(RemoteCenterExecutionSupervisor.prototype),{
  entries:new Map(),stopping:false,report:()=>{},
  guards:{acquire:async(key,onLost)=>{lose=onLost;return {alive:true,backendPid:123,release:async()=>{}};}},
  unsettled:()=>{started();return pending;},createRuntime:()=>{created++;},
  activation:{drain:async()=>{}},
 });
 const starting=supervisor.startEntry({node_id:'node',slot:'incremental-1',alive:true,accepting:true,activation_requested:true});
 await entered;lose();finish(false);await starting;
 assert.equal(created,0);assert.equal(supervisor.entries.size,0);
});
