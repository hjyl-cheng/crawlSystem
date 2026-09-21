import test from 'node:test';
import assert from 'node:assert/strict';
import {collectingWorkload} from '../src/remoteNodes/collectingWorkload.js';
import {settleTerminalRemoteHandoffs} from '../src/remoteNodes/centerExecutionRecovery.js';
import {RemoteCenterExecutionSupervisor} from '../src/remoteNodes/centerExecutionSupervisor.js';

function fixture() {
 const rows=[],entries=new Map(),writes=new Map(),paused=new Set();let transactions=0;
 for(let i=1;i<=20;i++){
  const row={node_state:'active',node_id:'node',slot:`incremental-${i}`,deployment_id:'deployment',config_hash:'hash',instance_id:'instance',relay_boot_id:'boot',
   rota_worker_id:`rota-${i}`,activation_requested:true,alive:true,accepting:true,enabled:true};
  rows.push(row);const worker={opts:{name:row.slot},toKey:k=>k,client:Promise.resolve({set:async(k,v)=>writes.set(k,v)}),
   pause:async()=>paused.add(i),resume:()=>paused.delete(i)};
  entries.set(`node/${row.slot}`,{row,worker,owned:true,queueReady:true,backendPid:i,supervisorId:`supervisor-${i}`,redis:{status:'ready'},
   rota:{workerId:row.rota_worker_id,workerInstanceId:`supervisor-${i}`,status:()=>({started:true,assignment:{ready:true}})}});
 }
 const store={pool:{query:async sql=>({rows:sql.includes('FROM pg_locks')&&!sql.includes('worker_connections')
  ? rows.map((r,i)=>({pid:i+1,lock_key:`remote-incremental-supervisor:node/${r.slot}`})):rows})},
  transaction:async action=>{transactions++;return action({query:async sql=>{
   if(sql.includes('FROM remote_ingestion.node_intake_requests') || sql.includes('WITH settled AS'))return {rows:[],rowCount:0};
   assert.fail(`unexpected per-slot transaction: ${sql}`);
  }});}};
 const supervisor=Object.assign(Object.create(RemoteCenterExecutionSupervisor.prototype),{store,entries,workload:collectingWorkload('incremental_collect'),settleHandoffs:settleTerminalRemoteHandoffs,guardPool:{options:{max:1}},stopping:false,maxSlots:32,allowedNodeIds:[],dashboardManaged:true,report:()=>{},channelStore:{}});
 return {supervisor,rows,writes,paused,transactions:()=>transactions};
}
test('20 remote consumers refresh capacity from a bulk snapshot without 20 serial readiness transactions',async()=>{
 const f=fixture();await f.supervisor.tick();
 assert.equal(f.writes.size,20);assert.equal([...f.writes.values()].filter(v=>v==='1').length,20);
 assert.equal(f.transactions(),2,'only batched handoff recovery and intake reconciliation may open transactions');
});
test('bulk capacity excludes disconnected, disabled, route-unready and lock-lost consumers',async()=>{
 const f=fixture();f.rows[0].alive=false;f.rows[1].enabled=false;
 f.supervisor.entries.get('node/incremental-3').rota.status=()=>({started:true,assignment:{ready:false}});
 f.supervisor.entries.get('node/incremental-4').backendPid=999;
 await f.supervisor.tick();
 assert.equal([...f.writes.values()].filter(v=>v==='1').length,16);
 for(const i of [1,2,3,4])assert.equal(f.writes.get(`intake:incremental-${i}`),'0');
});
