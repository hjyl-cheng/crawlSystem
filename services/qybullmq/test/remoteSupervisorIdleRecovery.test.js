import test from 'node:test';
import assert from 'node:assert/strict';
import {RemoteCenterExecutionSupervisor,supervisionLockKey} from '../src/remoteNodes/centerExecutionSupervisor.js';

function fixture({processing=false,controlState='RENEW_FAILED',activeTask=null,recovery=false,
  assignment=true,reclaiming=false,controlPending=false,ageMs=120000}={}) {
  const row={node_id:'11111111-1111-4111-8111-111111111111',slot:'incremental-3',activation_requested:true,
    alive:true,accepting:true,enabled:true,rota_worker_id:'test-worker'};
  const key=`${row.node_id}/${row.slot}`;let closed=0;
  const query=async sql=>({rows:sql.includes('SELECT w.*,s.rota_worker_id')?[row]:sql.includes('jsonb_to_recordset')?
    [{pid:10,lock_key:supervisionLockKey(row)}]:[],rowCount:0});
  const supervisor=new RemoteCenterExecutionSupervisor({store:{pool:{query},transaction:fn=>fn({query})},
    channelStore:{},activation:{},guardPool:{},connection:{},prefix:'isolated',allowedNodeIds:[row.node_id]});
  const entry={row,owned:true,queueReady:true,backendPid:10,processing,redis:{status:'ready'},
    notReadySince:Date.now()-ageMs,rota:{workerId:'test-worker',status:()=>({started:true,closing:false,
      active_job:processing,active_task_id:activeTask,recovery_pending:recovery,
      control_state:controlState,reclaim_in_flight:reclaiming,control_in_flight:controlPending,
      assignment:assignment?{ready:false,control_state:controlState}:null})},
    worker:{opts:{name:'fixture'},toKey:value=>value,pause:async()=>{},client:Promise.resolve({set:async()=>{}})}};
  supervisor.entries.set(key,entry);
  supervisor.closeEntry=async()=>{closed++;supervisor.entries.delete(key);};
  return {supervisor,entry,closed:()=>closed};
}

test('an idle expired route is recycled instead of leaving an enabled worker unready forever',async()=>{
  const f=fixture();await f.supervisor.tick();assert.equal(f.closed(),1);
});
test('idle recovery never interrupts collection, finalization or reserve waiting',async()=>{
  for(const options of [{processing:true},{activeTask:'task-1'},{recovery:true},{controlState:'paused_no_reserve'}]){
    const f=fixture(options);await f.supervisor.tick();assert.equal(f.closed(),0,JSON.stringify(options));
  }
});

test('an abandoned reclaim with no assignment is recycled after the readiness timeout',async()=>{
  const f=fixture({controlState:'RECLAIMING',assignment:false});
  await f.supervisor.tick();assert.equal(f.closed(),1);
});
test('reclaim recycling waits for all recovery work and the readiness timeout',async()=>{
  for(const options of [{reclaiming:true},{controlPending:true},{processing:true},{activeTask:'task'},
    {recovery:true},{ageMs:1000},{assignment:true}]){
    const f=fixture({controlState:'RECLAIMING',assignment:false,...options});
    await f.supervisor.tick();assert.equal(f.closed(),0,JSON.stringify(options));
  }
});
test('a lease error cannot recycle a still pending control request or reclaim',async()=>{
  for(const options of [{controlPending:true},{reclaiming:true}]){
    const f=fixture(options);await f.supervisor.tick();assert.equal(f.closed(),0,JSON.stringify(options));
  }
});
