import test from 'node:test';
import assert from 'node:assert/strict';
import {setTimeout as delay} from 'node:timers/promises';
import {RemoteChannelExecutionStore} from '../src/remoteNodes/channelExecutionStore.js';

function fixture() {
  let releaseQuery; const pending=new Promise(resolve=>{releaseQuery=resolve;});
  const released=[];
  const client={query:sql=>sql.startsWith('SELECT *,')?pending:Promise.resolve({rows:[]}),release:error=>released.push(error)};
  const executions=new RemoteChannelExecutionStore({channelStore:{store:{pool:{query:()=>pending,connect:async()=>client},lease:row=>row}},profileSecret:'test'});
  return {executions,released,releaseQuery};
}
test('waitClaim settles on cancellation while its PostgreSQL query never returns',async()=>{
  const f=fixture();const abort=new AbortController();
  const waiting=f.executions.waitClaim({taskId:'task',attemptId:'attempt'},{nodeId:'node',slot:'slot',signal:abort.signal})
    .then(()=>({ok:true}),error=>({error}));
  await delay(10);abort.abort(new Error('cancelled attempt'));
  const outcome=await Promise.race([waiting,delay(100).then(()=>({hung:true}))]);
  f.releaseQuery({rows:[{context:{execution_attempt_id:'attempt'},state:'pending'}]});
  assert.equal(outcome.hung,undefined,'cancelled claim must not wait for the database response');
  assert.equal(outcome.error?.message,'cancelled attempt');
  assert.equal(f.released.length,1);assert.ok(f.released[0],'uncertain client must be destroyed');
});
