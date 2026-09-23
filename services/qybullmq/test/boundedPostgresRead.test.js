import test from 'node:test';
import assert from 'node:assert/strict';
import {setTimeout as delay} from 'node:timers/promises';
import {getEventListeners} from 'node:events';
import {boundedPostgresRead} from '../src/remoteNodes/boundedPostgresRead.js';

const deferred=()=>{let resolve,reject;const promise=new Promise((a,b)=>{resolve=a;reject=b;});return {promise,resolve,reject};};
function fixture(query=async()=>({rows:[{ok:true}]})) {
  const calls=[],releases=[];
  return {calls,releases,pool:{connect:async()=>({query:(text,values)=>{calls.push({text,values});return query(text,values);},release:error=>releases.push(error)})}};
}
test('late checkout is released without issuing SQL or retaining the abort listener',async()=>{
  const borrowed=deferred(),abort=new AbortController();let queries=0;const releases=[];
  await assert.rejects(boundedPostgresRead({connect:()=>borrowed.promise},{text:'SELECT 1',signal:abort.signal,acquireTimeoutMs:10}),{code:'REMOTE_DB_ACQUIRE_TIMEOUT'});
  borrowed.resolve({query:()=>queries++,release:error=>releases.push(error)});await delay(0);
  assert.equal(queries,0);assert.deepEqual(releases,[undefined]);assert.equal(getEventListeners(abort.signal,'abort').length,0);
});
test('an already cancelled read does not borrow a connection',async()=>{
  const abort=new AbortController();abort.abort();let borrowed=0;
  await assert.rejects(boundedPostgresRead({connect:()=>borrowed++},{text:'SELECT 1',signal:abort.signal}),{name:'AbortError'});
  assert.equal(borrowed,0);
});
test('all query stages including BEGIN and COMMIT are bounded; late rejection is consumed',async()=>{
  for(const stage of ['BEGIN READ ONLY','SELECT 1','COMMIT']){
    const hanging=deferred();const f=fixture(sql=>sql===stage?hanging.promise:Promise.resolve({rows:[]}));
    await assert.rejects(boundedPostgresRead(f.pool,{text:'SELECT 1',timeoutMs:20}),{code:'REMOTE_DB_READ_TIMEOUT'});
    assert.equal(f.releases.length,1);assert.ok(f.releases[0]);
    hanging.reject(new Error('late network failure'));await delay(0);assert.equal(f.releases.length,1);
  }
});
test('successful reads use transaction local settings and release once',async()=>{
  const abort=new AbortController();const f=fixture();
  assert.deepEqual(await boundedPostgresRead(f.pool,{text:'SELECT $1::int',values:[7],signal:abort.signal}),{rows:[{ok:true}]});
  assert.equal(f.calls[0].text,'BEGIN READ ONLY');assert.match(f.calls[1].text,/set_config\('statement_timeout',\$1,true\)/);
  assert.deepEqual(f.calls[2],{text:'SELECT $1::int',values:[7]});assert.equal(f.calls[3].text,'COMMIT');
  assert.deepEqual(f.releases,[null]);assert.equal(getEventListeners(abort.signal,'abort').length,0);
  abort.abort();assert.equal(f.releases.length,1);
});
test('cancellation racing a result never starts COMMIT or returns the late row',async()=>{
  const abort=new AbortController();const f=fixture(async sql=>{
    if(sql==='SELECT 1')abort.abort(new Error('stale attempt'));
    return {rows:[{ok:true}]};
  });
  await assert.rejects(boundedPostgresRead(f.pool,{text:'SELECT 1',signal:abort.signal}),/stale attempt/);
  assert.equal(f.calls.some(c=>c.text==='COMMIT'),false);assert.equal(f.releases.length,1);
});
