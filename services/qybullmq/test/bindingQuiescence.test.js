import test from 'node:test';
import assert from 'node:assert/strict';
import {createBindingQuiescence} from '../src/remoteNodes/bindingQuiescence.js';

test('only a completed zero-in-flight receipt is cached, with independent attempt caches',async()=>{
  let stops=0,waits=0,finish;
  const routes={requestStop:async()=>{stops++;},waitQuiesced:()=>{waits++;return new Promise(resolve=>{finish=resolve;});}};
  const quiet=createBindingQuiescence({routes});
  const first=quiet('binding');assert.equal(quiet('binding'),first);
  await Promise.resolve();assert.equal(stops,1);assert.equal(waits,1);
  finish({active_managed_requests:1});await assert.rejects(first,{code:'REMOTE_NETWORK_NOT_QUIESCED'});
  const retry=quiet('binding');await Promise.resolve();finish({active_managed_requests:0});await retry;
  assert.equal(quiet('binding'),retry);assert.equal(stops,2);
  const nextAttempt=createBindingQuiescence({routes});const next=nextAttempt('binding');
  await Promise.resolve();finish({active_managed_requests:0});await next;assert.equal(stops,3);
});
