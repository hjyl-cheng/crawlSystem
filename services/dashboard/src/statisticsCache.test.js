import test from 'node:test';import assert from 'node:assert/strict';
import {createStatisticsCache} from './statisticsCache.js';
const deferred=()=>{let resolve,reject;const promise=new Promise((a,b)=>{resolve=a;reject=b;});return {promise,resolve,reject};};
test('simultaneous viewers share one query and fresh values retain their observation time',async()=>{
 let time=0,calls=0;const gate=deferred(),cache=createStatisticsCache({now:()=>time});
 const reads=Array.from({length:20},()=>cache.get('today',async()=>{calls++;return gate.promise;}));
 await Promise.resolve();assert.equal(calls,1);time=1000;gate.resolve({total:41});
 const values=await Promise.all(reads);assert.ok(values.every(v=>v.value.total===41&&v.generatedAt===new Date(1000).toISOString()));
 time=2000;const next=await cache.get('today',()=>{throw Error('must use cache');});assert.equal(next.generatedAt,values[0].generatedAt);
});
test('stale data returns during slow refresh; failed refresh retains evidence without an immediate query storm',async()=>{
 let time=0,calls=0;const cache=createStatisticsCache({now:()=>time,ttlMs:10,staleMs:100,retryMs:5});
 await cache.get('today',async()=>({total:9}));time=11;const gate=deferred();
 const result=await cache.get('today',async()=>{calls++;return gate.promise;});assert.equal(result.stale,true);assert.equal(result.value.total,9);
 gate.reject(Error('database unavailable'));await new Promise(r=>setImmediate(r));
 await cache.get('today',()=>{throw Error('cooldown ignored');});assert.equal(calls,1);
 time=200;await assert.rejects(cache.get('today',async()=>{throw Error('still unavailable');}),/still unavailable/);
});
test('different filter statistics are queued with bounded concurrency, excess requests fail without blocking lists',async()=>{
 const cache=createStatisticsCache({maxPending:1});const gate=deferred();let active=0,peak=0;
 const load=async()=>{active++;peak=Math.max(peak,active);await gate.promise;active--;return 1;};
 const a=cache.get('a',load),b=cache.get('b',load);
 await assert.rejects(cache.get('c',load),{code:'STATISTICS_BUSY'});assert.equal(peak,1);
 gate.resolve();await Promise.all([a,b]);assert.equal(peak,1);
});
test('query/filter cache has a finite size and evicts old completed entries',async()=>{
 const cache=createStatisticsCache({maxEntries:2});let calls=0;const load=async()=>++calls;
 await cache.get('a',load);await cache.get('b',load);await cache.get('c',load);await cache.get('a',load);assert.equal(calls,4);
});
