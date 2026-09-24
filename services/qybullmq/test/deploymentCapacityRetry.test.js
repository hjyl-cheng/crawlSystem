import test from 'node:test';
import assert from 'node:assert/strict';
import {createDeploymentCapacity} from '../src/remoteNodes/deploymentCapacity.js';
import {ProxyControlClient} from '../src/proxyControlClient.js';
const pool={query:async()=>({rows:[{workers:85}]})};
const response=(status,body)=>new Response(JSON.stringify(body),{status,headers:{'content-type':'application/json'}});
const busy=()=>response(503,{ok:false,code:'RESOURCE_SYNC_DEFERRED',error:'busy'});

test('capacity retries only explicit sync contention with the same absolute target',async()=>{
 const sent=[],delays=[];
 const client=new ProxyControlClient({controlUrl:'http://fixture',token:'fixture-control-token',maxAttempts:1,
  fetchImpl:async(url,options)=>{sent.push(JSON.parse(options.body));return sent.length<3?busy():response(200,{ok:true,role:'channel',provisioned:154});}});
 const capacity=createDeploymentCapacity({pool,client,localChannelSlots:62,sleepImpl:async ms=>delays.push(ms),random:()=>0});
 assert.deepEqual(await capacity.ensure(),{required:147,provisioned:154});
 assert.deepEqual(sent,Array(3).fill({role:'channel',minimum_slots:147}));assert.deepEqual(delays,[200,500]);
});

test('persistent contention has three total attempts and exposes a safe cause',async()=>{
 let calls=0;
 const client=new ProxyControlClient({controlUrl:'http://fixture',token:'fixture-control-token',maxAttempts:5,
  sleepImpl:()=>assert.fail('nested retry'),fetchImpl:async()=>{calls++;return busy();}});
 const capacity=createDeploymentCapacity({pool,client,localChannelSlots:62,sleepImpl:async()=>{},random:()=>0});
 await assert.rejects(capacity.ensure(),{code:'REMOTE_NETWORK_CAPACITY_UNAVAILABLE',reason:'RESOURCE_SYNC_DEFERRED',attempts:3});
 assert.equal(calls,3);
});

for(const [status,body,reason] of [
 [503,{error:'secret upstream detail'},'CAPACITY_SERVICE_UNAVAILABLE'],
 [401,{error:'credential detail'},'CAPACITY_AUTH_FAILED'],
 [400,{code:'INVALID_INPUT'},'CAPACITY_INVALID_REQUEST'],
 [200,{ok:true,role:'channel',provisioned:146},'CAPACITY_INVALID_RESPONSE'],
])test(`capacity classifies ${status}/${reason} without retry or leaking upstream text`,async()=>{
 let calls=0;
 const client=new ProxyControlClient({controlUrl:'http://fixture',token:'fixture-control-token',maxAttempts:1,fetchImpl:async()=>{calls++;return response(status,body);}});
 const capacity=createDeploymentCapacity({pool,client,localChannelSlots:62,sleepImpl:()=>assert.fail('unexpected retry')});
 await assert.rejects(capacity.ensure(),error=>error.code==='REMOTE_NETWORK_CAPACITY_UNAVAILABLE'&&error.reason===reason&&!JSON.stringify(error).includes('detail'));
 assert.equal(calls,1);
});

test('backoff uses a shared deadline rather than resetting the timeout per attempt',async()=>{
 let now=0,calls=0;const timeouts=[];
 const client={ensureCapacity:async(_,options)=>{calls++;timeouts.push(options?.timeoutMs);now+=60;throw Object.assign(new Error(),{status:503,code:'RESOURCE_SYNC_DEFERRED'});}};
 const capacity=createDeploymentCapacity({pool,client,localChannelSlots:62,timeoutMs:100,now:()=>now,sleepImpl:async ms=>{now+=ms},random:()=>0});
 await assert.rejects(capacity.ensure(),{reason:'CAPACITY_TIMEOUT'});assert.equal(calls,1);assert.deepEqual(timeouts,[100]);
});

test('capacity HTTP deadline aborts a stalled response and does not retry it',async()=>{
 let calls=0;
 const client=new ProxyControlClient({controlUrl:'http://fixture',token:'fixture-control-token',maxAttempts:1,
  fetchImpl:async(_,options)=>{calls++;return new Promise((resolve,reject)=>options.signal.addEventListener('abort',()=>reject(options.signal.reason),{once:true}));}});
 const capacity=createDeploymentCapacity({pool,client,localChannelSlots:62,timeoutMs:100});
 await assert.rejects(capacity.ensure(),{reason:'CAPACITY_TIMEOUT'});assert.equal(calls,1);
});
