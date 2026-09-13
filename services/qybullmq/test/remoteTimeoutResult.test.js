import test from 'node:test';
import assert from 'node:assert/strict';
import {gzipSync} from 'node:zlib';
import {randomUUID} from 'node:crypto';
import {mkdtemp,readFile,readdir,rm} from 'node:fs/promises';
import {tmpdir} from 'node:os';
import {join} from 'node:path';
import {encodeResult,decodeResult,hash} from '../src/remoteNodes/protocol.js';
import {RemoteChannelPlanExecutor} from '../src/remoteNodes/channelPlanExecutor.js';
import {RemoteResultSpool} from '../src/remoteNodes/spool.js';

const envelope=()=>({version:1,batch_id:randomUUID(),command_id:randomUUID(),generation:1,outcome:'failure',
  error:{__remote_error_v1:true,name:'TimeoutError',code:23,message:'The operation was aborted due to timeout'}});

test('a DOM timeout can be encoded and decoded without losing its original error classification',async()=>{
 const result=(await decodeResult(await encodeResult(envelope()))).value;
 assert.equal(result.error.code,'TimeoutError');assert.equal(result.error.native_code,23);
});

test('legacy numeric timeout results retain their exact receipt hash',async()=>{
 const raw=Buffer.from(JSON.stringify(envelope()));const decoded=await decodeResult(gzipSync(raw));
 assert.equal(decoded.value.error.code,'TimeoutError');assert.equal(decoded.sha256,hash(raw));
 await assert.rejects(decodeResult(gzipSync(Buffer.from(JSON.stringify({...envelope(),error:{code:{bad:true}}})))),{code:'INVALID_RESULT'});
});

for(const stale of [false,true])test(`real executor recovers a persisted legacy timeout (${stale?'expired':'live'} lease)`,async()=>{
 const directory=await mkdtemp(join(tmpdir(),'remote-timeout-'));
 try{
  const spool=new RemoteResultSpool({directory});await spool.init();const value=envelope();const raw=gzipSync(Buffer.from(JSON.stringify(value)));
  const saved=Buffer.from(JSON.stringify({task_id:randomUUID(),payload:raw.toString('base64')}));await spool.save('pending.json',saved);
  let uploads=0;const client={uploadCommand:async(_lease,bytes)=>{
   uploads++;assert.deepEqual(bytes,raw,'replay must not rewrite a previously published receipt');
   if(stale)throw Object.assign(Error('STALE_LEASE'),{code:'STALE_LEASE',status:409});
   return {durable:true,status:'received',batch_id:value.batch_id,command_id:value.command_id};
  }};
  const executor=new RemoteChannelPlanExecutor({client,spool,youtube:{openChannel:async()=>{},fetchDetail:async()=>{}},withSession:async()=>{}});
  executor.stop();assert.equal(await executor.runOnce(),'stopped');assert.equal(uploads,1);
  assert.equal(await spool.read('pending.json'),null);assert.equal(await spool.writable(),true);
  if(stale){const name=(await readdir(directory)).find(n=>n.endsWith('.stale'));assert.deepEqual(await readFile(join(directory,name)),saved);}
 }finally{await rm(directory,{recursive:true,force:true});}
});
