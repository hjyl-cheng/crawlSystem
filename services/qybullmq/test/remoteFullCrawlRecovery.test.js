import test from 'node:test';
import assert from 'node:assert/strict';
import {mkdtemp,rm} from 'node:fs/promises';
import {tmpdir} from 'node:os';
import {join} from 'node:path';
import {randomUUID} from 'node:crypto';
import {createRemoteFullCrawlWorker} from '../src/remoteNodes/fullCrawlWorker.js';
import {RemoteChannelNetworkSession} from '../src/remoteNodes/channelNetworkSession.js';
import {RemoteResultSpool} from '../src/remoteNodes/spool.js';
import {RemoteFullCrawlExecutionStore} from '../src/remoteNodes/fullCrawlExecutionStore.js';
import {RemoteFullCrawlTransportStore} from '../src/remoteNodes/fullCrawlTransportStore.js';
import {RemoteProtocolError} from '../src/remoteNodes/protocol.js';

async function fixture(t){
 const directory=await mkdtemp(join(tmpdir(),'full-recovery-'));
 t.after(()=>rm(directory,{recursive:true,force:true}));
 const spool=new RemoteResultSpool({directory});await spool.init();
 const slot='full-crawl-3',nodeId=randomUUID();
 const connection={version:1,mode:'full_crawl_collect',node_id:nodeId,slot,deployment_id:randomUUID(),config_hash:'a'.repeat(64),
  instance_id:randomUUID(),relay_boot_id:'b'.repeat(48),runtime_revision:'youtubejs-full-crawl-v1',accepting:true};
 const lease={task_id:randomUUID(),generation:1,worker_slot:slot,connection};
 const save=(name,value)=>spool.save(name,Buffer.from(JSON.stringify(value)));
 // Exercise the real center identity validator before simulating the expired
 // original connection. No request may proceed into a new browser identity.
 const store={transaction:action=>action({query:async()=>{throw new RemoteProtocolError('WORKER_CONNECTION_STALE');}})};
 const executions=new RemoteFullCrawlExecutionStore({store,verifyExecution:async()=>false});
 const transport=new RemoteFullCrawlTransportStore({executions});
 let polls=0;
 const worker=createRemoteFullCrawlWorker({spool,slot,nodeId,localRota:{},gateway:{},
  youtube:{openChannel(){assert.fail('no new browser');},fetchUploads(){},fetchDetail(){}},
  client:{transport:'nats',fullCrawlPoll:req=>{polls++;return transport.poll(nodeId,req);}}});
 await save('claim.json',{claim_id:randomUUID(),lease});
 await save('network.json',{phase:'closed',lease:{task_id:lease.task_id,generation:lease.generation}});
 return {spool,slot,nodeId,connection,lease,save,worker,polls:()=>polls};
}

test('legacy closed full-crawl session recovers original claim identity through the real worker factory',async t=>{
 const f=await fixture(t);
 assert.equal(await f.worker.runOnce(),'closed');
 assert.equal(await f.spool.read('network.json'),null);
 assert.equal(await f.spool.read('claim.json'),null);
 assert.equal(f.polls(),2);
});

for(const mismatch of ['task','generation','slot','node','missing'])test(`legacy recovery preserves mismatched ${mismatch} evidence without polling`,async t=>{
 const f=await fixture(t);const claim=await f.spool.read('claim.json');
 if(mismatch==='task')claim.lease.task_id=randomUUID();
 if(mismatch==='generation')claim.lease.generation++;
 if(mismatch==='slot')claim.lease.connection.slot='full-crawl-4';
 if(mismatch==='node')claim.lease.connection.node_id=randomUUID();
 if(mismatch==='missing')await f.spool.remove('claim.json');else await f.save('claim.json',claim);
 await assert.rejects(f.worker.runOnce(),{code:'FULL_CRAWL_RECOVERY_IDENTITY_MISMATCH'});
 assert.equal(f.polls(),0);assert.equal((await f.spool.read('network.json')).phase,'closed');
});

test('new interrupted network session durably retains the original connection',async t=>{
 const f=await fixture(t);await f.spool.remove('network.json');
 const session=new RemoteChannelNetworkSession({spool:f.spool,slot:f.slot,localRota:{boot:async()=>({boot_id:'boot'})},
  withRuntime:async()=>assert.fail('no browser'),client:{grantRoute:async()=>{throw new RemoteProtocolError('FIXTURE_INTERRUPTED',400);},abandonRoute:async()=>({abandoned:true})}});
 await assert.rejects(session.run(f.lease,{signal:new AbortController().signal},()=>{}),{code:'FIXTURE_INTERRUPTED'});
 const closed=await f.spool.read('network.json');
 assert.equal(closed.phase,'closed');assert.deepEqual(closed.lease.connection,f.connection);
 assert.equal(await f.worker.runOnce(),'closed');
});

test('executor preserves fatal protocol error instead of returning a generic stop',async t=>{
 const f=await fixture(t);await f.spool.remove('network.json');
 f.worker.client.fullCrawlPoll=async()=>{throw new RemoteProtocolError('INVALID_WORKER_CONNECTION',400);};
 await assert.rejects(f.worker.run(),{code:'INVALID_WORKER_CONNECTION',status:400});
 assert.ok(await f.spool.read('claim.json'));
});
