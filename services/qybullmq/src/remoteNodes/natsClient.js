import {connectNats as connect} from './natsConnection.js';
import {jetstream} from '@nats-io/jetstream';
import {randomUUID} from 'node:crypto';
import {natsEndpoint,resultEnvelope,encode,decode,unwrap} from './natsProtocol.js';
import {RemoteProtocolError,uuid} from './protocol.js';
import {createRecoveringNatsConnection} from './natsRecovery.js';

// Same collector interface as client.js. No HTTP fallback after NATS selection:
// two live transports must never independently own one execution.
export async function createRemoteNatsClient({url,token,nodeId,slot,allowLoopback=false,tls,timeoutMs=15000,receiptTimeoutMs=120000,transportHealthTimeoutMs=5000}){
  uuid(nodeId);if(!/^[a-z0-9-]{1,60}$/.test(slot)||typeof token!=='string'||token.length<32)throw new TypeError('node identity required');
  const connection=await createRecoveringNatsConnection({healthTimeoutMs:transportHealthTimeoutMs,connect:()=>connect({servers:natsEndpoint(url,{allowLoopback}),user:nodeId,pass:token,tls,
    inboxPrefix:`_INBOX.${nodeId}.${slot}.${randomUUID()}`,maxReconnectAttempts:-1,reconnectTimeWait:1000,reconnectJitter:500,timeout:5000})});
  const streams=new WeakMap();
  let heartbeatFailure=null;
  const request=async(operation,params={})=>{
    let nc;
    try{nc=await connection.get();const value=unwrap(decode((await nc.request(`qy.remote.rpc.${nodeId}.${operation}`,encode({version:1,token,params}),{timeout:timeoutMs})).data));
      if(operation==='node_heartbeat')heartbeatFailure=null;return value;}
    catch(error){
      if(operation==='node_heartbeat'){
        const raw=error instanceof RemoteProtocolError?error.code:error?.constructor?.name;
        const code=/^[A-Za-z][A-Za-z0-9_]{0,79}$/.test(raw??'')?raw:'TRANSPORT_ERROR';
        if(heartbeatFailure!==code)console.warn(JSON.stringify({event:'remote_nats_heartbeat_failed',code}));
        heartbeatFailure=code;
      }
      if(error instanceof RemoteProtocolError)throw error;
      if(nc)await connection.failed(nc,{heartbeat:operation==='node_heartbeat'});
      throw new RemoteProtocolError('NATS_TRANSPORT_UNAVAILABLE',503);}
  };
  const upload=async(operation,taskId,bytes)=>{
    const value=resultEnvelope(nodeId,token,operation,taskId,bytes);
    try{const nc=await connection.get();if(!streams.has(nc))streams.set(nc,jetstream(nc,{timeout:timeoutMs}));
      await streams.get(nc).publish(`${operation==='full_crawl_part'?'qy.remote.full.results':'qy.remote.results'}.${nodeId}`,encode(value),{msgID:value.receiptId});}
    catch{throw new RemoteProtocolError('NATS_RESULT_PENDING',503);}
    // Broker ACK does not pretend that the original SQL receipt was committed.
    // Leave the existing durable node spool intact until the writer confirms.
    const deadline=Date.now()+receiptTimeoutMs;
    while(Date.now()<deadline){const receipt=await request('result_receipt',{receiptId:value.receiptId});if(receipt)return unwrap(receipt);}
    throw new RemoteProtocolError('NATS_RESULT_PENDING',503);
  };
  return {
    transport:'nats',close:()=>connection.close(),
    fullCrawlPoll:value=>request('full_commands',value),fullCrawlHeartbeat:value=>request('full_heartbeat',value),
    fullCrawlReceipt:value=>request('full_receipt',value),
    fullCrawlRecovered:value=>request('full_recovered',value),
    fullCrawlStarted:value=>request('full_started',value),
    uploadFullCrawl:(request,part)=>upload('full_crawl_part',request.task_id,encode({request,...part,payload:part.payload.toString('base64')})),
    youtubeSession:value=>request('youtube_session',value),youtubeCheckpoint:value=>request('youtube_checkpoint',value),
    workerHeartbeat:value=>request('node_heartbeat',value),
    grantRoute:value=>request('network_grant',value),releaseRoute:value=>request('network_release',value),abandonRoute:value=>request('network_abandon',value),
    claim:async(claimId,workerSlot=null,connection=null)=>(await request('claim',{claim_id:claimId,...(workerSlot?{slot:workerSlot}:{}),...(connection?{connection}:{})})).lease,
    heartbeat:lease=>request('heartbeat',{task_id:lease.task_id,generation:lease.generation}),
    pollCommands:lease=>request('commands',{task_id:lease.task_id,generation:lease.generation}),
    wholeChannelInput:(lease,commandId,part)=>request('whole_channel_input',{task_id:lease.task_id,generation:lease.generation,command_id:commandId,part}),
    uploadWholeChannel:(lease,bytes)=>upload('whole_channel_result',lease.task_id,bytes),
    uploadCommand:(lease,bytes)=>upload('channel_result',lease.task_id,bytes),
    upload:(taskId,bytes)=>upload('work_result',taskId,bytes),
    receipt:batchId=>request('receipt',{batch_id:batchId}),
  };
}
