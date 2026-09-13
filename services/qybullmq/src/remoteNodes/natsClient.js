import {connectNats as connect} from './natsConnection.js';
import {jetstream} from '@nats-io/jetstream';
import {randomUUID} from 'node:crypto';
import {natsEndpoint,resultEnvelope,encode,decode,unwrap} from './natsProtocol.js';
import {RemoteProtocolError,uuid} from './protocol.js';

// Same collector interface as client.js. No HTTP fallback after NATS selection:
// two live transports must never independently own one execution.
export async function createRemoteNatsClient({url,token,nodeId,slot,allowLoopback=false,tls,timeoutMs=15000,receiptTimeoutMs=120000}){
  uuid(nodeId);if(!/^[a-z0-9-]{1,60}$/.test(slot)||typeof token!=='string'||token.length<32)throw new TypeError('node identity required');
  const nc=await connect({servers:natsEndpoint(url,{allowLoopback}),user:nodeId,pass:token,tls,
    inboxPrefix:`_INBOX.${nodeId}.${slot}.${randomUUID()}`,maxReconnectAttempts:-1,reconnectTimeWait:1000,reconnectJitter:500,timeout:5000});
  const js=jetstream(nc,{timeout:timeoutMs});
  const request=async(operation,params={})=>{
    try{return unwrap(decode((await nc.request(`qy.remote.rpc.${nodeId}.${operation}`,encode({version:1,token,params}),{timeout:timeoutMs})).data));}
    catch(error){if(error instanceof RemoteProtocolError)throw error;throw new RemoteProtocolError('NATS_TRANSPORT_UNAVAILABLE',503);}
  };
  const upload=async(operation,taskId,bytes)=>{
    const value=resultEnvelope(nodeId,token,operation,taskId,bytes);
    try{await js.publish(`qy.remote.results.${nodeId}`,encode(value),{msgID:value.receiptId});}
    catch{throw new RemoteProtocolError('NATS_RESULT_PENDING',503);}
    // Broker ACK does not pretend that the original SQL receipt was committed.
    // Leave the existing durable node spool intact until the writer confirms.
    const deadline=Date.now()+receiptTimeoutMs;
    while(Date.now()<deadline){const receipt=await request('result_receipt',{receiptId:value.receiptId});if(receipt)return unwrap(receipt);}
    throw new RemoteProtocolError('NATS_RESULT_PENDING',503);
  };
  return {
    transport:'nats',close:()=>nc.close(),
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
