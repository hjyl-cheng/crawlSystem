import {connectNats as connect} from './natsConnection.js';
import {jetstream,jetstreamManager,AckPolicy,DiscardPolicy,RetentionPolicy,StorageType} from '@nats-io/jetstream';
import {createRequestAdmission} from './requestAdmission.js';
import {RESULT_STREAM,RESULT_CONSUMER,natsEndpoint,resultEnvelope,encode,decode,failure} from './natsProtocol.js';
import {RemoteProtocolError,uuid} from './protocol.js';
import {forwardLocalIntakeSignals} from './localIntakeSignals.js';

export async function startRemoteNatsCenter({url,user='center',password,tls,allowLoopback=false,store,channelPlans,wholeChannels=null,resultWholeChannels=wholeChannels,routes,youtubeSessions,workerConnections,
  signals,heartbeatStore=store,heartbeatConnections=workerConnections,resultStore=store,resultChannelPlans=channelPlans,resultConcurrency=8,rpcConcurrency=32,heartbeatConcurrency=16,maxPending=1024,resultMaxBytes=1024*1024*1024,replicas=1,report=()=>{}}){
  for(const value of [resultConcurrency,rpcConcurrency,heartbeatConcurrency,maxPending,resultMaxBytes,replicas])if(!Number.isSafeInteger(value)||value<1)throw new TypeError('positive NATS capacity required');
  if(typeof signals?.watch!=='function')throw new TypeError('committed SQL notifications required for NATS transport');
  const nc=await connect({servers:natsEndpoint(url,{allowLoopback}),user,pass:password,tls,maxReconnectAttempts:-1,reconnectTimeWait:1000,reconnectJitter:500});
  const active=new Set();let closing=false,messages,subscription,stopForwarding;
  try{
    const jsm=await jetstreamManager(nc);const js=jetstream(nc);
    const config={name:RESULT_STREAM,subjects:['qy.remote.results.*'],storage:StorageType.File,retention:RetentionPolicy.Workqueue,
      discard:DiscardPolicy.New,max_bytes:resultMaxBytes,max_msg_size:2*1024*1024,num_replicas:replicas,duplicate_window:120*1e9};
    let existing;try{existing=await jsm.streams.info(RESULT_STREAM);}catch(error){if(error.status!==404)throw error;}
    if(!existing)await jsm.streams.add(config);
    else if(existing.config.retention!==config.retention||existing.config.discard!==config.discard||existing.config.storage!==config.storage
      ||JSON.stringify(existing.config.subjects)!==JSON.stringify(config.subjects)||existing.config.num_replicas!==replicas||existing.config.max_bytes!==resultMaxBytes)throw Error('NATS_STREAM_CONFIG_CONFLICT');
    let info;try{info=await jsm.consumers.info(RESULT_STREAM,RESULT_CONSUMER);}catch(error){if(error.status!==404)throw error;}
    if(!info)await jsm.consumers.add(RESULT_STREAM,{durable_name:RESULT_CONSUMER,ack_policy:AckPolicy.Explicit,ack_wait:30*1e9,max_ack_pending:Math.max(32,resultConcurrency*4)});
    else if(info.config.ack_policy!==AckPolicy.Explicit)throw Error('NATS_CONSUMER_CONFIG_CONFLICT');
    const normal=createRequestAdmission({concurrency:rpcConcurrency,maxPending,timeoutMs:5000});
    const heartbeats=createRequestAdmission({concurrency:heartbeatConcurrency,maxPending,timeoutMs:5000});
    // Waiting for committed SQL work holds neither an RPC worker nor a DB client.
    const waits=createRequestAdmission({concurrency:maxPending,maxPending:0,timeoutMs:5000});
    async function readAfterHint(key,read,ready){
      const deadline=Date.now()+10000;
      for(;;){
        const watch=signals.watch(key,{timeoutMs:Math.max(1,deadline-Date.now())});
        try{const value=await read();if(ready(value)||closing||Date.now()>=deadline)return value;await watch.wait;}
        finally{watch.cancel();}
      }
    }
    const calls={
      whole_channel_input:(id,p)=>{
        if(!wholeChannels)throw new RemoteProtocolError('WHOLE_CHANNEL_DISABLED',409);
        return wholeChannels.input(id,p);
      },
      youtube_session:(id,p)=>youtubeSessions.get(id,p),youtube_checkpoint:(id,p)=>youtubeSessions.checkpoint(id,p),
      node_heartbeat:(id,p)=>heartbeatConnections.heartbeat(id,p),
      network_grant:(id,p)=>routes.grant(id,p),network_release:(id,p)=>routes.release(id,p),network_abandon:(id,p)=>routes.abandon(id,p),
      heartbeat:(id,p)=>heartbeatStore.heartbeat(id,uuid(p.task_id),p.generation),
      claim:async(id,p)=>({lease:await readAfterHint(`node:${id}`,()=>workerConnections?workerConnections.claim(id,p):store.claim(id,uuid(p.claim_id),p.slot??null),Boolean)}),
      commands:(id,p)=>readAfterHint(`task:${uuid(p.task_id)}`,()=>channelPlans.poll(id,{task_id:p.task_id,generation:p.generation}),v=>v.status!=='leased'||v.commands.length>0),
      receipt:(id,p)=>store.receipt(id,uuid(p.batch_id)),
      result_receipt:(id,p)=>{
        if(!/^[a-f0-9]{64}$/.test(p.receiptId))throw new RemoteProtocolError('INVALID_ID',400);
        return readAfterHint(`receipt:${p.receiptId}`,async()=>(await store.pool.query('SELECT response FROM remote_ingestion.transport_receipts WHERE receipt_id=$1 AND node_id=$2',[p.receiptId,id])).rows[0]?.response??null,Boolean);
      },
    };
    async function rpc(msg){
      let release;
      try{
        const parts=msg.subject.split('.');const nodeId=uuid(parts[3]),operation=parts[4];
        if(parts.length!==5||!Object.hasOwn(calls,operation))throw new RemoteProtocolError('NOT_FOUND',404);
        const lane=['commands','claim','result_receipt'].includes(operation)?waits:['heartbeat','node_heartbeat'].includes(operation)?heartbeats:normal;
        release=await lane.acquire();
        const value=decode(msg.data);if(value?.version!==1||!value.params||typeof value.params!=='object')throw new RemoteProtocolError('INVALID_REQUEST',400);
        if(await (['heartbeat','node_heartbeat'].includes(operation)?heartbeatStore:store).authenticate(value.token)!==nodeId)throw new RemoteProtocolError('UNAUTHORIZED',401);
        msg.respond(encode({ok:true,value:await calls[operation](nodeId,value.params)}));
      }catch(error){msg.respond(encode(failure(error)));}finally{release?.();}
    }
    subscription=nc.subscribe('qy.remote.rpc.*.*',{queue:'remote-api',callback:(error,msg)=>{
      if(error||closing)return;const p=rpc(msg);active.add(p);p.finally(()=>active.delete(p)).catch(()=>{});
    }});
    stopForwarding=forwardLocalIntakeSignals(signals,nc);
    async function result(msg){
      const timer=setInterval(()=>msg.working(),10000);
      try{
        const value=decode(msg.data);const nodeId=uuid(msg.subject.split('.')[3]);
        if(value?.nodeId!==nodeId||value.version!==1||typeof value.payload!=='string')throw new RemoteProtocolError('INVALID_RESULT',400);
        const bytes=Buffer.from(value.payload,'base64');const canonical=resultEnvelope(nodeId,value.token,value.operation,value.taskId,bytes);
        if(canonical.payload!==value.payload||canonical.receiptId!==value.receiptId)throw new RemoteProtocolError('INVALID_RESULT_ID',400);
        const old=(await resultStore.pool.query('SELECT node_id,response FROM remote_ingestion.transport_receipts WHERE receipt_id=$1',[value.receiptId])).rows[0];
        if(old){if(old.node_id!==nodeId)throw new RemoteProtocolError('UNAUTHORIZED',401);await msg.ackAck();return;}
        let response;
        try{
          if(await resultStore.authenticate(value.token)!==nodeId)throw new RemoteProtocolError('UNAUTHORIZED',401);
          if(value.operation==='whole_channel_result'&&!resultWholeChannels)throw new RemoteProtocolError('WHOLE_CHANNEL_DISABLED',409);
          const receipt=value.operation==='whole_channel_result'?await resultWholeChannels.receive(nodeId,{task_id:value.taskId},bytes)
            :value.operation==='channel_result'?await resultChannelPlans.receive(nodeId,{task_id:value.taskId},bytes):await resultStore.receive(nodeId,value.taskId,bytes);
          response={ok:true,value:receipt};
        }catch(error){response=failure(error);if(response.status>=500)throw error;}
        // If the process dies between these commits, the existing receipt writer
        // is replayed idempotently. Neither a broker ACK nor a lost reply bypasses it.
        await resultStore.pool.query(`INSERT INTO remote_ingestion.transport_receipts(receipt_id,node_id,task_id,response)
          VALUES($1,$2,$3,$4) ON CONFLICT(receipt_id) DO NOTHING`,[value.receiptId,nodeId,value.taskId,response]);
        await msg.ackAck();
      }catch(error){
        const value=failure(error);report({event:'remote_nats_result_error',code:value.error});
        if(value.status<500)msg.term(value.error);else msg.nak(1000);
      }finally{clearInterval(timer);}
    }
    const consumer=await js.consumers.get(RESULT_STREAM,RESULT_CONSUMER);
    messages=await consumer.consume({max_messages:resultConcurrency});
    const processing=new Set();
    const run=(async()=>{for await(const msg of messages){
      const p=result(msg);processing.add(p);p.finally(()=>processing.delete(p)).catch(()=>{});
      if(processing.size>=resultConcurrency)await Promise.race(processing);
    }await Promise.allSettled([...processing]);})();
    let consumerFailed=false;
    run.catch(()=>{consumerFailed=true;report({event:'remote_nats_consumer_stopped'});void nc.close();});
    await nc.flush();
    return {connection:nc,
      async stats(){if(consumerFailed||nc.isClosed())throw Error('NATS_CONSUMER_UNAVAILABLE');const value=await jsm.consumers.info(RESULT_STREAM,RESULT_CONSUMER);return {pending:value.num_pending,unacknowledged:value.num_ack_pending,rpc:normal.snapshot(),heartbeats:heartbeats.snapshot(),waiting:waits.snapshot()};},
      async close(){closing=true;stopForwarding?.();subscription.unsubscribe();messages.stop();await run.catch(()=>{});await Promise.allSettled([...active]);await nc.close();},
    };
  }catch(error){stopForwarding?.();subscription?.unsubscribe();messages?.stop();await nc.close();throw error;}
}
