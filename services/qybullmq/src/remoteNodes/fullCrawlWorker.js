import {createRemoteYoutubeRuntime} from './youtubeRuntime.js';
import {RemoteChannelNetworkSession} from './channelNetworkSession.js';
import {RemoteFullCrawlExecutor} from './fullCrawlExecutor.js';
import {RemoteProtocolError} from './protocol.js';

export function createRemoteFullCrawlWorker({client,localRota,nodeId,slot,spool,gateway,youtube,renewMs=5000,pollMs=100,timeoutMs}){
  if(client.transport!=='nats')throw TypeError('FULL_CRAWL_NATS_REQUIRED');
  const fullClient={...client,pollCommands:async lease=>{
    let connection=lease.connection;
    if(!connection){
      // Older network journals omitted the original connection. Recover it
      // only from the matching durable claim, never from the new process.
      const claim=await spool.read('claim.json');
      if(claim?.lease?.task_id!==lease.task_id || claim.lease.generation!==lease.generation
        || (claim.lease.worker_slot && claim.lease.worker_slot!==slot)){
        throw new RemoteProtocolError('FULL_CRAWL_RECOVERY_IDENTITY_MISMATCH',400);
      }
      connection=claim.lease.connection;
    }
    if(connection?.mode!=='full_crawl_collect' || connection.slot!==slot || (nodeId && connection.node_id!==nodeId)){
      throw new RemoteProtocolError('FULL_CRAWL_RECOVERY_IDENTITY_MISMATCH',400);
    }
    return client.fullCrawlPoll({task_id:lease.task_id,generation:lease.generation,connection});
  }};
  const runtime=createRemoteYoutubeRuntime({client:fullClient,spool,gateway,youtube,channelIdFromLease:lease=>lease.input.channel_id});
  const networkSession=new RemoteChannelNetworkSession({client:fullClient,localRota,slot,spool,withRuntime:runtime.withRuntime,renewMs});
  return new RemoteFullCrawlExecutor({client:fullClient,spool,youtube:runtime.youtube,networkSession,pollMs,timeoutMs});
}
