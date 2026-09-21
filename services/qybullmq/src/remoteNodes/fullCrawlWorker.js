import {createRemoteYoutubeRuntime} from './youtubeRuntime.js';
import {RemoteChannelNetworkSession} from './channelNetworkSession.js';
import {RemoteFullCrawlExecutor} from './fullCrawlExecutor.js';

export function createRemoteFullCrawlWorker({client,localRota,slot,spool,gateway,youtube,renewMs=5000,pollMs=100,timeoutMs}){
  if(client.transport!=='nats')throw TypeError('FULL_CRAWL_NATS_REQUIRED');
  const fullClient={...client,pollCommands:lease=>client.fullCrawlPoll({task_id:lease.task_id,generation:lease.generation,connection:lease.connection})};
  const runtime=createRemoteYoutubeRuntime({client:fullClient,spool,gateway,youtube,channelIdFromLease:lease=>lease.input.channel_id});
  const networkSession=new RemoteChannelNetworkSession({client:fullClient,localRota,slot,spool,withRuntime:runtime.withRuntime,renewMs});
  return new RemoteFullCrawlExecutor({client:fullClient,spool,youtube:runtime.youtube,networkSession,pollMs,timeoutMs});
}
