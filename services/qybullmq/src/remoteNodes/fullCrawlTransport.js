import {applyFullCrawlProfileCheckpoint} from './fullCrawlProfileCheckpoint.js';
import {RemoteChannelRouteStore} from './channelRouteStore.js';
import {RemoteYoutubeSessionStore} from './youtubeSessionStore.js';
import {fullCrawlTaskAccess} from './fullCrawlTaskAccess.js';
import {RemoteFullCrawlTransportStore} from './fullCrawlTransportStore.js';
import {createRemoteRotaChannelRuntime} from './rotaChannelRuntimeAdapter.js';

// Shared by the NATS center and the per-slot managed runtime. Only a live
// transport readiness predicate can enable intake; construction alone cannot.
export function createFullCrawlTransport({executions,readRotaRoute,privateKey,secretKey,isReady,grantTtlMs=30000,stopTimeoutMs=45000}){
  if(typeof isReady!=='function')throw TypeError('full-crawl transport readiness required');
  const access=fullCrawlTaskAccess(executions);
  const routes=new RemoteChannelRouteStore({channelStore:access,assertBusinessFence:access.assertBusinessFence,
    businessRunId:task=>task.input.business_run_id,readRotaRoute,privateKey,secretKey,grantTtlMs});
  const youtubeSessions=new RemoteYoutubeSessionStore({routes});
  const service=new RemoteFullCrawlTransportStore({executions,routes,youtubeSessions});
  return {service,routes,youtubeSessions,
    forConnection(connection){
      const adapters=new Map();
      const adapter=request=>createRemoteRotaChannelRuntime({routes,nodeId:connection.node_id,lease:request,
        slot:connection.slot,stopTimeoutMs,youtubeSessions,youtubeSession:{}});
      return {
        ready:()=>isReady()===true,
        applyCheckpoint:(client,args)=>applyFullCrawlProfileCheckpoint(client,{routes,...args}),
        // SQL commit notifications are the wakeup mechanism. A notification
        // never substitutes for a durable task or stage.
        notifyTask:async()=>{},notifyStage:async()=>{},
        async open(args){
          const runtime=createRemoteRotaChannelRuntime({routes,nodeId:connection.node_id,lease:args.request,
            slot:connection.slot,stopTimeoutMs,youtubeSessions,
            youtubeSession:{profileGroup:args.profileGroup,attemptId:args.attemptId}});
          const handle=await runtime.acquire(args);adapters.set(args.request.task_id,{runtime,handle});
          await handle.execute({job:{data:{}}},async()=>{});
          return {bindingId:handle.binding.binding_id};
        },
        async stop({request}){
          const current=adapters.get(request.task_id)??{runtime:adapter(request),handle:{binding:null}};
          const quiet=await current.runtime.quiesce(current.handle);
          adapters.delete(request.task_id);
          return {...quiet,youtube_requests:current.handle.youtubeCheckpoint?.metrics,
            checkpoint:current.handle.youtubeCheckpoint??null,binding_id:current.handle.binding?.binding_id??null};
        },
      };
    },
  };
}
