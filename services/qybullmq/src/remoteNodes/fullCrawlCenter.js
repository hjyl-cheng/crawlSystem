import {createFullCrawlTransport} from './fullCrawlTransport.js';
import {RemoteCenterExecutionSupervisor} from './centerExecutionSupervisor.js';
import {RemoteFullCrawlExecutionStore} from './fullCrawlExecutionStore.js';
import {RemoteManagedFullCrawlRuntime} from './managedFullCrawlRuntime.js';
import {createCenterFullCrawlProcessor} from './centerFullCrawlProcessor.js';
import {fullCrawlSlotUnsettled,recoverFullCrawlSlot} from './fullCrawlCenterRecovery.js';

// Opt-in composition selected by the release startup only with explicit full
// execution configuration. Network and local compatibility must be ready.
export function createFullCrawlCenter({store,transportFactory,transportOptions=null,handoff,compatibility,...options}){
  if(typeof transportFactory!=='function'&&!transportOptions)throw new TypeError('full-crawl transport factory required');
  for(const name of ['execute','replay'])if(typeof compatibility?.[name]!=='function')throw new TypeError(`compatibility.${name} required`);
  for(const name of ['candidateSettled','fetchCompleted'])if(typeof handoff?.[name]!=='function')throw new TypeError(`handoff.${name} required`);
  let supervisor;
  const executions=new RemoteFullCrawlExecutionStore({store,verifyExecution:(client,row)=>supervisor.verifyExecution(client,row)});
  const transport=transportOptions?createFullCrawlTransport({...transportOptions,executions}):null;
  transportFactory??=args=>transport.forConnection(args.workerConnection);
  supervisor=new RemoteCenterExecutionSupervisor({...options,store,mode:'full_crawl_collect',activation:executions.activation,
    channelStore:{store},slotUnsettled:fullCrawlSlotUnsettled,recoverSlot:recoverFullCrawlSlot,settleHandoffs:false,
    createRuntime:args=>new RemoteManagedFullCrawlRuntime({...args,executionStore:executions,
      transport:transportFactory({...args,executions}),handoff}),
    createProcessor:args=>createCenterFullCrawlProcessor({...args,store,compatibility,handoff})});
  return {supervisor,executions,transport,activation:executions.activation};
}
