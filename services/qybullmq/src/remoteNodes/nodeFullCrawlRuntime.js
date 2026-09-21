import {RemoteIncrementalProcess,checkNodeIncrementalHealth} from './nodeIncrementalRuntime.js';
import {loadWorkerFiles} from './workerConfig.js';
import {runNodeProcess} from './nodeConnectionRuntime.js';
import {RemoteResultSpool} from './spool.js';
import {FULL_CRAWL_WORKLOAD} from './collectingWorkload.js';
import {createRemoteFullCrawlWorker} from './fullCrawlWorker.js';
export const checkNodeFullCrawlHealth=checkNodeIncrementalHealth;
export function runNodeFullCrawl({spoolDirectory='/var/lib/qy-node/full-spool',...options}={}){
  return runNodeProcess({...options,loadFiles:files=>loadWorkerFiles(files,{mode:FULL_CRAWL_WORKLOAD.mode}),
    runWorker:({signal,...args})=>new RemoteIncrementalProcess({...args,wholeChannel:false,
      workload:FULL_CRAWL_WORKLOAD,createWorker:createRemoteFullCrawlWorker,
      spool:new RemoteResultSpool({directory:spoolDirectory,maxBytes:256*1024*1024})}).run({signal})});
}
