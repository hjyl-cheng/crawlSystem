import {RemoteFullCrawlExecutionStore} from './fullCrawlExecutionStore.js';
import {createFullCrawlTransport} from './fullCrawlTransport.js';

// P4 provisioning can verify a real node connection without starting queue
// consumers. P5/P6 supply the fully assembled center through createFullCrawlCenter.
export function createFullCrawlDeploymentRuntime({store,image,...transportOptions}){
  const execution={allowsNode:()=>false,isProcessing:()=>false};
  const executions=new RemoteFullCrawlExecutionStore({store,verifyExecution:async()=>false});
  const transport=createFullCrawlTransport({...transportOptions,executions,isReady:()=>false});
  return {image,execution,activation:executions.activation,transport};
}
