import {randomUUID} from 'node:crypto';
import {setTimeout as delay} from 'node:timers/promises';
import {createNodeSsh,validateBootstrapPassword} from './serverNodeSsh.js';
import {deploymentControlFromEnv} from './nodeRuntime/deploymentControlClient.js';

export function createNodeWorkerRemoval({store,center,ssh,waitMs=60000,pollMs=500}) {
  const active=new Map();
  async function execute(node,password){
    const {operationId,slot}=node.workerRemoval;
    const input={nodeId:node.id,deploymentId:node.deployment.deploymentId,slot,operationId};
    let connection,reserved=false;
    try{
      connection=await ssh.connect(node,{keyOnly:true});await ssh.verify(connection,password);
      const check=await center.retire({...input,phase:'reserve'});reserved=true;
      if(check.operationId!==operationId || check.slot!==slot)throw Error('invalid retirement response');
      if(!check.removed){
        const deadline=Date.now()+waitMs;
        for(;;){
          try{await center.retire({...input,phase:'ready'});break;}
          catch(error){if(error.code!=='WORKER_RETIREMENT_WAIT'||Date.now()>=deadline)throw error;await delay(pollMs);}
        }
        await ssh.removeWorker(connection,node,input,password);
        await center.retire({...input,phase:'finish'});
      }
      await store.finishWorkerRemoval(node.id,operationId);
    }catch(error){
      const rejected=!reserved && ['WORKER_NOT_IDLE','WORKER_DEPLOYMENT_MISMATCH','WORKER_STATE_UNKNOWN'].includes(error.code);
      await store.finishWorkerRemoval(node.id,operationId,
        error.code==='WORKER_STATE_UNKNOWN'?'Worker 状态未知，未删除。':error.code==='WORKER_NOT_IDLE'?'该 Worker 有执行中或待恢复的任务，未删除。'
          :'删除尚未完成，采集数据已保留；请重试以核实容器和中心登记。',rejected).catch(()=>{});
    }finally{password=undefined;ssh.close(connection);active.delete(node.id);}
  }
  return {
    async start({id,version,slot,password=''}){
      validateBootstrapPassword(password);
      if(!/^incremental-[1-9][0-9]*$/.test(slot??''))throw Object.assign(Error('Worker 编号无效'),{statusCode:400});
      if(active.has(id))throw Object.assign(Error('该节点正在删除 Worker'),{statusCode:409});
      const updated=await store.beginWorkerRemoval({id,version,slot,operationId:randomUUID()});
      const task=execute(updated.nodes.find(n=>n.id===id),password);active.set(id,task);void task.catch(()=>{});
      return updated;
    },
    waitForIdle:()=>Promise.allSettled([...active.values()]),
  };
}

export function workerRemovalFromEnv(store,env=process.env){
  const center=deploymentControlFromEnv(env);
  return center && env.SERVER_NODE_STATE_DIR?createNodeWorkerRemoval({store,center,
    ssh:createNodeSsh({stateDir:env.SERVER_NODE_STATE_DIR})}):null;
}
