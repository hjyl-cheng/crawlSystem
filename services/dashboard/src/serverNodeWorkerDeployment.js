import {randomUUID} from 'node:crypto';
import {setTimeout as delay} from 'node:timers/promises';
import {createNodeSsh,validateBootstrapPassword} from './serverNodeSsh.js';
import {buildNodeCollectDeployment} from './nodeRuntime/collectDeployment.js';
import {deploymentControlFromEnv} from './nodeRuntime/deploymentControlClient.js';
import {registryCredentialsFromEnv} from './nodeRuntime/registryCredentials.js';

export function createNodeWorkerDeployment({store,ssh,center,image,gatewayUrl,waitMs=60000,pollMs=1000,registryCredentials=async()=>null}){
  const active=new Map();
  async function execute(node,operationId,plan,password){
    let connection;let step='center';
    const advance=patch=>store.advanceWorkerDeployment(node.id,operationId,patch);
    try{
      await advance({steps:{center:'running'}});
      const registry=await registryCredentials(plan.image);
      const credentials=await center.prepare(plan);
      if(credentials.nodeId!==node.id || credentials.deploymentId!==plan.deploymentId || credentials.readyForTasks!==false
        || !/^[a-f0-9]{64}$/.test(credentials.nodeToken) || typeof credentials.publicKey!=='string'
        || plan.registrations.some(row=>!/^[a-f0-9]{64}$/.test(credentials.relayTokens?.[row.slot])))throw new Error('中心部署登记返回内容不完整或与节点不匹配');
      await advance({steps:{center:'completed',ssh:'running'}});step='ssh';
      if(registry)credentials.registry=registry;
      connection=await ssh.connect(node,{keyOnly:true});await ssh.verify(connection,password);
      await advance({steps:{ssh:'completed'}});
      await ssh.deployWorkers(connection,node,plan,credentials,password,async next=>{
        step=next;await advance({remoteChanges:true,steps:{[step]:'running'}});
      },async finished=>advance({steps:{[finished]:'completed'}}));
      step='connection';await advance({steps:{connection:'running'}});
      const deadline=Date.now()+waitMs;let status;
      for(;;){
        status=await center.status(plan);
        if(status.nodeId===node.id && status.deploymentId===plan.deploymentId
          && status.workers?.length===plan.count && plan.registrations.every(row=>status.workers.some(w=>w.slot===row.slot && w.connected)))break;
        if(Date.now()>=deadline)throw new Error('Worker 已启动，但尚未全部连接中心；可重试检查，不会重新生成凭据');
        await delay(pollMs);
      }
      await advance({state:'connected',appliedCount:plan.count,steps:{connection:'completed'},finishedAt:new Date().toISOString(),error:null});
    }catch{
      // Neither SSH/HTTP errors nor the transient credential bundle reach the
      // registry/logging. A durable operation can safely retry the frozen plan.
      await advance({state:'failed',finishedAt:new Date().toISOString(),steps:{[step]:'failed'},
        error:`${({center:'中心登记',ssh:'SSH 连接',files:'部署文件准备',start:'容器启动',verify:'容器检查',connection:'中心连接检查'})[step]??'部署'}失败，请检查配置后重试；已有部署记录和采集暂存文件已保留。`}).catch(()=>{});
    }finally{password=undefined;ssh.close(connection);active.delete(node.id);}
  }
  return {
    async start({id,version,password=''}){
      validateBootstrapPassword(password);
      if(active.has(id))throw Object.assign(new Error('该节点正在部署 Worker'),{statusCode:409});
      const registry=await store.load();const node=registry.nodes.find(row=>row.id===id);
      if(!node)throw Object.assign(new Error('服务器不存在'),{statusCode:404});
      const plan=buildNodeCollectDeployment({node,image,gatewayUrl,...(node.deployment?.deploymentId?{deploymentId:node.deployment.deploymentId}:{})});
      const operationId=randomUUID();const updated=await store.beginWorkerDeployment({id,version,operationId,plan});
      const task=execute(node,operationId,plan,password);active.set(id,task);void task.catch(()=>{});return updated;
    },
    waitForIdle:()=>Promise.allSettled([...active.values()]),
  };
}
export function workerDeploymentFromEnv(store,env=process.env){
  const center=deploymentControlFromEnv(env);
  return center && env.SERVER_NODE_STATE_DIR && env.SERVER_NODE_COLLECT_IMAGE && env.SERVER_NODE_GATEWAY_URL
    ?createNodeWorkerDeployment({store,center,image:env.SERVER_NODE_COLLECT_IMAGE,gatewayUrl:env.SERVER_NODE_GATEWAY_URL,
      registryCredentials:registryCredentialsFromEnv(env),ssh:createNodeSsh({stateDir:env.SERVER_NODE_STATE_DIR})}):null;
}
