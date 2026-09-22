import {randomUUID} from 'node:crypto';
import {setTimeout as delay} from 'node:timers/promises';
import {createNodeSsh,validateBootstrapPassword} from './serverNodeSsh.js';
import {buildNodeCollectDeployment} from './nodeRuntime/collectDeployment.js';
import {deploymentControlFromEnv} from './nodeRuntime/deploymentControlClient.js';
import {registryCredentialsFromEnv} from './nodeRuntime/registryCredentials.js';
import {assertNodeWorkerDeployment,nodeWorkerTypes,nodeWorkerRole} from './nodeWorkerTypes.js';

export function createNodeWorkerDeployment({store,ssh,center,image,fullCrawlImage=null,fullCrawlDeploymentEnabled=false,gatewayUrl,natsUrl,waitMs=60000,pollMs=1000,registryCredentials=async()=>null}){
  const active=new Map();
  async function execute(node,operationId,plan,password){
    let connection;let step='ssh';
    const advance=patch=>store.advanceWorkerDeployment(node.id,operationId,patch);
    try{
      await advance({steps:{ssh:'running'}});
      connection=await ssh.connect(node,{keyOnly:true});await ssh.verify(connection,password);
      await advance({steps:{ssh:'completed',center:'running'}});step='center';
      const registry=await registryCredentials(plan.image);
      const credentials=await center.prepare(plan);
      if(credentials.nodeId!==node.id || credentials.deploymentId!==plan.deploymentId || credentials.readyForTasks!==false
        || !/^[a-f0-9]{64}$/.test(credentials.nodeToken) || typeof credentials.publicKey!=='string'
        || plan.registrations.some(row=>!/^[a-f0-9]{64}$/.test(credentials.relayTokens?.[row.slot])))throw new Error('中心部署登记返回内容不完整或与节点不匹配');
      const pausedSlots=credentials.pausedSlots??[];
      if(!Array.isArray(pausedSlots)||pausedSlots.some(slot=>!plan.slots.includes(slot)))throw new Error('中心暂停 Worker 列表无效');
      for(const slot of pausedSlots)Object.assign(plan.compose.services[slot],{restart:'no',profiles:['paused']});
      await advance({steps:{center:'completed'}});
      if(registry)credentials.registry=registry;
      await ssh.deployWorkers(connection,node,plan,credentials,password,async next=>{
        step=next;await advance({remoteChanges:true,steps:{[step]:'running'}});
      },async finished=>advance({steps:{[finished]:'completed'}}));
      step='connection';await advance({steps:{connection:'running'}});
      const deadline=Date.now()+waitMs;let status;
      for(;;){
        status=await center.status(plan);
        if(status.nodeId===node.id && status.deploymentId===plan.deploymentId
          && status.workers?.length>=plan.count && plan.registrations.every(row=>status.workers.some(w=>w.slot===row.slot && (pausedSlots.includes(row.slot)?w.paused:w.connected))))break;
        if(Date.now()>=deadline)throw new Error('Worker 已启动，但尚未全部连接中心；可重试检查，不会重新生成凭据');
        await delay(pollMs);
      }
      await advance({appliedCount:plan.count,steps:{connection:'completed'}});
      await advance({state:'connected',finishedAt:new Date().toISOString(),error:null});
    }catch(error){
      // Neither SSH/HTTP errors nor the transient credential bundle reach the
      // registry/logging. A durable operation can safely retry the frozen plan.
      await advance({state:'failed',finishedAt:new Date().toISOString(),steps:{[step]:'failed'},
        error:step==='center' && ['REMOTE_NETWORK_CAPACITY_UNAVAILABLE','REMOTE_NETWORK_CAPACITY_LIMIT','REMOTE_CONTROL_BUSY'].includes(error?.code) ? error.message : `${({center:'中心登记及网络准备',ssh:'SSH 连接与资源检查',files:'部署文件准备',pull:'下载镜像（最长 30 分钟）',start:'容器启动',verify:'容器检查',connection:'中心连接检查'})[step]??'部署'}失败，请检查配置后重试；已有部署记录和采集暂存文件已保留。`}).catch(()=>{});
    }finally{password=undefined;ssh.close(connection);active.delete(node.id);}
  }
  return {
    async start({id,version,password='',count,additionalCount,role,expectedInstalledCount}){
      validateBootstrapPassword(password);
      const invalid=message=>Object.assign(new Error(message),{statusCode:400});
      if(count!==undefined && (!Number.isSafeInteger(count)||count<1))throw invalid('部署数量必须为正整数');
      if(active.has(id))throw Object.assign(new Error('该节点正在部署 Worker'),{statusCode:409});
      const registry=await store.load();const node=registry.nodes.find(row=>row.id===id);
      if(!node)throw Object.assign(new Error('服务器不存在'),{statusCode:404});
      assertNodeWorkerDeployment(node);
      const workerRole=nodeWorkerRole(node);
      if(workerRole==='fullcrawl'&&(!fullCrawlDeploymentEnabled||!fullCrawlImage))throw Object.assign(new Error('全量节点部署尚未开放'),{statusCode:409});
      if(additionalCount!==undefined){
        if(count!==undefined || !Number.isSafeInteger(additionalCount)||additionalCount<1)throw invalid('新增数量必须为正整数，且不能同时填写总数');
        if(typeof role!=='string'||!Object.hasOwn(nodeWorkerTypes,role))throw invalid('请选择有效的 Worker 功能类型');
        assertNodeWorkerDeployment(node, role);
        const installed=node.deployment?.appliedCount??0;
        if(!Number.isInteger(expectedInstalledCount)||expectedInstalledCount!==installed)throw Object.assign(new Error('已部署数量发生变化，请刷新后重新确认新增数量'),{statusCode:409});
        count=installed+additionalCount;
        if(!Number.isSafeInteger(count))throw invalid('新增后的部署总数无效');
      }else if(role!==undefined||expectedInstalledCount!==undefined)throw invalid('请同时填写新增数量');
      const plan=buildNodeCollectDeployment({node:count===undefined?node:{...node,workers:[{role:workerRole,count}]},image:workerRole==='fullcrawl'?fullCrawlImage:image,gatewayUrl,natsUrl,...(node.deployment?.deploymentId?{deploymentId:node.deployment.deploymentId}:{})});
      const operationId=randomUUID();const updated=await store.beginWorkerDeployment({id,version,operationId,plan,count});
      const task=execute(node,operationId,plan,password);active.set(id,task);void task.catch(()=>{});return updated;
    },
    waitForIdle:()=>Promise.allSettled([...active.values()]),
  };
}
export function workerDeploymentFromEnv(store,env=process.env){
  const center=deploymentControlFromEnv(env);
  return center && env.SERVER_NODE_STATE_DIR && env.SERVER_NODE_COLLECT_IMAGE && env.SERVER_NODE_GATEWAY_URL && env.SERVER_NODE_NATS_URL
    ?createNodeWorkerDeployment({store,center,image:env.SERVER_NODE_COLLECT_IMAGE,fullCrawlImage:env.SERVER_NODE_FULL_CRAWL_IMAGE,fullCrawlDeploymentEnabled:env.SERVER_NODE_FULL_CRAWL_DEPLOYMENT_ENABLED==='true',gatewayUrl:env.SERVER_NODE_GATEWAY_URL,natsUrl:env.SERVER_NODE_NATS_URL,
      registryCredentials:registryCredentialsFromEnv(env),ssh:createNodeSsh({stateDir:env.SERVER_NODE_STATE_DIR})}):null;
}
