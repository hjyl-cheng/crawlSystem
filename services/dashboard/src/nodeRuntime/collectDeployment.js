import {buildNodeConnectionDeployment} from './connectionDeployment.js';
import {createHash} from 'node:crypto';

// Reuse the validated, fixed deployment recipe; only the explicit collecting
// image gains a durable spool and the original fingerprint runtime's resources.
export function buildNodeCollectDeployment(args) {
  const plan=buildNodeConnectionDeployment(args);
  plan.mode='incremental_collect';plan.memoryLimitMiB=plan.count*768;
  for(const [slot,service] of Object.entries(plan.compose.services)){
    const config=JSON.parse(plan.files[`${slot}.json`]);config.mode=plan.mode;
    plan.files[`${slot}.json`]=JSON.stringify(config)+'\n';
    Object.assign(plan.registrations.find(item=>item.slot===slot),{mode:plan.mode,
      configHash:createHash('sha256').update(plan.files[`${slot}.json`]).digest('hex')});
    service.labels['qy.remote.mode']=plan.mode;
    service.mem_limit='768m';service.pids_limit=128;service.stop_grace_period='16m';
    service.tmpfs=['/tmp:rw,noexec,nosuid,size=64m,uid=1000,gid=1000','/run/qy-node:rw,noexec,nosuid,size=1m,uid=1000,gid=1000'];
    service.volumes.push({type:'bind',source:`/var/lib/qy-node/spool/${slot}`,target:'/var/lib/qy-node/spool',read_only:false,bind:{create_host_path:false}});
  }
  // Hashes must describe the actual collecting config, not the connection-only recipe.
  return plan;
}

export function collectDeploymentPreview(node,env=process.env){
  if(!env.SERVER_NODE_COLLECT_IMAGE || !env.SERVER_NODE_GATEWAY_URL)return {available:false,reason:'中心尚未配置完整采集镜像和 HTTPS 网关。'};
  try{return {available:true,...buildNodeCollectDeployment({node,image:env.SERVER_NODE_COLLECT_IMAGE,gatewayUrl:env.SERVER_NODE_GATEWAY_URL,
    ...(node.deployment?.deploymentId?{deploymentId:node.deployment.deploymentId}:{})})};}
  catch(error){return {available:false,reason:error.message};}
}
