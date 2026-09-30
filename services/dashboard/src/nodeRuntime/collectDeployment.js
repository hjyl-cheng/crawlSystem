import {buildNodeConnectionDeployment} from './connectionDeployment.js';
import {createHash} from 'node:crypto';
import {nodeWorkerRole,nodeWorkerTypes} from '../nodeWorkerTypes.js';

// Workloads other than incremental run a dedicated image with its own runtime,
// capability and spool mount. deployWorkers.py checks the same fixed values.
const dedicatedProfiles=Object.freeze({
  fullcrawl:Object.freeze({runtimeRevision:'youtubejs-full-crawl-v1',capability:'youtube.full-crawl.v1',
    spoolTarget:'/var/lib/qy-node/full-spool',imageEnv:'SERVER_NODE_FULL_CRAWL_IMAGE',enabledEnv:'SERVER_NODE_FULL_CRAWL_DEPLOYMENT_ENABLED',label:'全量节点'}),
  discover:Object.freeze({runtimeRevision:'youtube-search-discover-v1',capability:'youtube.discover-page.v1',
    spoolTarget:'/var/lib/qy-node/discover-spool',imageEnv:'SERVER_NODE_DISCOVER_IMAGE',enabledEnv:'SERVER_NODE_DISCOVER_DEPLOYMENT_ENABLED',label:'Query 发现节点'}),
});

export const dedicatedWorkerProfile=role=>Object.hasOwn(dedicatedProfiles,role)?dedicatedProfiles[role]:null;

// The image for a role, or null while that role's deployment is not enabled.
export function workerDeploymentImage(role,env=process.env){
  const profile=dedicatedWorkerProfile(role);
  if(!profile)return env.SERVER_NODE_COLLECT_IMAGE||null;
  return env[profile.enabledEnv]==='true'?env[profile.imageEnv]||null:null;
}

// Reuse the validated, fixed deployment recipe; only the explicit collecting
// image gains a durable spool and the original fingerprint runtime's resources.
export function buildNodeCollectDeployment(args) {
  const role=nodeWorkerRole(args.node);
  const profile=dedicatedWorkerProfile(role);
  if(profile&&!args.natsUrl)throw new Error(`${profile.label}必须配置 NATS 接入地址`);
  const plan=buildNodeConnectionDeployment({...args,collecting:true});
  if(args.natsUrl){
    const endpoint=new URL(args.natsUrl);
    if(!['tls:','wss:'].includes(endpoint.protocol)||endpoint.username||endpoint.password||endpoint.search||endpoint.hash||(endpoint.protocol==='wss:'?endpoint.pathname!=='/node-messages':!['','/'].includes(endpoint.pathname)))throw new Error('NATS 节点接入地址必须使用 TLS');
    plan.natsUrl=endpoint.href.replace(/\/$/,'');
    if(!profile)plan.wholeChannel=true;
  }
  plan.mode=nodeWorkerTypes[role].mode;plan.memoryLimitMiB=plan.count*768;
  plan.spoolLimitMiB=plan.count*256;
  if(profile){plan.runtimeRevision=profile.runtimeRevision;plan.capability=profile.capability;}
  for(const [slot,service] of Object.entries(plan.compose.services)){
    const config=JSON.parse(plan.files[`${slot}.json`]);config.mode=plan.mode;
    plan.files[`${slot}.json`]=JSON.stringify(config)+'\n';
    Object.assign(plan.registrations.find(item=>item.slot===slot),{mode:plan.mode,
      configHash:createHash('sha256').update(plan.files[`${slot}.json`]).digest('hex')});
    service.labels['qy.remote.mode']=plan.mode;
    if(plan.natsUrl)service.environment={REMOTE_NODE_NATS_URL:plan.natsUrl,...(profile?{}:{REMOTE_NODE_WHOLE_CHANNEL:'true'})};
    service.mem_limit='768m';service.pids_limit=128;service.stop_grace_period='16m';
    service.tmpfs=['/tmp:rw,noexec,nosuid,size=64m,uid=1000,gid=1000','/run/qy-node:rw,noexec,nosuid,size=1m,uid=1000,gid=1000'];
    service.volumes.push({type:'bind',source:`/var/lib/qy-node/spool/${slot}`,target:profile?.spoolTarget??'/var/lib/qy-node/spool',read_only:false,bind:{create_host_path:false}});
  }
  // Hashes must describe the actual collecting config, not the connection-only recipe.
  return plan;
}

export function collectDeploymentPreview(node,env=process.env){
  const profile=dedicatedWorkerProfile(nodeWorkerRole(node));
  if(profile&&env[profile.enabledEnv]!=='true')return {available:false,reason:`${profile.label}部署尚未开放。`};
  const image=workerDeploymentImage(nodeWorkerRole(node),env);
  if(!image || !env.SERVER_NODE_GATEWAY_URL)return {available:false,reason:'中心尚未配置完整采集镜像和 HTTPS 网关。'};
  if(!env.SERVER_NODE_NATS_URL)return {available:false,reason:'中心尚未配置 NATS 节点接入地址，不能部署采集 Worker。'};
  try{return {available:true,...buildNodeCollectDeployment({node,image,gatewayUrl:env.SERVER_NODE_GATEWAY_URL,natsUrl:env.SERVER_NODE_NATS_URL,
    ...(node.deployment?.deploymentId?{deploymentId:node.deployment.deploymentId}:{})})};}
  catch(error){return {available:false,reason:error.message};}
}
