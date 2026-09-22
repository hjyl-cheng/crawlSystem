import {open} from 'node:fs/promises';
import {constants} from 'node:fs';

export function createDeploymentControlClient({url,token,fetchImpl=fetch}){
  const endpoint=new URL(url);
  if(endpoint.protocol!=='https:' || endpoint.username || endpoint.password || endpoint.search || endpoint.hash
    || typeof token!=='string' || token.length<32)throw new Error('节点部署控制连接配置无效');
  async function request(operation,value){
    try{
      const response=await fetchImpl(`${endpoint.href.replace(/\/$/,'')}/internal/node-deployments/${operation}`,{
        method:'POST',redirect:'error',signal:AbortSignal.timeout(15000),headers:{authorization:`Bearer ${token}`,'content-type':'application/json'},body:JSON.stringify(value)});
      let size=0;const chunks=[];
      for await(const chunk of response.body){size+=chunk.length;if(size>512*1024)throw new Error();chunks.push(chunk);}
      const result=JSON.parse(Buffer.concat(chunks).toString());
      if(!response.ok){
        const messages={WORKER_STATE_UNKNOWN:'Worker 状态无法确认，暂时不能删除',WORKER_NOT_IDLE:'该 Worker 有执行中或待恢复的任务，不能删除',
          WORKER_RETIREMENT_WAIT:'正在停止接单并释放执行资源，请稍候',
          WORKER_RETIREMENT_CONFLICT:'Worker 删除尚未完成或操作已变化，请刷新后重试',
          INVALID_WORKER_RETIREMENT:'Worker 删除参数无效',REMOTE_NETWORK_CAPACITY_UNAVAILABLE:'网络名额自动扩容暂未完成，请稍后重试；节点登记已保留',
          REMOTE_CONTROL_BUSY:'中心正在处理其他节点操作，本次操作未完成，请稍后重试',
          INVALID_EXECUTION_COUNT:'允许接任务数量应为 0 到实际已部署数量之间的整数',
          EXECUTION_COUNT_CONTROL_REMOVED:'接单容量由已部署 Worker 数量自动决定，不能单独设置',
          LOCAL_INTAKE_NOT_CONFIGURED:'中心服务器接任务控制尚未就绪',
          REMOTE_NETWORK_CAPACITY_LIMIT:'所需网络名额超过当前系统上限，请调整 Worker 数量',
          REMOTE_CENTER_EXECUTION_NOT_CONFIGURED:'中心尚未开放此节点的接任务控制',
          WORKER_NOT_READY:'Worker 尚未全部在线就绪，请稍后重试',WORKER_DEPLOYMENT_MISMATCH:'部署状态已变化，请刷新后重试',
          EXECUTION_CONTROL_CHANGED:'接任务状态已被其他操作修改，请刷新后重试'};
        if(messages[result.error])throw Object.assign(new Error(messages[result.error]),{statusCode:['REMOTE_NETWORK_CAPACITY_UNAVAILABLE','REMOTE_CONTROL_BUSY'].includes(result.error)?503:409,code:result.error});
        throw new Error();
      }
      return result;
    }catch(error){if(error.statusCode)throw error;throw new Error('中心部署控制请求失败，请核实中心接入服务和部署配置');}
  }
  return {retire:value=>request('retire',value),setExecution:value=>request('execution',value),prepare:plan=>request('prepare',{nodeId:plan.nodeId,deploymentId:plan.deploymentId,image:plan.image,files:plan.files}),
    status:plan=>request('status',{nodeId:plan.nodeId,deploymentId:plan.deploymentId})};
}

export function deploymentControlFromEnv(env=process.env){
  if(!env.SERVER_NODE_WORKER_CONTROL_URL || !env.SERVER_NODE_WORKER_CONTROL_TOKEN_FILE)return null;
  let client;
  const load=async()=>{
    if(!client){
      const file=await open(env.SERVER_NODE_WORKER_CONTROL_TOKEN_FILE,constants.O_RDONLY|constants.O_NOFOLLOW|constants.O_NONBLOCK);
      try {
        const info=await file.stat();
        if(!info.isFile() || info.size>512 || (info.mode&0o077))throw new Error('中心部署凭据文件权限无效');
        const buffer=Buffer.alloc(513);const {bytesRead}=await file.read(buffer,0,buffer.length,0);
        if(bytesRead>512)throw new Error('中心部署凭据文件无效');
        client=createDeploymentControlClient({url:env.SERVER_NODE_WORKER_CONTROL_URL,token:buffer.subarray(0,bytesRead).toString('utf8').trim()});
      } finally {await file.close();}
    }
    return client;
  };
  return {retire:async value=>(await load()).retire(value),setExecution:async value=>(await load()).setExecution(value),prepare:async plan=>(await load()).prepare(plan),status:async plan=>(await load()).status(plan)};
}
