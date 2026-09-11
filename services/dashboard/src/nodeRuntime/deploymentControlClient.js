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
      if(!response.ok){await response.body?.cancel();throw new Error();}
      let size=0;const chunks=[];
      for await(const chunk of response.body){size+=chunk.length;if(size>512*1024)throw new Error();chunks.push(chunk);}
      return JSON.parse(Buffer.concat(chunks).toString());
    }catch{throw new Error('中心部署控制请求失败，请核实中心接入服务和部署配置');}
  }
  return {prepare:plan=>request('prepare',{nodeId:plan.nodeId,deploymentId:plan.deploymentId,image:plan.image,files:plan.files}),
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
  return {prepare:async plan=>(await load()).prepare(plan),status:async plan=>(await load()).status(plan)};
}
