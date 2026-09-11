import { readNodeFile } from '../src/remoteNodes/workerConfig.js';
const token=(await readNodeFile(process.env.REMOTE_NODE_ADMIN_TOKEN_FILE,{secret:true})).toString().trim();
const id='00000000-0000-4000-8000-000000000000';
const response=await fetch(`http://127.0.0.1:${process.env.REMOTE_NODE_CENTER_PORT||3187}/internal/node-deployments/status`,{
  method:'POST',headers:{authorization:`Bearer ${token}`,'content-type':'application/json'},
  body:JSON.stringify({nodeId:id,deploymentId:id}),signal:AbortSignal.timeout(4000),redirect:'error',
});
const result=await response.json();
if(response.status!==200 || result.nodeId!==id || !Array.isArray(result.workers))throw new Error('REMOTE_CENTER_UNHEALTHY');
