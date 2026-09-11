import {randomBytes,createPublicKey,timingSafeEqual} from 'node:crypto';
import {parseWorkerConfig} from './workerConfig.js';
import {CHANNEL_PLAN_CAPABILITY} from './channelPlanContract.js';
import {hash,RemoteProtocolError,uuid} from './protocol.js';
const fail=code=>{throw new RemoteProtocolError(code);};

// A separate center credential authorizes Dashboard deployment. Node tokens can
// neither enroll nodes nor enable Workers. Credentials are recoverable only by
// the center, encrypted with the route store's existing authenticated cipher.
export function createRemoteDeploymentAdmin({store,routes,token,image,gatewayUrl,activation=null}){
  if(typeof token!=='string' || token.length<32 || !/^[a-z0-9][a-z0-9./:_-]*@sha256:[a-f0-9]{64}$/.test(image))throw new TypeError('fixed deployment image and admin token required');
  const endpoint=new URL(gatewayUrl);if(endpoint.protocol!=='https:' || endpoint.username || endpoint.password || endpoint.search || endpoint.hash)throw new TypeError('HTTPS gateway required');
  const publicKey=createPublicKey(routes.privateKey).export({type:'spki',format:'pem'});
  return {
    authenticate(value){const bytes=Buffer.from(value??'');const secret=Buffer.from(token);if(bytes.length!==secret.length || !timingSafeEqual(bytes,secret))throw new RemoteProtocolError('UNAUTHORIZED',401);},
    async prepare(value){
      if(!value || Object.keys(value).some(key=>!['nodeId','deploymentId','image','files'].includes(key)) || value.image!==image
        || !value.files || typeof value.files!=='object' || Array.isArray(value.files))throw new RemoteProtocolError('INVALID_DEPLOYMENT',400);
      uuid(value.nodeId);uuid(value.deploymentId);
      const names=Object.keys(value.files);if(names.length<1 || names.length>32)throw new RemoteProtocolError('INVALID_DEPLOYMENT',400);
      const configs=Array.from({length:names.length},(_,i)=>{
        const slot=`incremental-${i+1}`;const bytes=value.files[`${slot}.json`];
        if(typeof bytes!=='string' || Buffer.byteLength(bytes)>16384)throw new RemoteProtocolError('INVALID_DEPLOYMENT',400);
        const config=parseWorkerConfig(Buffer.from(bytes),{mode:'incremental_collect'});
        if(config.node_id!==value.nodeId || config.deployment_id!==value.deploymentId || config.slot!==slot
          || new URL(config.gateway_url).href!==endpoint.href)throw new RemoteProtocolError('INVALID_DEPLOYMENT',400);
        return config;
      });
      return store.transaction(async client=>{
        await client.query('SELECT pg_advisory_xact_lock(hashtext($1))',[`remote-deploy:${value.nodeId}`]);
        let node=(await client.query('SELECT * FROM remote_ingestion.nodes WHERE node_id=$1 FOR NO KEY UPDATE',[value.nodeId])).rows[0];
        const old=(await client.query('SELECT * FROM remote_ingestion.node_deployments WHERE node_id=$1 FOR UPDATE',[value.nodeId])).rows[0];
        if(node && (!old || node.state!=='active'))fail('REMOTE_DEPLOYMENT_NODE_CONFLICT');
        if(old && (old.deployment_id!==value.deploymentId || old.image!==value.image || old.worker_count>configs.length))fail('REMOTE_DEPLOYMENT_REQUIRES_DRAIN');
        const credentials=old?routes.decrypt(old.credentials_cipher,`node-deployment:${value.nodeId}`):{nodeToken:randomBytes(32).toString('hex'),relayTokens:{}};
        if(!node)await client.query(`INSERT INTO remote_ingestion.nodes(node_id,token_hash,capabilities,max_leases,slot_claims_required)
          VALUES($1,$2,$3,$4,true)`,[value.nodeId,hash(credentials.nodeToken),[CHANNEL_PLAN_CAPABILITY],configs.length]);
        for(const config of configs){
          credentials.relayTokens[config.slot]??=randomBytes(32).toString('hex');
          const workerId=`remote-${value.nodeId}-${config.slot}`;
          await client.query(`INSERT INTO remote_ingestion.network_slots(node_id,slot,rota_worker_id) VALUES($1,$2,$3) ON CONFLICT(node_id,slot) DO NOTHING`,[value.nodeId,config.slot,workerId]);
          const slot=(await client.query('SELECT rota_worker_id FROM remote_ingestion.network_slots WHERE node_id=$1 AND slot=$2',[value.nodeId,config.slot])).rows[0];
          if(slot.rota_worker_id!==workerId)fail('NETWORK_SLOT_CONFLICT');
          await client.query(`INSERT INTO remote_ingestion.worker_connections(node_id,slot,deployment_id,config_hash,role,mode)
            VALUES($1,$2,$3,$4,'incremental','incremental_collect') ON CONFLICT(node_id,slot) DO NOTHING`,[value.nodeId,config.slot,value.deploymentId,config.config_hash]);
          const row=(await client.query('SELECT * FROM remote_ingestion.worker_connections WHERE node_id=$1 AND slot=$2 FOR UPDATE',[value.nodeId,config.slot])).rows[0];
          if(row.deployment_id!==value.deploymentId || row.config_hash!==config.config_hash || row.mode!=='incremental_collect')fail('WORKER_DEPLOYMENT_CONFLICT');
        }
        await client.query('UPDATE remote_ingestion.nodes SET max_leases=$2,slot_claims_required=true WHERE node_id=$1',[value.nodeId,configs.length]);
        await client.query(`INSERT INTO remote_ingestion.node_deployments(node_id,deployment_id,image,worker_count,credentials_cipher)
          VALUES($1,$2,$3,$4,$5) ON CONFLICT(node_id) DO UPDATE SET worker_count=EXCLUDED.worker_count,
          credentials_cipher=EXCLUDED.credentials_cipher,updated_at=clock_timestamp()`,[value.nodeId,value.deploymentId,value.image,configs.length,routes.encrypt(credentials,`node-deployment:${value.nodeId}`)]);
        return {nodeId:value.nodeId,deploymentId:value.deploymentId,nodeToken:credentials.nodeToken,relayTokens:credentials.relayTokens,publicKey,
          readyForTasks:false};
      });
    },
    async status({nodeId,deploymentId}){
      uuid(nodeId);uuid(deploymentId);
      return store.transaction(async client=>{
        const rows=(await client.query(`SELECT w.*,n.state AS node_state,w.connected_until>clock_timestamp() AS connected
          FROM remote_ingestion.worker_connections w JOIN remote_ingestion.nodes n USING(node_id)
          WHERE w.node_id=$1 AND deployment_id=$2 ORDER BY slot`,[nodeId,deploymentId])).rows;
        const workers=[];
        for(const row of rows)workers.push({slot:row.slot,connected:row.connected===true,
          readyForTasks:row.connected===true && row.enabled && row.accepting && row.node_state==='active'
            && typeof activation?.verifyExecution==='function' && await activation.verifyExecution(client,row)===true});
        return {nodeId,deploymentId,workers};
      });
    },
  };
}
