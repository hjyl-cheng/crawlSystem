import {randomBytes,createPublicKey,timingSafeEqual} from 'node:crypto';
import {parseWorkerConfig} from './workerConfig.js';
import {CHANNEL_PLAN_CAPABILITY} from './channelPlanContract.js';
import {hash,RemoteProtocolError,uuid} from './protocol.js';
import {selectIntakeWorkers,intakeStatus} from './intakeSelection.js';
const fail=code=>{throw new RemoteProtocolError(code);};

// A separate center credential authorizes Dashboard deployment. Node tokens can
// neither enroll nodes nor enable Workers. Credentials are recoverable only by
// the center, encrypted with the route store's existing authenticated cipher.
export function createRemoteDeploymentAdmin({store,routes,token,image,gatewayUrl,activation=null,execution=null,capacity=null,localIntake=null,natsProvisioning=null}){
  if(typeof token!=='string' || token.length<32 || !/^[a-z0-9][a-z0-9./:_-]*@sha256:[a-f0-9]{64}$/.test(image))throw new TypeError('fixed deployment image and admin token required');
  const endpoint=new URL(gatewayUrl);if(endpoint.protocol!=='https:' || endpoint.username || endpoint.password || endpoint.search || endpoint.hash)throw new TypeError('HTTPS gateway required');
  const publicKey=createPublicKey(routes.privateKey).export({type:'spki',format:'pem'});
  return {
    authenticate(value){const bytes=Buffer.from(value??'');const secret=Buffer.from(token);if(bytes.length!==secret.length || !timingSafeEqual(bytes,secret))throw new RemoteProtocolError('UNAUTHORIZED',401);},
    async prepare(value){
      if(!value || Object.keys(value).some(key=>!['nodeId','deploymentId','image','files'].includes(key)) || value.image!==image
        || !value.files || typeof value.files!=='object' || Array.isArray(value.files))throw new RemoteProtocolError('INVALID_DEPLOYMENT',400);
      uuid(value.nodeId);uuid(value.deploymentId);
      const names=Object.keys(value.files);if(names.length<1)throw new RemoteProtocolError('INVALID_DEPLOYMENT',400);
      const configs=Array.from({length:names.length},(_,i)=>{
        const slot=`incremental-${i+1}`;const bytes=value.files[`${slot}.json`];
        if(typeof bytes!=='string' || Buffer.byteLength(bytes)>16384)throw new RemoteProtocolError('INVALID_DEPLOYMENT',400);
        const config=parseWorkerConfig(Buffer.from(bytes),{mode:'incremental_collect'});
        if(config.node_id!==value.nodeId || config.deployment_id!==value.deploymentId || config.slot!==slot
          || new URL(config.gateway_url).href!==endpoint.href)throw new RemoteProtocolError('INVALID_DEPLOYMENT',400);
        return config;
      });
      const result=await store.transaction(async client=>{
        await client.query('SELECT pg_advisory_xact_lock(hashtext($1))',[`remote-deploy:${value.nodeId}`]);
        let node=(await client.query('SELECT * FROM remote_ingestion.nodes WHERE node_id=$1 FOR NO KEY UPDATE',[value.nodeId])).rows[0];
        const old=(await client.query('SELECT * FROM remote_ingestion.node_deployments WHERE node_id=$1 FOR UPDATE',[value.nodeId])).rows[0];
        if(node && (!old || node.state!=='active'))fail('REMOTE_DEPLOYMENT_NODE_CONFLICT');
        if(old && (old.deployment_id!==value.deploymentId || old.image!==value.image))fail('REMOTE_DEPLOYMENT_REQUIRES_DRAIN');
        if(old?.worker_count>configs.length){
          // A failed expansion may leave unused registrations. Keep them reserved;
          // never remove or reuse a slot with a live connection or unsettled work.
          const excluded=(await client.query(`SELECT w.slot FROM remote_ingestion.worker_connections w
            WHERE w.node_id=$1 AND NOT (w.slot=ANY($2::text[])) AND
            (w.activation_requested OR w.enabled OR w.connected_until>clock_timestamp() OR
             EXISTS(SELECT 1 FROM remote_ingestion.tasks t WHERE t.target_node_id=w.node_id
               AND t.target_worker_slot=w.slot AND t.state IN ('pending','leased','received')))`,[value.nodeId,configs.map(c=>c.slot)])).rows;
          if(excluded.length)fail('REMOTE_DEPLOYMENT_REQUIRES_DRAIN');
        }
        const registeredCount=Math.max(old?.worker_count??0,configs.length);
        const credentials=old?routes.decrypt(old.credentials_cipher,`node-deployment:${value.nodeId}`):{nodeToken:randomBytes(32).toString('hex'),relayTokens:{}};
        if(!node)await client.query(`INSERT INTO remote_ingestion.nodes(node_id,token_hash,capabilities,max_leases,slot_claims_required)
          VALUES($1,$2,$3,$4,true)`,[value.nodeId,hash(credentials.nodeToken),[CHANNEL_PLAN_CAPABILITY],configs.length]);
        for(const config of configs){
          credentials.relayTokens[config.slot]??=randomBytes(32).toString('hex');
          const workerId=`remote-${value.nodeId}-${config.slot}`;
          await client.query(`INSERT INTO remote_ingestion.network_slots(node_id,slot,rota_worker_id) VALUES($1,$2,$3) ON CONFLICT(node_id,slot) DO NOTHING`,[value.nodeId,config.slot,workerId]);
          const slot=(await client.query('SELECT rota_worker_id FROM remote_ingestion.network_slots WHERE node_id=$1 AND slot=$2',[value.nodeId,config.slot])).rows[0];
          if(slot.rota_worker_id!==workerId)fail('NETWORK_SLOT_CONFLICT');
          await client.query(`INSERT INTO remote_ingestion.worker_connections(node_id,slot,deployment_id,config_hash,role,mode,activation_requested)
            VALUES($1,$2,$3,$4,'incremental','incremental_collect',false) ON CONFLICT(node_id,slot) DO NOTHING`,[value.nodeId,config.slot,value.deploymentId,config.config_hash]);
          const row=(await client.query('SELECT * FROM remote_ingestion.worker_connections WHERE node_id=$1 AND slot=$2 FOR UPDATE',[value.nodeId,config.slot])).rows[0];
          if(row.deployment_id!==value.deploymentId || row.config_hash!==config.config_hash || row.mode!=='incremental_collect')fail('WORKER_DEPLOYMENT_CONFLICT');
        }
        await client.query('UPDATE remote_ingestion.nodes SET max_leases=$2,slot_claims_required=true WHERE node_id=$1',[value.nodeId,registeredCount]);
        await client.query(`INSERT INTO remote_ingestion.node_deployments(node_id,deployment_id,image,worker_count,credentials_cipher)
          VALUES($1,$2,$3,$4,$5) ON CONFLICT(node_id) DO UPDATE SET worker_count=EXCLUDED.worker_count,
          credentials_cipher=EXCLUDED.credentials_cipher,updated_at=clock_timestamp()`,[value.nodeId,value.deploymentId,value.image,registeredCount,routes.encrypt(credentials,`node-deployment:${value.nodeId}`)]);
        return {nodeId:value.nodeId,deploymentId:value.deploymentId,nodeToken:credentials.nodeToken,relayTokens:credentials.relayTokens,publicKey,
          readyForTasks:false};
      });
      // Registration is durable before provisioning. A failed capacity request
      // can retry the same deployment without adding slots or issuing new IDs.
      await capacity?.ensure();
      await natsProvisioning?.sync();
      return result;
    },
    async setExecution(value){
      if(value?.nodeId==='local-center'){
        if(!localIntake)fail('LOCAL_INTAKE_NOT_CONFIGURED');
        const {nodeId,...control}=value;return localIntake.setExecution(control);
      }
      const byCount=Number.isInteger(value?.allowedCount);
      if(!value || Object.keys(value).some(k=>!['nodeId','deploymentId','enabled','workerCount','expectedRequested','allowedCount','expectedAllowedCount'].includes(k))
        || (byCount ? !Number.isInteger(value.expectedAllowedCount) : typeof value.enabled!=='boolean' || typeof value.expectedRequested!=='boolean')
        || !Number.isSafeInteger(value.workerCount) || value.workerCount<1)throw new RemoteProtocolError('INVALID_EXECUTION_CONTROL',400);
      uuid(value.nodeId);uuid(value.deploymentId);
      const desired=byCount?value.allowedCount:value.enabled?value.workerCount:0;
      if(desired<0 || desired>value.workerCount)throw new RemoteProtocolError('INVALID_EXECUTION_COUNT',400);
      if(desired>0 && !execution?.allowsNode(value.nodeId))fail('REMOTE_CENTER_EXECUTION_NOT_CONFIGURED');
      if(desired>0)await capacity?.ensure();
      await store.transaction(async client=>{
        await client.query('SELECT pg_advisory_xact_lock(hashtext($1))',[`remote-deploy:${value.nodeId}`]);
        const node=(await client.query('SELECT state FROM remote_ingestion.nodes WHERE node_id=$1 FOR NO KEY UPDATE',[value.nodeId])).rows[0];
        const deployment=(await client.query('SELECT * FROM remote_ingestion.node_deployments WHERE node_id=$1 FOR UPDATE',[value.nodeId])).rows[0];
        if(!node || !deployment || deployment.deployment_id!==value.deploymentId || value.workerCount>deployment.worker_count)fail('WORKER_DEPLOYMENT_MISMATCH');
        const rows=(await client.query(`SELECT *,connected_until>clock_timestamp() AS alive
          FROM remote_ingestion.worker_connections WHERE node_id=$1 AND deployment_id=$2 ORDER BY slot FOR UPDATE`,[value.nodeId,value.deploymentId])).rows;
        if(rows.length!==deployment.worker_count || rows.some(r=>r.mode!=='incremental_collect'))fail('WORKER_DEPLOYMENT_MISMATCH');
        const requested=rows.some(r=>r.activation_requested);
        const currentCount=rows.filter(r=>r.activation_requested).length;
        if(byCount ? currentCount!==value.expectedAllowedCount && currentCount!==desired
          : requested!==value.expectedRequested && !rows.every(r=>r.activation_requested===value.enabled))fail('EXECUTION_CONTROL_CHANGED');
        // Registration precedes installation. A failed additive deployment must
        // not remove control of the previously verified prefix of Worker slots.
        const installed=new Set(Array.from({length:value.workerCount},(_,i)=>`incremental-${i+1}`));
        const eligible=rows.filter(row=>installed.has(row.slot));
        if(eligible.length!==value.workerCount)fail('WORKER_DEPLOYMENT_MISMATCH');
        const selected=selectIntakeWorkers(eligible,desired);
        if(desired>0 && (node.state!=='active' || selected.some(r=>!r.activation_requested && (!r.alive || !r.accepting))))fail('WORKER_NOT_READY');
        // A pause changes desired intake only. The owner retains enabled until
        // it has drained the active processor and released its Rota session.
        await client.query('UPDATE remote_ingestion.worker_connections SET activation_requested=(slot=ANY($3::text[])) WHERE node_id=$1 AND deployment_id=$2',
          [value.nodeId,value.deploymentId,selected.map(r=>r.slot)]);
      });
      return this.status(value);
    },
    async status({nodeId,deploymentId}){
      if(nodeId==='local-center'){if(!localIntake)fail('LOCAL_INTAKE_NOT_CONFIGURED');return localIntake.status();}
      uuid(nodeId);uuid(deploymentId);
      const result=await store.transaction(async client=>{
        const rows=(await client.query(`SELECT w.*,n.state AS node_state,w.connected_until>clock_timestamp() AS connected,
          EXISTS(SELECT 1 FROM remote_ingestion.tasks t WHERE t.target_node_id=w.node_id AND t.target_worker_slot=w.slot
            AND t.state IN ('pending','leased','received')) AS unsettled
          FROM remote_ingestion.worker_connections w JOIN remote_ingestion.nodes n USING(node_id)
          WHERE w.node_id=$1 AND deployment_id=$2 ORDER BY slot`,[nodeId,deploymentId])).rows;
        const workers=[];
        for(const row of rows)workers.push({slot:row.slot,connected:row.connected===true,preparation:execution?.preparationState?.(row)??null,
          requested:row.activation_requested,enabled:row.enabled,active:execution?.isProcessing(row)===true || row.unsettled===true,
          readyForTasks:row.connected===true && row.activation_requested && row.enabled && row.accepting && row.node_state==='active'
            && typeof activation?.verifyExecution==='function' && await activation.verifyExecution(client,row)===true});
        return {nodeId,deploymentId,workers,executionAvailable:execution?.allowsNode(nodeId)===true,
          ...intakeStatus(workers)};
      });
      // Network inspection must not hold a business database transaction open.
      if(result.workers.some(w=>w.preparation==='waiting_network'))result.networkCapacity=await execution?.networkCapacity?.()??null;
      return result;
    },
  };
}
