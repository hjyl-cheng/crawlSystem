import {randomBytes,createPublicKey,timingSafeEqual} from 'node:crypto';
import {parseWorkerConfig} from './workerConfig.js';
import {collectingWorkload,FULL_CRAWL_WORKLOAD} from './collectingWorkload.js';
import {fullCrawlSlotUnsettled} from './fullCrawlCenterRecovery.js';
import {hash,RemoteProtocolError,uuid} from './protocol.js';
import {selectIntakeWorkers,intakeStatus} from './intakeSelection.js';
import {createWorkerRetirement} from './workerRetirement.js';
import {reconcileIntakeRequests} from './intakeRequests.js';
import {readIntakeControl,saveIntakeControl,changeIntakeControl} from './intakeControl.js';
const fail=code=>{throw new RemoteProtocolError(code);};

// A separate center credential authorizes Dashboard deployment. Node tokens can
// neither enroll nodes nor enable Workers. Credentials are recoverable only by
// the center, encrypted with the route store's existing authenticated cipher.
export function createRemoteDeploymentAdmin({store,routes,token,image,gatewayUrl,activation=null,execution=null,capacity=null,localIntake=null,natsProvisioning=null,fullCrawl=null}){
  if(typeof token!=='string' || token.length<32 || !/^[a-z0-9][a-z0-9./:_-]*@sha256:[a-f0-9]{64}$/.test(image))throw new TypeError('fixed deployment image and admin token required');
  if(fullCrawl&&(!/^[a-z0-9][a-z0-9./:_-]*@sha256:[a-f0-9]{64}$/.test(fullCrawl.image)||fullCrawl.image===image))throw new TypeError('dedicated fixed full-crawl image required');
  const executionFor=row=>row.mode===FULL_CRAWL_WORKLOAD.mode?fullCrawl?.execution:execution;
  const activationFor=row=>row.mode===FULL_CRAWL_WORKLOAD.mode?fullCrawl?.activation:activation;
  const endpoint=new URL(gatewayUrl);if(endpoint.protocol!=='https:' || endpoint.username || endpoint.password || endpoint.search || endpoint.hash)throw new TypeError('HTTPS gateway required');
  const publicKey=createPublicKey(routes.privateKey).export({type:'spki',format:'pem'});
  return {
    retire:createWorkerRetirement({store,execution,fullCrawlExecution:fullCrawl?.execution}),
    authenticate(value){const bytes=Buffer.from(value??'');const secret=Buffer.from(token);if(bytes.length!==secret.length || !timingSafeEqual(bytes,secret))throw new RemoteProtocolError('UNAUTHORIZED',401);},
    async prepare(value){
      if(!value || Object.keys(value).some(key=>!['nodeId','deploymentId','image','files'].includes(key)) || ![image,fullCrawl?.image].filter(Boolean).includes(value.image)
        || !value.files || typeof value.files!=='object' || Array.isArray(value.files))throw new RemoteProtocolError('INVALID_DEPLOYMENT',400);
      uuid(value.nodeId);uuid(value.deploymentId);
      const workload=collectingWorkload(value.image===image?'incremental_collect':FULL_CRAWL_WORKLOAD.mode);
      const slotPattern=workload.role==='fullcrawl'?/^full-crawl-[1-9][0-9]*\.json$/:/^incremental-[1-9][0-9]*\.json$/;
      const names=Object.keys(value.files);if(names.length<1)throw new RemoteProtocolError('INVALID_DEPLOYMENT',400);
      const configs=names.map(name=>{
        if(!slotPattern.test(name))throw new RemoteProtocolError('INVALID_DEPLOYMENT',400);
        const slot=name.slice(0,-5);const bytes=value.files[`${slot}.json`];
        if(typeof bytes!=='string' || Buffer.byteLength(bytes)>16384)throw new RemoteProtocolError('INVALID_DEPLOYMENT',400);
        const config=parseWorkerConfig(Buffer.from(bytes),{mode:workload.mode});
        if(config.node_id!==value.nodeId || config.deployment_id!==value.deploymentId || config.slot!==slot
          || new URL(config.gateway_url).href!==endpoint.href)throw new RemoteProtocolError('INVALID_DEPLOYMENT',400);
        return config;
      });
      const result=await store.transaction(async client=>{
        await client.query('SELECT pg_advisory_xact_lock(hashtext($1))',[`remote-deploy:${value.nodeId}`]);
        let node=(await client.query('SELECT * FROM remote_ingestion.nodes WHERE node_id=$1 FOR NO KEY UPDATE',[value.nodeId])).rows[0];
        const old=(await client.query('SELECT * FROM remote_ingestion.node_deployments WHERE node_id=$1 FOR UPDATE',[value.nodeId])).rows[0];
        if(node && (node.capabilities.length!==1||node.capabilities[0]!==workload.capability))fail('REMOTE_DEPLOYMENT_NODE_CONFLICT');
        if(node && (!old || node.state!=='active'))fail('REMOTE_DEPLOYMENT_NODE_CONFLICT');
        if(old && (old.deployment_id!==value.deploymentId || old.image!==value.image))fail('REMOTE_DEPLOYMENT_REQUIRES_DRAIN');
        if(old){
          const removed=(await client.query('SELECT 1 FROM remote_ingestion.worker_connections WHERE node_id=$1 AND retirement_id IS NOT NULL AND slot=ANY($2::text[]) LIMIT 1',[value.nodeId,configs.map(c=>c.slot)])).rowCount;
          if(removed)fail('WORKER_RETIREMENT_CONFLICT');
        }
        if(old?.worker_count>configs.length){
          // A failed expansion may leave unused registrations. Keep them reserved;
          // never remove or reuse a slot with a live connection or unsettled work.
          const excluded=(await client.query(`SELECT w.slot FROM remote_ingestion.worker_connections w
            WHERE w.node_id=$1 AND w.retired_at IS NULL AND NOT (w.slot=ANY($2::text[])) AND
            (w.activation_requested OR w.enabled OR w.connected_until>clock_timestamp() OR
             EXISTS(SELECT 1 FROM remote_ingestion.tasks t WHERE t.target_node_id=w.node_id
               AND t.target_worker_slot=w.slot AND t.state IN ('pending','leased','received')))`,[value.nodeId,configs.map(c=>c.slot)])).rows;
          if(excluded.length)fail('REMOTE_DEPLOYMENT_REQUIRES_DRAIN');
        }
        const absent=(await client.query(`SELECT 1 FROM remote_ingestion.worker_connections WHERE node_id=$1 AND retired_at IS NULL AND NOT (slot=ANY($2::text[])) LIMIT 1`,[value.nodeId,configs.map(c=>c.slot)])).rowCount;
        if(absent && (old?.worker_count??0)<=configs.length)fail('REMOTE_DEPLOYMENT_REQUIRES_DRAIN');
        const registeredCount=Math.max(old?.worker_count??0,configs.length);
        const credentials=old?routes.decrypt(old.credentials_cipher,`node-deployment:${value.nodeId}`):{nodeToken:randomBytes(32).toString('hex'),relayTokens:{}};
        if(!node)await client.query(`INSERT INTO remote_ingestion.nodes(node_id,token_hash,capabilities,max_leases,slot_claims_required)
          VALUES($1,$2,$3,$4,true)`,[value.nodeId,hash(credentials.nodeToken),[workload.capability],configs.length]);
        for(const config of configs){
          credentials.relayTokens[config.slot]??=randomBytes(32).toString('hex');
          const workerId=`remote-${value.nodeId}-${config.slot}`;
          await client.query(`INSERT INTO remote_ingestion.network_slots(node_id,slot,rota_worker_id) VALUES($1,$2,$3) ON CONFLICT(node_id,slot) DO NOTHING`,[value.nodeId,config.slot,workerId]);
          const slot=(await client.query('SELECT rota_worker_id FROM remote_ingestion.network_slots WHERE node_id=$1 AND slot=$2',[value.nodeId,config.slot])).rows[0];
          if(slot.rota_worker_id!==workerId)fail('NETWORK_SLOT_CONFLICT');
          await client.query(`INSERT INTO remote_ingestion.worker_connections(node_id,slot,deployment_id,config_hash,role,mode,activation_requested)
            VALUES($1,$2,$3,$4,$5,$6,false) ON CONFLICT(node_id,slot) DO NOTHING`,[value.nodeId,config.slot,value.deploymentId,config.config_hash,workload.role,workload.mode]);
          // The deployment lock serializes immutable registration changes.
          // Reading an existing slot must not wait for its live heartbeat.
          const row=(await client.query('SELECT * FROM remote_ingestion.worker_connections WHERE node_id=$1 AND slot=$2',[value.nodeId,config.slot])).rows[0];
          if(row.deployment_id!==value.deploymentId || row.config_hash!==config.config_hash || row.mode!==workload.mode||row.role!==workload.role)fail('WORKER_DEPLOYMENT_CONFLICT');
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
      if(byCount && (value.allowedCount<0 || value.allowedCount>value.workerCount))throw new RemoteProtocolError('INVALID_EXECUTION_COUNT',400);
      if(value.enabled===true)await capacity?.ensure();
      let control;
      await store.transaction(async client=>{
        await client.query('SELECT pg_advisory_xact_lock(hashtext($1))',[`remote-deploy:${value.nodeId}`]);
        const node=(await client.query('SELECT state FROM remote_ingestion.nodes WHERE node_id=$1 FOR SHARE',[value.nodeId])).rows[0];
        const deployment=(await client.query('SELECT * FROM remote_ingestion.node_deployments WHERE node_id=$1 FOR UPDATE',[value.nodeId])).rows[0];
        if(!node || !deployment || deployment.deployment_id!==value.deploymentId || value.workerCount>deployment.worker_count)fail('WORKER_DEPLOYMENT_MISMATCH');
        const rows=(await client.query(`SELECT *,connected_until>clock_timestamp() AS alive
          FROM remote_ingestion.worker_connections WHERE node_id=$1 AND deployment_id=$2 AND retired_at IS NULL ORDER BY slot`,[value.nodeId,value.deploymentId])).rows;
        if(rows.length!==deployment.worker_count || !collectingWorkload(rows[0]?.mode) || rows.some(r=>r.mode!==rows[0].mode))fail('WORKER_DEPLOYMENT_MISMATCH');
        const previous=(await client.query('SELECT * FROM remote_ingestion.node_intake_requests WHERE node_id=$1 FOR UPDATE',[value.nodeId])).rows[0];
        if(previous && previous.deployment_id!==value.deploymentId)fail('WORKER_DEPLOYMENT_MISMATCH');
        if(previous){const selected=new Set(previous.selected_slots);for(const row of rows)row.activation_requested=selected.has(row.slot);}
        const currentCount=rows.filter(r=>r.activation_requested).length;
        control=changeIntakeControl(await readIntakeControl(client,value.nodeId,currentCount,value.workerCount),value);
        const desired=control.effectiveCount;
        if(desired>0 && !executionFor(rows[0])?.allowsNode(value.nodeId))fail('REMOTE_CENTER_EXECUTION_NOT_CONFIGURED');
        // Registration precedes installation. A failed additive deployment must
        // not remove control of the previously verified prefix of Worker slots.
        const installed=new Set(rows.filter(r=>!r.retirement_id).sort((a,b)=>a.slot.localeCompare(b.slot,'en',{numeric:true})).slice(0,value.workerCount).map(r=>r.slot));
        const eligible=rows.filter(row=>installed.has(row.slot));
        if(rows.some(r=>r.retirement_id))fail('WORKER_RETIREMENT_CONFLICT');
        if(eligible.length!==value.workerCount)fail('WORKER_DEPLOYMENT_MISMATCH');
        const selected=selectIntakeWorkers(eligible,desired);
        if(desired>0 && (node.state!=='active' || selected.some(r=>!r.activation_requested && (!r.alive || !r.accepting))))fail('WORKER_NOT_READY');
        // Commit the user's request independently of live Worker writes. The
        // owner retains enabled until its current channel has drained.
        await client.query(`INSERT INTO remote_ingestion.node_intake_requests(node_id,deployment_id,selected_slots)
          VALUES($1,$2,$3) ON CONFLICT(node_id) DO UPDATE SET selected_slots=EXCLUDED.selected_slots,
          revision=remote_ingestion.node_intake_requests.revision+1,updated_at=clock_timestamp()`,
          [value.nodeId,value.deploymentId,selected.map(r=>r.slot)]);
        await saveIntakeControl(client,value.nodeId,control);
        await client.query("SELECT pg_notify('qy_remote_transport','supervisor')");
      });
      // Best effort only: the durable request is retried by the supervisor.
      await reconcileIntakeRequests(store,value.nodeId).catch(()=>{});
      try{return {...await this.status(value),saved:true};}
      catch{return {nodeId:value.nodeId,deploymentId:value.deploymentId,saved:true,
        allowedCount:control.effectiveCount,configuredCount:control.configuredCount,intakeEnabled:control.intakeEnabled,requested:control.effectiveCount>0,adjusting:true,observationPending:true};}
    },
    async status({nodeId,deploymentId}){
      if(nodeId==='local-center'){if(!localIntake)fail('LOCAL_INTAKE_NOT_CONFIGURED');return localIntake.status();}
      uuid(nodeId);uuid(deploymentId);
      const result=await store.transaction(async client=>{
        const request=(await client.query('SELECT selected_slots FROM remote_ingestion.node_intake_requests WHERE node_id=$1 AND deployment_id=$2',[nodeId,deploymentId])).rows[0];
        const desired=request?new Set(request.selected_slots):null;
        const rows=(await client.query(`SELECT w.*,n.state AS node_state,w.connected_until>clock_timestamp() AS connected,
          current_work.state AS task_state,current_work.command_state,
          EXISTS(SELECT 1 FROM remote_ingestion.tasks t WHERE t.target_node_id=w.node_id AND t.target_worker_slot=w.slot
            AND t.state IN ('pending','leased','received')) AS unsettled
          FROM remote_ingestion.worker_connections w JOIN remote_ingestion.nodes n USING(node_id)
          LEFT JOIN LATERAL (
            SELECT t.state,(SELECT c.state FROM remote_ingestion.channel_commands c
              WHERE c.task_id=t.task_id AND c.generation=t.generation
              ORDER BY c.created_at DESC LIMIT 1) AS command_state
            FROM remote_ingestion.tasks t WHERE t.target_node_id=w.node_id AND t.target_worker_slot=w.slot
              AND t.state IN ('pending','leased','received')
            ORDER BY CASE WHEN t.state='received' THEN 1 ELSE 0 END,t.created_at DESC LIMIT 1
          ) current_work ON true
          WHERE w.node_id=$1 AND deployment_id=$2 AND w.retired_at IS NULL ORDER BY slot`,[nodeId,deploymentId])).rows;
        const workers=[];
        for(const row of rows){
          const execution=executionFor(row),activation=activationFor(row);
          const verify=activation?.verifier?.(row.mode)??activation?.verifyExecution;
          if(row.mode===FULL_CRAWL_WORKLOAD.mode){
            row.unsettled=await fullCrawlSlotUnsettled(client,row);
            const current=(await client.query(`SELECT t.state,s.state AS stage_state FROM remote_ingestion.tasks t
              LEFT JOIN LATERAL (SELECT CASE WHEN s.applied_at IS NOT NULL OR EXISTS(SELECT 1 FROM remote_ingestion.full_crawl_result_batches b WHERE b.stage_id=s.stage_id AND b.state IN ('received','applied')) THEN 'received' ELSE 'pending' END AS state FROM remote_ingestion.full_crawl_stages s WHERE task_id=t.task_id AND generation=t.generation ORDER BY created_at DESC LIMIT 1) s ON true
              WHERE t.target_node_id=$1 AND t.target_worker_slot=$2 AND t.state IN ('pending','leased') ORDER BY t.created_at DESC LIMIT 1`,[row.node_id,row.slot])).rows[0];
            row.task_state=current?.state;row.command_state=current?.stage_state==='received'?'received':'pending';
          }
          workers.push({slot:row.slot,retiring:!!row.retirement_id,connected:row.connected===true,preparation:execution?.preparationState?.(row)??null,
          executionPhase:row.task_state==='leased' && row.command_state==='pending' ? 'collecting'
            : row.task_state==='leased' && row.command_state==='received' ? 'processing' : 'preparing',
          requested:desired?desired.has(row.slot):row.activation_requested,enabled:row.enabled,active:execution?.isProcessing(row)===true || row.unsettled===true,
          processing:execution?.isProcessing(row)===true,awaitingRecovery:row.unsettled===true && execution?.isProcessing(row)!==true,
          readyForTasks:row.connected===true && (desired?desired.has(row.slot):row.activation_requested) && row.activation_requested && row.enabled && row.accepting && row.node_state==='active'
            && typeof verify==='function' && await verify(client,row)===true});
        }
        return {nodeId,deploymentId,workers,executionAvailable:executionFor(rows[0]??{})?.allowsNode(nodeId)===true,
          adjusting:!!desired && rows.some(row=>row.activation_requested!==desired.has(row.slot)),
          ...intakeStatus(workers),...await readIntakeControl(client,nodeId,workers.filter(w=>w.requested).length,workers.length)};
      });
      // Network inspection must not hold a business database transaction open.
      if(result.workers.some(w=>w.preparation==='waiting_network')){
        const owner=result.workers[0]?.slot.startsWith('full-crawl-')?fullCrawl?.execution:execution;
        result.networkCapacity=await owner?.networkCapacity?.()??null;
      }
      return result;
    },
  };
}
