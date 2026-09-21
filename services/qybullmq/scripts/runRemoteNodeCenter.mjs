// Explicit opt-in gateway and incremental execution entry. Never applies schema
// or generates Clock plans. Queue consumers require a reviewed node allowlist.
import pg from 'pg';
import {assertFullCrawlReleaseSchema} from '../src/remoteNodes/fullCrawlReleaseSchema.js';
import {createFullCrawlReleaseRuntime} from '../src/remoteNodes/fullCrawlReleaseRuntime.js';
import {createFullCrawlDeploymentRuntime} from '../src/remoteNodes/fullCrawlDeploymentRuntime.js';
import {startRemoteNatsCenter} from '../src/remoteNodes/natsCenter.js';
import {createTransportSignals} from '../src/remoteNodes/transportSignals.js';
import {createNatsProvisioning} from '../src/remoteNodes/natsProvisioning.js';
import {once} from 'node:events';
import {RemoteCenterExecutionSupervisor} from '../src/remoteNodes/centerExecutionSupervisor.js';
import {ProxyControlClient} from '../src/proxyControlClient.js';
import {resolveWorkerIdentityPolicy} from '../src/identityPolicyCatalog.js';
import {createVideoDetailApiFallback} from '../src/videoDetailApiFallback.js';
import {createYoutubeApiSettingsLoader} from '../src/youtubeApiSettings.js';
import {PUBLICATION_WRITER_VERSION} from '../src/publicationWriterVersion.js';
import {readNodeFile} from '../src/remoteNodes/workerConfig.js';
import {RemoteNodeStore} from '../src/remoteNodes/store.js';
import {RemoteChannelPlanStore} from '../src/remoteNodes/channelPlanStore.js';
import {WholeChannelStore} from '../src/remoteNodes/wholeChannelStore.js';
import {RemoteChannelRouteStore} from '../src/remoteNodes/channelRouteStore.js';
import {RemoteYoutubeSessionStore} from '../src/remoteNodes/youtubeSessionStore.js';
import {RemoteWorkerActivationStore} from '../src/remoteNodes/workerActivationStore.js';
import {assertRemoteIncrementalBusinessFence} from '../src/remoteNodes/incrementalBusinessFence.js';
import {createRotaRemoteRouteReader} from '../src/remoteNodes/rotaRouteSource.js';
import {createRemoteDeploymentAdmin} from '../src/remoteNodes/deploymentAdmin.js';
import {createDeploymentCapacity} from '../src/remoteNodes/deploymentCapacity.js';
import {createLocalIntakeAdmin} from '../src/localIncrementalIntake.js';
import {createRemoteNodeGateway} from '../src/remoteNodes/gateway.js';

const required=name=>{const value=process.env[name];if(!value)throw new Error('REMOTE_CENTER_CONFIG_REQUIRED');return value;};
const secret=async name=>(await readNodeFile(required(name),{secret:true,maxBytes:16384})).toString().trim();
let fullCrawl;let fullGuardPool;let pool;let server;let guardPool;let supervisor;let capacity;let nats;let signals;let provisioning;let heartbeatPool;let resultPool;let natsMetrics;let wholeRetentionTimer;let retentionWork=null;let stage='credential_files';
try{
  if(process.env.REMOTE_NODE_FULL_CRAWL_EXECUTION_ENABLED==='true' && process.env.REMOTE_NODE_FULL_CRAWL_DEPLOYMENT_ENABLED!=='true')throw new Error('FULL_CRAWL_DEPLOYMENT_REQUIRED');
  const privateKey=await secret('REMOTE_NODE_ROUTE_PRIVATE_KEY_FILE');
  const encryptionKey=await secret('REMOTE_NODE_ENCRYPTION_KEY_FILE');
  if(!/^[a-f0-9]{64}$/.test(encryptionKey))throw new Error('REMOTE_CENTER_KEY_INVALID');
  const adminToken=await secret('REMOTE_NODE_ADMIN_TOKEN_FILE');
  const rotaToken=await secret('REMOTE_NODE_ROTA_TOKEN_FILE');
  // Transaction pooling shares PostgreSQL backends across gateway/result traffic.
  // Session locks and LISTEN below must keep their dedicated direct connection.
  const transactionDatabaseUrl=process.env.REMOTE_NODE_TRANSACTION_DATABASE_URL||required('REMOTE_NODE_DATABASE_URL');
  stage='schema';
  pool=new pg.Pool({connectionString:transactionDatabaseUrl,max:12,connectionTimeoutMillis:5000,
    application_name:'remote-node-center-gateway',options:`-c timezone=UTC -c publication.writer_version=${PUBLICATION_WRITER_VERSION}`});
  for(const table of ['node_deployments','node_intake_requests','worker_connections','channel_commands','network_bindings','youtube_sessions']){
    const row=(await pool.query('SELECT to_regclass($1) AS relation',[`remote_ingestion.${table}`])).rows[0];
    if(!row.relation)throw new Error('REMOTE_CENTER_SCHEMA_NOT_READY');
  }
  await pool.query('SELECT mode,enabled,activation_requested FROM remote_ingestion.worker_connections LIMIT 0');
  stage='route_configuration';
  const store=new RemoteNodeStore({pool});const channelPlans=new RemoteChannelPlanStore({store});
  let wholeChannels=null;
  if(process.env.REMOTE_NODE_WHOLE_CHANNEL==='true'){
    stage='whole_channel_schema';
    if(!process.env.REMOTE_NODE_NATS_URL)throw new Error('WHOLE_CHANNEL_NATS_REQUIRED');
    for(const table of ['whole_channel_inputs','whole_channel_chunks']){
      if(!(await pool.query('SELECT to_regclass($1) AS relation',[`remote_ingestion.${table}`])).rows[0].relation)throw new Error('WHOLE_CHANNEL_SCHEMA_NOT_READY');
    }
    await pool.query('SELECT input_json,pruned_at FROM remote_ingestion.whole_channel_inputs LIMIT 0');
    wholeChannels=new WholeChannelStore({channelPlans,assertBusinessFence:assertRemoteIncrementalBusinessFence});
  }
  const routes=new RemoteChannelRouteStore({channelStore:channelPlans,privateKey,secretKey:Buffer.from(encryptionKey,'hex'),
    assertBusinessFence:assertRemoteIncrementalBusinessFence,readRotaRoute:createRotaRemoteRouteReader({
      url:required('REMOTE_NODE_ROTA_ROUTE_URL'),token:rotaToken,allowLoopbackHttp:true})});
  fullCrawl=null;
  if(process.env.REMOTE_NODE_FULL_CRAWL_DEPLOYMENT_ENABLED==='true'){
    stage='full_crawl_deployment_schema';
    if(!process.env.REMOTE_NODE_NATS_URL)throw new Error('FULL_CRAWL_NATS_REQUIRED');
    for(const table of ['full_crawl_executions','full_crawl_stages','full_crawl_result_batches','full_crawl_result_parts','full_crawl_detail_reservations']){
      if(!(await pool.query('SELECT to_regclass($1) AS relation',[`remote_ingestion.${table}`])).rows[0].relation)throw new Error('FULL_CRAWL_SCHEMA_REQUIRED');
    }
    const transportOptions={privateKey,secretKey:Buffer.from(encryptionKey,'hex'),isReady:()=>nats?.isReady()===true,
      readRotaRoute:createRotaRemoteRouteReader({url:required('REMOTE_NODE_ROTA_ROUTE_URL'),token:rotaToken,allowLoopbackHttp:true})};
    if(process.env.REMOTE_NODE_FULL_CRAWL_EXECUTION_ENABLED==='true'){
      stage='full_crawl_execution_configuration';
      await assertFullCrawlReleaseSchema(pool.query.bind(pool));
      const profileSecret=await secret('REMOTE_NODE_PROFILE_SECRET_FILE');
      const controlToken=await secret('REMOTE_NODE_ROTA_CONTROL_TOKEN_FILE');
      if(controlToken.length<12 || controlToken===rotaToken)throw new Error('REMOTE_CENTER_CONTROL_CREDENTIAL_INVALID');
      const resolvedPolicy=resolveWorkerIdentityPolicy({role:'channel',policyId:required('ROTA_IDENTITY_POLICY_ID'),expectedWorkloadScope:required('ROTA_WORKLOAD_SCOPE_EXPECTED')});
      fullGuardPool=new pg.Pool({connectionString:required('REMOTE_NODE_DATABASE_URL'),max:5,connectionTimeoutMillis:5000,
        application_name:'remote-full-crawl-supervisor-locks',options:`-c timezone=UTC -c publication.writer_version=${PUBLICATION_WRITER_VERSION}`});
      fullCrawl=await createFullCrawlReleaseRuntime({store,guardPool:fullGuardPool,controlToken,profileSecret,resolvedPolicy,
        rotaClient:new ProxyControlClient({controlUrl:required('ROTA_PROXY_CONTROL_URL'),token:controlToken}),
        transportOptions,report:value=>console.log(JSON.stringify(value))});
    }else{
      fullCrawl=createFullCrawlDeploymentRuntime({store,image:required('REMOTE_NODE_FULL_CRAWL_IMAGE'),...transportOptions});
    }
  }
  const workerConnections=new RemoteWorkerActivationStore({store,verifyExecution:(client,row)=>supervisor?.verifyExecution(client,row)??false});
  const youtubeSessions=new RemoteYoutubeSessionStore({routes});
  if(process.env.REMOTE_NODE_AUTO_CAPACITY==='true') {
    stage='capacity_configuration';
    const controlToken=await secret('REMOTE_NODE_ROTA_CONTROL_TOKEN_FILE');
    if(controlToken.length<12 || controlToken===rotaToken)throw new Error('REMOTE_CENTER_CONTROL_CREDENTIAL_INVALID');
    capacity=createDeploymentCapacity({pool,localChannelSlots:Number(required('REMOTE_NODE_LOCAL_CHANNEL_SLOTS')),
      client:new ProxyControlClient({controlUrl:required('ROTA_PROXY_CONTROL_URL'),token:controlToken,timeoutMs:12000,maxAttempts:1})});
  }
  if(process.env.REMOTE_NODE_EXECUTION_ENABLED==='true'){
    stage='execution_configuration';
    if(process.env.YOUTUBEJS_EXTRACTOR_MODE!=='full')throw new Error('REMOTE_CENTER_EXECUTION_CONFIG_INVALID');
    const profileSecret=await secret('REMOTE_NODE_PROFILE_SECRET_FILE');
    const controlToken=await secret('REMOTE_NODE_ROTA_CONTROL_TOKEN_FILE');
    if(controlToken.length<12 || controlToken===rotaToken)throw new Error('REMOTE_CENTER_CONTROL_CREDENTIAL_INVALID');
    const redisUrl=new URL(required('REMOTE_NODE_REDIS_URL'));
    if(!['redis:','rediss:'].includes(redisUrl.protocol) || !['','/','/0'].includes(redisUrl.pathname)
      || redisUrl.search || redisUrl.hash)throw new Error('REMOTE_CENTER_REDIS_CONFIG_INVALID');
    const dashboardManaged=process.env.REMOTE_NODE_EXECUTION_ADMISSION==='dashboard';
    const allowedNodeIds=dashboardManaged?[]:required('REMOTE_NODE_EXECUTION_NODE_IDS').split(',').map(value=>value.trim());
    stage='identity_policy';
    const resolvedPolicy=resolveWorkerIdentityPolicy({role:'channel',policyId:required('ROTA_IDENTITY_POLICY_ID'),expectedWorkloadScope:required('ROTA_WORKLOAD_SCOPE_EXPECTED')});
    stage='api_settings';
    const getYoutubeApiSettings=createYoutubeApiSettingsLoader({query:pool.query.bind(pool)});
    const createApiFallback=process.env.YOUTUBEJS_VIDEO_API_BATCH_FALLBACK==='true'
      ?args=>createVideoDetailApiFallback({...args,loadSettings:getYoutubeApiSettings}):null;
    const loadWholeApiPolicy=async()=>{
      const settings=await getYoutubeApiSettings();
      return {enabled:Boolean(createApiFallback)&&settings.fallbackMode!=='disabled',
        available:Boolean(settings.apiKeys?.length),dailyRequestLimit:settings.dailyRequestLimit};
    };
    guardPool=new pg.Pool({connectionString:required('REMOTE_NODE_DATABASE_URL'),max:1,connectionTimeoutMillis:5000,
      application_name:'remote-node-supervisor-locks',options:`-c timezone=UTC -c publication.writer_version=${PUBLICATION_WRITER_VERSION}`});
    stage='supervisor_configuration';
    supervisor=new RemoteCenterExecutionSupervisor({store,channelStore:channelPlans,routes,youtubeSessions,activation:workerConnections,guardPool,
      connection:{host:redisUrl.hostname,port:Number(redisUrl.port||6379),username:redisUrl.username?decodeURIComponent(redisUrl.username):undefined,
        password:redisUrl.password?decodeURIComponent(redisUrl.password):undefined,tls:redisUrl.protocol==='rediss:'?{}:undefined,maxRetriesPerRequest:null},
      prefix:required('REMOTE_NODE_QUEUE_PREFIX'),allowedNodeIds,dashboardManaged,resolvedPolicy,profileSecret,createApiFallback,wholeChannels,loadWholeApiPolicy,
      rotaClient:new ProxyControlClient({controlUrl:required('ROTA_PROXY_CONTROL_URL'),token:controlToken}),
      proxyBaseUrl:'http://unused-center.invalid:8000',proxyPassword:'remote-transport-only',report:value=>console.log(JSON.stringify(value))});
  }
  if(process.env.REMOTE_NODE_NATS_URL){
    stage='nats_schema';
    await pool.query('SELECT retirement_id,retired_at FROM remote_ingestion.worker_connections LIMIT 0');
    const ready=(await pool.query("SELECT to_regclass('remote_ingestion.transport_receipts') AS relation")).rows[0];
    if(!ready.relation)throw new Error('REMOTE_NATS_SCHEMA_REQUIRED');
    const password=await secret('REMOTE_NODE_NATS_PASSWORD_FILE');
    signals=await createTransportSignals({connectionString:required('REMOTE_NODE_DATABASE_URL')});
    provisioning=createNatsProvisioning({pool,routes,file:required('REMOTE_NODE_NATS_AUTH_FILE'),centerPassword:password,localIntakePassword:process.env.LOCAL_INCREMENTAL_NATS_PASSWORD_FILE?await secret('LOCAL_INCREMENTAL_NATS_PASSWORD_FILE'):undefined,signals});
    await provisioning.sync();
    channelPlans.transportSignals=signals;
    const transportPool=(name,max)=>new pg.Pool({connectionString:transactionDatabaseUrl,max,connectionTimeoutMillis:5000,
      application_name:name,options:`-c timezone=UTC -c publication.writer_version=${PUBLICATION_WRITER_VERSION}`});
    heartbeatPool=transportPool('remote-node-nats-heartbeats',4);resultPool=transportPool('remote-node-nats-results',8);
    const heartbeatStore=new RemoteNodeStore({pool:heartbeatPool}),resultStore=new RemoteNodeStore({pool:resultPool});
    const resultChannelPlans=new RemoteChannelPlanStore({store:resultStore});
    stage='nats_connection';
    nats=await startRemoteNatsCenter({url:required('REMOTE_NODE_NATS_URL'),password,store,channelPlans,wholeChannels,routes,youtubeSessions,workerConnections,signals,fullCrawls:fullCrawl?.transport.service,
      heartbeatStore,heartbeatConnections:new RemoteWorkerActivationStore({store:heartbeatStore,verifyExecution:workerConnections.verifyExecution}),
      resultStore,resultChannelPlans,resultWholeChannels:wholeChannels?new WholeChannelStore({channelPlans:resultChannelPlans,assertBusinessFence:assertRemoteIncrementalBusinessFence}):null,
      resultConcurrency:Number(process.env.REMOTE_NODE_NATS_RESULT_CONCURRENCY||8),
      replicas:Number(process.env.REMOTE_NODE_NATS_REPLICAS||1),resultMaxBytes:Number(process.env.REMOTE_NODE_NATS_MAX_BYTES||1073741824),
      report:value=>console.log(JSON.stringify(value))});
    provisioning.start(value=>console.log(JSON.stringify(value)));
    let measuring=false;
    natsMetrics=setInterval(async()=>{if(measuring)return;measuring=true;try{console.log(JSON.stringify({event:'remote_nats_status',...await nats.stats()}));}
      catch{console.log(JSON.stringify({event:'remote_nats_status_unavailable'}));}finally{measuring=false;}},30000);
  }
  stage='gateway';
  const bind=process.env.REMOTE_NODE_CENTER_BIND||'127.0.0.1';
  if(!['127.0.0.1','0.0.0.0'].includes(bind))throw new Error('REMOTE_CENTER_BIND_INVALID');
  const localIntake=process.env.LOCAL_INCREMENTAL_INTAKE_CONTROL==='true'
    ?createLocalIntakeAdmin({query:pool.query.bind(pool),transaction:action=>store.transaction(action)}):null;
  const deploymentAdmin=createRemoteDeploymentAdmin({store,routes,token:adminToken,image:required('REMOTE_NODE_COLLECT_IMAGE'),gatewayUrl:required('REMOTE_NODE_GATEWAY_URL'),activation:workerConnections,execution:supervisor,capacity,localIntake,natsProvisioning:provisioning,fullCrawl});
  server=createRemoteNodeGateway({store,channelPlans,routes,youtubeSessions,workerConnections,deploymentAdmin,transportHealth:()=>nats?nats.stats():{transport:'http'},
    maxConcurrentRequests:Number(process.env.REMOTE_NODE_GATEWAY_CONCURRENCY || 64)});
  server.listen(Number(process.env.REMOTE_NODE_CENTER_PORT||3187),bind);await once(server,'listening');
  if(wholeChannels)wholeRetentionTimer=setInterval(()=>{
    if(retentionWork)return;
    retentionWork=wholeChannels.pruneApplied({retentionDays:7,limit:64})
      .then(count=>{if(count)console.log(JSON.stringify({event:'whole_channel_payloads_pruned',count}));})
      .catch(()=>console.log(JSON.stringify({event:'whole_channel_retention_failed'})))
      .finally(()=>{retentionWork=null;});
  },60000);
  let closing=false;
  const stop=async()=>{if(closing)return;closing=true;await supervisor?.stop();await fullCrawl?.close?.();
    await new Promise(resolve=>{server.close(resolve);server.closeIdleConnections();});
    clearInterval(natsMetrics);await provisioning?.close();await signals?.close();await nats?.close();
    clearInterval(wholeRetentionTimer);await retentionWork;
    await heartbeatPool?.end();await resultPool?.end();await guardPool?.end();await fullGuardPool?.end();await pool.end();};
  process.once('SIGTERM',()=>void stop().catch(()=>{process.exitCode=1;}));process.once('SIGINT',()=>void stop().catch(()=>{process.exitCode=1;}));
  stage='full_crawl_compatibility_start';
  await fullCrawl?.start?.();
  supervisor?.start();
  // Advertise readiness only once graceful signal handlers are installed.
  console.log(JSON.stringify({event:'remote_node_center_listening',full_crawl_execution:process.env.REMOTE_NODE_FULL_CRAWL_EXECUTION_ENABLED==='true',activation:supervisor?(supervisor.dashboardManaged?'dashboard_authorized':'explicit_node_allowlist'):'disabled'}));
}catch{
  clearInterval(wholeRetentionTimer);await retentionWork;
  clearInterval(natsMetrics);await supervisor?.stop().catch(()=>{});await fullCrawl?.close?.().catch(()=>{});
  if(server)await new Promise(resolve=>{server.close(resolve);server.closeIdleConnections();});
  await provisioning?.close();await signals?.close().catch(()=>{});await nats?.close().catch(()=>{});
  await heartbeatPool?.end();await resultPool?.end();await guardPool?.end();await fullGuardPool?.end();await pool?.end();console.error(JSON.stringify({event:'remote_node_center_start_failed',stage}));process.exitCode=1;
}
