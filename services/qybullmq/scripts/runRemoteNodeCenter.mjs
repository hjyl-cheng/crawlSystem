// Explicit opt-in gateway and incremental execution entry. Never applies schema
// or generates Clock plans. Queue consumers require a reviewed node allowlist.
import pg from 'pg';
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
import {RemoteChannelRouteStore} from '../src/remoteNodes/channelRouteStore.js';
import {RemoteYoutubeSessionStore} from '../src/remoteNodes/youtubeSessionStore.js';
import {RemoteWorkerActivationStore} from '../src/remoteNodes/workerActivationStore.js';
import {assertRemoteIncrementalBusinessFence} from '../src/remoteNodes/incrementalBusinessFence.js';
import {createRotaRemoteRouteReader} from '../src/remoteNodes/rotaRouteSource.js';
import {createRemoteDeploymentAdmin} from '../src/remoteNodes/deploymentAdmin.js';
import {createRemoteNodeGateway} from '../src/remoteNodes/gateway.js';

const required=name=>{const value=process.env[name];if(!value)throw new Error('REMOTE_CENTER_CONFIG_REQUIRED');return value;};
const secret=async name=>(await readNodeFile(required(name),{secret:true,maxBytes:16384})).toString().trim();
let pool;let server;let guardPool;let supervisor;let stage='credential_files';
try{
  const privateKey=await secret('REMOTE_NODE_ROUTE_PRIVATE_KEY_FILE');
  const encryptionKey=await secret('REMOTE_NODE_ENCRYPTION_KEY_FILE');
  if(!/^[a-f0-9]{64}$/.test(encryptionKey))throw new Error('REMOTE_CENTER_KEY_INVALID');
  const adminToken=await secret('REMOTE_NODE_ADMIN_TOKEN_FILE');
  const rotaToken=await secret('REMOTE_NODE_ROTA_TOKEN_FILE');
  stage='schema';
  pool=new pg.Pool({connectionString:required('REMOTE_NODE_DATABASE_URL'),max:12,connectionTimeoutMillis:5000,
    application_name:'remote-node-center-gateway',options:`-c timezone=UTC -c publication.writer_version=${PUBLICATION_WRITER_VERSION}`});
  for(const table of ['node_deployments','worker_connections','channel_commands','network_bindings','youtube_sessions']){
    const row=(await pool.query('SELECT to_regclass($1) AS relation',[`remote_ingestion.${table}`])).rows[0];
    if(!row.relation)throw new Error('REMOTE_CENTER_SCHEMA_NOT_READY');
  }
  await pool.query('SELECT mode,enabled,activation_requested FROM remote_ingestion.worker_connections LIMIT 0');
  stage='route_configuration';
  const store=new RemoteNodeStore({pool});const channelPlans=new RemoteChannelPlanStore({store});
  const routes=new RemoteChannelRouteStore({channelStore:channelPlans,privateKey,secretKey:Buffer.from(encryptionKey,'hex'),
    assertBusinessFence:assertRemoteIncrementalBusinessFence,readRotaRoute:createRotaRemoteRouteReader({
      url:required('REMOTE_NODE_ROTA_ROUTE_URL'),token:rotaToken,allowLoopbackHttp:true})});
  const workerConnections=new RemoteWorkerActivationStore({store,verifyExecution:(client,row)=>supervisor?.verifyExecution(client,row)??false});
  const youtubeSessions=new RemoteYoutubeSessionStore({routes});
  if(process.env.REMOTE_NODE_EXECUTION_ENABLED==='true'){
    stage='execution_configuration';
    if(process.env.YOUTUBEJS_EXTRACTOR_MODE!=='full')throw new Error('REMOTE_CENTER_EXECUTION_CONFIG_INVALID');
    const profileSecret=await secret('REMOTE_NODE_PROFILE_SECRET_FILE');
    const controlToken=await secret('REMOTE_NODE_ROTA_CONTROL_TOKEN_FILE');
    if(controlToken.length<12 || controlToken===rotaToken)throw new Error('REMOTE_CENTER_CONTROL_CREDENTIAL_INVALID');
    const redisUrl=new URL(required('REMOTE_NODE_REDIS_URL'));
    if(!['redis:','rediss:'].includes(redisUrl.protocol) || !['','/','/0'].includes(redisUrl.pathname)
      || redisUrl.search || redisUrl.hash)throw new Error('REMOTE_CENTER_REDIS_CONFIG_INVALID');
    const allowedNodeIds=required('REMOTE_NODE_EXECUTION_NODE_IDS').split(',').map(value=>value.trim());
    stage='identity_policy';
    const resolvedPolicy=resolveWorkerIdentityPolicy({role:'channel',policyId:required('ROTA_IDENTITY_POLICY_ID'),expectedWorkloadScope:required('ROTA_WORKLOAD_SCOPE_EXPECTED')});
    stage='api_settings';
    const getYoutubeApiSettings=createYoutubeApiSettingsLoader({query:pool.query.bind(pool)});
    const createApiFallback=process.env.YOUTUBEJS_VIDEO_API_BATCH_FALLBACK==='true'
      ?args=>createVideoDetailApiFallback({...args,loadSettings:getYoutubeApiSettings}):null;
    guardPool=new pg.Pool({connectionString:required('REMOTE_NODE_DATABASE_URL'),max:32,connectionTimeoutMillis:5000,
      application_name:'remote-node-supervisor-locks',options:`-c timezone=UTC -c publication.writer_version=${PUBLICATION_WRITER_VERSION}`});
    stage='supervisor_configuration';
    supervisor=new RemoteCenterExecutionSupervisor({store,channelStore:channelPlans,routes,youtubeSessions,activation:workerConnections,guardPool,
      connection:{host:redisUrl.hostname,port:Number(redisUrl.port||6379),username:redisUrl.username?decodeURIComponent(redisUrl.username):undefined,
        password:redisUrl.password?decodeURIComponent(redisUrl.password):undefined,tls:redisUrl.protocol==='rediss:'?{}:undefined,maxRetriesPerRequest:null},
      prefix:required('REMOTE_NODE_QUEUE_PREFIX'),allowedNodeIds,resolvedPolicy,profileSecret,createApiFallback,
      rotaClient:new ProxyControlClient({controlUrl:required('ROTA_PROXY_CONTROL_URL'),token:controlToken}),
      proxyBaseUrl:'http://unused-center.invalid:8000',proxyPassword:'remote-transport-only',report:value=>console.log(JSON.stringify(value))});
  }
  stage='gateway';
  const bind=process.env.REMOTE_NODE_CENTER_BIND||'127.0.0.1';
  if(!['127.0.0.1','0.0.0.0'].includes(bind))throw new Error('REMOTE_CENTER_BIND_INVALID');
  const deploymentAdmin=createRemoteDeploymentAdmin({store,routes,token:adminToken,image:required('REMOTE_NODE_COLLECT_IMAGE'),gatewayUrl:required('REMOTE_NODE_GATEWAY_URL'),activation:workerConnections});
  server=createRemoteNodeGateway({store,channelPlans,routes,youtubeSessions,workerConnections,deploymentAdmin});
  server.listen(Number(process.env.REMOTE_NODE_CENTER_PORT||3187),bind);await once(server,'listening');
  let closing=false;
  const stop=async()=>{if(closing)return;closing=true;await supervisor?.stop();
    await new Promise(resolve=>{server.close(resolve);server.closeIdleConnections();});
    await guardPool?.end();await pool.end();};
  process.once('SIGTERM',()=>void stop().catch(()=>{process.exitCode=1;}));process.once('SIGINT',()=>void stop().catch(()=>{process.exitCode=1;}));
  supervisor?.start();
  // Advertise readiness only once graceful signal handlers are installed.
  console.log(JSON.stringify({event:'remote_node_center_listening',activation:supervisor?'explicit_node_allowlist':'disabled'}));
}catch{
  await supervisor?.stop().catch(()=>{});await guardPool?.end();await pool?.end();console.error(JSON.stringify({event:'remote_node_center_start_failed',stage}));process.exitCode=1;
}
