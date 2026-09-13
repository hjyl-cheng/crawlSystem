// Disposable TLS NATS + PostgreSQL. Never reads production connection defaults.
import {execFileSync,spawn} from 'node:child_process';
import {mkdtemp,writeFile,rm,mkdir,copyFile,readFile} from 'node:fs/promises';
import {tmpdir} from 'node:os';
import {join} from 'node:path';
import {createServer} from 'node:net';
const testTimeout=Number(process.env.REMOTE_NATS_TEST_TIMEOUT_MS||180000);
if(!Number.isSafeInteger(testTimeout)||testTimeout<1000||testTimeout>900000)throw new Error('invalid isolated test timeout');
const root=await mkdtemp(join(tmpdir(),'qy-nats-integration-'));
const project=`qy-nats-integration-${process.pid}-${Date.now()}`;
const nodeId='11111111-1111-4111-8111-111111111111',token='a'.repeat(64),password='b'.repeat(64);
const port=await new Promise(resolve=>{const s=createServer();s.listen(0,'127.0.0.1',()=>{const p=s.address().port;s.close(()=>resolve(p));});});
const compose=['compose','-p',project,'-f',join(root,'compose.json')];
const docker=args=>execFileSync('docker',[...compose,...args],{encoding:'utf8',timeout:120000});
try{
 execFileSync('openssl',['req','-x509','-newkey','rsa:2048','-nodes','-keyout',join(root,'key.pem'),'-out',join(root,'cert.pem'),'-days','1','-subj','/CN=localhost','-addext','subjectAltName=DNS:localhost,IP:127.0.0.1'],{stdio:'ignore'});
 await mkdir(join(root,'auth'));await mkdir(join(root,'tls'));
 await copyFile(join(root,'cert.pem'),join(root,'tls/fullchain.pem'));await copyFile(join(root,'key.pem'),join(root,'tls/privkey.pem'));
 await copyFile(new URL('../../../deploy/nats/server.conf',import.meta.url),join(root,'server.conf'));
 await copyFile(new URL('../../../deploy/nats/start.sh',import.meta.url),join(root,'start.sh'));
 await writeFile(join(root,'auth/users.conf'),`users: ${JSON.stringify([{user:'center',password},{user:'local-intake',password:'c'.repeat(64),permissions:{publish:{deny:['>']},subscribe:['qy.local-intake.changed']}},{user:nodeId,password:token,permissions:{publish:[`qy.remote.rpc.${nodeId}.*`,`qy.remote.results.${nodeId}`],subscribe:[`_INBOX.${nodeId}.>`]}}])}\n`,{mode:0o600});
 await writeFile(join(root,'compose.json'),JSON.stringify({services:{redis:{image:'redis:7-alpine',ports:['127.0.0.1::6379'],command:['redis-server','--requirepass','remote-center-fixture-only']},broker:{image:'nats:2.12-alpine@sha256:b270f5e2428354c0335612694d7dd2fb588148e567a5757fdff325ef9c9332e6',entrypoint:['/bin/sh','/etc/nats/start.sh'],ports:[`127.0.0.1:${port}:4222`],volumes:[`${root}/server.conf:/etc/nats/server.conf:ro`,`${root}/start.sh:/etc/nats/start.sh:ro`,`${root}/auth:/run/nats-auth:ro`,`${root}/tls:/run/nats-tls:ro`,'data:/data'],mem_limit:'256m',cpus:1},postgres:{image:'postgres:16-alpine',environment:{POSTGRES_DB:'remote_node_ingestion_test',POSTGRES_USER:'test',POSTGRES_PASSWORD:'isolated-only'},ports:['127.0.0.1::5432'],mem_limit:'512m',cpus:1,healthcheck:{test:['CMD-SHELL','pg_isready -U test -d remote_node_ingestion_test'],interval:'1s',timeout:'2s',retries:30}}},volumes:{data:{}}}));
 if(process.env.REMOTE_NATS_TEST_WSS==='true'){
  await writeFile(join(root,'nginx.conf'),`events {}
http {map $http_upgrade $connection_upgrade {default upgrade; '' close;} upstream qy_remote_messages {server broker:9222;} server {listen 443 ssl;ssl_certificate /tls/fullchain.pem;ssl_certificate_key /tls/privkey.pem;${(await readFile(new URL('../../../deploy/nginx/qy.conf.template',import.meta.url),'utf8')).match(/    location = \/node-messages \{[\s\S]*?\n    \}/)[0]}}}`);
  const cfg=JSON.parse(await readFile(join(root,'compose.json'),'utf8'));cfg.services.broker.ports=[];
  cfg.services.proxy={image:'nginx:1.29-alpine',ports:[`127.0.0.1:${port}:443`],volumes:[`${root}/nginx.conf:/etc/nginx/nginx.conf:ro`,`${root}/tls:/tls:ro`]};
  await writeFile(join(root,'compose.json'),JSON.stringify(cfg));
 }
 docker(['up','-d','--wait']);
 const address=docker(['port','postgres','5432']).trim();if(!/^127\.0\.0\.1:\d+$/.test(address))throw Error('Expected loopback database');
 const redisAddress=docker(['port','redis','6379']).trim();
 const env={...process.env,DATABASE_URL:`postgresql://test:isolated-only@${address}/remote_node_ingestion_test`,REMOTE_NODE_TEST_REDIS_PORT:redisAddress.split(':')[1],LOCAL_INTAKE_TEST_DATABASE_URL:`postgresql://test:isolated-only@${address}/remote_node_ingestion_test`,LOCAL_INTAKE_TEST_REDIS_URL:`redis://:remote-center-fixture-only@${redisAddress}`,REMOTE_NATS_TEST_AUTH_FILE:join(root,'auth/users.conf'),REMOTE_NODE_TEST_DATABASE_URL:`postgresql://test:isolated-only@${address}/remote_node_ingestion_test`,REMOTE_NATS_TEST_URL:process.env.REMOTE_NATS_TEST_WSS==='true'?`wss://127.0.0.1:${port}/node-messages`:`tls://127.0.0.1:${port}`,REMOTE_NATS_TEST_CA:join(root,'cert.pem'),REMOTE_NATS_TEST_NODE_ID:nodeId,REMOTE_NATS_TEST_TOKEN:token,REMOTE_NATS_TEST_PASSWORD:password,REMOTE_NATS_TEST_LOCAL_PASSWORD:'c'.repeat(64)};
 const files=process.argv.slice(2);if(!files.length)files.push('test/remoteNats.postgres.integration.test.js','test/remoteIncrementalFence.postgres.integration.test.js','test/remoteControlNotifications.postgres.integration.test.js','test/remoteCenterEntry.integration.test.js','test/localIncrementalIntake.postgres.redis.integration.test.js');
 await new Promise((resolve,reject)=>{const child=spawn(process.execPath,['--test','--test-concurrency=1',...files],{env,stdio:'inherit',timeout:testTimeout});child.once('error',reject);child.once('exit',code=>code===0?resolve():reject(Error(`NATS integration failed: ${code}`)));});
}catch(error){console.error(docker(['logs','--tail','40','broker']));throw error;}finally{try{docker(['down','-v','--remove-orphans']);}finally{await rm(root,{recursive:true,force:true});}}
