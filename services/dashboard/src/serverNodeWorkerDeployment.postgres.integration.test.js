import test from 'node:test';
import assert from 'node:assert/strict';
import {randomUUID} from 'node:crypto';
import {once} from 'node:events';
import express from 'express';
import pg from 'pg';
import {createServerNodeStore} from './serverNodes.js';
import {createNodeWorkerDeployment} from './serverNodeWorkerDeployment.js';
import {serverNodesRoutes} from './serverNodesRoutes.js';
import {allowDashboardRequestDuringControlledMigration} from './controlledWritePolicy.js';
const url=process.env.SERVER_NODES_TEST_DATABASE_URL;
test('page deployment freezes saved count, persists progress, retries and expands without changing old workers',{skip:!url},async t=>{
 assert.equal(new URL(url).pathname,'/server_nodes_dashboard_test');
 const pool=new pg.Pool({connectionString:url});let server;t.after(async()=>{if(server)await new Promise(resolve=>{server.close(resolve);server.closeAllConnections();});await pool.end();});
 assert.equal((await pool.query('SELECT current_database() AS name')).rows[0].name,'server_nodes_dashboard_test');
 await pool.query("CREATE SCHEMA IF NOT EXISTS crawler; CREATE TABLE IF NOT EXISTS crawler.settings(setting_key TEXT PRIMARY KEY,value_json JSONB NOT NULL,updated_at TIMESTAMPTZ DEFAULT now())");
 const node={id:randomUUID(),name:'Test node',host:'192.0.2.8',port:22,username:'ubuntu',kind:'execution',sshAlias:'',notes:'',
   workers:[{role:'incremental',count:1}],provisioning:{state:'ready',systemId:'fixture',steps:{ssh:'completed',key:'completed',monitoring:'completed',metrics:'completed'}},runtime:{state:'ready'}};
 await pool.query("INSERT INTO crawler.settings(setting_key,value_json) VALUES('dashboard_server_nodes_v1',$1) ON CONFLICT(setting_key) DO UPDATE SET value_json=EXCLUDED.value_json",[{version:1,nodes:[node]}]);
 const store=createServerNodeStore(pool.query.bind(pool));const plans=[];let fail=true;let begins=0;let allowedCount=0,changeDuringDeployment=false,connected=true;const intakeCalls=[];
 const center={async prepare(plan){plans.push(plan);assert.equal((await store.load()).nodes[0].deployment.state,'running');return {nodeId:node.id,deploymentId:plan.deploymentId,readyForTasks:false,nodeToken:'a'.repeat(64),publicKey:'public fixture',relayTokens:Object.fromEntries(plan.registrations.map(r=>[r.slot,'b'.repeat(64)]))};},
   async status(plan){return {nodeId:node.id,deploymentId:plan.deploymentId,allowedCount,workers:plan.registrations.map(r=>({slot:r.slot,connected,readyForTasks:false}))};},
   async setExecution(value){
     const saved=(await store.load()).nodes[0].deployment;
     assert.equal(saved.appliedCount,value.workerCount);assert.equal(saved.steps.connection,'completed','admit only after every connection is verified');
     if(allowedCount!==value.expectedAllowedCount)throw Object.assign(new Error('changed'),{code:'EXECUTION_CONTROL_CHANGED'});
     intakeCalls.push(value);allowedCount=value.allowedCount;
   }};
 const ssh={async connect(){begins++;return {};},async verify(){},async deployWorkers(_conn,_node,plan,credentials,password,before,after){
   assert.equal(password,'fixture-password');assert.equal(credentials.nodeToken,'a'.repeat(64));assert.equal(credentials.registry.password,'c'.repeat(64));
   for(const step of ['files','start','verify']){await before(step);if(step==='start'&&fail)throw new Error('do not persist fixture-password or token');await after(step);}
   if(changeDuringDeployment)allowedCount=1;
 },close(){}};
 const image='registry.example/collect@sha256:'+'a'.repeat(64);const gatewayUrl='https://center.example';
 const deployment=createNodeWorkerDeployment({store,center,ssh,image,gatewayUrl,natsUrl:'tls://messages.example:4222',pollMs:1,waitMs:50,registryCredentials:async()=>({server:'registry.example',username:'node-pull',password:'c'.repeat(64)})});
 const app=express();app.use(express.json());app.use((req,res,next)=>allowDashboardRequestDuringControlledMigration(req.method,req.path)?next():res.sendStatus(423));
 app.use(serverNodesRoutes({store,layout:({body})=>body,workerDeployment:deployment,deploymentEnvironment:{SERVER_NODE_COLLECT_IMAGE:image,SERVER_NODE_GATEWAY_URL:gatewayUrl,SERVER_NODE_NATS_URL:'tls://messages.example:4222'}}));
 server=app.listen(0,'127.0.0.1');await once(server,'listening');const base=`http://127.0.0.1:${server.address().port}/api/server-nodes/${node.id}`;
 const post=body=>fetch(base+'/deploy-workers',{method:'POST',headers:{'content-type':'application/json'},body:JSON.stringify(body)});
 assert.equal((await post({version:1,image:'unsafe'})).status,400);assert.equal(begins,0);
 const preview=await (await fetch(base+'/worker-deployment')).json();assert.equal(preview.mode,'incremental_collect');assert.equal(preview.memoryLimitMiB,768);assert.equal(begins,0);
 assert.equal((await post({version:0,password:'fixture-password'})).status,409);assert.equal(begins,0);
 assert.equal((await post({version:1,password:'fixture-password'})).status,202);await deployment.waitForIdle();
 let registry=await store.load();assert.equal(registry.nodes[0].deployment.state,'failed');assert.equal(registry.nodes[0].deployment.steps.start,'failed');
 assert.ok(!JSON.stringify(registry).includes('fixture-password'));assert.ok(!JSON.stringify(registry).includes('c'.repeat(64)));const deploymentId=registry.nodes[0].deployment.deploymentId;
 fail=false;assert.equal((await post({version:registry.version,password:'fixture-password'})).status,202);await deployment.waitForIdle();
 registry=await store.load();assert.equal(registry.nodes[0].deployment.state,'connected');assert.equal(registry.nodes[0].deployment.deploymentId,deploymentId);
 const values=Object.fromEntries(['name','host','port','username','kind','notes','sshAlias','workers'].map(k=>[k,node[k]]));
 registry=await store.save({id:node.id,version:registry.version,node:{...values,workers:[{role:'incremental',count:2}]}});
 assert.equal((await post({version:registry.version,password:'fixture-password'})).status,202);await deployment.waitForIdle();
 registry=await store.load();assert.equal(registry.nodes[0].deployment.appliedCount,2);assert.equal(plans[0].files['incremental-1.json'],plans.at(-1).files['incremental-1.json']);
 registry=await store.save({id:node.id,version:registry.version,node:values});
 const calls=begins;assert.equal((await post({version:registry.version,password:'fixture-password'})).status,409);assert.equal(begins,calls);
 assert.equal((await store.deletionCheck(node.id)).allowed,false);
 // The manager submits the new count and deployment as one durable operation.
 assert.equal((await post({version:registry.version,count:0,password:'fixture-password'})).status,400);
 assert.equal((await post({version:registry.version,count:5,syncIntake:true,expectedAllowedCount:1,password:'fixture-password'})).status,409);
 assert.equal((await store.load()).version,registry.version,'invalid sync does not change the saved plan');
 fail=true;
 assert.equal((await post({version:registry.version,count:5,syncIntake:true,expectedAllowedCount:0,password:'fixture-password'})).status,202);
 await deployment.waitForIdle();registry=await store.load();
 assert.equal(registry.nodes[0].workers[0].count,5);assert.equal(registry.nodes[0].deployment.appliedCount,2);
 assert.equal(allowedCount,0);assert.equal(intakeCalls.length,0,'failed deployment cannot enable planned workers');
 fail=false;
 assert.equal((await post({version:registry.version,count:5,syncIntake:true,expectedAllowedCount:0,password:'fixture-password'})).status,202);
 await deployment.waitForIdle();registry=await store.load();
 assert.equal(allowedCount,5);assert.equal(registry.nodes[0].deployment.intakeSync.state,'completed');
 assert.equal(registry.nodes[0].deployment.state,'connected');
 changeDuringDeployment=true;
 assert.equal((await post({version:registry.version,count:6,syncIntake:true,expectedAllowedCount:5,password:'fixture-password'})).status,202);
 await deployment.waitForIdle();registry=await store.load();
 assert.equal(registry.nodes[0].deployment.appliedCount,6);assert.equal(registry.nodes[0].deployment.state,'connected');
 assert.equal(registry.nodes[0].deployment.intakeSync.state,'failed');assert.equal(allowedCount,1,'a newer manual intake setting wins');
 assert.equal(intakeCalls.length,1);
 assert.equal((await createServerNodeStore(pool.query.bind(pool)).load()).nodes[0].deployment.intakeSync.state,'failed','sync outcome survives a service restart');
 changeDuringDeployment=false;
 // A failed target must not force the user to keep retrying that large total.
 fail=true;
 assert.equal((await post({version:registry.version,count:20,password:'fixture-password'})).status,202);
 await deployment.waitForIdle();registry=await store.load();
 assert.equal(registry.nodes[0].deployment.appliedCount,6);
 assert.equal((await post({version:registry.version,additionalCount:2,role:'query',expectedInstalledCount:6,password:'fixture-password'})).status,400);
 assert.equal((await post({version:registry.version,additionalCount:2,role:'incremental',expectedInstalledCount:5,password:'fixture-password'})).status,409);
 assert.equal((await post({version:registry.version,additionalCount:2,count:8,role:'incremental',expectedInstalledCount:6,password:'fixture-password'})).status,400);
 fail=false;
 const addition={version:registry.version,additionalCount:2,role:'incremental',expectedInstalledCount:6,password:'fixture-password'};
 assert.equal((await post(addition)).status,202);await deployment.waitForIdle();registry=await store.load();
 assert.equal(registry.nodes[0].deployment.appliedCount,8);assert.equal(registry.nodes[0].workers[0].count,8);
 assert.equal((await post(addition)).status,409,'replaying the same add request cannot add twice');
 const registrations=plans.length;
 ssh.checkWorkerCapacity=async()=>{throw Object.assign(new Error('节点内存不足：实际 7.44 GiB'),{code:'NODE_DEPLOYMENT_INSUFFICIENT_MEMORY'});};
 assert.equal((await post({version:registry.version,additionalCount:12,role:'incremental',expectedInstalledCount:8,password:'fixture-password'})).status,202);
 await deployment.waitForIdle();registry=await store.load();
 assert.equal(plans.length,registrations+1,'manual deployment proceeds without memory-budget admission');
 assert.equal(registry.nodes[0].deployment.error,null);
 assert.equal(registry.nodes[0].deployment.appliedCount,20);
 // Running Docker containers alone are insufficient: every center heartbeat
 // must be live before a newly added slot is acknowledged or admitted.
 const intakeBefore=intakeCalls.length;connected=false;
 assert.equal((await post({version:registry.version,count:21,syncIntake:true,expectedAllowedCount:1,password:'fixture-password'})).status,202);
 await deployment.waitForIdle();registry=await store.load();
 assert.equal(registry.nodes[0].deployment.state,'failed');assert.equal(registry.nodes[0].deployment.steps.connection,'failed');
 assert.equal(registry.nodes[0].deployment.appliedCount,20);assert.equal(allowedCount,1);assert.equal(intakeCalls.length,intakeBefore);
 connected=true;
 assert.equal((await post({version:registry.version,count:21,syncIntake:true,expectedAllowedCount:1,password:'fixture-password'})).status,202);
 await deployment.waitForIdle();registry=await store.load();
 assert.equal(registry.nodes[0].deployment.state,'connected');assert.equal(registry.nodes[0].deployment.appliedCount,21);assert.equal(allowedCount,21);
 assert.equal((await post({version:registry.version,additionalCount:29,role:'incremental',expectedInstalledCount:21,syncIntake:true,expectedAllowedCount:21,password:'fixture-password'})).status,202);
 await deployment.waitForIdle();registry=await store.load();
 assert.equal(registry.nodes[0].deployment.appliedCount,50);assert.equal(allowedCount,50);
 assert.equal(plans.at(-1).files['incremental-1.json'],plans[0].files['incremental-1.json']);
});
