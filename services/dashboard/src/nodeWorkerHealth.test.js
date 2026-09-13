import test from 'node:test';
import assert from 'node:assert/strict';
import {spawnSync} from 'node:child_process';
import {fileURLToPath} from 'node:url';
import {buildNodeCollectDeployment} from './nodeRuntime/collectDeployment.js';
import {buildNodeConnectionDeployment} from './nodeRuntime/connectionDeployment.js';

const args={node:{id:'b18d8455-7881-43be-ae52-18cfcf160514',kind:'execution',provisioning:{state:'ready'},runtime:{state:'ready'},workers:[{role:'incremental',count:2}]},
  deploymentId:'c18d8455-7881-43be-ae52-18cfcf160514',image:'registry.example/node@sha256:'+'a'.repeat(64),gatewayUrl:'https://center.example'};
test('both node recipes disable inherited Docker probes without changing restart or drain behavior',()=>{
  for(const build of [buildNodeConnectionDeployment,buildNodeCollectDeployment]){
    for(const service of Object.values(build(args).compose.services)){
      assert.deepEqual(service.healthcheck,{disable:true});
      assert.equal(service.restart,'unless-stopped');
    }
  }
  assert.equal(buildNodeCollectDeployment(args).compose.services['incremental-1'].stop_grace_period,'16m');
});
const installer=fileURLToPath(new URL('./nodeRuntime/deployWorkers.py',import.meta.url));
const harness=String.raw`
import contextlib,importlib.util,io,json,pathlib,sys,tempfile
from unittest.mock import patch
spec=importlib.util.spec_from_file_location('installer',sys.argv[1]);m=importlib.util.module_from_spec(spec);spec.loader.exec_module(m)
v=json.load(sys.stdin);plan=v['plan'];real_path=pathlib.Path
with tempfile.TemporaryDirectory() as directory:
 root=real_path(directory);(root/'node-id').write_text(plan['nodeId'])
 bundle=root/'bundle.json';bundle.write_text(json.dumps({'plan':plan,'credentials':{'nodeId':plan['nodeId'],'deploymentId':plan['deploymentId']}}))
 def path(p):return root/'node-id' if str(p)=='/etc/qy-node/runtime/node-id' else real_path(p)
 def run(cmd,timeout):
  if cmd[-3:]==['ps','--all','-q']:return 'container-1 container-2'
  if cmd[:2]==['docker','inspect']:return json.dumps(v['containers'])
  raise AssertionError('verify must only inspect containers')
 with patch.object(m,'Path',path),patch.object(m,'run',run),patch.object(m.time,'monotonic',side_effect=[0,100]),contextlib.redirect_stdout(io.StringIO()):
  try:m.deploy(str(bundle),'verify');passed=True
  except RuntimeError:passed=False
 print(json.dumps({'passed':passed}))
`;
test('real installer checks running identities without Docker health; stopped, paused and duplicate slots fail',()=>{
  const plan=buildNodeCollectDeployment(args);
  const containers=Object.entries(plan.compose.services).map(([slot,service])=>({Config:{Image:plan.image,Labels:{...service.labels,'com.docker.compose.service':slot}},State:{Running:true,Paused:false,Restarting:false}}));
  const verify=items=>{
    const result=spawnSync('python3',['-c',harness,installer],{input:JSON.stringify({plan,containers:items}),encoding:'utf8'});
    assert.equal(result.status,0,result.stderr);return JSON.parse(result.stdout).passed;
  };
  assert.equal(verify(containers),true,'a running node without Docker Health must reach center heartbeat verification');
  const legacy=structuredClone(containers);legacy[0].State.Health={Status:'unhealthy'};
  assert.equal(verify(legacy),true,'legacy Docker probe cannot override the authoritative center heartbeat gate');
  for(const patch of [{Running:false},{Paused:true},{Restarting:true}]){
    const invalid=structuredClone(containers);Object.assign(invalid[0].State,patch);assert.equal(verify(invalid),false);
  }
  assert.equal(verify([containers[0],containers[0]]),false);
  const wrong=structuredClone(containers);wrong[0].Config.Labels['qy.node.id']='wrong';assert.equal(verify(wrong),false);
});

test('installer rejects reused HTTP containers when NATS deployment was requested',()=>{
  const plan=buildNodeCollectDeployment({...args,natsUrl:'wss://center.example/node-messages'});
  const containers=Object.entries(plan.compose.services).map(([slot,service])=>({Config:{Image:plan.image,Labels:service.labels,Env:Object.entries(service.environment).map(([key,value])=>key+'='+value)},State:{Running:true}}));
  const verify=items=>{
    const result=spawnSync('python3',['-c',harness,installer],{input:JSON.stringify({plan,containers:items}),encoding:'utf8'});
    assert.equal(result.status,0,result.stderr);return JSON.parse(result.stdout).passed;
  };
  assert.equal(verify(containers),true);
  const oldNats=structuredClone(containers);oldNats[0].Config.Env=['REMOTE_NODE_NATS_URL='+plan.natsUrl];
  assert.equal(verify(oldNats),false,'an old per-command NATS worker is not a whole-channel deployment');
  const oldHttp=structuredClone(containers);oldHttp[0].Config.Env=[];
  assert.equal(verify(oldHttp),false,'--no-recreate must not falsely approve an old HTTP worker');
  const wrongEndpoint=structuredClone(containers);wrongEndpoint[0].Config.Env=['REMOTE_NODE_NATS_URL=wss://wrong.example/node-messages'];
  assert.equal(verify(wrongEndpoint),false);
});
