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
 bundle=root/'bundle.json';bundle.write_text(json.dumps({'plan':plan,'credentials':{'nodeId':plan['nodeId'],'deploymentId':plan['deploymentId'],'pausedSlots':v.get('pausedSlots',[])}}))
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

test('installer accepts only stopped maintenance-paused containers with restart disabled',()=>{
  const plan=buildNodeCollectDeployment({...args,node:{...args.node,workerRole:'fullcrawl',workers:[{role:'fullcrawl',count:2}]},natsUrl:'tls://center.example:4222'});
  Object.assign(plan.compose.services['full-crawl-1'],{restart:'no',profiles:['paused']});
  const containers=Object.entries(plan.compose.services).map(([slot,service])=>({Config:{Image:plan.image,Labels:service.labels,
    Env:Object.entries(service.environment).map(([k,v])=>k+'='+v)},State:{Running:slot!=='full-crawl-1'},HostConfig:{RestartPolicy:{Name:service.restart}}}));
  const verify=items=>{
    const result=spawnSync('python3',['-c',harness,installer],{input:JSON.stringify({plan,pausedSlots:['full-crawl-1'],containers:items}),encoding:'utf8'});
    assert.equal(result.status,0,result.stderr);return JSON.parse(result.stdout).passed;
  };
  assert.equal(verify(containers),true);
  assert.equal(verify(containers.slice(1)),true,'a paused slot need not be created to deploy other workers');
  const running=structuredClone(containers);running[0].State.Running=true;
  assert.equal(verify(running),false,'a running paused process cannot pass verification');
  const automatic=structuredClone(containers);automatic[0].HostConfig.RestartPolicy.Name='unless-stopped';
  assert.equal(verify(automatic),false,'maintenance pause must survive Docker restart');
});

test('discover recipe uses its own mode, slots and spool, and the installer accepts only that profile',()=>{
  const node={...args.node,workerRole:'discover',workers:[{role:'discover',count:2}]};
  assert.throws(()=>buildNodeCollectDeployment({...args,node}),/NATS/);
  const plan=buildNodeCollectDeployment({...args,node,natsUrl:'tls://center.example:4222'});
  assert.equal(plan.mode,'discover_collect');
  assert.deepEqual(plan.slots,['discover-1','discover-2']);
  assert.equal(plan.runtimeRevision,'youtube-search-discover-v1');
  assert.equal(plan.capability,'youtube.discover-page.v1');
  assert.equal(plan.wholeChannel,undefined);
  const service=plan.compose.services['discover-1'];
  assert.deepEqual(service.environment,{REMOTE_NODE_NATS_URL:'tls://center.example:4222'});
  assert.equal(service.labels['qy.remote.mode'],'discover_collect');
  assert.ok(service.volumes.some(v=>v.target==='/var/lib/qy-node/discover-spool'&&v.source==='/var/lib/qy-node/spool/discover-1'&&v.read_only===false));
  assert.equal(JSON.parse(plan.files['discover-1.json']).role,'discover');
  const containers=Object.entries(plan.compose.services).map(([,item])=>({Config:{Image:plan.image,Labels:item.labels,
    Env:Object.entries(item.environment).map(([k,v])=>k+'='+v)},State:{Running:true}}));
  const verify=(candidate,items=containers)=>{
    const result=spawnSync('python3',['-c',harness,installer],{input:JSON.stringify({plan:candidate,containers:items}),encoding:'utf8'});
    assert.equal(result.status,0,result.stderr);return JSON.parse(result.stdout).passed;
  };
  assert.equal(verify(plan),true);
  const wrongSpool=structuredClone(plan);wrongSpool.compose.services['discover-1'].volumes.at(-1).target='/var/lib/qy-node/spool';
  assert.equal(verify(wrongSpool),false,'a discover worker cannot mount the incremental spool');
  const whole=structuredClone(plan);whole.wholeChannel=true;
  assert.equal(verify(whole),false,'discover never runs the whole-channel incremental transport');
  const wrongRole=structuredClone(plan);wrongRole.files['discover-1.json']=JSON.stringify({...JSON.parse(plan.files['discover-1.json']),role:'incremental'})+'\n';
  assert.equal(verify(wrongRole),false);
});
