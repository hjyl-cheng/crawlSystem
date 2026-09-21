import test from 'node:test';
import assert from 'node:assert/strict';
import {spawnSync} from 'node:child_process';
import {fileURLToPath} from 'node:url';
import {buildNodeCollectDeployment} from './nodeRuntime/collectDeployment.js';

test('the actual installer allows manual counts above the node memory estimate',()=>{
  const node={id:'54a7cdd3-eb9c-4713-8d2f-21f4a5279de0',kind:'execution',provisioning:{state:'ready'},runtime:{state:'ready'}};
  const script=String.raw`
import importlib.util,json,pathlib,sys,tempfile
from unittest.mock import patch
spec=importlib.util.spec_from_file_location('installer',sys.argv[1]);m=importlib.util.module_from_spec(spec);spec.loader.exec_module(m)
plan=json.load(sys.stdin);expected=sys.argv[2]
original=pathlib.Path.read_text
def read(path,*args,**kwargs):
    if str(path)=='/proc/meminfo':return 'MemTotal: 7806160 kB\n'
    if str(path)=='/etc/qy-node/runtime/node-id':return plan['nodeId']
    return original(path,*args,**kwargs)
class BeforeWrite(Exception):pass
with tempfile.TemporaryDirectory(prefix='worker-memory-test-') as directory:
    bundle=pathlib.Path(directory)/'bundle.json'
    bundle.write_text(json.dumps({'plan':plan,'credentials':{'nodeId':plan['nodeId'],'deploymentId':plan['deploymentId']}}))
    with patch.object(pathlib.Path,'read_text',read),patch.object(m,'run',return_value=''),patch.object(m,'directory',side_effect=BeforeWrite) as writes:
        try:m.deploy(str(bundle),'files');raise AssertionError('must stop before modifying node files')
        except BeforeWrite:assert expected=='accepted'
        except RuntimeError as error:
            assert expected=='rejected' and str(error)=='NODE_DEPLOYMENT_INSUFFICIENT_MEMORY',str(error)
            writes.assert_not_called()
`;
  for(const role of ['incremental','fullcrawl'])for(const count of [20,23,24,32,50,150]){
    const plan=buildNodeCollectDeployment({node:{...node,workers:[{role,count}]},image:'registry.example/worker@sha256:'+'a'.repeat(64),gatewayUrl:'https://center.example',natsUrl:'wss://messages.example/node-messages'});
    assert.ok(Object.values(plan.compose.services).every(service=>service.mem_limit==='768m'),'container protection limit must remain unchanged');
    const result=spawnSync('python3',['-c',script,fileURLToPath(new URL('./nodeRuntime/deployWorkers.py',import.meta.url)),'accepted'],{input:JSON.stringify(plan),encoding:'utf8',timeout:10000});
    assert.equal(result.status,0,JSON.stringify({stderr:result.stderr,error:result.error?.message,signal:result.signal}));
  }
});

test('NATS deployment preserves slot config hashes and rejects unencrypted endpoints',()=>{
 const args={node:{id:'54a7cdd3-eb9c-4713-8d2f-21f4a5279de0',kind:'execution',provisioning:{state:'ready'},runtime:{state:'ready'},workers:[{role:'incremental',count:2}]},deploymentId:'41041afe-bc67-418b-90bc-8a1a81499d65',image:'registry.example/worker@sha256:'+'a'.repeat(64),gatewayUrl:'https://center.example'};
 const http=buildNodeCollectDeployment(args),nats=buildNodeCollectDeployment({...args,natsUrl:'wss://messages.example/node-messages'});
 assert.deepEqual(nats.files,http.files);assert.deepEqual(nats.registrations,http.registrations);
 assert.ok(Object.values(nats.compose.services).every(s=>s.environment.REMOTE_NODE_NATS_URL==='wss://messages.example/node-messages'));
 assert.throws(()=>buildNodeCollectDeployment({...args,natsUrl:'nats://messages.example:4222'}));
});
