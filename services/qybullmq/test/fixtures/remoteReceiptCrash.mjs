import { RemoteChannelPlanExecutor } from '../../src/remoteNodes/channelPlanExecutor.js';
import { RemoteResultSpool } from '../../src/remoteNodes/spool.js';
import { createRemoteNodeClient } from '../../src/remoteNodes/client.js';

process.once('message', async config => {
  try {
    const remote=createRemoteNodeClient({...config.center,allowLoopbackHttp:true});
    const client={...remote,claim:()=>remote.claim(config.claimId),uploadCommand:async(...args)=>{
      if(config.commit)await remote.uploadCommand(...args);
      process.send({saved:true,committed:config.commit});
      await new Promise(()=>{}); // kill before the executor receives the ack
    }};
    const executor=new RemoteChannelPlanExecutor({client,spool:new RemoteResultSpool({directory:config.directory}),
      withSession:(_lease,_options,invoke)=>invoke(),pollMs:5,
      youtube:{openChannel:async()=>config.snapshot,fetchDetail:()=>{throw new Error('unexpected fixture detail');}}});
    await executor.runOnce();
  }catch(error){process.send({error:error.code||error.name});}
});
