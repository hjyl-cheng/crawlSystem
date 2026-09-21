import {mkdir,readFile,rename,writeFile} from 'node:fs/promises';
import {dirname} from 'node:path';
import {randomUUID} from 'node:crypto';
import {setTimeout as delay} from 'node:timers/promises';
import {LOCAL_INTAKE_SUBJECT} from './localIntakeSignals.js';
import {uuid} from './protocol.js';

// Reuse encrypted, per-node deployment credentials; never expose a shared node
// principal. Broker ACLs and the original application token check both apply.
export function createNatsProvisioning({pool,routes,file,centerUser='center',centerPassword,localIntakePassword,signals}){
  if(!file?.startsWith('/')||!centerPassword||centerPassword.length<32)throw new TypeError('NATS auth file and strong center credential required');
  if(localIntakePassword!==undefined && (typeof localIntakePassword!=='string'||localIntakePassword.length<32))throw new TypeError('strong local intake credential required');
  let pending=Promise.resolve();let loop;const stopped=new AbortController();
  async function write(){
    const rows=(await pool.query(`SELECT d.node_id,d.credentials_cipher,n.capabilities FROM remote_ingestion.node_deployments d
      JOIN remote_ingestion.nodes n USING(node_id) WHERE n.state <> 'disabled' ORDER BY d.node_id`)).rows;
    const users=[{user:centerUser,password:centerPassword}];
    if(localIntakePassword)users.push({user:'local-intake',password:localIntakePassword,permissions:{publish:{deny:['>']},subscribe:[LOCAL_INTAKE_SUBJECT]}});
    for(const row of rows){
      const id=uuid(row.node_id);const credentials=routes.decrypt(row.credentials_cipher,`node-deployment:${id}`);
      users.push({user:id,password:credentials.nodeToken,permissions:{
        publish:[`qy.remote.rpc.${id}.*`,row.capabilities?.includes('youtube.full-crawl.v1')?`qy.remote.full.results.${id}`:`qy.remote.results.${id}`],subscribe:[`_INBOX.${id}.>`],
      }});
    }
    const content=`users: ${JSON.stringify(users)}\n`;
    if(await readFile(file,'utf8').catch(error=>{if(error.code!=='ENOENT')throw error;return null;})===content)return;
    await mkdir(dirname(file),{recursive:true,mode:0o700});
    const temporary=`${file}.${randomUUID()}.tmp`;await writeFile(temporary,content,{mode:0o600});await rename(temporary,file);
  }
  const sync=()=>{const task=pending.catch(()=>{}).then(write);pending=task;return task;};
  return {sync,start(report=()=>{}){
    if(loop)return;
    loop=(async()=>{while(!stopped.signal.aborted){
      const watch=signals?.watch('credentials',{timeoutMs:60000,signal:stopped.signal});
      try{
        await sync().catch(()=>report({event:'remote_nats_credentials_sync_failed'}));
        if(watch)await watch.wait;else await delay(60000,null,{signal:stopped.signal}).catch(()=>{});
      }finally{watch?.cancel();}
    }})();
  },async close(){stopped.abort();await loop;await pending.catch(()=>{});}};
}
