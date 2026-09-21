import {jetstream,jetstreamManager,AckPolicy,DiscardPolicy,RetentionPolicy,StorageType} from '@nats-io/jetstream';
import {decode,failure,resultEnvelope} from './natsProtocol.js';
import {RemoteProtocolError,uuid} from './protocol.js';
export const FULL_CRAWL_RESULT_STREAM='QY_REMOTE_FULL_CRAWL_RESULTS';
export const FULL_CRAWL_RESULT_SUBJECT='qy.remote.full.results';
export async function startFullCrawlNatsResults({nc,fullCrawls,resultMaxBytes,replicas=1,resultConcurrency=8,report=()=>{}}){
  const store=fullCrawls.store,jsm=await jetstreamManager(nc),js=jetstream(nc);
  const config={name:FULL_CRAWL_RESULT_STREAM,subjects:[FULL_CRAWL_RESULT_SUBJECT+'.*'],storage:StorageType.File,
    retention:RetentionPolicy.Workqueue,discard:DiscardPolicy.New,max_bytes:resultMaxBytes,max_msg_size:2*1024*1024,
    num_replicas:replicas,duplicate_window:120*1e9};
  let old;try{old=await jsm.streams.info(config.name);}catch(error){if(error.status!==404)throw error;}
  if(old){if(['storage','retention','discard','max_bytes','num_replicas','max_msg_size'].some(k=>old.config[k]!==config[k])
    ||JSON.stringify(old.config.subjects)!==JSON.stringify(config.subjects))throw Error('NATS_FULL_STREAM_CONFIG_CONFLICT');}
  else await jsm.streams.add(config);
  const consumerName='full-center-receipts';
  let consumer;try{consumer=await jsm.consumers.info(config.name,consumerName);}catch(error){if(error.status!==404)throw error;}
  if(!consumer)await jsm.consumers.add(config.name,{durable_name:consumerName,ack_policy:AckPolicy.Explicit,ack_wait:30*1e9,max_ack_pending:32});
  else if(consumer.config.ack_policy!==AckPolicy.Explicit)throw Error('NATS_FULL_CONSUMER_CONFIG_CONFLICT');
  const messages=await(await js.consumers.get(config.name,consumerName)).consume({max_messages:resultConcurrency});
  async function receive(msg){
    const keep=setInterval(()=>msg.working(),10000);
    try{
      const value=decode(msg.data),nodeId=uuid(msg.subject.split('.')[4]);
      if(value?.nodeId!==nodeId||value.version!==1||value.operation!=='full_crawl_part'||typeof value.payload!=='string')throw new RemoteProtocolError('INVALID_RESULT',400);
      const bytes=Buffer.from(value.payload,'base64'),canonical=resultEnvelope(nodeId,value.token,value.operation,value.taskId,bytes);
      if(value.payload!==canonical.payload||value.receiptId!==canonical.receiptId)throw new RemoteProtocolError('INVALID_RESULT_ID',400);
      if(await store.authenticate(value.token)!==nodeId)throw new RemoteProtocolError('UNAUTHORIZED',401);
      const old=(await store.pool.query('SELECT node_id FROM remote_ingestion.transport_receipts WHERE receipt_id=$1',[value.receiptId])).rows[0];
      if(old){if(old.node_id!==nodeId)throw new RemoteProtocolError('UNAUTHORIZED',401);await msg.ackAck();return;}
      let response;
      try{response={ok:true,value:await fullCrawls.receive(nodeId,value.taskId,bytes)};}
      catch(error){response=failure(error);if(response.status>=500)throw error;}
      await store.pool.query(`INSERT INTO remote_ingestion.transport_receipts(receipt_id,node_id,task_id,response)
        VALUES($1,$2,$3,$4) ON CONFLICT(receipt_id) DO NOTHING`,[value.receiptId,nodeId,value.taskId,response]);
      await msg.ackAck();
    }catch(error){const info=failure(error);report({event:'full_crawl_nats_result_error',code:info.error});if(info.status>=500)msg.nak(1000);else msg.term(info.error);}
    finally{clearInterval(keep);}
  }
  const active=new Set();let failed=false,closing=false;
  const run=(async()=>{for await(const msg of messages){const promise=receive(msg);active.add(promise);promise.finally(()=>active.delete(promise));
    if(active.size>=resultConcurrency)await Promise.race(active);}await Promise.allSettled(active);})();
  run.catch(()=>{failed=true;void nc.close();});
  run.then(()=>{if(!closing){failed=true;void nc.close();}},()=>{});
  return {isReady:()=>!closing&&!failed&&!nc.isClosed(),
    async stats(){if(failed||nc.isClosed())throw Error('NATS_FULL_CONSUMER_UNAVAILABLE');return jsm.consumers.info(config.name,consumerName);},
    async close(){closing=true;messages.stop();await run;}};
}
