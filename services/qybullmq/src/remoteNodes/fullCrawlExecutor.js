import {randomUUID} from 'node:crypto';
import {setTimeout as delay} from 'node:timers/promises';
import {FullCrawlNodeJournal} from './fullCrawlNode.js';
import {validateFullCrawlExecution} from './fullCrawlProtocol.js';

export class RemoteFullCrawlExecutor {
  constructor({client,spool,youtube,networkSession,pollMs=100,timeoutMs=15*60*1000}){
    if(client.transport!=='nats'||!networkSession||!['openChannel','fetchUploads','fetchDetail'].every(k=>typeof youtube?.[k]==='function'))throw TypeError('full-crawl NATS and managed session required');
    Object.assign(this,{client,spool,youtube,networkSession,pollMs,timeoutMs});
    this.journal=new FullCrawlNodeJournal({client,spool});this.busy=false;this.stopping=false;
  }
  stop(){this.stopping=true;}
  async runOnce(){
    if(this.busy)throw Error('FULL_CRAWL_EXECUTOR_BUSY');this.busy=true;
    let timer,renewal=Promise.resolve(),done=false;
    try{
      await this.spool.init();
      // Replay raw evidence before interrupted network cleanup. It cannot grant
      // access to the old browser session or commit business state.
      await this.journal.recover();await this.networkSession.recover();
      let claim=await this.spool.read('claim.json');
      if(claim?.lease){
        const request={task_id:claim.lease.task_id,generation:claim.lease.generation,connection:claim.lease.connection};
        let state;try{state=await this.client.fullCrawlPoll(request);}catch(error){
          if(!['STALE_LEASE','WORKER_CONNECTION_STALE','FULL_CRAWL_BUSINESS_FENCE_STALE'].includes(error.code))throw error;
          state={status:'closed'};
        }
        if(state.status!=='leased'){await this.spool.remove('claim.json');return 'closed';}
        // An executor loop has exited, so any remaining claim is an interrupted
        // browser. The center must retire it before another lease can begin.
        throw Object.assign(Error('FULL_CRAWL_INTERRUPTED_EXECUTION'),{code:'FULL_CRAWL_INTERRUPTED_EXECUTION'});
      }
      if(this.stopping)return 'stopped';
      if(!await this.spool.writable())return 'blocked';
      claim??={claim_id:randomUUID()};await this.spool.save('claim.json',Buffer.from(JSON.stringify(claim)));
      let lease;try{lease=await this.client.claim(claim.claim_id,this.networkSession.slot);}catch(error){
        if(error.code==='CLAIM_EXPIRED')await this.spool.remove('claim.json');throw error;
      }
      if(!lease){await this.spool.remove('claim.json');return 'idle';}
      validateFullCrawlExecution(lease.input);
      if(lease.worker_slot!==this.networkSession.slot||!lease.connection)throw Error('FULL_CRAWL_CLAIM_IDENTITY');
      await this.spool.save('claim.json',Buffer.from(JSON.stringify({...claim,lease})));
      const request={task_id:lease.task_id,generation:lease.generation,connection:lease.connection};
      await this.client.fullCrawlHeartbeat(request);
      const abort=new AbortController(),signal=AbortSignal.any([abort.signal,AbortSignal.timeout(this.timeoutMs)]);
      const schedule=()=>{timer=setTimeout(()=>{renewal=this.client.fullCrawlHeartbeat(request)
        .then(()=>{if(!done)schedule();}).catch(error=>abort.abort(error));},lease.heartbeat_ms??10000);};schedule();
      const ended=await this.networkSession.run(lease,{signal},async({signal:activeSignal})=>{
        for(;;){
          activeSignal.throwIfAborted();
          const state=await this.client.fullCrawlPoll(request);
          for(const id of state.applied??[])await this.journal.applied(id);
          if(state.status!=='leased')return {status:state.status};
          for(const command of state.commands){
            if(command.stage==='close_fetch')return {command};
            await this.journal.execute({request,execution:lease.input,command,youtube:this.youtube,signal:activeSignal});
          }
          // A durable result may still await central application. Bound polling
          // even when the command is replayed until its applied receipt exists.
          await delay(this.pollMs,null,{signal:activeSignal});
        }
      });
      if(ended.command)await this.journal.execute({request,execution:lease.input,command:ended.command,youtube:this.youtube,signal,networkStopped:true});
      // Keep claim until central terminal settlement; no second browser session
      // may be opened for a close command whose acknowledgement was lost.
      if(!ended.command){await this.spool.remove('claim.json');return ended.status;}
      for(;;){
        signal.throwIfAborted();
        const state=await this.client.fullCrawlPoll(request);
        for(const id of state.applied??[])await this.journal.applied(id);
        if(state.status!=='leased'){await this.spool.remove('claim.json');return state.status;}
        await delay(this.pollMs,null,{signal});
      }
    }finally{done=true;clearTimeout(timer);await renewal;this.busy=false;}
  }
  async run({pollMs=1000,onStatus=()=>{},shutdownUploadMs=30000}={}){
    let failures=0,stoppedAt=null;
    for(;;){
      if(this.stopping)stoppedAt??=Date.now();
      try{
        const status=await this.runOnce();onStatus({status});failures=0;
        if(['stopped','blocked'].includes(status))return;
        if(status==='idle')await this.client.waitForActivation?.();
      }catch(error){
        failures++;onStatus({status:'retrying',code:error.code||error.message||'TRANSPORT_ERROR'});
        if(['SPOOL_FULL','JOURNAL_FULL','JOURNAL_CORRUPT','JOURNAL_IDENTITY_CONFLICT'].includes(error.message)){
          onStatus({status:'blocked',code:error.message});return;
        }
        // Preserve the actual protocol rejection for the process log. Returning
        // here hid it behind NODE_EXECUTOR_STOPPED and obscured crash loops.
        if([400,401,403,404,413,415].includes(error.status))throw error;
      }
      if(stoppedAt&&Date.now()-stoppedAt>=shutdownUploadMs)return;
      await delay(Math.min(30000,pollMs*2**Math.min(failures,4)));
    }
  }
}
