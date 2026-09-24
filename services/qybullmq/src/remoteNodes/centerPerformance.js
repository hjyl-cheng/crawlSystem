import {performance,monitorEventLoopDelay,PerformanceObserver} from 'node:perf_hooks';

const buckets=[1,5,10,25,50,100,250,500,1000,2000,5000,15000,60000,Infinity];
const operations=new Set(['other','transaction','admission','prepare','command_wait','apply','stop','checkpoint','finish','collecting','receiving','binding','awaiting_claim','stopping','recovering','finished','blocked','admitting']);
const operationName=value=>operations.has(value)?value:'other';

// Histograms have fixed buckets and labels, no SQL text, IDs or payloads. Pool
// wrapping preserves callback and promise APIs, including late checkout after
// a caller's deadline. No database calls are added by observation.
export class CenterPerformance {
  constructor({enabled=false,report=()=>{},intervalMs=10000,sampleEvery=20}={}) {
    if(!Number.isSafeInteger(sampleEvery)||sampleEvery<1)throw new TypeError('positive telemetry sample interval required');
    this.enabled=enabled;this.report=report;this.intervalMs=intervalMs;
    this.sampleEvery=sampleEvery;this.operationSequence=0;
    this.histograms=new Map();this.pools=new Map();this.clients=new WeakMap();
    if(!enabled)return;
    this.loop=monitorEventLoopDelay({resolution:20});this.loop.enable();
    this.gc=new PerformanceObserver(list=>{for(const entry of list.getEntries())this.record('gc_ms',entry.duration);});
    this.gc.observe({entryTypes:['gc']});
    this.previousCpu=process.cpuUsage();this.previousLoop=performance.eventLoopUtilization();this.previousAt=performance.now();
    this.sampleTimer=setInterval(()=>this.sample(),1000);this.sampleTimer.unref();
    this.reportTimer=setInterval(()=>this.flush(),intervalMs);this.reportTimer.unref();
  }
  record(key,value) {
    if(!this.enabled || !Number.isFinite(value) || value<0)return;
    let h=this.histograms.get(key);
    if(!h){if(this.histograms.size>=256)return;h={count:0,sum:0,max:0,buckets:Array(buckets.length).fill(0)};this.histograms.set(key,h);}
    h.count++;h.sum+=value;h.max=Math.max(h.max,value);h.buckets[buckets.findIndex(upper=>value<=upper)]++;
  }
  async measure(operation,action) {
    if(!this.enabled || ++this.operationSequence%this.sampleEvery!==0)return action();
    const name=operationName(operation),started=performance.now();
    try{return await action();}
    finally{this.record(`operation.${name}.ms`,performance.now()-started);}
  }
  beginTransaction(client,name,operation) {
    if(!this.enabled || ++this.operationSequence%this.sampleEvery!==0)return null;
    this.instrumentClient(client,name);
    const checkout=this.clients.get(client),previousQuery=client.query,previousSampled=checkout.sampled;
    const context={operation:operationName(operation),queries:0};const started=performance.now();
    checkout.context=context;checkout.sampled=true;client.query=checkout.timed;
    return ()=>{
      this.record(`operation.${context.operation}.ms`,performance.now()-started);
      this.record(`operation.${context.operation}.sql_count`,context.queries);
      checkout.context=null;checkout.sampled=previousSampled;client.query=previousQuery;
    };
  }
  attachPool(pool,name) {
    if(!this.enabled)return pool;
    if(!['gateway','heartbeat','result'].includes(name))throw new TypeError('unknown observed pool');
    this.pools.set(name,pool);pool.centerPerformance=this;pool.centerPerformancePoolName=name;
    const connect=pool.connect.bind(pool);const metrics=this;let sequence=0;
    pool.connect=function(callback){
      const sampled=metrics.enabled && ++sequence%metrics.sampleEvery===0;
      if(!sampled)return connect(callback);
      const started=performance.now();const operation='other';
      const borrowed=(error,client,release)=>{
        if(sampled)metrics.record(`${name}.acquire.${operation}.ms`,performance.now()-started);
        if(error){metrics.record(`${name}.acquire_errors`,1);return;}
        metrics.instrumentClient(client,name);
        const checkout=metrics.clients.get(client);checkout.sampled=true;client.query=checkout.timed;
        const acquired=performance.now();let done=false;
        client.release=function(...args){
          if(!done){done=true;client.query=checkout.original;checkout.sampled=false;metrics.record(`${name}.hold.${operation}.ms`,performance.now()-acquired);}
          return release(...args);
        };
      };
      if(typeof callback==='function')return connect((error,client,release)=>{
        borrowed(error,client,release);callback(error,client,client?.release??release);
      });
      return connect().then(client=>{borrowed(null,client,client.release.bind(client));return client;},error=>{borrowed(error);throw error;});
    };
    return pool;
  }
  instrumentClient(client,name) {
    if(this.clients.has(client))return;const checkout={sampled:false,original:client.query};this.clients.set(client,checkout);
    const query=client.query;const metrics=this;
    checkout.timed=function(...args){
      // Custom streaming Query instances retain their own completion semantics.
      if(!checkout.sampled || args[0]?.submit)return query.apply(this,args);
      const context=checkout.context;if(context)context.queries++;
      const operation=operationName(context?.operation);const started=performance.now();let finished=false;
      const finish=error=>{if(finished)return;finished=true;metrics.record(`${name}.sql.${operation}.ms`,performance.now()-started);if(error)metrics.record(`${name}.sql_errors_sampled`,1);};
      let callbackIndex=args.findLastIndex(arg=>typeof arg==='function');
      if(callbackIndex>=0){const callback=args[callbackIndex];args[callbackIndex]=function(error,...rest){finish(error);return callback.call(this,error,...rest);};}
      else if(typeof args[0]?.callback==='function'){
        const config=args[0],callback=config.callback;args[0]={...config,callback:function(error,...rest){finish(error);return callback.call(this,error,...rest);}};
        callbackIndex=0;
      }
      try{
        const result=query.apply(this,args);
        if(callbackIndex<0 && result?.then)return result.then(value=>{finish();return value;},error=>{finish(error);throw error;});
        return result;
      }catch(error){finish(error);throw error;}
    };
  }
  sample() {
    for(const [name,pool] of this.pools){this.record(`${name}.waiting`,pool.waitingCount);this.record(`${name}.idle`,pool.idleCount);this.record(`${name}.total`,pool.totalCount);}
  }
  snapshot() {
    const metrics={};
    for(const [key,h] of this.histograms){
      const percentile=p=>{let count=0;for(let i=0;i<h.buckets.length;i++){count+=h.buckets[i];if(count>=h.count*p)return Number.isFinite(buckets[i])?buckets[i]:h.max;}return h.max;};
      metrics[key]={count:h.count,mean:h.sum/h.count,max:h.max,p50_upper:percentile(.5),p95_upper:percentile(.95),p99_upper:percentile(.99)};
    }
    return metrics;
  }
  flush() {
    if(!this.enabled)return;
    const now=performance.now(),cpu=process.cpuUsage(),loop=performance.eventLoopUtilization();
    const event={event:'remote_center_performance',at:new Date().toISOString(),window_ms:now-this.previousAt,
      sample_every:this.sampleEvery,sql_timings_sampled:true,
      cpu_cores:(cpu.user+cpu.system-this.previousCpu.user-this.previousCpu.system)/1000/(now-this.previousAt),
      event_loop_utilization:performance.eventLoopUtilization(loop,this.previousLoop).utilization,
      event_loop_delay_p95_ms:this.loop.percentile(95)/1e6,memory:process.memoryUsage(),metrics:this.snapshot()};
    this.histograms.clear();this.loop.reset();this.previousAt=now;this.previousCpu=cpu;this.previousLoop=loop;
    try{this.report(event);}catch{/* Metrics cannot change execution outcomes. */}
    return event;
  }
  close() {
    clearInterval(this.sampleTimer);clearInterval(this.reportTimer);this.gc?.disconnect();this.loop?.disable();this.enabled=false;
  }
}
