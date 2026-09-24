// Bounded, auditable repair. Dry-run by default; never resets Rota budgets or
// edits BullMQ job state. Run from the center with its existing secret mounts.
import {readFile} from 'node:fs/promises';
import pg from 'pg';
import {Queue} from 'bullmq';
import {ProxyControlClient} from '../src/proxyControlClient.js';
import {RemoteNodeStore} from '../src/remoteNodes/store.js';
import {recordIncrementalBudgetExhaustion} from '../src/incrementalBudgetRecovery.js';
import {INCREMENTAL_QUEUE,INCREMENTAL_JOB_NAME} from '../src/incrementalPlan.js';
import {PUBLICATION_WRITER_VERSION} from '../src/publicationWriterVersion.js';
import {loadIncrementalBudgetPage} from '../src/incrementalBudgetReconciliation.js';

const args=process.argv.slice(2),apply=args.includes('--apply');
const option=name=>args[args.indexOf(name)+1];
if(!args.includes('--since')||!args.includes('--until'))throw Error('--since and --until are required');
const since=new Date(option('--since')),until=new Date(option('--until'));
const limit=args.includes('--limit')?Number(option('--limit')):25;
const cursor=args.includes('--cursor')?option('--cursor'):null;
if(args.includes('--cursor')&&(!cursor||cursor.startsWith('--')))throw Error('--cursor requires the previous next_cursor');
if(!Number.isFinite(+since)||!Number.isFinite(+until)||+until<=+since||+until-+since>86400000
  ||!Number.isSafeInteger(limit)||limit<1||limit>50)throw Error('bounded 24h window and limit 1..50 required');
const pool=new pg.Pool({connectionString:process.env.REMOTE_NODE_DATABASE_URL,max:1,connectionTimeoutMillis:5000,
  options:`-c publication.writer_version=${PUBLICATION_WRITER_VERSION} -c statement_timeout=8000 -c lock_timeout=1000`});
const store=new RemoteNodeStore({pool});
const redis=new URL(process.env.REMOTE_NODE_REDIS_URL);
const queue=new Queue(INCREMENTAL_QUEUE,{prefix:process.env.REMOTE_NODE_QUEUE_PREFIX,connection:{host:redis.hostname,port:Number(redis.port||6379),
  username:redis.username?decodeURIComponent(redis.username):undefined,password:redis.password?decodeURIComponent(redis.password):undefined,tls:redis.protocol==='rediss:'?{}:undefined}});
const control=new ProxyControlClient({token:(await readFile(process.env.REMOTE_NODE_ROTA_CONTROL_TOKEN_FILE,'utf8')).trim(),maxAttempts:1});
try {
  const {candidates,next_cursor}=await loadIncrementalBudgetPage(pool,{since,until,limit,cursor});
  for(const row of candidates){
    const out={plan_id:row.plan_id,job_id:row.job_id,apply};
    try {
      const job=await queue.getJob(row.job_id),state=job?await job.getState():'missing';
      if(state!=='failed'&&state!=='missing'){console.log(JSON.stringify({...out,result:'deferred',state}));continue;}
      const budget=await control.businessRunBudget(row.run_id);
      if(!budget.budget_exhausted_at||budget.business_tasks_used<budget.business_tasks_limit){console.log(JSON.stringify({...out,result:'not_exhausted'}));continue;}
      const activation=Math.max(Number(job?.attemptsStarted)||0,Number(row.activation)||0);
      if(!activation)throw Error('missing activation evidence');
      const recovered={id:row.job_id,name:INCREMENTAL_JOB_NAME,queueName:INCREMENTAL_QUEUE,data:row.payload_json,attemptsStarted:activation};
      const error=Object.assign(new Error('Rota Business Run budget exhausted'),{code:'BUSINESS_RUN_BUDGET_EXHAUSTED'});
      // Dry-run executes the identical SQL validation and writes in a transaction
      // that is explicitly rolled back, including all observations and outbox.
      let result;
      if(apply)result=await store.transaction(client=>recordIncrementalBudgetExhaustion(client,recovered,error));
      else {
        const rollback=Object.assign(new Error('dry-run rollback'),{dryRun:true});
        try {await store.transaction(async client=>{result=await recordIncrementalBudgetExhaustion(client,recovered,error);throw rollback;});}
        catch(e){if(e!==rollback)throw e;}
      }
      console.log(JSON.stringify({...out,result:apply?'settled':'eligible',replay:result.replay}));
    } catch(e){console.log(JSON.stringify({...out,result:'deferred',code:e.code??e.name}));}
  }
  // Advance past every examined record, including deferred items. Start a new
  // pass without --cursor to revisit them; never carry a dry-run cursor to apply.
  console.log(JSON.stringify({at:new Date().toISOString(),apply,examined:candidates.length,limit,
    since:since.toISOString(),until:until.toISOString(),next_cursor}));
} finally {await queue.close();await pool.end();}
