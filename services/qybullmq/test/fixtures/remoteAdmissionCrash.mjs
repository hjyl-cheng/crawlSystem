import pg from 'pg';
import { RemoteNodeStore } from '../../src/remoteNodes/store.js';
import { RemoteChannelPlanStore } from '../../src/remoteNodes/channelPlanStore.js';
import { RemoteChannelExecutionStore } from '../../src/remoteNodes/channelExecutionStore.js';
import { supervisionLockKey } from '../../src/remoteNodes/centerExecutionSupervisor.js';
import { assertIsolatedRemoteDatabase } from '../../src/remoteNodes/isolation.js';
import { PUBLICATION_WRITER_VERSION } from '../../src/publicationWriterVersion.js';
import { runRemoteIncrementalPlan } from '../../src/remoteNodes/incrementalCoordinator.js';
import { assertRemoteIncrementalBusinessFence } from '../../src/remoteNodes/incrementalBusinessFence.js';
import { Worker } from 'bullmq';
import { INCREMENTAL_QUEUE } from '../../src/incrementalPlan.js';

process.once('message', async config => {
  try {
    const pool = new pg.Pool({ connectionString: process.env.REMOTE_NODE_TEST_DATABASE_URL,
      options: `-c publication.writer_version=${PUBLICATION_WRITER_VERSION}` });
    await assertIsolatedRemoteDatabase(pool);
    const guard = await pool.connect();
    await guard.query('SELECT pg_advisory_lock(781138012,hashtext($1))', [supervisionLockKey(config.row)]);
    const store = new RemoteNodeStore({ pool });
    const channelStore = new RemoteChannelPlanStore({ store });
    const executions = new RemoteChannelExecutionStore({ channelStore, profileSecret: config.profileSecret });
    const worker=new Worker(INCREMENTAL_QUEUE,async job=>{
      const admission = await executions.prepare({...config.args,job});
      process.send({ admission });
      process.once('message', async ({lease}) => {
        try {
          const result=await runRemoteIncrementalPlan({channelStore,lease,assertBusinessFence:assertRemoteIncrementalBusinessFence,pollMs:5});
          process.send({completed:result});
        }catch(error){process.send({error:error.code||error.message});}
      });
      await new Promise(()=>{}); // killed before BullMQ acknowledgement
    },{...config.queue,lockDuration:3000,stalledInterval:500});
    worker.on('error',()=>{});
    worker.on('failed',(_job,error)=>process.send({error:error.code||error.message}));
  } catch (error) { process.send({ error: error.code || error.message }); process.exitCode = 1; }
});
