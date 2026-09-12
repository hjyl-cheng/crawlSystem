import assert from 'node:assert/strict';
import { randomUUID } from 'node:crypto';
import test from 'node:test';
import { Queue, QueueEvents, Worker } from 'bullmq';
import { runVideoExecutionResumable } from '../src/videoExecutionDeferral.js';
import { VideoExecutionRecoveryPendingError } from '../src/videoExecutionRecovery.js';

const redisUrl = process.env.VIDEO_EXECUTION_RECOVERY_TEST_REDIS_URL;
test('recovery waiting releases the only Worker for another channel without consuming a failure retry', { skip: !redisUrl,timeout: 15000 }, async t => {
  const url = new URL(redisUrl);
  assert.ok(['127.0.0.1','localhost'].includes(url.hostname));
  const connection = { host: url.hostname,port: Number(url.port),maxRetriesPerRequest: null };
  const name = `video-recovery-test-${randomUUID()}`;
  const queue = new Queue(name,{ connection });
  const events = new QueueEvents(name,{ connection });
  const order = [];
  let canResume = false;
  const waiting = await queue.add('channel',{ channel: 'waiting' },{ jobId: 'waiting',attempts: 1 });
  const following = await queue.add('channel',{ channel: 'following' },{ jobId: 'following',attempts: 1 });
  const worker = new Worker(name,(job,token)=>runVideoExecutionResumable({ job,token,execute: async () => {
    if (job.id === 'waiting' && !canResume) throw new VideoExecutionRecoveryPendingError('run-waiting',1000);
    if (job.id === 'following') {
      const deferred = await queue.getJob('waiting');
      assert.equal(await deferred.getState(),'delayed');
      assert.equal(deferred.attemptsMade,0);
      assert.equal(deferred.progress.stage,'waiting_video_recovery');
      canResume = true;
    }
    order.push(job.id);
    return { complete: true };
  } }),{ connection,concurrency: 1 });
  t.after(async () => { await worker.close();await events.close();await queue.obliterate({ force: true });await queue.close(); });
  await events.waitUntilReady();
  await Promise.all([following.waitUntilFinished(events,10000),waiting.waitUntilFinished(events,10000)]);
  assert.deepEqual(order,['following','waiting']);
  assert.equal(await queue.getFailedCount(),0);
});
