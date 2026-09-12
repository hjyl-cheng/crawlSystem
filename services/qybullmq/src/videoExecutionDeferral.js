import { DelayedError } from 'bullmq';
import { isVideoExecutionRecoveryPending } from './videoExecutionRecovery.js';

export async function runVideoExecutionResumable({ job, token, execute }) {
  try {
    return await execute();
  } catch (error) {
    if (!isVideoExecutionRecoveryPending(error)) throw error;
    await job.updateProgress({ stage: 'waiting_video_recovery',run_id: error.runId });
    await job.moveToDelayed(Date.now() + error.delayMs,token);
    throw new DelayedError();
  }
}
