import { AsyncLocalStorage } from "node:async_hooks";
import { DelayedError } from "bullmq";

const replay = new AsyncLocalStorage();

export function withVideoApiReplay(action) {
  return replay.run(true, action);
}

export function isVideoApiReplay() {
  return replay.getStore() === true;
}

// Stored API evidence does not need a network identity or another Rota budget.
// A remaining network step must re-enter normal managed execution first.
export function assertVideoApiNetworkAllowed() {
  if (!isVideoApiReplay()) return;
  const error = new Error("API continuation requires a managed network step");
  error.code = "VIDEO_API_NETWORK_REQUIRED";
  throw error;
}

export function isVideoApiHandoff(error) {
  return ["VIDEO_API_PENDING", "VIDEO_API_NETWORK_REQUIRED"].includes(error?.code);
}

export function videoApiPendingError(requestId) {
  const error = new Error("Waiting for durable video API completion");
  error.code = "VIDEO_API_PENDING";
  error.requestId = requestId;
  return error;
}

export async function deferVideoApiJob({ job, token, requestId, delayMs = 15000 }) {
  const data = { ...job.data, video_api_continuation: { request_id: requestId } };
  await job.updateData(data);
  job.data = data;
  await job.updateProgress({ stage: "waiting_video_api", request_id: requestId });
  await job.moveToDelayed(Date.now() + delayMs, token);
  throw new DelayedError();
}

export async function gateVideoApiJob({ query, job, token, delayMs = 15000 }) {
  const requestId = job.data?.video_api_continuation?.request_id;
  if (!requestId) return;
  const row = (await query(`SELECT status,run_id FROM crawler.youtube_api_detail_requests
    WHERE request_id=$1`, [requestId])).rows[0];
  const runId = job.data.run_id ?? (job.data.plan_id ? `incremental:${job.data.plan_id}` : null);
  if (!row || row.run_id !== runId) throw new Error("Video API continuation identity conflicts");
  if (row.status === "pending") {
    await job.moveToDelayed(Date.now() + delayMs, token);
    throw new DelayedError();
  }
}

export async function runVideoApiResumable({ job, token, execute, executeReplay, delayMs }) {
  try {
    if (job.data?.video_api_continuation) {
      try {
        return await withVideoApiReplay(executeReplay);
      } catch (error) {
        if (error?.code !== "VIDEO_API_NETWORK_REQUIRED") throw error;
      }
    }
    const result = await execute();
    if (result?.video_api_pending) throw videoApiPendingError(result.video_api_pending);
    return result;
  } catch (error) {
    if (error?.code !== "VIDEO_API_PENDING") throw error;
    return deferVideoApiJob({ job, token, requestId: error.requestId, delayMs });
  }
}
