import { safeJobId } from "./queues.js";

function requiredText(value, field) {
  const normalized = String(value ?? "").trim();
  if (!normalized) throw new TypeError(`${field} is required`);
  return normalized;
}

export function buildDemoChannelCrawlJob({
  pageId,
  channelId,
  pipelineCycleId = null,
} = {}) {
  const normalizedPageId = requiredText(pageId, "pageId");
  const normalizedChannelId = requiredText(channelId, "channelId");
  const normalizedPipelineCycleId = String(pipelineCycleId ?? "").trim() || null;
  return {
    name: "channel-crawl",
    data: {
      demo: true,
      dispatch_generation: 1,
      channel_id: normalizedChannelId,
      channel_url: `https://www.youtube.com/channel/${normalizedChannelId}`,
      crawl_mode: "full",
      full_intent_id: `discover-demo:${normalizedPageId}:${normalizedChannelId}`,
      pipeline_cycle_id: normalizedPipelineCycleId,
    },
    options: {
      jobId: safeJobId("channel-crawl", "demo", normalizedPageId, normalizedChannelId),
    },
  };
}
