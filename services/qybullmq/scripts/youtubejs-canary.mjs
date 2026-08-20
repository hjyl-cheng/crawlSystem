#!/usr/bin/env node

const proxyControlUrl = String(
  process.env.ROTA_PROXY_CONTROL_URL
  || process.env.PROXY_RECONCILER_URL
  || "http://rota-core:8001/api/v1/proxy-control",
).replace(/\/+$/, "");
const rotaBaseUrl = String(process.env.ROTA_PROXY_BASE_URL || "http://youtube-rota-qy-core:8000");
const rotaPassword = String(process.env.ROTA_BULLMQ_PROXY_PASSWORD || "");
const workerId = String(process.env.PROXY_WORKER_ID || `youtubejs-canary-${process.pid}`);
const channelIds = process.argv.slice(2).filter((value) => value.startsWith("UC"));
const channels = channelIds.length > 0
  ? channelIds
  : ["UCnum4N1hZ7DQPRfkw6DtAwg", "UC-DeVjPmfJsjfcmdxoKqMug"];
const videos = channelIds.length > 0
  ? []
  : ["OLlQdup2a-w", "Cg4FvuCLtxg", "nx-zpFc-Avo", "0DQFfGkbZXI"];

async function claimProxy() {
  if (process.env.YOUTUBE_PROXY_URL) return { managed: false, proxy: null };
  const fixedProxyUser = String(process.env.YOUTUBE_PROXY_USER || "").trim();
  if (fixedProxyUser && rotaPassword) {
    const proxyUrl = new URL(rotaBaseUrl);
    proxyUrl.username = fixedProxyUser;
    proxyUrl.password = rotaPassword;
    process.env.YOUTUBE_PROXY_URL = proxyUrl.toString();
    return { managed: false, proxy: null };
  }
  if (!rotaPassword) return { managed: false, proxy: null };
  const response = await fetch(`${proxyControlUrl}/claim`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({ role: "channel", worker_id: workerId }),
  });
  const result = await response.json();
  if (!response.ok || !result?.ready || !result?.proxy_user) {
    throw new Error(`proxy claim failed: HTTP ${response.status} ${JSON.stringify(result)}`);
  }
  const proxyUrl = new URL(rotaBaseUrl);
  proxyUrl.username = String(result.proxy_user);
  proxyUrl.password = rotaPassword;
  process.env.YOUTUBE_PROXY_URL = proxyUrl.toString();
  return { managed: true, proxy: result };
}

async function releaseProxy(claim) {
  if (!claim.managed) return;
  await fetch(`${proxyControlUrl}/release`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({ worker_id: workerId }),
  }).catch(() => null);
}

const claim = await claimProxy();
const {
  openYoutubeJsChannel,
  fetchYoutubeJsVideoDetail,
  youtubeJsState,
} = await import("../src/youtubeJs.js");
const { ChannelExecutionRuntime } = await import("../src/channelExecutionRuntime.js");
const { closeDb } = await import("../src/db.js");

const startedAt = Date.now();
const output = { proxy_id: claim.proxy?.proxy_id ?? null, channels: [], videos: [], errors: [] };
const runtime = new ChannelExecutionRuntime();
try {
  if (!claim.proxy) throw new Error("Canary requires a managed qy proxy claim");
  const execution = await runtime.run({
    job: {
      id: `youtubejs-canary-${Date.now()}`,
      queueName: "youtube-channel-crawl",
      attemptsMade: 0,
      data: { channel_id: channels[0], run_id: null },
    },
    proxy: claim.proxy,
    getProxySnapshot: () => claim.proxy,
    proxyUrl: process.env.YOUTUBE_PROXY_URL,
    workerId,
    language: process.env.YOUTUBE_CONTROL_LANGUAGE || process.env.YOUTUBE_LANGUAGE || "en",
    country: process.env.YOUTUBE_COUNTRY || "BR",
    timezone: process.env.BROWSER_PROFILE_TIMEZONE || process.env.TZ || "UTC",
  }, async () => {
    for (const channelId of channels) {
      const channelStartedAt = Date.now();
      try {
        const channel = await openYoutubeJsChannel(channelId);
        const bundle = await channel.fetchContents(30);
        output.channels.push({
          channel_id: channelId,
          elapsed_ms: Date.now() - channelStartedAt,
          metadata: channel.metadata,
          count: bundle.entries.length,
          request_count: bundle.raw?.request_count ?? null,
          ids: bundle.entries.map((entry) => entry.video_id),
          types: bundle.entries.map((entry) => entry.content_type),
          type_sources: bundle.entries.map((entry) => entry.type_source),
          urls: bundle.entries.map((entry) => entry.url),
          tab_counts: bundle.tab_counts,
          untyped_ids: bundle.untyped_ids,
        });
      } catch (error) {
        output.errors.push({ scope: "channel", id: channelId, error: String(error?.message || error) });
      }
    }
    for (const videoId of videos) {
      const videoStartedAt = Date.now();
      try {
        const detail = await fetchYoutubeJsVideoDetail(videoId);
        output.videos.push({
          video_id: videoId,
          elapsed_ms: Date.now() - videoStartedAt,
          published_at: detail.published_at,
          published_at_precision: detail.published_at_precision,
          duration_seconds: detail.duration_seconds,
          view_count_text: detail.view_count_text,
          like_count: detail.like_count,
          comment_count: detail.comment_count,
          comments_disabled: detail.comments_disabled,
          availability: detail.availability,
          playability_status: detail.playability_status,
          playability_reason: detail.playability_reason,
          live_status: detail.live_status,
        });
      } catch (error) {
        output.errors.push({ scope: "video", id: videoId, error: String(error?.message || error) });
      }
    }
    output.runtime = youtubeJsState();
    return { channel_count: output.channels.length, video_count: output.videos.length };
  });
  output.execution = execution.execution;
} finally {
  await runtime.close();
  await closeDb();
  await releaseProxy(claim);
}
output.elapsed_ms = Date.now() - startedAt;
process.stdout.write(`${JSON.stringify(output, null, 2)}\n`);
if (output.errors.length > 0) process.exitCode = 1;
