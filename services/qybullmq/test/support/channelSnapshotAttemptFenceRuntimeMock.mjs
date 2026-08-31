function state() {
  const value = globalThis.__channelSnapshotAttemptFenceState;
  if (!value) throw new Error("channel Snapshot attempt Fence test state is missing");
  return value;
}

function uploads(videoId, { publishedAt = null, evidenceComplete = false } = {}) {
  return {
    playlist_id: "UUstalledSnapshotFence",
    entries: [{
      video_id: videoId,
      position: 1,
      title: videoId,
      url: `https://www.youtube.com/watch?v=${videoId}`,
      thumbnail_url: `https://i.ytimg.com/vi/${videoId}/hqdefault.jpg`,
      published_at: publishedAt,
      published_at_status: publishedAt == null ? "unresolved" : "exact",
      published_at_precision: publishedAt == null ? "unknown" : "second",
      published_at_source: publishedAt == null ? null : "youtubejs_microformat",
    }],
    untyped_ids: [],
    tab_counts: { videos: 1, shorts: 0, live: 0 },
    scan: { stop_reason: "test_boundary" },
    raw: { source: "channel_snapshot_attempt_fence_test" },
    activity_evidence_complete: evidenceComplete,
  };
}

export async function fetchChannelInitial() {
  throw new Error("legacy channel fetch must not run in the Snapshot Fence test");
}

export async function fetchChannelDataApiDetails() {
  return null;
}

export async function fetchChannelUploads() {
  throw new Error("yt-dlp Uploads fetch must not run in the Snapshot Fence test");
}

export async function fetchChannelYtDlpMetadata() {
  throw new Error("yt-dlp metadata fetch must not run in the Snapshot Fence test");
}

export async function fetchVideoDataApiDetails() {
  return null;
}

export async function fetchVideoCommentThreadsDataApi() {
  return null;
}

export async function fetchVideoYtDlpDetail() {
  return null;
}

export function parseChannelHeader() {
  return {};
}

export function youtubeJsChannelEnabled() {
  return true;
}

export function youtubeJsDetailEnabled() {
  return false;
}

export async function openYoutubeJsChannel(channelId) {
  return {
    metadata: {
      channel_id: channelId,
      channel_url: `https://www.youtube.com/channel/${channelId}`,
      title: "Stalled Snapshot Fence",
      handle: "@stalledSnapshotFence",
      description: "Complete metadata keeps this test on the YouTube.js path.",
      country: "Brazil",
      subscriber_count: 10_000,
      subscriber_count_text: "10000",
      subscriber_count_source: "youtube_about",
      total_view_count: 1_000_000,
      view_count_text: "1000000",
      view_count_source: "youtube_about",
      total_video_count: 2,
      video_count_text: "2",
      video_count_source: "youtube_about",
    },
    about_observed: true,
    about_error: null,
    raw: { engine: "snapshot-fence-test" },
    fetchContents: async () => {
      const current = state();
      current.fetchCount += 1;
      if (current.fetchCount === 1) {
        current.firstFetchStarted.resolve();
        await current.releaseFirstFetch.promise;
        return current.scenario === "dormant"
          ? uploads("stale-attempt-old-video", {
              publishedAt: "2000-01-01T00:00:00.000Z",
              evidenceComplete: true,
            })
          : uploads("stale-attempt-video");
      }
      if (current.fetchCount === 2) {
        current.secondFetchStarted.resolve();
        await current.releaseSecondFetch.promise;
        return current.scenario === "dormant"
          ? uploads("current-attempt-recent-video", {
              publishedAt: new Date(Date.now() - 86_400_000).toISOString(),
              evidenceComplete: true,
            })
          : uploads("current-attempt-video");
      }
      throw new Error(`unexpected Uploads fetch number: ${current.fetchCount}`);
    },
  };
}

export async function fetchYoutubeJsVideoDetail() {
  return null;
}

export async function fetchYoutubeJsCommentFirstPage() {
  return null;
}
