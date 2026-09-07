const channelId = process.env.FULL_CRAWL_TEST_CHANNEL_ID;
const videoPrefix = process.env.FULL_CRAWL_TEST_VIDEO_PREFIX;
const publishedAt = process.env.FULL_CRAWL_TEST_PUBLISHED_AT;

async function requested(surface, videoId = null) {
  await new Promise((resolve, reject) => {
    process.send({ event: "youtube_request", surface, videoId }, (error) => (
      error ? reject(error) : resolve()
    ));
  });
  if (videoId && videoId === process.env.FULL_CRAWL_TEST_BLOCK_VIDEO_ID) {
    await new Promise(() => {});
  }
}

export async function openYoutubeJsChannel(requestedChannelId) {
  if (requestedChannelId !== channelId) throw new Error("Unexpected fixture Channel identity");
  await requested("channel");
  return {
    about_requested: true,
    about_observed: true,
    metadata: {
      channel_id: channelId,
      channel_url: `https://www.youtube.com/channel/${channelId}`,
      handle: `@${channelId}`,
      title: "Full Crawl restart fixture",
      country: "Brazil",
      subscriber_count: 10_000,
      subscriber_count_text: "10,000",
      subscriber_count_source: "youtube_about",
    },
    raw: { engine: "youtubejs", request_counts: { get_channel: 1, get_about: 1 } },
  };
}

export async function fetchYoutubeJsChannelUploads(requestedChannelId) {
  if (requestedChannelId !== channelId) throw new Error("Unexpected fixture Uploads identity");
  await requested("uploads");
  return {
    playlist_id: `UU${channelId.slice(2)}`,
    entries: Array.from({ length: 10 }, (_, index) => ({
      video_id: `${videoPrefix}${index + 1}`,
      position: index + 1,
      url: `https://www.youtube.com/watch?v=${videoPrefix}${index + 1}`,
      title: `Video ${index + 1}`,
      published_at: publishedAt,
      published_at_status: "exact",
      published_at_precision: "date_only",
      published_at_source: "youtubejs_player_microformat",
    })),
    activity_evidence_complete: true,
    scan: {
      complete: true,
      stop_reason: "limit",
      terminal_reason: "limit",
      pages: 1,
      inspected_count: 10,
      parse_gap_count: 0,
    },
  };
}

export async function fetchYoutubeJsVideoDetail(videoId) {
  if (!videoId.startsWith(videoPrefix)) throw new Error("Unexpected fixture Video identity");
  await requested("detail", videoId);
  return {
    id: videoId,
    title: `Detail ${videoId}`,
    published_at: publishedAt,
    published_at_status: "exact",
    published_at_precision: "date_only",
    published_at_source: "youtubejs_player_microformat",
    view_count_text: "100",
    duration_seconds: 60,
    access_status: "public",
    access_status_source: "youtubejs_player",
    playability_kind: "content",
    comments_disabled: true,
    content_type_signals: {
      source: "youtubei_player",
      canonical_url: `https://www.youtube.com/watch?v=${videoId}`,
      is_shorts_eligible: false,
      is_live_content: false,
      is_live: false,
      is_upcoming: false,
      is_live_now: false,
    },
  };
}
