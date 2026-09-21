import { assertFullCrawlCollector } from "./fullCrawlCollector.js";

/**
 * Adapter for the existing in-process YouTubeJS collector.
 *
 * The adapter only translates the shared stage interface. It does not own
 * database state, retries, API continuation or business completion.
 */
export function createLocalFullCrawlCollector({ youtube } = {}) {
  if (!youtube || typeof youtube !== "object" || Array.isArray(youtube)
      || typeof youtube.fetchChannel !== "function"
      || typeof youtube.fetchUploads !== "function"
      || typeof youtube.fetchDetail !== "function") {
    throw new TypeError("Local Full Crawl YouTubeJS Adapter is required");
  }

  const collector = Object.freeze({
    collectAdmission(channelId, options) {
      return youtube.fetchChannel(channelId, options);
    },
    collectUploads(channelId, contentLimit, options) {
      return youtube.fetchUploads(channelId, contentLimit, options);
    },
    collectDetail(videoId, options) {
      return youtube.fetchDetail(videoId, options);
    },
  });
  return assertFullCrawlCollector(collector);
}
