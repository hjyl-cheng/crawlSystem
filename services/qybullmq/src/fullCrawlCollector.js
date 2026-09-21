/**
 * The seam used by the shared Full Crawl lifecycle.
 *
 * A collector owns network collection for one stage. The lifecycle retains
 * business state, checkpoints, retry budgets and publication handoff. A
 * collector must not mutate crawler tables or manufacture a business run.
 */
export const FULL_CRAWL_COLLECTOR_STAGES = Object.freeze([
  "admission",
  "uploads",
  "detail",
]);

const REQUIRED_METHODS = Object.freeze([
  "collectAdmission",
  "collectUploads",
  "collectDetail",
]);

export function assertFullCrawlCollector(collector) {
  if (!collector || typeof collector !== "object" || Array.isArray(collector)) {
    throw new TypeError("Full Crawl Collector is required");
  }
  for (const method of REQUIRED_METHODS) {
    if (typeof collector[method] !== "function") {
      throw new TypeError(`Full Crawl Collector.${method} is required`);
    }
  }
  return collector;
}
