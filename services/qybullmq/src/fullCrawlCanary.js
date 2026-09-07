import { Queue } from "bullmq";

export const FULL_CRAWL_CANARY_BATCH_PREFIX = "fullcrawl-youtubejs-canary-";

export function isFullCrawlCanaryBatch(value) {
  return typeof value === "string"
    && /^fullcrawl-youtubejs-canary-[A-Za-z0-9][A-Za-z0-9._-]*$/.test(value);
}

export function isFullCrawlCanaryPayload(data) {
  return isFullCrawlCanaryBatch(data?.dispatch_batch_id)
    || isFullCrawlCanaryBatch(data?.pipeline_cycle_id);
}

export function assertFullCrawlCanaryJob(name, data) {
  if (name !== "channel-snapshot"
      || !isFullCrawlCanaryBatch(data?.dispatch_batch_id)
      || data.pipeline_cycle_id !== data.dispatch_batch_id
      || data.reject_if_no_recent_content !== true
      || data.crawl_mode !== "full"
      || data.query_id != null) {
    throw new Error("Full Crawl canary accepts only an ordinary migration snapshot in its frozen batch");
  }
}

export function fullCrawlCanaryPrefix(prefix = "bull") {
  return `${prefix || "bull"}-fullcrawl-youtubejs-v1`;
}

export function assertFullCrawlWorkerLane(job, canary) {
  if (job.queueName !== "youtube-channel-crawl") return;
  if (canary !== isFullCrawlCanaryPayload(job.data)) {
    throw new Error("Full Crawl Job was delivered to the wrong Worker lane");
  }
  if (canary) assertFullCrawlCanaryJob(job.name, job.data);
}

export function fullCrawlWorkerPrefix({ prefix, enabledQueues, canary = false }) {
  if (!canary) return prefix;
  if (enabledQueues.length !== 1 || enabledQueues[0] !== "youtube-channel-crawl") {
    throw new Error("Full Crawl canary Worker must consume only youtube-channel-crawl");
  }
  return fullCrawlCanaryPrefix(prefix);
}

export class FullCrawlRoutingQueue extends Queue {
  constructor(name, options) {
    super(name, options);
    this.canaryQueue = new Queue(name, {
      ...options,
      prefix: fullCrawlCanaryPrefix(options?.prefix),
    });
  }

  async add(name, data, options) {
    if (!isFullCrawlCanaryPayload(data)) return super.add(name, data, options);
    assertFullCrawlCanaryJob(name, data);
    return this.canaryQueue.add(name, data, options);
  }

  async addBulk(jobs) {
    const canary = jobs.filter((job) => isFullCrawlCanaryPayload(job.data));
    if (canary.length === 0) return super.addBulk(jobs);
    if (canary.length !== jobs.length) throw new Error("Cannot mix Full Crawl canary and ordinary bulk dispatch");
    for (const job of canary) assertFullCrawlCanaryJob(job.name, job.data);
    return this.canaryQueue.addBulk(jobs);
  }

  async getJob(id) {
    const [ordinary, canary] = await Promise.all([super.getJob(id), this.canaryQueue.getJob(id)]);
    if (ordinary && canary) throw new Error(`Full Crawl Job exists in both queue lanes: ${id}`);
    if ((ordinary && isFullCrawlCanaryPayload(ordinary.data))
        || (canary && !isFullCrawlCanaryPayload(canary.data))) {
      throw new Error(`Full Crawl Job is persisted in the wrong queue lane: ${id}`);
    }
    return canary ?? ordinary;
  }

  async getJobs(types, start = 0, end = -1, ascending = false) {
    const [ordinary, canary] = await Promise.all([
      super.getJobs(types, 0, end, ascending),
      this.canaryQueue.getJobs(types, 0, end, ascending),
    ]);
    if (canary.length === 0) return ordinary.slice(start);
    const jobs = [...ordinary, ...canary].sort((left, right) => (
      (ascending ? 1 : -1) * (left.timestamp - right.timestamp)
    ));
    return jobs.slice(start, end < 0 ? undefined : end + 1);
  }

  async getJobCounts(...types) {
    const [ordinary, canary] = await Promise.all([
      super.getJobCounts(...types), this.canaryQueue.getJobCounts(...types),
    ]);
    return Object.fromEntries([...new Set([...Object.keys(ordinary), ...Object.keys(canary)])]
      .map((type) => [type, (ordinary[type] ?? 0) + (canary[type] ?? 0)]));
  }

  async close() {
    const results = await Promise.allSettled([super.close(), this.canaryQueue.close()]);
    const errors = results.filter((result) => result.status === "rejected").map((result) => result.reason);
    if (errors.length) throw new AggregateError(errors, "Full Crawl queue lane shutdown failed");
  }
}
