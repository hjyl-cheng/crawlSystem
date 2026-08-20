const DEFAULT_INCREMENTAL_QUEUE = "youtube-channel-incremental";

export class ChannelSlotExecutorClosedError extends Error {
  constructor() {
    super("channel slot executor is closed");
    this.name = "ChannelSlotExecutorClosedError";
  }
}

export class ChannelSlotExecutorPausedError extends Error {
  constructor(reason) {
    super(`channel slot executor paused: ${String(reason || "paused")}`);
    this.name = "ChannelSlotExecutorPausedError";
    this.reason = String(reason || "paused");
  }
}

export class ChannelSlotExecutor {
  constructor({
    incrementalQueue = DEFAULT_INCREMENTAL_QUEUE,
    fullMaxWaitMs = Number(process.env.CHANNEL_FULL_JOB_MAX_WAIT_MS || 120000),
    now = Date.now,
  } = {}) {
    this.active = null;
    this.pending = [];
    this.sequence = 0;
    this.incrementalQueue = incrementalQueue;
    this.fullMaxWaitMs = Math.max(0, Number(fullMaxWaitMs) || 0);
    this.now = now;
    this.pausedReason = null;
    this.rejectWhilePaused = false;
    this.closed = false;
    this.closePromise = null;
    this.resolveClose = null;
  }

  execute(job, executeJob) {
    if (this.closed) return Promise.reject(new ChannelSlotExecutorClosedError());
    if (this.pausedReason && this.rejectWhilePaused) {
      return Promise.reject(new ChannelSlotExecutorPausedError(this.pausedReason));
    }
    if (typeof executeJob !== "function") throw new TypeError("executeJob must be a function");
    return new Promise((resolve, reject) => {
      this.sequence += 1;
      this.pending.push({
        sequence: this.sequence,
        enqueuedAt: this.now(),
        job,
        execute: executeJob,
        resolve,
        reject,
      });
      this.#drain();
    });
  }

  run(job, executeJob) {
    return this.execute(job, executeJob);
  }

  pause(reason = "paused", { rejectWaiting = false } = {}) {
    if (this.closed) return this.state();
    this.pausedReason = String(reason || "paused");
    this.rejectWhilePaused = rejectWaiting === true;
    if (this.rejectWhilePaused) {
      const error = new ChannelSlotExecutorPausedError(this.pausedReason);
      for (const entry of this.pending.splice(0)) entry.reject(error);
    }
    return this.state();
  }

  resume() {
    if (this.closed) return this.state();
    this.pausedReason = null;
    this.rejectWhilePaused = false;
    this.#drain();
    return this.state();
  }

  close() {
    if (this.closePromise) return this.closePromise;
    this.closed = true;
    this.pausedReason = "closed";
    const error = new ChannelSlotExecutorClosedError();
    for (const entry of this.pending.splice(0)) entry.reject(error);
    if (!this.active) return Promise.resolve();
    this.closePromise = new Promise((resolve) => {
      this.resolveClose = resolve;
    });
    return this.closePromise;
  }

  state() {
    const oldestFull = this.pending
      .filter((entry) => entry.job?.queueName !== this.incrementalQueue)
      .reduce((oldest, entry) => Math.min(oldest, entry.enqueuedAt), Number.POSITIVE_INFINITY);
    return {
      active_job_id: this.active?.job?.id == null ? null : String(this.active.job.id),
      active_queue: this.active?.job?.queueName ?? null,
      waiting: this.pending.length,
      paused: this.pausedReason !== null,
      pause_reason: this.pausedReason,
      closed: this.closed,
      oldest_full_wait_ms: Number.isFinite(oldestFull) ? Math.max(0, this.now() - oldestFull) : 0,
    };
  }

  #nextIndex() {
    const overdueFullIndex = this.pending.findIndex((entry) => (
      entry.job?.queueName !== this.incrementalQueue
      && this.now() - entry.enqueuedAt >= this.fullMaxWaitMs
    ));
    if (overdueFullIndex >= 0) return overdueFullIndex;
    const incrementalIndex = this.pending.findIndex(
      (entry) => entry.job?.queueName === this.incrementalQueue,
    );
    return incrementalIndex >= 0 ? incrementalIndex : 0;
  }

  #drain() {
    if (this.closed || this.pausedReason || this.active || this.pending.length === 0) return;
    const [entry] = this.pending.splice(this.#nextIndex(), 1);
    this.active = entry;
    Promise.resolve()
      .then(entry.execute)
      .then(entry.resolve, entry.reject)
      .finally(() => {
        if (this.active === entry) this.active = null;
        if (this.closed) {
          this.resolveClose?.();
          this.resolveClose = null;
          return;
        }
        this.#drain();
      });
  }
}

let defaultExecutor = null;

export function channelSlotExecutor() {
  if (!defaultExecutor) defaultExecutor = new ChannelSlotExecutor();
  return defaultExecutor;
}
