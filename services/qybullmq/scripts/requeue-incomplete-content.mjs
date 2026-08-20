import { Pool } from "pg";
import {
  CONTENT_COMPLETENESS_REPAIR_VERSION,
  enqueueContentRepairTargets,
  loadContentRepairTargets,
  prepareContentRepairTargets,
  reconcileLiveDurationNotApplicable,
} from "../src/contentRepair.js";
import { closeQueues, createQueues } from "../src/queues.js";

const apply = process.argv.includes("--apply");
const retryEnqueued = process.argv.includes("--retry-enqueued");
const limitRunsArg = process.argv.find((arg) => arg.startsWith("--limit-runs="));
const limitRuns = Math.max(0, Number.parseInt(limitRunsArg?.split("=")[1] ?? "0", 10) || 0);
const batchId = `repair-${Date.now()}`;
const db = new Pool({
  host: process.env.POSTGRES_HOST || "127.0.0.1",
  port: Number(process.env.POSTGRES_PORT || 5432),
  user: process.env.POSTGRES_USER || "bullmq",
  password: process.env.POSTGRES_PASSWORD || "bullmq",
  database: process.env.POSTGRES_DB || "bullmq_crawler",
  max: 2,
});
const queues = createQueues();
const client = await db.connect();
const dbQuery = (text, params = []) => client.query(text, params);

try {
  if (apply) {
    await client.query("SELECT pg_advisory_lock(hashtext('crawler-requeue-incomplete-content'))");
    await client.query("BEGIN");
    await reconcileLiveDurationNotApplicable(dbQuery);
  }
  const targets = await loadContentRepairTargets(dbQuery, {
    retryEnqueued,
    limitRuns,
  });
  const summary = {
    apply,
    batch_id: batchId,
    repair_version: CONTENT_COMPLETENESS_REPAIR_VERSION,
    retry_enqueued: retryEnqueued,
    limit_runs: limitRuns,
    detail_candidates: targets.detailRows.length,
    detail_runs: targets.detailRuns.length,
    channel_runs: targets.channelRuns.length,
    stale_runs: targets.staleRuns.length,
  };
  if (apply) {
    await prepareContentRepairTargets(dbQuery, targets, {
      batchId,
    });
    await client.query("COMMIT");
    summary.enqueued = await enqueueContentRepairTargets(dbQuery, queues, targets, {
      batchId,
      retryEnqueued,
    });
  }
  console.log(JSON.stringify(summary));
} catch (error) {
  if (apply) await client.query("ROLLBACK").catch(() => {});
  throw error;
} finally {
  if (apply) {
    await client.query("SELECT pg_advisory_unlock(hashtext('crawler-requeue-incomplete-content'))").catch(() => {});
  }
  client.release();
  await db.end();
  await closeQueues(queues);
}
