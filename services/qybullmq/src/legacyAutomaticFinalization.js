import { sharedCrawlerSchedulerActivationAdmission } from "./migrationSystemRetryAdmission.js";
import { normalizeQueryScheduler, QUERY_SCHEDULER_KEY } from "./queryScheduler.js";

export async function resumeLegacyAutomaticFinalizationWithFence({
  scheduler,
  withTransaction,
  now = new Date().toISOString(),
} = {}) {
  if (typeof withTransaction !== "function") throw new TypeError("withTransaction is required");
  if (scheduler?.status !== "stopped" || scheduler?.stop_reason !== "no_schedulable_query") {
    return Object.freeze({ scheduler, resumed: false, admission: null });
  }
  const result = await withTransaction(async (client) => {
    const schedulerRows = await client.query(
      `SELECT value_json
       FROM crawler.settings
       WHERE setting_key=$1
       FOR UPDATE`,
      [QUERY_SCHEDULER_KEY],
    );
    if (schedulerRows.rows.length !== 1) {
      return { scheduler, resumed: false, admission: null };
    }
    const lockedScheduler = normalizeQueryScheduler(schedulerRows.rows[0].value_json);
    if (lockedScheduler.status !== "stopped"
        || lockedScheduler.stop_reason !== "no_schedulable_query") {
      return { scheduler: lockedScheduler, resumed: false, admission: null };
    }
    const admission = await sharedCrawlerSchedulerActivationAdmission(client);
    if (!admission.allowed) {
      return { scheduler: lockedScheduler, resumed: false, admission };
    }
    const patch = {
      status: "finishing",
      stopped_at: null,
      stop_reason: "upstream_drained",
      updated_at: now,
      updated_by: "controller",
    };
    const updated = await client.query(
      `UPDATE crawler.settings
       SET value_json=value_json || $2::jsonb,updated_at=now()
       WHERE setting_key=$1
       RETURNING value_json`,
      [QUERY_SCHEDULER_KEY, JSON.stringify(patch)],
    );
    return {
      scheduler: normalizeQueryScheduler(updated.rows[0]?.value_json ?? lockedScheduler),
      resumed: updated.rowCount === 1,
      admission: null,
    };
  });
  return Object.freeze(result);
}
