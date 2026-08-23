export const LATEST_CONTENT_ENRICH_OPERATIONAL_SQL = `
  SELECT
    queues_json->'youtube-content-enrich'->'content_enrich_operational' AS operational,
    created_at
  FROM crawler.controller_ticks
  WHERE queues_json->'youtube-content-enrich'->'content_enrich_operational' IS NOT NULL
  ORDER BY tick_id DESC
  LIMIT 1
`;

function validDate(value, field) {
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) throw new TypeError(`${field} must be a valid date`);
  return date;
}

export async function loadLatestContentEnrichOperational(queryDb, {
  now = () => new Date(),
  staleAfterMs = 2 * 60_000,
} = {}) {
  if (typeof queryDb !== "function") throw new TypeError("queryDb is required");
  const result = await queryDb(LATEST_CONTENT_ENRICH_OPERATIONAL_SQL);
  const row = result.rows[0] ?? null;
  if (!row?.operational || !row?.created_at) {
    return { status: "unavailable", sampled_at: null, snapshot: null };
  }
  const sampledAt = validDate(row.created_at, "created_at");
  const observedAt = validDate(now(), "now");
  const maximumAgeMs = Math.max(1_000, Number(staleAfterMs) || 2 * 60_000);
  const stale = observedAt.getTime() - sampledAt.getTime() > maximumAgeMs;
  const activeAlerts = Array.isArray(row.operational?.alerts?.active)
    ? row.operational.alerts.active
    : [];
  return {
    status: stale ? "stale" : activeAlerts.length > 0 ? "alerting" : "ok",
    sampled_at: sampledAt.toISOString(),
    snapshot: row.operational,
  };
}
