import { parseArgs } from "node:util";
import { closeDb, query, withTransaction } from "../src/db.js";
import {
  applyLegacyDormantBackfill,
  loadLegacyDormantBackfillCandidates,
} from "../src/legacyDormantBackfill.js";

function optionsFromArgs() {
  const { values } = parseArgs({
    options: {
      apply: { type: "boolean", default: false },
      limit: { type: "string", default: "100000" },
      "channel-id": { type: "string" },
      "observed-at": { type: "string" },
      help: { type: "boolean", short: "h", default: false },
    },
    strict: true,
  });
  if (values.help) {
    console.log("Usage: node scripts/backfillLegacyDormantChannels.mjs [--apply] [--limit=N] [--channel-id=UC...] [--observed-at=ISO]");
    process.exit(0);
  }
  const limit = Number.parseInt(values.limit, 10);
  if (!Number.isSafeInteger(limit) || limit <= 0 || limit > 1000000) {
    throw new TypeError("limit must be an integer between 1 and 1000000");
  }
  const observedAt = values["observed-at"] ?? new Date().toISOString();
  if (Number.isNaN(new Date(observedAt).getTime())) {
    throw new TypeError("observed-at must be a timestamp");
  }
  return {
    apply: values.apply,
    limit,
    channelId: values["channel-id"] ?? null,
    observedAt: new Date(observedAt).toISOString(),
  };
}

const options = optionsFromArgs();
try {
  const candidates = await loadLegacyDormantBackfillCandidates({ query }, options);
  const summary = {
    apply: options.apply,
    observed_at: options.observedAt,
    candidate_count: candidates.length,
    first_channel_ids: candidates.slice(0, 20).map((row) => row.channel_id),
    applied_count: 0,
    skipped_count: 0,
  };
  if (options.apply) {
    for (const candidate of candidates) {
      const result = await withTransaction((client) => applyLegacyDormantBackfill(client, {
        channelId: candidate.channel_id,
        observedAt: options.observedAt,
      }));
      if (result.applied) summary.applied_count += 1;
      else summary.skipped_count += 1;
    }
  }
  console.log(JSON.stringify(summary));
} finally {
  await closeDb();
}
