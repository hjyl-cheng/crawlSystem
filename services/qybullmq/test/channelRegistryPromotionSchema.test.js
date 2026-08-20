import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import test from "node:test";
import { SUCCESSFUL_PUBLICATION_FINALIZE_STATUSES } from "../src/finalizePolicy.js";

test("Channel Registry promotion evidence is paired, unique, relational, and immutable", async () => {
  const schema = await readFile(new URL("../src/schema.sql", import.meta.url), "utf8");

  assert.match(schema, /ADD COLUMN IF NOT EXISTS registry_promotion_candidate_id BIGINT/);
  assert.match(schema, /ADD COLUMN IF NOT EXISTS registry_promotion_run_id TEXT/);
  assert.match(schema, /ADD COLUMN IF NOT EXISTS publication_finalized_status TEXT/);
  assert.match(schema, /ADD COLUMN IF NOT EXISTS publication_finalized_at TIMESTAMPTZ/);
  assert.match(schema, /channel_runs_publication_finalize_check/);
  assert.match(schema, /guard_channel_run_publication_finalize/);
  assert.match(schema, /Channel Run Publication Finalize evidence cannot regress/);
  assert.match(schema, /channels_registry_promotion_pair_check/);
  assert.match(schema, /channels_registry_promotion_candidate_fk/);
  assert.match(schema, /channels_registry_promotion_run_fk/);
  assert.match(
    schema,
    /FOREIGN KEY \(registry_promotion_candidate_id,channel_id\)[\s\S]*REFERENCES crawler\.channel_candidates\(candidate_id,channel_id\)/,
  );
  assert.match(
    schema,
    /FOREIGN KEY \(registry_promotion_run_id,channel_id,registry_promotion_candidate_id\)[\s\S]*REFERENCES crawler\.channel_runs\(run_id,channel_id,candidate_id\)/,
  );
  assert.match(schema, /channel_runs_candidate_channel_fk/);
  assert.match(schema, /ux_crawler_channels_registry_promotion_candidate/);
  assert.match(schema, /ux_crawler_channels_registry_promotion_run/);
  assert.match(schema, /Channel Registry promotion evidence is immutable/);
  assert.match(schema, /Channel Registry promotion Run must Finalize before a later Run/);
  const registryGuard = schema.match(
    /guard_channel_registry_promotion\(\)[\s\S]*?\$crawler_registry_guard\$;/,
  )?.[0] ?? "";
  assert.match(registryGuard, /AND NOT crawler\.registry_promotion_is_complete/);
  const publicationGapRepair = schema.match(
    /registry_publication_gap_repair_is_allowed\([\s\S]*?\$crawler_registry_publication_gap_repair\$;/,
  )?.[0] ?? "";
  assert.match(publicationGapRepair, /repair_run\.result_json#>>'\{publication_gap_repair,status\}'='required'/);
  assert.match(
    publicationGapRepair,
    /repair_run\.result_json#>>'\{publication_gap_repair,root_run_id\}'=promotion_run\.run_id/,
  );
  assert.match(
    publicationGapRepair,
    /repair_run\.result_json#>>'\{final_repair,parent_run_id\}'=promotion_run\.run_id/,
  );
  assert.match(publicationGapRepair, /source_run\.run_id=promotion_run\.run_id/);
  assert.match(publicationGapRepair, /source_run\.result_json#>>'\{publication_gap_repair,root_run_id\}'/);
  const promotionComplete = schema.match(
    /registry_promotion_is_complete\([\s\S]*?\$crawler_registry_promotion_complete\$;/,
  )?.[0] ?? "";
  assert.match(promotionComplete, /publication_finalized_status='ready_auto'/);
  assert.match(promotionComplete, /publication_finalized_at IS NOT NULL/);
  assert.match(promotionComplete, /owner\.seed_status='complete'/);
  assert.match(promotionComplete, /pending_owner\.seed_status='pending'/);
  assert.match(promotionComplete, /pending_owner\.ownership_reference->>'initial_full_run_id'=\$2/);
  assert.match(schema, /BEFORE INSERT OR UPDATE ON crawler\.channels/);
  assert.match(schema, /guard_channel_registry_promotion_candidate/);
  assert.match(schema, /trg_channel_registry_promotion_candidate/);
  assert.match(schema, /guard_channel_registry_promotion_run/);
  assert.match(schema, /trg_channel_registry_promotion_run/);
  assert.match(schema, /promotion Run identity is immutable and must be Full Crawl/);
  assert.match(schema, /OLD\.accepted_at IS NOT NULL/);
  assert.doesNotMatch(
    schema.match(/guard_channel_registry_promotion_candidate\(\)[\s\S]*?\$crawler_registry_candidate_guard\$;/)?.[0] ?? "",
    /channel\.status IN/,
  );

  const finalizeGuard = schema.match(
    /guard_channel_run_publication_finalize\(\)[\s\S]*?\$crawler_finalize_guard\$;/,
  )?.[0] ?? "";
  assert.doesNotMatch(finalizeGuard, /NEW\.status IS DISTINCT FROM 'done'/);
  assert.doesNotMatch(finalizeGuard, /NEW\.detail_status IS DISTINCT FROM 'done'/);
  assert.doesNotMatch(finalizeGuard, /NEW\.finished_at IS NULL/);

  const finalizeBackfill = schema.match(
    /UPDATE crawler\.channel_runs AS run[\s\S]*?FROM crawler\.finalized_profiles AS finalized[\s\S]*?;/,
  )?.[0] ?? "";
  assert.doesNotMatch(finalizeBackfill, /SET status='done'/);
  assert.doesNotMatch(finalizeBackfill, /detail_status='done'/);
  assert.match(finalizeBackfill, /run\.publication_finalized_status IS NULL/);
  assert.match(finalizeBackfill, /run\.publication_finalized_at IS NULL/);

  const finalizeConstraint = schema.match(
    /channel_runs_publication_finalize_check CHECK \(([\s\S]*?)\n\);/,
  )?.[1];
  assert.ok(finalizeConstraint);
  const schemaStatuses = [...finalizeConstraint.matchAll(/'(ready_[^']+)'/g)]
    .map((match) => match[1]);
  assert.deepEqual(
    [...new Set(schemaStatuses)].sort(),
    [...SUCCESSFUL_PUBLICATION_FINALIZE_STATUSES].sort(),
  );

  const evidenceMembershipClauses = [...schema.matchAll(
    /(?:publication_finalized_status|finalized\.status) (?:NOT )?IN \(([^)]+)\)/g,
  )].map((match) => [...match[1].matchAll(/'([^']+)'/g)].map((value) => value[1]));
  assert.ok(evidenceMembershipClauses.length >= 3);
  for (const statuses of evidenceMembershipClauses) {
    assert.deepEqual(
      [...new Set(statuses)].sort(),
      [...SUCCESSFUL_PUBLICATION_FINALIZE_STATUSES].sort(),
    );
  }
});
