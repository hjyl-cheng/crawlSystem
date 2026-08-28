import assert from "node:assert/strict";
import test from "node:test";
import { applyMigrationActivityGate } from "../src/migrationActivityGate.js";

function clientFixture({
  recent = 0,
  uncertain = 0,
  configured = {},
  initialActivityEvidence = { evidence_complete: true },
  activityMetrics = null,
  pendingAbout = null,
  evidenceRows = null,
} = {}) {
  const queries = [];
  const run = {
    run_id: "run:gate",
    channel_id: "UCgate",
    candidate_id: 42,
    started_at: "2026-07-21T23:59:00Z",
    channel_status: "active",
    dormant_cycle: 0,
    result_json: {
      dispatch_batch_id: "migration:batch",
      migration_activity_gate: {
        required: true,
        decision: "pending",
        max_age_days: 90,
        ...configured,
      },
      ...(initialActivityEvidence == null
        ? {}
        : { migration_activity_initial_evidence: initialActivityEvidence }),
      ...(activityMetrics == null
        ? {}
        : { migration_activity_metrics: activityMetrics }),
      ...(pendingAbout == null
        ? {}
        : { pending_initial_about_observation: pendingAbout }),
    },
  };
  return {
    queries,
    client: {
      async query(sql, params = []) {
        queries.push({ sql, params });
        if (sql.includes("FOR UPDATE OF run,channel")) return { rows: [run] };
        if (sql.includes("SELECT candidate.source_content_id")) {
          return {
            rows: evidenceRows ?? [
              ...Array.from({ length: recent }, (_, index) => ({
                source_content_id: `recent-${index}`,
                detail_status: "done",
                content_key: `UCgate:video:recent-${index}`,
                scope_status: null,
                content_type: "video",
                published_at: "2026-07-01T00:00:00Z",
                published_at_status: "exact",
                published_at_precision: "second",
                published_at_source: "test_detail",
              })),
              ...Array.from({ length: uncertain }, (_, index) => ({
                source_content_id: `uncertain-${index}`,
                detail_status: "unavailable",
                content_key: null,
                scope_status: null,
              })),
            ],
          };
        }
        if (sql.includes("INSERT INTO crawler.crawl_observation_keys")) {
          return { rowCount: 1, rows: [{ observation_id: params[1] }] };
        }
        if (sql.includes("INSERT INTO crawler.channel_domain_cursors")) {
          return { rowCount: 1, rows: [] };
        }
        if (sql.includes("SELECT * FROM crawler.channel_domain_cursors")) {
          return { rowCount: 1, rows: [{ latest_sequence: 0 }] };
        }
        return { rowCount: 1, rows: [] };
      },
    },
  };
}

test("a Live in progress remains uncertain even though its candidate scope is excluded", async () => {
  const fixture = clientFixture({
    evidenceRows: [{
      source_content_id: "live-now",
      detail_status: "done",
      content_key: null,
      scope_status: "excluded",
      scope_reason: "live_in_progress",
      content_type: "live",
    }],
  });

  const result = await applyMigrationActivityGate(fixture.client, {
    runId: "run:gate",
    detailStatus: "done",
    evaluatedAt: "2026-07-21T23:59:00.000Z",
  });

  assert.equal(result.decision, "inconclusive");
  assert.equal(result.uncertainContentCount, 1);
  assert.equal(
    fixture.queries.some(({ sql }) => sql.includes("SET status='dormant'")),
    false,
  );
});

test("an incomplete Uploads scan cannot become dormant after old details resolve", async () => {
  const fixture = clientFixture({
    initialActivityEvidence: {
      evidence_complete: false,
      recent_published_content_count: 0,
      uncertain_content_count: 0,
    },
    evidenceRows: [{
      source_content_id: "old-video",
      detail_status: "done",
      content_key: "UCgate:video:old-video",
      scope_status: null,
      content_type: "video",
      published_at: "2026-01-01T00:00:00Z",
      published_at_status: "exact",
      published_at_precision: "second",
      published_at_source: "test_detail",
    }],
  });

  const result = await applyMigrationActivityGate(fixture.client, {
    runId: "run:gate",
    detailStatus: "done",
    evaluatedAt: "2026-07-22T00:01:00Z",
  });

  assert.equal(result.decision, "inconclusive");
  assert.equal(result.dormant, false);
  assert.equal(
    fixture.queries.some(({ sql }) => sql.includes("SET status='dormant'")),
    false,
  );
  const runUpdate = fixture.queries.find(({ sql }) => (
    sql.includes("UPDATE crawler.channel_runs") && sql.includes("migration_activity_gate")
  ));
  const stored = JSON.parse(runUpdate.params[1]);
  assert.equal(stored.evidence_complete, false);
  assert.equal(stored.relation_counts.outside, 1);
});

test("an age-excluded Candidate contributes its saved outside relation", async () => {
  const fixture = clientFixture({
    evidenceRows: [{
      source_content_id: "old-video",
      detail_status: "done",
      content_key: null,
      scope_status: "excluded",
      scope_reason: "older_than_max_age",
      scope_relation: "outside",
      content_type: "video",
    }],
  });

  const result = await applyMigrationActivityGate(fixture.client, {
    runId: "run:gate",
    detailStatus: "done",
    evaluatedAt: "2026-07-22T00:01:00Z",
  });

  assert.equal(result.decision, "dormant");
  const runUpdate = fixture.queries.find(({ sql }) => (
    sql.includes("UPDATE crawler.channel_runs") && sql.includes("migration_activity_gate")
  ));
  const stored = JSON.parse(runUpdate.params[1]);
  assert.equal(stored.relation_counts.outside, 1);
});

test("migration activity gate activates a Channel and records the UTC reference day", async () => {
  const fixture = clientFixture({
    recent: 1,
    pendingAbout: { channelId: "UCgate", runId: "run:gate" },
  });
  const result = await applyMigrationActivityGate(fixture.client, {
    runId: "run:gate",
    detailStatus: "done",
    evaluatedAt: "2026-07-22T00:01:00Z",
  });

  assert.equal(result.decision, "passed");
  assert.equal(result.dispatchBatchId, "migration:batch");
  assert.equal(fixture.queries.some(({ sql }) => sql.includes("SET status='active'")), true);
  assert.equal(fixture.queries.some(({ sql }) => sql.includes("SET status='rejected'")), false);
  const runUpdate = fixture.queries.find(({ sql }) => (
    sql.includes("UPDATE crawler.channel_runs") && sql.includes("migration_activity_gate")
  ));
  const stored = JSON.parse(runUpdate.params[1]);
  assert.equal(stored.reference_day, "2026-07-21");
  assert.equal(stored.reference_at, "2026-07-21T23:59:00.000Z");
  assert.equal(stored.decision, "passed");
  assert.equal(stored.classifier_version, "publication-time-evidence-v1");
  assert.equal(stored.policy_version, "migration-activity-v2");
  assert.deepEqual(stored.unresolved_by_status_counts, {
    relative: 0,
    estimated: 0,
    unavailable: 0,
    unresolved: 0,
  });
  const evidenceQuery = fixture.queries.find(({ sql }) => sql.includes("SELECT candidate.source_content_id"));
  assert.doesNotMatch(evidenceQuery.sql, /BETWEEN|interval '1 day'/);
  assert.doesNotMatch(runUpdate.sql, /pending_initial_about_observation/);
  assert.equal(
    fixture.queries.some(({ sql }) => sql.includes("INSERT INTO crawler.crawl_observations")),
    false,
  );
});

test("migration activity gate records one Channel-level dormant reversal after detail", async () => {
  const fixture = clientFixture({
    recent: 1,
    initialActivityEvidence: {
      evidence_complete: true,
      recent_published_content_count: 0,
      uncertain_content_count: 1,
    },
    activityMetrics: {
      details_requested_due_to_unresolved_count: 1,
    },
  });

  const result = await applyMigrationActivityGate(fixture.client, {
    runId: "run:gate",
    detailStatus: "done",
    evaluatedAt: "2026-07-22T00:01:00Z",
  });

  assert.equal(result.decision, "passed");
  assert.equal(result.dormantReversedAfterDetailCount, 1);
  const runUpdate = fixture.queries.find(({ sql }) => (
    sql.includes("UPDATE crawler.channel_runs") && sql.includes("migration_activity_gate")
  ));
  const stored = JSON.parse(runUpdate.params[1]);
  assert.equal(stored.details_requested_due_to_unresolved_count, 1);
  assert.equal(stored.dormant_reversed_after_detail_count, 1);
  assert.match(runUpdate.sql, /migration_activity_metrics/);
  assert.match(runUpdate.sql, /dormant_reversed_after_detail_count/);
});

test("migration activity gate admits the Channel as dormant and preserves About for Finalize", async () => {
  const fixture = clientFixture({
    pendingAbout: { channelId: "UCgate", runId: "run:gate" },
  });
  const result = await applyMigrationActivityGate(fixture.client, {
    runId: "run:gate",
    detailStatus: "done",
    evaluatedAt: "2026-07-21T23:59:30Z",
  });

  assert.equal(result.decision, "dormant");
  assert.equal(result.reason, "no_published_content_within_90_days");
  assert.match(result.dormantRecheckDay, /^2026-\d{2}-\d{2}$/);
  assert.equal(fixture.queries.some(({ sql }) => sql.includes("SET status='dormant'")), true);
  assert.equal(fixture.queries.some(({ sql }) => (
    sql.includes("UPDATE crawler.channel_candidates") && sql.includes("status='accepted'")
  )), true);
  assert.equal(fixture.queries.some(({ sql }) => (
    sql.includes("UPDATE crawler.channel_runs") && sql.includes("SET status='done'")
  )), true);
  assert.equal(fixture.queries.some(({ sql }) => sql.includes("ready_for_agent=false")), true);
  assert.equal(fixture.queries.some(({ sql }) => sql.includes("INSERT INTO crawler.crawl_observations")), true);
  const runUpdate = fixture.queries.find(({ sql }) => (
    sql.includes("UPDATE crawler.channel_runs") && sql.includes("initial_video_observation_id")
  ));
  assert.doesNotMatch(runUpdate.sql, /result_json-'pending_initial_about_observation'/);
  const outboxInsert = fixture.queries.find(({ sql }) => sql.includes("INSERT INTO crawler.crawler_outbox"));
  const event = JSON.parse(outboxInsert.params[7]);
  assert.equal(event.payload.activity.lifecycle_status, "dormant");
  assert.equal(event.payload.activity.recent_published_content_count, 0);
});

test("complete Uploads evidence ends migration without querying or storing video details", async () => {
  const fixture = clientFixture();
  const result = await applyMigrationActivityGate(fixture.client, {
    runId: "run:gate",
    detailStatus: "done",
    evaluatedAt: "2026-07-22T00:01:00Z",
    activityEvidence: {
      complete: true,
      source: "uploads_publication_dates",
      referenceDay: "2026-07-22",
      referenceAt: "2026-07-22T00:01:00Z",
      recentPublishedContentCount: 0,
      uncertainContentCount: 0,
      inspectedContentCount: 30,
      excludedUpcomingCount: 1,
      newestPublishedDay: "2026-03-01",
    },
  });

  assert.equal(result.decision, "dormant");
  assert.equal(fixture.queries.some(({ sql }) => sql.includes("FROM crawler.content_candidates candidate")), false);
  const runUpdate = fixture.queries.find(({ sql }) => (
    sql.includes("UPDATE crawler.channel_runs") && sql.includes("initial_video_observation_id")
  ));
  const stored = JSON.parse(runUpdate.params[1]);
  assert.equal(stored.reference_day, "2026-07-22");
  assert.equal(stored.reference_at, "2026-07-22T00:01:00.000Z");
  assert.equal(stored.evidence_source, "uploads_publication_dates");
  assert.equal(stored.inspected_content_count, 30);
  assert.equal(stored.excluded_upcoming_count, 1);
  assert.equal(stored.newest_published_day, "2026-03-01");
  assert.equal(stored.classifier_version, "publication-time-evidence-v1");
  assert.equal(stored.policy_version, "migration-activity-v2");
});

test("a persisted terminal migration decision is returned without writing again", async () => {
  const fixture = clientFixture({
    configured: {
      decision: "inconclusive",
      recent_published_content_count: 0,
      uncertain_content_count: 1,
    },
  });
  const result = await applyMigrationActivityGate(fixture.client, {
    runId: "run:gate",
    detailStatus: "done",
  });

  assert.equal(result.decision, "inconclusive");
  assert.equal(result.activate, true);
  assert.equal(fixture.queries.length, 1);
});
