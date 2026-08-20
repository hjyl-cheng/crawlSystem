import assert from "node:assert/strict";
import test from "node:test";
import {
  applyLegacyDormantBackfill,
  loadLegacyDormantBackfillCandidates,
} from "../src/legacyDormantBackfill.js";

function clientFixture({ eligible = true } = {}) {
  const queries = [];
  return {
    queries,
    client: {
      async query(sql, params = []) {
        queries.push({ sql, params });
        if (sql.includes("FOR UPDATE OF channel")) {
          return {
            rows: eligible
              ? [{
                  channel_id: "UClegacyzero",
                  latest_run_id: "run:legacyzero",
                  dormant_since: null,
                  dormant_cycle: 0,
                  run_started_at: "2026-07-18T06:00:00Z",
                  candidate_count: 15,
                }]
              : [],
          };
        }
        if (sql.includes("INSERT INTO crawler.crawl_observation_keys")) {
          return { rowCount: 1, rows: [{ observation_id: params[1] }] };
        }
        if (sql.includes("INSERT INTO crawler.channel_domain_cursors")) {
          return { rowCount: 1, rows: [] };
        }
        if (sql.includes("SELECT * FROM crawler.channel_domain_cursors")) {
          return { rowCount: 1, rows: [{ latest_sequence: 1 }] };
        }
        return { rowCount: 1, rows: [] };
      },
    },
  };
}

test("legacy dormant preview selects active and legacy rejected Channels with no stored content", async () => {
  const fixture = clientFixture();
  await loadLegacyDormantBackfillCandidates(fixture.client, { limit: 500 });

  const select = fixture.queries[0];
  assert.match(select.sql, /channel\.status='active'/);
  assert.match(select.sql, /channel\.status='rejected'/);
  assert.match(select.sql, /no_published_content_within_90_days/);
  assert.match(select.sql, /NOT EXISTS\s*\(\s*SELECT 1\s*FROM crawler\.contents/s);
  assert.equal(select.params[0], 500);
});

test("legacy zero-content Channel becomes dormant through a Video Observation", async () => {
  const fixture = clientFixture();
  const result = await applyLegacyDormantBackfill(fixture.client, {
    channelId: "UClegacyzero",
    observedAt: "2026-07-23T08:00:00Z",
  });

  assert.equal(result.applied, true);
  assert.equal(result.channelId, "UClegacyzero");
  assert.match(result.dormantRecheckDay, /^2026-\d{2}-\d{2}$/);
  assert.equal(fixture.queries.some(({ sql }) => (
    sql.includes("UPDATE crawler.channels") && sql.includes("status='dormant'")
  )), true);
  assert.equal(fixture.queries.some(({ sql }) => sql.includes("ready_for_agent=false")), true);
  const observationInsert = fixture.queries.find(({ sql }) => (
    sql.includes("INSERT INTO crawler.crawl_observations")
  ));
  assert.equal(observationInsert.params[8], "migration_baseline");
  const outboxInsert = fixture.queries.find(({ sql }) => (
    sql.includes("INSERT INTO crawler.crawler_outbox")
  ));
  const event = JSON.parse(outboxInsert.params[7]);
  assert.equal(event.payload.activity.lifecycle_status, "dormant");
  assert.equal(event.payload.activity.recent_published_content_count, 0);
  assert.equal(event.payload.discovery.payload.items, 0);
});

test("legacy dormant backfill rechecks eligibility under lock", async () => {
  const fixture = clientFixture({ eligible: false });
  const result = await applyLegacyDormantBackfill(fixture.client, {
    channelId: "UChascontent",
    observedAt: "2026-07-23T08:00:00Z",
  });

  assert.deepEqual(result, {
    applied: false,
    channelId: "UChascontent",
    reason: "not_eligible",
  });
  assert.equal(fixture.queries.some(({ sql }) => sql.includes("status='dormant'")), false);
});
