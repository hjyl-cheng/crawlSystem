import assert from "node:assert/strict";
import test from "node:test";
import {
  ensureAutomaticPublicationOnboarding,
  PublicationChannelOnboardingConflict,
  reconcileAutomaticPublicationBacklog,
  reconcilePublicationAfterFullCrawl,
} from "../src/publicationChannelOnboarding.js";
import { completePublicationOperationalFixture } from "./support/publicationOperationalFixtures.js";

const STREAM_ID = "11111111-1111-4111-8111-111111111111";
const SECOND_STREAM_ID = "22222222-2222-4222-8222-222222222222";

function onboardingClient({
  streams = [STREAM_ID],
  existingOwner = null,
  promotion = true,
  initialPackageReady = true,
  missingPublicationDomain = null,
  channel = {},
} = {}) {
  const calls = [];
  let owner = typeof existingOwner === "string" ? {
    publication_stream_id: existingOwner,
    onboarding_mode: "baseline",
    seed_status: "complete",
    ownership_reference: {},
  } : existingOwner;
  const fixture = completePublicationOperationalFixture("UCnew");
  return {
    calls,
    async query(sql, params = []) {
      const text = String(sql);
      calls.push({ sql: text, params });
      if (text.includes("publication-auto-onboarding:transaction-guard") || text.startsWith("RELEASE SAVEPOINT")) {
        return { rows: [], rowCount: 0 };
      }
      if (text.includes("publication-auto-onboarding:find-owner")) {
        return { rows: owner ? [owner] : [], rowCount: owner ? 1 : 0 };
      }
      if (text.includes("publication-auto-onboarding:load-channel")) {
        return {
          rows: [{
            channel_id: "UCnew",
            status: "active",
            agent_status: "done",
            created_at: "2026-07-28T03:00:00.000Z",
            latest_run_id: "run:new",
            initial_full_run_id: promotion ? "run:new" : null,
            initial_candidate_id: promotion ? 42 : null,
            promotion_accepted_at: promotion ? "2026-07-28T03:01:00.000Z" : null,
            initial_full_run_finalized_status: promotion ? "ready_auto" : null,
            initial_full_run_finalized_at: promotion ? "2026-07-28T03:10:00.000Z" : null,
            current_finalized_run_id: promotion ? "run:new" : null,
            current_finalized_status: promotion ? "ready_auto" : null,
            current_finalized_at: promotion ? "2026-07-28T03:10:00.000Z" : null,
            ...channel,
          }],
          rowCount: 1,
        };
      }
      if (text.includes("publication-auto-onboarding:active-streams")) {
        return {
          rows: streams.map((stream) => (
            typeof stream === "string"
              ? {
                publication_stream_id: stream,
                source_identity_json: {},
                automatic_onboarding_destination: null,
                capture_enabled_at: "2026-07-28T02:00:00.000Z",
              }
              : {
                capture_enabled_at: "2026-07-28T02:00:00.000Z",
                source_identity_json: {},
                automatic_onboarding_destination: null,
                ...stream,
              }
          )),
          rowCount: streams.length,
        };
      }
      if (text.includes("publication-auto-onboarding:online-routes")) {
        return {
          rows: [{ route_count: 1, all_online: true, destinations: ["business"] }],
          rowCount: 1,
        };
      }
      if (text.includes("publication-readiness:sources")) {
        const availableSources = (initialPackageReady ? fixture.sources : fixture.sources.slice(0, 2))
          .filter((source) => source.observation_kind !== missingPublicationDomain);
        return {
          rows: availableSources,
          rowCount: availableSources.length,
        };
      }
      if (text.includes("publication-readiness:channels")) {
        return { rows: [{ row: fixture.channel }], rowCount: 1 };
      }
      if (text.includes("publication-readiness:contents")) {
        return { rows: fixture.contents, rowCount: fixture.contents.length };
      }
      if (text.includes("publication-readiness:agents")) {
        return initialPackageReady && missingPublicationDomain !== "agent"
          ? { rows: [{ row: fixture.agent, config: fixture.agentConfig }], rowCount: 1 }
          : { rows: [], rowCount: 0 };
      }
      if (text.includes("publication-auto-onboarding:insert-owner")) {
        owner = {
          publication_stream_id: params[0],
          onboarding_mode: "bootstrap",
          seed_status: "pending",
          ownership_reference: JSON.parse(params[2]),
        };
        return { rows: [{ publication_stream_id: owner.publication_stream_id, channel_id: params[1] }], rowCount: 1 };
      }
      if (text.includes("publication-auto-onboarding:insert-delivery")) {
        return { rows: [], rowCount: 1 };
      }
      if (text.includes("publication-auto-onboarding:verify-deliveries")) {
        return { rows: [{ destination: "business", mode: "online" }], rowCount: 1 };
      }
      if (text.includes("publication-auto-onboarding:repair-outbox")) {
        return { rows: [], rowCount: 0 };
      }
      if (text.includes("publication-auto-onboarding:record-source-gap")) {
        const pendingAutomaticOwner = owner?.onboarding_mode === "bootstrap"
          && owner?.seed_status === "pending"
          && owner?.ownership_reference?.onboarding_mode === "automatic_bootstrap"
          && owner?.ownership_reference?.initial_full_run_id === params[0];
        const sqlAllowsPendingAutomaticOwner = text.includes("owner.seed_status='pending'")
          && text.includes("ownership_reference->>'initial_full_run_id'");
        const recorded = owner == null
          || (pendingAutomaticOwner && sqlAllowsPendingAutomaticOwner);
        return {
          rows: recorded ? [{ run_id: params[0] }] : [],
          rowCount: recorded ? 1 : 0,
        };
      }
      throw new Error(`unexpected query: ${text}`);
    },
  };
}

test("a newly created Channel inherits the fully online Publication route exactly once", async () => {
  const client = onboardingClient();
  const result = await ensureAutomaticPublicationOnboarding(client, {
    channelId: "UCnew",
    runId: "run:new",
  });

  assert.deepEqual(result, {
    status: "registered",
    publication_stream_id: STREAM_ID,
    destinations: ["business"],
  });
  const streamLookup = client.calls.find((call) => call.sql.includes("active-streams"));
  assert.deepEqual(streamLookup.params, ["2026-07-28T03:01:00.000Z"]);
  assert.match(streamLookup.sql, /\$1::timestamptz>=capture_enabled_at/);
  assert.equal(client.calls.filter((call) => call.sql.includes("insert-owner")).length, 1);
  assert.equal(client.calls.filter((call) => call.sql.includes("insert-delivery")).length, 1);
});

test("the first Channel uses an explicit Stream route before any Channel delivery exists", async () => {
  const client = onboardingClient({
    streams: [{
      publication_stream_id: STREAM_ID,
      automatic_onboarding_destination: "business",
    }],
  });
  const inheritedRouteQuery = client.query.bind(client);
  client.query = async (sql, params = []) => {
    if (String(sql).includes("publication-auto-onboarding:online-routes")) {
      throw new Error("the first Channel cannot inherit a route from another Channel");
    }
    return inheritedRouteQuery(sql, params);
  };

  const result = await ensureAutomaticPublicationOnboarding(client, {
    channelId: "UCnew",
    runId: "run:new",
  });

  assert.deepEqual(result, {
    status: "registered",
    publication_stream_id: STREAM_ID,
    destinations: ["business"],
  });
});

test("a Full Crawl without accepted Candidate promotion evidence cannot auto-register a Channel", async () => {
  const client = onboardingClient({ promotion: false });
  const result = await ensureAutomaticPublicationOnboarding(client, {
    channelId: "UCnew",
    runId: "run:new",
  });

  assert.equal(result.status, "not_new_channel");
  assert.equal(client.calls.some((call) => call.sql.includes("active-streams")), false);
  assert.equal(client.calls.some((call) => call.sql.includes("insert-owner")), false);
});

test("a later finalized Full Run cannot stand in for the immutable Promotion Run", async () => {
  const client = onboardingClient({
    channel: {
      latest_run_id: "run:later",
      initial_full_run_id: "run:new",
    },
  });
  const result = await ensureAutomaticPublicationOnboarding(client, {
    channelId: "UCnew",
    runId: "run:later",
  });

  assert.equal(result.status, "not_initial_full_run");
  assert.equal(client.calls.some((call) => call.sql.includes("active-streams")), false);
  assert.equal(client.calls.some((call) => call.sql.includes("insert-owner")), false);
});

test("a proven Publication Gap child Repair may complete the immutable Initial Package", async () => {
  const client = onboardingClient({
    channel: {
      latest_run_id: "run:repair-child",
      initial_full_run_id: "run:new",
      current_finalized_run_id: "run:repair-child",
      current_finalized_status: "ready_auto",
      current_finalized_at: "2026-07-28T04:10:00.000Z",
      current_run_crawl_mode: "full",
      current_run_repair_parent_id: "run:new",
      current_run_repair_mode: "channel",
      current_run_publication_gap_status: "required",
      current_run_publication_gap_root_run_id: "run:new",
      promotion_publication_gap_status: "required",
    },
  });
  const result = await ensureAutomaticPublicationOnboarding(client, {
    channelId: "UCnew",
    runId: "run:repair-child",
    revisionType: "repair",
  });

  assert.equal(result.status, "registered");
  assert.equal(result.publication_stream_id, STREAM_ID);
});

test("a generic child Repair cannot borrow the Promotion Run Publication Gap", async () => {
  const client = onboardingClient({
    channel: {
      latest_run_id: "run:repair-child",
      initial_full_run_id: "run:new",
      current_finalized_run_id: "run:repair-child",
      current_finalized_status: "ready_auto",
      current_finalized_at: "2026-07-28T04:10:00.000Z",
      current_run_crawl_mode: "full",
      current_run_repair_parent_id: "run:new",
      current_run_repair_mode: "channel",
      current_run_publication_gap_status: null,
      current_run_publication_gap_root_run_id: null,
      promotion_publication_gap_status: "required",
    },
  });
  const result = await ensureAutomaticPublicationOnboarding(client, {
    channelId: "UCnew",
    runId: "run:repair-child",
    revisionType: "repair",
  });

  assert.equal(result.status, "not_initial_full_run");
  assert.equal(client.calls.some((call) => call.sql.includes("insert-owner")), false);
});

test("an ordinary later Full Run cannot use Publication Gap evidence", async () => {
  const client = onboardingClient({
    channel: {
      latest_run_id: "run:repair-child",
      initial_full_run_id: "run:new",
      current_finalized_run_id: "run:repair-child",
      current_finalized_status: "ready_auto",
      current_finalized_at: "2026-07-28T04:10:00.000Z",
      current_run_crawl_mode: "full",
      current_run_repair_parent_id: "run:new",
      current_run_repair_mode: "channel",
      promotion_publication_gap_status: "required",
    },
  });
  const result = await ensureAutomaticPublicationOnboarding(client, {
    channelId: "UCnew",
    runId: "run:repair-child",
    revisionType: "incremental",
  });

  assert.equal(result.status, "not_initial_full_run");
  assert.equal(client.calls.some((call) => call.sql.includes("insert-owner")), false);
});

test("the immutable Promotion Run must itself pass the Finalize gate", async () => {
  const client = onboardingClient({
    channel: {
      initial_full_run_finalized_status: null,
      initial_full_run_finalized_at: null,
    },
  });
  const result = await ensureAutomaticPublicationOnboarding(client, {
    channelId: "UCnew",
    runId: "run:new",
  });

  assert.equal(result.status, "initial_full_run_not_finalized");
  assert.equal(client.calls.some((call) => call.sql.includes("active-streams")), false);
  assert.equal(client.calls.some((call) => call.sql.includes("insert-owner")), false);
});

test("a ready_partial Promotion Run cannot borrow ready_auto from a later Full Run", async () => {
  const client = onboardingClient({
    channel: {
      latest_run_id: "run:later",
      initial_full_run_finalized_status: "ready_partial",
      initial_full_run_finalized_at: "2026-07-28T03:10:00.000Z",
      current_finalized_run_id: "run:later",
      current_finalized_status: "ready_auto",
      current_finalized_at: "2026-07-28T04:10:00.000Z",
    },
  });
  const result = await ensureAutomaticPublicationOnboarding(client, {
    channelId: "UCnew",
    runId: "run:new",
  });

  assert.equal(result.status, "initial_full_run_not_finalized");
  assert.equal(client.calls.some((call) => call.sql.includes("publication-readiness:")), false);
  assert.equal(client.calls.some((call) => call.sql.includes("insert-owner")), false);
});

test("automatic onboarding waits for an active complete Initial Package", async () => {
  const dormant = onboardingClient({
    channel: {
      status: "dormant",
      agent_status: "skipped",
      initial_full_run_finalized_status: "ready_partial",
      current_finalized_status: "ready_partial",
    },
  });
  const dormantResult = await ensureAutomaticPublicationOnboarding(dormant, {
    channelId: "UCnew",
    runId: "run:new",
  });
  assert.equal(dormantResult.status, "channel_not_eligible");

  const partial = onboardingClient({
    channel: { current_finalized_status: "ready_partial" },
  });
  const partialResult = await ensureAutomaticPublicationOnboarding(partial, {
    channelId: "UCnew",
    runId: "run:new",
  });
  assert.equal(partialResult.status, "initial_package_not_ready");
  assert.equal(partial.calls.some((call) => call.sql.includes("insert-owner")), false);

  const missingDomain = onboardingClient({ initialPackageReady: false });
  const missingDomainResult = await ensureAutomaticPublicationOnboarding(missingDomain, {
    channelId: "UCnew",
    runId: "run:new",
  });
  assert.equal(missingDomainResult.status, "initial_package_not_ready");
  assert.equal(
    missingDomainResult.domains.find((domain) => domain.domain === "agent").readiness_status,
    "not_ready",
  );
  assert.equal(missingDomain.calls.some((call) => call.sql.includes("insert-owner")), false);
});

test("incomplete Registry promotion evidence cannot auto-register a Channel", async () => {
  const client = onboardingClient();
  const query = client.query.bind(client);
  client.query = async (sql, params = []) => {
    if (String(sql).includes("publication-auto-onboarding:load-channel")) {
      client.calls.push({ sql: String(sql), params });
      return {
        rows: [{
          channel_id: "UCnew",
          status: "active",
          created_at: "2026-07-28T03:00:00.000Z",
          latest_run_id: "run:new",
          initial_full_run_id: null,
          initial_candidate_id: 42,
          promotion_accepted_at: "2026-07-28T03:01:00.000Z",
        }],
        rowCount: 1,
      };
    }
    return query(sql, params);
  };

  const result = await ensureAutomaticPublicationOnboarding(client, {
    channelId: "UCnew",
    runId: "run:new",
  });

  assert.equal(result.status, "not_new_channel");
  assert.equal(client.calls.some((call) => call.sql.includes("active-streams")), false);
});

test("an explicitly owned Channel bypasses automatic registration", async () => {
  const client = onboardingClient({ existingOwner: STREAM_ID });
  const result = await ensureAutomaticPublicationOnboarding(client, {
    channelId: "UCnew",
    runId: "run:new",
  });

  assert.equal(result.status, "existing");
  assert.equal(result.publication_stream_id, STREAM_ID);
  assert.equal(client.calls.filter((call) => call.sql.includes("find-owner")).length, 1);
  assert.equal(client.calls.some((call) => call.sql.includes("load-channel")), false);
});

test("a pending manual Bootstrap owner stays on the normal reconciliation path", async () => {
  const client = onboardingClient({
    existingOwner: {
      publication_stream_id: STREAM_ID,
      onboarding_mode: "bootstrap",
      seed_status: "pending",
      ownership_reference: { cohort_id: "manual-cutover" },
    },
  });
  const result = await ensureAutomaticPublicationOnboarding(client, {
    channelId: "UCnew",
    runId: "run:new",
  });

  assert.deepEqual(result, {
    status: "existing",
    publication_stream_id: STREAM_ID,
    destinations: null,
  });
  assert.equal(client.calls.some((call) => call.sql.includes("load-channel")), false);
});

test("an incomplete automatic owner resumes the immutable Initial Package", async () => {
  const client = onboardingClient({
    existingOwner: {
      publication_stream_id: STREAM_ID,
      onboarding_mode: "bootstrap",
      seed_status: "pending",
      ownership_reference: {
        onboarding_mode: "automatic_bootstrap",
        initial_full_run_id: "run:new",
      },
    },
  });
  const result = await ensureAutomaticPublicationOnboarding(client, {
    channelId: "UCnew",
    runId: "run:new",
  });

  assert.deepEqual(result, {
    status: "recovering",
    publication_stream_id: STREAM_ID,
    destinations: ["business"],
  });
  assert.equal(client.calls.some((call) => call.sql.includes("load-channel")), true);
  assert.equal(client.calls.some((call) => call.sql.includes("publication-readiness:")), true);
  assert.equal(client.calls.some((call) => call.sql.includes("insert-owner")), false);
});

test("an incomplete automatic owner records a recoverable Publication Gap", async () => {
  const client = onboardingClient({
    missingPublicationDomain: "about",
    existingOwner: {
      publication_stream_id: STREAM_ID,
      onboarding_mode: "bootstrap",
      seed_status: "pending",
      ownership_reference: {
        onboarding_mode: "automatic_bootstrap",
        initial_full_run_id: "run:new",
      },
    },
  });

  const result = await reconcilePublicationAfterFullCrawl(client, {
    channelId: "UCnew",
    runId: "run:new",
    asOf: "2026-07-28T03:10:00.000Z",
    revisionType: "incremental",
  });

  assert.equal(result.status, "not_owned");
  assert.equal(result.publication_gap_repair.recorded, true);
  assert.deepEqual(result.publication_gap_repair.domains, ["channel"]);
  const gapCall = client.calls.find((call) => call.sql.includes("record-source-gap"));
  assert.ok(gapCall);
  assert.match(gapCall.sql, /owner\.seed_status='pending'/);
  assert.match(gapCall.sql, /ownership_reference->>'initial_full_run_id'/);
});

test("a pending automatic owner for a different Promotion Run cannot record a gap", async () => {
  const client = onboardingClient({
    missingPublicationDomain: "about",
    existingOwner: {
      publication_stream_id: STREAM_ID,
      onboarding_mode: "bootstrap",
      seed_status: "pending",
      ownership_reference: {
        onboarding_mode: "automatic_bootstrap",
        initial_full_run_id: "run:other",
      },
    },
  });

  const result = await reconcilePublicationAfterFullCrawl(client, {
    channelId: "UCnew",
    runId: "run:new",
    asOf: "2026-07-28T03:10:00.000Z",
    revisionType: "incremental",
  });

  assert.equal(result.status, "not_owned");
  assert.equal(result.publication_gap_repair.recorded, false);
});

test("automatic onboarding refuses to guess between two online Streams", async () => {
  const client = onboardingClient({ streams: [STREAM_ID, SECOND_STREAM_ID] });
  await assert.rejects(
    ensureAutomaticPublicationOnboarding(client, { channelId: "UCnew", runId: "run:new" }),
    (error) => error instanceof PublicationChannelOnboardingConflict
      && /multiple online Publication Streams/.test(error.message),
  );
  assert.equal(client.calls.some((call) => call.sql.includes("insert-owner")), false);
});

test("automatic onboarding excludes a dead-letter Recovery Stream from new Channel ownership", async () => {
  const client = onboardingClient({
    streams: [
      STREAM_ID,
      {
        publication_stream_id: SECOND_STREAM_ID,
        source_identity_json: { stream_role: "dead_letter_recovery" },
      },
    ],
  });

  const result = await ensureAutomaticPublicationOnboarding(client, {
    channelId: "UCnew",
    runId: "run:new",
  });

  assert.equal(result.status, "registered");
  assert.equal(result.publication_stream_id, STREAM_ID);
  assert.equal(client.calls.filter((call) => call.sql.includes("online-routes")).length, 1);
});

test("the compensation scan is bounded and does nothing before an online route exists", async () => {
  const calls = [];
  const result = await reconcileAutomaticPublicationBacklog({
    limit: 17,
    query: async (sql, params) => {
      calls.push({ sql: String(sql), params });
      return { rows: [] };
    },
    withTransaction: async () => {
      throw new Error("transaction must not run for an empty backlog");
    },
  });

  assert.deepEqual(result, {
    scanned: 0,
    registered: 0,
    reconciled: 0,
    skipped: 0,
    failed: 0,
    failures: [],
  });
  assert.deepEqual(calls[0].params, [17]);
  assert.match(calls[0].sql, /promotion_candidate\.accepted_at>=stream\.capture_enabled_at/);
  assert.match(calls[0].sql, /channel\.registry_promotion_candidate_id/);
  assert.match(calls[0].sql, /channel\.registry_promotion_run_id/);
  assert.match(calls[0].sql, /channel\.registry_promotion_run_id AS run_id/);
  assert.match(
    calls[0].sql,
    /promotion_run\.publication_finalized_status='ready_auto'/,
  );
  assert.match(calls[0].sql, /current_finalized\.status='ready_auto'/);
  assert.match(calls[0].sql, /channel\.status='active'/);
  assert.match(calls[0].sql, /channel\.agent_status='done'/);
  assert.doesNotMatch(calls[0].sql, /channel\.latest_run_id AS run_id/);
  assert.match(calls[0].sql, /ORDER BY promotion_candidate\.accepted_at/);
  assert.match(calls[0].sql, /delivery\.mode<>'online'/);
});
