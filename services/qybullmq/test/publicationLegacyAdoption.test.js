import assert from "node:assert/strict";
import { mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";
import { publicationLegacyAdoptionCommand } from "../scripts/managePublicationLegacyAdoption.mjs";
import {
  PublicationLegacyAdoptionAdministrator,
  buildPublicationLegacyAdoptionEvidence,
  buildPublicationLegacyAdoptionTarget,
  publicationLegacyAdoptionConfig,
  publicationLegacyAdoptionConfirmation,
  publicationLegacyAdoptionSummary,
  readPublicationLegacyAdoptionEvidence,
} from "../src/publicationLegacyAdoption.js";
import { completePublicationOperationalFixture } from "./support/publicationOperationalFixtures.js";

const STREAM_ID = "11111111-1111-4111-8111-111111111111";
const CHANNEL_ID = "UClegacyadoption";
const AS_OF = "2026-07-28T04:00:00.000Z";
const CAPTURE_ENABLED_AT = "2026-07-28T02:00:00.000Z";
const PROMOTION_ACCEPTED_AT = "2026-07-28T01:00:00.000Z";
const REGISTRY_CREATED_AT = "2026-07-28T00:30:00.000Z";

function environment(overrides = {}) {
  return {
    CRAWLER_DATABASE_URL: "postgres://crawler-admin@crawler/crawler_test",
    BUSINESS_DATABASE_URL: "postgres://business-admin@business/business_test",
    EXPECTED_CRAWLER_DATABASE: "crawler_test",
    EXPECTED_BUSINESS_DATABASE: "business_test",
    EXPECTED_CRAWLER_CHANNEL_COUNT: "1",
    EXPECTED_BUSINESS_CHANNEL_COUNT: "5",
    PUBLICATION_STREAM_ID: STREAM_ID,
    PUBLICATION_DESTINATION: "business",
    PUBLICATION_LEGACY_ADOPTION_KEY: "legacy-20260728-v1",
    PUBLICATION_LEGACY_ADOPTION_AS_OF: AS_OF,
    PUBLICATION_CHANNEL_IDS_FILE: "/tmp/legacy-channels.txt",
    PUBLICATION_OPERATOR: "publication-test",
    PUBLICATION_ACTION_REASON: "adopt finalized tracked channels without crawling",
    ...overrides,
  };
}

function fixture() {
  const config = publicationLegacyAdoptionConfig(environment());
  const target = buildPublicationLegacyAdoptionTarget(config, [CHANNEL_ID]);
  return { config, target };
}

function candidate(domain, ready, marker) {
  return {
    ready,
    result_hash: ready ? `sha256:${marker.repeat(64)}` : null,
    issues: ready ? [] : [{ domain, code: `${domain}_not_ready` }],
  };
}

function evidenceSource({
  channelReady = true,
  videoReady = true,
  agentReady = false,
  promotionAcceptedAt = PROMOTION_ACCEPTED_AT,
} = {}) {
  return {
    streams: [{ capture_enabled_at: CAPTURE_ENABLED_AT }],
    channels: [{
      channel_id: CHANNEL_ID,
      status: "active",
      created_at: REGISTRY_CREATED_AT,
      registry_promotion_candidate_id: "42",
      registry_promotion_run_id: "run:promotion",
      promotion_candidate_id: "42",
      promotion_status: "accepted",
      promotion_accepted_at: promotionAcceptedAt,
      finalized_full_run_id: "run:legacy-full",
      finalized_at: "2026-07-28T03:10:00.000Z",
    }],
    ownerships: [],
    deliveries: [],
    readiness: {
      channels: [{
        channel_id: CHANNEL_ID,
        domains: {
          channel: candidate("channel", channelReady, "a"),
          video: candidate("video", videoReady, "b"),
          agent: candidate("agent", agentReady, "c"),
        },
      }],
    },
  };
}

function buildEvidence(config, target, options = {}) {
  return buildPublicationLegacyAdoptionEvidence({
    config,
    target,
    source: evidenceSource(options),
    business: { ownerships: [] },
  });
}

function planState(config, target, evidence, {
  sourceOwnerships = [],
  sourceDeliveries = [],
  businessOwnerships = [],
} = {}) {
  return {
    source: {
      identity: {
        database_name: config.expectedCrawlerDatabase,
        server_address: "10.0.0.1",
        server_port: 5432,
        channel_count: config.expectedCrawlerChannelCount,
      },
      ownerships: sourceOwnerships,
      deliveries: sourceDeliveries,
    },
    business: {
      identity: {
        database_name: config.expectedBusinessDatabase,
        server_address: "10.0.0.2",
        server_port: 5432,
        channel_count: config.expectedBusinessChannelCount,
      },
      ownerships: businessOwnerships,
    },
    evidence,
    target,
  };
}

test("Legacy Adoption config and CLI require explicit, reviewable inputs", () => {
  const config = publicationLegacyAdoptionConfig(environment());
  assert.equal(config.asOf, AS_OF);
  assert.equal(config.concurrency, 6);
  assert.equal(config.evidenceFile, null);
  assert.deepEqual(publicationLegacyAdoptionCommand([]), {
    help: false,
    apply: false,
    output: null,
  });
  assert.deepEqual(publicationLegacyAdoptionCommand(["--apply", "--output", "/tmp/result.json"]), {
    help: false,
    apply: true,
    output: "/tmp/result.json",
  });
  assert.throws(() => publicationLegacyAdoptionConfig(environment(), { apply: true }), /EVIDENCE_FILE/);
  assert.throws(() => publicationLegacyAdoptionConfig(environment({
    EXPECTED_BUSINESS_DATABASE: "crawler_test",
  })), /must be different/);
  assert.throws(() => publicationLegacyAdoptionCommand(["--all"]), /unknown option/);
});

test("Legacy Adoption evidence reports independent Domain Bootstrap readiness", () => {
  const { config, target } = fixture();
  const evidence = buildEvidence(config, target, { agentReady: false });
  const state = planState(config, target, evidence);
  const summary = publicationLegacyAdoptionSummary(state, target, evidence);
  const channelOnlyEvidence = buildEvidence(config, target, { videoReady: false });
  const channelOnlySummary = publicationLegacyAdoptionSummary(
    planState(config, target, channelOnlyEvidence),
    target,
    channelOnlyEvidence,
  );
  assert.deepEqual(summary.domain_ready, { channel: 1, video: 1, agent: 0 });
  assert.equal(summary.channels_with_any_ready_domain, 1);
  assert.equal(summary.waiting_for_first_ready_domain, 0);
  assert.equal(summary.channel_and_video_ready, 1);
  assert.equal(summary.complete_three_domain_package, 0);
  assert.deepEqual(channelOnlySummary.domain_ready, { channel: 1, video: 0, agent: 0 });
  assert.equal(channelOnlySummary.channels_with_any_ready_domain, 1);
  assert.equal(channelOnlySummary.waiting_for_first_ready_domain, 0);
  assert.equal(channelOnlySummary.channel_and_video_ready, 0);
  assert.equal(summary.crawler_refetch_required, false);
  assert.match(
    publicationLegacyAdoptionConfirmation(config, target, evidence),
    /^ADOPT_LEGACY_TRACKED_CHANNELS:/,
  );
});

test("Legacy Adoption evidence is immutable and bound to the exact Channel set", async () => {
  const { config, target } = fixture();
  const evidence = buildEvidence(config, target, { agentReady: true });
  const directory = await mkdtemp(join(tmpdir(), "publication-legacy-adoption-"));
  try {
    const valid = join(directory, "valid.json");
    await writeFile(valid, `${JSON.stringify({ evidence })}\n`, "utf8");
    assert.deepEqual(
      await readPublicationLegacyAdoptionEvidence(valid, config, target),
      evidence,
    );
    const tampered = join(directory, "tampered.json");
    const changed = structuredClone(evidence);
    changed.channels[0].domains[0].result_hash = `sha256:${"f".repeat(64)}`;
    await writeFile(tampered, `${JSON.stringify({ evidence: changed })}\n`, "utf8");
    await assert.rejects(
      readPublicationLegacyAdoptionEvidence(tampered, config, target),
      /evidence hash mismatch/,
    );
  } finally {
    await rm(directory, { recursive: true, force: true });
  }
});

test("Legacy Adoption rejects a Channel promoted after Capture was enabled", () => {
  const { config, target } = fixture();
  assert.throws(
    () => buildEvidence(config, target, {
      promotionAcceptedAt: "2026-07-28T02:00:00.000Z",
    }),
    /entered the Registry after Publication Capture was enabled/,
  );
});

function fakePools(config, target, events, options = {}) {
  const calls = [];
  const operational = completePublicationOperationalFixture(CHANNEL_ID);
  const state = {
    sourceOwners: structuredClone(options.sourceOwners ?? []),
    sourceDeliveries: structuredClone(options.sourceDeliveries ?? []),
    businessOwners: structuredClone(options.businessOwners ?? []),
    channel: {
      channel_id: CHANNEL_ID,
      status: "active",
      created_at: REGISTRY_CREATED_AT,
      registry_promotion_candidate_id: "42",
      registry_promotion_run_id: "run:promotion",
      promotion_candidate_id: "42",
      promotion_status: "accepted",
      promotion_accepted_at: PROMOTION_ACCEPTED_AT,
      ...options.channel,
    },
    latestFinalize: {
      run_id: "run:legacy-full",
      publication_finalized_at: "2026-07-28T03:10:00.000Z",
      ...options.latestFinalize,
    },
    failSourceOnce: options.failSourceOnce === true,
    mutateFinalizeAfterBusinessRegistration:
      options.mutateFinalizeAfterBusinessRegistration === true,
    businessWritePending: false,
  };
  const sourceStream = {
    publication_stream_id: STREAM_ID,
    source_deployment_key: "qy-test",
    source_identity_json: { database: "crawler_test", deployment: "qy" },
    status: "active",
    minimum_writer_version: "publication-reconciler-v1",
    capture_enabled_at: CAPTURE_ENABLED_AT,
  };
  const businessStream = {
    publication_stream_id: STREAM_ID,
    source_deployment_key: sourceStream.source_deployment_key,
    source_identity_json: sourceStream.source_identity_json,
    status: "active",
    accepted_contract_versions: [1],
  };

  function client(side) {
    return {
      async query(sql, params = []) {
        const text = String(sql);
        calls.push({ side, sql: text, params });
        if (text === "COMMIT") {
          if (side === "business" && state.businessWritePending) {
            state.businessWritePending = false;
            if (state.mutateFinalizeAfterBusinessRegistration) {
              state.mutateFinalizeAfterBusinessRegistration = false;
              state.latestFinalize = {
                run_id: "run:newer-full",
                publication_finalized_at: "2026-07-28T03:20:00.000Z",
              };
            }
          }
          return { rows: [], rowCount: 0 };
        }
        if (/^(BEGIN|ROLLBACK|SET LOCAL|RELEASE SAVEPOINT)/.test(text)
          || text.includes("SAVEPOINT publication_channel_mutation_lock_guard")) {
          return { rows: [], rowCount: 0 };
        }
        if (text.includes("pg_advisory_xact_lock")) return { rows: [{}], rowCount: 1 };
        if (text.includes("publication-legacy-adoption:source-identity")) {
          return { rows: [{
            database_name: config.expectedCrawlerDatabase,
            server_address: "10.0.0.1",
            server_port: 5432,
            channel_count: config.expectedCrawlerChannelCount,
            stream_ready: true,
            ownership_ready: true,
            delivery_ready: true,
            current_ready: true,
            revision_ready: true,
            outbox_ready: true,
          }] };
        }
        if (text.includes("publication-legacy-adoption:business-identity")) {
          return { rows: [{
            database_name: config.expectedBusinessDatabase,
            server_address: "10.0.0.2",
            server_port: 5432,
            channel_count: config.expectedBusinessChannelCount,
            stream_ready: true,
            ownership_ready: true,
            inbox_ready: true,
            cursor_ready: true,
            entity_current_ready: true,
            video_current_ready: true,
          }] };
        }
        if (text.includes("publication-legacy-adoption:source-stream")) {
          return { rows: [sourceStream], rowCount: 1 };
        }
        if (text.includes("publication-legacy-adoption:business-stream")) {
          return { rows: [businessStream], rowCount: 1 };
        }
        if (text.includes("publication-legacy-adoption:source-channels")) {
          return { rows: [{
            ...state.channel,
            finalized_full_run_id: state.latestFinalize.run_id,
            finalized_at: state.latestFinalize.publication_finalized_at,
          }] };
        }
        if (text.includes("publication-readiness:channels")) {
          return { rows: [{ row: operational.channel }] };
        }
        if (text.includes("publication-readiness:contents")) {
          return { rows: operational.contents.map((row) => ({ row })) };
        }
        if (text.includes("publication-readiness:agents")) {
          return { rows: [{ row: operational.agent, config: operational.agentConfig }] };
        }
        if (text.includes("publication-readiness:sources")) {
          return { rows: operational.sources };
        }
        if (text.includes("publication-legacy-adoption:source-ownerships")) {
          return { rows: state.sourceOwners };
        }
        if (text.includes("publication-legacy-adoption:source-deliveries")) {
          return { rows: state.sourceDeliveries };
        }
        if (text.includes("publication-legacy-adoption:business-ownerships")) {
          return { rows: state.businessOwners };
        }
        if (text.includes("publication-legacy-adoption:lock-business-stream")) {
          return { rows: [businessStream], rowCount: 1 };
        }
        if (text.includes("publication-legacy-adoption:lock-business-ownerships")) {
          return { rows: state.businessOwners };
        }
        if (text.includes("publication-legacy-adoption:insert-business-ownerships")) {
          const inserts = JSON.parse(params[1]);
          const missing = inserts.filter((item) => !state.businessOwners.some((owner) => (
            owner.channel_id === item.channel_id
          )));
          if (missing.length > 0) events.push("business-ownership");
          if (missing.length > 0) state.businessWritePending = true;
          for (const item of missing) state.businessOwners.push({
            channel_id: item.channel_id,
            active_publication_stream_id: STREAM_ID,
            status: "active",
            previous_publication_stream_id: null,
            projection_mode: "held_shadow",
            ownership_reference: item.ownership_reference,
          });
          return { rows: [], rowCount: missing.length };
        }
        if (text.includes("publication-legacy-adoption:verify-business-ownerships")) {
          return { rows: state.businessOwners, rowCount: state.businessOwners.length };
        }
        if (text.includes("publication-legacy-adoption:lock-source-stream")) {
          if (state.failSourceOnce) {
            state.failSourceOnce = false;
            throw new Error("simulated Source interruption");
          }
          return { rows: [sourceStream], rowCount: 1 };
        }
        if (text.includes("publication-legacy-adoption:lock-source-channel")) {
          const {
            promotion_candidate_id: _candidateId,
            promotion_status: _promotionStatus,
            promotion_accepted_at: _acceptedAt,
            ...channel
          } = state.channel;
          return { rows: [channel], rowCount: 1 };
        }
        if (text.includes("publication-legacy-adoption:lock-source-promotion")) {
          return { rows: [{
            promotion_candidate_id: state.channel.promotion_candidate_id,
            promotion_status: state.channel.promotion_status,
            promotion_accepted_at: state.channel.promotion_accepted_at,
          }], rowCount: 1 };
        }
        if (text.includes("publication-legacy-adoption:lock-source-finalize")) {
          return { rows: [state.latestFinalize], rowCount: 1 };
        }
        if (text.includes("publication-legacy-adoption:lock-source-owner")) {
          return { rows: state.sourceOwners };
        }
        if (text.includes("publication-legacy-adoption:lock-source-deliveries")) {
          return { rows: state.sourceDeliveries };
        }
        if (text.includes("publication-legacy-adoption:insert-source-owner")) {
          if (state.sourceOwners.some((owner) => (
            owner.publication_stream_id === STREAM_ID && owner.channel_id === CHANNEL_ID
          ))) return { rows: [], rowCount: 0 };
          events.push("source-ownership");
          state.sourceOwners.push({
            publication_stream_id: STREAM_ID,
            channel_id: CHANNEL_ID,
            status: "owned",
            onboarding_mode: "bootstrap",
            seed_status: "pending",
            ownership_reference: JSON.parse(params[2]),
            owned_at: AS_OF,
            seed_completed_at: null,
            sealed_at: null,
          });
          return { rows: [], rowCount: 1 };
        }
        if (text.includes("publication-legacy-adoption:verify-source-owner")) {
          return { rows: state.sourceOwners, rowCount: state.sourceOwners.length };
        }
        if (text.includes("publication-legacy-adoption:insert-source-delivery")) {
          if (state.sourceDeliveries.some((delivery) => (
            delivery.destination === target.destination
            && delivery.publication_stream_id === STREAM_ID
            && delivery.channel_id === CHANNEL_ID
          ))) return { rows: [], rowCount: 0 };
          events.push("source-delivery");
          state.sourceDeliveries.push({
            destination: target.destination,
            publication_stream_id: STREAM_ID,
            channel_id: CHANNEL_ID,
            mode: "online",
            source_ownership_reference: JSON.parse(params[3]),
            online_at: AS_OF,
            sealed_at: null,
          });
          return { rows: [], rowCount: 1 };
        }
        if (text.includes("publication-legacy-adoption:verify-source-delivery")) {
          return { rows: state.sourceDeliveries };
        }
        throw new Error(`unexpected ${side} query: ${text.slice(0, 100)}`);
      },
      release() {},
    };
  }
  return {
    calls,
    state,
    crawlerPool: { async connect() { return client("source"); } },
    businessPool: { async connect() { return client("business"); } },
  };
}

function plannedReconciler(evidence, events = []) {
  return async (_client, input) => {
    events.push("reconcile");
    const expected = evidence.channels.find((row) => row.channel_id === input.channelId);
    return {
      status: "revised",
      domains: expected.domains.map((domain) => ({
        domain: domain.domain,
        status: domain.ready ? "revision_created" : "not_ready",
        result_hash: domain.result_hash,
        readiness_reasons: domain.readiness_reasons,
      })),
      revisions: expected.domains.filter((domain) => domain.ready).map((domain) => ({
        domain: domain.domain,
      })),
    };
  };
}

test("Legacy Adoption applies Business ownership before Source delivery and creates no Crawl", async () => {
  const { config, target } = fixture();
  const events = [];
  const pools = fakePools(config, target, events);
  const planner = new PublicationLegacyAdoptionAdministrator({
    crawlerPool: pools.crawlerPool,
    businessPool: pools.businessPool,
    config,
    target,
  });
  const plan = await planner.inspectReadOnly();
  const evidence = plan.evidence;
  const administrator = new PublicationLegacyAdoptionAdministrator({
    crawlerPool: pools.crawlerPool,
    businessPool: pools.businessPool,
    config,
    target,
    evidence,
    reconcile: plannedReconciler(evidence, events),
  });
  const result = await administrator.apply();
  const sourceLock = pools.calls.findIndex((call) => (
    call.side === "source"
      && call.sql.includes("publication-channel-mutation-lock:channel")
  ));
  const finalizeRead = pools.calls.findIndex((call) => (
    call.side === "source"
      && call.sql.includes("publication-legacy-adoption:lock-source-finalize")
  ));
  assert.ok(sourceLock >= 0 && finalizeRead > sourceLock);
  assert.deepEqual(events, [
    "business-ownership",
    "source-ownership",
    "source-delivery",
    "reconcile",
  ]);
  assert.equal(result.business.inserted, 1);
  assert.equal(result.crawler.ownerships_inserted, 1);
  assert.equal(result.crawler.deliveries_inserted, 1);
  assert.deepEqual(result.crawler.revisions_created, { channel: 1, video: 1, agent: 1 });
  assert.equal(pools.state.businessOwners[0].projection_mode, "held_shadow");
  assert.equal(
    pools.state.businessOwners[0].ownership_reference.onboarding_mode,
    "legacy_tracked_adoption",
  );

  events.length = 0;
  const replay = await administrator.apply();
  assert.deepEqual(events, []);
  assert.equal(replay.business.inserted, 0);
  assert.equal(replay.crawler.ownerships_inserted, 0);
  assert.equal(replay.crawler.deliveries_inserted, 0);
  assert.equal(replay.crawler.recovered, 1);
  assert.deepEqual(replay.crawler.revisions_created, { channel: 0, video: 0, agent: 0 });
});

test("Legacy Adoption resumes a Business-only interruption only with the original Evidence", async () => {
  const { config, target } = fixture();
  const events = [];
  const pools = fakePools(config, target, events, { failSourceOnce: true });
  const planner = new PublicationLegacyAdoptionAdministrator({
    crawlerPool: pools.crawlerPool,
    businessPool: pools.businessPool,
    config,
    target,
  });
  const { evidence } = await planner.inspectReadOnly();
  const administrator = new PublicationLegacyAdoptionAdministrator({
    crawlerPool: pools.crawlerPool,
    businessPool: pools.businessPool,
    config,
    target,
    evidence,
    reconcile: plannedReconciler(evidence, events),
  });

  await assert.rejects(administrator.apply(), /completed only partially/);
  assert.equal(pools.state.businessOwners.length, 1);
  assert.equal(pools.state.sourceOwners.length, 0);

  const differentConfig = publicationLegacyAdoptionConfig(environment({
    PUBLICATION_LEGACY_ADOPTION_KEY: "legacy-20260728-v2",
  }));
  const differentTarget = buildPublicationLegacyAdoptionTarget(differentConfig, [CHANNEL_ID]);
  const replanner = new PublicationLegacyAdoptionAdministrator({
    crawlerPool: pools.crawlerPool,
    businessPool: pools.businessPool,
    config: differentConfig,
    target: differentTarget,
  });
  await assert.rejects(
    replanner.inspectReadOnly(),
    /Business ownership exists without Crawler ownership; resume its original operation/,
  );

  events.length = 0;
  const resumed = await administrator.apply();
  assert.deepEqual(events, ["source-ownership", "source-delivery", "reconcile"]);
  assert.equal(resumed.business.inserted, 0);
  assert.equal(resumed.crawler.ownerships_inserted, 1);
  assert.equal(pools.state.sourceOwners[0].ownership_reference.evidence_hash, evidence.evidence_hash);
  assert.equal(
    pools.state.businessOwners[0].ownership_reference.evidence_hash,
    evidence.evidence_hash,
  );
});

test("Legacy Adoption rejects a newer Finalize inside the per-Channel Source transaction", async () => {
  const { config, target } = fixture();
  const events = [];
  const pools = fakePools(config, target, events, {
    mutateFinalizeAfterBusinessRegistration: true,
  });
  const planner = new PublicationLegacyAdoptionAdministrator({
    crawlerPool: pools.crawlerPool,
    businessPool: pools.businessPool,
    config,
    target,
  });
  const { evidence } = await planner.inspectReadOnly();
  const administrator = new PublicationLegacyAdoptionAdministrator({
    crawlerPool: pools.crawlerPool,
    businessPool: pools.businessPool,
    config,
    target,
    evidence,
    reconcile: plannedReconciler(evidence, events),
  });

  await assert.rejects(
    administrator.apply(),
    (error) => {
      assert.equal(error.details.crawler.failed, 1);
      assert.match(error.details.failures[0].error, /Registry or Finalize changed/);
      return true;
    },
  );
  assert.deepEqual(events, ["business-ownership"]);
  assert.equal(pools.state.sourceOwners.length, 0);
  assert.equal(pools.state.latestFinalize.run_id, "run:newer-full");
});

test("Plan and Apply both allow sealed Delivery history for a pre-existing non-Legacy owner", async () => {
  const { config, target } = fixture();
  const events = [];
  const sourceReference = { onboarding_mode: "initial_cohort", cohort: "canary" };
  const sourceOwners = [{
    publication_stream_id: STREAM_ID,
    channel_id: CHANNEL_ID,
    status: "owned",
    onboarding_mode: "bootstrap",
    seed_status: "complete",
    ownership_reference: sourceReference,
    owned_at: "2026-07-27T00:00:00.000Z",
    seed_completed_at: "2026-07-27T00:10:00.000Z",
    sealed_at: null,
  }];
  const sourceDeliveries = [{
    destination: target.destination,
    publication_stream_id: STREAM_ID,
    channel_id: CHANNEL_ID,
    mode: "online",
    source_ownership_reference: sourceReference,
    online_at: "2026-07-27T00:10:00.000Z",
    sealed_at: null,
  }, {
    destination: "historical-archive",
    publication_stream_id: STREAM_ID,
    channel_id: CHANNEL_ID,
    mode: "sealed",
    source_ownership_reference: sourceReference,
    online_at: "2026-07-27T00:10:00.000Z",
    sealed_at: "2026-07-27T01:00:00.000Z",
  }];
  const businessOwnerships = [{
    channel_id: CHANNEL_ID,
    active_publication_stream_id: STREAM_ID,
    status: "active",
    previous_publication_stream_id: null,
    projection_mode: "online",
    ownership_reference: { onboarding_mode: "initial_cohort" },
  }];
  const pools = fakePools(config, target, events, {
    sourceOwners,
    sourceDeliveries,
    businessOwners: businessOwnerships,
  });
  const planner = new PublicationLegacyAdoptionAdministrator({
    crawlerPool: pools.crawlerPool,
    businessPool: pools.businessPool,
    config,
    target,
  });
  const plan = await planner.inspectReadOnly();
  assert.equal(plan.evidence.channels[0].topology.disposition, "preexisting");
  assert.deepEqual(plan.evidence.expected_inserts, {
    business_ownerships: 0,
    source_ownerships: 0,
    source_deliveries: 0,
    revisions: { channel: 0, video: 0, agent: 0 },
    outbox_rows: 0,
  });
  const administrator = new PublicationLegacyAdoptionAdministrator({
    crawlerPool: pools.crawlerPool,
    businessPool: pools.businessPool,
    config,
    target,
    evidence: plan.evidence,
    reconcile: async () => assert.fail("pre-existing Channel must not reconcile"),
  });
  const result = await administrator.apply();
  assert.deepEqual(events, []);
  assert.equal(result.crawler.preexisting, 1);
});

test("Legacy Adoption does not use an unpaired online Business owner", async () => {
  const { config, target } = fixture();
  const pools = fakePools(config, target, [], {
    businessOwners: [{
      channel_id: CHANNEL_ID,
      active_publication_stream_id: STREAM_ID,
      status: "active",
      previous_publication_stream_id: null,
      projection_mode: "online",
      ownership_reference: { onboarding_mode: "unrelated" },
    }],
  });
  const planner = new PublicationLegacyAdoptionAdministrator({
    crawlerPool: pools.crawlerPool,
    businessPool: pools.businessPool,
    config,
    target,
  });
  await assert.rejects(
    planner.inspectReadOnly(),
    /Business ownership exists without Crawler ownership/,
  );
});
