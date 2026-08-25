import assert from "node:assert/strict";
import test from "node:test";
import {
  PublicationCurrentSeedConflict,
  seedPublicationCurrents,
} from "../src/publicationCurrentStore.js";
import { publicationResultHash } from "../src/publicationResultHash.js";

const STREAM_ID = "11111111-1111-4111-8111-111111111111";
const CHANNEL_ID = "UCpublication-current";
const OBSERVED_AT = "2026-07-27T01:00:00.000Z";

function readyCurrent(domain, marker = domain) {
  const payload = domain === "video" ? {
    channel_id: CHANNEL_ID,
    window_policy: {
      policy_version: "video-window-v1",
      max_age_days: 90,
      max_items: 30,
    },
    items: [{
      position: 1,
      content_id: `video-${marker}`,
      item_hash: `sha256:${"a".repeat(64)}`,
    }],
  } : { channel_id: CHANNEL_ID, marker };
  return {
    ready: true,
    contract_version: 1,
    policy_version: domain === "video" ? "video-window-v1" : "publication-policy-v1",
    payload,
    result_hash: publicationResultHash(domain, payload),
    source_refs: { cursor: { latest_complete_observed_at: OBSERVED_AT } },
    issues: [],
  };
}

function fakeClient({ onboardingMode = "baseline", existing = [], owned = true } = {}) {
  const calls = [];
  return {
    calls,
    async query(sql, params) {
      calls.push({ sql, params });
      if (sql.includes("publication-current:transaction-guard") || sql.startsWith("RELEASE SAVEPOINT")) {
        return { rows: [] };
      }
      if (sql.includes("publication-current:lock-channel")) {
        return { rows: owned ? [{
          channel_status: "owned",
          onboarding_mode: onboardingMode,
          seed_status: "pending",
          stream_status: "active",
        }] : [] };
      }
      if (sql.includes("publication-current:lock-domains")) return { rows: existing };
      if (sql.includes("publication-current:store-domain")) {
        return { rows: [{
          domain: params[2],
          readiness_status: params[5],
          result_hash: params[8],
          data_sequence: "0",
          current_revision_id: null,
        }] };
      }
      if (sql.includes("publication-current:seed-status")) {
        const stored = calls.filter((call) => call.sql.includes("publication-current:store-domain"));
        return { rows: [{
          seed_status: stored.length === 3 ? "complete" : "pending",
          seed_completed_at: stored.length === 3 ? OBSERVED_AT : null,
        }] };
      }
      throw new Error(`unexpected query: ${sql}`);
    },
  };
}

test("Current Seed Store locks and writes Domains in canonical order", async () => {
  const client = fakeClient();
  const result = await seedPublicationCurrents(client, {
    publicationStreamId: STREAM_ID,
    channelId: CHANNEL_ID,
    currents: {
      agent: readyCurrent("agent", "a"),
      video: readyCurrent("video", "b"),
      channel: readyCurrent("channel", "c"),
    },
  });

  assert.equal(result.status, "ready");
  assert.equal(result.seed_status, "complete");
  assert.deepEqual(result.domains.map((item) => item.domain), ["channel", "video", "agent"]);
  assert.deepEqual(
    client.calls
      .filter((call) => call.sql.includes("publication-current:store-domain"))
      .map((call) => call.params[2]),
    ["channel", "video", "agent"],
  );
  const lockDomains = client.calls.findIndex((call) => call.sql.includes("publication-current:lock-domains"));
  const lockChannel = client.calls.findIndex((call) => call.sql.includes("publication-current:lock-channel"));
  assert.ok(lockDomains >= 0 && lockDomains < lockChannel);
});

test("a Ready Domain can be stored while the three-Domain Seed remains pending", async () => {
  const result = await seedPublicationCurrents(fakeClient(), {
    publicationStreamId: STREAM_ID,
    channelId: CHANNEL_ID,
    currents: { channel: readyCurrent("channel", "c") },
  });

  assert.equal(result.status, "ready");
  assert.equal(result.seed_status, "pending");
});

test("Current Seed Store refuses Bootstrap and missing ownership without writing", async () => {
  const bootstrap = fakeClient({ onboardingMode: "bootstrap" });
  const bootstrapResult = await seedPublicationCurrents(bootstrap, {
    publicationStreamId: STREAM_ID,
    channelId: CHANNEL_ID,
    currents: { channel: readyCurrent("channel", "c") },
  });
  assert.deepEqual(
    { status: bootstrapResult.status, reason: bootstrapResult.reason },
    { status: "not_seedable", reason: "bootstrap_requires_revision" },
  );
  assert.equal(
    bootstrap.calls.some((call) => call.sql.includes("publication-current:store-domain")),
    false,
  );

  const missing = fakeClient({ owned: false });
  const missingResult = await seedPublicationCurrents(missing, {
    publicationStreamId: STREAM_ID,
    channelId: CHANNEL_ID,
    currents: { channel: readyCurrent("channel", "c") },
  });
  assert.equal(missingResult.status, "not_owned");
  assert.equal(
    missing.calls.some((call) => call.sql.includes("publication-current:store-domain")),
    false,
  );
});

test("Current Seed Store refuses to replace an existing Sequence-0 Result", async () => {
  const seeded = readyCurrent("channel", "c");
  const client = fakeClient({ existing: [{
    domain: "channel",
    data_sequence: "0",
    current_revision_id: null,
    result_hash: seeded.result_hash,
  }] });

  await assert.rejects(
    seedPublicationCurrents(client, {
      publicationStreamId: STREAM_ID,
      channelId: CHANNEL_ID,
      currents: { channel: readyCurrent("channel", "d") },
    }),
    (error) => error instanceof PublicationCurrentSeedConflict
      && error.details.current_result_hash === seeded.result_hash,
  );
  assert.equal(
    client.calls.some((call) => call.sql.includes("publication-current:store-domain")),
    false,
  );
});

test("Current Seed Store cannot overwrite an online Revision Current", async () => {
  const client = fakeClient({ existing: [{
    domain: "channel",
    data_sequence: "1",
    current_revision_id: "22222222-2222-4222-8222-222222222222",
  }] });

  await assert.rejects(
    seedPublicationCurrents(client, {
      publicationStreamId: STREAM_ID,
      channelId: CHANNEL_ID,
      currents: { channel: readyCurrent("channel", "c") },
    }),
    (error) => error instanceof PublicationCurrentSeedConflict
      && error.details.data_sequence === "1",
  );
  assert.equal(
    client.calls.some((call) => call.sql.includes("publication-current:store-domain")),
    false,
  );
});

test("NotReady Current requires structured diagnostics before touching PostgreSQL", async () => {
  const client = fakeClient();
  await assert.rejects(
    seedPublicationCurrents(client, {
      publicationStreamId: STREAM_ID,
      channelId: CHANNEL_ID,
      currents: {
        channel: {
          ready: false,
          contract_version: 1,
          policy_version: "publication-policy-v1",
          payload: { channel_id: CHANNEL_ID },
          result_hash: null,
          source_refs: {},
          issues: [],
        },
      },
    }),
    /NotReady Current must include at least one issue/,
  );
  assert.equal(client.calls.length, 0);
});

test("Ready Current rejects diagnostics and a result hash that does not match the payload", async () => {
  const withIssues = readyCurrent("channel", "c");
  withIssues.issues = [{ domain: "channel", code: "unexpected" }];
  const badHash = readyCurrent("agent", "a");
  badHash.result_hash = `sha256:${"f".repeat(64)}`;

  await assert.rejects(
    seedPublicationCurrents(fakeClient(), {
      publicationStreamId: STREAM_ID,
      channelId: CHANNEL_ID,
      currents: { channel: withIssues },
    }),
    /Ready Current cannot include readiness issues/,
  );
  await assert.rejects(
    seedPublicationCurrents(fakeClient(), {
      publicationStreamId: STREAM_ID,
      channelId: CHANNEL_ID,
      currents: { agent: badHash },
    }),
    /result_hash does not match its payload/,
  );
});

test("Video Result Hash rejects non-contiguous Current positions", () => {
  const current = readyCurrent("video", "contiguous");
  current.payload.items.push({
    position: 3,
    content_id: "video-position-gap",
    item_hash: `sha256:${"b".repeat(64)}`,
  });

  assert.throws(
    () => publicationResultHash("video", current.payload),
    /positions must be contiguous from 1/,
  );
});

test("Current Seed Store requires an already-open transaction", async () => {
  const client = {
    calls: [],
    async query(sql) {
      this.calls.push(sql);
      if (sql.includes("publication-current:transaction-guard")) {
        throw new Error("SAVEPOINT can only be used in transaction blocks");
      }
      throw new Error(`unexpected query: ${sql}`);
    },
  };

  await assert.rejects(
    seedPublicationCurrents(client, {
      publicationStreamId: STREAM_ID,
      channelId: CHANNEL_ID,
      currents: { channel: readyCurrent("channel", "c") },
    }),
    /SAVEPOINT can only be used in transaction blocks/,
  );
  assert.equal(client.calls.length, 1);
});
