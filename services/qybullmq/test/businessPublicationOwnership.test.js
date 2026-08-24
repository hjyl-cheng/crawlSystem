import assert from "node:assert/strict";
import test from "node:test";
import { ensureAutomaticBusinessBootstrapOwnership } from "../src/businessPublicationOwnership.js";

const STREAM_ID = "11111111-1111-4111-8111-111111111111";

function bootstrapEnvelope(overrides = {}) {
  return {
    revision_id: "33333333-3333-4333-8333-333333333333",
    publication_stream_id: STREAM_ID,
    revision_type: "bootstrap",
    channel_id: "UCnew",
    domain: "channel",
    data_sequence: 1,
    previous_data_sequence: null,
    ...overrides,
  };
}

function ownershipClient({
  existing = null,
  projectionModes = ["held_shadow"],
  automaticProjectionMode = null,
} = {}) {
  const calls = [];
  let ownership = existing;
  return {
    calls,
    async query(sql, params = []) {
      const text = String(sql);
      calls.push({ sql: text, params });
      if (text.includes("business-publication-ownership:find")) {
        return { rows: ownership ? [ownership] : [], rowCount: ownership ? 1 : 0 };
      }
      if (text.includes("business-publication-ownership:inherit-policy")) {
        return {
          rows: [{
            owner_count: projectionModes.length,
            all_online: projectionModes.length > 0
              && projectionModes.every((projectionMode) => projectionMode === "online"),
            automatic_onboarding_projection_mode: automaticProjectionMode,
          }],
        };
      }
      if (text.includes("business-publication-ownership:insert")) {
        ownership = {
          channel_id: params[0],
          active_publication_stream_id: params[1],
          status: "active",
          projection_mode: params[3],
        };
        return { rows: [{ channel_id: params[0] }], rowCount: 1 };
      }
      throw new Error(`unexpected query: ${text}`);
    },
  };
}

test("a valid new Channel Bootstrap inherits held Shadow ownership", async () => {
  const client = ownershipClient({ projectionModes: ["held_shadow", "held_shadow"] });
  const result = await ensureAutomaticBusinessBootstrapOwnership(client, bootstrapEnvelope());

  assert.equal(result.status, "registered");
  assert.equal(result.created, true);
  assert.equal(result.ownership.active_publication_stream_id, STREAM_ID);
  assert.equal(result.ownership.projection_mode, "held_shadow");
  const insert = client.calls.find((call) => call.sql.includes("ownership:insert"));
  assert.equal(insert.params[3], "held_shadow");
  assert.equal(
    client.calls.some((call) => call.sql.includes("FOR UPDATE")),
    false,
  );
});

test("a fully online Stream gives new Channels online projection ownership", async () => {
  const client = ownershipClient({ projectionModes: ["online", "online"] });
  const result = await ensureAutomaticBusinessBootstrapOwnership(client, bootstrapEnvelope());

  assert.equal(result.ownership.projection_mode, "online");
  const policy = client.calls.find((call) => call.sql.includes("inherit-policy"));
  assert.match(policy.sql, /status IN \('active','cutover_pending'\)/);
});

test("the first Channel uses the explicit Business Stream projection policy", async () => {
  const client = ownershipClient({
    projectionModes: [],
    automaticProjectionMode: "online",
  });
  const result = await ensureAutomaticBusinessBootstrapOwnership(client, bootstrapEnvelope());

  assert.equal(result.status, "registered");
  assert.equal(result.ownership.projection_mode, "online");
});

test("a held cutover-pending Owner prevents a new Channel from inheriting online", async () => {
  const client = ownershipClient({ projectionModes: ["online", "held_shadow"] });
  const result = await ensureAutomaticBusinessBootstrapOwnership(client, bootstrapEnvelope());

  assert.equal(result.ownership.projection_mode, "held_shadow");
});

test("an existing Channel owner is never overwritten", async () => {
  const existing = {
    channel_id: "UCnew",
    active_publication_stream_id: "22222222-2222-4222-8222-222222222222",
    status: "active",
    projection_mode: "online",
  };
  const client = ownershipClient({ existing });
  const result = await ensureAutomaticBusinessBootstrapOwnership(client, bootstrapEnvelope());

  assert.equal(result.status, "existing");
  assert.equal(result.created, false);
  assert.equal(result.ownership, existing);
  assert.equal(client.calls.length, 1);
});

test("non-Channel Bootstrap revisions cannot create ownership", async () => {
  const client = ownershipClient();
  const result = await ensureAutomaticBusinessBootstrapOwnership(
    client,
    bootstrapEnvelope({ domain: "video" }),
  );

  assert.deepEqual(result, { status: "not_bootstrap", created: false, ownership: null });
  assert.equal(client.calls.length, 0);
});
