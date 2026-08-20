import assert from "node:assert/strict";
import test from "node:test";
import {
  channelCandidateCanFailAdmission,
  claimChannelRegistryPromotion,
  resolveChannelRegistryRunId,
} from "../src/channelRegistryPromotion.js";

function promotionInput(overrides = {}) {
  return {
    candidateId: 42,
    runId: "run:new",
    channelId: "UCnew",
    channelUrl: "https://www.youtube.com/channel/UCnew",
    handle: "@new",
    title: "New Channel",
    country: "Brazil",
    countryCode: "BR",
    countryCanonicalName: "Brazil",
    subscriberCount: 1234,
    subscriberCountText: "1.23K subscribers",
    readyForAgent: true,
    sourceJson: { source: "query" },
    ...overrides,
  };
}

test("an accepted Candidate retry without job data recovers its immutable Promotion Run", () => {
  const runId = resolveChannelRegistryRunId({
    requestedRunId: null,
    candidate: { candidate_id: 42, status: "accepted" },
    channel: {
      channel_id: "UCnew",
      registry_promotion_candidate_id: 42,
      registry_promotion_run_id: "run:new",
    },
  });

  assert.equal(runId, "run:new");
});

test("an explicitly identified later Full Repair keeps its own Run ID", () => {
  const runId = resolveChannelRegistryRunId({
    requestedRunId: "run:repair",
    candidate: { candidate_id: 42, status: "accepted" },
    channel: {
      channel_id: "UCnew",
      registry_promotion_candidate_id: 42,
      registry_promotion_run_id: "run:new",
    },
  });

  assert.equal(runId, "run:repair");
});

test("an accepted Promotion Candidate cannot fail admission again during crash recovery", () => {
  assert.equal(channelCandidateCanFailAdmission({ status: "accepted" }), false);
  assert.equal(channelCandidateCanFailAdmission({ status: "validating" }), true);
});

test("the Channel Registry primary-key insert winner is the only promoted Candidate", async () => {
  const calls = [];
  const client = {
    async query(sql, params) {
      const statement = String(sql);
      calls.push({ sql: statement, params });
      if (statement.includes("channel-registry-promotion:claim")) {
        return {
          rowCount: 1,
          rows: [{
            channel_id: "UCnew",
            registry_promotion_candidate_id: 42,
            registry_promotion_run_id: "run:new",
          }],
        };
      }
      if (statement.includes("channel-registry-promotion:accept-candidate")) {
        return { rowCount: 1, rows: [{ candidate_id: 42 }] };
      }
      throw new Error(`unexpected query: ${statement}`);
    },
  };

  const result = await claimChannelRegistryPromotion(client, promotionInput());

  assert.deepEqual(result, {
    status: "promoted",
    promoted: true,
    channel_id: "UCnew",
    promotion_candidate_id: 42,
    promotion_run_id: "run:new",
  });
  assert.match(calls[0].sql, /ON CONFLICT \(channel_id\) DO NOTHING/);
  assert.match(calls[0].sql, /registry_promotion_candidate_id,registry_promotion_run_id/);
  assert.equal(calls.some((call) => call.sql.includes("mark-existing")), false);
});

test("a Channel Registry primary-key conflict marks only the losing Candidate existing", async () => {
  const calls = [];
  const client = {
    async query(sql, params) {
      const statement = String(sql);
      calls.push({ sql: statement, params });
      if (statement.includes("channel-registry-promotion:claim")) {
        return { rowCount: 0, rows: [] };
      }
      if (statement.includes("channel-registry-promotion:load-winner")) {
        return {
          rowCount: 1,
          rows: [{
            channel_id: "UCnew",
            registry_promotion_candidate_id: 7,
            registry_promotion_run_id: "run:winner",
          }],
        };
      }
      if (statement.includes("channel-registry-promotion:mark-existing")) {
        return { rowCount: 1, rows: [] };
      }
      throw new Error(`unexpected query: ${statement}`);
    },
  };

  const result = await claimChannelRegistryPromotion(client, promotionInput({
    candidateId: 8,
    runId: "run:loser",
  }));

  assert.deepEqual(result, {
    status: "existing",
    promoted: false,
    channel_id: "UCnew",
    promotion_candidate_id: 7,
    promotion_run_id: "run:winner",
  });
  assert.equal(calls.some((call) => call.sql.includes("accept-candidate")), false);
  const loser = calls.find((call) => call.sql.includes("mark-existing"));
  assert.deepEqual(loser.params.slice(0, 2), [8, "UCnew"]);
  assert.match(loser.sql, /status IN \('discovered','queued','validating'\)/);
});

test("a failed Candidate status transition rolls back the Registry promotion", async () => {
  const client = {
    async query(sql) {
      const statement = String(sql);
      if (statement.includes("channel-registry-promotion:claim")) {
        return { rowCount: 1, rows: [{ channel_id: "UCnew" }] };
      }
      if (statement.includes("channel-registry-promotion:accept-candidate")) {
        return { rowCount: 0, rows: [] };
      }
      throw new Error(`unexpected query: ${statement}`);
    },
  };

  await assert.rejects(
    claimChannelRegistryPromotion(client, promotionInput()),
    /Candidate is not claimable/,
  );
});
