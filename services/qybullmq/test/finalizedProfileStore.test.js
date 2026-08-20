import assert from "node:assert/strict";
import test from "node:test";
import {
  commitFinalizedProfile,
  synchronizeFinalizedRun,
} from "../src/finalizedProfileStore.js";
import {
  FINALIZABLE_CHANNEL_STATUSES,
  SUCCESSFUL_PUBLICATION_FINALIZE_STATUSES,
} from "../src/finalizePolicy.js";

function clientFixture({ apply = true, latest = null, synchronize = true } = {}) {
  const calls = [];
  return {
    calls,
    async query(sql, params = []) {
      calls.push({ sql: String(sql), params });
      if (String(sql).includes("publication-channel-mutation-lock:run")) {
        return { rowCount: 1, rows: [{ channel_id: "UCfull" }] };
      }
      if (String(sql).includes("finalized-profile-store:apply")) {
        return {
          rowCount: apply ? 1 : 0,
          rows: apply ? [{ channel_id: "UCfull", applied: true, synchronized: true }] : [],
        };
      }
      if (String(sql).includes("finalized-profile-store:load-race")) {
        return { rowCount: latest ? 1 : 0, rows: latest ? [latest] : [] };
      }
      if (String(sql).includes("finalized-profile-store:sync-run")) {
        return { rowCount: synchronize ? 1 : 0, rows: [] };
      }
      if (String(sql).includes("publication-auto-onboarding:find-owner")) {
        return { rowCount: 0, rows: [] };
      }
      if (
        String(sql).includes("publication-auto-onboarding:transaction-guard")
        || String(sql).startsWith("RELEASE SAVEPOINT publication_auto_onboarding_guard")
      ) {
        return { rowCount: 0, rows: [] };
      }
      if (String(sql).includes("publication-auto-onboarding:load-channel")) {
        return {
          rowCount: 1,
          rows: [{
            channel_id: "UCfull",
            status: "active",
            created_at: "2026-07-27T15:00:00.000Z",
            latest_run_id: "run:full",
            initial_full_run_id: "run:full",
            initial_candidate_id: 42,
            promotion_accepted_at: "2026-07-27T15:01:00.000Z",
          }],
        };
      }
      if (String(sql).includes("publication-auto-onboarding:active-streams")) {
        return { rowCount: 0, rows: [] };
      }
      if (String(sql).includes("publication-reconciler:find-owner")) {
        return { rowCount: 0, rows: [] };
      }
      return { rowCount: 1, rows: [] };
    },
  };
}

function input(overrides = {}) {
  return {
    channelId: "UCfull",
    runId: "run:full",
    status: "ready_auto",
    profile: { channel: { channel_id: "UCfull" } },
    quality: { quality_status: "ready_auto" },
    publicationAsOf: "2026-07-27T16:00:00.000Z",
    ...overrides,
  };
}

test("Finalize apply and Publication reconcile share one ordered transaction", async () => {
  const client = clientFixture();
  const result = await commitFinalizedProfile(client, input());

  assert.equal(result.applied, true);
  assert.equal(result.publication.status, "not_owned");
  const lock = client.calls.findIndex((call) => (
    call.sql.includes("publication-channel-mutation-lock:channel")
  ));
  const apply = client.calls.findIndex((call) => call.sql.includes("finalized-profile-store:apply"));
  const publication = client.calls.findIndex((call) => (
    call.sql.includes("publication-auto-onboarding:find-owner")
  ));
  assert.ok(lock >= 0 && apply > lock && publication > apply);
});

test("Finalize Profile and durable Run evidence are written by one atomic SQL statement", async () => {
  const client = clientFixture();
  await commitFinalizedProfile(client, input());

  const apply = client.calls.find((call) => call.sql.includes("finalized-profile-store:apply"));
  assert.match(apply.sql, /WITH applied_profile AS/);
  assert.match(apply.sql, /synchronized_run AS/);
  assert.match(apply.sql, /UPDATE crawler\.channel_runs/);
  assert.doesNotMatch(apply.sql, /IN \('ready_auto','ready_partial'\)/);
  assert.deepEqual(apply.params.at(-2), SUCCESSFUL_PUBLICATION_FINALIZE_STATUSES);
  assert.deepEqual(apply.params.at(-1), FINALIZABLE_CHANNEL_STATUSES);
  assert.match(apply.sql, /status=ANY\(\$9::text\[\]\)/);
  assert.equal(
    client.calls.some((call) => call.sql.includes("finalized-profile-store:sync-run")),
    false,
  );
});

test("deduplicated Finalize still reconciles an owned Publication Current", async () => {
  const client = clientFixture();
  const result = await commitFinalizedProfile(client, input({
    deduplicated: true,
    profile: undefined,
    quality: undefined,
    publicationRevisionType: "repair",
  }));

  assert.equal(result.deduplicated, true);
  assert.equal(result.publication.status, "not_owned");
  assert.equal(
    client.calls.some((call) => call.sql.includes("finalized-profile-store:apply")),
    false,
  );
});

test("a stale Finalize cannot reconcile or overwrite the retained profile", async () => {
  const client = clientFixture({
    apply: false,
    latest: {
      latest_run_id: "run:newer",
      finalized_run_id: "run:newer",
      finalized_status: "ready_auto",
    },
  });
  const result = await commitFinalizedProfile(client, input());

  assert.equal(result.skip_reason, "stale_run_race");
  assert.equal(result.publication, null);
  assert.equal(
    client.calls.some((call) => call.sql.includes("publication-auto-onboarding:find-owner")),
    false,
  );
});

test("Run synchronization preserves the Finalize status mapping", async () => {
  const client = clientFixture();
  await synchronizeFinalizedRun(client, {
    runId: "run:pending-agent",
    finalizedStatus: "pending_agent",
  });

  const lock = client.calls.findIndex((call) => (
    call.sql.includes("publication-channel-mutation-lock:run")
  ));
  const syncIndex = client.calls.findIndex((call) => (
    call.sql.includes("finalized-profile-store:sync-run")
  ));
  const sync = client.calls.find((call) => call.sql.includes("finalized-profile-store:sync-run"));
  assert.ok(lock >= 0 && syncIndex > lock);
  assert.deepEqual(sync.params.slice(0, 2), ["run:pending-agent", "waiting_agent"]);
  assert.equal(sync.params[3], false);
});

test("a successful Finalize records durable gate evidence on its Run", async () => {
  const client = clientFixture();
  await synchronizeFinalizedRun(client, {
    runId: "run:ready-partial",
    finalizedStatus: "ready_partial",
  });

  const sync = client.calls.find((call) => call.sql.includes("finalized-profile-store:sync-run"));
  assert.match(sync.sql, /publication_finalized_status/);
  assert.match(sync.sql, /publication_finalized_at/);
  assert.deepEqual(sync.params, [
    "run:ready-partial",
    "done",
    null,
    true,
    "ready_partial",
    FINALIZABLE_CHANNEL_STATUSES,
    null,
  ]);
  assert.match(sync.sql, /SET status=\$2/);
  assert.match(sync.sql, /channel\.status=ANY\(\$6::text\[\]\)/);
});

test("deduplicated Finalize cannot synchronize or reconcile a mismatched Run", async () => {
  const client = clientFixture({ synchronize: false });
  const result = await commitFinalizedProfile(client, input({
    deduplicated: true,
    runId: "run:another-channel",
    profile: undefined,
    quality: undefined,
  }));

  const sync = client.calls.find((call) => call.sql.includes("finalized-profile-store:sync-run"));
  assert.match(sync.sql, /run\.channel_id=\$7/);
  assert.equal(sync.params[6], "UCfull");
  assert.equal(result.skip_reason, "stale_run_race");
  assert.equal(result.publication, null);
  assert.equal(
    client.calls.some((call) => call.sql.includes("publication-auto-onboarding:find-owner")),
    false,
  );
});

test("Run synchronization cannot update a Run that appeared after lock lookup", async () => {
  const client = clientFixture();
  client.query = async function query(sql, params = []) {
    this.calls.push({ sql: String(sql), params });
    if (String(sql).includes("publication-channel-mutation-lock:run")) {
      return { rowCount: 0, rows: [] };
    }
    return { rowCount: 1, rows: [] };
  };

  const result = await synchronizeFinalizedRun(client, {
    runId: "run:not-visible-at-lock",
    finalizedStatus: "ready_auto",
  });

  assert.equal(result.rowCount, 0);
  assert.equal(
    client.calls.some((call) => call.sql.includes("finalized-profile-store:sync-run")),
    false,
  );
});
