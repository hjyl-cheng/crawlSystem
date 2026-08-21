import assert from "node:assert/strict";
import test from "node:test";
import {
  PostgresBusinessPublicationProjector,
  projectBusinessPublicationChannels,
} from "../src/businessPublicationProjector.js";

const STREAM_A = "11111111-1111-4111-8111-111111111111";
const STREAM_B = "22222222-2222-4222-8222-222222222222";
const WATERMARK = "publication_projection_existing";
const VERSION_VECTOR_A = Object.freeze({
  channel: {
    publication_stream_id: STREAM_A,
    sequence: 1,
    revision_id: "aaaaaaaa-0000-4000-8000-000000000001",
    result_hash: "sha256:channel-a",
  },
  video: null,
  agent: null,
});
const VERSION_VECTOR_B = Object.freeze({
  channel: {
    publication_stream_id: STREAM_B,
    sequence: 1,
    revision_id: "bbbbbbbb-0000-4000-8000-000000000001",
    result_hash: "sha256:channel-b",
  },
  video: null,
  agent: null,
});

function result(rows = []) {
  return { rows, rowCount: rows.length };
}

function mixedStreamPool({ projectionError = null, projectionRows = null } = {}) {
  const projections = (projectionRows ?? [
    {
      projection_id: "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
      activation_id: "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaa01",
      publication_stream_id: STREAM_A,
      channel_id: "channel-a",
      version_vector: VERSION_VECTOR_A,
      status: "pending",
      attempts: 0,
      created_at: new Date("2026-08-04T00:00:00.000Z"),
      released_by_cutover_id: null,
    },
    {
      projection_id: "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb",
      activation_id: "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbb01",
      publication_stream_id: STREAM_B,
      channel_id: "channel-b",
      version_vector: VERSION_VECTOR_B,
      status: "pending",
      attempts: 0,
      created_at: new Date("2026-08-04T00:00:01.000Z"),
      released_by_cutover_id: null,
    },
  ]).map((row) => ({ ...row }));
  const claimed = [];
  const retried = [];
  const publishedVersionVectors = new Map();
  for (const projection of projections) {
    if (!publishedVersionVectors.has(projection.channel_id)) {
      publishedVersionVectors.set(projection.channel_id, projection.version_vector);
    }
  }

  const claimClient = {
    async query(sql, params = []) {
      const text = String(sql);
      if (text.includes("business-publication-projector:claim")) {
        const eligible = projections.filter((row) => (
          ["pending", "retry_wait"].includes(row.status)
            && !projections.some((predecessor) => (
              predecessor.channel_id === row.channel_id
                && predecessor.publication_stream_id === row.publication_stream_id
                && predecessor.status !== "delivered"
                && (
                  predecessor.created_at < row.created_at
                    || (
                      predecessor.created_at.getTime() === row.created_at.getTime()
                        && predecessor.projection_id < row.projection_id
                    )
                )
            ))
        )).sort((left, right) => (
          left.created_at - right.created_at
            || left.projection_id.localeCompare(right.projection_id)
        ));
        const selectedStream = eligible[0]?.publication_stream_id;
        const rows = text.includes("selected_stream")
          ? eligible
            .filter((row) => row.publication_stream_id === selectedStream)
            .slice(0, Number(params[0]))
          : eligible;
        claimed.push(...rows.map((row) => row.projection_id));
        for (const row of rows) {
          row.status = "leased";
          row.attempts += 1;
        }
        return result(rows.map((row) => ({
          projection_id: row.projection_id,
          publication_stream_id: row.publication_stream_id,
          channel_id: row.channel_id,
          attempts: row.attempts,
        })));
      }
      return result();
    },
    release() {},
  };

  const projectionClient = {
    async query(sql, params = []) {
      const text = String(sql);
      const channelIds = params[0] ?? [];
      if (text.includes("business-publication-projector:ownership")) {
        if (projectionError) throw projectionError;
        return result(channelIds.map((channelId) => {
          const projection = projections.find((row) => row.channel_id === channelId);
          return {
            channel_id: channelId,
            active_publication_stream_id: projection.publication_stream_id,
            status: "active",
            projection_mode: "online",
          };
        }));
      }
      if (text.includes("business-publication-projector:open-outbox")) {
        return result(projections.filter((row) => (
          channelIds.includes(row.channel_id) && row.status !== "delivered"
        )));
      }
      if (text.includes("business-publication-projector:version-vectors")) {
        return result(channelIds.flatMap((channelId) => {
          const vector = projections.find((row) => row.channel_id === channelId).version_vector;
          return [{
            channel_id: channelId,
            domain: "channel",
            publication_stream_id: vector.channel.publication_stream_id,
            active_sequence: vector.channel.sequence,
            active_revision_id: vector.channel.revision_id,
            active_result_hash: vector.channel.result_hash,
          }];
        }));
      }
      if (text.includes("business-publication-projector:published-version-vectors")) {
        return result(channelIds.map((channelId) => ({
          channel_id: channelId,
          version_vector: publishedVersionVectors.get(channelId),
        })));
      }
      if (text.includes("business-publication-projector:projected-version-vector-coverage")) {
        const targets = JSON.parse(params[0]);
        return result(targets.map((target) => ({
          channel_id: target.channel_id,
          batch_id: WATERMARK,
          action: "upsert",
          snapshot_id: `snapshot-${target.channel_id}`,
        })));
      }
      if (text.includes("SELECT watermark FROM public.creator_search_active")) {
        return result([{ watermark: WATERMARK }]);
      }
      if (text.includes("business-publication-projector:delivered")) {
        const delivered = projections.filter((row) => params[0].includes(row.projection_id));
        for (const row of delivered) row.status = "delivered";
        return result(delivered.map((row) => ({
            projection_id: row.projection_id,
            released_by_cutover_id: null,
          })));
      }
      return result();
    },
    release() {},
  };

  let connectionCount = 0;
  return {
    claimed,
    projections,
    retried,
    setPublishedVersionVector(channelId, versionVector) {
      publishedVersionVectors.set(channelId, versionVector);
    },
    async connect() {
      const client = connectionCount % 2 === 0 ? claimClient : projectionClient;
      connectionCount += 1;
      return client;
    },
    async query(sql, params = []) {
      if (String(sql).includes("business-publication-projector:retry")) {
        retried.push(...params[0]);
        for (const row of projections.filter((item) => params[0].includes(item.projection_id))) {
          row.status = "retry_wait";
        }
        return result(params[0].map(() => ({ status: "retry_wait" })));
      }
      return result();
    },
  };
}

function fixtureProjection({ key, streamId, channelId, createdAt }) {
  return {
    projection_id: `projection-${key}`,
    activation_id: `activation-${key}`,
    publication_stream_id: streamId,
    channel_id: channelId,
    version_vector: {
      channel: {
        publication_stream_id: streamId,
        sequence: 1,
        revision_id: `revision-${key}`,
        result_hash: `sha256:${key}`,
      },
      video: null,
      agent: null,
    },
    status: "pending",
    attempts: 0,
    created_at: new Date(createdAt),
    released_by_cutover_id: null,
  };
}

test("Projector Worker claims only one Publication Stream from mixed eligible work", async () => {
  const pool = mixedStreamPool();
  const projector = new PostgresBusinessPublicationProjector(pool, {
    workerId: "mixed-stream-regression",
    batchSize: 25,
  });

  const summary = await projector.runOnce();

  assert.equal(summary.outcome, "covered_by_current");
  assert.equal(summary.publication_stream_id, STREAM_A);
  assert.equal(summary.claimed, 1);
  assert.equal(summary.channels, 1);
  assert.equal(summary.delivered, 1);
  assert.deepEqual(pool.claimed, ["aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"]);
  assert.deepEqual(pool.retried, []);
});

test("Projector Worker eventually processes each eligible Publication Stream", async () => {
  const pool = mixedStreamPool();
  const projector = new PostgresBusinessPublicationProjector(pool, {
    workerId: "mixed-stream-fairness",
    batchSize: 25,
  });

  const first = await projector.runOnce();
  const second = await projector.runOnce();

  assert.equal(first.publication_stream_id, STREAM_A);
  assert.equal(second.publication_stream_id, STREAM_B);
  assert.deepEqual(pool.claimed, [
    "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
    "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb",
  ]);
  assert.deepEqual(pool.projections.map((row) => row.status), ["delivered", "delivered"]);
});

test("Projector does not starve a smaller Stream behind multiple full batches", async () => {
  const mainRows = Array.from({ length: 51 }, (_, index) => fixtureProjection({
    key: `main-${String(index).padStart(2, "0")}`,
    streamId: STREAM_A,
    channelId: `channel-main-${String(index).padStart(2, "0")}`,
    createdAt: `2026-08-04T00:00:${String(index).padStart(2, "0")}.000Z`,
  }));
  const recoveryRow = fixtureProjection({
    key: "recovery",
    streamId: STREAM_B,
    channelId: "channel-recovery",
    createdAt: "2026-08-04T00:01:00.000Z",
  });
  const pool = mixedStreamPool({ projectionRows: [...mainRows, recoveryRow] });
  const projector = new PostgresBusinessPublicationProjector(pool, {
    workerId: "mixed-stream-volume-fairness",
    batchSize: 25,
  });

  const summaries = [];
  while (pool.projections.some((row) => row.status !== "delivered")) {
    summaries.push(await projector.runOnce());
  }

  assert.deepEqual(summaries.map((summary) => summary.publication_stream_id), [
    STREAM_A,
    STREAM_A,
    STREAM_A,
    STREAM_B,
  ]);
  assert.deepEqual(summaries.map((summary) => summary.claimed), [25, 25, 1, 1]);
  assert.equal(pool.projections.every((row) => row.status === "delivered"), true);
});

test("Projector coalesces same-Channel rows and delivers every claimed row", async () => {
  const first = fixtureProjection({
    key: "coalesced-first",
    streamId: STREAM_A,
    channelId: "channel-coalesced",
    createdAt: "2026-08-04T00:00:00.000Z",
  });
  const second = {
    ...fixtureProjection({
      key: "coalesced-second",
      streamId: STREAM_A,
      channelId: "channel-coalesced",
      createdAt: "2026-08-04T00:00:01.000Z",
    }),
    version_vector: first.version_vector,
  };
  const pool = mixedStreamPool({ projectionRows: [first, second] });
  const projector = new PostgresBusinessPublicationProjector(pool, {
    workerId: "same-channel-coalescing",
    batchSize: 25,
  });

  const summary = await projector.runOnce();

  assert.equal(summary.claimed, 1);
  assert.equal(summary.channels, 1);
  assert.equal(summary.delivered, 2);
  assert.equal(summary.covered, 1);
  assert.equal(pool.projections.every((row) => row.status === "delivered"), true);
});

test("Projector preserves different Version Vectors from the same Channel", async () => {
  const first = fixtureProjection({
    key: "causal-first",
    streamId: STREAM_A,
    channelId: "channel-causal",
    createdAt: "2026-08-04T00:00:00.000Z",
  });
  const second = fixtureProjection({
    key: "causal-second",
    streamId: STREAM_A,
    channelId: "channel-causal",
    createdAt: "2026-08-04T00:00:01.000Z",
  });
  second.version_vector.channel.sequence = 2;
  const pool = mixedStreamPool({ projectionRows: [first, second] });
  const projector = new PostgresBusinessPublicationProjector(pool, {
    workerId: "same-channel-causal-order",
    batchSize: 25,
  });

  const firstSummary = await projector.runOnce();

  assert.equal(firstSummary.claimed, 1);
  assert.equal(firstSummary.delivered, 1);
  assert.deepEqual(pool.projections.map((row) => row.status), ["delivered", "pending"]);

  pool.setPublishedVersionVector(second.channel_id, second.version_vector);
  const secondSummary = await projector.runOnce();

  assert.equal(secondSummary.claimed, 1);
  assert.equal(secondSummary.delivered, 1);
  assert.deepEqual(pool.projections.map((row) => row.status), ["delivered", "delivered"]);
});

test("Projector treats an already absent retracted Channel as covered", async () => {
  const channelId = "channel-already-absent";
  const versionVector = {
    channel: {
      publication_stream_id: STREAM_A,
      sequence: 2,
      revision_id: "aaaaaaaa-0000-4000-8000-000000000002",
      result_hash: `sha256:${"a".repeat(64)}`,
    },
    video: null,
    agent: null,
  };
  const projection = {
    projection_id: "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaa2",
    activation_id: "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaa02",
    publication_stream_id: STREAM_A,
    channel_id: channelId,
    version_vector: versionVector,
    status: "leased",
    attempts: 1,
    created_at: new Date("2026-08-21T00:00:00.000Z"),
    released_by_cutover_id: null,
  };
  const statements = [];
  const client = {
    async query(sql, params = []) {
      const text = String(sql);
      statements.push(text);
      if (text.includes("business-publication-projector:ownership")) {
        return result([{
          channel_id: channelId,
          active_publication_stream_id: STREAM_A,
          status: "active",
          projection_mode: "online",
        }]);
      }
      if (text.includes("business-publication-projector:open-outbox")) {
        return result([projection]);
      }
      if (text.includes("pg_advisory_xact_lock")) return result();
      if (text.includes("business-publication-projector:published-version-vectors")) {
        return result([{ channel_id: channelId, version_vector: null }]);
      }
      if (text.includes("business-publication-projector:covered-retractions")) {
        assert.equal(JSON.parse(params[0])[0].channel_id, channelId);
        return result([{ channel_id: channelId }]);
      }
      if (text.includes("SELECT watermark FROM public.creator_search_active")) {
        return result([{ watermark: WATERMARK }]);
      }
      if (text.includes("business-publication-projector:delivered")) {
        assert.deepEqual(params[0], [projection.projection_id]);
        return result([{
          projection_id: projection.projection_id,
          released_by_cutover_id: null,
        }]);
      }
      throw new Error(`unexpected SQL for already absent retraction: ${text}`);
    },
  };

  const projected = await projectBusinessPublicationChannels(client, [channelId]);

  assert.equal(projected.outcome, "covered_by_current");
  assert.equal(projected.covered, 1);
  assert.equal(projected.projected, 0);
  assert.equal(projected.delivered, 1);
  assert.equal(
    statements.some((statement) => statement.includes("business-publication-projector:import-batch")),
    false,
  );
});

test("Projector retry changes only rows from the selected Publication Stream", async () => {
  const pool = mixedStreamPool({ projectionError: new Error("fixture projection failure") });
  const projector = new PostgresBusinessPublicationProjector(pool, {
    workerId: "mixed-stream-retry-isolation",
    batchSize: 25,
  });

  const summary = await projector.runOnce();

  assert.equal(summary.outcome, "failed");
  assert.equal(summary.publication_stream_id, STREAM_A);
  assert.deepEqual(pool.claimed, ["aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"]);
  assert.deepEqual(pool.retried, ["aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"]);
  assert.deepEqual(pool.projections.map((row) => row.status), ["retry_wait", "pending"]);
});

test("Projector rolls back a claim if the database returns more than one Stream", async () => {
  const statements = [];
  const claimClient = {
    async query(sql) {
      statements.push(String(sql));
      if (String(sql).includes("business-publication-projector:claim")) {
        return result([
          { projection_id: "a", publication_stream_id: STREAM_A, channel_id: "channel-a", attempts: 1 },
          { projection_id: "b", publication_stream_id: STREAM_B, channel_id: "channel-b", attempts: 1 },
        ]);
      }
      return result();
    },
    release() {},
  };
  const pool = {
    async connect() { return claimClient; },
    async query() { throw new Error("retry must not run for a rolled-back claim"); },
  };
  const projector = new PostgresBusinessPublicationProjector(pool, {
    workerId: "mixed-stream-defense",
  });

  await assert.rejects(projector.runOnce(), /claim crossed Publication Streams/);
  assert.equal(statements[0], "BEGIN");
  assert.match(statements[1], /set_config\('statement_timeout'/);
  assert.match(statements[1], /set_config\('lock_timeout'/);
  assert.match(statements[2], /business-publication-projector:claim/);
  assert.equal(statements[3], "ROLLBACK");
  assert.equal(statements.includes("COMMIT"), false);
});

test("Projector rolls back a timed-out Claim without retrying or consuming attempts", async () => {
  const statements = [];
  let retryCalled = false;
  const claimClient = {
    async query(sql) {
      const text = String(sql);
      statements.push(text);
      if (text.includes("business-publication-projector:claim")) {
        const error = new Error("canceling statement due to statement timeout");
        error.code = "57014";
        throw error;
      }
      return result();
    },
    release() {},
  };
  const pool = {
    async connect() { return claimClient; },
    async query() {
      retryCalled = true;
      return result();
    },
  };
  const projector = new PostgresBusinessPublicationProjector(pool, {
    workerId: "claim-timeout-regression",
    claimStatementTimeoutMs: 15000,
  });

  await assert.rejects(projector.runOnce(), /statement timeout/);
  assert.deepEqual(statements.slice(-2), [
    statements.find((statement) => statement.includes("business-publication-projector:claim")),
    "ROLLBACK",
  ]);
  assert.equal(retryCalled, false);
});

test("Projection rejects an Outbox row that no longer matches active Ownership", async () => {
  const client = {
    async query(sql) {
      const text = String(sql);
      if (text.includes("business-publication-projector:ownership")) {
        return result([{
          channel_id: "channel-a",
          active_publication_stream_id: STREAM_A,
          status: "active",
          projection_mode: "online",
        }]);
      }
      if (text.includes("business-publication-projector:open-outbox")) {
        return result([{
          projection_id: "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb",
          publication_stream_id: STREAM_B,
          channel_id: "channel-a",
          status: "leased",
          version_vector: VERSION_VECTOR_B,
        }]);
      }
      throw new Error(`unexpected SQL after Stream mismatch: ${text}`);
    },
  };

  await assert.rejects(
    projectBusinessPublicationChannels(client, ["channel-a"]),
    /Outbox must match the active Publication Stream/,
  );
});

test("Historical Projection may batch old Vectors across different current Owner Streams", async () => {
  const channelIds = ["channel-old-owner", "channel-recovered-owner"];
  const versionVectors = Object.fromEntries(channelIds.map((channelId, index) => [
    channelId,
    {
      channel: {
        publication_stream_id: STREAM_A,
        sequence: index + 1,
        revision_id: `historical-revision-${index + 1}`,
        result_hash: `sha256:historical-${index + 1}`,
      },
      video: null,
      agent: null,
    },
  ]));
  const client = {
    async query(sql, params = []) {
      const text = String(sql);
      if (text.includes("business-publication-projector:ownership")) {
        return result([
          {
            channel_id: channelIds[0],
            active_publication_stream_id: STREAM_A,
            status: "active",
            projection_mode: "online",
          },
          {
            channel_id: channelIds[1],
            active_publication_stream_id: STREAM_B,
            status: "active",
            projection_mode: "online",
          },
        ]);
      }
      if (text.includes("business-publication-projector:open-outbox")) return result();
      if (text.includes("pg_advisory_xact_lock")) return result();
      if (text.includes("business-publication-projector:projected-version-vector-coverage")) {
        return result(JSON.parse(params[0]).map((target) => ({
          channel_id: target.channel_id,
          batch_id: WATERMARK,
          action: "upsert",
          snapshot_id: `snapshot-${target.channel_id}`,
        })));
      }
      if (text.includes("SELECT watermark FROM public.creator_search_active")) {
        return result([{ watermark: WATERMARK }]);
      }
      throw new Error(`unexpected SQL in Historical Projection fixture: ${text}`);
    },
  };

  const projected = await projectBusinessPublicationChannels(client, channelIds, {
    markOutbox: false,
    projectionStatuses: [],
    versionVectors,
    historical: true,
  });

  assert.equal(projected.outcome, "covered_by_current");
  assert.equal(projected.publication_stream_id, STREAM_A);
  assert.equal(projected.covered, 2);
});
