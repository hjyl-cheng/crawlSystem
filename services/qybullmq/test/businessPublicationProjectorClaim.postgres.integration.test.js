import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import test from "node:test";
import pg from "pg";
import { claimBusinessPublicationProjectionRows } from "../src/businessPublicationProjector.js";

const { Pool } = pg;
const integrationUrl = process.env.PUBLICATION_BUSINESS_PROJECTION_POSTGRES_TEST_URL;

function quoteIdentifier(value) {
  return `"${String(value).replaceAll('"', '""')}"`;
}

function databaseUrl(connectionString, databaseName) {
  const url = new URL(connectionString);
  url.pathname = `/${databaseName}`;
  return url.toString();
}

const CLAIM_SCHEMA = `
  CREATE SCHEMA publication;

  CREATE TABLE publication.channel_ownership (
    channel_id TEXT PRIMARY KEY,
    active_publication_stream_id UUID NOT NULL,
    status TEXT NOT NULL,
    projection_mode TEXT NOT NULL
  );

  CREATE TABLE publication.projection_outbox (
    projection_id UUID PRIMARY KEY,
    publication_stream_id UUID NOT NULL,
    channel_id TEXT NOT NULL,
    status TEXT NOT NULL,
    attempts INTEGER NOT NULL DEFAULT 0,
    next_attempt_at TIMESTAMPTZ NOT NULL,
    lease_owner TEXT,
    lease_expires_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
  );

  CREATE INDEX projection_claim_fixture
  ON publication.projection_outbox (status,next_attempt_at,created_at,projection_id);

  CREATE INDEX idx_business_publication_projection_predecessor
  ON publication.projection_outbox (
    channel_id,publication_stream_id,created_at,projection_id
  );
`;

async function insertFixture(pool) {
  const streamA = randomUUID();
  const streamB = randomUUID();
  const channels = {
    a1: `claim-a1-${randomUUID()}`,
    a2: `claim-a2-${randomUUID()}`,
    b1: `claim-b1-${randomUUID()}`,
    b2: `claim-b2-${randomUUID()}`,
    mismatch: `claim-mismatch-${randomUUID()}`,
  };
  const rows = [
    { key: "mismatch", stream: streamA, ownerStream: streamB, createdAt: "1999-12-31T23:59:59Z" },
    { key: "a1", stream: streamA, ownerStream: streamA, createdAt: "2000-01-01T00:00:00Z" },
    {
      key: "a1Later",
      channelKey: "a1",
      stream: streamA,
      ownerStream: streamA,
      createdAt: "2000-01-01T00:00:00.500Z",
    },
    { key: "a2", stream: streamA, ownerStream: streamA, createdAt: "2000-01-01T00:00:01Z" },
    { key: "b1", stream: streamB, ownerStream: streamB, createdAt: "2000-01-01T00:00:02Z" },
    { key: "b2", stream: streamB, ownerStream: streamB, createdAt: "2000-01-01T00:00:03Z" },
  ].map((row) => ({
    ...row,
    projectionId: randomUUID(),
    channelId: channels[row.channelKey ?? row.key],
  }));

  for (const row of rows) {
    await pool.query(
      `INSERT INTO publication.channel_ownership (
         channel_id,active_publication_stream_id,status,projection_mode
       ) VALUES ($1,$2,'active','online')
       ON CONFLICT (channel_id) DO NOTHING`,
      [row.channelId, row.ownerStream],
    );
    await pool.query(
      `INSERT INTO publication.projection_outbox (
         projection_id,publication_stream_id,channel_id,status,attempts,
         next_attempt_at,created_at
       ) VALUES ($1,$2,$3,'pending',0,'2000-01-01T00:00:00Z',$4)`,
      [row.projectionId, row.stream, row.channelId, row.createdAt],
    );
  }
  return { streamA, streamB, rows };
}

test("Projector claim is single-Stream and concurrency-safe in PostgreSQL", {
  skip: !integrationUrl,
  timeout: 60000,
}, async () => {
  const databaseName = `qy_projector_claim_${randomUUID().replaceAll("-", "")}_test`;
  assert.match(databaseName, /_test$/i);
  const adminPool = new Pool({ connectionString: databaseUrl(integrationUrl, "postgres"), max: 1 });
  let pool;
  try {
    await adminPool.query(`CREATE DATABASE ${quoteIdentifier(databaseName)}`);
    pool = new Pool({ connectionString: databaseUrl(integrationUrl, databaseName), max: 5 });
    await pool.query(CLAIM_SCHEMA);
    const fixture = await insertFixture(pool);

    const single = await pool.connect();
    try {
      await single.query("BEGIN");
      const claimed = await claimBusinessPublicationProjectionRows(single, {
        batchSize: 25,
        workerId: "postgres-single-stream",
        leaseSeconds: 300,
      });
      assert.equal(claimed.length, 2);
      assert.deepEqual(
        [...new Set(claimed.map((row) => String(row.publication_stream_id)))],
        [fixture.streamA],
      );
      const state = (await single.query(
        `SELECT channel_id,status,attempts
         FROM publication.projection_outbox ORDER BY created_at`,
      )).rows;
      assert.deepEqual(state.map((row) => [row.status, Number(row.attempts)]), [
        ["pending", 0],
        ["leased", 1],
        ["pending", 0],
        ["leased", 1],
        ["pending", 0],
        ["pending", 0],
      ]);
      await single.query("ROLLBACK");
    } finally {
      single.release();
    }

    const first = await pool.connect();
    const second = await pool.connect();
    try {
      await Promise.all([first.query("BEGIN"), second.query("BEGIN")]);
      const [firstClaim, secondClaim] = await Promise.all([
        claimBusinessPublicationProjectionRows(first, {
          batchSize: 2,
          workerId: "postgres-concurrent-a",
          leaseSeconds: 300,
        }),
        claimBusinessPublicationProjectionRows(second, {
          batchSize: 2,
          workerId: "postgres-concurrent-b",
          leaseSeconds: 300,
        }),
      ]);
      assert.equal(firstClaim.length, 2);
      assert.equal(secondClaim.length, 2);
      for (const claim of [firstClaim, secondClaim]) {
        assert.equal(new Set(claim.map((row) => String(row.publication_stream_id))).size, 1);
      }
      assert.deepEqual(
        new Set([...firstClaim, ...secondClaim].map((row) => String(row.publication_stream_id))),
        new Set([fixture.streamA, fixture.streamB]),
      );
      assert.equal(
        new Set([...firstClaim, ...secondClaim].map((row) => row.projection_id)).size,
        4,
      );
      assert.equal(
        [...firstClaim, ...secondClaim].some((row) => (
          row.channel_id === fixture.rows.find((item) => item.key === "mismatch").channelId
        )),
        false,
      );
    } finally {
      await Promise.all([
        first.query("ROLLBACK").catch(() => {}),
        second.query("ROLLBACK").catch(() => {}),
      ]);
      first.release();
      second.release();
    }
  } finally {
    if (pool) await pool.end().catch(() => {});
    await adminPool.query(`DROP DATABASE IF EXISTS ${quoteIdentifier(databaseName)} WITH (FORCE)`)
      .catch(() => {});
    await adminPool.end().catch(() => {});
  }
});

test("Projector claim preserves blocking statuses and isolates predecessor Streams", {
  skip: !integrationUrl,
  timeout: 60000,
}, async () => {
  const databaseName = `qy_projector_claim_status_${randomUUID().replaceAll("-", "")}_test`;
  assert.match(databaseName, /_test$/i);
  const adminPool = new Pool({ connectionString: databaseUrl(integrationUrl, "postgres"), max: 1 });
  let pool;
  try {
    await adminPool.query(`CREATE DATABASE ${quoteIdentifier(databaseName)}`);
    pool = new Pool({ connectionString: databaseUrl(integrationUrl, databaseName), max: 2 });
    await pool.query(CLAIM_SCHEMA);
    const streamA = randomUUID();
    const streamB = randomUUID();
    const channels = Object.fromEntries([
      "held",
      "dead",
      "delivered",
      "old-stream",
      "expired-lease",
    ].map((key) => [key, `claim-${key}-${randomUUID()}`]));
    for (const channelId of Object.values(channels)) {
      await pool.query(
        `INSERT INTO publication.channel_ownership (
           channel_id,active_publication_stream_id,status,projection_mode
         ) VALUES ($1,$2,'active','online')`,
        [channelId, streamA],
      );
    }
    const rows = [
      [channels.held, streamA, "held_shadow", "2000-01-01T00:00:00Z", null],
      [channels.held, streamA, "pending", "2000-01-01T00:00:01Z", null],
      [channels.dead, streamA, "dead_letter", "2000-01-01T00:00:00Z", null],
      [channels.dead, streamA, "pending", "2000-01-01T00:00:01Z", null],
      [channels.delivered, streamA, "delivered", "2000-01-01T00:00:00Z", null],
      [channels.delivered, streamA, "pending", "2000-01-01T00:00:01Z", null],
      [channels["old-stream"], streamB, "dead_letter", "2000-01-01T00:00:00Z", null],
      [channels["old-stream"], streamA, "pending", "2000-01-01T00:00:01Z", null],
      [channels["expired-lease"], streamA, "leased", "2000-01-01T00:00:00Z", "2000-01-01T00:00:00Z"],
    ];
    for (const [channelId, streamId, status, createdAt, leaseExpiresAt] of rows) {
      await pool.query(
        `INSERT INTO publication.projection_outbox (
           projection_id,publication_stream_id,channel_id,status,attempts,
           next_attempt_at,lease_owner,lease_expires_at,created_at
         ) VALUES ($1,$2,$3,$4,0,'2000-01-01T00:00:00Z',$5,$6,$7)`,
        [
          randomUUID(),
          streamId,
          channelId,
          status,
          status === "leased" ? "expired-owner" : null,
          leaseExpiresAt,
          createdAt,
        ],
      );
    }

    const client = await pool.connect();
    try {
      await client.query("BEGIN");
      const claimed = await claimBusinessPublicationProjectionRows(client, {
        batchSize: 25,
        workerId: "postgres-status-semantics",
        leaseSeconds: 300,
      });
      assert.deepEqual(
        new Set(claimed.map((row) => row.channel_id)),
        new Set([channels.delivered, channels["old-stream"], channels["expired-lease"]]),
      );
      assert.equal(claimed.some((row) => row.channel_id === channels.held), false);
      assert.equal(claimed.some((row) => row.channel_id === channels.dead), false);
      await client.query("ROLLBACK");
    } finally {
      client.release();
    }
  } finally {
    if (pool) await pool.end().catch(() => {});
    await adminPool.query(`DROP DATABASE IF EXISTS ${quoteIdentifier(databaseName)} WITH (FORCE)`)
      .catch(() => {});
    await adminPool.end().catch(() => {});
  }
});

test("Projector claim stays bounded during a stale-statistics Outbox burst", {
  skip: !integrationUrl,
  timeout: 60000,
}, async () => {
  const databaseName = `qy_projector_claim_volume_${randomUUID().replaceAll("-", "")}_test`;
  assert.match(databaseName, /_test$/i);
  const adminPool = new Pool({ connectionString: databaseUrl(integrationUrl, "postgres"), max: 1 });
  let pool;
  try {
    await adminPool.query(`CREATE DATABASE ${quoteIdentifier(databaseName)}`);
    pool = new Pool({ connectionString: databaseUrl(integrationUrl, databaseName), max: 2 });
    await pool.query(CLAIM_SCHEMA);
    const streamId = randomUUID();
    await pool.query(
      `INSERT INTO publication.projection_outbox (
         projection_id,publication_stream_id,channel_id,status,attempts,
         next_attempt_at,created_at
       )
       SELECT (substr(hash,1,8)||'-'||substr(hash,9,4)||'-4'||substr(hash,14,3)
                 ||'-8'||substr(hash,18,3)||'-'||substr(hash,21,12))::uuid,
              $1::uuid,'volume-channel-'||(1+(position%20000)),'delivered',0,
              '2000-01-01T00:00:00Z',
              timestamp '2000-01-01T00:00:00Z'+position*interval '1 millisecond'
       FROM (
         SELECT position,md5('delivered-'||position) AS hash
         FROM generate_series(1,130000) position
       ) source`,
      [streamId],
    );
    await pool.query("ANALYZE publication.projection_outbox");
    await pool.query(
      `INSERT INTO publication.channel_ownership (
         channel_id,active_publication_stream_id,status,projection_mode
       )
       SELECT 'volume-channel-'||position,$1::uuid,'active','online'
       FROM generate_series(1,6000) position`,
      [streamId],
    );
    await pool.query(
      `INSERT INTO publication.projection_outbox (
         projection_id,publication_stream_id,channel_id,status,attempts,
         next_attempt_at,created_at
       )
       SELECT (substr(hash,1,8)||'-'||substr(hash,9,4)||'-4'||substr(hash,14,3)
                 ||'-8'||substr(hash,18,3)||'-'||substr(hash,21,12))::uuid,
              $1::uuid,'volume-channel-'||position,'pending',0,
              '2000-01-01T00:00:00Z',
              timestamp '2001-01-01T00:00:00Z'+position*interval '1 millisecond'
       FROM (
         SELECT position,md5('pending-'||position) AS hash
         FROM generate_series(1,6000) position
       ) source`,
      [streamId],
    );

    const client = await pool.connect();
    try {
      await client.query("BEGIN");
      await client.query("SET LOCAL statement_timeout='5s'");
      const startedAt = Date.now();
      const claimed = await claimBusinessPublicationProjectionRows(client, {
        batchSize: 25,
        workerId: "postgres-volume-regression",
        leaseSeconds: 300,
      });
      const durationMs = Date.now() - startedAt;
      assert.equal(claimed.length, 25);
      assert.ok(durationMs < 5000, `Claim took ${durationMs}ms`);
      await client.query("ROLLBACK");
    } finally {
      client.release();
    }
    const attempts = await pool.query(
      "SELECT max(attempts)::int AS maximum_attempts FROM publication.projection_outbox",
    );
    assert.equal(Number(attempts.rows[0].maximum_attempts), 0);
  } finally {
    if (pool) await pool.end().catch(() => {});
    await adminPool.query(`DROP DATABASE IF EXISTS ${quoteIdentifier(databaseName)} WITH (FORCE)`)
      .catch(() => {});
    await adminPool.end().catch(() => {});
  }
});
