import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import test from "node:test";
import pg from "pg";
import {
  PublicationCurrentSeedConflict,
  seedPublicationCurrents,
} from "../src/publicationCurrentStore.js";
import { publicationResultHash } from "../src/publicationResultHash.js";

const { Pool } = pg;
const integrationUrl = process.env.PUBLICATION_POSTGRES_TEST_URL;

function current(channelId, domain, marker, observedAt = "2026-07-27T01:00:00.000Z") {
  const payload = domain === "video" ? {
    channel_id: channelId,
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
  } : { channel_id: channelId, marker };
  return {
    ready: true,
    contract_version: 1,
    policy_version: domain === "video" ? "video-window-v1" : "publication-policy-v1",
    payload,
    result_hash: publicationResultHash(domain, payload),
    source_refs: {
      cursor: { latest_complete_observed_at: observedAt },
      current: { marker },
    },
    issues: [],
  };
}

async function transaction(pool, action) {
  const client = await pool.connect();
  try {
    await client.query("BEGIN");
    const result = await action(client);
    await client.query("COMMIT");
    return result;
  } catch (error) {
    await client.query("ROLLBACK").catch(() => {});
    throw error;
  } finally {
    client.release();
  }
}

async function seedOwnedChannel(pool, {
  streamId,
  channelId,
  sourceKey,
  createStream = true,
}) {
  await pool.query(
    `INSERT INTO crawler.channels (channel_id,channel_url,title,status)
     VALUES ($1,$2,'Publication Current integration','active')`,
    [channelId, `https://www.youtube.com/channel/${channelId}`],
  );
  if (createStream) {
    await pool.query(
      `INSERT INTO publication.stream (
         publication_stream_id,source_deployment_key,source_identity_json,
         created_by,created_reason,status_changed_by,status_reason
       ) VALUES ($1,$2,$3::jsonb,'integration-test','isolated test',
                 'integration-test','stream registered')`,
      [streamId, sourceKey, JSON.stringify({ database: "isolated-test" })],
    );
  }
  await pool.query(
    `INSERT INTO publication.channel_stream_state (
       publication_stream_id,channel_id,onboarding_mode,
       state_changed_by,state_reason
     ) VALUES ($1,$2,'baseline','integration-test','baseline cohort')`,
    [streamId, channelId],
  );
}

async function cleanup(pool, { streamId, channelId }) {
  await pool.query(
    "DELETE FROM publication.domain_current WHERE publication_stream_id=$1 AND channel_id=$2",
    [streamId, channelId],
  ).catch(() => {});
  await pool.query(
    "DELETE FROM publication.channel_delivery_state WHERE publication_stream_id=$1 AND channel_id=$2",
    [streamId, channelId],
  ).catch(() => {});
  await pool.query(
    "DELETE FROM publication.channel_stream_state WHERE publication_stream_id=$1 AND channel_id=$2",
    [streamId, channelId],
  ).catch(() => {});
  await pool.query("DELETE FROM publication.stream WHERE publication_stream_id=$1", [streamId]).catch(() => {});
  await pool.query("DELETE FROM crawler.channels WHERE channel_id=$1", [channelId]).catch(() => {});
}

test("Publication Current Seed is idempotent and preserves Ready state across NotReady evaluation", {
  skip: !integrationUrl,
}, async () => {
  const pool = new Pool({ connectionString: integrationUrl, max: 4 });
  const suffix = randomUUID().replaceAll("-", "");
  const streamId = randomUUID();
  const channelId = `UCpublication${suffix}`;
  const sourceKey = `publication-integration-${suffix}`;
  try {
    await seedOwnedChannel(pool, { streamId, channelId, sourceKey });

    const initial = await transaction(pool, (client) => seedPublicationCurrents(client, {
      publicationStreamId: streamId,
      channelId,
      currents: {
        agent: current(channelId, "agent", "a"),
        channel: current(channelId, "channel", "c"),
        video: current(channelId, "video", "b"),
      },
    }));
    assert.equal(initial.status, "ready");
    assert.equal(initial.seed_status, "complete");

    const first = await pool.query(
      `SELECT domain,readiness_status,payload_json,result_hash,source_refs,
              complete_observed_at,data_sequence,current_revision_id
       FROM publication.domain_current
       WHERE publication_stream_id=$1 AND channel_id=$2
       ORDER BY CASE domain WHEN 'channel' THEN 1 WHEN 'video' THEN 2 ELSE 3 END`,
      [streamId, channelId],
    );
    assert.deepEqual(first.rows.map((row) => row.domain), ["channel", "video", "agent"]);
    assert.ok(first.rows.every((row) => row.readiness_status === "ready"));
    assert.ok(first.rows.every((row) => String(row.data_sequence) === "0"));
    assert.ok(first.rows.every((row) => row.current_revision_id === null));
    const trustedChannel = structuredClone(first.rows[0]);

    const notReady = await transaction(pool, (client) => seedPublicationCurrents(client, {
      publicationStreamId: streamId,
      channelId,
      currents: {
        channel: {
          ready: false,
          contract_version: 2,
          policy_version: "broken-policy",
          payload: { channel_id: channelId, marker: "x" },
          result_hash: null,
          source_refs: { current: { marker: "failed-evaluation" } },
          issues: [{ domain: "channel", code: "channel_contract_field_incomplete", field: "links" }],
        },
      },
    }));
    assert.equal(notReady.status, "not_ready");
    assert.equal(notReady.seed_status, "complete");

    const retained = (await pool.query(
      `SELECT domain,contract_version,policy_version,readiness_status,
              readiness_reasons,payload_json,result_hash,source_refs,
              complete_observed_at,data_sequence,current_revision_id
       FROM publication.domain_current
       WHERE publication_stream_id=$1 AND channel_id=$2 AND domain='channel'`,
      [streamId, channelId],
    )).rows[0];
    assert.equal(retained.readiness_status, "not_ready");
    assert.equal(retained.contract_version, 1);
    assert.equal(retained.policy_version, "publication-policy-v1");
    assert.deepEqual(retained.payload_json, trustedChannel.payload_json);
    assert.equal(retained.result_hash, trustedChannel.result_hash);
    assert.deepEqual(retained.source_refs, trustedChannel.source_refs);
    assert.equal(retained.complete_observed_at.toISOString(), trustedChannel.complete_observed_at.toISOString());
    assert.equal(retained.readiness_reasons[0].field, "links");

    await assert.rejects(
      transaction(pool, (client) => seedPublicationCurrents(client, {
        publicationStreamId: streamId,
        channelId,
        currents: { channel: current(channelId, "channel", "d", "2026-07-27T02:00:00.000Z") },
      })),
      (error) => error instanceof PublicationCurrentSeedConflict,
    );

    const restored = await transaction(pool, (client) => seedPublicationCurrents(client, {
      publicationStreamId: streamId,
      channelId,
      currents: { channel: current(channelId, "channel", "c", "2026-07-27T02:00:00.000Z") },
    }));
    assert.equal(restored.status, "ready");
    assert.equal(restored.seed_status, "complete");

    await pool.query(
      `UPDATE publication.channel_stream_state
       SET status='sealed',sealed_at=now(),
           final_version_vector='{"channel":0,"video":0,"agent":0}'::jsonb,
           state_changed_at=now(),state_changed_by='integration-test',state_reason='sealed test'
       WHERE publication_stream_id=$1 AND channel_id=$2`,
      [streamId, channelId],
    );
    const sealed = await transaction(pool, (client) => seedPublicationCurrents(client, {
      publicationStreamId: streamId,
      channelId,
      currents: { channel: current(channelId, "channel", "e") },
    }));
    assert.deepEqual(
      { status: sealed.status, reason: sealed.reason },
      { status: "not_owned", reason: "channel_stream_sealed" },
    );
  } finally {
    await cleanup(pool, { streamId, channelId });
    await pool.end();
  }
});

test("Publication Current Seed serializes concurrent writers on Channel ownership", {
  skip: !integrationUrl,
}, async () => {
  const pool = new Pool({ connectionString: integrationUrl, max: 4 });
  const suffix = randomUUID().replaceAll("-", "");
  const streamId = randomUUID();
  const channelId = `UCpublicationlock${suffix}`;
  const sourceKey = `publication-lock-${suffix}`;
  let firstClient;
  let secondClient;
  let firstOpen = false;
  let secondOpen = false;
  try {
    await seedOwnedChannel(pool, { streamId, channelId, sourceKey });
    firstClient = await pool.connect();
    secondClient = await pool.connect();
    await firstClient.query("BEGIN");
    firstOpen = true;
    await secondClient.query("BEGIN");
    secondOpen = true;
    await secondClient.query("SET LOCAL statement_timeout = '5s'");

    await seedPublicationCurrents(firstClient, {
      publicationStreamId: streamId,
      channelId,
      currents: { channel: current(channelId, "channel", "c") },
    });

    let secondFinished = false;
    const secondSeed = seedPublicationCurrents(secondClient, {
      publicationStreamId: streamId,
      channelId,
      currents: { channel: current(channelId, "channel", "d") },
    }).then(
      (result) => {
        secondFinished = true;
        return { result };
      },
      (error) => {
        secondFinished = true;
        return { error };
      },
    );
    await new Promise((resolve) => setTimeout(resolve, 100));
    assert.equal(secondFinished, false, "the second writer must wait for the Channel ownership row lock");

    await firstClient.query("COMMIT");
    firstOpen = false;
    const secondOutcome = await secondSeed;
    assert.ok(secondOutcome.error instanceof PublicationCurrentSeedConflict);
    await secondClient.query("ROLLBACK");
    secondOpen = false;

    const stored = (await pool.query(
      `SELECT payload_json,result_hash,data_sequence
       FROM publication.domain_current
       WHERE publication_stream_id=$1 AND channel_id=$2 AND domain='channel'`,
      [streamId, channelId],
    )).rows[0];
    assert.equal(stored.payload_json.marker, "c");
    assert.equal(stored.result_hash, current(channelId, "channel", "c").result_hash);
    assert.equal(String(stored.data_sequence), "0");
  } finally {
    if (firstOpen) await firstClient?.query("ROLLBACK").catch(() => {});
    if (secondOpen) await secondClient?.query("ROLLBACK").catch(() => {});
    firstClient?.release();
    secondClient?.release();
    await cleanup(pool, { streamId, channelId });
    await pool.end();
  }
});

test("Publication Current Seed does not serialize different Channels in one Stream", {
  skip: !integrationUrl,
}, async () => {
  const pool = new Pool({ connectionString: integrationUrl, max: 4 });
  const suffix = randomUUID().replaceAll("-", "");
  const streamId = randomUUID();
  const firstChannelId = `UCpublicationparallel1${suffix}`;
  const secondChannelId = `UCpublicationparallel2${suffix}`;
  const sourceKey = `publication-parallel-${suffix}`;
  let firstClient;
  let secondClient;
  let firstOpen = false;
  let secondOpen = false;
  try {
    await seedOwnedChannel(pool, {
      streamId,
      channelId: firstChannelId,
      sourceKey,
    });
    await seedOwnedChannel(pool, {
      streamId,
      channelId: secondChannelId,
      sourceKey,
      createStream: false,
    });
    firstClient = await pool.connect();
    secondClient = await pool.connect();
    await firstClient.query("BEGIN");
    firstOpen = true;
    await secondClient.query("BEGIN");
    secondOpen = true;
    await secondClient.query("SET LOCAL statement_timeout = '2s'");

    await seedPublicationCurrents(firstClient, {
      publicationStreamId: streamId,
      channelId: firstChannelId,
      currents: { channel: current(firstChannelId, "channel", "c") },
    });
    const secondResult = await seedPublicationCurrents(secondClient, {
      publicationStreamId: streamId,
      channelId: secondChannelId,
      currents: { channel: current(secondChannelId, "channel", "d") },
    });
    assert.equal(secondResult.status, "ready");

    await secondClient.query("COMMIT");
    secondOpen = false;
    await firstClient.query("COMMIT");
    firstOpen = false;
  } finally {
    if (firstOpen) await firstClient?.query("ROLLBACK").catch(() => {});
    if (secondOpen) await secondClient?.query("ROLLBACK").catch(() => {});
    firstClient?.release();
    secondClient?.release();
    await cleanup(pool, { streamId, channelId: firstChannelId });
    await cleanup(pool, { streamId, channelId: secondChannelId });
    await pool.end();
  }
});

test("Publication Current Seed rejects a Pool without an open transaction", {
  skip: !integrationUrl,
}, async () => {
  const pool = new Pool({ connectionString: integrationUrl, max: 1 });
  const channelId = `UCpublicationpool${randomUUID().replaceAll("-", "")}`;
  try {
    await assert.rejects(
      seedPublicationCurrents(pool, {
        publicationStreamId: randomUUID(),
        channelId,
        currents: { channel: current(channelId, "channel", "c") },
      }),
      /SAVEPOINT can only be used in transaction blocks/,
    );
  } finally {
    await pool.end();
  }
});
