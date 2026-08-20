import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import test from "node:test";
import pg from "pg";
import { observationFactsHash } from "../src/crawlObservationStore.js";
import { reconcilePublication } from "../src/publicationReconciler.js";
import { PUBLICATION_WRITER_VERSION } from "../src/publicationWriterVersion.js";

const { Pool } = pg;
const integrationUrl = process.env.PUBLICATION_POSTGRES_TEST_URL;
const AS_OF = "2026-07-27T12:00:00.000Z";

function publicationPool(max) {
  return new Pool({
    connectionString: integrationUrl,
    max,
    options: `-c publication.writer_version=${PUBLICATION_WRITER_VERSION}`,
  });
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

async function writeOperationalChannel(client, {
  channelId,
  marker,
  sequence,
  observedAt,
}) {
  const factsHash = observationFactsHash({ channel_id: channelId, marker });
  const identityHash = observationFactsHash({ channel_id: channelId, title: `Publication ${marker}` });
  const runId = `publication-${marker}-${randomUUID()}`;
  const observationId = randomUUID();
  const externalLinks = [{
    title: "Website",
    display_url: "example.com",
    target_url: "https://example.com/",
  }];

  await client.query(
    `INSERT INTO crawler.channels (
       channel_id,channel_url,handle,title,country,country_code,country_canonical_name,
       subscriber_count,subscriber_count_text,subscriber_count_status,subscriber_count_source,
       total_view_count,total_view_count_text,total_view_count_status,total_view_count_source,
       total_video_count,total_video_count_text,total_video_count_status,total_video_count_source,
       status,source_json,avatar_url,keywords,keywords_status,available_tabs,
       available_tabs_status,about_description,description_status,joined_date_text,joined_at,
       joined_at_precision,external_links,external_links_status,rss_url,vanity_channel_url,
       is_family_safe,is_verified,is_verified_status,about_identity_last_observed_at,
       about_last_observed_at,about_identity_current_hash,about_current_hash
     ) VALUES (
       $1,$2,'@publication',$3,'Brazil','BR','Brazil',
       1000,'1K subscribers','exact','youtube_about',
       50000,'50,000 views','exact','youtube_about',
       50,'50 videos','exact','youtube_about',
       'active','{}'::jsonb,'https://yt3.example/avatar.jpg',ARRAY['publication','testing'],
       'observed',ARRAY['videos','shorts','live'],'observed',
       'A complete channel description.','exact','Joined Jan 1, 2020','2020-01-01',
       'date_only',$4::jsonb,'observed',$5,'https://www.youtube.com/@publication',
       true,true,'verified',$6::timestamptz,$6::timestamptz,$7,$8
     )
     ON CONFLICT (channel_id) DO UPDATE
     SET title=EXCLUDED.title,
         about_identity_last_observed_at=EXCLUDED.about_identity_last_observed_at,
         about_last_observed_at=EXCLUDED.about_last_observed_at,
         about_identity_current_hash=EXCLUDED.about_identity_current_hash,
         about_current_hash=EXCLUDED.about_current_hash,
         updated_at=now()`,
    [
      channelId,
      `https://www.youtube.com/channel/${channelId}`,
      `Publication ${marker}`,
      JSON.stringify(externalLinks),
      `https://www.youtube.com/feeds/videos.xml?channel_id=${channelId}`,
      observedAt,
      identityHash,
      factsHash,
    ],
  );
  await client.query(
    `INSERT INTO crawler.channel_runs (
       run_id,channel_id,status,crawl_mode,detail_status,trigger_reason,
       policy_version,crawler_version,started_at,finished_at
     ) VALUES ($1,$2,'done','incremental','done','manual',
               'v16-rule-6','integration-test',$3::timestamptz,$3::timestamptz)`,
    [runId, channelId, observedAt],
  );
  await client.query(
    `INSERT INTO crawler.crawl_observations (
       observation_id,observed_at,channel_id,run_id,observation_kind,kind_sequence,
       trigger_reason,outcome,outcome_reason_code,result_summary_json,facts_hash,
       crawler_version,extractor_versions
     ) VALUES (
       $1,$2::timestamptz,$3,$4,'about',$5,'manual','complete','about_complete',
       '{}'::jsonb,$6,'integration-test','{}'::jsonb
     )`,
    [observationId, observedAt, channelId, runId, sequence, factsHash],
  );
  await client.query(
    `INSERT INTO crawler.channel_domain_cursors (
       channel_id,observation_kind,latest_sequence,latest_observation_id,
       latest_observed_at,latest_complete_observation_id,
       latest_complete_observed_at,current_facts_hash
     ) VALUES ($1,'about',$2,$3,$4::timestamptz,$3,$4::timestamptz,$5)
     ON CONFLICT (channel_id,observation_kind) DO UPDATE
     SET latest_sequence=EXCLUDED.latest_sequence,
         latest_observation_id=EXCLUDED.latest_observation_id,
         latest_observed_at=EXCLUDED.latest_observed_at,
         latest_complete_observation_id=EXCLUDED.latest_complete_observation_id,
         latest_complete_observed_at=EXCLUDED.latest_complete_observed_at,
         current_facts_hash=EXCLUDED.current_facts_hash,
         updated_at=now()`,
    [channelId, sequence, observationId, observedAt, factsHash],
  );
}

async function insertStream(pool, { streamId, sourceKey }) {
  await pool.query(
    `INSERT INTO publication.stream (
       publication_stream_id,source_deployment_key,source_identity_json,
       minimum_writer_version,capture_enabled_at,created_by,created_reason,
       status_changed_by,status_reason
     ) VALUES (
       $1,$2,'{"database":"isolated-test"}'::jsonb,'publication-reconciler-v1',now(),
       'integration-test','isolated test','integration-test','capture enabled'
     )`,
    [streamId, sourceKey],
  );
}

async function insertOwnership(pool, {
  streamId,
  channelId,
  onboardingMode = "bootstrap",
  destinations = [],
}) {
  await pool.query(
    `INSERT INTO publication.channel_stream_state (
       publication_stream_id,channel_id,onboarding_mode,state_changed_by,state_reason
     ) VALUES ($1,$2,$3,'integration-test','owned for isolated test')`,
    [streamId, channelId, onboardingMode],
  );
  for (const destination of destinations) {
    if (destination.mode === "online") {
      await pool.query(
        `INSERT INTO publication.channel_delivery_state (
           destination,publication_stream_id,channel_id,mode,latest_successful_baseline_id,
           channel_watermark_sequence,video_watermark_sequence,agent_watermark_sequence,
           online_at,state_changed_by,state_reason
         ) VALUES ($1,$2,$3,'online',$4,0,0,0,now(),'integration-test','online test')`,
        [destination.destination, streamId, channelId, randomUUID()],
      );
    } else {
      await pool.query(
        `INSERT INTO publication.channel_delivery_state (
           destination,publication_stream_id,channel_id,state_changed_by,state_reason
         ) VALUES ($1,$2,$3,'integration-test','hold test')`,
        [destination.destination, streamId, channelId],
      );
    }
  }
}

async function setupChannel(pool, {
  streamId,
  channelId,
  sourceKey,
  marker = "one",
  createStream = true,
  destinations = [],
}) {
  await writeOperationalChannel(pool, {
    channelId,
    marker,
    sequence: 1,
    observedAt: "2026-07-27T09:00:00.000Z",
  });
  if (createStream) await insertStream(pool, { streamId, sourceKey });
  await insertOwnership(pool, { streamId, channelId, destinations });
}

async function reconcileChannel(client, channelId) {
  return reconcilePublication(client, {
    channelId,
    domains: ["channel"],
    asOf: AS_OF,
  });
}

test("Revision, Outbox, and Current commit and roll back with the caller transaction", {
  skip: !integrationUrl,
}, async () => {
  const pool = publicationPool(4);
  const suffix = randomUUID().replaceAll("-", "");
  const streamId = randomUUID();
  const channelId = `UCreconcileatomic${suffix}`;
  try {
    await setupChannel(pool, {
      streamId,
      channelId,
      sourceKey: `reconcile-atomic-${suffix}`,
      destinations: [
        { destination: "business-hold", mode: "hold" },
        { destination: "business-online", mode: "online" },
      ],
    });

    const rollbackClient = await pool.connect();
    try {
      await rollbackClient.query("BEGIN");
      const rolledBack = await reconcileChannel(rollbackClient, channelId);
      assert.equal(rolledBack.revisions[0].data_sequence, 1);
      await rollbackClient.query("ROLLBACK");
    } finally {
      rollbackClient.release();
    }
    const afterRollback = await pool.query(
      `SELECT
         (SELECT count(*)::int FROM publication.revision WHERE publication_stream_id=$1) AS revisions,
         (SELECT count(*)::int FROM publication.outbox outbox
            JOIN publication.revision revision USING (revision_id)
            WHERE revision.publication_stream_id=$1) AS outbox,
         (SELECT count(*)::int FROM publication.domain_current WHERE publication_stream_id=$1) AS currents`,
      [streamId],
    );
    assert.deepEqual(afterRollback.rows[0], { revisions: 0, outbox: 0, currents: 0 });

    const committed = await transaction(pool, (client) => reconcileChannel(client, channelId));
    assert.equal(committed.status, "revised");
    assert.notEqual(committed.revisions[0].occurred_at, AS_OF);
    const stored = await pool.query(
      `SELECT current.data_sequence,current.current_revision_id,current.result_hash,
              revision.revision_type,revision.previous_data_sequence,
              array_agg(outbox.status ORDER BY outbox.destination) AS outbox_statuses
       FROM publication.domain_current current
       JOIN publication.revision revision
         ON revision.revision_id=current.current_revision_id
        AND revision.publication_stream_id=current.publication_stream_id
        AND revision.channel_id=current.channel_id
        AND revision.domain=current.domain
        AND revision.data_sequence=current.data_sequence
        AND revision.result_hash=current.result_hash
       JOIN publication.outbox outbox ON outbox.revision_id=revision.revision_id
       WHERE current.publication_stream_id=$1 AND current.channel_id=$2 AND current.domain='channel'
       GROUP BY current.data_sequence,current.current_revision_id,current.result_hash,
                revision.revision_type,revision.previous_data_sequence`,
      [streamId, channelId],
    );
    assert.equal(String(stored.rows[0].data_sequence), "1");
    assert.equal(stored.rows[0].revision_type, "bootstrap");
    assert.equal(stored.rows[0].previous_data_sequence, null);
    assert.deepEqual(stored.rows[0].outbox_statuses, ["held", "pending"]);
  } finally {
    await pool.end();
  }
});

test("Baseline Sequence 0 advances to Incremental Sequence 1 on the first change", {
  skip: !integrationUrl,
}, async () => {
  const pool = publicationPool(3);
  const suffix = randomUUID().replaceAll("-", "");
  const streamId = randomUUID();
  const channelId = `UCreconcilebaseline${suffix}`;
  try {
    await writeOperationalChannel(pool, {
      channelId,
      marker: "baseline",
      sequence: 1,
      observedAt: "2026-07-27T09:00:00.000Z",
    });
    await insertStream(pool, { streamId, sourceKey: `reconcile-baseline-${suffix}` });
    await insertOwnership(pool, {
      streamId,
      channelId,
      onboardingMode: "baseline",
      destinations: [{ destination: "business", mode: "hold" }],
    });

    const seeded = await transaction(pool, (client) => reconcileChannel(client, channelId));
    assert.equal(seeded.status, "seeded");
    assert.equal(seeded.domains[0].data_sequence, 0);
    assert.equal(seeded.revisions.length, 0);

    const changed = await transaction(pool, async (client) => {
      await writeOperationalChannel(client, {
        channelId,
        marker: "changed",
        sequence: 2,
        observedAt: "2026-07-27T10:00:00.000Z",
      });
      return reconcileChannel(client, channelId);
    });
    assert.deepEqual(
      {
        type: changed.revisions[0].revision_type,
        sequence: changed.revisions[0].data_sequence,
        previousSequence: changed.revisions[0].previous_data_sequence,
        previousHash: changed.revisions[0].previous_result_hash,
        outboxStatus: changed.revisions[0].outbox[0].status,
      },
      {
        type: "incremental",
        sequence: 1,
        previousSequence: 0,
        previousHash: seeded.domains[0].result_hash,
        outboxStatus: "held",
      },
    );
  } finally {
    await pool.end();
  }
});

test("NotReady refresh preserves a published Current without violating its database constraint", {
  skip: !integrationUrl,
}, async () => {
  const pool = publicationPool(3);
  const suffix = randomUUID().replaceAll("-", "");
  const streamId = randomUUID();
  const channelId = `UCreconcilenotready${suffix}`;
  try {
    await setupChannel(pool, {
      streamId,
      channelId,
      sourceKey: `reconcile-not-ready-${suffix}`,
    });
    const published = await transaction(pool, (client) => reconcileChannel(client, channelId));
    assert.equal(published.status, "revised");

    const before = await pool.query(
      `SELECT readiness_status,readiness_reasons,payload_json,result_hash,source_refs,
              complete_observed_at,data_sequence,current_revision_id
       FROM publication.domain_current
       WHERE publication_stream_id=$1 AND channel_id=$2 AND domain='channel'`,
      [streamId, channelId],
    );
    assert.equal(before.rows[0].readiness_status, "ready");
    assert.equal(String(before.rows[0].data_sequence), "1");

    const notReady = await transaction(pool, async (client) => {
      await client.query(
        "DELETE FROM crawler.channel_domain_cursors WHERE channel_id=$1 AND observation_kind='about'",
        [channelId],
      );
      return reconcileChannel(client, channelId);
    });
    assert.equal(notReady.status, "not_ready");
    assert.equal(notReady.revisions.length, 0);

    const after = await pool.query(
      `SELECT readiness_status,readiness_reasons,payload_json,result_hash,source_refs,
              complete_observed_at,data_sequence,current_revision_id,
              (SELECT count(*)::int FROM publication.revision WHERE publication_stream_id=$1) AS revisions
       FROM publication.domain_current
       WHERE publication_stream_id=$1 AND channel_id=$2 AND domain='channel'`,
      [streamId, channelId],
    );
    assert.equal(after.rows[0].readiness_status, "not_ready");
    assert.ok(after.rows[0].readiness_reasons.length > 0);
    assert.deepEqual(after.rows[0].payload_json, before.rows[0].payload_json);
    assert.equal(after.rows[0].result_hash, before.rows[0].result_hash);
    assert.deepEqual(after.rows[0].source_refs, before.rows[0].source_refs);
    assert.equal(
      after.rows[0].complete_observed_at.toISOString(),
      before.rows[0].complete_observed_at.toISOString(),
    );
    assert.equal(String(after.rows[0].data_sequence), "1");
    assert.equal(after.rows[0].current_revision_id, before.rows[0].current_revision_id);
    assert.equal(after.rows[0].revisions, 1);
  } finally {
    await pool.end();
  }
});

test("Business preservation stays NotReady at Sequence 0 until a real Bootstrap source arrives", {
  skip: !integrationUrl,
}, async () => {
  const pool = publicationPool(3);
  const suffix = randomUUID().replaceAll("-", "");
  const streamId = randomUUID();
  const channelId = `UCreconcilepreservation${suffix}`;
  const source = {
    type: "legacy_business_active_snapshot",
    database_name: "business_integration_test",
    active_watermark: "business-release-integration-test",
    snapshot_id: `snapshot-${suffix}`,
    captured_at: "2026-07-26T08:00:00.000Z",
  };
  const links = [{
    title: "Preserved Business Link",
    display_url: null,
    target_url: "https://business.example/",
    favicon_url: null,
    position: 0,
    link_type: "website",
    purpose: "public_reference",
  }];
  const preservationBaseline = {
    status: "available",
    payload: { channel_id: channelId, links },
    source,
  };
  try {
    await setupChannel(pool, {
      streamId,
      channelId,
      sourceKey: `reconcile-preservation-${suffix}`,
      destinations: [{ destination: "business", mode: "hold" }],
    });
    await pool.query(
      `UPDATE crawler.channels
       SET external_links='[]'::jsonb,external_links_status='observed'
       WHERE channel_id=$1`,
      [channelId],
    );
    await pool.query(
      "DELETE FROM crawler.channel_domain_cursors WHERE channel_id=$1 AND observation_kind='about'",
      [channelId],
    );

    const preserved = await transaction(pool, (client) => reconcilePublication(client, {
      channelId,
      domains: ["channel"],
      asOf: AS_OF,
      preservationBaselines: { channel: preservationBaseline },
    }));
    assert.equal(preserved.status, "preservation_seeded");
    assert.equal(preserved.seed_status, "pending");
    assert.equal(preserved.revisions.length, 0);

    const storedPreservation = await pool.query(
      `SELECT readiness_status,payload_json,result_hash,source_refs,data_sequence,current_revision_id,
              (SELECT count(*)::int FROM publication.revision WHERE publication_stream_id=$1) AS revisions,
              (SELECT count(*)::int FROM publication.outbox outbox
                 JOIN publication.revision revision USING (revision_id)
                 WHERE revision.publication_stream_id=$1) AS outbox
       FROM publication.domain_current
       WHERE publication_stream_id=$1 AND channel_id=$2 AND domain='channel'`,
      [streamId, channelId],
    );
    assert.equal(storedPreservation.rows[0].readiness_status, "not_ready");
    assert.deepEqual(storedPreservation.rows[0].payload_json, preservationBaseline.payload);
    assert.equal(String(storedPreservation.rows[0].data_sequence), "0");
    assert.equal(storedPreservation.rows[0].current_revision_id, null);
    assert.equal(storedPreservation.rows[0].revisions, 0);
    assert.equal(storedPreservation.rows[0].outbox, 0);
    assert.deepEqual(storedPreservation.rows[0].source_refs.publication_preservation.source, source);

    const bootstrapped = await transaction(pool, async (client) => {
      await writeOperationalChannel(client, {
        channelId,
        marker: "later-source",
        sequence: 2,
        observedAt: "2026-07-27T10:00:00.000Z",
      });
      return reconcileChannel(client, channelId);
    });
    assert.equal(bootstrapped.revisions[0].revision_type, "bootstrap");
    assert.equal(bootstrapped.revisions[0].data_sequence, 1);
    assert.equal(bootstrapped.revisions[0].previous_data_sequence, null);
    assert.equal(bootstrapped.revisions[0].previous_result_hash, null);
    assert.deepEqual(bootstrapped.revisions[0].payload.links, links);
    assert.deepEqual(
      bootstrapped.revisions[0].source.publication_merge.preservation_source,
      source,
    );
  } finally {
    await pool.end();
  }
});

test("database constraints keep Revision and terminal Outbox state immutable", {
  skip: !integrationUrl,
}, async () => {
  const pool = publicationPool(3);
  const suffix = randomUUID().replaceAll("-", "");
  const streamId = randomUUID();
  const channelId = `UCreconcileconstraints${suffix}`;
  try {
    await setupChannel(pool, {
      streamId,
      channelId,
      sourceKey: `reconcile-constraints-${suffix}`,
      destinations: [{ destination: "business", mode: "hold" }],
    });
    const result = await transaction(pool, (client) => reconcileChannel(client, channelId));
    const revisionId = result.revisions[0].revision_id;

    await assert.rejects(
      pool.query(
        "UPDATE publication.revision SET payload_json='{}'::jsonb WHERE revision_id=$1",
        [revisionId],
      ),
      (error) => error?.code === "55000" && error.message.includes("Revision rows are immutable"),
    );
    await assert.rejects(
      pool.query("DELETE FROM publication.revision WHERE revision_id=$1", [revisionId]),
      (error) => error?.code === "55000" && error.message.includes("Revision rows are immutable"),
    );
    await assert.rejects(
      pool.query(
        `UPDATE publication.domain_current
         SET result_hash=$3
         WHERE publication_stream_id=$1 AND channel_id=$2 AND domain='channel'`,
        [streamId, channelId, `sha256:${"f".repeat(64)}`],
      ),
      (error) => error?.code === "23503"
        && error.constraint === "domain_current_current_revision_id_fkey",
    );

    await assert.rejects(
      pool.query(
        `UPDATE publication.outbox SET status='delivered',updated_at=now()
         WHERE destination='business' AND revision_id=$1`,
        [revisionId],
      ),
      (error) => error?.code === "55000" && error.message.includes("invalid Publication Outbox"),
    );
    await pool.query(
      `UPDATE publication.outbox SET status='pending',updated_at=now()
       WHERE destination='business' AND revision_id=$1`,
      [revisionId],
    );
    await pool.query(
      `UPDATE publication.outbox
       SET status='leased',attempts=attempts+1,lease_owner='integration-test',
           lease_expires_at=now()+interval '1 minute',updated_at=now()
       WHERE destination='business' AND revision_id=$1`,
      [revisionId],
    );
    await pool.query(
      `UPDATE publication.outbox
       SET status='delivered',lease_owner=NULL,lease_expires_at=NULL,
           receipt_id='receipt-1',receipt_status='accepted',receipt_received_at=now(),
           receipt_json='{"accepted":true}'::jsonb,delivered_at=now(),updated_at=now()
       WHERE destination='business' AND revision_id=$1`,
      [revisionId],
    );
    await assert.rejects(
      pool.query(
        `UPDATE publication.outbox SET last_error='changed after delivery'
         WHERE destination='business' AND revision_id=$1`,
        [revisionId],
      ),
      (error) => error?.code === "55000" && error.message.includes("delivered Publication Outbox"),
    );
    await assert.rejects(
      pool.query(
        "DELETE FROM publication.outbox WHERE destination='business' AND revision_id=$1",
        [revisionId],
      ),
      (error) => error?.code === "55000" && error.message.includes("Outbox rows cannot be deleted"),
    );
  } finally {
    await pool.end();
  }
});

test("same-Channel transactions produce contiguous Revisions", {
  skip: !integrationUrl,
  timeout: 15000,
}, async () => {
  const pool = publicationPool(4);
  const suffix = randomUUID().replaceAll("-", "");
  const streamId = randomUUID();
  const channelId = `UCreconcilelock${suffix}`;
  let firstClient;
  let secondClient;
  let firstOpen = false;
  let secondOpen = false;
  try {
    await setupChannel(pool, {
      streamId,
      channelId,
      sourceKey: `reconcile-lock-${suffix}`,
    });
    const bootstrap = await transaction(pool, (client) => reconcileChannel(client, channelId));
    assert.equal(bootstrap.revisions[0].data_sequence, 1);

    firstClient = await pool.connect();
    secondClient = await pool.connect();
    await firstClient.query("BEGIN");
    firstOpen = true;
    await secondClient.query("BEGIN");
    secondOpen = true;
    await secondClient.query("SET LOCAL statement_timeout = '5s'");

    await writeOperationalChannel(firstClient, {
      channelId,
      marker: "two",
      sequence: 2,
      observedAt: "2026-07-27T10:00:00.000Z",
    });
    const secondRevision = await reconcileChannel(firstClient, channelId);
    assert.equal(secondRevision.revisions[0].data_sequence, 2);

    let secondFinished = false;
    const secondWork = (async () => {
      await writeOperationalChannel(secondClient, {
        channelId,
        marker: "three",
        sequence: 3,
        observedAt: "2026-07-27T11:00:00.000Z",
      });
      const reconciled = await reconcileChannel(secondClient, channelId);
      secondFinished = true;
      return reconciled;
    })();
    await new Promise((resolve) => setTimeout(resolve, 100));
    assert.equal(secondFinished, false, "the second writer must wait for the first Channel transaction");

    await firstClient.query("COMMIT");
    firstOpen = false;
    const thirdRevision = await secondWork;
    assert.equal(thirdRevision.revisions[0].data_sequence, 3);
    await secondClient.query("COMMIT");
    secondOpen = false;

    const sequences = await pool.query(
      `SELECT data_sequence FROM publication.revision
       WHERE publication_stream_id=$1 AND channel_id=$2 AND domain='channel'
       ORDER BY data_sequence`,
      [streamId, channelId],
    );
    assert.deepEqual(sequences.rows.map((row) => Number(row.data_sequence)), [1, 2, 3]);
  } finally {
    if (firstOpen) await firstClient?.query("ROLLBACK").catch(() => {});
    if (secondOpen) await secondClient?.query("ROLLBACK").catch(() => {});
    firstClient?.release();
    secondClient?.release();
    await pool.end();
  }
});

test("different Channels in one Stream reconcile independently", {
  skip: !integrationUrl,
  timeout: 10000,
}, async () => {
  const pool = publicationPool(4);
  const suffix = randomUUID().replaceAll("-", "");
  const streamId = randomUUID();
  const firstChannelId = `UCreconcileparallel1${suffix}`;
  const secondChannelId = `UCreconcileparallel2${suffix}`;
  let firstClient;
  let secondClient;
  let firstOpen = false;
  let secondOpen = false;
  try {
    await setupChannel(pool, {
      streamId,
      channelId: firstChannelId,
      sourceKey: `reconcile-parallel-${suffix}`,
    });
    await setupChannel(pool, {
      streamId,
      channelId: secondChannelId,
      sourceKey: `reconcile-parallel-${suffix}`,
      createStream: false,
      marker: "two",
    });

    firstClient = await pool.connect();
    secondClient = await pool.connect();
    await firstClient.query("BEGIN");
    firstOpen = true;
    await secondClient.query("BEGIN");
    secondOpen = true;
    await secondClient.query("SET LOCAL statement_timeout = '2s'");

    const first = await reconcileChannel(firstClient, firstChannelId);
    assert.equal(first.revisions[0].data_sequence, 1);
    const second = await reconcileChannel(secondClient, secondChannelId);
    assert.equal(second.revisions[0].data_sequence, 1);

    await secondClient.query("COMMIT");
    secondOpen = false;
    await firstClient.query("COMMIT");
    firstOpen = false;
  } finally {
    if (firstOpen) await firstClient?.query("ROLLBACK").catch(() => {});
    if (secondOpen) await secondClient?.query("ROLLBACK").catch(() => {});
    firstClient?.release();
    secondClient?.release();
    await pool.end();
  }
});

test("Reconciler rejects a bare Pool", {
  skip: !integrationUrl,
}, async () => {
  const pool = publicationPool(1);
  try {
    await assert.rejects(
      reconcilePublication(pool, {
        channelId: `UCreconcilepool${randomUUID().replaceAll("-", "")}`,
        domains: ["channel"],
        asOf: AS_OF,
      }),
      /SAVEPOINT can only be used in transaction blocks/,
    );
  } finally {
    await pool.end();
  }
});
