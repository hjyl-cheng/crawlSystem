import test from "node:test";
import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import pg from "pg";
import { randomUUID } from "node:crypto";
import {
  createMigrationControlBatch,
  prepareMigrationControlList,
  startControlledMigrationChannel,
  controlMigrationBatch,
  reconcileMigrationControl,
  loadMigrationControlProgress,
} from "../src/migrationBatchControl.js";
import { sourceSnapshotHash } from "../src/migrationSource.js";
const url = process.env.MIGRATION_CONTROL_TEST_URL;
test(
  "real SQL freezes All, gates racing starts, drains paused work, resumes and releases unstarted IDs on end",
  { skip: !url },
  async () => {
    assert.equal(new URL(url).pathname, "/migration_control_test");
    const pool = new pg.Pool({ connectionString: url, max: 8 });
    const query = (...args) => pool.query(...args);
    const withTransaction = async (fn) => {
      const c = await pool.connect();
      try {
        await c.query("BEGIN");
        const r = await fn(c);
        await c.query("COMMIT");
        return r;
      } catch (e) {
        await c.query("ROLLBACK");
        throw e;
      } finally {
        c.release();
      }
    };
    try {
      await query(
        "DROP SCHEMA IF EXISTS publication CASCADE; DROP SCHEMA IF EXISTS crawler CASCADE",
      );
      await query(
        await readFile(new URL("../src/schema.sql", import.meta.url), "utf8"),
      );
      await query(
        await readFile(
          new URL("../src/migrationInventorySchema.sql", import.meta.url),
          "utf8",
        ),
      );
      const source = "control-test-" + randomUUID(),
        sync = randomUUID();
      await query(
        `INSERT INTO crawler.migration_channel_inventory_syncs(source_id,source_database,source_database_oid,status,sync_token,eligible_count) VALUES($1,'legacy_test',42,'ready',$2,3)`,
        [source, sync],
      );
      const makeSnapshot = (id, n) => {
        const s = {
          source_id: source,
          source_database: "legacy_test",
          source_database_oid: "42",
          source_candidate_id: String(n),
          source_candidate_status: "discovered",
          channel_id: id,
          channel_url: "https://www.youtube.com/channel/" + id,
          title: id,
          priority: 100,
          source_json: { source: "test" },
          snapshot_json: {},
        };
        return { ...s, snapshot_sha256: sourceSnapshotHash(s) };
      };
      const ids = ["UCcontrol1", "UCcontrol2", "UCcontrol3"];
      for (let n = 0; n < ids.length; n++)
        await query(
          `INSERT INTO crawler.migration_channel_inventory(source_id,source_candidate_id,channel_id,channel_url,title,source_candidate_status,sync_token) VALUES($1,$2,$3,$4,$3,'discovered',$5)`,
          [
            source,
            n + 1,
            ids[n],
            "https://www.youtube.com/channel/" + ids[n],
            sync,
          ],
        );
      const b = await createMigrationControlBatch({
        withTransaction,
        selection: "all",
        sourceId: source,
      });
      await prepareMigrationControlList({
        withTransaction,
        batchId: b.batch_id,
      });
      await query(
        `INSERT INTO crawler.migration_channel_inventory(source_id,source_candidate_id,channel_id,channel_url,source_candidate_status,sync_token) VALUES($1,4,'UCnewlater','https://youtube.com/channel/UCnewlater','discovered',$2)`,
        [source, sync],
      );
      let progress = await loadMigrationControlProgress(query);
      assert.equal(progress.active.total_count, 3);
      for (let n = 0; n < ids.length; n++)
        await query(
          "UPDATE crawler.migration_control_items SET snapshot_json=$3 WHERE batch_id=$1 AND channel_id=$2",
          [b.batch_id, ids[n], makeSnapshot(ids[n], n + 1)],
        );
      await query(
        "UPDATE crawler.migration_control_batches SET max_in_flight=1 WHERE batch_id=$1",
        [b.batch_id],
      );
      const first = await startControlledMigrationChannel({
        query,
        withTransaction,
        batchId: b.batch_id,
        channelId: ids[0],
      });
      assert(first.started);
      assert.equal(
        (
          await startControlledMigrationChannel({
            query,
            withTransaction,
            batchId: b.batch_id,
            channelId: ids[1],
          })
        ).started,
        false,
        "in-flight channel limit applies",
      );
      progress = await loadMigrationControlProgress(query);
      await controlMigrationBatch({
        withTransaction,
        batchId: b.batch_id,
        action: "pause",
        version: progress.active.version,
      });
      assert.equal(
        (
          await startControlledMigrationChannel({
            query,
            withTransaction,
            batchId: b.batch_id,
            channelId: ids[1],
          })
        ).started,
        false,
      );
      let additions = 0;
      const queue = {
        add: async () => {
          additions++;
        },
      };
      await reconcileMigrationControl({ query, withTransaction, queue });
      assert.equal(
        (await loadMigrationControlProgress(query)).active.status,
        "pausing",
      );
      assert.equal(additions, 0);
      // A channel that was already admitted can finish after pause was requested.
      await query(
        "UPDATE crawler.channel_candidates SET status='rejected',reject_reason='test_policy' WHERE candidate_id=$1",
        [first.candidate_id],
      );
      await reconcileMigrationControl({ query, withTransaction, queue });
      progress = await loadMigrationControlProgress(query);
      assert.equal(progress.active.status, "paused");
      assert.equal(progress.active.counts.rejected, 1);
      await assert.rejects(
        controlMigrationBatch({
          withTransaction,
          batchId: b.batch_id,
          action: "resume",
          version: 0,
        }),
        /状态/,
      );
      await controlMigrationBatch({
        withTransaction,
        batchId: b.batch_id,
        action: "resume",
        version: progress.active.version,
      });
      const results = await Promise.all(
        [ids[1], ids[2]].map((channelId) =>
          startControlledMigrationChannel({
            query,
            withTransaction,
            batchId: b.batch_id,
            channelId,
          }),
        ),
      );
      assert.equal(
        results.filter((r) => r.started).length,
        1,
        "concurrent workers serialize admission",
      );
      progress = await loadMigrationControlProgress(query);
      await controlMigrationBatch({
        withTransaction,
        batchId: b.batch_id,
        action: "stop",
        version: progress.active.version,
      });
      await reconcileMigrationControl({ query, withTransaction, queue });
      progress = await loadMigrationControlProgress(query);
      assert.equal(progress.active.status, "stopping");
      assert.equal(progress.active.counts.released, 1);
      await query(
        "UPDATE crawler.channel_candidates SET status='rejected',reject_reason='test_policy' WHERE candidate_id=$1",
        [results.find((r) => r.started).candidate_id],
      );
      await reconcileMigrationControl({ query, withTransaction, queue });
      progress = await loadMigrationControlProgress(query);
      assert.equal(progress.active, null);
      assert.equal(progress.batches[0].status, "ended");
      assert.equal(progress.batches[0].counts.rejected, 2);
      const released = (
        await query(
          "SELECT channel_id FROM crawler.migration_control_items WHERE batch_id=$1 AND state='released'",
          [b.batch_id],
        )
      ).rows[0].channel_id;
      assert.equal(
        Number(
          (
            await query(
              "SELECT count(*) FROM crawler.migration_channel_intents WHERE channel_id=$1",
              [released],
            )
          ).rows[0].count,
        ),
        0,
      );
      const next = await createMigrationControlBatch({
        withTransaction,
        selection: "all",
        sourceId: source,
      });
      await prepareMigrationControlList({
        withTransaction,
        batchId: next.batch_id,
      });
      assert.equal(
        (await loadMigrationControlProgress(query)).active.total_count,
        2,
        "released and newly added IDs are available for the next batch",
      );

      await reconcileMigrationControl({
        query,
        withTransaction,
        queue,
        sourceLoader: async ({ channelId, candidateId }) =>
          makeSnapshot(channelId, candidateId),
      });
      const nextItems = (
        await query(
          "SELECT channel_id FROM crawler.migration_control_items WHERE batch_id=$1 ORDER BY ordinal",
          [next.batch_id],
        )
      ).rows;
      for (const [index, item] of nextItems.entries()) {
        const started = await startControlledMigrationChannel({
          query,
          withTransaction,
          batchId: next.batch_id,
          channelId: item.channel_id,
        });
        assert(started.started);
        await query(
          "UPDATE crawler.channel_candidates SET status='accepted' WHERE candidate_id=$1",
          [started.candidate_id],
        );
        await query(
          "INSERT INTO crawler.channels(channel_id,channel_url,title) VALUES($1,$2,$1)",
          [item.channel_id, "https://youtube.com/channel/" + item.channel_id],
        );
        await query(
          "INSERT INTO crawler.channel_runs(run_id,channel_id,candidate_id,status,publication_finalized_status,publication_finalized_at) VALUES($1,$2,$3,'done',$4,now())",
          [
            "test-" + item.channel_id,
            item.channel_id,
            started.candidate_id,
            index === 0 ? "ready_auto" : "ready_partial",
          ],
        );
      }
      const stream = randomUUID(),
        revision = randomUUID(),
        publishedChannel = nextItems[0].channel_id;
      await query(
        "INSERT INTO publication.stream(publication_stream_id,source_deployment_key,source_identity_json,created_by,created_reason,status_changed_by,status_reason) VALUES($1,'test','{}','test','test','test','test')",
        [stream],
      );
      await query(
        "INSERT INTO publication.channel_stream_state(publication_stream_id,channel_id,onboarding_mode,state_changed_by,state_reason) VALUES($1,$2,'bootstrap','test','test')",
        [stream, publishedChannel],
      );
      await query(
        "INSERT INTO publication.revision(revision_id,publication_stream_id,channel_id,domain,data_sequence,revision_type,operation,contract_version,policy_version,occurred_at,source_refs,result_hash,payload_hash,payload_json) VALUES($1,$2,$3,'channel',1,'bootstrap','replace',1,'test',now(),'{}',$4,$4,'{}')",
        [revision, stream, publishedChannel, "sha256:" + "0".repeat(64)],
      );
      await query(
        "INSERT INTO publication.outbox(destination,revision_id,status) VALUES('test',$1,'pending')",
        [revision],
      );
      await reconcileMigrationControl({ query, withTransaction, queue });
      assert.equal(
        (await loadMigrationControlProgress(query)).active.publishing_count,
        1,
      );
      await query(
        "UPDATE publication.outbox SET status='leased',lease_owner='test',lease_expires_at=now()+interval '1 minute' WHERE revision_id=$1",
        [revision],
      );
      await query(
        "UPDATE publication.outbox SET lease_owner=NULL,lease_expires_at=NULL,status='delivered',delivered_at=now(),receipt_id='test',receipt_status='accepted',receipt_received_at=now() WHERE revision_id=$1",
        [revision],
      );
      await reconcileMigrationControl({ query, withTransaction, queue });
      const finished = (await loadMigrationControlProgress(query)).batches[0];
      assert.equal(finished.status, "completed");
      assert.equal(finished.counts.success, 1);
      assert.equal(finished.counts.dormant, 1);

      // Empty selections and controls before the first coordinator tick are valid.
      const empty = await createMigrationControlBatch({
        withTransaction,
        selection: "all",
        sourceId: source,
      });
      await controlMigrationBatch({
        withTransaction,
        batchId: empty.batch_id,
        action: "pause",
        version: 1,
      });
      await reconcileMigrationControl({ query, withTransaction, queue });
      let header = (await loadMigrationControlProgress(query)).active;
      assert.equal(header.status, "paused");
      assert.equal(header.frozen_at, null);
      await controlMigrationBatch({
        withTransaction,
        batchId: empty.batch_id,
        action: "resume",
        version: header.version,
      });
      await reconcileMigrationControl({ query, withTransaction, queue });
      assert.equal(
        (await loadMigrationControlProgress(query)).batches[0].status,
        "completed",
      );
      const beforePrepare = await createMigrationControlBatch({
        withTransaction,
        selection: "100",
        sourceId: source,
      });
      await controlMigrationBatch({
        withTransaction,
        batchId: beforePrepare.batch_id,
        action: "stop",
        version: 1,
      });
      await reconcileMigrationControl({ query, withTransaction, queue });
      assert.equal(
        (await loadMigrationControlProgress(query)).batches[0].status,
        "ended",
      );

      // Recovery remains in-flight; exhausted manual retry is a failure, not an endless drain.
      for (let n = 5; n <= 6; n++)
        await query(
          "INSERT INTO crawler.migration_channel_inventory(source_id,source_candidate_id,channel_id,channel_url,source_candidate_status,sync_token) VALUES($1,$2,$3,$4,'discovered',$5)",
          [
            source,
            n,
            "UCfailure" + n,
            "https://youtube.com/channel/UCfailure" + n,
            sync,
          ],
        );
      const failedBatch = await createMigrationControlBatch({
        withTransaction,
        selection: "all",
        sourceId: source,
      });
      await reconcileMigrationControl({
        query,
        withTransaction,
        queue,
        sourceLoader: async ({ channelId, candidateId }) =>
          makeSnapshot(channelId, candidateId),
      });
      const admitted = [];
      for (let n = 5; n <= 6; n++) {
        const item = await startControlledMigrationChannel({
          query,
          withTransaction,
          batchId: failedBatch.batch_id,
          channelId: "UCfailure" + n,
        });
        admitted.push(item.candidate_id);
        assert.equal(
          (
            await startControlledMigrationChannel({
              query,
              withTransaction,
              batchId: failedBatch.batch_id,
              channelId: "UCfailure" + n,
            })
          ).started,
          false,
          "replayed launch does not duplicate",
        );
      }
      await query(
        "UPDATE crawler.channel_candidates SET status='failed',snapshot_attempts=3 WHERE candidate_id=$1",
        [admitted[0]],
      );
      await query(
        "UPDATE crawler.channel_candidates SET status='failed',snapshot_active_job_id='failed-root',snapshot_active_job_attempt=2 WHERE candidate_id=$1",
        [admitted[1]],
      );
      await query(
        "INSERT INTO crawler.migration_system_retry_items(migration_intent_id,candidate_id,failed_dispatch_batch_id,failed_dispatch_generation,failed_job_id,failed_job_attempt,failure_code,failure_category,status) SELECT migration_intent_id,target_candidate_id,$2,dispatch_attempts,'failed-root',2,'test','test','retrying' FROM crawler.migration_channel_intents WHERE target_candidate_id=$1",
        [admitted[1], failedBatch.batch_id],
      );
      await reconcileMigrationControl({
        query,
        withTransaction,
        queue,
        maxSnapshotAttempts: 3,
      });
      header = (await loadMigrationControlProgress(query)).active;
      assert.equal(header.counts.failed, 1);
      assert.equal(header.counts.started, 1);
      await controlMigrationBatch({
        withTransaction,
        batchId: header.batch_id,
        action: "stop",
        version: header.version,
      });
      await reconcileMigrationControl({
        query,
        withTransaction,
        queue,
        maxSnapshotAttempts: 3,
      });
      assert.equal(
        (await loadMigrationControlProgress(query)).active.status,
        "stopping",
      );
      await query(
        "UPDATE crawler.migration_system_retry_items SET status='pending' WHERE candidate_id=$1",
        [admitted[1]],
      );
      await reconcileMigrationControl({
        query,
        withTransaction,
        queue,
        maxSnapshotAttempts: 3,
      });
      header = (await loadMigrationControlProgress(query)).batches[0];
      assert.equal(header.status, "ended");
      assert.equal(header.counts.failed, 2);
    } finally {
      await pool.end();
    }
  },
);
