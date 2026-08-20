import assert from "node:assert/strict";
import test from "node:test";
import {
  lockPublicationChannelMutation,
  lockPublicationRunMutation,
} from "../src/publicationChannelMutationLock.js";

function clientFixture() {
  const calls = [];
  return {
    calls,
    async query(sql, params = []) {
      calls.push({ sql: String(sql), params });
      return { rows: [], rowCount: 0 };
    },
  };
}

test("Channel and Run mutations use the same transaction lock namespace", async () => {
  const client = clientFixture();
  await lockPublicationChannelMutation(client, "UCshared");
  await lockPublicationRunMutation(client, "run:shared");

  const channelLock = client.calls.find((call) => (
    call.sql.includes("publication-channel-mutation-lock:channel")
  ));
  const runLock = client.calls.find((call) => (
    call.sql.includes("publication-channel-mutation-lock:run")
  ));
  assert.match(channelLock.sql, /hashtextextended\(\$1,\$2\)/);
  assert.match(runLock.sql, /hashtextextended\(run\.channel_id,\$2\)/);
  assert.equal(channelLock.params[1], runLock.params[1]);
});

test("Publication Channel mutation locks require an explicit transaction", async () => {
  const error = new Error("SAVEPOINT can only be used in transaction blocks");
  error.code = "25P01";
  const client = {
    async query(sql) {
      if (String(sql).includes("transaction-guard")) throw error;
      assert.fail("lock query must not run without a transaction");
    },
  };

  await assert.rejects(
    lockPublicationChannelMutation(client, "UCtransaction"),
    (caught) => caught === error,
  );
});
