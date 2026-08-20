const PUBLICATION_CHANNEL_MUTATION_LOCK_SEED = 781137242;

function activeClient(client) {
  if (!client || typeof client.query !== "function") {
    throw new TypeError("an active PostgreSQL client is required");
  }
  return client;
}

function requiredText(value, field) {
  const output = String(value ?? "").trim();
  if (!output) throw new TypeError(`${field} is required`);
  return output;
}

async function assertTransaction(client) {
  await client.query(
    "/* publication-channel-mutation-lock:transaction-guard */ "
      + "SAVEPOINT publication_channel_mutation_lock_guard",
  );
  await client.query("RELEASE SAVEPOINT publication_channel_mutation_lock_guard");
}

export async function lockPublicationChannelMutation(clientValue, channelIdValue) {
  const client = activeClient(clientValue);
  const channelId = requiredText(channelIdValue, "channelId");
  await assertTransaction(client);
  return client.query(
    `/* publication-channel-mutation-lock:channel */
     SELECT pg_advisory_xact_lock(hashtextextended($1,$2))`,
    [channelId, PUBLICATION_CHANNEL_MUTATION_LOCK_SEED],
  );
}

export async function lockPublicationRunMutation(clientValue, runIdValue) {
  const client = activeClient(clientValue);
  const runId = requiredText(runIdValue, "runId");
  await assertTransaction(client);
  return client.query(
    `/* publication-channel-mutation-lock:run */
     SELECT run.channel_id,
            pg_advisory_xact_lock(hashtextextended(run.channel_id,$2))
     FROM crawler.channel_runs AS run
     WHERE run.run_id=$1`,
    [runId, PUBLICATION_CHANNEL_MUTATION_LOCK_SEED],
  );
}
