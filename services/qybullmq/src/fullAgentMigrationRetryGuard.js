function text(value) {
  return String(value ?? "").trim() || null;
}

function positiveInteger(value) {
  const normalized = Number(value);
  return Number.isSafeInteger(normalized) && normalized > 0 ? normalized : null;
}

function nonNegativeInteger(value) {
  const normalized = Number(value);
  return Number.isSafeInteger(normalized) && normalized >= 0 ? normalized : null;
}

function normalizeScope(value) {
  const channelId = text(value?.channelId ?? value?.channel_id);
  const runId = text(value?.runId ?? value?.run_id ?? value?.latest_run_id);
  if (!channelId || !runId) {
    throw new TypeError("full Agent channelId and runId are required");
  }
  const rawCandidateId = value?.candidateId ?? value?.candidate_id;
  const rawDispatchBatchId = value?.dispatchBatchId ?? value?.candidate_dispatch_batch_id;
  const rawDispatchGeneration = value?.dispatchGeneration
    ?? value?.candidate_dispatch_generation;
  const candidateId = rawCandidateId == null ? null : positiveInteger(rawCandidateId);
  const dispatchBatchId = text(rawDispatchBatchId);
  const dispatchGeneration = rawDispatchGeneration == null
    ? null
    : nonNegativeInteger(rawDispatchGeneration);
  const identityParts = [candidateId, dispatchBatchId, dispatchGeneration];
  if (identityParts.some((part) => part == null)
      && identityParts.some((part) => part != null)) {
    throw new TypeError("full Agent Candidate identity must be complete");
  }
  if (rawCandidateId != null && candidateId == null) {
    throw new TypeError("full Agent Candidate identity is invalid");
  }
  if (rawDispatchGeneration != null && dispatchGeneration == null) {
    throw new TypeError("full Agent Candidate identity is invalid");
  }
  return Object.freeze({
    channelId,
    runId,
    candidateId,
    dispatchBatchId,
    dispatchGeneration,
  });
}

export function fullAgentMigrationRunScope(value) {
  return normalizeScope(value);
}

function normalizedScopes(values) {
  if (!Array.isArray(values)) throw new TypeError("full Agent run scopes must be an array");
  const unique = new Map();
  for (const value of values) {
    const scope = normalizeScope(value);
    const key = `${scope.channelId}\u0000${scope.runId}`;
    const existing = unique.get(key);
    if (existing && (
      existing.candidateId !== scope.candidateId
      || existing.dispatchBatchId !== scope.dispatchBatchId
      || existing.dispatchGeneration !== scope.dispatchGeneration
    )) {
      throw new TypeError("full Agent Run has conflicting Candidate identities");
    }
    unique.set(key, scope);
  }
  return [...unique.values()].sort((left, right) => (
    left.channelId.localeCompare(right.channelId) || left.runId.localeCompare(right.runId)
  ));
}

function requiredClient(client) {
  if (!client || typeof client.query !== "function") {
    throw new TypeError("an active PostgreSQL client is required");
  }
  return client;
}

export async function lockGenericFullAgentAgainstMigrationSystemRetry(clientValue, scopeValue) {
  return lockGenericFullAgentBatchAgainstMigrationSystemRetry(clientValue, [scopeValue]);
}

export async function lockGenericFullAgentBatchAgainstMigrationSystemRetry(
  clientValue,
  scopeValues,
) {
  const client = requiredClient(clientValue);
  const scopes = normalizedScopes(scopeValues);
  if (scopes.length === 0) return true;
  const migrationScopes = scopes.filter(({ candidateId }) => candidateId != null);
  if (migrationScopes.length > 0) {
    const expectedCandidateIds = migrationScopes.map(({ candidateId }) => candidateId);
    const expectedDispatchBatchIds = migrationScopes
      .map(({ dispatchBatchId }) => dispatchBatchId);
    const expectedDispatchGenerations = migrationScopes
      .map(({ dispatchGeneration }) => dispatchGeneration);
    const lockedCandidates = await client.query(
      `/* full-agent-migration-guard:candidates */
       WITH expected(candidate_id,dispatch_batch_id,dispatch_generation) AS (
         SELECT * FROM unnest($1::bigint[],$2::text[],$3::bigint[])
       )
       SELECT candidate.candidate_id,candidate.dispatch_batch_id,
              candidate.snapshot_dispatch_generation
       FROM expected
       JOIN crawler.channel_candidates candidate
         ON candidate.candidate_id=expected.candidate_id
        AND candidate.dispatch_batch_id=expected.dispatch_batch_id
        AND candidate.snapshot_dispatch_generation=expected.dispatch_generation
       ORDER BY candidate.candidate_id
       FOR UPDATE OF candidate`,
      [expectedCandidateIds, expectedDispatchBatchIds, expectedDispatchGenerations],
    );
    if (lockedCandidates.rows.length !== migrationScopes.length) return false;

    const lockedRetries = await client.query(
      `/* full-agent-migration-guard:retries */
       WITH expected(candidate_id,dispatch_batch_id,dispatch_generation) AS (
         SELECT * FROM unnest($1::bigint[],$2::text[],$3::bigint[])
       )
       SELECT retry.system_retry_id,retry.status
       FROM expected
       JOIN crawler.migration_system_retry_items retry
         ON retry.candidate_id=expected.candidate_id
        AND (
          retry.failed_dispatch_batch_id=expected.dispatch_batch_id
          OR retry.failed_dispatch_batch_id IS NULL
        )
        AND COALESCE(retry.retry_dispatch_generation,retry.failed_dispatch_generation)
              =expected.dispatch_generation
       ORDER BY expected.candidate_id,retry.system_retry_id
       FOR UPDATE OF retry`,
      [expectedCandidateIds, expectedDispatchBatchIds, expectedDispatchGenerations],
    );
    if (lockedRetries.rows.some(({ status }) => (
      ["retrying", "pending", "dispatched"].includes(String(status))
    ))) return false;
  }

  const lockedRuns = await client.query(
    `/* full-agent-migration-guard:runs */
     WITH expected(channel_id,run_id,candidate_id,dispatch_batch_id) AS (
       SELECT * FROM unnest($1::text[],$2::text[],$3::bigint[],$4::text[])
     )
     SELECT run.run_id,run.channel_id,run.candidate_id
     FROM expected
     JOIN crawler.channel_runs run
       ON run.run_id=expected.run_id
      AND run.channel_id=expected.channel_id
      AND run.candidate_id IS NOT DISTINCT FROM expected.candidate_id
      AND (
        expected.candidate_id IS NULL
        OR COALESCE(run.result_json->>'dispatch_batch_id',
                    run.result_json->>'pipeline_cycle_id')=expected.dispatch_batch_id
      )
     ORDER BY run.run_id
     FOR UPDATE OF run`,
    [
      scopes.map(({ channelId }) => channelId),
      scopes.map(({ runId }) => runId),
      scopes.map(({ candidateId }) => candidateId),
      scopes.map(({ dispatchBatchId }) => dispatchBatchId),
    ],
  );
  if (lockedRuns.rows.length !== scopes.length) return false;

  const lockedChannels = await client.query(
    `/* full-agent-migration-guard:channels */
     WITH expected(channel_id,run_id) AS (
       SELECT * FROM unnest($1::text[],$2::text[])
     )
     SELECT channel.channel_id,channel.latest_run_id,channel.status
     FROM expected
     JOIN crawler.channels channel
       ON channel.channel_id=expected.channel_id
      AND channel.latest_run_id=expected.run_id
      AND channel.status='active'
     ORDER BY channel.channel_id
     FOR UPDATE OF channel`,
    [
      scopes.map(({ channelId }) => channelId),
      scopes.map(({ runId }) => runId),
    ],
  );
  return lockedChannels.rows.length === scopes.length;
}
