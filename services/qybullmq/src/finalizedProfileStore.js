import {
  FINALIZABLE_CHANNEL_STATUSES,
  finalizedRunState,
  isSuccessfulPublicationFinalize,
  SUCCESSFUL_PUBLICATION_FINALIZE_STATUSES,
} from "./finalizePolicy.js";
import {
  lockPublicationChannelMutation,
  lockPublicationRunMutation,
} from "./publicationChannelMutationLock.js";
import { reconcilePublicationAfterFullCrawl } from "./publicationChannelOnboarding.js";

function finalizedRunAssignments({
  alias,
  stateParam,
  profileParam,
  terminalParam,
  finalizedStatusParam,
}) {
  return `status=${stateParam},
         detail_status=CASE WHEN ${terminalParam}::boolean THEN 'done' ELSE ${alias}.detail_status END,
         profile_json=COALESCE(${profileParam}::jsonb,${alias}.profile_json),
         finished_at=CASE
           WHEN ${terminalParam}::boolean THEN COALESCE(${alias}.finished_at,now())
           ELSE ${alias}.finished_at
         END,
         publication_finalized_status=CASE
           WHEN ${terminalParam}::boolean THEN ${finalizedStatusParam}
           ELSE ${alias}.publication_finalized_status
         END,
         publication_finalized_at=CASE
           WHEN ${terminalParam}::boolean THEN COALESCE(${alias}.publication_finalized_at,now())
           ELSE ${alias}.publication_finalized_at
         END,
         updated_at=now()`;
}

function requiredText(value, field) {
  const output = String(value ?? "").trim();
  if (!output) throw new TypeError(`${field} is required`);
  return output;
}

function object(value, field) {
  if (!value || typeof value !== "object" || Array.isArray(value)) {
    throw new TypeError(`${field} must be an object`);
  }
  return value;
}

function timestamp(value, field) {
  const parsed = new Date(value);
  if (Number.isNaN(parsed.getTime())) throw new TypeError(`${field} must be a timestamp`);
  return parsed.toISOString();
}

function activeClient(client) {
  if (!client || typeof client.query !== "function") {
    throw new TypeError("an active PostgreSQL client is required");
  }
  return client;
}

async function synchronizeFinalizedRunLocked(client, {
  runId,
  finalizedStatus,
  profile = null,
  channelId = null,
}) {
  const normalizedRunId = requiredText(runId, "runId");
  const status = requiredText(finalizedStatus, "finalizedStatus");
  const expectedChannelId = channelId == null ? null : requiredText(channelId, "channelId");
  const state = finalizedRunState(status);
  const assignments = finalizedRunAssignments({
    alias: "run",
    stateParam: "$2",
    profileParam: "$3",
    terminalParam: "$4",
    finalizedStatusParam: "$5",
  });
  return client.query(
    `/* finalized-profile-store:sync-run */
     UPDATE crawler.channel_runs AS run
     SET ${assignments}
     WHERE run.run_id=$1
       AND ($7::text IS NULL OR run.channel_id=$7)
       AND EXISTS (
         SELECT 1 FROM crawler.channels channel
         WHERE channel.channel_id=run.channel_id
           AND channel.latest_run_id=run.run_id
           AND channel.status=ANY($6::text[])
       )`,
    [
      normalizedRunId,
      state.status,
      profile == null ? null : JSON.stringify(object(profile, "profile")),
      state.terminal,
      status,
      FINALIZABLE_CHANNEL_STATUSES,
      expectedChannelId,
    ],
  );
}

export async function synchronizeFinalizedRun(clientValue, input) {
  const client = activeClient(clientValue);
  const runId = requiredText(input?.runId, "runId");
  const locked = await lockPublicationRunMutation(client, runId);
  if (locked.rowCount !== 1) return { rows: [], rowCount: 0 };
  return synchronizeFinalizedRunLocked(client, { ...input, runId });
}

function normalizeCommitInput(input) {
  const deduplicated = input?.deduplicated === true;
  const status = requiredText(input?.status, "status");
  const revisionType = String(input?.publicationRevisionType ?? "incremental").trim();
  if (!new Set(["incremental", "repair"]).has(revisionType)) {
    throw new TypeError("publicationRevisionType must be incremental or repair");
  }
  return {
    channelId: requiredText(input?.channelId, "channelId"),
    runId: requiredText(input?.runId, "runId"),
    status,
    profile: deduplicated ? null : object(input?.profile, "profile"),
    quality: deduplicated ? null : object(input?.quality, "quality"),
    deduplicated,
    publicationAsOf: timestamp(input?.publicationAsOf, "publicationAsOf"),
    publicationRevisionType: revisionType,
  };
}

async function reconcileFinalizedPublication(client, input) {
  if (!isSuccessfulPublicationFinalize(input.status)) return null;
  return reconcilePublicationAfterFullCrawl(client, {
    channelId: input.channelId,
    runId: input.runId,
    asOf: input.publicationAsOf,
    revisionType: input.publicationRevisionType,
  });
}

export async function commitFinalizedProfile(clientValue, inputValue) {
  const client = activeClient(clientValue);
  const input = normalizeCommitInput(inputValue);
  const state = finalizedRunState(input.status);
  const assignments = finalizedRunAssignments({
    alias: "run",
    stateParam: "$6",
    profileParam: "$4",
    terminalParam: "$7",
    finalizedStatusParam: "$3",
  });
  await lockPublicationChannelMutation(client, input.channelId);

  if (input.deduplicated) {
    const synchronized = await synchronizeFinalizedRunLocked(client, {
      runId: input.runId,
      finalizedStatus: input.status,
      channelId: input.channelId,
    });
    if (synchronized.rowCount !== 1) {
      return {
        applied: false,
        deduplicated: true,
        skip_reason: "stale_run_race",
        publication: null,
      };
    }
    const publication = await reconcileFinalizedPublication(client, input);
    return { applied: false, deduplicated: true, publication };
  }

  const applied = await client.query(
    `/* finalized-profile-store:apply */
     WITH applied_profile AS (
       INSERT INTO crawler.finalized_profiles (
         channel_id,run_id,status,profile_json,quality_json,finalized_at,updated_at
       )
       SELECT $1,$2,$3,$4::jsonb,$5::jsonb,
              CASE WHEN $3=ANY($8::text[]) THEN now() ELSE NULL END,now()
       WHERE EXISTS (
         SELECT 1 FROM crawler.channels
         WHERE channel_id=$1 AND latest_run_id=$2
           AND status=ANY($9::text[])
       )
       ON CONFLICT (channel_id) DO UPDATE
       SET run_id=EXCLUDED.run_id,status=EXCLUDED.status,profile_json=EXCLUDED.profile_json,
           quality_json=EXCLUDED.quality_json,
           finalized_at=CASE WHEN EXCLUDED.status=ANY($8::text[])
             THEN now() ELSE crawler.finalized_profiles.finalized_at END,
           updated_at=now()
       WHERE EXISTS (
         SELECT 1 FROM crawler.channels
         WHERE channel_id=EXCLUDED.channel_id AND latest_run_id=EXCLUDED.run_id
           AND status=ANY($9::text[])
       )
         AND (
           crawler.finalized_profiles.run_id IS DISTINCT FROM EXCLUDED.run_id
           OR NOT (crawler.finalized_profiles.status=ANY($8::text[]))
           OR EXCLUDED.status='ready_auto'
           OR (
             crawler.finalized_profiles.status='ready_partial'
             AND EXCLUDED.status='ready_partial'
           )
         )
       RETURNING channel_id
     ), synchronized_run AS (
       UPDATE crawler.channel_runs AS run
       SET ${assignments}
       WHERE run.run_id=$2 AND run.channel_id=$1
         AND EXISTS (SELECT 1 FROM applied_profile)
       RETURNING run.run_id
     )
     SELECT EXISTS (SELECT 1 FROM applied_profile) AS applied,
            EXISTS (SELECT 1 FROM synchronized_run) AS synchronized`,
    [
      input.channelId,
      input.runId,
      input.status,
      JSON.stringify(input.profile),
      JSON.stringify(input.quality),
      state.status,
      state.terminal,
      SUCCESSFUL_PUBLICATION_FINALIZE_STATUSES,
      FINALIZABLE_CHANNEL_STATUSES,
    ],
  );
  const applyResult = applied.rows[0] ?? {};
  if (applyResult.applied !== true) {
    const latest = await client.query(
      `/* finalized-profile-store:load-race */
       SELECT channel.latest_run_id,finalized.run_id AS finalized_run_id,
              finalized.status AS finalized_status
       FROM crawler.channels channel
       LEFT JOIN crawler.finalized_profiles finalized ON finalized.channel_id=channel.channel_id
       WHERE channel.channel_id=$1`,
      [input.channelId],
    );
    const row = latest.rows[0] ?? {};
    const statusRegression = String(row.latest_run_id ?? "") === input.runId
      && String(row.finalized_run_id ?? "") === input.runId;
    if (statusRegression) {
      await synchronizeFinalizedRunLocked(client, {
        runId: input.runId,
        finalizedStatus: row.finalized_status,
        channelId: input.channelId,
      });
    }
    return {
      applied: false,
      deduplicated: false,
      skip_reason: statusRegression ? "finalize_status_regression" : "stale_run_race",
      latest_run_id: row.latest_run_id ?? null,
      retained_status: row.finalized_status ?? null,
      publication: null,
    };
  }

  if (applyResult.synchronized !== true) {
    throw new Error(`Finalized Channel Run is missing: ${input.runId}`);
  }
  const publication = await reconcileFinalizedPublication(client, input);
  return { applied: true, deduplicated: false, publication };
}
