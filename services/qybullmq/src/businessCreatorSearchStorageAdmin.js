import { createHash } from "node:crypto";
import { environmentValue } from "./runtimeEnvironment.js";

const SEARCH_PUBLISH_LOCK = "kol_demo:creator-search-publish";

function requiredText(value, field) {
  const output = String(value ?? "").trim();
  if (!output) throw new TypeError(`${field} is required`);
  return output;
}

function explicitCount(environment, name) {
  const raw = requiredText(environment[name], name);
  if (!/^(0|[1-9][0-9]*)$/.test(raw)) {
    throw new TypeError(`${name} must be an explicit non-negative integer`);
  }
  const value = Number(raw);
  if (!Number.isSafeInteger(value)) {
    throw new TypeError(`${name} must be an explicit non-negative integer`);
  }
  return value;
}

export function businessCreatorSearchStorageConfig(environment = process.env) {
  return {
    databaseUrl: environmentValue("BUSINESS_ADMIN_DATABASE_URL", { environment }),
    expectedDatabase: requiredText(
      environment.EXPECTED_BUSINESS_DATABASE,
      "EXPECTED_BUSINESS_DATABASE",
    ),
    expectedBusinessChannelCount: explicitCount(
      environment,
      "EXPECTED_BUSINESS_CHANNEL_COUNT",
    ),
    actor: requiredText(environment.PUBLICATION_OPERATOR, "PUBLICATION_OPERATOR"),
    reason: requiredText(
      environment.PUBLICATION_ACTION_REASON,
      "PUBLICATION_ACTION_REASON",
    ),
    rollbackWatermark:
      String(environment.BUSINESS_CREATOR_SEARCH_ROLLBACK_WATERMARK ?? "").trim() || null,
  };
}

function digest(value) {
  return `sha256:${createHash("sha256").update(JSON.stringify(value)).digest("hex")}`;
}

export function businessCreatorSearchStorageConfirmation(config, state, action) {
  if (action !== "activate" && action !== "rollback") {
    throw new TypeError("Creator Search storage action must be activate or rollback");
  }
  const prefix = action === "activate"
    ? "ACTIVATE_CREATOR_SEARCH_INCREMENTAL"
    : "ROLLBACK_CREATOR_SEARCH_STORAGE";
  const watermark = action === "rollback"
    ? requiredText(state.rollback_target_watermark, "rollback target watermark")
    : requiredText(state.active_watermark, "active watermark");
  const rowCount = action === "rollback"
    ? Number(state.rollback_target_expected_count)
    : Number(state.live_count);
  if (!Number.isSafeInteger(rowCount) || rowCount < 0) {
    throw new TypeError("Creator Search storage row count is invalid");
  }
  return `${prefix}:${config.expectedDatabase}:${watermark}:${rowCount}:${digest({
    action,
    actor: config.actor,
    reason: config.reason,
    expected_database: config.expectedDatabase,
    expected_business_channel_count: config.expectedBusinessChannelCount,
    active_watermark: state.active_watermark,
    live_count: Number(state.live_count),
    legacy_count: Number(state.legacy_count),
    parity_diffs: Number(state.parity_diffs),
    write_mode: state.write_mode,
    read_mode: state.read_mode,
    rollback_target_watermark: state.rollback_target_watermark ?? null,
    rollback_target_count: state.rollback_target_count == null
      ? null
      : Number(state.rollback_target_count),
    rollback_target_expected_count: state.rollback_target_expected_count == null
      ? null
      : Number(state.rollback_target_expected_count),
    rollback_target_parity_diffs: state.rollback_target_parity_diffs == null
      ? null
      : Number(state.rollback_target_parity_diffs),
  })}`;
}

const INSPECT_STORAGE_SQL = `
  WITH active_legacy AS (
    SELECT search.*
    FROM public.creator_search_active active
    JOIN public.creator_search_current search USING(watermark)
    WHERE active.singleton=true
  ), parity AS (
    SELECT legacy.channel_id AS legacy_channel_id,
           live.channel_id AS live_channel_id,
           CASE WHEN legacy.channel_id IS NULL THEN NULL ELSE
             to_jsonb(legacy)-'watermark' END AS legacy_document,
           CASE WHEN live.channel_id IS NULL THEN NULL ELSE
             to_jsonb(live)-'watermark' END AS live_document
    FROM active_legacy legacy
    FULL JOIN public.creator_search_live live USING(channel_id)
  )
  SELECT current_database() AS database_name,
         current_user AS database_user,
         identity.database_kind AS identity_kind,
         identity.database_name AS identity_database,
         (SELECT count(*)::int FROM public.channels) AS channel_count,
         storage.write_mode,
         storage.read_mode,
         active.watermark AS active_watermark,
         (SELECT count(*)::int FROM public.creator_search_live) AS live_count,
         (SELECT count(*)::int FROM active_legacy) AS legacy_count,
         (SELECT count(*)::int FROM parity
          WHERE legacy_channel_id IS NULL OR live_channel_id IS NULL
             OR legacy_document IS DISTINCT FROM live_document) AS parity_diffs,
         (SELECT count(*)::int FROM publication.projection_outbox
          WHERE status IN ('pending','retry_wait','leased'))
           AS in_flight_projection_count,
         (SELECT count(*)::int FROM publication.channel_ownership
          WHERE status<>'active' OR projection_mode<>'online')
           AS abnormal_ownership_count,
         (SELECT COALESCE(jsonb_object_agg(status,row_count ORDER BY status),'{}'::jsonb)
          FROM (SELECT status,count(*)::int AS row_count
                FROM publication.projection_outbox GROUP BY status) counts)
           AS projection_status_counts,
         pg_database_size(current_database())::bigint AS database_bytes,
         to_regprocedure(
           'public.activate_creator_search_incremental_v1(text,integer,text,text)'
         ) IS NOT NULL AS activate_function_ready,
         to_regprocedure(
           'public.rollback_creator_search_incremental_storage_v1(text,integer,text,text)'
         ) IS NOT NULL AS rollback_function_ready
  FROM publication.database_identity identity
  CROSS JOIN publication.creator_search_storage_state storage
  LEFT JOIN public.creator_search_active active ON active.singleton=true
  WHERE identity.singleton=true AND storage.singleton=true
`;

const INSPECT_ROLLBACK_TARGET_SQL = `
  WITH RECURSIVE release_chain AS (
    SELECT release.watermark,release.previous_watermark,
           release.changed_channel_count,0 AS depth
    FROM public.creator_search_active active
    JOIN public.creator_search_releases release USING(watermark)
    WHERE active.singleton=true
    UNION ALL
    SELECT previous.watermark,previous.previous_watermark,
           previous.changed_channel_count,chain.depth+1
    FROM release_chain chain
    JOIN public.creator_search_releases previous
      ON previous.watermark=chain.previous_watermark
    WHERE chain.watermark<>$1 AND chain.depth<10000
  ), checked AS (
    SELECT chain.*,
           (SELECT count(*)::int FROM publication.creator_search_changes change
            WHERE change.watermark=chain.watermark) AS actual_change_count
    FROM release_chain chain
  ), rollback_changes AS (
    SELECT chain.depth,change.channel_id,change.before_document
    FROM release_chain chain
    JOIN publication.creator_search_changes change
      ON change.watermark=chain.watermark
    WHERE chain.watermark<>$1
  ), target_overrides AS (
    SELECT DISTINCT ON (channel_id) channel_id,before_document
    FROM rollback_changes
    ORDER BY channel_id,depth DESC
  ), expected_target AS (
    SELECT live.channel_id,to_jsonb(live)-'watermark' AS document
    FROM public.creator_search_live live
    LEFT JOIN target_overrides target_change USING(channel_id)
    WHERE target_change.channel_id IS NULL
    UNION ALL
    SELECT channel_id,before_document-'watermark' AS document
    FROM target_overrides
    WHERE before_document IS NOT NULL
  ), target_legacy AS (
    SELECT search.channel_id,to_jsonb(search)-'watermark' AS document
    FROM public.creator_search_current search
    WHERE search.watermark=$1
  ), target_parity AS (
    SELECT expected.channel_id AS expected_channel_id,
           legacy.channel_id AS legacy_channel_id,
           expected.document AS expected_document,
           legacy.document AS legacy_document
    FROM expected_target expected
    FULL JOIN target_legacy legacy USING(channel_id)
  )
  SELECT EXISTS(SELECT 1 FROM public.creator_search_releases WHERE watermark=$1)
           AS rollback_target_exists,
         (SELECT count(*)::int FROM public.creator_search_current WHERE watermark=$1)
           AS rollback_target_count,
         (SELECT count(*)::int FROM expected_target)
           AS rollback_target_expected_count,
         (SELECT count(*)::int FROM target_parity
          WHERE expected_channel_id IS NULL OR legacy_channel_id IS NULL
             OR expected_document IS DISTINCT FROM legacy_document)
           AS rollback_target_parity_diffs,
         EXISTS(SELECT 1 FROM checked WHERE watermark=$1) AS rollback_target_reachable,
         (SELECT count(*)::int FROM checked
          WHERE watermark<>$1 AND (
            changed_channel_count IS NULL
            OR actual_change_count=0
            OR actual_change_count IS DISTINCT FROM changed_channel_count
          )) AS rollback_chain_errors
`;

function integer(value, field) {
  const output = Number(value);
  if (!Number.isSafeInteger(output) || output < 0) {
    throw new Error(`Creator Search storage returned an invalid ${field}`);
  }
  return output;
}

function normalizeState(row) {
  if (!row) throw new Error("Creator Search storage state is missing");
  return {
    ...row,
    channel_count: integer(row.channel_count, "Channel count"),
    live_count: integer(row.live_count, "Live count"),
    legacy_count: integer(row.legacy_count, "Legacy count"),
    parity_diffs: integer(row.parity_diffs, "parity count"),
    in_flight_projection_count: integer(
      row.in_flight_projection_count,
      "in-flight Projection count",
    ),
    abnormal_ownership_count: integer(
      row.abnormal_ownership_count,
      "abnormal ownership count",
    ),
    database_bytes: row.database_bytes == null
      ? null
      : integer(row.database_bytes, "database size"),
    projection_status_counts: row.projection_status_counts ?? {},
  };
}

function assertIdentity(state, config) {
  if (state.database_name !== config.expectedDatabase) {
    throw new Error(
      `refusing unexpected Business database ${state.database_name || "unknown"}; expected ${config.expectedDatabase}`,
    );
  }
  if (state.identity_kind !== "business" || state.identity_database !== state.database_name) {
    throw new Error("Business database identity marker is missing or mismatched");
  }
  if (state.channel_count !== config.expectedBusinessChannelCount) {
    throw new Error(`Business Channel count changed: ${state.channel_count}`);
  }
}

function activationBlockers(state) {
  const blockers = [];
  if (state.activate_function_ready === false) blockers.push("cutover function is missing");
  if (!state.active_watermark) blockers.push("active Creator Search watermark is missing");
  if (state.write_mode !== "shadow" || state.read_mode !== "legacy") {
    blockers.push(`storage mode is ${state.write_mode}/${state.read_mode}, expected shadow/legacy`);
  }
  if (state.live_count !== state.legacy_count) {
    blockers.push(`Live/Legacy row counts differ (${state.live_count}/${state.legacy_count})`);
  }
  if (state.parity_diffs !== 0) blockers.push(`${state.parity_diffs} Live/Legacy rows differ`);
  if (state.in_flight_projection_count !== 0) {
    blockers.push(`${state.in_flight_projection_count} Projection Outbox rows are in flight`);
  }
  if (state.abnormal_ownership_count !== 0) {
    blockers.push(`${state.abnormal_ownership_count} Channel ownership rows are not online/active`);
  }
  return blockers;
}

async function inspectStorage(client, config) {
  const state = normalizeState((await client.query(INSPECT_STORAGE_SQL)).rows[0]);
  assertIdentity(state, config);
  return state;
}

function assertExpected(state, expectedWatermark, expectedLiveCount) {
  if (state.active_watermark !== expectedWatermark) {
    throw new Error("Creator Search active watermark changed after the approved plan");
  }
  if (state.live_count !== expectedLiveCount) {
    throw new Error("Creator Search Live row count changed after the approved plan");
  }
}

export class BusinessCreatorSearchStorageAdministrator {
  constructor({ pool, config }) {
    if (!pool?.connect) throw new TypeError("a PostgreSQL Pool is required");
    this.pool = pool;
    this.config = config;
  }

  async inspectReadOnly() {
    const client = await this.pool.connect();
    let began = false;
    try {
      await client.query("BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY");
      began = true;
      await client.query("SET LOCAL lock_timeout='10s'");
      await client.query("SET LOCAL statement_timeout='180s'");
      const state = await inspectStorage(client, this.config);
      const blockers = activationBlockers(state);
      await client.query("ROLLBACK");
      began = false;
      return { ready: blockers.length === 0, blockers, state };
    } catch (error) {
      if (began) await client.query("ROLLBACK").catch(() => {});
      throw error;
    } finally {
      client.release();
    }
  }

  async apply({ expectedWatermark, expectedLiveCount }) {
    const watermark = requiredText(expectedWatermark, "expected watermark");
    if (!Number.isSafeInteger(expectedLiveCount) || expectedLiveCount < 0) {
      throw new TypeError("expected Live count must be a non-negative integer");
    }
    const client = await this.pool.connect();
    let began = false;
    try {
      await client.query("BEGIN TRANSACTION ISOLATION LEVEL SERIALIZABLE");
      began = true;
      await client.query("SET LOCAL lock_timeout='10s'");
      await client.query("SET LOCAL statement_timeout='180s'");
      await client.query(
        `SELECT pg_advisory_xact_lock(hashtext('${SEARCH_PUBLISH_LOCK}'))`,
      );
      const state = await inspectStorage(client, this.config);
      assertExpected(state, watermark, expectedLiveCount);
      const blockers = activationBlockers(state);
      if (blockers.length > 0) {
        throw new Error(`Creator Search storage cutover is blocked: ${blockers.join("; ")}`);
      }
      const result = (await client.query(
        `SELECT public.activate_creator_search_incremental_v1($1,$2,$3,$4)
           AS storage_mode`,
        [watermark, expectedLiveCount, this.config.actor, this.config.reason],
      )).rows[0];
      if (result?.storage_mode !== "incremental") {
        throw new Error("Creator Search storage cutover returned an unexpected mode");
      }
      await client.query("COMMIT");
      began = false;
      return {
        outcome: "applied",
        database_name: state.database_name,
        active_watermark: watermark,
        live_count: expectedLiveCount,
        storage_mode: result.storage_mode,
      };
    } catch (error) {
      if (began) await client.query("ROLLBACK").catch(() => {});
      throw error;
    } finally {
      client.release();
    }
  }

  async inspectRollbackReadOnly(targetWatermark = this.config.rollbackWatermark) {
    const target = requiredText(targetWatermark, "BUSINESS_CREATOR_SEARCH_ROLLBACK_WATERMARK");
    const client = await this.pool.connect();
    let began = false;
    try {
      await client.query("BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY");
      began = true;
      await client.query("SET LOCAL lock_timeout='10s'");
      await client.query("SET LOCAL statement_timeout='180s'");
      const state = await inspectStorage(client, this.config);
      const targetState = (await client.query(INSPECT_ROLLBACK_TARGET_SQL, [target])).rows[0] ?? {};
      const combined = {
        ...state,
        rollback_target_watermark: target,
        rollback_target_count: integer(
          targetState.rollback_target_count,
          "rollback target count",
        ),
        rollback_target_expected_count: integer(
          targetState.rollback_target_expected_count,
          "rollback target expected count",
        ),
        rollback_target_parity_diffs: integer(
          targetState.rollback_target_parity_diffs,
          "rollback target parity count",
        ),
        rollback_target_exists: targetState.rollback_target_exists === true,
        rollback_target_reachable: targetState.rollback_target_reachable === true,
        rollback_chain_errors: integer(
          targetState.rollback_chain_errors,
          "rollback chain error count",
        ),
      };
      const blockers = [];
      if (state.rollback_function_ready === false) blockers.push("storage rollback function is missing");
      if (state.write_mode !== "incremental" || state.read_mode !== "live") {
        blockers.push(`storage mode is ${state.write_mode}/${state.read_mode}, expected incremental/live`);
      }
      if (!combined.rollback_target_exists) blockers.push("rollback target release is missing");
      if (!combined.rollback_target_reachable) blockers.push("rollback target is not reachable from active release");
      if (combined.rollback_chain_errors !== 0) {
        blockers.push(`${combined.rollback_chain_errors} rollback releases have incomplete changes`);
      }
      if (
        combined.rollback_target_exists
          && combined.rollback_target_reachable
          && combined.rollback_chain_errors === 0
          && (
            combined.rollback_target_count !== combined.rollback_target_expected_count
            || combined.rollback_target_parity_diffs !== 0
          )
      ) {
        blockers.push(
          `rollback target Legacy snapshot has ${combined.rollback_target_count}/${combined.rollback_target_expected_count} rows and ${combined.rollback_target_parity_diffs} parity differences`,
        );
      }
      if (state.in_flight_projection_count !== 0) {
        blockers.push(`${state.in_flight_projection_count} Projection Outbox rows are in flight`);
      }
      await client.query("ROLLBACK");
      began = false;
      return { ready: blockers.length === 0, blockers, state: combined };
    } catch (error) {
      if (began) await client.query("ROLLBACK").catch(() => {});
      throw error;
    } finally {
      client.release();
    }
  }

  async rollback({
    expectedActiveWatermark,
    expectedCurrentLiveCount,
    targetWatermark,
    expectedLiveCount,
  }) {
    const expectedActive = requiredText(expectedActiveWatermark, "expected active watermark");
    const target = requiredText(targetWatermark, "rollback target watermark");
    if (!Number.isSafeInteger(expectedCurrentLiveCount) || expectedCurrentLiveCount < 0) {
      throw new TypeError("expected current Live count must be a non-negative integer");
    }
    if (!Number.isSafeInteger(expectedLiveCount) || expectedLiveCount < 0) {
      throw new TypeError("expected rollback Live count must be a non-negative integer");
    }
    const client = await this.pool.connect();
    let began = false;
    try {
      await client.query("BEGIN TRANSACTION ISOLATION LEVEL SERIALIZABLE");
      began = true;
      await client.query("SET LOCAL lock_timeout='10s'");
      await client.query("SET LOCAL statement_timeout='900s'");
      await client.query(
        `SELECT pg_advisory_xact_lock(hashtext('${SEARCH_PUBLISH_LOCK}'))`,
      );
      const state = await inspectStorage(client, this.config);
      assertExpected(state, expectedActive, expectedCurrentLiveCount);
      if (state.write_mode !== "incremental" || state.read_mode !== "live") {
        throw new Error("Creator Search storage is not in incremental/live mode");
      }
      if (state.in_flight_projection_count !== 0) {
        throw new Error("Creator Search storage rollback is blocked by in-flight Projections");
      }
      const result = (await client.query(
        `SELECT public.rollback_creator_search_incremental_storage_v1($1,$2,$3,$4)
           AS rollback_count`,
        [target, expectedLiveCount, this.config.actor, this.config.reason],
      )).rows[0];
      const rollbackCount = integer(result?.rollback_count, "rollback count");
      await client.query("COMMIT");
      began = false;
      return {
        outcome: "rolled_back",
        database_name: state.database_name,
        previous_active_watermark: expectedActive,
        active_watermark: target,
        live_count: expectedLiveCount,
        rollback_count: rollbackCount,
      };
    } catch (error) {
      if (began) await client.query("ROLLBACK").catch(() => {});
      throw error;
    } finally {
      client.release();
    }
  }
}
