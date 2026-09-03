export function postgresReadOnlyQuery(client) {
  if (!client || typeof client.query !== "function") {
    throw new TypeError("an active PostgreSQL client is required");
  }
  return async (sql, params = []) => {
    await client.query("BEGIN TRANSACTION READ ONLY");
    try {
      const guard = await client.query("SHOW transaction_read_only");
      if (guard.rows[0]?.transaction_read_only !== "on") {
        throw new Error("PostgreSQL probe transaction is not read-only");
      }
      const result = await client.query(sql, params);
      await client.query("COMMIT");
      return result;
    } catch (error) {
      await client.query("ROLLBACK").catch(() => {});
      throw error;
    }
  };
}

function requiredText(value, field) {
  const normalized = String(value ?? "").trim();
  if (!normalized) throw new TypeError(`${field} is required`);
  return normalized;
}

function optionalText(value) {
  const normalized = String(value ?? "").trim();
  return normalized || null;
}

function isoDay(value, field) {
  const normalized = requiredText(value, field);
  const parsed = new Date(`${normalized}T00:00:00.000Z`);
  if (!/^\d{4}-\d{2}-\d{2}$/.test(normalized)
      || Number.isNaN(parsed.getTime())
      || parsed.toISOString().slice(0, 10) !== normalized) {
    throw new TypeError(`${field} must be an ISO date`);
  }
  return normalized;
}

function postgresDay(value, field) {
  if (value instanceof Date) {
    if (Number.isNaN(value.getTime())) throw new TypeError(`${field} must be an ISO date`);
    return value.toISOString().slice(0, 10);
  }
  return isoDay(value, field);
}

function planFromClockStateRow(row, planDay, {
  capacityFactor,
  playerCap,
  nextCap,
  capacityVersion,
  plannerConfigVersion,
}) {
  if (!row) return null;
  const channelId = String(row.channel_id);
  const syntheticPlanId = `clock-state:${channelId}:${planDay}`;
  return {
    clock: {
      source: "channel_clock_state",
      formal_daily_plan: false,
      plan_id: syntheticPlanId,
      plan_day: planDay,
      channel_id: channelId,
      status: "clock_state_probe",
      plan_mode: "read_only_probe",
      due_day: postgresDay(row.video_due_day, "channel_clock_state.video_due_day"),
      scheduled_at: null,
      finished_at: null,
      lifecycle_status: String(row.lifecycle_status),
      video_due_at: new Date(row.video_due_at).toISOString(),
      video_last_complete_at: row.video_last_complete_at == null
        ? null
        : new Date(row.video_last_complete_at).toISOString(),
      video_last_outcome: row.video_last_outcome == null
        ? null
        : String(row.video_last_outcome),
      clock_version: Number(row.clock_version),
      note: "Only the channel ID is sampled; formal Daily Plan inputs are neither read nor claimed",
    },
    plan: {
      plan_id: syntheticPlanId,
      plan_mode: "manual/read_only_probe",
      formal_daily_plan: false,
      plan_day: planDay,
      channel_id: channelId,
      task_mask: { about: false, video: true, agent: false },
      capacity: {
        factor: capacityFactor,
        player_cap: playerCap,
        next_cap: nextCap,
        version: capacityVersion,
      },
      planner_config_version: plannerConfigVersion,
    },
  };
}

export async function selectIncrementalYoutubeJsVideoProbePlan(query, {
  channelId = null,
  planDay,
  clockStateFallback = {
    capacityFactor: 1,
    playerCap: 1,
    nextCap: 0,
    capacityVersion: "read-only-probe-v1",
    plannerConfigVersion: "video-plan-1",
  },
} = {}) {
  if (typeof query !== "function") throw new TypeError("query is required");
  const normalizedChannelId = optionalText(channelId);
  const normalizedPlanDay = isoDay(planDay, "planDay");
  if (clockStateFallback) {
    const fallback = await query(
      `SELECT clock.channel_id,clock.lifecycle_status,
              clock.video_due_at,clock.video_due_day::text AS video_due_day,
              clock.video_last_complete_at,clock.video_last_outcome,
              clock.clock_version
       FROM feature_clock.channel_clock_state clock
       WHERE clock.lifecycle_status='active'
         AND ($1::text IS NULL OR clock.channel_id=$1::text)
         AND NOT EXISTS (
           SELECT 1
           FROM feature_clock.daily_channel_plans active_plan
           WHERE active_plan.channel_id=clock.channel_id
             AND active_plan.status IN ('dispatching','dispatched','running')
         )
       ORDER BY clock.video_due_at,clock.dispatch_slot,clock.channel_id
       LIMIT 1`,
      [normalizedChannelId],
    );
    const fallbackConfig = {
      capacityFactor: Number(clockStateFallback.capacityFactor),
      playerCap: Number(clockStateFallback.playerCap),
      nextCap: Number(clockStateFallback.nextCap),
      capacityVersion: requiredText(
        clockStateFallback.capacityVersion,
        "clockStateFallback.capacityVersion",
      ),
      plannerConfigVersion: requiredText(
        clockStateFallback.plannerConfigVersion,
        "clockStateFallback.plannerConfigVersion",
      ),
    };
    if (!Number.isFinite(fallbackConfig.capacityFactor)
        || fallbackConfig.capacityFactor < 0
        || fallbackConfig.capacityFactor > 1
        || !Number.isSafeInteger(fallbackConfig.playerCap)
        || fallbackConfig.playerCap < 0
        || !Number.isSafeInteger(fallbackConfig.nextCap)
        || fallbackConfig.nextCap < 0) {
      throw new TypeError("clockStateFallback capacity is invalid");
    }
    const fallbackSelected = planFromClockStateRow(
      fallback.rows[0],
      normalizedPlanDay,
      fallbackConfig,
    );
    if (fallbackSelected) return fallbackSelected;
  }

  const scope = normalizedChannelId
    ? `channel ${normalizedChannelId}`
    : "the Channel Clock sample set";
  throw new Error(
    `no inactive Video Clock sample found for ${scope}; channels with an active Daily Plan are always excluded`,
  );
}
