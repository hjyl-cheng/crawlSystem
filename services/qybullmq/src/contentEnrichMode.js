export const CONTENT_ENRICH_SETTING_KEY = "content_enrich_dispatch";
export const CONTENT_ENRICH_CLOCK_MODE = "clock";
export const CONTENT_ENRICH_QUEUE_MODE = "queue";
export const CONTENT_ENRICH_DISPATCH_LOCK_KEY = "content-enrich-dispatch-v1";
export const CONTENT_ENRICH_DISPATCH_MUTEX_SETTING_KEY = "content_enrich_dispatch_mutex";

export async function loadContentEnrichMode(client, { lock = false } = {}) {
  if (!client || typeof client.query !== "function") {
    throw new TypeError("an active PostgreSQL client is required");
  }
  const result = await client.query(
    `SELECT COALESCE(value_json->>'mode','clock') AS mode
     FROM crawler.settings
     WHERE setting_key='content_enrich_dispatch'
     ${lock ? "FOR SHARE" : ""}`,
  );
  const mode = String(result.rows[0]?.mode ?? CONTENT_ENRICH_CLOCK_MODE).trim().toLowerCase();
  if (![CONTENT_ENRICH_CLOCK_MODE, CONTENT_ENRICH_QUEUE_MODE].includes(mode)) {
    throw new Error(`unsupported Content Enrich consumer mode: ${mode}`);
  }
  return mode;
}

export async function switchContentEnrichMode(client, {
  mode,
  changedBy,
  reason,
  now = new Date(),
} = {}) {
  if (!client || typeof client.query !== "function") {
    throw new TypeError("an active PostgreSQL client is required");
  }
  const targetMode = String(mode ?? "").trim().toLowerCase();
  if (![CONTENT_ENRICH_CLOCK_MODE, CONTENT_ENRICH_QUEUE_MODE].includes(targetMode)) {
    throw new TypeError("mode must be clock or queue");
  }
  const actor = String(changedBy ?? "").trim();
  const changeReason = String(reason ?? "").trim();
  if (!actor) throw new TypeError("changedBy is required");
  if (!changeReason) throw new TypeError("reason is required");
  await client.query(
    "SELECT pg_advisory_xact_lock(hashtext($1))",
    [CONTENT_ENRICH_DISPATCH_LOCK_KEY],
  );
  await client.query(
    `INSERT INTO crawler.settings (setting_key,value_json,updated_at)
     VALUES ($1,'{"owner":null,"expires_at":null}'::jsonb,clock_timestamp())
     ON CONFLICT (setting_key) DO NOTHING`,
    [CONTENT_ENRICH_DISPATCH_MUTEX_SETTING_KEY],
  );
  const mutex = await client.query(
    `SELECT value_json
     FROM crawler.settings
     WHERE setting_key=$1
     FOR UPDATE`,
    [CONTENT_ENRICH_DISPATCH_MUTEX_SETTING_KEY],
  );
  const mutexClock = await client.query("SELECT clock_timestamp() AS observed_at");
  const mutexValue = mutex.rows[0]?.value_json ?? {};
  const mutexOwner = String(mutexValue.owner ?? "").trim();
  const mutexExpiresAt = Date.parse(String(mutexValue.expires_at ?? ""));
  const mutexObservedAt = Date.parse(String(mutexClock.rows[0]?.observed_at ?? ""));
  if (mutexOwner) {
    if (!Number.isFinite(mutexExpiresAt) || !Number.isFinite(mutexObservedAt)) {
      throw new Error("Content Enrich dispatch mutex state is invalid");
    }
    if (mutexExpiresAt > mutexObservedAt) {
      throw new Error(`Content Enrich dispatch mutex is active: ${mutexOwner}`);
    }
  }
  await client.query(
    `UPDATE crawler.settings
     SET value_json=jsonb_build_object('owner',NULL,'expires_at',NULL),
         updated_at=clock_timestamp()
     WHERE setting_key=$1`,
    [CONTENT_ENRICH_DISPATCH_MUTEX_SETTING_KEY],
  );
  await client.query(
    `INSERT INTO crawler.settings (setting_key,value_json,updated_at)
     VALUES ('content_enrich_dispatch','{"mode":"clock"}'::jsonb,now())
     ON CONFLICT (setting_key) DO NOTHING`,
  );
  const current = await client.query(
    `SELECT value_json
     FROM crawler.settings
     WHERE setting_key='content_enrich_dispatch'
     FOR UPDATE`,
  );
  const previousMode = String(current.rows[0]?.value_json?.mode ?? CONTENT_ENRICH_CLOCK_MODE)
    .trim()
    .toLowerCase();
  const updated = await client.query(
    `UPDATE crawler.settings
     SET value_json=value_json || jsonb_build_object(
           'mode',$1::text,
           'changed_at',$2::timestamptz,
           'changed_by',$3::text,
           'reason',$4::text
         ),
         updated_at=now()
     WHERE setting_key='content_enrich_dispatch'
     RETURNING value_json`,
    [targetMode, new Date(now).toISOString(), actor, changeReason],
  );
  return {
    previous_mode: previousMode,
    mode: String(updated.rows[0]?.value_json?.mode ?? targetMode),
    changed_at: new Date(now).toISOString(),
    changed_by: actor,
    reason: changeReason,
  };
}
