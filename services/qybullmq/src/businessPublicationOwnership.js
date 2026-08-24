const BUSINESS_OWNERSHIP_ACTOR = "business-publication-ingress-v1";
const BUSINESS_OWNERSHIP_REASON = "automatic ownership for a new Channel Bootstrap";

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

function automaticBootstrapEnvelope(envelope) {
  return envelope?.revision_type === "bootstrap"
    && envelope?.domain === "channel"
    && Number(envelope?.data_sequence) === 1
    && envelope?.previous_data_sequence == null;
}

async function findOwnership(client, channelId) {
  const result = await client.query(
    `/* business-publication-ownership:find */
     SELECT channel_id,active_publication_stream_id,status,projection_mode,
            ownership_reference,state_changed_at
     FROM publication.channel_ownership
     WHERE channel_id=$1`,
    [channelId],
  );
  return result.rows[0] ?? null;
}

export async function ensureAutomaticBusinessBootstrapOwnership(clientValue, envelope) {
  const client = activeClient(clientValue);
  if (!automaticBootstrapEnvelope(envelope)) {
    return { status: "not_bootstrap", created: false, ownership: null };
  }
  const channelId = requiredText(envelope.channel_id, "envelope.channel_id");
  const streamId = requiredText(
    envelope.publication_stream_id,
    "envelope.publication_stream_id",
  ).toLowerCase();
  const existing = await findOwnership(client, channelId);
  if (existing) return { status: "existing", created: false, ownership: existing };

  const policy = await client.query(
    `/* business-publication-ownership:inherit-policy */
     SELECT stream.automatic_onboarding_projection_mode,
            count(owner.channel_id)::int AS owner_count,
            bool_and(owner.projection_mode='online') AS all_online
     FROM publication.stream AS stream
     LEFT JOIN publication.channel_ownership AS owner
       ON owner.active_publication_stream_id=stream.publication_stream_id
      AND owner.status IN ('active','cutover_pending')
     WHERE stream.publication_stream_id=$1::uuid AND stream.status='active'
     GROUP BY stream.publication_stream_id,
              stream.automatic_onboarding_projection_mode`,
    [streamId],
  );
  const inherited = policy.rows[0] ?? {};
  const explicitProjectionMode = String(
    inherited.automatic_onboarding_projection_mode ?? "",
  ).trim();
  if (!explicitProjectionMode && Number(inherited.owner_count ?? 0) === 0) {
    return { status: "automatic_onboarding_not_enabled", created: false, ownership: null };
  }
  const projectionMode = explicitProjectionMode || (
    inherited.all_online === true ? "online" : "held_shadow"
  );
  const ownershipReference = {
    onboarding_mode: "automatic_bootstrap",
    publication_stream_id: streamId,
    first_revision_id: requiredText(envelope.revision_id, "envelope.revision_id").toLowerCase(),
  };
  const inserted = await client.query(
    `/* business-publication-ownership:insert */
     INSERT INTO publication.channel_ownership (
       channel_id,active_publication_stream_id,status,previous_publication_stream_id,
       ownership_reference,projection_mode,state_changed_by,state_reason
     ) VALUES ($1,$2::uuid,'active',NULL,$3::jsonb,$4,$5,$6)
     ON CONFLICT (channel_id) DO NOTHING
     RETURNING channel_id`,
    [
      channelId,
      streamId,
      JSON.stringify(ownershipReference),
      projectionMode,
      BUSINESS_OWNERSHIP_ACTOR,
      BUSINESS_OWNERSHIP_REASON,
    ],
  );
  const ownership = await findOwnership(client, channelId);
  return {
    status: inserted.rowCount === 1 ? "registered" : "existing",
    created: inserted.rowCount === 1,
    ownership,
  };
}
