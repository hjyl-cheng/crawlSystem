import { AGENT_TAXONOMY_VERSION } from "./publicationContract.js";

export async function reconcileFullCrawlAgentState(dbQuery, {
  channelId,
  eligible,
} = {}) {
  if (typeof dbQuery !== "function") throw new TypeError("dbQuery is required");
  const normalizedChannelId = String(channelId ?? "").trim();
  if (!normalizedChannelId) throw new TypeError("channelId is required");
  if (typeof eligible !== "boolean") throw new TypeError("eligible must be boolean");

  const result = await dbQuery(
    `UPDATE crawler.channels channel
     SET ready_for_agent=$2,
         agent_status=CASE
           WHEN channel.agent_status='done' AND EXISTS (
             SELECT 1
             FROM crawler.agent_profiles profile
             JOIN crawler.agent_configs config
               ON config.config_id=profile.agent_config_id
             WHERE profile.channel_id=channel.channel_id
               AND profile.agent_mode='basic'
               AND profile.status='success'
               AND NULLIF(btrim(profile.agent_model),'') IS NOT NULL
               AND profile.agent_config_id IS NOT NULL
               AND (
                 (
                   config.provider='local-offline'
                   AND profile.prompt_template_id IS NULL
                   AND profile.prompt_hash IS NULL
                   AND profile.prompt_variant='local_offline'
                 )
                 OR
                 (
                   config.provider<>'local-offline'
                   AND profile.prompt_template_id IS NOT NULL
                   AND profile.prompt_hash ~ '^[0-9a-f]{64}$'
                   AND profile.prompt_variant IN ('country_required','country_resolved')
                 )
               )
               AND profile.input_content_ids IS NOT NULL
               AND profile.input_content_hash ~ '^sha256:[0-9a-f]{64}$'
               AND profile.taxonomy_version=$3
               AND profile.agent_version_hash ~ '^sha256:[0-9a-f]{64}$'
           ) THEN 'done'
           WHEN $2 THEN 'pending'
           ELSE 'skipped'
         END,
         updated_at=now()
     WHERE channel.channel_id=$1
       AND channel.status<>'removed'
     RETURNING channel_id,ready_for_agent,agent_status`,
    [normalizedChannelId, eligible, AGENT_TAXONOMY_VERSION],
  );
  const state = result.rows?.[0];
  if (!state) throw new Error(`active Full Crawl Channel not found: ${normalizedChannelId}`);
  return state;
}
