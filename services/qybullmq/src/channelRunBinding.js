export async function prepareChannelRun(client, {
  runId,
  channelId,
  candidateId,
  crawlMode,
  contentLimit,
  resultJson,
}) {
  const prepared = await client.query(
    `INSERT INTO crawler.channel_runs (
       run_id,channel_id,candidate_id,status,crawl_mode,content_limit,detail_status,started_at,result_json,updated_at
     ) VALUES ($1,$2,$3,'running',$4,$5,'pending',now(),$6::jsonb,now())
     ON CONFLICT (run_id) DO UPDATE
     SET status=CASE
           WHEN crawler.channel_runs.publication_finalized_at IS NULL THEN 'running'
           ELSE crawler.channel_runs.status
         END,
         content_limit=CASE
           WHEN crawler.channel_runs.publication_finalized_at IS NULL THEN EXCLUDED.content_limit
           ELSE crawler.channel_runs.content_limit
         END,
         detail_status=CASE
           WHEN crawler.channel_runs.publication_finalized_at IS NULL THEN 'pending'
           ELSE crawler.channel_runs.detail_status
         END,
         result_json=CASE
           WHEN crawler.channel_runs.publication_finalized_at IS NULL
             THEN crawler.channel_runs.result_json || EXCLUDED.result_json
           ELSE crawler.channel_runs.result_json
         END,
         updated_at=CASE
           WHEN crawler.channel_runs.publication_finalized_at IS NULL THEN now()
           ELSE crawler.channel_runs.updated_at
         END
     WHERE crawler.channel_runs.channel_id=EXCLUDED.channel_id
       AND crawler.channel_runs.candidate_id IS NOT DISTINCT FROM EXCLUDED.candidate_id
       AND crawler.channel_runs.crawl_mode=EXCLUDED.crawl_mode
     RETURNING run_id,publication_finalized_at`,
    [runId, channelId, candidateId, crawlMode, contentLimit, JSON.stringify(resultJson)],
  );
  if (prepared.rowCount !== 1) {
    throw new Error(`Channel Run identity conflict: ${runId}`);
  }
  if (prepared.rows[0]?.publication_finalized_at == null) {
    const bound = await client.query(
      `UPDATE crawler.channels AS channel
       SET latest_run_id=$1,updated_at=now()
       WHERE channel.channel_id=$2 AND channel.status='active'
         AND (
           channel.registry_promotion_run_id IS NULL
           OR channel.registry_promotion_run_id=$1
           OR crawler.registry_promotion_is_complete(
             channel.channel_id,
             channel.registry_promotion_run_id
           )
           OR crawler.registry_publication_gap_repair_is_allowed(
             channel.channel_id,
             channel.latest_run_id,
             $1
           )
         )
       RETURNING channel.latest_run_id`,
      [runId, channelId],
    );
    if (bound.rowCount !== 1) {
      const current = await client.query(
        `SELECT channel.status,channel.latest_run_id,channel.registry_promotion_run_id,
                crawler.registry_promotion_is_complete(
                  channel.channel_id,
                  channel.registry_promotion_run_id
                ) AS promotion_complete,
                crawler.registry_publication_gap_repair_is_allowed(
                  channel.channel_id,
                  channel.latest_run_id,
                  $2
                ) AS publication_gap_repair_allowed
         FROM crawler.channels AS channel
         WHERE channel.channel_id=$1`,
        [channelId, runId],
      );
      const channel = current.rows[0];
      const promotionReady = channel?.registry_promotion_run_id == null
        || String(channel.registry_promotion_run_id) === String(runId)
        || channel.promotion_complete === true
        || channel.publication_gap_repair_allowed === true;
      if (
        String(channel?.latest_run_id ?? "") !== String(runId)
        || !promotionReady
      ) {
        throw new Error(`Channel Registry promotion Run must Finalize before a later Run: ${channelId}`);
      }
    }
  }
}

export async function prepareChannelRunAndBindJob({ job, runId, prepare }) {
  const value = String(runId ?? "").trim();
  if (!value) throw new Error("run_id is required before binding a channel job");
  if (typeof prepare !== "function") throw new Error("channel run preparation callback is required");

  await prepare();
  return bindPreparedChannelRun({ job, runId: value });
}

export async function bindPreparedChannelRun({ job, runId }) {
  const value = String(runId ?? "").trim();
  if (!value) throw new Error("run_id is required before binding a channel job");
  if (typeof job?.updateData === "function" && job.data?.run_id !== value) {
    const data = { ...(job.data ?? {}), run_id: value };
    await job.updateData(data);
    job.data = data;
  }
  return value;
}
