export const DISCOVERY_PAGE_READY_CHANNEL = "crawler:discovery-page-qualification-ready:v1";

const readyDiscoveryPagesSql = `
  SELECT DISTINCT page.page_id
  FROM crawler.query_pages page
  WHERE page.status='running'
    AND page.result_json->>'qualification_phase'='awaiting_snapshot_validation'
    AND ($1::bigint IS NULL OR EXISTS (
      SELECT 1
      FROM crawler.channel_candidate_sources source
      WHERE source.page_id=page.page_id
        AND source.candidate_id=$1::bigint
    ))
    AND ($2::text IS NULL OR page.page_id=$2::text)
    AND (
      SELECT count(DISTINCT source.candidate_id)
      FROM crawler.channel_candidate_sources source
      WHERE source.page_id=page.page_id
    ) >= page.candidate_count
    AND NOT EXISTS (
      SELECT 1
      FROM crawler.channel_candidate_sources source
      JOIN crawler.channel_candidates candidate ON candidate.candidate_id=source.candidate_id
      WHERE source.page_id=page.page_id
        AND candidate.status IN ('discovered','queued','validating')
    )
  ORDER BY page.page_id`;

function positiveInteger(value) {
  const parsed = Number(value);
  return Number.isInteger(parsed) && parsed > 0 ? parsed : null;
}

function nonemptyText(value) {
  const parsed = String(value ?? "").trim();
  return parsed || null;
}

export async function publishReadyDiscoveryPages({ query, queue, candidateId = null, pageId = null }) {
  const normalizedCandidateId = positiveInteger(candidateId);
  const normalizedPageId = nonemptyText(pageId);
  if (!normalizedCandidateId && !normalizedPageId) return [];

  const result = await query(readyDiscoveryPagesSql, [normalizedCandidateId, normalizedPageId]);
  const pageIds = [...new Set(result.rows.map((row) => nonemptyText(row.page_id)).filter(Boolean))];
  if (pageIds.length === 0) return [];

  const client = await queue.client;
  await client.publish(DISCOVERY_PAGE_READY_CHANNEL, JSON.stringify({
    type: "discovery_page_qualification_ready",
    page_ids: pageIds,
  }));
  return pageIds;
}

export function createCoalescedWakeup(run, { delayMs = 25, onError = null } = {}) {
  let timer = null;
  let running = false;
  let pending = false;
  let closed = false;
  const delay = Math.max(0, Number(delayMs) || 0);

  const arm = () => {
    if (closed || running || timer || !pending) return;
    timer = setTimeout(async () => {
      timer = null;
      if (closed || !pending) return;
      pending = false;
      running = true;
      try {
        await run();
      } catch (error) {
        if (onError) onError(error);
      } finally {
        running = false;
        arm();
      }
    }, delay);
  };

  return {
    request() {
      if (closed) return false;
      pending = true;
      arm();
      return true;
    },
    close() {
      closed = true;
      pending = false;
      if (timer) clearTimeout(timer);
      timer = null;
    },
  };
}
