import { buildVideoPublicationItem } from "./videoPublicationCurrent.js";

function text(value) {
  const output = String(value ?? "").trim();
  return output || null;
}

function uniqueContentKeys(values) {
  return [...new Set((Array.isArray(values) ? values : [values]).map(text).filter(Boolean))].sort();
}

export async function refreshVideoPublicationItemHashes(client, contentKeys) {
  if (!client || typeof client.query !== "function") {
    throw new TypeError("an active PostgreSQL client is required");
  }
  const keys = uniqueContentKeys(contentKeys);
  if (keys.length === 0) {
    return {
      requested_count: 0,
      found_count: 0,
      ready_count: 0,
      incomplete_count: 0,
      changed_count: 0,
      items: [],
    };
  }
  const selected = await client.query(
    `SELECT to_jsonb(content) - 'raw_json' AS row
     FROM crawler.contents content
     WHERE content.content_key=ANY($1::text[])
     ORDER BY content.content_key
     FOR UPDATE`,
    [keys],
  );
  const items = selected.rows.map((wrapper) => {
    const row = wrapper.row ?? wrapper;
    const item = buildVideoPublicationItem(row, { channelId: row.channel_id });
    return {
      content_key: row.content_key,
      ready: item.ready,
      item_hash: item.item_hash,
      issues: item.issues,
    };
  });
  const updates = items.map((item) => ({
    content_key: item.content_key,
    publication_item_hash: item.item_hash,
  }));
  const changed = updates.length === 0
    ? { rowCount: 0 }
    : await client.query(
        `WITH input AS (
           SELECT *
           FROM jsonb_to_recordset($1::jsonb)
             AS item(content_key text,publication_item_hash text)
         )
         UPDATE crawler.contents content
         SET publication_item_hash=input.publication_item_hash
         FROM input
         WHERE content.content_key=input.content_key
           AND content.publication_item_hash IS DISTINCT FROM input.publication_item_hash`,
        [JSON.stringify(updates)],
      );
  return {
    requested_count: keys.length,
    found_count: items.length,
    ready_count: items.filter((item) => item.ready).length,
    incomplete_count: items.filter((item) => !item.ready).length,
    changed_count: changed.rowCount,
    items,
  };
}
