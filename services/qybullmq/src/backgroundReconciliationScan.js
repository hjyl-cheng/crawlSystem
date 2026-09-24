import { randomUUID } from 'node:crypto';

const LEASE_SECONDS = 60;
export function scanPageSize(value = 200) {
  if (!Number.isSafeInteger(value) || value < 1 || value > 200) throw new TypeError('scan page size must be 1..200');
  return value;
}

// A cursor is metadata, never authority to publish. Each business transaction
// also rechecks its existing ownership/Run predicates. Holding this small row
// fences old scanners even if a lease expires while their query is running.
export async function claimChannelScan({ withTransaction, scope, pageSize = 200, roundPauseMs = 0, batchId = null }) {
  scanPageSize(pageSize);
  if (typeof scope !== 'string' || !scope || !Number.isSafeInteger(roundPauseMs) || roundPauseMs < 0) throw new TypeError('invalid scan options');
  return withTransaction(async client => {
    await client.query('INSERT INTO crawler.background_reconciliation_scans(scope) VALUES($1) ON CONFLICT DO NOTHING', [scope]);
    const row = (await client.query(`SELECT * FROM crawler.background_reconciliation_scans
      WHERE scope=$1 AND (lease_until IS NULL OR lease_until<clock_timestamp())
      AND (upper_channel_id IS NOT NULL OR last_completed_at IS NULL
        OR last_completed_at<=clock_timestamp()-($2*interval '1 millisecond'))
      FOR UPDATE SKIP LOCKED`, [scope, roundPauseMs])).rows[0];
    if (!row) return null;
    const source = batchId == null ? 'crawler.channels' : 'crawler.channel_candidates';
    let upper = row.upper_channel_id;
    if (upper == null) {
      upper = (await client.query(`SELECT channel_id FROM ${source}
        ${batchId == null ? '' : 'WHERE dispatch_batch_id=$1'} ORDER BY channel_id DESC LIMIT 1`,
      batchId == null ? [] : [batchId])).rows[0]?.channel_id ?? '';
    }
    const ids = upper === '' ? [] : (await client.query(`/* background-reconciliation:page */
      SELECT channel_id FROM ${source} WHERE channel_id>$1 AND channel_id<=$2
      ${batchId == null ? '' : 'AND dispatch_batch_id=$4'} ORDER BY channel_id LIMIT $3`,
    batchId == null ? [row.after_channel_id, upper, pageSize] : [row.after_channel_id, upper, pageSize, batchId])).rows.map(r => r.channel_id);
    const token = randomUUID();
    await client.query(`UPDATE crawler.background_reconciliation_scans SET lease_token=$2,
      lease_until=clock_timestamp()+($3*interval '1 second'),upper_channel_id=$4,
      round_started_at=CASE WHEN upper_channel_id IS NULL THEN clock_timestamp() ELSE round_started_at END,
      updated_at=clock_timestamp() WHERE scope=$1`, [scope, token, LEASE_SECONDS, upper]);
    return { ...row, scope, lease_token: token, upper_channel_id: upper, ids, pageSize };
  });
}

export async function scanTransaction({ withTransaction }, claim, action, after = null) {
  return withTransaction(async client => {
    const owned = await client.query(`SELECT scope FROM crawler.background_reconciliation_scans
      WHERE scope=$1 AND lease_token=$2 AND lease_until>clock_timestamp() FOR UPDATE`, [claim.scope, claim.lease_token]);
    if (owned.rowCount !== 1) throw new Error('SCAN_LEASE_LOST');
    const result = await action(client);
    if (after !== null && !claim.ids.includes(after)) throw new Error('SCAN_CURSOR_OUTSIDE_PAGE');
    await client.query(`UPDATE crawler.background_reconciliation_scans SET
      after_channel_id=COALESCE($3,after_channel_id),lease_until=clock_timestamp()+($4*interval '1 second'),
      updated_at=clock_timestamp() WHERE scope=$1 AND lease_token=$2`, [claim.scope, claim.lease_token, after, LEASE_SECONDS]);
    return result;
  });
}

export async function finishChannelScan(options, claim, after = claim.ids.at(-1) ?? claim.after_channel_id) {
  if (after !== claim.after_channel_id && !claim.ids.includes(after)) throw new Error('SCAN_CURSOR_OUTSIDE_PAGE');
  const pageDone = !claim.ids.length || after === claim.ids.at(-1);
  const wrapped = pageDone && (claim.ids.length < claim.pageSize || after === claim.upper_channel_id);
  const result = await scanTransaction(options, claim, async client => {
    await client.query(`UPDATE crawler.background_reconciliation_scans SET after_channel_id=$3,
      upper_channel_id=CASE WHEN $4 THEN NULL ELSE upper_channel_id END,
      completed_rounds=completed_rounds+CASE WHEN $4 THEN 1 ELSE 0 END,
      last_completed_at=CASE WHEN $4 THEN clock_timestamp() ELSE last_completed_at END
      WHERE scope=$1 AND lease_token=$2`, [claim.scope, claim.lease_token, wrapped ? '' : after, wrapped]);
    return { wrapped, after_channel_id: wrapped ? '' : after };
  });
  await releaseChannelScan(options, claim);
  return result;
}

export async function releaseChannelScan({ withTransaction }, claim) {
  await withTransaction(client => client.query(`UPDATE crawler.background_reconciliation_scans
    SET lease_token=NULL,lease_until=NULL WHERE scope=$1 AND lease_token=$2`, [claim.scope, claim.lease_token]));
}
