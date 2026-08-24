import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { refreshVideoPublicationItemHashes } from "./videoPublicationItemStore.js";

export const DISABLED_COMMENT_COUNT_REPAIR_VERSION = "disabled-comment-count-zero-v1";

function canonicalJson(value) {
  if (value === null || typeof value !== "object") return value;
  if (Array.isArray(value)) return value.map(canonicalJson);
  return Object.fromEntries(
    Object.keys(value).sort().map((key) => [key, canonicalJson(value[key])]),
  );
}

function sha256(value) {
  return `sha256:${createHash("sha256").update(value).digest("hex")}`;
}

function safeCount(value, field) {
  const parsed = Number(value);
  assert.ok(Number.isSafeInteger(parsed) && parsed >= 0, `${field} must be a safe count`);
  return parsed;
}

function text(value) {
  const output = String(value ?? "").trim();
  return output || null;
}

function normalizedTarget(row) {
  return {
    content_key: text(row.content_key),
    channel_id: text(row.channel_id),
    source_content_id: text(row.source_content_id),
    comment_count: row.comment_count == null ? null : String(row.comment_count),
    comment_count_status: text(row.comment_count_status),
    comment_count_source: text(row.comment_count_source),
    publication_item_hash: text(row.publication_item_hash),
  };
}

export function disabledCommentCountRepairEvidenceHash(evidenceValue) {
  const { evidence_hash: ignored, ...evidence } = evidenceValue;
  return sha256(JSON.stringify(canonicalJson(evidence)));
}

export function disabledCommentCountRepairConfirmation(evidence) {
  return [
    "repair-disabled-comment-counts",
    evidence.database_name,
    evidence.target_count,
    evidence.evidence_hash,
  ].join(":");
}

export async function inspectDisabledCommentCountRepair(client, {
  operator = null,
  reason = null,
} = {}) {
  const state = (await client.query(
    `SELECT current_database() AS database_name,
            (SELECT count(*)::bigint FROM crawler.channels) AS channel_count,
            (SELECT count(*)::bigint FROM crawler.contents) AS content_count,
            constraint_state.convalidated AS constraint_validated
     FROM pg_constraint constraint_state
     WHERE constraint_state.conrelid='crawler.contents'::regclass
       AND constraint_state.conname='contents_comment_state_shape'`,
  )).rows[0];
  if (!state) throw new Error("contents_comment_state_shape is not installed");

  const targets = (await client.query(
    `SELECT content_key,channel_id,source_content_id,comment_count,
            comment_count_status,comment_count_source,publication_item_hash
     FROM crawler.contents
     WHERE comments_disabled IS TRUE
       AND (
         comment_count IS DISTINCT FROM 0
         OR comment_count_status IS DISTINCT FROM 'disabled'
       )
     ORDER BY channel_id,content_key`,
  )).rows.map(normalizedTarget);
  const blockers = (await client.query(
    `SELECT count(*)::bigint AS blocker_count
     FROM crawler.contents
     WHERE NOT (
       (comments_disabled IS TRUE
         AND comment_count=0
         AND comment_count_status='disabled')
       OR (comments_disabled IS DISTINCT FROM TRUE
         AND comment_count_status<>'disabled')
     )
       AND NOT (
         comments_disabled IS TRUE
         AND (
           comment_count IS DISTINCT FROM 0
           OR comment_count_status IS DISTINCT FROM 'disabled'
         )
       )`,
  )).rows[0] ?? {};
  const channelIds = [...new Set(targets.map((row) => row.channel_id))];
  const evidence = {
    evidence_version: DISABLED_COMMENT_COUNT_REPAIR_VERSION,
    operator: text(operator),
    reason: text(reason),
    database_name: text(state.database_name),
    channel_count: safeCount(state.channel_count, "channel_count"),
    content_count: safeCount(state.content_count, "content_count"),
    constraint_validated: state.constraint_validated === true,
    target_count: targets.length,
    target_channel_count: channelIds.length,
    missing_source_count: targets.filter((row) => row.comment_count_source == null).length,
    blocker_count: safeCount(blockers.blocker_count, "blocker_count"),
    affected_channel_ids: channelIds,
    targets,
  };
  return { ...evidence, evidence_hash: disabledCommentCountRepairEvidenceHash(evidence) };
}

export async function applyDisabledCommentCountRepair(client, {
  expectedEvidenceHash,
  expectedTargetCount,
  operator,
  reason,
} = {}) {
  await client.query("SELECT pg_advisory_xact_lock(hashtext($1))", [
    DISABLED_COMMENT_COUNT_REPAIR_VERSION,
  ]);
  assert.ok(text(operator), "repair operator is required");
  assert.ok(text(reason), "repair reason is required");
  const before = await inspectDisabledCommentCountRepair(client, { operator, reason });
  assert.equal(before.evidence_hash, expectedEvidenceHash, "repair evidence changed after plan");
  assert.equal(before.target_count, expectedTargetCount, "repair target count changed after plan");
  assert.equal(before.blocker_count, 0, "ambiguous comment-state rows require separate review");

  const contentKeys = before.targets.map((row) => row.content_key);
  const updated = contentKeys.length === 0
    ? { rows: [], rowCount: 0 }
    : await client.query(
        `UPDATE crawler.contents
         SET comment_count=0,
             comment_count_status='disabled',
             comment_count_source=COALESCE(
               NULLIF(btrim(comment_count_source),''),
               'stored_comments_disabled_evidence'
             ),
             raw_json=COALESCE(raw_json,'{}'::jsonb)
               || jsonb_build_object(
                    'disabled_comment_count_repair',
                    jsonb_build_object(
                      'version',$2::text,
                      'repaired_at',transaction_timestamp(),
                      'previous_comment_count',comment_count,
                      'previous_comment_count_status',comment_count_status,
                      'previous_comment_count_source',comment_count_source,
                      'operator',$3::text,
                      'reason',$4::text
                    )
                  )
         WHERE content_key=ANY($1::text[])
           AND comments_disabled IS TRUE
           AND (
             comment_count IS DISTINCT FROM 0
             OR comment_count_status IS DISTINCT FROM 'disabled'
           )
         RETURNING content_key,channel_id`,
        [contentKeys, DISABLED_COMMENT_COUNT_REPAIR_VERSION, text(operator), text(reason)],
      );
  assert.equal(updated.rowCount, before.target_count, "not every planned row was repaired");

  const hashes = await refreshVideoPublicationItemHashes(client, contentKeys);
  await client.query(
    `ALTER TABLE crawler.contents
     VALIDATE CONSTRAINT contents_comment_state_shape`,
  );
  const after = await inspectDisabledCommentCountRepair(client, { operator, reason });
  assert.equal(after.target_count, 0, "disabled comment-count repair left target rows behind");
  assert.equal(after.blocker_count, 0, "comment-state blockers remain after repair");
  assert.equal(after.constraint_validated, true, "comment-state constraint was not validated");

  return {
    repair_version: DISABLED_COMMENT_COUNT_REPAIR_VERSION,
    repaired_count: updated.rowCount,
    affected_channel_ids: before.affected_channel_ids,
    publication_hashes: {
      requested_count: hashes.requested_count,
      found_count: hashes.found_count,
      ready_count: hashes.ready_count,
      incomplete_count: hashes.incomplete_count,
      changed_count: hashes.changed_count,
    },
    before,
    after,
  };
}
