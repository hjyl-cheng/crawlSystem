import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { reconcilePublication } from "./publicationReconciler.js";

export const ACTIVE_LIVE_CONTENT_REPAIR_VERSION = "active-live-policy-removal-v1";
export const ACTIVE_LIVE_CONTENT_REPAIR_NEVER_RETRY_AT = "9999-12-31T23:59:59.999Z";
export const ACTIVE_LIVE_CONTENT_REPAIR_TARGETS = Object.freeze([
  Object.freeze({ channel_id: "UCLcE8Gv364C8KhHaNU8Uuqg", content_id: "VbelbdhEsyc" }),
  Object.freeze({ channel_id: "UCU3rOxhYFgsyyI84jJ1hWdg", content_id: "31Zx91GmgQ4" }),
  Object.freeze({ channel_id: "UCU3rOxhYFgsyyI84jJ1hWdg", content_id: "5cYXGzbOMKI" }),
  Object.freeze({ channel_id: "UCU3rOxhYFgsyyI84jJ1hWdg", content_id: "FjSOEf1KQqE" }),
  Object.freeze({ channel_id: "UCU3rOxhYFgsyyI84jJ1hWdg", content_id: "Qa_StRP6d70" }),
  Object.freeze({ channel_id: "UCU3rOxhYFgsyyI84jJ1hWdg", content_id: "jzgCMq8QD08" }),
  Object.freeze({ channel_id: "UCU3rOxhYFgsyyI84jJ1hWdg", content_id: "pXmoL9b6PDU" }),
  Object.freeze({ channel_id: "UCZhzBiaNLLpPR-6bgQkGYTw", content_id: "czICsuetl3s" }),
  Object.freeze({ channel_id: "UCtjBQydUJkwodtxwSYthRlw", content_id: "YPtD8wFmRsY" }),
]);

function object(value) {
  return value && typeof value === "object" && !Array.isArray(value) ? value : {};
}

function text(value) {
  const output = String(value ?? "").trim();
  return output || null;
}

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

function targetKey(value) {
  return `${text(value?.channel_id) ?? ""}\u0000${text(value?.content_id) ?? ""}`;
}

function compareText(left, right) {
  return Buffer.compare(
    Buffer.from(String(left ?? ""), "utf8"),
    Buffer.from(String(right ?? ""), "utf8"),
  );
}

function normalizedTargets(value) {
  assert.ok(Array.isArray(value) && value.length > 0, "repair targets must be a non-empty array");
  const targets = value.map((item) => ({
    channel_id: text(item?.channel_id),
    content_id: text(item?.content_id),
  }));
  for (const target of targets) {
    assert.ok(target.channel_id, "repair target channel_id is required");
    assert.ok(target.content_id, "repair target content_id is required");
  }
  const keys = targets.map(targetKey);
  assert.equal(new Set(keys).size, keys.length, "repair targets must be unique");
  return targets.sort((left, right) => (
    compareText(left.channel_id, right.channel_id)
    || compareText(left.content_id, right.content_id)
  ));
}

function booleanText(value) {
  const normalized = text(value)?.toLowerCase();
  if (normalized === "true") return true;
  if (normalized === "false") return false;
  return null;
}

function detailEvidence(row) {
  const detail = object(row.detail_json);
  return {
    live_status: text(row.detail_live_status),
    is_live: booleanText(row.detail_is_live),
    was_live: booleanText(row.detail_was_live),
    detail_hash: sha256(JSON.stringify(canonicalJson(detail))),
  };
}

function normalizedContent(row) {
  return {
    channel_id: text(row.channel_id),
    content_id: text(row.source_content_id),
    content_key: text(row.content_key),
    content_type: text(row.content_type),
    publication_item_hash: text(row.publication_item_hash),
  };
}

function normalizedCandidate(row) {
  return {
    candidate_id: text(row.candidate_id),
    run_id: text(row.run_id),
    channel_id: text(row.channel_id),
    content_id: text(row.source_content_id),
    content_key: text(row.content_key),
    content_type: text(row.content_type),
    detail_status: text(row.detail_status),
    api_status: text(row.api_status),
    disposition: text(row.disposition),
    evidence: detailEvidence(row),
  };
}

function normalizedCurrent(row) {
  return {
    publication_stream_id: text(row.publication_stream_id),
    channel_id: text(row.channel_id),
    readiness_status: text(row.readiness_status),
    data_sequence: Number(row.data_sequence),
    current_revision_id: text(row.current_revision_id),
    online_delivery_count: Number(row.online_delivery_count ?? 0),
    target_items: Array.isArray(row.target_items)
      ? row.target_items.map((item) => ({
          content_id: text(item?.content_id),
          item_hash: text(item?.item_hash),
        })).sort((left, right) => compareText(left.content_id, right.content_id))
      : [],
  };
}

function blocker(code, target = null, details = {}) {
  return {
    code,
    ...(target ? { channel_id: target.channel_id, content_id: target.content_id } : {}),
    ...details,
  };
}

function sortedBlockers(value) {
  return value.sort((left, right) => (
    compareText(left.code, right.code)
    || compareText(left.channel_id, right.channel_id)
    || compareText(left.content_id, right.content_id)
  ));
}

function buildEvidence({
  identity,
  targets,
  contents,
  candidates,
  currents,
  enrichTasks,
  operator,
  reason,
}) {
  const expectedKeys = new Set(targets.map(targetKey));
  const blockers = [];
  for (const row of contents) {
    if (!expectedKeys.has(targetKey({ channel_id: row.channel_id, content_id: row.content_id }))) {
      blockers.push(blocker("unexpected_content_mapping", null, {
        channel_id: row.channel_id,
        content_id: row.content_id,
      }));
    }
  }
  for (const row of candidates) {
    if (!expectedKeys.has(targetKey({ channel_id: row.channel_id, content_id: row.content_id }))) {
      blockers.push(blocker("unexpected_candidate_mapping", null, {
        channel_id: row.channel_id,
        content_id: row.content_id,
      }));
    }
  }

  const targetEvidence = targets.map((target) => {
    const matchingContents = contents.filter((row) => targetKey({
      channel_id: row.channel_id,
      content_id: row.content_id,
    }) === targetKey(target));
    const matchingCandidates = candidates.filter((row) => targetKey({
      channel_id: row.channel_id,
      content_id: row.content_id,
    }) === targetKey(target));
    const matchingCurrents = currents.filter((row) => row.channel_id === target.channel_id);
    const current = matchingCurrents.length === 1 ? matchingCurrents[0] : null;
    const currentItems = current?.target_items.filter((item) => item.content_id === target.content_id) ?? [];
    const content = matchingContents.length === 1 ? matchingContents[0] : null;
    const candidate = matchingCandidates.length === 1 ? matchingCandidates[0] : null;
    const currentItem = currentItems.length === 1 ? currentItems[0] : null;

    if (matchingContents.length !== 1) {
      blockers.push(blocker("target_content_count_mismatch", target, {
        actual_count: matchingContents.length,
      }));
    }
    if (matchingCandidates.length !== 1) {
      blockers.push(blocker("target_candidate_count_mismatch", target, {
        actual_count: matchingCandidates.length,
      }));
    }
    if (!current || currentItems.length !== 1) {
      blockers.push(blocker("target_publication_current_count_mismatch", target, {
        actual_count: currentItems.length,
      }));
    }
    if (content && content.content_type !== "live") {
      blockers.push(blocker("target_content_type_not_live", target, {
        actual: content.content_type,
      }));
    }
    if (candidate && (
      candidate.disposition !== "stored"
      || candidate.detail_status !== "done"
      || candidate.api_status !== "done"
      || candidate.content_type !== "live"
      || candidate.content_key !== content?.content_key
    )) {
      blockers.push(blocker("target_candidate_not_stored", target, {
        disposition: candidate.disposition,
        detail_status: candidate.detail_status,
        api_status: candidate.api_status,
        content_type: candidate.content_type,
        content_key_matches: candidate.content_key === content?.content_key,
      }));
    }
    if (candidate && (
      candidate.evidence.live_status !== "is_live"
      || candidate.evidence.is_live !== true
      || candidate.evidence.was_live !== false
    )) {
      blockers.push(blocker("target_active_live_evidence_mismatch", target, {
        live_status: candidate.evidence.live_status,
        is_live: candidate.evidence.is_live,
        was_live: candidate.evidence.was_live,
      }));
    }
    if (current && current.readiness_status !== "ready") {
      blockers.push(blocker("target_publication_current_not_ready", target, {
        readiness_status: current.readiness_status,
      }));
    }
    if (current && current.online_delivery_count < 1) {
      blockers.push(blocker("target_publication_has_no_online_delivery", target));
    }
    if (content && currentItem && content.publication_item_hash !== currentItem.item_hash) {
      blockers.push(blocker("target_publication_item_hash_mismatch", target, {
        crawler_hash: content.publication_item_hash,
        current_hash: currentItem.item_hash,
      }));
    }

    return {
      ...target,
      content,
      candidate,
      publication_current: current ? {
        publication_stream_id: current.publication_stream_id,
        data_sequence: current.data_sequence,
        current_revision_id: current.current_revision_id,
        item: currentItem,
      } : null,
    };
  });

  for (const channelId of [...new Set(targets.map((target) => target.channel_id))]) {
    const currentCount = currents.filter((row) => row.channel_id === channelId).length;
    if (currentCount !== 1) {
      blockers.push(blocker("target_channel_video_current_count_mismatch", null, {
        channel_id: channelId,
        actual_count: currentCount,
      }));
    }
  }
  for (const task of enrichTasks) {
    if (["queued", "running"].includes(text(task.status))) {
      blockers.push(blocker("target_content_enrich_task_active", null, {
        content_key: text(task.content_key),
        task_id: text(task.task_id),
        status: text(task.status),
      }));
    }
  }

  return {
    evidence_version: ACTIVE_LIVE_CONTENT_REPAIR_VERSION,
    operator: text(operator),
    reason: text(reason),
    database_name: text(identity.database_name),
    channel_count: Number(identity.channel_count),
    content_count: Number(identity.content_count),
    target_count: targets.length,
    target_channel_count: new Set(targets.map((target) => target.channel_id)).size,
    matched_content_count: contents.filter((row) => expectedKeys.has(targetKey({
      channel_id: row.channel_id,
      content_id: row.content_id,
    }))).length,
    matched_candidate_count: candidates.filter((row) => expectedKeys.has(targetKey({
      channel_id: row.channel_id,
      content_id: row.content_id,
    }))).length,
    matched_publication_count: targetEvidence.filter(
      (target) => target.publication_current?.item,
    ).length,
    active_enrich_task_count: enrichTasks.filter(
      (task) => ["queued", "running"].includes(text(task.status)),
    ).length,
    blocker_count: blockers.length,
    blockers: sortedBlockers(blockers),
    targets: targetEvidence,
  };
}

export function activeLiveContentRepairEvidenceHash(evidenceValue) {
  const { evidence_hash: ignored, ...evidence } = evidenceValue;
  return sha256(JSON.stringify(canonicalJson(evidence)));
}

export function activeLiveContentRepairConfirmation(evidence) {
  return [
    "repair-active-live-content",
    evidence.database_name,
    evidence.target_count,
    evidence.target_channel_count,
    evidence.evidence_hash,
  ].join(":");
}

export async function inspectActiveLiveContentRepair(client, {
  operator = null,
  reason = null,
  targets: targetValue = ACTIVE_LIVE_CONTENT_REPAIR_TARGETS,
} = {}) {
  const targets = normalizedTargets(targetValue);
  const contentIds = targets.map((target) => target.content_id);
  const channelIds = [...new Set(targets.map((target) => target.channel_id))].sort(compareText);
  const identity = (await client.query(
    `SELECT current_database() AS database_name,
            (SELECT count(*)::bigint FROM crawler.channels) AS channel_count,
            (SELECT count(*)::bigint FROM crawler.contents) AS content_count`,
  )).rows[0];
  assert.ok(identity, "Crawler database identity query returned no row");

  const contents = (await client.query(
    `SELECT channel_id,source_content_id,content_key,content_type,publication_item_hash
     FROM crawler.contents
     WHERE source_content_id=ANY($1::text[])
     ORDER BY channel_id,source_content_id,content_key`,
    [contentIds],
  )).rows.map(normalizedContent);
  const candidates = (await client.query(
    `SELECT candidate_id,run_id,channel_id,source_content_id,content_key,content_type,
            detail_status,api_status,disposition,result_json->'detail' AS detail_json,
            result_json#>>'{detail,live_status}' AS detail_live_status,
            result_json#>>'{detail,is_live}' AS detail_is_live,
            result_json#>>'{detail,was_live}' AS detail_was_live
     FROM crawler.content_candidates
     WHERE source_content_id=ANY($1::text[])
     ORDER BY channel_id,source_content_id,candidate_id`,
    [contentIds],
  )).rows.map(normalizedCandidate);
  const currents = (await client.query(
    `SELECT current.publication_stream_id,current.channel_id,current.readiness_status,
            current.data_sequence,current.current_revision_id,
            (
              SELECT count(*)::int
              FROM publication.channel_delivery_state delivery
              WHERE delivery.publication_stream_id=current.publication_stream_id
                AND delivery.channel_id=current.channel_id
                AND delivery.mode='online'
            ) AS online_delivery_count,
            (
              SELECT COALESCE(
                jsonb_agg(
                jsonb_build_object(
                  'content_id',item.value->>'content_id',
                  'item_hash',item.value->>'item_hash'
                ) ORDER BY item.value->>'content_id'
                ),
                '[]'::jsonb
              )
              FROM jsonb_array_elements(
                COALESCE(current.payload_json->'items','[]'::jsonb)
              ) AS item(value)
              WHERE item.value->>'content_id'=ANY($2::text[])
            ) AS target_items
     FROM publication.domain_current current
     JOIN publication.channel_stream_state owner
       ON owner.publication_stream_id=current.publication_stream_id
      AND owner.channel_id=current.channel_id
      AND owner.status='owned'
     JOIN publication.stream stream
       ON stream.publication_stream_id=owner.publication_stream_id
      AND stream.status='active'
      AND stream.capture_enabled_at IS NOT NULL
     WHERE current.domain='video' AND current.channel_id=ANY($1::text[])
     ORDER BY current.channel_id,current.publication_stream_id`,
    [channelIds, contentIds],
  )).rows.map(normalizedCurrent);
  const contentKeys = contents.map((row) => row.content_key);
  const enrichTasks = contentKeys.length === 0 ? [] : (await client.query(
    `SELECT task_id,content_key,status
     FROM crawler.content_enrich_tasks
     WHERE content_key=ANY($1::text[])
     ORDER BY content_key,task_id`,
    [contentKeys],
  )).rows;

  const evidence = buildEvidence({
    identity,
    targets,
    contents,
    candidates,
    currents,
    enrichTasks,
    operator,
    reason,
  });
  return {
    ...evidence,
    evidence_hash: activeLiveContentRepairEvidenceHash(evidence),
  };
}

function expectedRetractions(targets) {
  return targets.map((target) => ({
    content_id: target.content_id,
    reason: "policy_removed",
  })).sort((left, right) => compareText(left.content_id, right.content_id));
}

async function inspectPostState(client, { targets, revisionIds }) {
  const contentIds = targets.map((target) => target.content_id);
  const stored = (await client.query(
    `SELECT
       (SELECT count(*)::int FROM crawler.contents
         WHERE source_content_id=ANY($1::text[])) AS remaining_contents,
       (SELECT count(*)::int FROM crawler.content_candidates
         WHERE source_content_id=ANY($1::text[])
           AND content_key IS NULL
           AND detail_status='done'
           AND api_status='not_needed'
           AND disposition='terminal_excluded'
           AND result_json#>>'{scope,reason}'='live_in_progress'
           AND result_json#>>'{active_live_content_repair,version}'=$2) AS excluded_candidates,
       (SELECT count(*)::int
        FROM publication.domain_current current
        CROSS JOIN LATERAL jsonb_array_elements(
          COALESCE(current.payload_json->'items','[]'::jsonb)
        ) AS item(value)
        WHERE current.domain='video'
          AND item.value->>'content_id'=ANY($1::text[])) AS current_targets,
       (SELECT count(*)::int FROM publication.revision
         WHERE revision_id=ANY($3::uuid[])
           AND revision_type='repair'
           AND domain='video') AS repair_revisions,
       (SELECT count(*)::int FROM publication.outbox
         WHERE revision_id=ANY($3::uuid[])) AS publication_outbox_rows`,
    [contentIds, ACTIVE_LIVE_CONTENT_REPAIR_VERSION, revisionIds],
  )).rows[0];
  return {
    remaining_contents: Number(stored.remaining_contents),
    excluded_candidates: Number(stored.excluded_candidates),
    current_targets: Number(stored.current_targets),
    repair_revisions: Number(stored.repair_revisions),
    publication_outbox_rows: Number(stored.publication_outbox_rows),
  };
}

export async function applyActiveLiveContentRepair(client, {
  expectedEvidenceHash,
  expectedTargetCount,
  operator,
  reason,
  targets: targetValue = ACTIVE_LIVE_CONTENT_REPAIR_TARGETS,
} = {}) {
  const targets = normalizedTargets(targetValue);
  assert.ok(text(operator), "repair operator is required");
  assert.ok(text(reason), "repair reason is required");
  await client.query("SELECT pg_advisory_xact_lock(hashtext($1))", [
    ACTIVE_LIVE_CONTENT_REPAIR_VERSION,
  ]);
  const before = await inspectActiveLiveContentRepair(client, {
    operator,
    reason,
    targets,
  });
  assert.equal(before.evidence_hash, expectedEvidenceHash, "repair evidence changed after plan");
  assert.equal(before.target_count, expectedTargetCount, "repair target count changed after plan");
  assert.equal(before.blocker_count, 0, "active-Live repair has unresolved blockers");

  const candidateIds = before.targets.map((target) => target.candidate.candidate_id);
  const contentKeys = before.targets.map((target) => target.content.content_key);
  const updated = await client.query(
    `UPDATE crawler.content_candidates candidate
     SET content_key=NULL,
         content_type='live',
         type_status='resolved',
         detail_status='done',
         api_status='not_needed',
         missing_fields='{}'::text[],
         disposition='terminal_excluded',
         next_attempt_at=$2::timestamptz,
         result_json=COALESCE(candidate.result_json,'{}'::jsonb)
           || jsonb_build_object(
                'scope',COALESCE(candidate.result_json->'scope','{}'::jsonb)
                  || jsonb_build_object(
                       'status','excluded',
                       'reason','live_in_progress',
                       'source',$3::text
                     ),
                'disposition',jsonb_build_object(
                  'version','video-disposition-v1',
                  'kind','terminal_excluded',
                  'reason_code','live_in_progress',
                  'retry_class','incremental_rediscovery',
                  'retryable',false,
                  'observed_at',transaction_timestamp(),
                  'next_attempt_at',$2::timestamptz
                ),
                'active_live_content_repair',jsonb_build_object(
                  'version',$3::text,
                  'repaired_at',transaction_timestamp(),
                  'operator',$4::text,
                  'reason',$5::text,
                  'previous_disposition',candidate.disposition,
                  'previous_content_key',candidate.content_key
                )
              ),
         error_message=NULL,
         finished_at=COALESCE(candidate.finished_at,transaction_timestamp()),
         updated_at=transaction_timestamp()
     WHERE candidate.candidate_id=ANY($1::bigint[])
       AND candidate.disposition='stored'
       AND candidate.content_key=ANY($6::text[])
     RETURNING candidate_id`,
    [
      candidateIds,
      ACTIVE_LIVE_CONTENT_REPAIR_NEVER_RETRY_AT,
      ACTIVE_LIVE_CONTENT_REPAIR_VERSION,
      text(operator),
      text(reason),
      contentKeys,
    ],
  );
  assert.equal(updated.rowCount, targets.length, "not every planned Candidate was excluded");

  const deleted = await client.query(
    `DELETE FROM crawler.contents
     WHERE content_key=ANY($1::text[])
     RETURNING content_key`,
    [contentKeys],
  );
  assert.equal(deleted.rowCount, targets.length, "not every planned Content row was deleted");

  const transactionTime = (await client.query(
    "SELECT transaction_timestamp() AS transaction_timestamp",
  )).rows[0]?.transaction_timestamp;
  assert.ok(transactionTime, "transaction timestamp is unavailable");
  const asOf = new Date(transactionTime).toISOString();
  const targetsByChannel = new Map();
  for (const target of targets) {
    const values = targetsByChannel.get(target.channel_id) ?? [];
    values.push(target);
    targetsByChannel.set(target.channel_id, values);
  }

  const reconciliations = [];
  for (const [channelId, channelTargets] of [...targetsByChannel.entries()].sort((left, right) => (
    compareText(left[0], right[0])
  ))) {
    const contentIds = channelTargets.map((target) => target.content_id).sort(compareText);
    const reconciliation = await reconcilePublication(client, {
      channelId,
      domains: ["video"],
      asOf,
      revisionType: "repair",
      policyRemovalContentIds: contentIds,
    });
    assert.equal(
      reconciliation.status,
      "revised",
      `Video Repair was not created for ${channelId}: ${JSON.stringify(reconciliation.domains)}`,
    );
    assert.equal(
      reconciliation.revisions.length,
      1,
      `expected one Video Repair for ${channelId}`,
    );
    const revision = reconciliation.revisions[0];
    assert.equal(revision.revision_type, "repair");
    assert.equal(revision.operation, "apply_window_delta");
    assert.deepEqual(revision.payload.upserts, [], `unexpected Video upserts for ${channelId}`);
    assert.deepEqual(revision.payload.window_exits, [], `unexpected Window Exits for ${channelId}`);
    assert.deepEqual(
      [...revision.payload.retractions].sort((left, right) => (
        compareText(left.content_id, right.content_id)
      )),
      expectedRetractions(channelTargets),
      `unexpected Video retractions for ${channelId}`,
    );
    reconciliations.push(reconciliation);
  }

  const revisionIds = reconciliations.flatMap((item) => (
    item.revisions.map((revision) => revision.revision_id)
  ));
  const after = await inspectPostState(client, { targets, revisionIds });
  assert.equal(after.remaining_contents, 0, "active-Live Content rows remain after repair");
  assert.equal(after.excluded_candidates, targets.length, "active-Live Candidates were not excluded");
  assert.equal(after.current_targets, 0, "active-Live items remain in Publication Current");
  assert.equal(after.repair_revisions, targetsByChannel.size, "Video Repair Revision count mismatch");
  assert.ok(
    after.publication_outbox_rows >= targetsByChannel.size,
    "Video Repair Revisions did not create Publication Outbox rows",
  );

  return {
    repair_version: ACTIVE_LIVE_CONTENT_REPAIR_VERSION,
    repaired_content_count: deleted.rowCount,
    excluded_candidate_count: updated.rowCount,
    repaired_channel_count: targetsByChannel.size,
    retry_policy: {
      old_candidate_network_retry: false,
      replay_discovery: "future_incremental_run",
      next_attempt_at: ACTIVE_LIVE_CONTENT_REPAIR_NEVER_RETRY_AT,
    },
    revision_ids: revisionIds,
    reconciliations,
    before,
    after,
  };
}
