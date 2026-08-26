import assert from "node:assert/strict";
import test from "node:test";
import {
  applyIncrementalVideoDetail,
  executeIncrementalVideo as executeIncrementalVideoWithSystemClock,
  fetchIncrementalVideoDetail,
} from "../src/incrementalVideo.js";
import { runWithChannelExecution } from "../src/channelExecutionContext.js";

function executeIncrementalVideo(options) {
  return executeIncrementalVideoWithSystemClock({
    ...options,
    now: () => new Date(options.startedAt),
  });
}

function plan() {
  return {
    job_id: "incremental__UCvideo__20260720__clock_7__hash",
    plan_id: "4c6ff5ea-b6d9-53f9-b8cf-43cfe430298d",
    plan_day: "2026-07-20",
    scheduled_at: "2026-07-20T13:25:40.000Z",
    channel_id: "UCvideo",
    capacity: { factor: 1, player_cap: 20, next_cap: 8, version: "capacity-1" },
    planner_config_version: "video-plan-1",
  };
}

function detail(videoId, viewCount, { contentType = "video" } = {}) {
  const isShort = contentType === "short";
  const isLive = contentType === "live";
  return {
    id: videoId,
    title: `Title ${videoId}`,
    published_at: "2026-07-19T00:00:00.000Z",
    published_at_precision: "second",
    view_count: viewCount,
    view_count_text: String(viewCount),
    duration_seconds: 90,
    comment_count: 2,
    comment_count_status: "exact",
    comment_count_source: "youtubejs_comments",
    comments_disabled: null,
    comments_first_page: {
      version: 1,
      collected_at: "2026-07-20T00:00:00.000Z",
      sort: "TOP_COMMENTS",
      total_count: 2,
      returned_count: 1,
      comments: [{ comment_id: `comment-${videoId}`, text: `Comment ${videoId}` }],
    },
    availability: "public",
    access_status: "public",
    content_type_signals: {
      source: "youtubei_player",
      canonical_url: isShort
        ? `https://www.youtube.com/shorts/${videoId}`
        : `https://www.youtube.com/watch?v=${videoId}`,
      is_shorts_eligible: isShort,
      is_live_content: isLive,
      is_live: false,
      is_upcoming: false,
      is_live_now: false,
    },
    extractor_version: "youtubei.js@test",
  };
}

function databaseFixture({
  beforeTransaction = null,
  beforeEnrichTaskLock = null,
  enrichMode = "clock",
  failPublication = false,
} = {}) {
  let transactionCount = 0;
  const state = {
    candidates: new Set(["candidate-only"]),
    candidateRows: [],
    contents: [
      {
        content_key: "UCvideo:video:known-anchor",
        channel_id: "UCvideo",
        source_content_id: "known-anchor",
        content_type: "video",
        published_at: "2026-07-10T00:00:00.000Z",
        last_seen_at: "2026-07-20T00:00:00.000Z",
        view_count: 200,
        player_last_observed_at: "2026-07-20T00:00:00.000Z",
        next_last_observed_at: null,
        video_change_probability: 0,
        like_count: null,
        comment_count: null,
      },
      {
        content_key: "UCvideo:video:old-video",
        channel_id: "UCvideo",
        source_content_id: "old-video",
        content_type: "video",
        published_at: "2026-07-09T00:00:00.000Z",
        last_seen_at: "2026-07-20T00:00:00.000Z",
        view_count: 90,
        player_last_observed_at: null,
        next_last_observed_at: null,
        like_count: null,
        comment_count: null,
      },
    ],
    sql: [],
    outbox: [],
    observationSummaries: [],
    extractorVersions: [],
    cursorKind: null,
    cursorUpdates: [],
    cursorAnchors: ["known-anchor", "old-video"],
    enrichPending: new Set(),
    enrichLeased: new Set(),
    enrichTerminalSchedule: new Map(),
    enrichTaskStates: new Map(),
    enrichMode,
    channelStatus: "active",
  };

  const client = {
    async query(sql, params = []) {
      state.sql.push(sql);
      if (failPublication && sql.includes("publication-reconciler:find-owner")) {
        throw new Error("simulated Publication failure");
      }
      if (sql.includes("setting_key='content_enrich_dispatch'")) {
        return { rowCount: 1, rows: [{ mode: state.enrichMode }] };
      }
      if (sql.includes("INSERT INTO crawler.crawl_observation_keys")) {
        return { rowCount: 1, rows: [{ observation_id: params[1] }] };
      }
      if (sql.includes("INSERT INTO crawler.channel_domain_cursors")) {
        state.cursorKind = params[1];
        return { rowCount: 1, rows: [] };
      }
      if (sql.includes("SELECT * FROM crawler.channel_domain_cursors")) {
        return { rowCount: 1, rows: [{ latest_sequence: 0 }] };
      }
      if (sql.includes("SELECT status,reject_reason,dormant_reason")) {
        return {
          rowCount: 1,
          rows: [{
            status: state.channelStatus,
            reject_reason: null,
            dormant_reason: null,
            dormant_since: null,
            dormant_recheck_day: null,
            dormant_last_probe_at: null,
            dormant_cycle: 0,
          }],
        };
      }
      if (sql.includes("count(DISTINCT source_content_id)::int")) {
        const recent = state.contents.filter((row) => row.published_at != null).length;
        return { rowCount: 1, rows: [{ recent_published_content_count: recent }] };
      }
      if (sql.includes("UPDATE crawler.channels") && sql.includes("SET status='active'")) {
        state.channelStatus = "active";
        return { rowCount: 1, rows: [] };
      }
      if (sql.includes("UPDATE crawler.channels") && sql.includes("SET status='dormant'")) {
        state.channelStatus = "dormant";
        return { rowCount: 1, rows: [] };
      }
      if (sql.includes("UPDATE crawler.channel_domain_cursors")) {
        state.cursorUpdates.push({
          outcome: params[4],
          anchorVideoIds: params[6],
          sourceCursor: params[7],
        });
        if (Array.isArray(params[6])) state.cursorAnchors = [...params[6]];
        return { rowCount: 1, rows: [] };
      }
      if (sql.includes("FROM crawler.contents") && sql.includes("source_content_id=ANY")) {
        const known = new Set(state.contents.map((row) => row.source_content_id));
        const rows = params[1].filter((id) => known.has(id)).map((video_id) => ({ video_id }));
        return { rowCount: rows.length, rows };
      }
      if (sql.includes("UPDATE crawler.contents content") && sql.includes("jsonb_to_recordset")) {
        for (const input of JSON.parse(params[2])) {
          const row = state.contents.find((item) => item.source_content_id === input.video_id);
          if (row && input.content_type) row.content_type = input.content_type;
          if (row && row.published_at == null && input.published_at) {
            row.published_at = input.published_at;
          }
        }
        return { rowCount: 1, rows: [] };
      }
      if (sql.includes("SELECT task.*") && sql.includes("FROM crawler.content_enrich_tasks task")) {
        const contentKey = params[0];
        if (beforeEnrichTaskLock) await beforeEnrichTaskLock(state, contentKey);
        const recorded = state.enrichTaskStates.get(contentKey);
        if (recorded) return { rowCount: 1, rows: [{ ...recorded }] };
        if (state.enrichLeased.has(contentKey)) {
          return {
            rowCount: 1,
            rows: [{
              task_id: `player-refresh:${contentKey}`,
              status: "leased",
              attempts: 0,
              dispatch_generation: 1,
              lease_live: true,
              retry_due: true,
            }],
          };
        }
        if (state.enrichTerminalSchedule.has(contentKey)) {
          return {
            rowCount: 1,
            rows: [{
              task_id: `player-refresh:${contentKey}`,
              status: "terminal",
              attempts: 5,
              dispatch_generation: 1,
              lease_live: false,
              retry_due: state.enrichTerminalSchedule.get(contentKey) === "due",
            }],
          };
        }
        if (state.enrichPending.has(contentKey)) {
          return {
            rowCount: 1,
            rows: [{
              task_id: `player-refresh:${contentKey}`,
              status: "queued",
              attempts: 0,
              dispatch_generation: 1,
              lease_live: false,
              retry_due: true,
            }],
          };
        }
        return { rowCount: 0, rows: [] };
      }
      if (sql.includes("INSERT INTO crawler.contents")) {
        state.contents.push({
          content_key: params[0],
          channel_id: params[1],
          source_content_id: params[5],
          content_type: params[3],
          published_at: params[15],
          view_count: params[23],
          player_last_observed_at: params[39],
          next_last_observed_at: params[40] ? params[39] : null,
          like_count: params[27],
          comment_count: params[30],
          comment_count_status: params[31],
          comments_disabled: params[32],
          comments_first_page: params[43] == null ? null : JSON.parse(params[43]),
          access_status: params[35],
          last_enriched_at: params[40] ? params[39] : null,
        });
        return { rowCount: 1, rows: [] };
      }
      if (sql.includes("INSERT INTO crawler.content_candidates")) {
        const candidate = {
          run_id: params[0],
          channel_id: params[1],
          source_content_id: params[2],
          position: params[3],
          content_type: params[7],
          type_status: params[8],
          detail_status: params[10],
          api_status: params[11],
          missing_fields: params[12],
          disposition: params[14],
          next_attempt_at: params[15],
          result_json: JSON.parse(params[16]),
          error_message: params[17],
          first_seen_ledger_status: params[20],
          first_seen_ledger_observation_id: params[21],
        };
        const existing = state.candidateRows.find(
          (row) => row.run_id === candidate.run_id
            && row.source_content_id === candidate.source_content_id,
        );
        if (existing) {
          if (existing.disposition === "deferred"
              && ["stored", "terminal_excluded"].includes(candidate.disposition)) {
            const { disposition: fromDisposition, ...deferredEvidence } = existing.result_json;
            candidate.result_json.recovery = {
              from_disposition: fromDisposition,
              deferred_evidence: deferredEvidence,
              deferred_error_message: existing.error_message,
              resolved_at: candidate.result_json.disposition.observed_at,
            };
          }
          if (["pending", "consumed"].includes(existing.first_seen_ledger_status)) {
            candidate.first_seen_ledger_status = existing.first_seen_ledger_status;
            candidate.first_seen_ledger_observation_id = existing.first_seen_ledger_observation_id;
          }
          Object.assign(existing, candidate);
        }
        else {
          candidate.candidate_id = String(state.candidateRows.length + 1);
          state.candidateRows.push(candidate);
        }
        return { rowCount: 1, rows: [{ candidate_id: existing?.candidate_id ?? candidate.candidate_id }] };
      }
      if (sql.includes("UPDATE crawler.content_candidates candidate")
          && sql.includes("first_seen_ledger_status='consumed'")) {
        const requested = new Set(params[1].map(String));
        const claimed = state.candidateRows.filter((row) => (
          requested.has(String(row.candidate_id))
          && row.channel_id === params[0]
          && row.first_seen_ledger_status === "pending"
        ));
        for (const row of claimed) {
          row.first_seen_ledger_status = "consumed";
          row.first_seen_ledger_observation_id = params[2];
        }
        return {
          rowCount: claimed.length,
          rows: claimed.map((row) => ({ candidate_id: String(row.candidate_id) })),
        };
      }
      if (sql.includes("row_number() OVER") && sql.includes("ranked.disposition='deferred'")) {
        const latest = new Map();
        state.candidateRows.forEach((row, index) => {
          if (row.channel_id !== params[0]) return;
          latest.set(row.source_content_id, { ...row, candidate_id: index + 1 });
        });
        const rows = [...latest.values()]
          .filter((row) => row.disposition === "deferred")
          .filter((row) => !state.contents.some(
            (content) => content.channel_id === row.channel_id
              && content.source_content_id === row.source_content_id,
          ))
          .sort((left, right) => left.candidate_id - right.candidate_id)
          .map((row) => ({ video_id: row.source_content_id }));
        return { rowCount: rows.length, rows };
      }
      if (sql.includes("AS enrich_pending") && sql.includes("FROM crawler.contents content")) {
        const cutoff = new Date(`${params[1]}T00:00:00.000Z`);
        cutoff.setUTCDate(cutoff.getUTCDate() - Number(params[2]));
        const observedPublishedAt = new Map(
          JSON.parse(params[4] ?? "[]").map((item) => [item.video_id, item.published_at]),
        );
        const pendingBypassesWindow = sql.includes("candidate.enrich_pending");
        const clockOwnsEnrich = params[3] !== false;
        const protectsLiveLease = sql.includes("AS player_enrich_leased");
        const protectsTerminalWait = sql.includes("AS player_enrich_terminal_waiting")
          && sql.includes("NOT candidate.player_enrich_terminal_waiting");
        const protectsRetryWait = sql.includes("AS player_enrich_retry_waiting")
          && sql.includes("NOT candidate.player_enrich_retry_waiting");
        const selectsDueTerminal = sql.includes("task.status='terminal'")
          && sql.includes("task.next_retry_at<=now()");
        const queueProtectsTerminal = sql.includes("AS player_enrich_open")
          && sql.includes("task.status='terminal'");
        const rows = state.contents
          .filter((row) => {
            const isPending = state.enrichPending.has(row.content_key);
            const isLeased = state.enrichLeased.has(row.content_key);
            const taskState = state.enrichTaskStates.get(row.content_key);
            const retryWaiting = ["queued", "failed"].includes(taskState?.status)
              && taskState.retry_due === false;
            const terminalSchedule = state.enrichTerminalSchedule.get(row.content_key);
            const effectivePublishedAt = row.published_at
              ?? observedPublishedAt.get(row.source_content_id)
              ?? null;
            const published = effectivePublishedAt == null ? null : new Date(effectivePublishedAt);
            const isRecent = published != null && published >= cutoff;
            if (isLeased && protectsLiveLease) return false;
            if (terminalSchedule === "future" && protectsTerminalWait) return false;
            if (retryWaiting && protectsRetryWait) return false;
            if (terminalSchedule && !clockOwnsEnrich && queueProtectsTerminal) return false;
            if (terminalSchedule === "due" && selectsDueTerminal) {
              return clockOwnsEnrich && pendingBypassesWindow;
            }
            if (isPending) return clockOwnsEnrich && pendingBypassesWindow;
            return isRecent;
          })
          .map((row) => ({
            ...row,
            published_at: row.published_at
              ?? observedPublishedAt.get(row.source_content_id)
              ?? null,
            enrich_pending: state.enrichPending.has(row.content_key),
          }));
        return { rowCount: rows.length, rows };
      }
      if (sql.includes("SELECT * FROM crawler.contents") && sql.includes("FOR UPDATE")) {
        return { rowCount: params[0].length, rows: state.contents.filter((row) => params[0].includes(row.content_key)) };
      }
      if (sql.includes("UPDATE crawler.contents\n     SET content_type=CASE")) {
        const row = state.contents.find((item) => item.content_key === params[0]);
        row.content_type = params[35] ?? row.content_type;
        row.title = params[9] ?? row.title;
        row.thumbnail_url = params[10] ?? row.thumbnail_url;
        if (params[12]) row.description = params[11];
        if (params[15]) row.hashtags = params[14];
        if (params[17]) row.keywords = params[16];
        row.published_at = params[18] ?? row.published_at;
        row.published_at_source = params[19] ?? row.published_at_source;
        row.published_at_precision = params[20] ?? row.published_at_precision;
        row.duration_seconds = params[22] ?? row.duration_seconds;
        row.view_count = params[2];
        row.view_count_source = params[24] ?? row.view_count_source;
        row.comments_disabled = params[5];
        row.comments_first_page = params[38] == null ? row.comments_first_page : JSON.parse(params[38]);
        row.video_change_probability = params[8];
        row.access_status = params[27] ?? row.access_status;
        row.last_enriched_at = params[1];
        return { rowCount: 1, rows: [] };
      }
      if (sql.includes("INSERT INTO crawler.content_enrich_tasks")) {
        const contentKey = params[1];
        if (sql.includes("jsonb_build_object('last_outcome'")) {
          const prior = state.enrichTaskStates.get(contentKey);
          const status = params[4];
          const incrementsAttempts = params[13] === true;
          const priorAttempts = Number(
            prior?.attempts ?? (state.enrichTerminalSchedule.has(contentKey) ? 5 : 0),
          );
          const resetsTerminalRound = params[15] === true;
          const attempts = resetsTerminalRound
            ? (incrementsAttempts ? 1 : 0)
            : incrementsAttempts
              ? (["done", "skipped"].includes(prior?.status) ? 1 : priorAttempts + 1)
              : priorAttempts;
          const guardsInitialSuccess = sql.includes(
            "CASE WHEN $15::boolean THEN $13::timestamptz ELSE NULL END",
          );
          const lastSuccessAt = params[14] === true
            ? params[12]
            : prior?.last_success_at ?? (guardsInitialSuccess ? null : params[12]);
          state.enrichTaskStates.set(contentKey, {
            task_id: prior?.task_id ?? `player-refresh:${contentKey}`,
            status,
            attempts,
            last_success_at: lastSuccessAt,
            dispatch_generation: prior?.dispatch_generation ?? 1,
            lease_live: false,
            retry_due: params[10] != null,
          });
          state.enrichPending.delete(contentKey);
          state.enrichTerminalSchedule.delete(contentKey);
          if (status === "failed") state.enrichPending.add(contentKey);
          if (status === "terminal") state.enrichTerminalSchedule.set(contentKey, "future");
          return { rowCount: 1, rows: [] };
        }
        if (state.enrichTerminalSchedule.has(contentKey)) {
          const retainsTerminal = /status='terminal'\s+AND \$8::timestamptz IS NOT NULL/.test(sql);
          if (retainsTerminal && params[7]) state.enrichTerminalSchedule.set(contentKey, "future");
          else state.enrichTerminalSchedule.delete(contentKey);
        }
        return { rowCount: 1, rows: [] };
      }
      if (sql.includes("UPDATE crawler.content_enrich_tasks")
          && (sql.includes("status='done'") || sql.includes("SET status=$3"))) {
        state.enrichPending.delete(params[0]);
        if (sql.includes("status='terminal'") && sql.includes("next_retry_at<=now()")) {
          state.enrichTerminalSchedule.delete(params[0]);
        }
        return { rowCount: 1, rows: [] };
      }
      if (sql.includes("INSERT INTO crawler.crawler_outbox")) {
        state.outbox.push(JSON.parse(params[7]));
        return { rowCount: 1, rows: [] };
      }
      if (sql.includes("INSERT INTO crawler.crawl_observations")) {
        state.observationSummaries.push(JSON.parse(params[14]));
        state.extractorVersions.push(JSON.parse(params[17]));
        return { rowCount: 1, rows: [] };
      }
      return { rowCount: 1, rows: [] };
    },
  };

  return {
    state,
    withTransaction: async (action) => {
      if (beforeTransaction && transactionCount === 0) await beforeTransaction(state);
      transactionCount += 1;
      return action(client);
    },
    query: async (sql, params = []) => {
      state.sql.push(sql);
      if (sql.includes("unnest(cursor.anchor_video_ids)")) {
        const byId = new Map(state.contents.map((row) => [row.source_content_id, row]));
        return {
          rows: [{
            anchors: state.cursorAnchors
              .map((video_id) => ({ video_id, published_at: byId.get(video_id)?.published_at ?? null }))
              .filter((row) => row.published_at != null),
          }],
        };
      }
      if (sql.includes("SELECT video_id,published_at")) {
        return {
          rows: [...state.contents]
            .sort((left, right) => right.published_at.localeCompare(left.published_at))
            .map((row) => ({ video_id: row.source_content_id, published_at: row.published_at })),
        };
      }
      if (sql.includes("FROM crawler.contents") && sql.includes("source_content_id=ANY")) {
        const known = new Set(state.contents.map((row) => row.source_content_id));
        return { rows: params[1].filter((id) => known.has(id)).map((video_id) => ({ video_id })) };
      }
      if (sql.includes("DISTINCT ON (candidate.source_content_id)")) {
        const requested = new Set(params[1]);
        const latest = new Map();
        state.candidateRows.forEach((row, index) => {
          if (row.channel_id !== params[0] || !requested.has(row.source_content_id)) return;
          latest.set(row.source_content_id, { ...row, candidate_id: index + 1 });
        });
        return { rows: [...latest.values()] };
      }
      if (sql.includes("FROM crawler.content_candidates candidate")
          && sql.includes("candidate.first_seen_ledger_status='pending'")) {
        return {
          rows: state.candidateRows
            .filter((row) => row.channel_id === params[0])
            .filter((row) => row.first_seen_ledger_status === "pending")
            .map((row) => {
              const content = state.contents.find((item) => (
                item.channel_id === row.channel_id
                && item.source_content_id === row.source_content_id
              ));
              return {
                ...row,
                video_id: row.source_content_id,
                content_key: content?.content_key ?? row.content_key,
                published_at: content?.published_at ?? null,
                published_at_precision: content?.published_at_precision ?? "unknown",
              };
            }),
        };
      }
      if (sql.includes("SELECT * FROM crawler.contents")) {
        return { rows: state.contents.map((row) => ({ ...row })) };
      }
      if (sql.includes("FROM crawler.content_candidates candidate")
          && sql.includes("candidate.next_attempt_at<=$2::timestamptz")) {
        const dueAt = new Date(params[1]).getTime();
        return {
          rows: state.candidateRows
            .filter((row) => row.channel_id === params[0])
            .filter((row) => ["deferred", "terminal_excluded"].includes(row.disposition))
            .filter((row) => row.next_attempt_at && new Date(row.next_attempt_at).getTime() <= dueAt)
            .filter((row) => !state.contents.some(
              (content) => content.source_content_id === row.source_content_id,
            ))
            .map((row) => ({ ...row })),
        };
      }
      throw new Error(`unexpected query: ${sql}`);
    },
  };
}

test("Video discovery defers a public detail without authoritative type evidence", async () => {
  const fixture = databaseFixture();
  const result = await executeIncrementalVideo({
    plan: plan(),
    runId: "incremental:deferred-type",
    startedAt: "2026-07-20T00:00:00.000Z",
    query: fixture.query,
    withTransaction: fixture.withTransaction,
    getChannelSnapshot: async () => ({
      async scanUploads() {
        return {
          playlist_id: "UUvideo",
          entries: [{
            id: "public-without-type",
            position: 1,
            title: "Public without type",
            published_day: "2026-07-19",
          }],
          pages: 1,
          item_count: 1,
          parse_gap_count: 0,
          anchor_matched: true,
          matched_anchor_id: "known-anchor",
          stop_reason: "anchor_matched",
          terminal_reason: "anchor_matched",
          complete: true,
          raw: { engine: "youtubei.js@test" },
        };
      },
    }),
    fetchDetail: async (videoId) => ({
      id: videoId,
      title: "Player returned a partial detail",
      access_status: "public",
      availability: "public",
      ytdlp_client: "web",
      extractor_version: "yt-dlp@test",
      content_type_signals: {
        source: "yt_dlp_player",
        canonical_url: null,
        is_shorts_eligible: null,
        is_live_content: null,
      },
    }),
  });

  assert.equal(result.outcome, "partial");
  assert.equal(result.first_seen_count, 0);
  assert.equal(
    fixture.state.contents.some((row) => row.source_content_id === "public-without-type"),
    false,
  );
  assert.equal(fixture.state.candidateRows.length, 1);
  assert.deepEqual(fixture.state.candidateRows[0], {
    candidate_id: "1",
    run_id: "incremental:deferred-type",
    channel_id: "UCvideo",
    source_content_id: "public-without-type",
    position: 1,
    content_type: null,
    type_status: "unresolved",
    detail_status: "done",
    api_status: "not_needed",
    missing_fields: ["content_type"],
    disposition: "deferred",
    next_attempt_at: "2026-07-20T06:00:00.000Z",
    first_seen_ledger_status: "not_applicable",
    first_seen_ledger_observation_id: null,
    result_json: {
      flat: {
        id: "public-without-type",
        position: 1,
        title: "Public without type",
        published_day: "2026-07-19",
      },
      detail: {
        id: "public-without-type",
        title: "Player returned a partial detail",
        access_status: "public",
        availability: "public",
        ytdlp_client: "web",
        extractor_version: "yt-dlp@test",
        content_type_signals: {
          source: "yt_dlp_player",
          canonical_url: null,
          is_shorts_eligible: null,
          is_live_content: null,
        },
      },
      classification: null,
      access: {
        access_status: "public",
        access_status_source: "yt_dlp",
      },
      disposition: {
        version: "video-disposition-v1",
        kind: "deferred",
        reason_code: "authoritative_type_unresolved",
        retry_class: "alternate_player",
        retryable: true,
        observed_at: "2026-07-20T00:00:00.000Z",
        next_attempt_at: "2026-07-20T06:00:00.000Z",
      },
      extractor: {
        source: "yt_dlp",
        client: "web",
        version: "yt-dlp@test",
      },
    },
    error_message: "authoritative content type evidence is missing",
  });
  assert.deepEqual(
    fixture.state.outbox[0].payload.discovery.payload.dispositions,
    [{
      video_id: "public-without-type",
      kind: "deferred",
      reason_code: "authoritative_type_unresolved",
      retry_class: "alternate_player",
    }],
  );
  assert.equal(fixture.state.cursorUpdates[0].anchorVideoIds, null);
  assert.equal(fixture.state.cursorUpdates[0].sourceCursor, null);
});

test("Video discovery accounts for every new ID with exactly one disposition", async () => {
  const fixture = databaseFixture();
  const result = await executeIncrementalVideo({
    plan: plan(),
    runId: "incremental:mixed-disposition-ledger",
    startedAt: "2026-07-20T00:00:00.000Z",
    query: fixture.query,
    withTransaction: fixture.withTransaction,
    getChannelSnapshot: async () => ({
      async scanUploads() {
        return {
          playlist_id: "UUvideo",
          entries: [
            { id: "stored-video", position: 1, title: "Stored" },
            { id: "deferred-video", position: 2, title: "Deferred" },
            {
              id: "scheduled-live",
              position: 3,
              title: "Scheduled",
              content_type: "live",
              is_upcoming: true,
            },
            { id: "known-anchor", position: 4, title: "Known" },
          ],
          pages: 1,
          item_count: 4,
          parse_gap_count: 0,
          anchor_matched: true,
          matched_anchor_id: "known-anchor",
          stop_reason: "anchor_matched",
          terminal_reason: "anchor_matched",
          complete: true,
          raw: { engine: "youtubei.js@test" },
        };
      },
    }),
    fetchDetail: async (videoId) => videoId === "deferred-video"
      ? {
          id: videoId,
          access_status: "public",
          availability: "public",
          content_type_signals: {
            source: "yt_dlp_player",
            canonical_url: null,
            is_shorts_eligible: null,
            is_live_content: null,
          },
        }
      : detail(videoId, 10),
  });

  const discovery = fixture.state.outbox[0].payload.discovery.payload;
  assert.equal(result.outcome, "partial");
  assert.equal(discovery.discovered_count, 3);
  assert.equal(discovery.silent_drop_count, 0);
  assert.equal(
    discovery.stored_count + discovery.deferred_count + discovery.terminal_excluded_count,
    discovery.discovered_count,
  );
  assert.deepEqual(
    [...new Set(discovery.dispositions.map((item) => item.video_id))].sort(),
    ["deferred-video", "scheduled-live", "stored-video"],
  );
  assert.deepEqual(
    discovery.dispositions.map((item) => item.kind).sort(),
    ["deferred", "stored", "terminal_excluded"],
  );
  assert.equal(fixture.state.candidateRows.length, 3);
  assert.equal(fixture.state.cursorUpdates[0].anchorVideoIds, null);
  assert.equal(fixture.state.cursorUpdates[0].sourceCursor, null);
});

test("First-Seen Video keeps an incomplete public detail open for Enrich", async () => {
  const fixture = databaseFixture();
  const videoId = "first-seen-incomplete-detail";
  const contentKey = `UCvideo:video:${videoId}`;

  const result = await executeIncrementalVideo({
    plan: plan(),
    runId: "incremental:first-seen-incomplete-detail",
    startedAt: "2026-07-20T00:00:00.000Z",
    query: fixture.query,
    withTransaction: fixture.withTransaction,
    getChannelSnapshot: async () => ({
      async scanUploads() {
        return {
          playlist_id: "UUvideo",
          entries: [
            { id: videoId, position: 1, title: "First-Seen incomplete" },
            { id: "known-anchor", position: 2, title: "Known" },
          ],
          pages: 1,
          item_count: 2,
          parse_gap_count: 0,
          anchor_matched: true,
          matched_anchor_id: "known-anchor",
          stop_reason: "anchor_matched",
          terminal_reason: "anchor_matched",
          complete: true,
          raw: { engine: "youtubei.js@test" },
        };
      },
    }),
    fetchDetail: async (fetchedVideoId) => ({
      ...detail(fetchedVideoId, 10),
      duration_seconds: null,
    }),
  });

  assert.equal(result.first_seen_count, 1);
  const content = fixture.state.contents.find((row) => row.content_key === contentKey);
  assert.equal(content.last_enriched_at, null);
  assert.deepEqual(
    (({ status, attempts, last_success_at }) => ({ status, attempts, last_success_at }))(
      fixture.state.enrichTaskStates.get(contentKey),
    ),
    { status: "failed", attempts: 1, last_success_at: null },
  );
  assert.deepEqual(
    (({ detail_status, missing_fields, error_message }) => ({
      detail_status,
      missing_fields,
      error_message,
    }))(fixture.state.candidateRows.find((row) => row.source_content_id === videoId)),
    {
      detail_status: "failed",
      missing_fields: ["detail"],
      error_message: "Content Enrich detail is missing the required public Video surface",
    },
  );
});

test("Video discovery idempotently resolves a deferred candidate and retains its evidence", async () => {
  const fixture = databaseFixture();
  let authoritative = false;
  const execute = (startedAt) => executeIncrementalVideo({
    plan: plan(),
    runId: "incremental:deferred-replay",
    startedAt,
    query: fixture.query,
    withTransaction: fixture.withTransaction,
    getChannelSnapshot: async () => ({
      async scanUploads() {
        return {
          playlist_id: "UUvideo",
          entries: [
            { id: "replayed-video", position: 1, title: "Replay" },
            { id: "known-anchor", position: 2, title: "Known" },
          ],
          pages: 1,
          item_count: 2,
          parse_gap_count: 0,
          anchor_matched: true,
          matched_anchor_id: "known-anchor",
          stop_reason: "anchor_matched",
          terminal_reason: "anchor_matched",
          complete: true,
          raw: { engine: "youtubei.js@test" },
        };
      },
    }),
    fetchDetail: async (videoId) => authoritative
      ? detail(videoId, 12)
      : {
          id: videoId,
          title: "Incomplete Player detail",
          access_status: "public",
          availability: "public",
          content_type_signals: {
            source: "yt_dlp_player",
            canonical_url: null,
            is_shorts_eligible: null,
            is_live_content: null,
          },
        },
  });

  const deferred = await execute("2026-07-20T00:00:00.000Z");
  assert.equal(deferred.outcome, "partial");
  authoritative = true;
  const recovered = await execute("2026-07-20T06:00:00.000Z");

  assert.equal(recovered.outcome, "complete");
  assert.equal(fixture.state.candidateRows.length, 1);
  assert.equal(
    fixture.state.contents.filter((row) => row.source_content_id === "replayed-video").length,
    1,
  );
  const candidate = fixture.state.candidateRows[0];
  assert.equal(candidate.disposition, "stored");
  assert.equal(candidate.next_attempt_at, null);
  assert.equal(candidate.result_json.disposition.reason_code, "content_stored");
  assert.equal(
    candidate.result_json.recovery.from_disposition.reason_code,
    "authoritative_type_unresolved",
  );
  assert.equal(candidate.result_json.recovery.deferred_error_message,
    "authoritative content type evidence is missing");
  assert.equal(
    candidate.result_json.recovery.deferred_evidence.detail.title,
    "Incomplete Player detail",
  );
  assert.equal(candidate.result_json.recovery.resolved_at, "2026-07-20T06:00:00.000Z");
});

test("Video discovery throttles a deferred recheck before it is due without advancing the cursor", async () => {
  const fixture = databaseFixture();
  const fetched = [];
  const execute = ({ runId, startedAt, jobId }) => executeIncrementalVideo({
    plan: { ...plan(), job_id: jobId },
    runId,
    startedAt,
    query: fixture.query,
    withTransaction: fixture.withTransaction,
    getChannelSnapshot: async () => ({
      async scanUploads() {
        return {
          playlist_id: "UUvideo",
          entries: [
            { id: "throttled-deferred", position: 1, title: "Deferred" },
            { id: "known-anchor", position: 2, title: "Known" },
          ],
          pages: 1,
          item_count: 2,
          parse_gap_count: 0,
          anchor_matched: true,
          matched_anchor_id: "known-anchor",
          stop_reason: "anchor_matched",
          terminal_reason: "anchor_matched",
          complete: true,
          raw: { engine: "youtubei.js@test" },
        };
      },
    }),
    fetchDetail: async (videoId) => {
      fetched.push(videoId);
      return videoId === "throttled-deferred"
        ? {
            id: videoId,
            title: "Incomplete Player detail",
            access_status: "public",
            availability: "public",
            content_type_signals: {
              source: "yt_dlp_player",
              canonical_url: null,
              is_shorts_eligible: null,
              is_live_content: null,
            },
          }
        : detail(videoId, 10);
    },
  });

  await execute({
    runId: "incremental:deferred-throttle:first",
    startedAt: "2026-07-20T00:00:00.000Z",
    jobId: "incremental__deferred-throttle__first",
  });
  const early = await execute({
    runId: "incremental:deferred-throttle:early",
    startedAt: "2026-07-20T01:00:00.000Z",
    jobId: "incremental__deferred-throttle__early",
  });

  assert.equal(early.outcome, "partial");
  assert.equal(fetched.filter((videoId) => videoId === "throttled-deferred").length, 1);
  assert.equal(
    fixture.state.candidateRows.filter(
      (row) => row.source_content_id === "throttled-deferred",
    ).length,
    1,
  );
  const earlyDiscovery = fixture.state.outbox.at(-1).payload.discovery.payload;
  assert.equal(earlyDiscovery.discovered_count, 0);
  assert.deepEqual(earlyDiscovery.pending_deferred_video_ids, ["throttled-deferred"]);
  assert.equal(fixture.state.cursorUpdates.at(-1).sourceCursor, null);
  assert.equal(fixture.state.cursorUpdates.at(-1).anchorVideoIds, null);

  const due = await execute({
    runId: "incremental:deferred-throttle:due",
    startedAt: "2026-07-20T06:00:00.000Z",
    jobId: "incremental__deferred-throttle__due",
  });
  assert.equal(due.outcome, "partial");
  assert.equal(fetched.filter((videoId) => videoId === "throttled-deferred").length, 2);
  assert.equal(
    fixture.state.candidateRows.filter(
      (row) => row.source_content_id === "throttled-deferred",
    ).length,
    2,
  );
  const dueDiscovery = fixture.state.outbox.at(-1).payload.discovery.payload;
  assert.deepEqual(dueDiscovery.recheck_deferred_video_ids, ["throttled-deferred"]);
  assert.equal(fixture.state.cursorUpdates.at(-1).sourceCursor, null);
  assert.equal(fixture.state.cursorUpdates.at(-1).anchorVideoIds, null);
});

test("an outstanding deferred ID blocks the cursor even when it leaves the Uploads page", async () => {
  const fixture = databaseFixture();
  const fetched = [];
  const execute = ({ runId, startedAt, jobId, scanEntries }) => executeIncrementalVideo({
    plan: { ...plan(), job_id: jobId },
    runId,
    startedAt,
    query: fixture.query,
    withTransaction: fixture.withTransaction,
    getChannelSnapshot: async () => ({
      async scanUploads() {
        return {
          playlist_id: "UUvideo",
          entries: scanEntries,
          pages: 1,
          item_count: scanEntries.length,
          parse_gap_count: 0,
          anchor_matched: true,
          matched_anchor_id: "known-anchor",
          stop_reason: "anchor_matched",
          terminal_reason: "anchor_matched",
          complete: true,
          raw: { engine: "youtubei.js@test" },
        };
      },
    }),
    fetchDetail: async (videoId) => {
      fetched.push(videoId);
      return videoId === "deferred-off-page"
        ? {
            id: videoId,
            title: "Incomplete Player detail",
            access_status: "public",
            availability: "public",
            content_type_signals: {
              source: "yt_dlp_player",
              canonical_url: null,
              is_shorts_eligible: null,
              is_live_content: null,
            },
          }
        : detail(videoId, 10);
    },
  });

  await execute({
    runId: "incremental:deferred-off-page:first",
    startedAt: "2026-07-20T00:00:00.000Z",
    jobId: "incremental__deferred-off-page__first",
    scanEntries: [
      { id: "deferred-off-page", position: 1, title: "Deferred" },
      { id: "known-anchor", position: 2, title: "Known" },
    ],
  });
  const early = await execute({
    runId: "incremental:deferred-off-page:early",
    startedAt: "2026-07-20T01:00:00.000Z",
    jobId: "incremental__deferred-off-page__early",
    scanEntries: [{ id: "known-anchor", position: 1, title: "Known" }],
  });

  assert.equal(early.outcome, "partial");
  assert.equal(fetched.filter((videoId) => videoId === "deferred-off-page").length, 1);
  assert.deepEqual(
    fixture.state.outbox.at(-1).payload.discovery.payload.blocking_deferred_video_ids,
    ["deferred-off-page"],
  );
  assert.equal(fixture.state.cursorUpdates.at(-1).sourceCursor, null);
  assert.equal(fixture.state.cursorUpdates.at(-1).anchorVideoIds, null);
});

test("Video discovery closes a private detail and schedules a low-frequency recheck", async () => {
  const fixture = databaseFixture();
  const result = await executeIncrementalVideo({
    plan: plan(),
    runId: "incremental:private-terminal",
    startedAt: "2026-07-20T00:00:00.000Z",
    query: fixture.query,
    withTransaction: fixture.withTransaction,
    getChannelSnapshot: async () => ({
      async scanUploads() {
        return {
          playlist_id: "UUvideo",
          entries: [{ id: "private-video", position: 1, title: "Private" }],
          pages: 1,
          item_count: 1,
          parse_gap_count: 0,
          anchor_matched: true,
          matched_anchor_id: "known-anchor",
          crossed_anchor_ids: ["known-anchor"],
          stop_reason: "anchor_matched",
          terminal_reason: "anchor_matched",
          complete: true,
          raw: { engine: "youtubei.js@test" },
        };
      },
    }),
    fetchDetail: async (videoId) => ({
      id: videoId,
      title: "Private",
      access_status: "private",
      availability: "private",
      extractor_version: "youtubei.js@test",
    }),
  });

  assert.equal(result.outcome, "complete");
  assert.equal(result.first_seen_count, 0);
  assert.equal(
    fixture.state.contents.some((row) => row.source_content_id === "private-video"),
    false,
  );
  assert.deepEqual(
    (({
      type_status,
      detail_status,
      api_status,
      missing_fields,
      disposition,
      next_attempt_at,
      result_json,
      error_message,
    }) => ({
      type_status,
      detail_status,
      api_status,
      missing_fields,
      disposition,
      next_attempt_at,
      disposition_evidence: result_json.disposition,
      access_evidence: result_json.access,
      error_message,
    }))(fixture.state.candidateRows[0]),
    {
      type_status: "unavailable",
      detail_status: "unavailable",
      api_status: "unavailable",
      missing_fields: [],
      disposition: "terminal_excluded",
      next_attempt_at: "2026-07-27T00:00:00.000Z",
      disposition_evidence: {
        version: "video-disposition-v1",
        kind: "terminal_excluded",
        reason_code: "access_private",
        retry_class: "low_frequency_access_recheck",
        retryable: false,
        observed_at: "2026-07-20T00:00:00.000Z",
        next_attempt_at: "2026-07-27T00:00:00.000Z",
      },
      access_evidence: {
        access_status: "private",
        access_status_source: "youtubejs_player",
      },
      error_message: null,
    },
  );
  assert.deepEqual(
    fixture.state.outbox[0].payload.discovery.payload.dispositions,
    [{
      video_id: "private-video",
      kind: "terminal_excluded",
      reason_code: "access_private",
      retry_class: "low_frequency_access_recheck",
    }],
  );
  assert.deepEqual(fixture.state.cursorUpdates[0].anchorVideoIds.slice(0, 2), [
    "private-video",
    "known-anchor",
  ]);
  assert.notEqual(fixture.state.cursorUpdates[0].sourceCursor, null);
});

test("Video discovery does not recheck a terminal ID from Uploads before it is due", async () => {
  const fixture = databaseFixture();
  const fetched = [];
  const execute = ({ runId, startedAt, jobId }) => executeIncrementalVideo({
    plan: { ...plan(), job_id: jobId },
    runId,
    startedAt,
    query: fixture.query,
    withTransaction: fixture.withTransaction,
    getChannelSnapshot: async () => ({
      async scanUploads() {
        return {
          playlist_id: "UUvideo",
          entries: [
            { id: "private-on-uploads", position: 1, title: "Private" },
            { id: "known-anchor", position: 2, title: "Known" },
          ],
          pages: 1,
          item_count: 2,
          parse_gap_count: 0,
          anchor_matched: true,
          matched_anchor_id: "known-anchor",
          stop_reason: "anchor_matched",
          terminal_reason: "anchor_matched",
          complete: true,
          raw: { engine: "youtubei.js@test" },
        };
      },
    }),
    fetchDetail: async (videoId) => {
      fetched.push({ videoId, startedAt });
      return videoId === "private-on-uploads"
        ? {
            id: videoId,
            access_status: "private",
            availability: "private",
            extractor_version: "youtubei.js@test",
          }
        : detail(videoId, 10);
    },
  });

  await execute({
    runId: "incremental:terminal-current-page:first",
    startedAt: "2026-07-20T00:00:00.000Z",
    jobId: "incremental__terminal-current-page__first",
  });
  await execute({
    runId: "incremental:terminal-current-page:early",
    startedAt: "2026-07-21T00:00:00.000Z",
    jobId: "incremental__terminal-current-page__early",
  });
  assert.equal(
    fetched.filter((item) => item.videoId === "private-on-uploads").length,
    1,
  );
  assert.equal(
    fixture.state.candidateRows.filter((row) => row.source_content_id === "private-on-uploads").length,
    1,
  );

  await execute({
    runId: "incremental:terminal-current-page:due",
    startedAt: "2026-07-27T00:00:00.000Z",
    jobId: "incremental__terminal-current-page__due",
  });
  assert.equal(
    fetched.filter((item) => item.videoId === "private-on-uploads").length,
    2,
  );
  assert.equal(
    fixture.state.candidateRows.filter((row) => row.source_content_id === "private-on-uploads").length,
    2,
  );
  const dueDiscovery = fixture.state.outbox.at(-1).payload.discovery.payload;
  assert.equal(dueDiscovery.discovered_count, 0);
  assert.deepEqual(dueDiscovery.dispositions, []);
  assert.deepEqual(dueDiscovery.recheck_dispositions, [{
    video_id: "private-on-uploads",
    kind: "terminal_excluded",
    reason_code: "access_private",
    retry_class: "low_frequency_access_recheck",
  }]);
});

test("Video discovery rechecks a due terminal exclusion outside the current Uploads page", async () => {
  const fixture = databaseFixture();
  const execute = ({ runId, startedAt, jobId, scanEntries, accessStatus }) => executeIncrementalVideo({
    plan: { ...plan(), job_id: jobId },
    runId,
    startedAt,
    query: fixture.query,
    withTransaction: fixture.withTransaction,
    getChannelSnapshot: async () => ({
      async scanUploads() {
        return {
          playlist_id: "UUvideo",
          entries: scanEntries,
          pages: 1,
          item_count: scanEntries.length,
          parse_gap_count: 0,
          anchor_matched: true,
          matched_anchor_id: "known-anchor",
          stop_reason: "anchor_matched",
          terminal_reason: "anchor_matched",
          complete: true,
          raw: { engine: "youtubei.js@test" },
        };
      },
    }),
    fetchDetail: async (videoId) => accessStatus === "private"
      ? {
          id: videoId,
          title: "Private",
          access_status: "private",
          availability: "private",
          extractor_version: "youtubei.js@test",
        }
      : detail(videoId, 12),
  });

  const first = await execute({
    runId: "incremental:terminal-recheck:first",
    startedAt: "2026-07-20T00:00:00.000Z",
    jobId: "incremental__terminal-recheck__first",
    scanEntries: [
      { id: "recheck-private", position: 1, title: "Private" },
      { id: "known-anchor", position: 2, title: "Known" },
    ],
    accessStatus: "private",
  });
  assert.equal(first.outcome, "complete");
  assert.equal(
    fixture.state.contents.some((row) => row.source_content_id === "recheck-private"),
    false,
  );

  const second = await execute({
    runId: "incremental:terminal-recheck:second",
    startedAt: "2026-07-27T00:00:00.000Z",
    jobId: "incremental__terminal-recheck__second",
    scanEntries: [{ id: "known-anchor", position: 1, title: "Known" }],
    accessStatus: "public",
  });

  assert.equal(second.outcome, "complete");
  assert.equal(second.first_seen_count, 1);
  assert.equal(
    fixture.state.contents.filter((row) => row.source_content_id === "recheck-private").length,
    1,
  );
  const recoveredCandidate = fixture.state.candidateRows.find(
    (row) => row.run_id === "incremental:terminal-recheck:second"
      && row.source_content_id === "recheck-private",
  );
  assert.equal(recoveredCandidate.disposition, "stored");
  assert.equal(recoveredCandidate.result_json.disposition.reason_code, "content_stored");
  assert.equal(fixture.state.cursorUpdates.at(-1).outcome, "complete");
});

test("a failed low-frequency terminal recheck preserves the non-blocking conclusion", async () => {
  const fixture = databaseFixture();
  const execute = ({ runId, startedAt, jobId, scanEntries, recheckFails = false }) => (
    executeIncrementalVideo({
      plan: { ...plan(), job_id: jobId },
      runId,
      startedAt,
      query: fixture.query,
      withTransaction: fixture.withTransaction,
      getChannelSnapshot: async () => ({
        async scanUploads() {
          return {
            playlist_id: "UUvideo",
            entries: scanEntries,
            pages: 1,
            item_count: scanEntries.length,
            parse_gap_count: 0,
            anchor_matched: true,
            matched_anchor_id: "known-anchor",
            stop_reason: "anchor_matched",
            terminal_reason: "anchor_matched",
            complete: true,
            raw: { engine: "youtubei.js@test" },
          };
        },
      }),
      fetchDetail: async (videoId) => {
        if (recheckFails && videoId === "private-recheck-failure") {
          throw new Error("temporary Player timeout");
        }
        return videoId === "private-recheck-failure"
          ? {
              id: videoId,
              title: "Private",
              access_status: "private",
              availability: "private",
              extractor_version: "youtubei.js@test",
            }
          : detail(videoId, 12);
      },
    })
  );

  await execute({
    runId: "incremental:terminal-recheck-failure:first",
    startedAt: "2026-07-20T00:00:00.000Z",
    jobId: "incremental__terminal-recheck-failure__first",
    scanEntries: [
      { id: "private-recheck-failure", position: 1, title: "Private" },
      { id: "known-anchor", position: 2, title: "Known" },
    ],
  });
  const retried = await execute({
    runId: "incremental:terminal-recheck-failure:second",
    startedAt: "2026-07-27T00:00:00.000Z",
    jobId: "incremental__terminal-recheck-failure__second",
    scanEntries: [{ id: "known-anchor", position: 1, title: "Known" }],
    recheckFails: true,
  });

  assert.equal(retried.outcome, "complete");
  assert.equal(retried.first_seen_count, 0);
  const recheckCandidate = fixture.state.candidateRows.find(
    (row) => row.run_id === "incremental:terminal-recheck-failure:second"
      && row.source_content_id === "private-recheck-failure",
  );
  assert.equal(recheckCandidate.disposition, "terminal_excluded");
  assert.equal(recheckCandidate.result_json.disposition.reason_code, "access_private");
  assert.equal(
    recheckCandidate.result_json.collection_error.message,
    "temporary Player timeout",
  );
  assert.equal(
    recheckCandidate.result_json.flat.disposition_recheck.prior_reason_code,
    "access_private",
  );
  assert.equal(fixture.state.cursorUpdates.at(-1).sourceCursor != null, true);
});

test("an incomplete Uploads scan does not consume or downgrade a due terminal recheck", async () => {
  const fixture = databaseFixture();
  const fetched = [];
  const execute = ({ runId, startedAt, jobId, complete }) => executeIncrementalVideo({
    plan: { ...plan(), job_id: jobId },
    runId,
    startedAt,
    query: fixture.query,
    withTransaction: fixture.withTransaction,
    getChannelSnapshot: async () => ({
      async scanUploads() {
        return {
          playlist_id: "UUvideo",
          entries: [
            { id: "private-incomplete-recheck", position: 1, title: "Private" },
            ...(complete
              ? [{ id: "known-anchor", position: 2, title: "Known" }]
              : []),
          ],
          pages: 1,
          item_count: complete ? 2 : 1,
          parse_gap_count: 0,
          anchor_matched: complete,
          matched_anchor_id: complete ? "known-anchor" : null,
          stop_reason: complete ? "anchor_matched" : "max_items",
          terminal_reason: complete ? "anchor_matched" : "max_items",
          complete,
          raw: { engine: "youtubei.js@test" },
        };
      },
    }),
    fetchDetail: async (videoId) => {
      fetched.push(videoId);
      return videoId === "private-incomplete-recheck"
        ? {
            id: videoId,
            access_status: "private",
            availability: "private",
            extractor_version: "youtubei.js@test",
          }
        : detail(videoId, 10);
    },
  });

  await execute({
    runId: "incremental:terminal-incomplete-recheck:first",
    startedAt: "2026-07-20T00:00:00.000Z",
    jobId: "incremental__terminal-incomplete-recheck__first",
    complete: true,
  });
  const retried = await execute({
    runId: "incremental:terminal-incomplete-recheck:second",
    startedAt: "2026-07-27T00:00:00.000Z",
    jobId: "incremental__terminal-incomplete-recheck__second",
    complete: false,
  });

  assert.equal(retried.outcome, "partial");
  assert.equal(
    fetched.filter((videoId) => videoId === "private-incomplete-recheck").length,
    1,
  );
  assert.equal(
    fixture.state.candidateRows.filter(
      (row) => row.source_content_id === "private-incomplete-recheck",
    ).length,
    1,
  );
  const discovery = fixture.state.outbox.at(-1).payload.discovery.payload;
  assert.deepEqual(discovery.recheck_dispositions, []);
  assert.deepEqual(discovery.blocking_deferred_video_ids, []);
});

test("Video execution deduplicates Contents and samples current Uploads dates in the same cycle", async () => {
  const fixture = databaseFixture();
  fixture.state.contents.push({
    content_key: "UCvideo:video:uploads-dated-video",
    channel_id: "UCvideo",
    source_content_id: "uploads-dated-video",
    content_type: "video",
    published_at: null,
    last_seen_at: "2026-07-20T00:00:00.000Z",
    view_count: null,
    player_last_observed_at: null,
    next_last_observed_at: null,
    like_count: null,
    comment_count: null,
  });
  const fetched = [];
  const result = await executeIncrementalVideo({
    plan: plan(),
    runId: "incremental:plan",
    startedAt: "2026-07-20T00:00:00.000Z",
    query: fixture.query,
    withTransaction: fixture.withTransaction,
    getChannelSnapshot: async () => ({
      async scanUploads(options) {
        assert.deepEqual(options.anchors, [
          { id: "known-anchor", published_day: "2026-07-10" },
          { id: "old-video", published_day: "2026-07-09" },
        ]);
        return {
          playlist_id: "UUvideo",
          entries: [
            { id: "new-video", position: 1, content_type: "video", title: "New" },
            { id: "candidate-only", position: 2, content_type: "video", title: "Known Candidate" },
            {
              id: "uploads-dated-video",
              position: 3,
              content_type: "video",
              title: "Uploads dated",
              published_day: "2026-07-19",
              published_at: "2026-07-19",
              published_at_status: "relative",
              published_at_precision: "date_only",
              published_at_source: "youtube_uploads_relative_time",
            },
            { id: "known-anchor", position: 4, content_type: "video", title: "Known" },
          ],
          pages: 1,
          item_count: 4,
          parse_gap_count: 0,
          anchor_matched: true,
          matched_anchor_id: "known-anchor",
          stop_reason: "anchor_matched",
          terminal_reason: "anchor_matched",
          complete: true,
          raw: { engine: "youtubei.js@test" },
        };
      },
    }),
    fetchDetail: async (videoId) => {
      fetched.push(videoId);
      const resolved = detail(videoId, videoId === "new-video" ? 10 : 100);
      return videoId === "new-video"
        ? {
            ...resolved,
            comment_count: null,
            comment_count_status: "disabled",
            comments_disabled: true,
            comments_first_page: null,
          }
        : resolved;
    },
  });

  assert.equal(result.outcome, "complete");
  assert.equal(result.first_seen_count, 2);
  assert.deepEqual(fetched, ["new-video", "candidate-only", "uploads-dated-video", "old-video"]);
  assert.equal(fixture.state.contents.filter((row) => row.source_content_id === "new-video").length, 1);
  assert.equal(fixture.state.contents.filter((row) => row.source_content_id === "candidate-only").length, 1);
  assert.deepEqual(
    (({ comment_count, comment_count_status, comments_disabled }) => ({
      comment_count,
      comment_count_status,
      comments_disabled,
    }))(fixture.state.contents.find((row) => row.source_content_id === "new-video")),
    {
      comment_count: 0,
      comment_count_status: "disabled",
      comments_disabled: true,
    },
  );
  assert.equal(fixture.state.contents.find((row) => row.source_content_id === "old-video").view_count, 100);
  assert.equal(
    fixture.state.contents.find((row) => row.source_content_id === "uploads-dated-video").view_count,
    100,
  );
  assert.equal(
    fixture.state.contents.find((row) => row.source_content_id === "old-video")
      .comments_first_page?.comments?.[0]?.comment_id,
    "comment-old-video",
  );
  assert.equal(fetched.includes("candidate-only"), true);
  assert.deepEqual(fixture.state.outbox.map((event) => event.observation_kind), [
    "video",
  ]);
  assert.equal(fixture.state.outbox[0].payload.discovery.outcome, "complete");
  assert.equal(fixture.state.outbox[0].payload.recent_sampling.outcome, "complete");
  assert.equal(fixture.state.outbox[0].plan_id, plan().plan_id);
  assert.equal(fixture.state.observationSummaries[0].discovery.parse_gap_count, 0);
  assert.equal(fixture.state.sql.some((sql) => /snapshot/i.test(sql)), false);
  assert.equal(fixture.state.sql.some((sql) => sql.includes("channel_video_catalog")), false);
  const discoveryWrite = fixture.state.sql.findIndex((sql) => sql.includes("INSERT INTO crawler.contents"));
  const discoverySql = fixture.state.sql[discoveryWrite];
  assert.match(
    discoverySql,
    /comments_disabled=CASE[\s\S]*WHEN EXCLUDED\.comments_disabled OR EXCLUDED\.comment_count IS NOT NULL[\s\S]*WHEN crawler\.contents\.comments_disabled OR crawler\.contents\.comment_count IS NOT NULL/,
  );
  assert.match(
    discoverySql,
    /access_status=CASE[\s\S]*EXCLUDED\.access_status='unknown'[\s\S]*crawler\.contents\.access_status/,
  );
  assert.match(discoverySql, /ON CONFLICT \(channel_id,source_content_id\)/);
  assert.match(
    discoverySql,
    /content_type=CASE[\s\S]*WHEN \$43::boolean THEN EXCLUDED\.content_type[\s\S]*ELSE crawler\.contents\.content_type/,
  );
  assert.doesNotMatch(discoverySql, /content_type=EXCLUDED\.content_type/);
  const crawlerOutbox = fixture.state.sql.findIndex((sql) => (
    sql.includes("INSERT INTO crawler.crawler_outbox")
  ));
  const publication = fixture.state.sql.findIndex((sql) => (
    sql.includes("publication-reconciler:find-owner")
  ));
  assert.ok(crawlerOutbox >= 0 && publication > crawlerOutbox);
});

test("Video execution classifies every First-Seen Video independently of the stored Video refresh cap", async () => {
  const fixture = databaseFixture();
  const fetched = [];
  const result = await executeIncrementalVideo({
    plan: {
      ...plan(),
      capacity: { ...plan().capacity, player_cap: 1, next_cap: 0 },
    },
    runId: "incremental:first-seen-independent-cap",
    startedAt: "2026-07-20T00:00:00.000Z",
    query: fixture.query,
    withTransaction: fixture.withTransaction,
    getChannelSnapshot: async () => ({
      async scanUploads() {
        return {
          playlist_id: "UUvideo",
          entries: [
            { id: "new-video-a", position: 1, content_type: null, title: "New A" },
            { id: "new-video-b", position: 2, content_type: null, title: "New B" },
            { id: "known-anchor", position: 3, content_type: null, title: "Known" },
          ],
          pages: 1,
          item_count: 3,
          parse_gap_count: 0,
          anchor_matched: true,
          matched_anchor_id: "known-anchor",
          stop_reason: "anchor_matched",
          terminal_reason: "anchor_matched",
          complete: true,
          raw: { engine: "youtubei.js@test" },
        };
      },
    }),
    fetchDetail: async (videoId) => {
      fetched.push(videoId);
      return detail(videoId, videoId === "old-video" ? 100 : 10);
    },
  });

  assert.equal(result.outcome, "complete");
  assert.equal(result.first_seen_count, 2);
  assert.deepEqual(fetched, ["new-video-a", "new-video-b", "old-video"]);
  assert.equal(
    fixture.state.contents.filter((row) => row.source_content_id.startsWith("new-video-")).length,
    2,
  );
  assert.equal(fixture.state.contents.find((row) => row.source_content_id === "old-video").view_count, 100);
});

test("Video execution rechecks First-Seen after acquiring its transaction", async () => {
  const fixture = databaseFixture({
    beforeTransaction(state) {
      state.contents.push({
        content_key: "UCvideo:video:race-video",
        channel_id: "UCvideo",
        source_content_id: "race-video",
        content_type: "video",
        published_at: "2026-07-19T00:00:00.000Z",
        last_seen_at: "2026-07-20T00:00:00.000Z",
        view_count: 10,
        player_last_observed_at: "2026-07-20T00:00:00.000Z",
        next_last_observed_at: null,
        video_change_probability: 0,
        like_count: null,
        comment_count: null,
      });
    },
  });
  const fetched = [];
  const result = await executeIncrementalVideo({
    plan: plan(),
    runId: "incremental:race-plan",
    startedAt: "2026-07-20T00:00:00.000Z",
    query: fixture.query,
    withTransaction: fixture.withTransaction,
    getChannelSnapshot: async () => ({
      async scanUploads() {
        return {
          playlist_id: "UUvideo",
          entries: [
            { id: "race-video", position: 1, content_type: "video", title: "Race" },
            { id: "known-anchor", position: 2, content_type: "video", title: "Known" },
          ],
          pages: 1,
          item_count: 2,
          parse_gap_count: 0,
          anchor_matched: true,
          matched_anchor_id: "known-anchor",
          crossed_anchor_ids: [],
          stop_reason: "anchor_matched",
          terminal_reason: "anchor_matched",
          complete: true,
          raw: { engine: "youtubei.js@test" },
        };
      },
    }),
    fetchDetail: async (videoId) => {
      fetched.push(videoId);
      return detail(videoId, videoId === "old-video" ? 100 : 10);
    },
  });

  assert.equal(result.outcome, "complete");
  assert.equal(result.first_seen_count, 0);
  assert.equal(fixture.state.contents.filter((row) => row.source_content_id === "race-video").length, 1);
  assert.equal(fetched.includes("race-video"), true);
  assert.equal(fixture.state.sql.some((sql) => sql.includes("channel_video_catalog")), false);
});

test("Video execution keeps its backup Anchor pool when a later Anchor closes Discovery", async () => {
  const fixture = databaseFixture();
  const anchorIds = Array.from(
    { length: 20 },
    (_, index) => `anchor-${String(index + 1).padStart(2, "0")}`,
  );
  fixture.state.cursorAnchors = [...anchorIds];
  fixture.state.contents = anchorIds.map((sourceContentId, index) => ({
    content_key: `UCvideo:video:${sourceContentId}`,
    channel_id: "UCvideo",
    source_content_id: sourceContentId,
    content_type: "video",
    published_at: `2026-07-${String(20 - index).padStart(2, "0")}T00:00:00.000Z`,
    last_seen_at: "2026-07-20T00:00:00.000Z",
    view_count: 100,
    player_last_observed_at: "2026-07-20T00:00:00.000Z",
    next_last_observed_at: "2026-07-20T00:00:00.000Z",
    video_change_probability: 0,
    like_count: null,
    comment_count: null,
  }));

  const result = await executeIncrementalVideo({
    plan: plan(),
    runId: "incremental:backup-anchor-pool",
    startedAt: "2026-07-20T00:00:00.000Z",
    query: fixture.query,
    withTransaction: fixture.withTransaction,
    getChannelSnapshot: async () => ({
      async scanUploads() {
        return {
          playlist_id: "UUvideo",
          entries: [
            { id: "new-video", position: 1, content_type: "video", published_day: "2026-07-20" },
            { id: "anchor-05", position: 2, content_type: "video", published_day: "2026-07-16" },
          ],
          pages: 1,
          item_count: 2,
          parse_gap_count: 0,
          anchor_matched: true,
          matched_anchor_id: "anchor-05",
          crossed_anchor_ids: anchorIds.slice(0, 4),
          stop_reason: "anchor_matched",
          terminal_reason: "anchor_matched",
          complete: true,
          raw: { engine: "youtubei.js@test" },
        };
      },
    }),
    fetchDetail: async (videoId) => detail(videoId, 100),
  });

  assert.equal(result.outcome, "complete");
  assert.deepEqual(fixture.state.cursorUpdates[0].anchorVideoIds, [
    "new-video",
    ...anchorIds.slice(0, 19),
  ]);
});

test("Video execution updates Content type without creating a second Video identity", async () => {
  const fixture = databaseFixture();
  fixture.state.contents.push({
    content_key: "UCvideo:video:type-changing",
    channel_id: "UCvideo",
    source_content_id: "type-changing",
    content_type: "video",
    published_at: "2026-07-18T00:00:00.000Z",
    last_seen_at: "2026-07-20T00:00:00.000Z",
    view_count: 100,
    player_last_observed_at: null,
    next_last_observed_at: null,
    video_change_probability: 0,
    like_count: null,
    comment_count: null,
  });

  const result = await executeIncrementalVideo({
    plan: plan(),
    runId: "incremental:type-change",
    startedAt: "2026-07-20T00:00:00.000Z",
    query: fixture.query,
    withTransaction: fixture.withTransaction,
    getChannelSnapshot: async () => ({
      async scanUploads() {
        return {
          playlist_id: "UUvideo",
          entries: [
            { id: "type-changing", position: 1, content_type: "short", published_day: "2026-07-18" },
            { id: "known-anchor", position: 2, content_type: "video", published_day: "2026-07-10" },
          ],
          pages: 1,
          item_count: 2,
          parse_gap_count: 0,
          anchor_matched: true,
          matched_anchor_id: "known-anchor",
          stop_reason: "anchor_matched",
          terminal_reason: "anchor_matched",
          complete: true,
          raw: { engine: "youtubei.js@test" },
        };
      },
    }),
    fetchDetail: async (videoId) => detail(videoId, 100, {
      contentType: videoId === "type-changing" ? "short" : "video",
    }),
  });

  assert.equal(result.outcome, "complete");
  assert.equal(
    fixture.state.contents.filter((row) => row.source_content_id === "type-changing").length,
    1,
  );
  assert.equal(
    fixture.state.contents.find((row) => row.source_content_id === "type-changing").content_type,
    "short",
  );
});

test("Video sampling recovers queued detail when published_at is unresolved", async () => {
  const contentKey = "UCvideo:video:missing-published-at";
  const fixture = databaseFixture({
    beforeTransaction(state) {
      state.contents.push({
        content_key: contentKey,
        channel_id: "UCvideo",
        source_content_id: "missing-published-at",
        content_type: "video",
        published_at: null,
        last_seen_at: "2026-07-20T00:00:00.000Z",
        view_count: null,
        player_last_observed_at: null,
        next_last_observed_at: null,
        video_change_probability: null,
        like_count: null,
        comment_count: null,
      });
      state.enrichPending.add(contentKey);
    },
  });
  const fetched = [];

  const result = await executeIncrementalVideo({
    plan: {
      ...plan(),
      capacity: { ...plan().capacity, player_cap: 1, next_cap: 0 },
    },
    runId: "incremental:recover-missing-published-at",
    startedAt: "2026-07-20T00:00:00.000Z",
    query: fixture.query,
    withTransaction: fixture.withTransaction,
    getChannelSnapshot: async () => ({
      async scanUploads() {
        return {
          playlist_id: "UUvideo",
          entries: [{
            id: "known-anchor",
            position: 1,
            content_type: "video",
            title: "Known",
            published_day: "2026-07-10",
          }],
          pages: 1,
          item_count: 1,
          parse_gap_count: 0,
          anchor_matched: true,
          matched_anchor_id: "known-anchor",
          stop_reason: "anchor_matched",
          terminal_reason: "anchor_matched",
          complete: true,
          raw: { engine: "youtubei.js@test" },
        };
      },
    }),
    fetchDetail: async (videoId) => {
      fetched.push(videoId);
      return {
        ...detail(videoId, 123),
        title: "Recovered title",
        thumbnail_url: "https://i.ytimg.com/vi/missing-published-at/maxresdefault.jpg",
        description: "Recovered description",
        hashtags: ["recovered"],
        hashtags_observed: true,
        keywords: ["one", "two"],
        keywords_observed: true,
        duration_seconds: 91,
        duration_source: "yt_dlp",
        published_at_source: "yt_dlp_timestamp",
        view_count_source: "yt_dlp",
        ytdlp_client: "web_safari",
      };
    },
  });

  assert.equal(result.outcome, "complete");
  assert.deepEqual(fetched, ["missing-published-at"]);
  const recovered = fixture.state.contents.find((row) => row.content_key === contentKey);
  assert.equal(recovered.title, "Recovered title");
  assert.equal(recovered.published_at, "2026-07-19T00:00:00.000Z");
  assert.equal(recovered.published_at_source, "yt_dlp_timestamp");
  assert.equal(recovered.duration_seconds, 91);
  assert.equal(recovered.view_count_source, "yt_dlp");
  assert.equal(fixture.state.enrichPending.has(contentKey), false);
});

test("Video Clock does not consume an open Enrich Task after the database owner switches to the queue", async () => {
  const contentKey = "UCvideo:video:queue-owned";
  const fixture = databaseFixture({
    enrichMode: "queue",
    beforeTransaction(state) {
      state.contents.push({
        content_key: contentKey,
        channel_id: "UCvideo",
        source_content_id: "queue-owned",
        content_type: "video",
        published_at: "2026-07-19T00:00:00.000Z",
        last_seen_at: "2026-07-20T00:00:00.000Z",
        view_count: null,
        player_last_observed_at: null,
        next_last_observed_at: null,
        video_change_probability: null,
        like_count: null,
        comment_count: null,
      });
      state.enrichPending.add(contentKey);
    },
  });
  const fetched = [];

  const result = await executeIncrementalVideo({
    plan: {
      ...plan(),
      capacity: { ...plan().capacity, player_cap: 20, next_cap: 0 },
    },
    runId: "incremental:queue-owned-enrich",
    startedAt: "2026-07-20T00:00:00.000Z",
    query: fixture.query,
    withTransaction: fixture.withTransaction,
    getChannelSnapshot: async () => ({
      async scanUploads() {
        return {
          playlist_id: "UUvideo",
          entries: [{
            id: "known-anchor",
            position: 1,
            content_type: "video",
            title: "Known",
            published_day: "2026-07-10",
          }],
          pages: 1,
          item_count: 1,
          parse_gap_count: 0,
          anchor_matched: true,
          matched_anchor_id: "known-anchor",
          stop_reason: "anchor_matched",
          terminal_reason: "anchor_matched",
          complete: true,
          raw: { engine: "youtubei.js@test" },
        };
      },
    }),
    fetchDetail: async (videoId) => {
      fetched.push(videoId);
      return detail(videoId, 123);
    },
  });

  assert.equal(result.outcome, "complete");
  assert.equal(fetched.includes("queue-owned"), false);
  assert.equal(fixture.state.enrichPending.has(contentKey), true);
});

test("Video Clock does not bypass a recent Enrich Task's retry schedule", async () => {
  const contentKey = "UCvideo:video:retry-waiting";
  const fixture = databaseFixture({
    beforeTransaction(state) {
      state.contents.push({
        content_key: contentKey,
        channel_id: "UCvideo",
        source_content_id: "retry-waiting",
        content_type: "video",
        published_at: "2026-07-19T00:00:00.000Z",
        last_seen_at: "2026-07-20T00:00:00.000Z",
        view_count: null,
        player_last_observed_at: null,
        next_last_observed_at: null,
        video_change_probability: null,
        like_count: null,
        comment_count: null,
      });
      state.enrichPending.add(contentKey);
      state.enrichTaskStates.set(contentKey, {
        task_id: `player-refresh:${contentKey}`,
        status: "failed",
        attempts: 3,
        dispatch_generation: 1,
        lease_live: false,
        retry_due: false,
      });
    },
  });
  const fetched = [];

  await executeIncrementalVideo({
    plan: plan(),
    runId: "incremental:retry-waiting",
    startedAt: "2026-07-20T00:00:00.000Z",
    query: fixture.query,
    withTransaction: fixture.withTransaction,
    getChannelSnapshot: async () => ({
      async scanUploads() {
        return {
          playlist_id: "UUvideo",
          entries: [{
            id: "known-anchor",
            position: 1,
            content_type: "video",
            title: "Known",
            published_day: "2026-07-10",
          }],
          pages: 1,
          item_count: 1,
          parse_gap_count: 0,
          anchor_matched: true,
          matched_anchor_id: "known-anchor",
          stop_reason: "anchor_matched",
          terminal_reason: "anchor_matched",
          complete: true,
          raw: { engine: "youtubei.js@test" },
        };
      },
    }),
    fetchDetail: async (videoId) => {
      fetched.push(videoId);
      return detail(videoId, 123);
    },
  });

  assert.equal(fetched.includes("retry-waiting"), false);
  assert.deepEqual(
    (({ status, attempts }) => ({ status, attempts }))(
      fixture.state.enrichTaskStates.get(contentKey),
    ),
    { status: "failed", attempts: 3 },
  );
});

test("Video Clock discards a result when the Task enters retry wait before its row lock", async () => {
  const contentKey = "UCvideo:video:retry-race";
  let movedToRetryWait = false;
  const fixture = databaseFixture({
    beforeTransaction(state) {
      state.contents.push({
        content_key: contentKey,
        channel_id: "UCvideo",
        source_content_id: "retry-race",
        content_type: "video",
        published_at: "2026-07-19T00:00:00.000Z",
        last_seen_at: "2026-07-20T00:00:00.000Z",
        view_count: null,
        player_last_observed_at: null,
        next_last_observed_at: null,
        video_change_probability: null,
        like_count: null,
        comment_count: null,
      });
      state.enrichPending.add(contentKey);
    },
    beforeEnrichTaskLock(state, lockedContentKey) {
      if (lockedContentKey !== contentKey || movedToRetryWait) return;
      movedToRetryWait = true;
      state.enrichTaskStates.set(contentKey, {
        task_id: `player-refresh:${contentKey}`,
        status: "failed",
        attempts: 3,
        dispatch_generation: 1,
        lease_live: false,
        retry_due: false,
      });
    },
  });
  const fetched = [];

  await executeIncrementalVideo({
    plan: plan(),
    runId: "incremental:retry-race",
    startedAt: "2026-07-20T00:00:00.000Z",
    query: fixture.query,
    withTransaction: fixture.withTransaction,
    getChannelSnapshot: async () => ({
      async scanUploads() {
        return {
          playlist_id: "UUvideo",
          entries: [{ id: "known-anchor", position: 1, title: "Known" }],
          pages: 1,
          item_count: 1,
          parse_gap_count: 0,
          anchor_matched: true,
          matched_anchor_id: "known-anchor",
          stop_reason: "anchor_matched",
          terminal_reason: "anchor_matched",
          complete: true,
          raw: { engine: "youtubei.js@test" },
        };
      },
    }),
    fetchDetail: async (videoId) => {
      fetched.push(videoId);
      return detail(videoId, 123);
    },
  });

  assert.equal(fetched.includes("retry-race"), true);
  assert.deepEqual(
    (({ status, attempts }) => ({ status, attempts }))(
      fixture.state.enrichTaskStates.get(contentKey),
    ),
    { status: "failed", attempts: 3 },
  );
  assert.equal(
    fixture.state.contents.find((row) => row.content_key === contentKey).last_enriched_at,
    undefined,
  );
});

test("Video Clock commits a failed Task attempt before Publication reconciliation", async () => {
  const contentKey = "UCvideo:video:publication-failed-retry";
  const fixture = databaseFixture({ failPublication: true });
  fixture.state.contents.push({
    content_key: contentKey,
    channel_id: "UCvideo",
    source_content_id: "publication-failed-retry",
    content_type: "video",
    published_at: "2026-07-19T00:00:00.000Z",
    last_seen_at: "2026-07-20T00:00:00.000Z",
    view_count: null,
    player_last_observed_at: null,
    next_last_observed_at: null,
    video_change_probability: null,
    like_count: null,
    comment_count: null,
  });
  fixture.state.enrichPending.add(contentKey);
  fixture.state.enrichTaskStates.set(contentKey, {
    task_id: `player-refresh:${contentKey}`,
    status: "failed",
    attempts: 2,
    dispatch_generation: 1,
    lease_live: false,
    retry_due: true,
  });
  const rollbackTransactions = async (action) => {
    const pendingBefore = structuredClone(fixture.state.enrichPending);
    const tasksBefore = structuredClone(fixture.state.enrichTaskStates);
    try {
      return await fixture.withTransaction(action);
    } catch (error) {
      fixture.state.enrichPending.clear();
      for (const value of pendingBefore) fixture.state.enrichPending.add(value);
      fixture.state.enrichTaskStates.clear();
      for (const [key, value] of tasksBefore) fixture.state.enrichTaskStates.set(key, value);
      throw error;
    }
  };

  await assert.rejects(executeIncrementalVideo({
    plan: plan(),
    runId: "incremental:publication-failed-retry",
    startedAt: "2026-07-20T00:00:00.000Z",
    query: fixture.query,
    withTransaction: rollbackTransactions,
    getChannelSnapshot: async () => ({
      async scanUploads() {
        return {
          playlist_id: "UUvideo",
          entries: [{ id: "known-anchor", position: 1, title: "Known" }],
          pages: 1,
          item_count: 1,
          parse_gap_count: 0,
          anchor_matched: true,
          matched_anchor_id: "known-anchor",
          stop_reason: "anchor_matched",
          terminal_reason: "anchor_matched",
          complete: true,
          raw: { engine: "youtubei.js@test" },
        };
      },
    }),
    fetchDetail: async (videoId) => {
      if (videoId === "publication-failed-retry") {
        throw new Error("temporary Player timeout before Publication failure");
      }
      return detail(videoId, 123);
    },
  }), /simulated Publication failure/);

  assert.deepEqual(
    (({ status, attempts }) => ({ status, attempts }))(
      fixture.state.enrichTaskStates.get(contentKey),
    ),
    { status: "failed", attempts: 3 },
  );
});

test("Clock rollback does not sample a recent Video while its Enrich Worker lease is still live", async () => {
  const contentKey = "UCvideo:video:worker-leased";
  const fixture = databaseFixture({
    enrichMode: "clock",
    beforeTransaction(state) {
      state.contents.push({
        content_key: contentKey,
        channel_id: "UCvideo",
        source_content_id: "worker-leased",
        content_type: "video",
        published_at: "2026-07-19T00:00:00.000Z",
        last_seen_at: "2026-07-20T00:00:00.000Z",
        view_count: null,
        player_last_observed_at: null,
        next_last_observed_at: null,
        video_change_probability: null,
        like_count: null,
        comment_count: null,
      });
      state.enrichPending.add(contentKey);
      state.enrichLeased.add(contentKey);
    },
  });
  const fetched = [];

  await executeIncrementalVideo({
    plan: plan(),
    runId: "incremental:worker-lease-rollback",
    startedAt: "2026-07-20T00:00:00.000Z",
    query: fixture.query,
    withTransaction: fixture.withTransaction,
    getChannelSnapshot: async () => ({
      async scanUploads() {
        return {
          playlist_id: "UUvideo",
          entries: [{
            id: "known-anchor",
            position: 1,
            content_type: "video",
            title: "Known",
            published_day: "2026-07-10",
          }],
          pages: 1,
          item_count: 1,
          parse_gap_count: 0,
          anchor_matched: true,
          matched_anchor_id: "known-anchor",
          stop_reason: "anchor_matched",
          terminal_reason: "anchor_matched",
          complete: true,
          raw: { engine: "youtubei.js@test" },
        };
      },
    }),
    fetchDetail: async (videoId) => {
      fetched.push(videoId);
      return detail(videoId, 123);
    },
  });

  assert.equal(fetched.includes("worker-leased"), false);
  assert.equal(fixture.state.enrichPending.has(contentKey), true);
});

test("scheduled terminal access rechecks remain exclusive to the active Enrich owner", async () => {
  async function runScenario({ mode, schedule, videoId, publishedAt, fetchError = null }) {
    const contentKey = `UCvideo:video:${videoId}`;
    const fixture = databaseFixture({
      enrichMode: mode,
      beforeTransaction(state) {
        state.contents.push({
          content_key: contentKey,
          channel_id: "UCvideo",
          source_content_id: videoId,
          content_type: "video",
          published_at: publishedAt,
          last_seen_at: "2026-07-20T00:00:00.000Z",
          view_count: null,
          player_last_observed_at: null,
          next_last_observed_at: null,
          video_change_probability: null,
          like_count: null,
          comment_count: null,
          access_status: "private",
        });
        state.enrichTerminalSchedule.set(contentKey, schedule);
      },
    });
    const fetched = [];
    await executeIncrementalVideo({
      plan: {
        ...plan(),
        capacity: { ...plan().capacity, player_cap: 20, next_cap: 0 },
      },
      runId: `incremental:terminal-recheck:${mode}:${schedule}`,
      startedAt: "2026-07-20T00:00:00.000Z",
      query: fixture.query,
      withTransaction: fixture.withTransaction,
      getChannelSnapshot: async () => ({
        async scanUploads() {
          return {
            playlist_id: "UUvideo",
            entries: [{
              id: "known-anchor",
              position: 1,
              content_type: "video",
              title: "Known",
              published_day: "2026-07-10",
            }],
            pages: 1,
            item_count: 1,
            parse_gap_count: 0,
            anchor_matched: true,
            matched_anchor_id: "known-anchor",
            stop_reason: "anchor_matched",
            terminal_reason: "anchor_matched",
            complete: true,
            raw: { engine: "youtubei.js@test" },
          };
        },
      }),
      fetchDetail: async (fetchedVideoId) => {
        fetched.push(fetchedVideoId);
        if (fetchError) throw fetchError;
        return detail(fetchedVideoId, 123);
      },
    });
    return { contentKey, fetched, fixture };
  }

  const clockFuture = await runScenario({
    mode: "clock",
    schedule: "future",
    videoId: "terminal-clock-future",
    publishedAt: "2026-07-19T00:00:00.000Z",
  });
  const queueFuture = await runScenario({
    mode: "queue",
    schedule: "future",
    videoId: "terminal-queue-future",
    publishedAt: "2026-07-19T00:00:00.000Z",
  });
  const queueDue = await runScenario({
    mode: "queue",
    schedule: "due",
    videoId: "terminal-queue-due",
    publishedAt: "2026-01-01T00:00:00.000Z",
  });
  const clockDue = await runScenario({
    mode: "clock",
    schedule: "due",
    videoId: "terminal-clock-due",
    publishedAt: "2026-01-01T00:00:00.000Z",
  });
  const failedClockDue = await runScenario({
    mode: "clock",
    schedule: "due",
    videoId: "terminal-clock-failed",
    publishedAt: "2026-01-01T00:00:00.000Z",
    fetchError: new Error("temporary Player timeout"),
  });

  assert.equal(clockFuture.fetched.includes("terminal-clock-future"), false);
  assert.equal(queueFuture.fetched.includes("terminal-queue-future"), false);
  assert.equal(queueDue.fetched.includes("terminal-queue-due"), false);
  assert.equal(clockDue.fetched.filter((videoId) => videoId === "terminal-clock-due").length, 1);
  assert.equal(clockDue.fixture.state.enrichTerminalSchedule.has(clockDue.contentKey), false);
  assert.deepEqual(
    (({ status, attempts }) => ({ status, attempts }))(
      clockDue.fixture.state.enrichTaskStates.get(clockDue.contentKey),
    ),
    { status: "done", attempts: 0 },
  );
  assert.equal(
    failedClockDue.fetched.filter((videoId) => videoId === "terminal-clock-failed").length,
    1,
  );
  assert.equal(failedClockDue.fixture.state.enrichTerminalSchedule.has(failedClockDue.contentKey), false);
  assert.equal(failedClockDue.fixture.state.enrichPending.has(failedClockDue.contentKey), true);
  assert.deepEqual(
    (({ status, attempts }) => ({ status, attempts }))(
      failedClockDue.fixture.state.enrichTaskStates.get(failedClockDue.contentKey),
    ),
    { status: "failed", attempts: 1 },
  );
});

test("shared Video detail storage preserves an explicit terminal access evidence source", async () => {
  let update = null;
  const client = {
    async query(sql, params) {
      update = { sql, params };
      return { rowCount: 1, rows: [] };
    },
  };

  await applyIncrementalVideoDetail(client, {
    row: {
      content_key: "UCvideo:video:private-error",
      channel_id: "UCvideo",
      source_content_id: "private-error",
      content_type: "video",
      content_type_source: "youtube_uploads",
      view_count: null,
      like_count: null,
      comment_count: null,
      video_change_probability: null,
    },
    detail: {
      access_status: "private",
      access_status_source: "yt_dlp_detail",
    },
    observedAt: "2026-07-20T00:00:00.000Z",
    detailMetadataKey: "content_enrich_detail",
  });

  assert.equal(update.params[27], "private");
  assert.equal(update.params[28], "yt_dlp_detail");
});

test("shared Video detail storage persists disabled comments as an authoritative zero", async () => {
  let update = null;
  const client = {
    async query(sql, params) {
      update = { sql, params };
      return { rowCount: 1, rows: [] };
    },
  };

  await applyIncrementalVideoDetail(client, {
    row: {
      content_key: "UCvideo:video:comments-disabled",
      channel_id: "UCvideo",
      source_content_id: "comments-disabled",
      content_type: "video",
      content_type_source: "youtube_uploads",
      view_count: null,
      like_count: null,
      comment_count: 12,
      comments_disabled: false,
      video_change_probability: null,
    },
    detail: {
      comments_disabled: true,
      comment_count: null,
      comment_count_status: "disabled",
      comment_count_source: "youtubejs_comments",
      access_status: "public",
      access_status_source: "youtubejs_player",
    },
    observedAt: "2026-08-24T00:00:00.000Z",
  });

  assert.equal(update.params[4], 0);
  assert.equal(update.params[5], true);
  assert.match(
    update.sql,
    /WHEN \$35::boolean AND \$6::boolean THEN 0/,
  );
});

test("Video detail falls back to yt-dlp when YouTube.js is challenged", async () => {
  const calls = [];
  const result = await fetchIncrementalVideoDetail("new-video", {
    fetchYoutubeJs: async () => {
      calls.push("youtubejs");
      throw new Error("YouTube bot challenge HTTP 200 for /youtubei/v1/player");
    },
    fetchYtDlp: async (videoId, url) => {
      calls.push("yt-dlp");
      assert.equal(videoId, "new-video");
      assert.equal(url, "https://www.youtube.com/watch?v=new-video");
      return detail(videoId, 42);
    },
  });

  assert.deepEqual(calls, ["youtubejs", "yt-dlp"]);
  assert.equal(result.view_count, 42);
});

test("Video detail falls back to yt-dlp when YouTube.js returns incomplete facts", async () => {
  const calls = [];
  const result = await fetchIncrementalVideoDetail("partial-video", {
    fetchYoutubeJs: async () => {
      calls.push("youtubejs");
      return {
        id: "partial-video",
        title: "YouTube.js title",
        description: "Long description collected by YouTube.js",
        description_status: "exact",
        description_source: "youtubejs_player",
        published_at: "2026-07-19T12:34:56.000Z",
        published_at_precision: "second",
        published_at_source: "youtubejs_microformat",
        comment_count: 2,
        comment_count_status: "exact",
        comment_count_source: "youtubejs_comments",
        comments_first_page: {
          returned_count: 1,
          comments: [{ comment_id: "comment-partial-video", text: "Useful comment" }],
        },
        access_status: "public",
        access_status_source: "youtubejs_playability",
        content_type_signals: {
          source: "youtubei_player",
          canonical_url: "https://www.youtube.com/watch?v=partial-video",
          is_shorts_eligible: false,
          is_live_content: false,
        },
      };
    },
    fetchYtDlp: async () => {
      calls.push("yt-dlp");
      return {
        ...detail("partial-video", 84),
        title: null,
        description: null,
        description_status: "unresolved",
        description_source: null,
        published_at: "2026-07-18T23:00:00.000Z",
        published_at_precision: "second",
        published_at_source: "yt_dlp_timestamp",
        comment_count: null,
        comment_count_status: "unresolved",
        comment_count_source: null,
        comments_first_page: null,
        access_status: "unknown",
        access_status_source: null,
        ytdlp_client: "web",
        extractor_version: "yt-dlp@test",
      };
    },
  });

  assert.deepEqual(calls, ["youtubejs", "yt-dlp"]);
  assert.equal(result.view_count, 84);
  assert.equal(result.duration_seconds, 90);
  assert.equal(result.title, "YouTube.js title");
  assert.equal(result.description, "Long description collected by YouTube.js");
  assert.equal(result.published_at, "2026-07-19T12:34:56.000Z");
  assert.equal(result.published_at_precision, "second");
  assert.equal(
    result.publication_evidence_conflict?.reason_code,
    "equal_quality_publication_conflict",
  );
  assert.equal(
    result.publication_evidence_conflict?.resolution?.reason_code,
    "equal_quality_conflict_current_retained",
  );
  assert.equal(result.comment_count, 2);
  assert.equal(result.comments_first_page.returned_count, 1);
  assert.equal(result.access_status, "public");
  assert.equal(result.content_type_signals.source, "youtubei_player");
  assert.equal(result.ytdlp_client, "web");
});

test("Video detail verifies YouTube.js disabled comments and keeps yt-dlp visible comments", async () => {
  const calls = [];
  const result = await fetchIncrementalVideoDetail("comments-visible-in-fallback", {
    fetchYoutubeJs: async () => {
      calls.push("youtubejs");
      return {
        ...detail("comments-visible-in-fallback", 42),
        comment_count: 0,
        comment_count_status: "disabled",
        comment_count_source: "youtubejs_comments",
        comments_status_source: "youtubejs_comments",
        comments_disabled: true,
        comments_first_page: {
          version: 1,
          collected_at: "2026-08-25T00:00:00.000Z",
          sort: "TOP_COMMENTS",
          total_count: 0,
          returned_count: 0,
          comments: [],
        },
      };
    },
    fetchYtDlp: async () => {
      calls.push("yt-dlp");
      return {
        ...detail("comments-visible-in-fallback", 42),
        comment_count: 19,
        comment_count_status: "exact",
        comment_count_source: "yt_dlp",
        comments_status_source: "yt_dlp",
        comments_disabled: false,
        comments_first_page: {
          version: 1,
          collected_at: "2026-08-25T00:00:01.000Z",
          sort: "TOP_COMMENTS",
          total_count: 19,
          returned_count: 15,
          comments: [{ comment_id: "visible-comment", text: "Visible comment" }],
        },
        comments_first_page_status: "collected",
        comments_first_page_source: "yt_dlp_top_comments",
        ytdlp_client: "web",
      };
    },
  });

  assert.deepEqual(calls, ["youtubejs", "yt-dlp"]);
  assert.equal(result.comments_disabled, false);
  assert.equal(result.comment_count, 19);
  assert.equal(result.comment_count_status, "exact");
  assert.equal(result.comment_count_source, "yt_dlp");
  assert.equal(result.comments_status_source, "yt_dlp");
  assert.equal(result.comments_first_page.returned_count, 15);
  assert.equal(result.comments_first_page_source, "yt_dlp_top_comments");
});

test("Video detail does not let a yt-dlp disabled result erase visible YouTube.js comments", async () => {
  const result = await fetchIncrementalVideoDetail("comments-visible-in-youtubejs", {
    fetchYoutubeJs: async () => ({
      ...detail("comments-visible-in-youtubejs", 42),
      view_count: null,
      view_count_text: null,
      comment_count: 12,
      comment_count_status: "exact",
      comment_count_source: "youtubejs_comments",
      comments_status_source: "youtubejs_comments",
      comments_disabled: false,
      comments_first_page: {
        version: 1,
        collected_at: "2026-08-25T00:00:00.000Z",
        sort: "TOP_COMMENTS",
        total_count: 12,
        returned_count: 1,
        comments: [{ comment_id: "youtubejs-comment", text: "Visible comment" }],
      },
      comments_first_page_status: "collected",
      comments_first_page_source: "youtubejs_comments",
    }),
    fetchYtDlp: async () => ({
      ...detail("comments-visible-in-youtubejs", 84),
      comment_count: 0,
      comment_count_status: "disabled",
      comment_count_source: "yt_dlp",
      comments_status_source: "yt_dlp",
      comments_disabled: true,
      comments_first_page: {
        version: 1,
        collected_at: "2026-08-25T00:00:01.000Z",
        sort: "TOP_COMMENTS",
        total_count: 0,
        returned_count: 0,
        comments: [],
      },
      comments_first_page_status: "disabled",
      comments_first_page_source: "yt_dlp_top_comments",
      ytdlp_client: "web",
    }),
  });

  assert.equal(result.view_count, 84);
  assert.equal(result.comments_disabled, false);
  assert.equal(result.comment_count, 12);
  assert.equal(result.comment_count_status, "exact");
  assert.equal(result.comment_count_source, "youtubejs_comments");
  assert.equal(result.comments_status_source, "youtubejs_comments");
  assert.equal(result.comments_first_page.returned_count, 1);
  assert.equal(result.comments_first_page_source, "youtubejs_comments");
});

test("Video detail fallback failure keeps the usable YouTube.js partial evidence", async () => {
  const fallbackFailure = new Error("yt-dlp parser failed");

  await assert.rejects(
    fetchIncrementalVideoDetail("partial-fallback-failure", {
      fetchYoutubeJs: async () => ({
        id: "partial-fallback-failure",
        title: "Partial title",
        published_at: "2026-07-19T00:00:00.000Z",
        published_at_precision: "second",
        access_status: "public",
      }),
      fetchYtDlp: async () => {
        throw fallbackFailure;
      },
    }),
    (error) => {
      assert(error instanceof AggregateError);
      assert.equal(error.errors.at(-1), fallbackFailure);
      assert.equal(error.partial_detail.title, "Partial title");
      return true;
    },
  );
});

test("failed yt-dlp verification does not persist an unverified YouTube.js disabled result", async () => {
  await assert.rejects(
    fetchIncrementalVideoDetail("comments-verification-failure", {
      fetchYoutubeJs: async () => ({
        ...detail("comments-verification-failure", 42),
        view_count: null,
        view_count_text: null,
        comment_count: 0,
        comment_count_status: "disabled",
        comment_count_source: "youtubejs_comments",
        comments_status_source: "youtubejs_comments",
        comments_disabled: true,
        comments_first_page: {
          version: 1,
          collected_at: "2026-08-25T00:00:00.000Z",
          sort: "TOP_COMMENTS",
          total_count: 0,
          returned_count: 0,
          comments: [],
        },
      }),
      fetchYtDlp: async () => {
        throw new Error("yt-dlp verification failed");
      },
    }),
    (error) => {
      assert(error instanceof AggregateError);
      assert.equal(error.partial_detail.title, "Title comments-verification-failure");
      assert.equal(error.partial_detail.comment_count, null);
      assert.equal(error.partial_detail.comment_count_status, "unresolved");
      assert.equal(error.partial_detail.comments_disabled, null);
      assert.equal(error.partial_detail.comments_first_page, null);
      return true;
    },
  );
});

test("an incomplete unlisted detail is not downgraded by a public yt-dlp patch", async () => {
  const calls = [];
  const result = await fetchIncrementalVideoDetail("unlisted-partial", {
    fetchYoutubeJs: async () => {
      calls.push("youtubejs");
      return {
        id: "unlisted-partial",
        title: "Unlisted title",
        access_status: "unlisted",
        access_status_source: "youtubejs_microformat",
        availability: "unlisted",
        is_unlisted: true,
        published_at: "2026-07-19T00:00:00.000Z",
        published_at_precision: "second",
        content_type_signals: {
          source: "youtubei_player",
          canonical_url: "https://www.youtube.com/watch?v=unlisted-partial",
          is_shorts_eligible: false,
          is_live_content: false,
        },
      };
    },
    fetchYtDlp: async () => {
      calls.push("yt-dlp");
      return {
        ...detail("unlisted-partial", 91),
        access_status: "public",
        access_status_source: "yt_dlp_playability",
        availability: "public",
        ytdlp_client: "web",
      };
    },
  });

  assert.deepEqual(calls, ["youtubejs", "yt-dlp"]);
  assert.equal(result.access_status, "unlisted");
  assert.equal(result.access_status_source, "youtubejs_microformat");
  assert.equal(result.availability, "unlisted");
  assert.equal(result.view_count, 91);
  assert.equal(result.duration_seconds, 90);
});

test("Video detail propagates cancellation and does not start a fallback after abort", async () => {
  const controller = new AbortController();
  const leaseLost = new Error("Enrich lease lost");
  let forwardedSignal = null;
  let fallbackCalls = 0;

  await assert.rejects(
    fetchIncrementalVideoDetail("cancelled-video", {
      signal: controller.signal,
      fetchYoutubeJs: async (_videoId, { signal } = {}) => {
        forwardedSignal = signal ?? null;
        controller.abort(leaseLost);
        return { id: "cancelled-video", published_at: "2026-07-19T00:00:00.000Z" };
      },
      fetchYtDlp: async () => {
        fallbackCalls += 1;
        return detail("cancelled-video", 84);
      },
    }),
    (error) => error === leaseLost,
  );

  assert.equal(forwardedSignal, controller.signal);
  assert.equal(fallbackCalls, 0);
});

test("Video discovery forwards cancellation through the real capture loop and stops the batch", async () => {
  const fixture = databaseFixture();
  const controller = new AbortController();
  const leaseLost = new Error("Enrich lease lost during discovery detail");
  const calls = [];
  let forwardedSignal = null;

  const operation = () => executeIncrementalVideo({
    plan: plan(),
    runId: "incremental:detail-cancelled",
    startedAt: "2026-07-20T00:00:00.000Z",
    query: fixture.query,
    withTransaction: fixture.withTransaction,
    getChannelSnapshot: async () => ({
      async scanUploads() {
        return {
          playlist_id: "UUvideo",
          entries: [
            { id: "cancel-first", position: 1, title: "Cancel first" },
            { id: "must-not-run", position: 2, title: "Must not run" },
            { id: "known-anchor", position: 3, title: "Known anchor" },
          ],
          pages: 1,
          item_count: 3,
          parse_gap_count: 0,
          anchor_matched: true,
          matched_anchor_id: "known-anchor",
          stop_reason: "anchor_matched",
          terminal_reason: "anchor_matched",
          complete: true,
          raw: { engine: "youtubei.js@test" },
        };
      },
    }),
    fetchDetail: async (videoId, { signal } = {}) => {
      calls.push(videoId);
      forwardedSignal = signal ?? null;
      if (videoId === "cancel-first") controller.abort(leaseLost);
      return detail(videoId, 84);
    },
  });

  await assert.rejects(
    runWithChannelExecution({ abort_signal: controller.signal }, operation),
    (error) => error === leaseLost,
  );
  assert.equal(forwardedSignal, controller.signal);
  assert.deepEqual(calls, ["cancel-first"]);
});

test("Video discovery falls back to the migration yt-dlp Uploads collector without losing anchor safety", async () => {
  const fixture = databaseFixture();
  const youtubeJsFailure = new Error("YouTube.js playlist request failed");
  const fallbackCalls = [];

  const result = await executeIncrementalVideo({
    plan: plan(),
    runId: "incremental:uploads-fallback",
    startedAt: "2026-07-20T00:00:00.000Z",
    query: fixture.query,
    withTransaction: fixture.withTransaction,
    getChannelSnapshot: async () => {
      throw youtubeJsFailure;
    },
    fetchUploads: async (channelId, limit) => {
      fallbackCalls.push({ channelId, limit });
      return {
        channel_id: channelId,
        playlist_id: "UUvideo",
        entries: [
          {
            video_id: "fallback-new-video",
            position: 1,
            title: "Fallback new video",
            url: "https://www.youtube.com/watch?v=fallback-new-video",
            thumbnail_url: "https://img.example/fallback-new-video.jpg",
            duration_seconds: 90,
            view_count_text: "84",
            published_at: "2026-07-19T12:00:00.000Z",
            published_at_precision: "second",
            content_type: "video",
            is_live: false,
            is_upcoming: false,
          },
          {
            video_id: "known-anchor",
            position: 2,
            title: "Known anchor",
            url: "https://www.youtube.com/watch?v=known-anchor",
            published_at: "2026-07-10T00:00:00.000Z",
            published_at_precision: "second",
            content_type: "video",
            is_live: false,
            is_upcoming: false,
          },
        ],
        activity_parse_gap_count: 0,
        scan: {
          pages: null,
          inspected_count: 2,
          parse_gap_count: 0,
          selected_count: 2,
          stop_reason: "max_items",
          terminal_reason: "max_items",
          complete: false,
        },
        raw: { engine: "yt-dlp@test" },
      };
    },
    fetchDetail: async (videoId) => detail(videoId, 84),
  });

  assert.equal(result.outcome, "complete");
  assert.equal(result.first_seen_count, 1);
  assert.deepEqual(fallbackCalls, [{ channelId: "UCvideo", limit: 50 }]);
  assert.deepEqual(fixture.state.extractorVersions[0], {
    youtubejs: null,
    yt_dlp: "yt-dlp@test",
  });
  assert.deepEqual(fixture.state.cursorUpdates[0].anchorVideoIds.slice(0, 2), [
    "fallback-new-video",
    "known-anchor",
  ]);
  assert.deepEqual(JSON.parse(fixture.state.cursorUpdates[0].sourceCursor), {
    playlist_id: "UUvideo",
    matched_anchor_id: "known-anchor",
    crossed_anchor_ids: [],
    terminal_reason: "anchor_matched",
  });
  assert.equal(
    fixture.state.outbox[0].payload.discovery.payload.stop_reason,
    "anchor_matched",
  );
});

test("Video discovery also falls back when YouTube.js reports a pagination error", async () => {
  const fixture = databaseFixture();
  const paginationError = new Error("YouTube.js continuation failed");
  let fallbackCalls = 0;

  const result = await executeIncrementalVideo({
    plan: plan(),
    runId: "incremental:uploads-pagination-fallback",
    startedAt: "2026-07-20T00:00:00.000Z",
    query: fixture.query,
    withTransaction: fixture.withTransaction,
    getChannelSnapshot: async () => ({
      async scanUploads() {
        return {
          playlist_id: "UUvideo",
          entries: [{ id: "partial-page-video", position: 1 }],
          pages: 1,
          item_count: 1,
          parse_gap_count: 0,
          anchor_matched: false,
          matched_anchor_id: null,
          stop_reason: "pagination_error",
          terminal_reason: "pagination_error",
          complete: false,
          error: paginationError,
          raw: { engine: "youtubei.js@test" },
        };
      },
    }),
    fetchUploads: async (channelId) => {
      fallbackCalls += 1;
      return {
        channel_id: channelId,
        playlist_id: "UUvideo",
        entries: [{
          video_id: "known-anchor",
          position: 1,
          title: "Known anchor",
          published_at: "2026-07-10T00:00:00.000Z",
          content_type: "video",
        }],
        activity_parse_gap_count: 0,
        scan: {
          parse_gap_count: 0,
          stop_reason: "max_items",
          terminal_reason: "max_items",
          complete: false,
        },
        raw: { engine: "yt-dlp@test" },
      };
    },
    fetchDetail: async (videoId) => detail(videoId, 100),
  });

  assert.equal(result.outcome, "complete");
  assert.equal(fallbackCalls, 1);
  assert.equal(
    JSON.parse(fixture.state.cursorUpdates[0].sourceCursor).matched_anchor_id,
    "known-anchor",
  );
});

test("yt-dlp Uploads fallback stays Partial when its bounded result does not reach an anchor", async () => {
  const fixture = databaseFixture();
  let detailCalls = 0;

  const result = await executeIncrementalVideo({
    plan: plan(),
    runId: "incremental:uploads-fallback-no-anchor",
    startedAt: "2026-07-20T00:00:00.000Z",
    query: fixture.query,
    withTransaction: fixture.withTransaction,
    getChannelSnapshot: async () => {
      throw new Error("YouTube.js playlist request failed");
    },
    fetchUploads: async (channelId) => ({
      channel_id: channelId,
      playlist_id: "UUvideo",
      entries: [{
        video_id: "fallback-without-anchor",
        position: 1,
        title: "Fallback without anchor",
        published_at: "2026-07-19T00:00:00.000Z",
        content_type: "video",
      }],
      activity_parse_gap_count: 0,
      scan: {
        parse_gap_count: 0,
        stop_reason: "max_items",
        terminal_reason: "max_items",
        complete: false,
      },
      raw: { engine: "yt-dlp@test" },
    }),
    fetchDetail: async (videoId) => {
      detailCalls += 1;
      return detail(videoId, 84);
    },
  });

  assert.equal(result.outcome, "partial");
  assert.equal(result.first_seen_count, 0);
  assert.equal(detailCalls, 0);
  assert.equal(fixture.state.cursorUpdates[0].anchorVideoIds, null);
  assert.equal(fixture.state.cursorUpdates[0].sourceCursor, null);
  assert.equal(
    fixture.state.outbox[0].payload.discovery.payload.stop_reason,
    "max_items",
  );
});

test("Video discovery does not persist an upcoming live before it starts", async () => {
  const fixture = databaseFixture();
  const result = await executeIncrementalVideo({
    plan: plan(),
    runId: "incremental:upcoming-live",
    startedAt: "2026-07-20T00:00:00.000Z",
    query: fixture.query,
    withTransaction: fixture.withTransaction,
    getChannelSnapshot: async () => ({
      async scanUploads() {
        return {
          playlist_id: "UUvideo",
          entries: [
            {
              id: "upcoming-live",
              position: 1,
              content_type: "live",
              is_upcoming: true,
              title: "Scheduled",
              published_day: "2026-07-20",
            },
            {
              id: "known-anchor",
              position: 2,
              content_type: "video",
              title: "Known",
              published_day: "2026-07-10",
            },
          ],
          pages: 1,
          item_count: 2,
          parse_gap_count: 0,
          anchor_matched: true,
          matched_anchor_id: "known-anchor",
          stop_reason: "anchor_matched",
          terminal_reason: "anchor_matched",
          complete: true,
          raw: { engine: "youtubei.js@test" },
        };
      },
    }),
    fetchDetail: async (videoId) => videoId === "upcoming-live"
      ? { id: videoId, is_upcoming: true, live_status: "is_upcoming" }
      : detail(videoId, 100),
  });

  assert.equal(result.first_seen_count, 0);
  assert.equal(
    fixture.state.contents.some((row) => row.source_content_id === "upcoming-live"),
    false,
  );
  assert.deepEqual(
    (({ disposition, next_attempt_at, result_json }) => ({
      disposition,
      next_attempt_at,
      disposition_evidence: result_json.disposition,
      upload_evidence: result_json.flat,
    }))(fixture.state.candidateRows[0]),
    {
      disposition: "terminal_excluded",
      next_attempt_at: "2026-07-21T00:00:00.000Z",
      disposition_evidence: {
        version: "video-disposition-v1",
        kind: "terminal_excluded",
        reason_code: "upcoming_live",
        retry_class: "low_frequency_access_recheck",
        retryable: false,
        observed_at: "2026-07-20T00:00:00.000Z",
        next_attempt_at: "2026-07-21T00:00:00.000Z",
      },
      upload_evidence: {
        id: "upcoming-live",
        position: 1,
        content_type: "live",
        is_upcoming: true,
        title: "Scheduled",
        published_day: "2026-07-20",
      },
    },
  );
  assert.deepEqual(
    fixture.state.outbox[0].payload.discovery.payload.dispositions,
    [{
      video_id: "upcoming-live",
      kind: "terminal_excluded",
      reason_code: "upcoming_live",
      retry_class: "low_frequency_access_recheck",
    }],
  );
});

test("Video discovery honors an upcoming state first revealed by Player detail", async () => {
  const fixture = databaseFixture();
  const result = await executeIncrementalVideo({
    plan: plan(),
    runId: "incremental:detail-upcoming-live",
    startedAt: "2026-07-20T00:00:00.000Z",
    query: fixture.query,
    withTransaction: fixture.withTransaction,
    getChannelSnapshot: async () => ({
      async scanUploads() {
        return {
          playlist_id: "UUvideo",
          entries: [{ id: "detail-upcoming", position: 1, title: "Scheduled" }],
          pages: 1,
          item_count: 1,
          parse_gap_count: 0,
          anchor_matched: true,
          matched_anchor_id: "known-anchor",
          stop_reason: "anchor_matched",
          terminal_reason: "anchor_matched",
          complete: true,
          raw: { engine: "youtubei.js@test" },
        };
      },
    }),
    fetchDetail: async (videoId) => ({
      ...detail(videoId, 0, { contentType: "live" }),
      is_upcoming: true,
      live_status: "is_upcoming",
      live_scheduled_at: "2026-07-21T12:00:00.000Z",
      content_type_signals: {
        source: "youtubei_player",
        canonical_url: `https://www.youtube.com/watch?v=${videoId}`,
        is_shorts_eligible: false,
        is_live_content: true,
        is_live: false,
        is_upcoming: true,
        is_live_now: false,
      },
    }),
  });

  assert.equal(result.outcome, "complete");
  assert.equal(result.first_seen_count, 0);
  assert.equal(
    fixture.state.contents.some((row) => row.source_content_id === "detail-upcoming"),
    false,
  );
  assert.equal(fixture.state.candidateRows[0].disposition, "terminal_excluded");
  assert.equal(
    fixture.state.candidateRows[0].result_json.disposition.reason_code,
    "upcoming_live",
  );
});

test("Video discovery does not persist a live broadcast while it is in progress", async () => {
  const fixture = databaseFixture();
  for (const [index, row] of fixture.state.contents.entries()) {
    row.published_at = `2026-01-0${index + 1}T00:00:00.000Z`;
    row.published_at_status = "exact";
    row.published_at_precision = "second";
    row.published_at_source = "test_existing_detail";
  }
  const fetched = [];
  const result = await executeIncrementalVideo({
    plan: plan(),
    runId: "incremental:active-live",
    startedAt: "2026-07-20T00:00:00.000Z",
    query: fixture.query,
    withTransaction: fixture.withTransaction,
    getChannelSnapshot: async () => ({
      async scanUploads() {
        return {
          playlist_id: "UUvideo",
          entries: [
            {
              id: "active-live",
              position: 1,
              content_type: "live",
              is_live: true,
              title: "Live now",
              published_day: "2026-07-20",
            },
            {
              id: "known-anchor",
              position: 2,
              content_type: "video",
              title: "Known",
              published_day: "2026-07-10",
            },
          ],
          pages: 1,
          item_count: 2,
          parse_gap_count: 0,
          anchor_matched: true,
          matched_anchor_id: "known-anchor",
          stop_reason: "anchor_matched",
          terminal_reason: "anchor_matched",
          complete: true,
          raw: { engine: "youtubei.js@test" },
        };
      },
    }),
    fetchDetail: async (videoId) => {
      fetched.push(videoId);
      return videoId === "active-live" ? {
          ...detail(videoId, 100),
          is_live: true,
          live_status: "is_live",
          live_started_at: "2026-07-20T00:00:00.000Z",
        }
        : detail(videoId, 100);
    },
  });

  assert.equal(result.first_seen_count, 0);
  assert.equal(
    fixture.state.contents.some((row) => row.source_content_id === "active-live"),
    false,
  );
  assert.equal(fetched.includes("active-live"), false);
  assert.equal(fixture.state.candidateRows[0].disposition, "terminal_excluded");
  assert.equal(
    fixture.state.candidateRows[0].result_json.disposition.reason_code,
    "live_in_progress",
  );
  assert.equal(result.lifecycle_status, "active");
  assert.equal(fixture.state.channelStatus, "active");
  assert.equal(
    fixture.state.outbox[0].payload.activity_evidence.uncertain_content_count,
    1,
  );
});

test("a throttled live Candidate still blocks Incremental dormancy from current Uploads", async () => {
  const fixture = databaseFixture();
  for (const [index, row] of fixture.state.contents.entries()) {
    row.published_at = `2026-01-0${index + 1}T00:00:00.000Z`;
    row.published_at_status = "exact";
    row.published_at_precision = "second";
    row.published_at_source = "test_existing_detail";
  }
  fixture.state.candidateRows.push({
    candidate_id: "1",
    run_id: "incremental:prior-live",
    channel_id: "UCvideo",
    source_content_id: "still-live",
    disposition: "terminal_excluded",
    next_attempt_at: "2026-07-21T00:00:00.000Z",
    result_json: {
      disposition: {
        kind: "terminal_excluded",
        reason_code: "live_in_progress",
      },
    },
  });
  const fetched = [];
  const result = await executeIncrementalVideo({
    plan: { ...plan(), job_id: "incremental__throttled_live" },
    runId: "incremental:throttled-live",
    startedAt: "2026-07-20T01:00:00.000Z",
    query: fixture.query,
    withTransaction: fixture.withTransaction,
    getChannelSnapshot: async () => ({
      async scanUploads() {
        return {
          playlist_id: "UUvideo",
          entries: [
            {
              id: "still-live",
              position: 1,
              content_type: "live",
              is_live: true,
              title: "Still live",
            },
            { id: "known-anchor", position: 2, title: "Known" },
          ],
          pages: 1,
          item_count: 2,
          parse_gap_count: 0,
          anchor_matched: true,
          matched_anchor_id: "known-anchor",
          stop_reason: "anchor_matched",
          terminal_reason: "anchor_matched",
          complete: true,
          raw: { engine: "youtubei.js@test" },
        };
      },
    }),
    fetchDetail: async (videoId) => {
      fetched.push(videoId);
      return detail(videoId, 100);
    },
  });

  assert.equal(fetched.includes("still-live"), false);
  assert.equal(result.lifecycle_status, "active");
  assert.equal(fixture.state.channelStatus, "active");
  assert.equal(fixture.state.candidateRows.length, 1);
  assert.equal(
    fixture.state.outbox[0].payload.activity_evidence.uncertain_content_count,
    1,
  );
});

test("Video discovery stores an unlisted detail without treating it as public", async () => {
  const fixture = databaseFixture();
  const result = await executeIncrementalVideo({
    plan: plan(),
    runId: "incremental:unlisted-video",
    startedAt: "2026-07-20T00:00:00.000Z",
    query: fixture.query,
    withTransaction: fixture.withTransaction,
    getChannelSnapshot: async () => ({
      async scanUploads() {
        return {
          playlist_id: "UUvideo",
          entries: [
            { id: "unlisted-video", position: 1, content_type: "video", published_day: "2026-07-19" },
            { id: "known-anchor", position: 2, content_type: "video", published_day: "2026-07-10" },
          ],
          pages: 1,
          item_count: 2,
          parse_gap_count: 0,
          anchor_matched: true,
          matched_anchor_id: "known-anchor",
          stop_reason: "anchor_matched",
          terminal_reason: "anchor_matched",
          complete: true,
          raw: { engine: "youtubei.js@test" },
        };
      },
    }),
    fetchDetail: async (videoId) => ({
      ...detail(videoId, 100),
      availability: videoId === "unlisted-video" ? "unlisted" : "public",
      access_status: videoId === "unlisted-video" ? "unlisted" : "public",
      playability_status: "OK",
    }),
  });

  assert.equal(result.outcome, "complete");
  assert.equal(
    fixture.state.contents.find((row) => row.source_content_id === "unlisted-video").access_status,
    "unlisted",
  );
  assert.deepEqual(
    (({ content_type, type_status, detail_status, disposition, next_attempt_at, result_json }) => ({
      content_type,
      type_status,
      detail_status,
      disposition,
      next_attempt_at,
      disposition_evidence: result_json.disposition,
      stored_content_key: result_json.content_key,
    }))(fixture.state.candidateRows[0]),
    {
      content_type: "video",
      type_status: "resolved",
      detail_status: "done",
      disposition: "stored",
      next_attempt_at: null,
      disposition_evidence: {
        version: "video-disposition-v1",
        kind: "stored",
        reason_code: "content_stored",
        retry_class: null,
        retryable: false,
        observed_at: "2026-07-20T00:00:00.000Z",
        next_attempt_at: null,
      },
      stored_content_key: "UCvideo:video:unlisted-video",
    },
  );
  assert.deepEqual(
    fixture.state.outbox[0].payload.discovery.payload.dispositions,
    [{
      video_id: "unlisted-video",
      kind: "stored",
      reason_code: "content_stored",
      retry_class: null,
    }],
  );
});

test("an incomplete Uploads scan does not advance the incremental cursor", async () => {
  const fixture = databaseFixture();
  const fetched = [];
  const result = await executeIncrementalVideo({
    plan: plan(),
    runId: "incremental:incomplete-scan",
    startedAt: "2026-07-20T00:00:00.000Z",
    query: fixture.query,
    withTransaction: fixture.withTransaction,
    getChannelSnapshot: async () => ({
      async scanUploads() {
        return {
          playlist_id: "UUvideo",
          entries: [
            { id: "new-1", position: 1, content_type: "video", published_day: "2026-07-20" },
            { id: "new-2", position: 2, content_type: "video", published_day: "2026-07-19" },
          ],
          pages: 1,
          item_count: 2,
          parse_gap_count: 0,
          anchor_matched: false,
          matched_anchor_id: null,
          stop_reason: "max_items",
          terminal_reason: "max_items",
          complete: false,
          raw: { engine: "youtubei.js@test" },
        };
      },
    }),
    fetchDetail: async (videoId) => {
      fetched.push(videoId);
      return detail(videoId, 100);
    },
  });

  assert.equal(result.outcome, "partial");
  assert.equal(result.first_seen_count, 0);
  assert.deepEqual(fetched, []);
  assert.deepEqual(
    fixture.state.candidateRows.map((row) => ({
      video_id: row.source_content_id,
      disposition: row.disposition,
      reason_code: row.result_json.disposition.reason_code,
    })),
    [
      {
        video_id: "new-1",
        disposition: "deferred",
        reason_code: "discovery_scan_incomplete",
      },
      {
        video_id: "new-2",
        disposition: "deferred",
        reason_code: "discovery_scan_incomplete",
      },
    ],
  );
  const incompleteDiscovery = fixture.state.outbox[0].payload.discovery.payload;
  assert.equal(incompleteDiscovery.discovered_count, 2);
  assert.equal(incompleteDiscovery.deferred_count, 2);
  assert.deepEqual(
    incompleteDiscovery.dispositions.map((item) => item.video_id),
    ["new-1", "new-2"],
  );
  assert.equal(
    incompleteDiscovery.stored_count
      + incompleteDiscovery.deferred_count
      + incompleteDiscovery.terminal_excluded_count,
    incompleteDiscovery.discovered_count,
  );
  assert.equal(fixture.state.cursorUpdates.length, 1);
  assert.equal(fixture.state.cursorUpdates[0].anchorVideoIds, null);
  assert.equal(fixture.state.cursorUpdates[0].sourceCursor, null);

  let secondScanAnchors = null;
  await executeIncrementalVideo({
    plan: {
      ...plan(),
      job_id: "incremental__UCvideo__20260721__clock_8__hash",
      plan_id: "bb4ca796-c784-4c7d-b1cf-7a46a8944575",
      plan_day: "2026-07-21",
      scheduled_at: "2026-07-21T13:25:40.000Z",
    },
    runId: "incremental:incomplete-scan-retry",
    startedAt: "2026-07-21T00:00:00.000Z",
    query: fixture.query,
    withTransaction: fixture.withTransaction,
    getChannelSnapshot: async () => ({
      async scanUploads(options) {
        secondScanAnchors = options.anchors;
        return {
          playlist_id: "UUvideo",
          entries: [
            { id: "missed-between", position: 1, content_type: "video", published_day: "2026-07-20" },
            { id: "new-1", position: 2, content_type: "video", published_day: "2026-07-20" },
            { id: "known-anchor", position: 3, content_type: "video", published_day: "2026-07-10" },
          ],
          pages: 1,
          item_count: 3,
          parse_gap_count: 0,
          anchor_matched: true,
          matched_anchor_id: "known-anchor",
          stop_reason: "anchor_matched",
          terminal_reason: "anchor_matched",
          complete: true,
          raw: { engine: "youtubei.js@test" },
        };
      },
    }),
    fetchDetail: async (videoId) => ({
      ...detail(videoId, 100),
      published_at: videoId === "old-video"
        ? "2026-07-09T00:00:00.000Z"
        : "2026-07-19T00:00:00.000Z",
    }),
  });

  assert.deepEqual(secondScanAnchors, [
    { id: "known-anchor", published_day: "2026-07-10" },
    { id: "old-video", published_day: "2026-07-09" },
  ]);
});

test("Catch-up exhaustion classifies the latest 30 Videos in one discovery run", async () => {
  const fixture = databaseFixture();
  const fetched = [];
  const scannedEntries = Array.from({ length: 150 }, (_, index) => ({
    id: `catchup-${String(index + 1).padStart(3, "0")}`,
    position: index + 1,
    content_type: "video",
    published_day: "2026-07-20",
  }));

  const executeAttempt = (runId, jobId) => executeIncrementalVideo({
    plan: { ...plan(), job_id: jobId },
    runId,
    startedAt: "2026-07-20T00:00:00.000Z",
    query: fixture.query,
    withTransaction: fixture.withTransaction,
    getChannelSnapshot: async () => ({
      async scanUploads() {
        return {
          playlist_id: "UUvideo",
          entries: scannedEntries,
          pages: 3,
          first_page_item_count: 100,
          catch_up_item_count: 50,
          item_count: 150,
          parse_gap_count: 0,
          anchor_matched: false,
          matched_anchor_id: null,
          stop_reason: "catchup_limit",
          terminal_reason: "catchup_limit",
          complete: false,
          raw: { engine: "youtubei.js@test" },
        };
      },
    }),
    fetchDetail: async (videoId) => {
      fetched.push(videoId);
      return detail(videoId, 100);
    },
  });

  const result = await executeAttempt(
    "incremental:catchup-latest-30",
    "incremental__UCvideo__20260720__clock_7__latest-30",
  );
  assert.equal(result.outcome, "complete");
  assert.equal(result.first_seen_count, 30);
  assert.deepEqual(
    fixture.state.contents
      .filter((row) => row.source_content_id.startsWith("catchup-"))
      .map((row) => row.source_content_id),
    scannedEntries.slice(0, 30).map((entry) => entry.id),
  );
  assert.equal(fetched.some((id) => scannedEntries.slice(30).some((entry) => entry.id === id)), false);
  assert.equal(fixture.state.cursorUpdates.length, 1);
  assert.equal(fixture.state.cursorUpdates[0].outcome, "complete");
  assert.deepEqual(
    fixture.state.cursorUpdates[0].anchorVideoIds,
    scannedEntries.slice(0, 20).map((entry) => entry.id),
  );
  assert.deepEqual(JSON.parse(fixture.state.cursorUpdates[0].sourceCursor), {
    playlist_id: "UUvideo",
    matched_anchor_id: null,
    crossed_anchor_ids: [],
    terminal_reason: "gap_abandoned_latest_30",
    gap_abandonment: {
      policy_version: "latest-30-on-catchup-limit-v1",
      source_stop_reason: "catchup_limit",
      scanned_item_count: 150,
      first_page_item_count: 100,
      catch_up_item_count: 50,
      catch_up_item_limit: 50,
      selected_item_count: 30,
    },
  });
  assert.equal(fixture.state.outbox.length, 1);
  assert.equal(fixture.state.outbox[0].outcome, "complete");
  const discovery = fixture.state.outbox[0].payload.discovery.payload;
  assert.equal(discovery.stop_reason, "gap_abandoned_latest_30");
  assert.equal(discovery.items, 30);
  assert.deepEqual(discovery.gap_abandonment, {
    policy_version: "latest-30-on-catchup-limit-v1",
    source_stop_reason: "catchup_limit",
    scanned_item_count: 150,
    first_page_item_count: 100,
    catch_up_item_count: 50,
    catch_up_item_limit: 50,
    selected_item_count: 30,
    scanned_video_ids: scannedEntries.map((entry) => entry.id),
    selected_video_ids: scannedEntries.slice(0, 30).map((entry) => entry.id),
    abandoned_anchor_ids: ["known-anchor", "old-video"],
  });
});

test("Catch-up exhaustion with parse gaps remains Partial and has no Content side effects", async () => {
  const fixture = databaseFixture();
  const initialContents = structuredClone(fixture.state.contents);
  const fetched = [];
  const scannedEntries = Array.from({ length: 150 }, (_, index) => ({
    id: `parse-gap-${index + 1}`,
    position: index + 1,
    content_type: "video",
    published_day: "2026-07-20",
  }));

  const result = await executeIncrementalVideo({
    plan: plan(),
    runId: "incremental:catchup-parse-gap",
    startedAt: "2026-07-20T00:00:00.000Z",
    query: fixture.query,
    withTransaction: fixture.withTransaction,
    getChannelSnapshot: async () => ({
      async scanUploads() {
        return {
          playlist_id: "UUvideo",
          entries: scannedEntries,
          pages: 3,
          first_page_item_count: 100,
          catch_up_item_count: 50,
          item_count: 150,
          parse_gap_count: 1,
          anchor_matched: false,
          matched_anchor_id: null,
          stop_reason: "catchup_limit",
          terminal_reason: "catchup_limit",
          complete: false,
          raw: { engine: "youtubei.js@test" },
        };
      },
    }),
    fetchDetail: async (videoId) => {
      fetched.push(videoId);
      return detail(videoId, 100);
    },
  });

  assert.equal(result.outcome, "partial");
  assert.deepEqual(fetched, []);
  assert.deepEqual(fixture.state.contents, initialContents);
  assert.equal(fixture.state.candidateRows.length, scannedEntries.length);
  assert.equal(
    fixture.state.candidateRows.every((row) => (
      row.disposition === "deferred"
      && row.result_json.disposition.reason_code === "discovery_scan_incomplete"
    )),
    true,
  );
  assert.equal(
    fixture.state.outbox[0].payload.discovery.payload.deferred_count,
    scannedEntries.length,
  );
  assert.equal(fixture.state.cursorUpdates[0].anchorVideoIds, null);
  assert.equal(fixture.state.cursorUpdates[0].sourceCursor, null);
  assert.equal(fixture.state.outbox[0].payload.discovery.payload.stop_reason, "catchup_limit");
});

test("Catch-up cannot abandon a gap before the configured budget is exhausted", async () => {
  const fixture = databaseFixture();
  const initialContents = structuredClone(fixture.state.contents);
  const fetched = [];
  const scannedEntries = Array.from({ length: 149 }, (_, index) => ({
    id: `early-stop-${index + 1}`,
    position: index + 1,
    content_type: "video",
    published_day: "2026-07-20",
  }));

  const result = await executeIncrementalVideo({
    plan: plan(),
    runId: "incremental:catchup-before-limit",
    startedAt: "2026-07-20T00:00:00.000Z",
    query: fixture.query,
    withTransaction: fixture.withTransaction,
    getChannelSnapshot: async () => ({
      async scanUploads() {
        return {
          playlist_id: "UUvideo",
          entries: scannedEntries,
          pages: 3,
          first_page_item_count: 100,
          catch_up_item_count: 49,
          item_count: 149,
          parse_gap_count: 0,
          anchor_matched: false,
          matched_anchor_id: null,
          stop_reason: "catchup_limit",
          terminal_reason: "catchup_limit",
          complete: false,
          raw: { engine: "youtubei.js@test" },
        };
      },
    }),
    fetchDetail: async (videoId) => {
      fetched.push(videoId);
      return detail(videoId, 100);
    },
  });

  assert.equal(result.outcome, "partial");
  assert.deepEqual(fetched, []);
  assert.deepEqual(fixture.state.contents, initialContents);
  assert.equal(fixture.state.candidateRows.length, scannedEntries.length);
  assert.equal(
    fixture.state.candidateRows.every((row) => (
      row.disposition === "deferred"
      && row.result_json.disposition.reason_code === "discovery_scan_incomplete"
    )),
    true,
  );
  assert.equal(fixture.state.enrichPending.size, 0);
  assert.equal(fixture.state.cursorUpdates[0].anchorVideoIds, null);
  assert.equal(fixture.state.cursorUpdates[0].sourceCursor, null);
  assert.equal(fixture.state.outbox[0].payload.discovery.payload.stop_reason, "catchup_limit");
});

test("Video discovery does not invent a video type when Watch detail collection fails", async () => {
  const fixture = databaseFixture();
  const result = await executeIncrementalVideo({
    plan: plan(),
    runId: "incremental:publish-day-fallback",
    startedAt: "2026-07-20T00:00:00.000Z",
    query: fixture.query,
    withTransaction: fixture.withTransaction,
    getChannelSnapshot: async () => ({
      async scanUploads() {
        return {
          playlist_id: "UUvideo",
          entries: [
            {
              id: "new-video",
              position: 1,
              content_type: null,
              title: "New",
              published_day: "2026-07-19",
            },
            {
              id: "known-anchor",
              position: 2,
              content_type: "video",
              title: "Known",
              published_day: "2026-07-10",
            },
          ],
          pages: 1,
          item_count: 2,
          parse_gap_count: 0,
          anchor_matched: true,
          matched_anchor_id: "known-anchor",
          stop_reason: "anchor_matched",
          terminal_reason: "anchor_matched",
          complete: true,
          raw: { engine: "youtubei.js@test" },
        };
      },
    }),
    fetchDetail: async () => {
      throw new Error("detail unavailable");
    },
  });

  assert.equal(result.outcome, "partial");
  assert.equal(result.first_seen_count, 0);
  assert.equal(
    fixture.state.contents.some((row) => row.source_content_id === "new-video"),
    false,
  );
  assert.deepEqual(
    fixture.state.outbox[0].payload.discovery.payload.unresolved_video_ids,
    ["new-video"],
  );
  assert.deepEqual(
    (({ detail_status, missing_fields, disposition, next_attempt_at, result_json, error_message }) => ({
      detail_status,
      missing_fields,
      disposition,
      next_attempt_at,
      disposition_evidence: result_json.disposition,
      collection_error: result_json.collection_error,
      upload_evidence: result_json.flat,
      error_message,
    }))(fixture.state.candidateRows[0]),
    {
      detail_status: "failed",
      missing_fields: ["detail", "content_type"],
      disposition: "deferred",
      next_attempt_at: "2026-07-20T01:00:00.000Z",
      disposition_evidence: {
        version: "video-disposition-v1",
        kind: "deferred",
        reason_code: "detail_collection_failed",
        retry_class: "player_retry",
        retryable: true,
        observed_at: "2026-07-20T00:00:00.000Z",
        next_attempt_at: "2026-07-20T01:00:00.000Z",
      },
      collection_error: {
        name: "Error",
        code: null,
        message: "detail unavailable",
      },
      upload_evidence: {
        id: "new-video",
        position: 1,
        content_type: null,
        title: "New",
        published_day: "2026-07-19",
      },
      error_message: "detail unavailable",
    },
  );
  assert.deepEqual(
    fixture.state.outbox[0].payload.discovery.payload.dispositions,
    [{
      video_id: "new-video",
      kind: "deferred",
      reason_code: "detail_collection_failed",
      retry_class: "player_retry",
    }],
  );
  assert.equal(fixture.state.cursorUpdates[0].sourceCursor, null);
});

test("Video detail route challenges abort the cycle before a Partial Observation is committed", async () => {
  const fixture = databaseFixture();
  const challenge = Object.assign(
    new Error("YouTube bot challenge HTTP 200 for /youtubei/v1/player"),
    {
      youtube_failure_evidence: {
        status: 200,
        body: "Sign in to confirm you're not a bot",
        source: "youtubejs_player",
        target_url: "https://www.youtube.com/youtubei/v1/player",
        client: "WEB",
      },
    },
  );

  await assert.rejects(
    executeIncrementalVideo({
      plan: plan(),
      runId: "incremental:route-challenge",
      startedAt: "2026-07-20T00:00:00.000Z",
      query: fixture.query,
      withTransaction: fixture.withTransaction,
      getChannelSnapshot: async () => ({
        async scanUploads() {
          return {
            playlist_id: "UUvideo",
            entries: [
              {
                id: "challenged-video",
                position: 1,
                content_type: null,
                title: "Challenged",
                published_day: "2026-07-19",
              },
              {
                id: "known-anchor",
                position: 2,
                content_type: "video",
                title: "Known",
                published_day: "2026-07-10",
              },
            ],
            pages: 1,
            item_count: 2,
            parse_gap_count: 0,
            anchor_matched: true,
            matched_anchor_id: "known-anchor",
            stop_reason: "anchor_matched",
            terminal_reason: "anchor_matched",
            complete: true,
            raw: { engine: "youtubei.js@test" },
          };
        },
      }),
      fetchDetail: async () => {
        throw challenge;
      },
    }),
    (error) => error === challenge,
  );

  assert.equal(fixture.state.outbox.length, 0);
  assert.equal(fixture.state.cursorUpdates.length, 0);
});
