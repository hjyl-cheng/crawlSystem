import {
  observationFactsHash,
} from "../src/crawlObservationStore.js";
import {
  buildVideoActivityEvidence,
} from "../src/videoActivityLifecycle.js";

function discovery() {
  return {
    outcome: "complete",
    payload: {
      pages: 1,
      items: 0,
      anchor_matched: true,
      stop_reason: "anchor_matched",
      parse_gap_count: 0,
      first_seen: [],
      first_seen_count: 0,
      detail_success_count: 0,
      detail_failure_count: 0,
    },
  };
}

function recentSampling() {
  return {
    outcome: "complete",
    payload: {
      recent_count: 0,
      stale_ratio: 0,
      selected_count: 0,
      success_count: 0,
      failure_count: 0,
      next_count: 0,
      comparable_view_count: 0,
      view_changed_count: 0,
      view_delta_total: 0,
      engagement_changed_count: 0,
    },
  };
}

function lifecycle(overrides = {}) {
  return {
    recent_published_content_count: 1,
    uncertain_content_count: 0,
    classifier_version: "publication-time-evidence-v1",
    policy_version: "incremental-video-activity-v5",
    relation_counts: {
      inside: 1,
      outside: 2,
      after_as_of: 0,
      cutoff_overlap: 0,
      unresolved: 0,
    },
    unresolved_by_status_counts: {
      relative: 0,
      estimated: 0,
      unavailable: 0,
      unresolved: 0,
    },
    evidence_complete: true,
    evidence_scan_complete: true,
    evidence_scan_rows: 3,
    evidence_scan_page_count: 1,
    evidence_scan_elapsed_ms: 2.1614830009639263,
    evidence_scan_truncated_count: 0,
    evidence_scan_truncated_count_is_lower_bound: false,
    evidence_scan_stop_reason: "complete",
    evidence_scan_row_limit: 1000,
    evidence_scan_page_size: 200,
    evidence_scan_time_budget_ms: 500,
    ...overrides,
  };
}

function event({ eventId, observationId, sequence, evidence, activity }) {
  const payload = {
    discovery: discovery(),
    recent_sampling: recentSampling(),
    activity_evidence: buildVideoActivityEvidence(evidence),
    ...(activity ? { activity } : {}),
  };
  return {
    event_id: eventId,
    event_type: "crawler.observation.recorded",
    event_version: 1,
    observation_id: observationId,
    plan_id: "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
    channel_id: "UCfeatureContract",
    observation_kind: "video",
    kind_sequence: sequence,
    observed_at: "2026-09-03T10:00:00.000Z",
    outcome: "complete",
    crawler_version: "qybullmq-contract-sample",
    payload_hash: observationFactsHash(payload),
    payload,
  };
}

const activeEvidence = lifecycle();
const inconclusiveEvidence = lifecycle({
  recent_published_content_count: 0,
  uncertain_content_count: 1,
  relation_counts: {
    inside: 0,
    outside: 2,
    after_as_of: 0,
    cutoff_overlap: 0,
    unresolved: 1,
  },
  unresolved_by_status_counts: {
    relative: 1,
    estimated: 0,
    unavailable: 0,
    unresolved: 0,
  },
  evidence_complete: false,
  evidence_scan_complete: false,
  evidence_scan_rows: 2,
  evidence_scan_truncated_count: 1,
  evidence_scan_truncated_count_is_lower_bound: true,
  evidence_scan_stop_reason: "row_limit",
  evidence_scan_row_limit: 2,
  evidence_scan_page_size: 2,
});

const samples = [
  event({
    eventId: "11111111-1111-4111-8111-111111111111",
    observationId: "22222222-2222-4222-8222-222222222222",
    sequence: 1,
    evidence: activeEvidence,
    activity: {
      window_days: 90,
      recent_published_content_count: 1,
      lifecycle_status: "active",
      dormant_reason: null,
      dormant_since: null,
      dormant_recheck_day: null,
      dormant_cycle: 0,
    },
  }),
  event({
    eventId: "33333333-3333-4333-8333-333333333333",
    observationId: "44444444-4444-4444-8444-444444444444",
    sequence: 2,
    evidence: inconclusiveEvidence,
  }),
];

process.stdout.write(JSON.stringify(samples));
