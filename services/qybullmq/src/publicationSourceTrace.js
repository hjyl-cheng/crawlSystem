function object(value) {
  return value && typeof value === "object" && !Array.isArray(value) ? value : {};
}

function text(value) {
  const output = String(value ?? "").trim();
  return output || null;
}

function integer(value) {
  if (value === null || value === undefined || value === "") return null;
  const parsed = Number(value);
  return Number.isSafeInteger(parsed) ? parsed : null;
}

function timestamp(value) {
  if (value === null || value === undefined || value === "") return null;
  const parsed = new Date(value);
  return Number.isNaN(parsed.getTime()) ? null : parsed.toISOString();
}

function compareText(left, right) {
  return Buffer.compare(Buffer.from(String(left ?? ""), "utf8"), Buffer.from(String(right ?? ""), "utf8"));
}

function sortedIssues(issues) {
  const unique = new Map();
  for (const issue of issues) {
    unique.set([issue.domain, issue.code].join("\u0000"), issue);
  }
  return [...unique.values()].sort((left, right) => (
    compareText(left.domain, right.domain) || compareText(left.code, right.code)
  ));
}

export function buildPublicationSourceTrace(source, current, domain) {
  const cursor = object(source?.cursor);
  const completeObservation = object(source?.complete_observation);
  const latestObservation = object(source?.latest_observation);
  const run = object(source?.run);
  const issues = [];
  const expectedKind = domain === "channel" ? "about" : domain;
  const observationId = text(completeObservation.observation_id);
  const cursorChannelId = text(cursor.channel_id);
  if (!cursorChannelId) issues.push({ domain, code: "source_cursor_missing" });
  const runId = text(completeObservation.run_id);
  if (!observationId) {
    issues.push({ domain, code: "source_complete_observation_missing" });
  } else {
    if (text(cursor.latest_complete_observation_id) !== observationId) {
      issues.push({ domain, code: "source_cursor_observation_mismatch" });
    }
    if (text(completeObservation.observation_kind) !== expectedKind || completeObservation.outcome !== "complete") {
      issues.push({ domain, code: "source_observation_not_complete" });
    }
    if (text(completeObservation.channel_id) && text(completeObservation.channel_id) !== cursorChannelId) {
      issues.push({ domain, code: "source_observation_channel_mismatch" });
    }
    if (!runId || text(run.run_id) !== runId) issues.push({ domain, code: "source_run_missing" });
    if (runId && text(run.channel_id) !== cursorChannelId) {
      issues.push({ domain, code: "source_run_channel_mismatch" });
    }
  }
  const currentObservationId = text(current?.observation_id);
  if (currentObservationId && observationId && currentObservationId !== observationId) {
    issues.push({ domain, code: "source_current_observation_mismatch" });
  }
  const currentFactsHash = text(current?.facts_hash);
  const observationFactsHashValue = text(completeObservation.facts_hash);
  if (!currentFactsHash) issues.push({ domain, code: "source_current_hash_missing" });
  if (currentFactsHash && observationFactsHashValue && currentFactsHash !== observationFactsHashValue) {
    issues.push({ domain, code: "source_current_hash_mismatch" });
  }
  if (!timestamp(current?.observed_at)) issues.push({ domain, code: "source_current_observed_at_missing" });
  return {
    traceable: issues.length === 0,
    current: {
      observation_id: currentObservationId,
      observed_at: timestamp(current?.observed_at),
      facts_hash: currentFactsHash,
      business_hash: text(current?.business_hash),
      identity_observed_at: timestamp(current?.identity_observed_at),
      identity_hash: text(current?.identity_hash),
    },
    cursor: text(cursor.channel_id) ? {
      latest_sequence: integer(cursor.latest_sequence),
      latest_observation_id: text(cursor.latest_observation_id),
      latest_observed_at: timestamp(cursor.latest_observed_at),
      latest_complete_observation_id: text(cursor.latest_complete_observation_id),
      latest_complete_observed_at: timestamp(cursor.latest_complete_observed_at),
      current_facts_hash: text(cursor.current_facts_hash),
    } : null,
    latest_observation: text(latestObservation.observation_id) ? {
      observation_id: text(latestObservation.observation_id),
      observed_at: timestamp(latestObservation.observed_at),
      outcome: text(latestObservation.outcome),
      outcome_reason_code: text(latestObservation.outcome_reason_code),
    } : null,
    complete_observation: observationId ? {
      observation_id: observationId,
      observed_at: timestamp(completeObservation.observed_at),
      run_id: runId,
      kind_sequence: integer(completeObservation.kind_sequence),
      outcome: text(completeObservation.outcome),
      outcome_reason_code: text(completeObservation.outcome_reason_code),
      facts_hash: observationFactsHashValue,
      crawler_version: text(completeObservation.crawler_version),
      extractor_versions: object(completeObservation.extractor_versions),
    } : null,
    run: text(run.run_id) ? {
      run_id: text(run.run_id),
      status: text(run.status),
      crawl_mode: text(run.crawl_mode),
      plan_id: text(run.plan_id),
      policy_version: text(run.policy_version),
      crawler_version: text(run.crawler_version),
    } : null,
    issues: sortedIssues(issues),
  };
}
