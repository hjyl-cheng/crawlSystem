const MILLISECONDS_PER_DAY = 86400000;

const VALUE_STATUSES = new Set(["exact", "relative", "estimated"]);
const EMPTY_STATUSES = new Set(["unavailable", "unresolved"]);
const PRECISIONS = new Set(["second", "date_only"]);
const GENERIC_UPLOAD_SOURCES = new Set(["youtube_uploads", "uploads_playlist"]);
const EVIDENCE_QUALITY = Object.freeze({
  exact: Object.freeze({ second: 500, date_only: 400 }),
  estimated: Object.freeze({ second: 320, date_only: 310 }),
  relative: Object.freeze({ second: 220, date_only: 210 }),
  unavailable: Object.freeze({ unknown: 100 }),
  unresolved: Object.freeze({ unknown: 0 }),
});

export const PUBLICATION_TIME_CLASSIFIER_VERSION = "publication-time-evidence-v1";

function text(value) {
  const normalized = String(value ?? "").trim();
  return normalized || null;
}

function unresolvedEvidence(status = "unresolved") {
  return {
    published_at: null,
    published_at_status: EMPTY_STATUSES.has(status) ? status : "unresolved",
    published_at_precision: "unknown",
    published_at_source: null,
  };
}

function utcDateOnly(value) {
  const raw = text(value);
  if (!raw) return null;
  const directDay = raw && /^(\d{4}-\d{2}-\d{2})$/.exec(raw)?.[1];
  const parsedValue = directDay ? null : new Date(value);
  const day = directDay ?? (!Number.isNaN(parsedValue?.getTime())
    ? parsedValue.toISOString().slice(0, 10)
    : null);
  if (!day) return null;
  const parsed = new Date(`${day}T00:00:00.000Z`);
  if (Number.isNaN(parsed.getTime()) || parsed.toISOString().slice(0, 10) !== day) return null;
  return parsed.toISOString();
}

function normalizedTimestamp(value, precision) {
  if (value == null || (typeof value === "string" && !value.trim())) return null;
  if (precision === "date_only") return utcDateOnly(value);
  const parsed = new Date(value);
  return Number.isNaN(parsed.getTime()) ? null : parsed.toISOString();
}

function inferredPrecision(value) {
  if (value == null || (typeof value === "string" && !value.trim())) return "unknown";
  const raw = text(value);
  if (/^\d{4}-\d{2}-\d{2}$/.test(raw ?? "")) return "date_only";
  const parsed = new Date(value);
  return Number.isNaN(parsed.getTime()) ? "unknown" : "second";
}

export function publicationEvidenceFromFields(input = {}, {
  publishedAt = input?.published_at,
  fallbackSource = null,
  missingStatus = "unresolved",
} = {}) {
  const source = text(input?.published_at_source)
    ?? text(input?.source)
    ?? text(fallbackSource);
  const precision = text(input?.published_at_precision)?.toLowerCase()
    ?? inferredPrecision(publishedAt);
  const status = text(input?.published_at_status)?.toLowerCase()
    ?? (source === "youtube_uploads_relative_time"
      ? "relative"
      : publishedAt != null
          && source
          && !GENERIC_UPLOAD_SOURCES.has(source)
          && precision !== "unknown"
        ? "exact"
        : missingStatus);
  return normalizePublicationEvidence({
    published_at: publishedAt,
    published_at_status: status,
    published_at_precision: precision,
    published_at_source: source,
  });
}

export function normalizePublicationEvidence(input = {}) {
  const status = text(input?.published_at_status)?.toLowerCase() ?? "unresolved";
  if (EMPTY_STATUSES.has(status)) return unresolvedEvidence(status);
  const precision = text(input?.published_at_precision)?.toLowerCase() ?? "unknown";
  const source = text(input?.published_at_source);
  if (!VALUE_STATUSES.has(status) || !PRECISIONS.has(precision) || !source) {
    return unresolvedEvidence();
  }
  const publishedAt = normalizedTimestamp(input?.published_at, precision);
  if (!publishedAt) return unresolvedEvidence();
  return {
    published_at: publishedAt,
    published_at_status: status,
    published_at_precision: precision,
    published_at_source: source,
  };
}

function evidenceQuality(evidence) {
  return EVIDENCE_QUALITY[evidence.published_at_status]?.[evidence.published_at_precision] ?? 0;
}

function evidenceValueEquals(left, right) {
  return left.published_at === right.published_at
    && left.published_at_status === right.published_at_status
    && left.published_at_precision === right.published_at_precision;
}

export function selectPublicationEvidence(current, candidate) {
  const normalizedCurrent = normalizePublicationEvidence(current);
  const normalizedCandidate = normalizePublicationEvidence(candidate);
  const currentQuality = evidenceQuality(normalizedCurrent);
  const candidateQuality = evidenceQuality(normalizedCandidate);
  if (currentQuality > candidateQuality) {
    return { evidence: normalizedCurrent, selected: "current", reason_code: "current_stronger" };
  }
  if (candidateQuality > currentQuality) {
    return { evidence: normalizedCandidate, selected: "candidate", reason_code: "candidate_stronger" };
  }
  if (evidenceValueEquals(normalizedCurrent, normalizedCandidate)) {
    return { evidence: normalizedCurrent, selected: "equal", reason_code: "equivalent" };
  }
  return {
    evidence: normalizedCurrent,
    selected: "current",
    reason_code: "equal_quality_conflict_current_retained",
    conflict: {
      reason_code: "equal_quality_publication_conflict",
      current: normalizedCurrent,
      candidate: normalizedCandidate,
    },
  };
}

export function publicationEvidenceConflictRecord(selection) {
  if (!selection?.conflict) return null;
  return {
    ...selection.conflict,
    resolution: {
      selected: selection.selected,
      reason_code: selection.reason_code,
    },
  };
}

function sqlReference(value) {
  const reference = String(value ?? "");
  if (!/^[a-z_][a-z0-9_.]*$/i.test(reference)) {
    throw new TypeError("publication evidence SQL references must be trusted identifiers");
  }
  return reference;
}

function evidenceQualitySql(reference) {
  const value = sqlReference(reference);
  const valueCases = ["exact", "estimated", "relative"].map((status) => (
    `WHEN ${value}.published_at_status='${status}'`
    + ` AND ${value}.published_at IS NOT NULL`
    + ` AND NULLIF(btrim(${value}.published_at_source),'') IS NOT NULL`
    + ` THEN CASE ${value}.published_at_precision`
    + ` WHEN 'second' THEN ${EVIDENCE_QUALITY[status].second}`
    + ` WHEN 'date_only' THEN ${EVIDENCE_QUALITY[status].date_only}`
    + " ELSE 0 END"
  ));
  return `(CASE ${valueCases.join(" ")}`
    + ` WHEN ${value}.published_at_status='unavailable' THEN ${EVIDENCE_QUALITY.unavailable.unknown}`
    + " ELSE 0 END)";
}

function evidenceDistinctSql(current, candidate) {
  const currentValue = sqlReference(current);
  const candidateValue = sqlReference(candidate);
  return `(${currentValue}.published_at IS DISTINCT FROM ${candidateValue}.published_at`
    + ` OR ${currentValue}.published_at_status IS DISTINCT FROM ${candidateValue}.published_at_status`
    + ` OR ${currentValue}.published_at_precision IS DISTINCT FROM ${candidateValue}.published_at_precision)`;
}

function evidenceJsonSql(reference) {
  const value = sqlReference(reference);
  return `jsonb_build_object(`
    + `'published_at',${value}.published_at,`
    + `'published_at_status',${value}.published_at_status,`
    + `'published_at_precision',${value}.published_at_precision,`
    + `'published_at_source',${value}.published_at_source)`;
}

export function publicationEvidenceCandidateWinsSql(current, candidate) {
  const currentQuality = evidenceQualitySql(current);
  const candidateQuality = evidenceQualitySql(candidate);
  return `((${candidateQuality}) > (${currentQuality}))`;
}

export function publicationEvidenceConflictPatchSql(current, candidate) {
  const currentQuality = evidenceQualitySql(current);
  const candidateQuality = evidenceQualitySql(candidate);
  const distinct = evidenceDistinctSql(current, candidate);
  return `(CASE WHEN (${candidateQuality}) = (${currentQuality})`
    + ` AND (${candidateQuality}) > ${EVIDENCE_QUALITY.unavailable.unknown}`
    + ` AND ${distinct}`
    + ` THEN jsonb_build_object('publication_evidence_conflict',jsonb_build_object(`
    + `'reason_code','equal_quality_publication_conflict',`
    + `'current',${evidenceJsonSql(current)},`
    + `'candidate',${evidenceJsonSql(candidate)},`
    + `'resolution',jsonb_build_object(`
    + `'selected','current','reason_code','equal_quality_conflict_current_retained')))`
    + ` ELSE '{}'::jsonb END)`;
}

function classification(evidence, relation, reasonCode, basis) {
  return {
    relation,
    reason_code: reasonCode,
    basis,
    precision: evidence.published_at_precision,
    source: evidence.published_at_source,
    classifier_version: PUBLICATION_TIME_CLASSIFIER_VERSION,
  };
}

export function classifyPublicationWindow(input, { asOf, maxAgeDays } = {}) {
  const evidence = normalizePublicationEvidence(input);
  const observedAt = new Date(asOf);
  const windowDays = Number(maxAgeDays);
  if (Number.isNaN(observedAt.getTime()) || !Number.isFinite(windowDays) || windowDays <= 0) {
    return classification(evidence, "unresolved", "invalid_window", null);
  }
  if (evidence.published_at_status !== "exact") {
    return classification(
      evidence,
      "unresolved",
      `status_${evidence.published_at_status}`,
      null,
    );
  }

  const publishedMs = new Date(evidence.published_at).getTime();
  const asOfMs = observedAt.getTime();
  const cutoffMs = asOfMs - (windowDays * MILLISECONDS_PER_DAY);
  if (evidence.published_at_precision === "second") {
    if (publishedMs > asOfMs) {
      return classification(evidence, "after_as_of", "instant_after_as_of", "instant");
    }
    if (publishedMs <= cutoffMs) {
      return classification(evidence, "outside", "instant_at_or_before_cutoff", "instant");
    }
    return classification(evidence, "inside", "instant_inside_window", "instant");
  }

  const publishedDay = Math.floor(publishedMs / MILLISECONDS_PER_DAY);
  const asOfDay = Math.floor(asOfMs / MILLISECONDS_PER_DAY);
  const cutoffDay = Math.floor(cutoffMs / MILLISECONDS_PER_DAY);
  if (publishedDay > asOfDay) {
    return classification(evidence, "after_as_of", "date_after_as_of", "utc_civil_date");
  }
  if (publishedDay < cutoffDay) {
    return classification(evidence, "outside", "date_before_cutoff_day", "utc_civil_date");
  }
  if (publishedDay === cutoffDay) {
    return classification(evidence, "cutoff_overlap", "date_only_spans_cutoff", "utc_civil_date");
  }
  return classification(evidence, "inside", "date_inside_window", "utc_civil_date");
}
